// Package client is the local user-facing process. It manages the local TTY,
// spawns the ssh subprocess that brings up the remote attach, runs the
// silent infinite reconnect loop, and handles the `~.`/`~~` escape sequences.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/zziigguurraatt/continuous-ssh/internal/altscreen"
	"github.com/zziigguurraatt/continuous-ssh/internal/buffer"
	"github.com/zziigguurraatt/continuous-ssh/internal/chunk"
	"github.com/zziigguurraatt/continuous-ssh/internal/dlog"
	"github.com/zziigguurraatt/continuous-ssh/internal/proto"
)

// Run is the client entry point. argv excludes the program name.
// Anything other than our own `--debug` / `--debug-file` / `--trace-file`
// flags is forwarded to `ssh` as arguments. The remote always runs the
// user's login shell.
func Run(argv []string) int {
	sshArgs, sessionID, logLevel, mirrorStderr, err := parseArgs(argv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuous-ssh: %v\n", err)
		return 2
	}
	debug := logLevel >= dlog.LevelVerbose

	stdinFd := int(os.Stdin.Fd())
	if !term.IsTerminal(stdinFd) {
		fmt.Fprintln(os.Stderr, "continuous-ssh: stdin must be a terminal (piped stdin is not supported)")
		return 1
	}

	// Only create a per-invocation log file when the user explicitly
	// asks for logging. Without --debug/--debug-file/--trace-file,
	// dlog.Setup with an empty path falls through to io.Discard — no
	// file, no clutter in ~/.continuous-ssh/clients/.
	var logPath string
	if debug {
		p, perr := clientLogPath(sshArgs)
		if perr == nil {
			_ = os.MkdirAll(filepath.Dir(p), 0o700)
			logPath = p
		}
	}
	// stderr mirror only when explicitly asked (--debug). Trace mode
	// would flood the terminal; file-only flags route everything to
	// the log file.
	var stderrSink io.Writer
	if mirrorStderr {
		stderrSink = dlog.CRLFWriter{W: os.Stderr}
	}
	_ = dlog.Setup(logPath, logLevel, stderrSink)
	dlog.E("client starting level=%d sshArgs=%v", logLevel, sshArgs)
	// In file-only modes (--debug-file / --trace-file) the user has no
	// terminal mirror to tell them where the log went. Print the path
	// once on startup so they can `tail -f` it from another shell.
	if debug && !mirrorStderr && logPath != "" {
		fmt.Fprintf(os.Stderr, "continuous-ssh: logging to %s\n", logPath)
	}

	// No on-disk buffer on the client — sliding 10 MiB RAM window only.
	// We ACK every 5 MiB so the daemon trims its own buffer in lockstep;
	// past bytes that fall off the window are still tracked by the byte
	// counter (Buffer.Len) so dedup on reconnect works by offset alone.
	const clientBufRAM uint64 = 10 << 20
	outputBuf, err := buffer.New("", clientBufRAM, 0, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuous-ssh: %v\n", err)
		return 1
	}

	c := &client{
		sshArgs:   sshArgs,
		debug:     debug,
		logLevel:  logLevel,
		sessionID: sessionID, // empty unless --session was supplied
		outputBuf: outputBuf,
		stdinFd:   stdinFd,
		termOut:   &crlfTranslator{w: os.Stdout},
		pongCh:    make(chan struct{}, 1),
	}

	// Raw mode is entered lazily on the first successful HELLO_ACK (see
	// the activate() closure in c.run). Until then stdin stays in cooked
	// mode so Ctrl-C generates SIGINT and Go's default handler exits the
	// process — useful for bailing out of a hung first connect.
	defer func() {
		if c.oldState != nil {
			_ = term.Restore(stdinFd, c.oldState)
		}
	}()

	// SIGTERM/SIGHUP can land while we're in raw mode (`kill <pid>` from
	// another shell, parent terminal closing, etc.). Go's default handler
	// would exit immediately, skipping our deferred term.Restore and
	// leaving the terminal in raw mode forever. Catch those signals and
	// run the cleanup before exiting. SIGINT is handled by raw mode
	// itself once we're in it (Ctrl-C becomes a keystroke forwarded to
	// the remote), so we don't need to intercept it.
	//
	// SIGKILL still wins — there's no userspace recovery from that —
	// so users out of options can still wedge the terminal. `reset` or
	// `stty sane` fixes it.
	cleanupSigCh := make(chan os.Signal, 1)
	signal.Notify(cleanupSigCh, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		s, ok := <-cleanupSigCh
		if !ok {
			return
		}
		if c.oldState != nil {
			_ = term.Restore(stdinFd, c.oldState)
			sendTerminalReset(os.Stdout, c.inAltScreen.Load())
		}
		fmt.Fprintf(os.Stderr, "continuous-ssh: terminated by %v\n", s)
		os.Exit(143)
	}()
	defer signal.Stop(cleanupSigCh)

	code := c.run()

	if c.oldState != nil {
		_ = term.Restore(stdinFd, c.oldState)
	}
	_ = outputBuf.Close(false)

	// Only reset terminal state if we ever entered raw mode. If we never
	// did (initial connect never succeeded), the terminal was in cooked
	// mode all along and there's nothing to undo.
	if c.oldState != nil {
		sendTerminalReset(os.Stdout, c.inAltScreen.Load())
	}

	// Non-zero exit codes from the daemon encode why the session ended.
	// We print the appropriate one-line notice AFTER sendTerminalReset so
	// it always lands on the main screen, never inside an alt-screen
	// buffer that's about to be discarded. In the alt-screen case the
	// reset's \e[?1049l restored the cursor to its pre-alt position
	// (already a clean line); in the no-alt case the cursor is wherever
	// the remote command left it (potentially mid-line), so we prefix a
	// newline to guarantee the notice lands on its own line.
	dlog.E("post-exit check: aborted=%v code=%d", c.aborted, code)
	if !c.aborted {
		var msg string
		switch code {
		case 129:
			msg = "continuous-ssh: remote daemon stopped."
		case 133:
			msg = "continuous-ssh: remote daemon stopped while disconnected; buffered output replayed."
		case 130:
			msg = "continuous-ssh: session was not cleanly shut down; recovery aborted."
		case 132:
			msg = fmt.Sprintf("continuous-ssh: incompatible protocol (local=%d.%d, remote=%d.%d). Re-deploy the matching xssh binary to the remote.",
				proto.ProtocolMajor, proto.ProtocolMinor, c.remoteMajor, c.remoteMinor)
		case 134:
			msg = "continuous-ssh: remote daemon stopped because the host-wide disk cap was exceeded (long disconnect with fast output). Run `xssh rm` on the remote to free space."
		case 135:
			msg = "continuous-ssh: cannot start new session — the host-wide disk cap is reached. Connect with plain ssh and run `xssh rm` to free space."
		case 136:
			msg = "continuous-ssh: remote session no longer exists; nothing to reconnect to."
		}
		if msg != "" {
			// Emit a bare CR (no LF) before the message: returns the
			// cursor to column 0 of whatever line it's currently on. If
			// the line had stray content (a half-typed bash prompt, etc.)
			// the message overwrites it cleanly without introducing a
			// blank line above. If the cursor was already at column 0
			// (htop's \e[?1049l restored it there, or the last byte was
			// a newline), \r is a no-op. Either way the message lands on
			// exactly one line.
			fmt.Fprint(os.Stdout, "\r")
			fmt.Fprintln(os.Stdout, msg)
		}
	}

	if c.aborted {
		// In alt-screen mode, \e[?1049l (in sendTerminalReset) has just
		// restored the cursor to column 0 of the line immediately after
		// where the command was typed — perfect place to print the
		// message. Outside alt-screen the cursor could be anywhere, so
		// emit a leading newline first to land on a clean line.
		if c.inAltScreen.Load() {
			fmt.Fprint(os.Stdout, "Connection aborted.\n")
		} else {
			fmt.Fprint(os.Stdout, "\nConnection aborted.\n")
		}
		return 130
	}
	dlog.E("client exit code=%d", code)
	return code
}

// parseArgs splits the client argv into (ssh-args, logLevel, mirrorStderr).
//
// Grammar:
//
//	xssh [--debug | --debug-file | --trace-file] [ssh-args...]
//
// --debug       enables verbose logging to a per-invocation file
//               under ~/.continuous-ssh/clients/<date>-<target>-<pid>.log
//               AND mirrors to stderr (CR-LF-translated in raw mode).
//
// --debug-file  same level (verbose) but file-only — no stderr mirror.
//               The log path is printed to stderr once on startup so
//               you can `tail -f` it from another shell.
//
// --trace-file  bumps to trace level — file gains per-frame chatter
//               (OUT/IN frames, every ACK sent). High volume. Always
//               file-only; trace would flood the terminal.
//
// --session ID  reconnect to an existing session by id instead of
//               starting a new one. Useful after a previous xssh
//               invocation bailed out with exit 137 (e.g. a host-key
//               mismatch while traveling through a rogue network):
//               the remote daemon is still running, and this flag
//               lets you reattach to it after fixing the underlying
//               issue. The session id is printed in the exit-137
//               message and also visible in `xssh ls` on the remote.
//
// Every other argv element is forwarded to `ssh` verbatim.
func parseArgs(argv []string) (sshArgs []string, sessionID string, logLevel int, mirrorStderr bool, err error) {
	logLevel = dlog.LevelError
	for i := 0; i < len(argv); i++ {
		t := argv[i]
		switch t {
		case "--debug":
			logLevel = dlog.LevelVerbose
			mirrorStderr = true
		case "--debug-file":
			logLevel = dlog.LevelVerbose
		case "--trace-file":
			logLevel = dlog.LevelTrace
		case "--session":
			if i+1 >= len(argv) {
				return nil, "", 0, false, errors.New("--session requires an argument")
			}
			sessionID = argv[i+1]
			i++
		default:
			sshArgs = append(sshArgs, t)
		}
	}
	if len(sshArgs) == 0 {
		return nil, "", 0, false, errors.New("ssh target required")
	}
	return sshArgs, sessionID, logLevel, mirrorStderr, nil
}

// clientLogPath builds a per-invocation log path under
// ~/.continuous-ssh/clients/. Filename format:
//
//	<YYYYMMDD-HHMMSS>-<sanitized-target>-<pid>.log
//
// The timestamp makes the directory sortable; the target makes it easy
// to find logs for a given host at a glance; the PID disambiguates if
// two invocations land in the same second. Each xssh invocation gets
// its own file — no append-mode interleaving with concurrent sessions.
func clientLogPath(sshArgs []string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	target := "unknown-host"
	// Best-effort target extraction: scan from the end of argv for the
	// first non-flag token. Handles the canonical `xssh [flags] user@host`
	// case. Doesn't try to be a complete ssh-arg parser; if you put the
	// target before value-taking flags the filename will fall back to the
	// "unknown-host" default, signalling that the heuristic didn't find
	// it. parseArgs above has already rejected empty-target invocations.
	for i := len(sshArgs) - 1; i >= 0; i-- {
		t := sshArgs[i]
		if strings.HasPrefix(t, "-") {
			break
		}
		target = sanitizeForFilename(t)
		break
	}
	stamp := time.Now().Format("20060102-150405")
	name := fmt.Sprintf("%s-%s-%d.log", stamp, target, os.Getpid())
	return filepath.Join(home, ".continuous-ssh", "clients", name), nil
}

// firstFewBytes returns up to n bytes of p for use in log lines —
// keeps debug logs compact while still preserving content correlation
// with offsets.
func firstFewBytes(p []byte, n int) []byte {
	if len(p) > n {
		return p[:n]
	}
	return p
}

// sanitizeForFilename replaces filesystem-unfriendly characters in a
// host string so it's safe to use as a path component.
func sanitizeForFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-' || r == '.' || r == '_':
			b.WriteRune(r)
		case r == '@':
			b.WriteString("_at_")
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

type client struct {
	sshArgs   []string
	debug     bool // true when logLevel >= LevelVerbose (kept for terse checks)
	logLevel  int  // dlog.LevelError / LevelVerbose / LevelTrace
	sessionID string
	outputBuf *buffer.Buffer

	stdinFd  int
	oldState *term.State // set after the first activate(); nil while still cooked
	termOut  io.Writer

	// currentPC points at the active session's proto.Conn so that the SIGWINCH
	// handler can fire RESIZE frames at it without coordinating through a
	// channel. Nil when no session is active.
	currentPC atomic.Pointer[proto.Conn]

	// currentSSH points at the running ssh subprocess for the current
	// attempt. Used by the wake-detect probe path to force a fast
	// reconnect when the link is found to be dead post-sleep.
	currentSSH atomic.Pointer[exec.Cmd]

	// pongCh signals "Pong received" from readFrames into the
	// wake-detect probe goroutine. Buffered (size 1) so readFrames
	// can drop a Pong non-blockingly when nobody is waiting.
	pongCh chan struct{}

	// lastOutByte is the last byte we wrote to the local terminal via an
	// OUTPUT frame. Used by the post-exit notice path to decide whether
	// to prepend a newline (the remote command may have ended its output
	// mid-line, in which case our notice would visually attach to that
	// line; if the last byte was already a CR/LF, no prefix is needed).
	lastOutByte byte

	// ackedThrough is the highest byte offset we've ACK'd to the daemon
	// already. We emit a new ACK whenever (outputBuf.Len() - ackedThrough)
	// crosses ackInterval, or when ackIdleMax elapses with any unacked
	// bytes pending. ackMu serialises updates between handleOutputFrame
	// (size trigger) and watchAckIdle (time trigger).
	ackMu        sync.Mutex
	ackedThrough uint64

	// remoteMajor/Minor are populated on protocol-version mismatch so
	// the post-exit message in Run can name both sides' versions.
	remoteMajor uint8
	remoteMinor uint8

	// altScanner watches the remote's output for alt-screen-buffer
	// enter/exit escape sequences so we know whether we're currently in
	// alt-screen mode at exit time. On abort while in alt-screen we emit
	// \e[?1049l (which restores the cursor from the running program's
	// fresh save); otherwise we emit \e[?1047l (no cursor restore, avoids
	// any stale save from a long-exited earlier program).
	altScanner  altscreen.Scanner
	inAltScreen atomic.Bool

	aborted bool
}

func (c *client) run() int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stdinCh := make(chan []byte, 16)

	// activate flips the local terminal into raw mode and starts the
	// goroutines that need it (stdin escape parsing, SIGWINCH watching).
	// It's invoked from runOnce on the first successful HELLO_ACK and is
	// idempotent thereafter. Until activate is called, stdin remains in
	// cooked mode and Ctrl-C raises SIGINT, which Go's default handler
	// exits the process for — giving the user an out if the initial
	// connect is failing.
	activate := func() error {
		if c.oldState != nil {
			return nil
		}
		s, err := term.MakeRaw(c.stdinFd)
		if err != nil {
			return err
		}
		c.oldState = s
		go c.readStdinForever(ctx, stdinCh, cancel)
		go c.watchWindowSize(ctx)
		go c.watchAckIdle(ctx)
		go c.watchForWake(ctx)
		return nil
	}

	// haveHelloAcked tracks whether any attempt in this invocation has
	// received a HELLO_ACK yet. Was previously inferred from
	// `c.sessionID == ""`, but that's no longer accurate now that
	// --session can pre-populate sessionID before any attempt runs.
	haveHelloAcked := false
	for {
		if ctx.Err() != nil {
			c.aborted = true
			return 130
		}
		// runOnce uses "this attempt is a fresh-new-session creation"
		// to decide between --new and --session for the remote attach;
		// that's distinct from "is this the first attempt of the
		// invocation". Pass the fresh-session bit explicitly.
		result := c.runOnce(ctx, c.sessionID == "", stdinCh, activate)
		if result.helloAcked {
			haveHelloAcked = true
		}
		if result.exit {
			return result.exitCode
		}
		if ctx.Err() != nil {
			c.aborted = true
			return 130
		}
		// First-connect failures don't retry — there's nothing yet to
		// reconnect to, and silently re-attempting would mask real
		// problems (wrong host, ssh auth failure, remote xssh binary
		// missing). Surface ssh's own diagnostic and exit.
		//
		// Read `haveHelloAcked` here (AFTER the update above), not a
		// snapshot taken before runOnce ran: an attempt that completes
		// HELLO_ACK and then dies mid-session on the very first
		// iteration is a reconnect scenario, not a first-connect
		// failure. A stale pre-iteration snapshot would misroute it
		// into the bail-with-"initial connection failed" branch.
		if !haveHelloAcked {
			msg := strings.TrimSpace(result.sshStderr)
			// Remote `xssh` missing on PATH: offer to push the
			// local binary up before bailing. On success we loop
			// back to retry the original connection; on user
			// declination, the push failing, etc., we fall through
			// to the standard bail-with-message path.
			if isRemoteXsshMissing(msg) && c.offerAutoInstall(msg) {
				continue
			}
			headline := "continuous-ssh: initial connection failed"
			if isRemoteXsshMissing(msg) {
				headline = "continuous-ssh: remote `xssh` not found on PATH — install/deploy the binary on the remote first"
			}
			if msg != "" {
				fmt.Fprintf(os.Stderr, "%s\n%s\n", headline, msg)
			} else {
				fmt.Fprintln(os.Stderr, headline)
			}
			return 1
		}
		// Reconnect-path failure. Retry-forever is meant for the
		// "TCP couldn't connect" case (laptop just resumed, wifi
		// dropping in and out, roaming networks). For everything
		// else (auth changed, host key changed, xssh binary missing
		// on the remote), looping would hide the cause.
		//
		// helloAcked=true means this attempt reached the daemon and
		// died later — definitely a mid-session network blip, retry.
		// helloAcked=false means we never got a handshake; look at
		// ssh's own stderr to decide.
		if !result.helloAcked && !isTransientConnectError(result.sshStderr) {
			msg := strings.TrimSpace(result.sshStderr)
			// Remote `xssh` missing on reconnect: offer to push
			// the local binary up. Same path as the first-connect
			// branch, but we have to dance the local terminal out
			// of raw mode for the prompt and back into raw mode
			// if we proceed to retry.
			if isRemoteXsshMissing(msg) {
				if c.offerAutoInstallRaw(msg) {
					continue
				}
			}
			headline := "continuous-ssh: reconnect failed"
			if isRemoteXsshMissing(msg) {
				headline = "continuous-ssh: remote `xssh` is missing from PATH — redeploy the binary that existed before the disconnect"
			}
			if msg != "" {
				fmt.Fprintf(os.Stderr, "%s\n%s\n", headline, msg)
			} else {
				fmt.Fprintln(os.Stderr, headline)
			}
			// Tell the user how to reattach after fixing the
			// underlying problem — the remote daemon is still
			// running, only the local link refused.
			if c.sessionID != "" {
				fmt.Fprintf(os.Stderr,
					"Re-attach after fixing with: xssh --session %s [ssh-args...] <target>\n",
					c.sessionID)
			}
			return 137
		}
		dlog.V("reconnect: backing off")
		select {
		case <-ctx.Done():
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (c *client) readStdinForever(ctx context.Context, stdinCh chan<- []byte, onAbort context.CancelFunc) {
	defer close(stdinCh)
	buf := make([]byte, 4096)
	atStartOfLine := true
	pendingTilde := false

	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			return
		}
		out := make([]byte, 0, n)
		for _, b := range buf[:n] {
			switch {
			case pendingTilde:
				pendingTilde = false
				switch b {
				case '.':
					dlog.E("abort: ~. detected")
					onAbort()
					return
				case '~':
					out = append(out, '~')
				default:
					out = append(out, '~', b)
				}
				atStartOfLine = b == '\r' || b == '\n'
			case atStartOfLine && b == '~':
				pendingTilde = true
			default:
				out = append(out, b)
				atStartOfLine = b == '\r' || b == '\n'
			}
		}
		if len(out) > 0 {
			select {
			case stdinCh <- out:
			case <-ctx.Done():
				return
			}
		}
	}
}

// watchWindowSize installs a SIGWINCH handler and sends a RESIZE frame on
// every size change. The frame is fired at whatever proto.Conn is currently
// active (set by runOnce); if none, the change is dropped — the next session
// will pick up the current size during its initial RESIZE.
func (c *client) watchWindowSize(ctx context.Context) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGWINCH)
	defer signal.Stop(sigCh)
	for {
		select {
		case <-ctx.Done():
			return
		case <-sigCh:
			c.sendResize()
		}
	}
}

func (c *client) sendResize() {
	pc := c.currentPC.Load()
	if pc == nil {
		return
	}
	cols, rows, err := term.GetSize(c.stdinFd)
	if err != nil {
		dlog.V("term.GetSize: %v", err)
		return
	}
	payload := chunk.ResizePayload{Cols: uint16(cols), Rows: uint16(rows)}.Encode()
	if err := pc.WriteFrame(proto.Frame{Type: proto.Resize, Payload: payload}); err != nil {
		dlog.V("resize write: %v", err)
		return
	}
	dlog.V("sent RESIZE %dx%d", cols, rows)
}

type runResult struct {
	exit     bool
	exitCode int
	// sshStderr captures whatever ssh wrote to its own stderr during
	// this attempt — the underlying connection failure reason, in
	// ssh's own words ("Permission denied (publickey)", "Could not
	// resolve hostname …", etc). Populated on every return so the
	// caller can surface it when a first-connect attempt fails.
	sshStderr string
	// helloAcked is true if this attempt got at least as far as a
	// successful HELLO_ACK decode. The reconnect loop uses this to
	// distinguish "the daemon was reachable, but our connection died
	// mid-session" (true → retry: just a network blip) from "we never
	// got a handshake at all" (false → look at stderr to decide).
	helloAcked bool
}

func (c *client) runOnce(ctx context.Context, first bool, stdinCh <-chan []byte, activate func() error) runResult {
	sshCmd := c.makeSSHCmd(first)
	dlog.V("ssh exec: %v", sshCmd.Args)

	sshStdin, err := sshCmd.StdinPipe()
	if err != nil {
		dlog.E("ssh stdin pipe: %v", err)
		return runResult{}
	}
	sshStdout, err := sshCmd.StdoutPipe()
	if err != nil {
		dlog.E("ssh stdout pipe: %v", err)
		return runResult{}
	}

	// Always capture ssh's stderr into a bounded buffer so a
	// first-connect failure can surface ssh's own diagnostic
	// ("Permission denied (publickey)", "Could not resolve hostname",
	// etc.) instead of a generic message. In debug mode the captured
	// lines are also mirrored to dlog. The reader goroutine is
	// started AFTER sshCmd.Start so a Start failure can't leave a
	// goroutine blocked reading from an orphaned pipe.
	stderrPipe, perr := sshCmd.StderrPipe()
	if perr != nil {
		dlog.E("ssh stderr pipe: %v", perr)
		return runResult{}
	}
	var stderrBuf bytes.Buffer
	var stderrWG sync.WaitGroup
	const maxStderrCapture = 8 << 10 // 8 KiB is plenty for ssh diagnostics

	if err := sshCmd.Start(); err != nil {
		dlog.E("ssh start: %v", err)
		return runResult{sshStderr: err.Error()}
	}
	// Publish the running ssh subprocess so the wake-detect probe
	// can kill it on Pong timeout. Cleared by the deferred store at
	// the very end of runOnce (after the session loop returns).
	c.currentSSH.Store(sshCmd)
	defer c.currentSSH.Store(nil)
	stderrWG.Add(1)
	go func() {
		defer stderrWG.Done()
		scan := bufio.NewScanner(stderrPipe)
		for scan.Scan() {
			line := scan.Text()
			if c.debug {
				dlog.V("ssh stderr: %s", line)
			}
			if stderrBuf.Len() < maxStderrCapture {
				stderrBuf.WriteString(line)
				stderrBuf.WriteByte('\n')
			}
		}
	}()

	pc := proto.NewConn(sshStdout, sshStdin)

	mode := chunk.ModeAttach
	if first {
		mode = chunk.ModeNew
	}
	helloPayload, err := (&chunk.Hello{
		Mode:      mode,
		SessionID: c.sessionID,
		Output:    manifestOf(c.outputBuf),
	}).Encode()
	if err != nil {
		dlog.E("hello encode: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String()}
	}
	dlog.V("sending HELLO mode=%d outputTotal=%d", mode, c.outputBuf.Len())
	if err := pc.WriteFrame(proto.Frame{Type: proto.Hello, Payload: helloPayload}); err != nil {
		dlog.E("hello write: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String()}
	}

	ack, err := pc.ReadFrame()
	if err != nil {
		dlog.E("hello_ack read: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String()}
	}
	// Attach can refuse a `--new` session up-front (currently used
	// when the global disk-cap is at or above DiskBudget) by writing
	// an EXIT frame instead of HELLO_ACK. We treat that as fatal —
	// no retry loop will fix a host-wide policy decision.
	if ack.Type == proto.Exit {
		code := 0
		if len(ack.Payload) >= 4 {
			code = int(int32(binary.BigEndian.Uint32(ack.Payload[:4])))
		}
		dlog.E("EXIT received in HELLO_ACK position code=%d", code)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{exit: true, exitCode: code}
	}
	if ack.Type != proto.HelloAck {
		dlog.E("expected HELLO_ACK got %s", ack.Type)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String()}
	}
	ackHello, err := chunk.DecodeHello(ack.Payload)
	if err != nil {
		dlog.E("hello_ack decode: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String()}
	}
	// HELLO_ACK successfully decoded: we've completed the handshake
	// with the remote daemon. Mark the attempt as having reached
	// HELLO_ACK so the reconnect loop knows a later failure on this
	// attempt is a mid-session disconnect (worth retrying), not a
	// pre-handshake setup failure (worth surfacing).
	helloAcked := true
	// Protocol-version check. Major mismatch is fatal — there's no
	// point retrying because the remote binary is what needs replacing.
	// We surface this to the user via exit code 132 + a clear message
	// (see post-exit switch in Run).
	if ackHello.Major != proto.ProtocolMajor {
		dlog.E("protocol mismatch: remote=%d.%d local=%d.%d",
			ackHello.Major, ackHello.Minor, proto.ProtocolMajor, proto.ProtocolMinor)
		c.remoteMajor = ackHello.Major
		c.remoteMinor = ackHello.Minor
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{exit: true, exitCode: 132}
	}
	if ackHello.Minor != proto.ProtocolMinor {
		dlog.V("protocol minor differs: remote=%d.%d local=%d.%d (compatible)",
			ackHello.Major, ackHello.Minor, proto.ProtocolMajor, proto.ProtocolMinor)
	} else {
		dlog.V("protocol negotiated: local=%d.%d remote=%d.%d",
			proto.ProtocolMajor, proto.ProtocolMinor, ackHello.Major, ackHello.Minor)
	}
	if c.sessionID == "" {
		c.sessionID = ackHello.SessionID
		dlog.V("session id assigned: %s", c.sessionID)
	}
	dlog.V("HELLO_ACK daemonTotal=%d altScreen=%v", ackHello.Output.Total, ackHello.AltScreen)

	// Reattach-into-alt-screen continuity. The daemon advertises
	// whether its current foreground program is in the alt-screen
	// buffer. If yes AND our local terminal isn't already there
	// (i.e. this is a fresh xssh invocation, not an in-loop retry —
	// retries keep alt-screen on the local end across the gap), we
	// need to enter alt-screen on the local terminal before the
	// daemon's output stream starts arriving, and queue a Ctrl-L
	// to make the remote program redraw cleanly into the now-correct
	// buffer. Without this, `xssh --session <id>` into a session
	// whose vim was open would scribble into the user's main screen.
	needRedrawKick := false
	if ackHello.AltScreen && !c.inAltScreen.Load() {
		fmt.Fprint(c.termOut, "\x1b[?1049h")
		c.inAltScreen.Store(true)
		needRedrawKick = true
	}

	// New session — reset the ACK watermark so the first ACK fires after
	// 5 MiB of NEW data, not based on whatever the previous session
	// happened to be at. Guarded because watchAckIdle may be running.
	c.ackMu.Lock()
	c.ackedThrough = c.outputBuf.Len()
	c.ackMu.Unlock()

	// First successful HELLO_ACK switches the local TTY to raw mode and
	// starts the stdin/SIGWINCH goroutines. No-op on subsequent reconnects.
	if err := activate(); err != nil {
		dlog.E("activate raw mode: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{sshStderr: stderrBuf.String(), helloAcked: helloAcked}
	}

	// Trigger the remote program's redraw. Ctrl-L (0x0c) goes
	// straight to pc as a Stdin frame — the remote PTY delivers
	// it to vim/htop/less which respond with a fresh repaint into
	// the alt-screen we just entered. Goes via pc directly rather
	// than through stdinCh because stdinCh is receive-only here.
	if needRedrawKick {
		if werr := pc.WriteFrame(proto.Frame{Type: proto.Stdin, Payload: []byte{0x0c}}); werr != nil {
			dlog.V("alt-screen Ctrl-L kick: %v", werr)
		}
	}

	// This session is now active. Hand it to SIGWINCH and send the initial
	// RESIZE so the remote PTY matches our window.
	c.currentPC.Store(pc)
	defer c.currentPC.Store(nil)
	c.sendResize()

	sessCtx, sessCancel := context.WithCancel(ctx)
	defer sessCancel()

	exitCh := make(chan int, 1)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.readFrames(pc, exitCh)
		sessCancel()
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.forwardStdin(sessCtx, pc, stdinCh)
	}()

	sshDone := make(chan error, 1)
	go func() { sshDone <- sshCmd.Wait() }()

	var result runResult
	select {
	case code := <-exitCh:
		dlog.E("session: EXIT code=%d", code)
		result = runResult{exit: true, exitCode: code}
	case <-ctx.Done():
		dlog.V("session: context done (abort) — sending SHUTDOWN")
		// Best-effort: tell the daemon to kill the remote shell and
		// clean up its session dir. We launch WriteFrame in its own
		// goroutine with a tight timeout so the abort path always
		// makes progress even if pc.WriteFrame is wedged (e.g. the
		// SSH stdin pipe is full because the daemon's readUpstream is
		// blocked on something). When the timeout fires we fall
		// through to killSSH which closes the pipes, unblocking any
		// stuck WriteFrame.
		shutdownDone := make(chan struct{})
		go func() {
			defer close(shutdownDone)
			_ = pc.WriteFrame(proto.Frame{Type: proto.Shutdown})
		}()
		select {
		case <-shutdownDone:
		case <-time.After(500 * time.Millisecond):
			dlog.E("SHUTDOWN write timed out, force-killing ssh")
		}
		// Give ssh a moment to deliver the frame and exit on its own
		// (which it will once the remote attach disconnects).
		select {
		case <-sshDone:
		case <-time.After(2 * time.Second):
		}
	case err := <-sshDone:
		dlog.E("session: ssh exited err=%v", err)
	}

	sessCancel()
	c.killSSH(sshCmd)
	wg.Wait()
	stderrWG.Wait()

	// readFrames may have observed the EXIT frame after the select fired
	// on sshDone (the bytes were sitting in ssh's stdout pipe buffer).
	// Drain exitCh now so we don't accidentally retry past a successful
	// session end.
	if !result.exit {
		select {
		case code := <-exitCh:
			dlog.E("late EXIT drained after sshDone code=%d", code)
			result = runResult{exit: true, exitCode: code}
		default:
		}
	}
	// Propagate helloAcked so the reconnect loop can tell apart a
	// mid-session disconnect (retry) from a pre-handshake failure
	// (likely surface and bail).
	result.helloAcked = helloAcked
	return result
}

func (c *client) makeSSHCmd(first bool) *exec.Cmd {
	// ssh -T does not allocate a PTY, and consequently does not propagate
	// TERM to the remote session. Without TERM, ncurses apps on the remote
	// (vim, htop, less) fail with "Error opening terminal: unknown." We
	// fix this by prefixing the remote command with an inline env var
	// assignment: the remote shell sees `TERM=foo xssh attach …` and runs
	// xssh with TERM set in its environment.
	termValue := os.Getenv("TERM")
	if termValue == "" {
		termValue = "xterm-256color"
	}

	// Prepend the common user-install locations to PATH so the remote
	// shell finds `xssh` whether it was installed system-wide
	// (`/usr/local/bin/`) or in any of the user-local spots. The static
	// list is `~/bin/`, `~/.local/bin/`, `~/go/bin/`. On top of that,
	// if the remote has Go installed, dynamically append its effective
	// bindir (`go env GOBIN`, or `$(go env GOPATH)/bin` when GOBIN is
	// unset) — matches the Makefile's `do_deploy` probe so a binary
	// pushed by the auto-install feature can be found on the next
	// connect even when the user has a custom GOBIN/GOPATH.
	//
	// $PATH / $HOME / the $(…) command-sub are all expanded by the
	// remote shell at command-line parse time, before it runs xssh.
	// Nothing is shellQuoted here; the whole value stays inside "..."
	// so any spaces from unusual $HOME or go env output don't break
	// the assignment.
	pathToken := `PATH="$PATH:$HOME/bin:$HOME/.local/bin:$HOME/go/bin` +
		`$(command -v go >/dev/null 2>&1 && ` +
		`{ g=$(go env GOBIN); g=${g:-$(go env GOPATH)/bin}; ` +
		`printf ':%s' "$g"; })"`
	tokens := []string{
		pathToken,
		"TERM=" + shellQuote(termValue),
		shellQuote("xssh"),
		shellQuote("attach"),
	}
	// Forward the verbosity level to the remote. The remote dlog is
	// always file-only (no stderr mirror — ssh's stderr surfaces to
	// the user's terminal) regardless of which flag was used locally.
	switch c.logLevel {
	case dlog.LevelTrace:
		tokens = append(tokens, shellQuote("--trace"))
	case dlog.LevelVerbose:
		tokens = append(tokens, shellQuote("--debug"))
	}
	if first {
		tokens = append(tokens, shellQuote("--new"))
	} else {
		tokens = append(tokens, shellQuote("--session"), shellQuote(c.sessionID))
	}

	// -T disables remote PTY allocation by ssh itself; we manage the PTY
	// on the daemon side. Disconnect detection (ServerAliveInterval /
	// ServerAliveCountMax) is left to the user's ~/.ssh/config or
	// explicit -o flags.
	sshArgs := []string{"-T"}
	sshArgs = append(sshArgs, c.sshArgs...)
	sshArgs = append(sshArgs, tokens...)
	return exec.Command("ssh", sshArgs...)
}

// shellQuote returns s safely usable as a single token in a POSIX shell
// command line. Empty or "boring" alphanumeric+punctuation strings pass
// through unchanged; anything else is wrapped in single quotes, with any
// embedded ' escaped as the standard '\'' sequence.
func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	safe := true
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch {
		case b >= 'a' && b <= 'z':
		case b >= 'A' && b <= 'Z':
		case b >= '0' && b <= '9':
		case b == '-' || b == '_' || b == '.' || b == '/' || b == ':' || b == ',' || b == '+' || b == '@' || b == '%':
		default:
			safe = false
		}
		if !safe {
			break
		}
	}
	if safe {
		return s
	}
	var b []byte
	b = append(b, '\'')
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			b = append(b, '\'', '\\', '\'', '\'')
		} else {
			b = append(b, s[i])
		}
	}
	b = append(b, '\'')
	return string(b)
}

func (c *client) killSSH(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

// isTransientConnectError returns true when ssh's stderr looks like
// a TCP-level connect failure or a DNS lookup failure — the cases
// the reconnect-forever loop is meant to handle. OpenSSH emits very
// specific prefixes for these:
//
//	"ssh: connect to host <h> port <p>: <strerror>"
//	"ssh: Could not resolve hostname <h>: <reason>"
//
// Anything else (Permission denied, Host key verification failed,
// "command not found", an empty buffer, etc.) is treated as fatal so
// the loop surfaces it instead of hiding it behind silent retries.
func isTransientConnectError(stderr string) bool {
	s := strings.TrimSpace(stderr)
	if s == "" {
		return false
	}
	return strings.Contains(s, "ssh: connect to ") ||
		strings.Contains(s, "ssh: Could not resolve hostname")
}

// isRemoteXsshMissing returns true when ssh's stderr looks like
// the remote shell couldn't find the `xssh` binary on PATH — the
// deployment-problem case. Conservative match: requires both the
// binary name and a shell-specific "not found" phrase to appear in
// the same captured stderr. Covers the common shells:
//
//	bash:  "bash: xssh: command not found"
//	zsh:   "zsh: command not found: xssh"
//	dash:  "/bin/sh: 1: xssh: not found"
//	fish:  "fish: Unknown command: xssh"
func isRemoteXsshMissing(stderr string) bool {
	if !strings.Contains(stderr, "xssh") {
		return false
	}
	return strings.Contains(stderr, "command not found") ||
		strings.Contains(stderr, ": not found") ||
		strings.Contains(stderr, "Unknown command")
}

func (c *client) readFrames(pc *proto.Conn, exitCh chan<- int) {
	for {
		f, err := pc.ReadFrame()
		if err != nil {
			dlog.V("read frames: %v", err)
			return
		}
		switch f.Type {
		case proto.Output:
			c.handleOutputFrame(f)
		case proto.Pong:
			// Reply to a wake-detect Ping. Signal the prober
			// non-blockingly — if nobody's currently waiting
			// (e.g. the probe already timed out and moved on),
			// drop the Pong rather than blocking readFrames.
			select {
			case c.pongCh <- struct{}{}:
			default:
			}
			dlog.V("PONG received")
		case proto.Exit:
			code := 0
			if len(f.Payload) >= 4 {
				code = int(int32(binary.BigEndian.Uint32(f.Payload[:4])))
			}
			dlog.E("EXIT received code=%d payloadLen=%d", code, len(f.Payload))
			select {
			case exitCh <- code:
			default:
			}
			return
		default:
			dlog.V("ignoring frame %s", f.Type)
		}
	}
}

func (c *client) handleOutputFrame(f proto.Frame) {
	off := f.Offset
	payload := f.Payload
	total := c.outputBuf.Len()

	dlog.T("IN  off=%d len=%d total=%d firstBytes=%q", off, len(payload), total, firstFewBytes(payload, 16))

	if off+uint64(len(payload)) <= total {
		dlog.T("output overlap: off=%d len=%d total=%d (skipped)", off, len(payload), total)
		return
	}
	if off > total {
		dlog.E("output gap: off=%d total=%d (dropping frame) firstBytes=%q", off, total, firstFewBytes(payload, 16))
		return
	}
	skip := total - off
	newBytes := payload[skip:]
	if event := c.altScanner.Scan(newBytes); event != 0 {
		c.inAltScreen.Store(event == 'h')
	}
	_, _ = c.termOut.Write(newBytes)
	_ = c.outputBuf.Append(newBytes)
	if n := len(newBytes); n > 0 {
		c.lastOutByte = newBytes[n-1]
	}
	c.maybeSendAck()
}

// ackInterval and ackIdleMax control when the client tells the daemon
// "you can drop everything up to here":
//   - ackInterval: emit ACK after this many newly-displayed bytes since
//     the last ACK. Throttles the message rate during fast output.
//   - ackIdleMax: emit ACK after this much wall time has elapsed with
//     any unacked bytes pending. Prevents the daemon from holding slow
//     trickle output indefinitely — steady but tiny streams (log tails,
//     keystroke echo) never hit the size threshold but would otherwise
//     accumulate on the daemon side forever.
const (
	ackInterval uint64        = 5 << 20
	ackIdleMax  time.Duration = time.Second
)

func (c *client) maybeSendAck() { c.tryAck(ackInterval) }

// tryAck sends an ACK if there are at least minBytes unacknowledged
// bytes. Used by both the size trigger (handleOutputFrame, minBytes=
// ackInterval) and the time trigger (watchAckIdle, minBytes=1).
func (c *client) tryAck(minBytes uint64) {
	pc := c.currentPC.Load()
	if pc == nil {
		return
	}
	c.ackMu.Lock()
	total := c.outputBuf.Len()
	if total-c.ackedThrough < minBytes {
		c.ackMu.Unlock()
		return
	}
	c.ackedThrough = total
	c.ackMu.Unlock()
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], total)
	if err := pc.WriteFrame(proto.Frame{Type: proto.Ack, Payload: b[:]}); err != nil {
		dlog.V("ack write: %v", err)
		return
	}
	dlog.T("ACK sent at offset %d", total)
}

// wakeSleepThreshold is the minimum wall-clock-vs-monotonic skew that
// counts as a "we were asleep" event. Picked above any plausible
// scheduler jitter or NTP slew so we don't trip on those.
//
// wakePongTimeout is how long the client waits for the Pong reply to
// a wake-triggered Ping before giving up on the link and force-
// reconnecting. Generous enough to cover a slow but alive link;
// short enough that the user notices a stalled session reasonably
// soon after wake.
const (
	wakeSleepThreshold = 20 * time.Second
	wakePongTimeout    = 10 * time.Second
)

// watchForWake watches the wall clock vs the monotonic clock and,
// when a gap shows up (the only way that happens on a normal system
// is suspend-to-RAM), sends a Ping probe over the active session.
// If no Pong comes back within wakePongTimeout we assume the link
// died during sleep and kill the ssh subprocess; the existing
// reconnect loop then takes over.
//
// Cheap to run when nothing's happening: a 1 s ticker comparing two
// timestamps and almost always doing nothing.
func (c *client) watchForWake(ctx context.Context) {
	var lastWall int64
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			wall := time.Now().UnixMilli()
			if lastWall != 0 {
				gap := time.Duration(wall-lastWall) * time.Millisecond
				if gap >= wakeSleepThreshold {
					dlog.E("wake detected (wall gap=%s); probing link", gap)
					c.probeLinkOrKill()
				}
			}
			lastWall = wall
		}
	}
}

// probeLinkOrKill sends a Ping on the current session and waits up
// to wakePongTimeout for a Pong. On success it returns silently. On
// timeout (or if the link's already gone) it kills the ssh
// subprocess so the existing reconnect loop takes over.
func (c *client) probeLinkOrKill() {
	pc := c.currentPC.Load()
	if pc == nil {
		return // no active session — nothing to probe
	}
	// Drain any stale Pong sitting from a previous (timed-out) probe.
	select {
	case <-c.pongCh:
	default:
	}
	if werr := pc.WriteFrame(proto.Frame{Type: proto.Ping}); werr != nil {
		dlog.V("ping write: %v (link likely dead)", werr)
		c.forceReconnect()
		return
	}
	dlog.V("PING sent, waiting %s for PONG", wakePongTimeout)
	select {
	case <-c.pongCh:
		dlog.V("link healthy across the wake")
	case <-time.After(wakePongTimeout):
		dlog.E("no PONG within %s; forcing reconnect", wakePongTimeout)
		c.forceReconnect()
	}
}

// forceReconnect kills the running ssh subprocess so readFrames /
// runOnce return with a read error and the run loop iterates into a
// fresh reconnect attempt.
func (c *client) forceReconnect() {
	cmd := c.currentSSH.Load()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
}

// watchAckIdle wakes once per ackIdleMax and fires an ACK if any bytes
// are pending. This is the fallback for low-rate streams that never
// cross the 5 MiB size threshold.
func (c *client) watchAckIdle(ctx context.Context) {
	t := time.NewTicker(ackIdleMax)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.tryAck(1)
		}
	}
}

func (c *client) forwardStdin(ctx context.Context, pc *proto.Conn, stdinCh <-chan []byte) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-stdinCh:
			if !ok {
				return
			}
			if err := pc.WriteFrame(proto.Frame{Type: proto.Stdin, Payload: data}); err != nil {
				dlog.V("stdin write failed: %v (TTY stdin, dropping)", err)
				return
			}
		}
	}
}

func manifestOf(b *buffer.Buffer) chunk.Manifest {
	return chunk.Manifest{
		Total:      b.Len(),
		FirstIndex: b.FirstChunkIndex(),
		Hashes:     b.ChunkHashes(),
	}
}

func sendTerminalReset(w io.Writer, inAltScreen bool) {
	// Choose the alt-screen exit sequence based on whether we're currently
	// in alt-screen mode (tracked by altScanner from the byte stream):
	//   \e[?1049l   exit alt buffer + restore cursor from the matching
	//               \e[?1049h save. Correct when a program is still in
	//               alt-screen — the save is fresh.
	//   \e[?1047l   exit alt buffer, no cursor restore. Correct when no
	//               program is currently in alt-screen — avoids restoring
	//               from a stale save left by an earlier-exited program.
	//
	// Followed by a defensive reset of modes a misbehaving program might
	// have left on:
	//   \e[?25h     show cursor.
	//   \e[?1000l..\e[?1006l  disable mouse tracking.
	//   \e[?2004l   disable bracketed-paste mode.
	//   \e[?1004l   disable focus tracking.
	//   \e[?1l      DECCKM off — normal cursor keys.
	//   \e[?7h      auto-wrap on.
	//   \e[m        reset SGR.
	//
	// No trailing newline: most remote programs already emit one on
	// graceful exit. The abort path prepends its own newline before
	// "Connection aborted." if the cursor was mid-line.
	exitAlt := "\x1b[?1047l"
	if inAltScreen {
		exitAlt = "\x1b[?1049l"
	}
	fmt.Fprint(w,
		exitAlt+
			"\x1b[?25h"+
			"\x1b[?1000l\x1b[?1002l\x1b[?1003l\x1b[?1006l"+
			"\x1b[?2004l"+
			"\x1b[?1004l"+
			"\x1b[?1l"+
			"\x1b[?7h"+
			"\x1b[m")
}

// crlfTranslator wraps an io.Writer and translates each bare '\n' (one not
// already preceded by '\r') into '\r\n'. In PTY mode the remote shell sees a
// TTY and emits proper '\r\n', so this is normally a no-op. It still guards
// against malformed lines or programs that bypass termios processing.
type crlfTranslator struct {
	mu       sync.Mutex
	w        io.Writer
	lastByte byte
}

func (c *crlfTranslator) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, 0, len(p)+8)
	last := c.lastByte
	for _, b := range p {
		if b == '\n' && last != '\r' {
			out = append(out, '\r', '\n')
		} else {
			out = append(out, b)
		}
		last = b
	}
	c.lastByte = last
	if _, err := c.w.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}

// remoteEnv is the structured result of the one-shot ssh probe we
// run when offering to auto-install the binary. Fields:
//
//	uid       remote effective UID (0 → root → install to /usr/local/bin/)
//	goos      mapped from `uname -s` (linux, darwin, …)
//	goarch    mapped from `uname -m` (amd64, arm64, arm, 386, …)
//	homeDir   remote `$HOME`, for display only
//	userBin   the directory `go install` would drop binaries into on
//	          the remote, mirroring the Makefile's `do_deploy` probe:
//	          GOBIN if set, else GOPATH/bin, else $HOME/go/bin.
type remoteEnv struct {
	uid     int
	goos    string
	goarch  string
	homeDir string
	userBin string
}

// queryRemoteEnv runs a single ssh command on the remote that emits
// five newline-separated fields. Reuses the user's ssh args so any
// `-i key`, `-p port`, `-o ...` flags carry over.
func (c *client) queryRemoteEnv() (*remoteEnv, error) {
	const probe = `id -u
uname -s
uname -m
printf '%s\n' "$HOME"
if command -v go >/dev/null 2>&1; then
  gobin=$(go env GOBIN)
  if [ -n "$gobin" ]; then printf '%s\n' "$gobin"
  else printf '%s\n' "$(go env GOPATH)/bin"; fi
else
  printf '%s\n' "$HOME/go/bin"
fi
`
	args := []string{"-T"}
	args = append(args, c.sshArgs...)
	args = append(args, probe)
	cmd := exec.Command("ssh", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ssh probe: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) < 5 {
		return nil, fmt.Errorf("ssh probe: expected 5 lines, got %d", len(lines))
	}
	uid, err := strconv.Atoi(strings.TrimSpace(lines[0]))
	if err != nil {
		return nil, fmt.Errorf("ssh probe: bad uid %q: %w", lines[0], err)
	}
	return &remoteEnv{
		uid:     uid,
		goos:    unameToGOOS(strings.TrimSpace(lines[1])),
		goarch:  unameToGOARCH(strings.TrimSpace(lines[2])),
		homeDir: strings.TrimSpace(lines[3]),
		userBin: strings.TrimSpace(lines[4]),
	}, nil
}

// unameToGOOS / unameToGOARCH translate `uname` output into Go's GOOS
// and GOARCH naming. Unrecognized values pass through unchanged so the
// arch-mismatch check below can still report what we saw.
func unameToGOOS(s string) string {
	switch s {
	case "Linux":
		return "linux"
	case "Darwin":
		return "darwin"
	case "FreeBSD":
		return "freebsd"
	case "OpenBSD":
		return "openbsd"
	case "NetBSD":
		return "netbsd"
	}
	return s
}

func unameToGOARCH(s string) string {
	switch s {
	case "x86_64", "amd64":
		return "amd64"
	case "aarch64", "arm64":
		return "arm64"
	case "armv7l", "armv6l", "armhf", "arm":
		return "arm"
	case "i386", "i486", "i586", "i686":
		return "386"
	}
	return s
}

// offerAutoInstall is the cooked-mode entry point — used from the
// first-connect bail path, where the terminal hasn't been switched
// into raw mode yet so reading user input via bufio works fine.
// Returns true iff the binary was successfully pushed and the caller
// should retry the connection.
func (c *client) offerAutoInstall(sshStderr string) bool {
	return c.runAutoInstall(sshStderr)
}

// offerAutoInstallRaw is the raw-mode entry point — used from the
// reconnect bail path, where the terminal is in raw mode. We
// temporarily restore cooked mode for the prompt, then re-enter raw
// mode if the install succeeded and we're about to retry. (If the
// install was declined or failed, the caller will print the error
// and exit — restoring is moot but harmless.)
func (c *client) offerAutoInstallRaw(sshStderr string) bool {
	if c.oldState == nil {
		// Should not happen — reconnect path implies activate() ran.
		return c.runAutoInstall(sshStderr)
	}
	cooked, err := term.GetState(c.stdinFd)
	if err != nil {
		return false
	}
	_ = term.Restore(c.stdinFd, c.oldState)
	defer func() {
		if cooked != nil {
			_ = term.Restore(c.stdinFd, cooked)
		}
	}()
	return c.runAutoInstall(sshStderr)
}

// runAutoInstall implements the shared flow: print headline + ssh
// stderr, probe the remote, validate arch, prompt for path, push
// the local binary. Caller is responsible for terminal mode.
func (c *client) runAutoInstall(sshStderr string) bool {
	self, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "auto-install: locate local binary: %v\n", err)
		return false
	}
	fi, err := os.Stat(self)
	if err != nil {
		fmt.Fprintf(os.Stderr, "auto-install: stat local binary: %v\n", err)
		return false
	}

	fmt.Fprintln(os.Stderr, "continuous-ssh: remote `xssh` not found on PATH")
	if msg := strings.TrimSpace(sshStderr); msg != "" {
		fmt.Fprintln(os.Stderr, msg)
	}
	fmt.Fprintln(os.Stderr)

	env, err := c.queryRemoteEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "auto-install: ssh probe failed: %v\n", err)
		return false
	}

	if env.goos != runtime.GOOS || env.goarch != runtime.GOARCH {
		fmt.Fprintf(os.Stderr,
			"auto-install: arch mismatch — local binary is %s/%s, remote is %s/%s.\nRebuild for the remote architecture (see `make pi64`/`pi32`/`pi-zero`) and run `make deploy HOST=...`.\n",
			runtime.GOOS, runtime.GOARCH, env.goos, env.goarch)
		return false
	}

	var defaultPath string
	if env.uid == 0 {
		defaultPath = "/usr/local/bin/xssh"
	} else {
		defaultPath = strings.TrimRight(env.userBin, "/") + "/xssh"
	}

	target := extractSSHTarget(c.sshArgs)
	fmt.Fprintf(os.Stderr,
		"Push local %s (%s, %s/%s) to %s:%s?\n",
		self, humanBytesShort(fi.Size()), runtime.GOOS, runtime.GOARCH,
		target, defaultPath)
	fmt.Fprintln(os.Stderr, "  [Y]es        install at the default location above")
	fmt.Fprintln(os.Stderr, "  [n]o         cancel")
	fmt.Fprintln(os.Stderr, "  any path     install at that path instead (e.g. /opt/bin/xssh)")
	fmt.Fprint(os.Stderr, ": ")

	reader := bufio.NewReader(os.Stdin)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	line = strings.TrimSpace(line)

	var destPath string
	switch strings.ToLower(line) {
	case "", "y", "yes":
		destPath = defaultPath
	case "n", "no":
		fmt.Fprintln(os.Stderr, "Cancelled.")
		return false
	default:
		if !isSafeRemotePath(line) {
			fmt.Fprintln(os.Stderr, "Path contains unsafe characters; cancelled.")
			return false
		}
		destPath = line
	}

	fmt.Fprintf(os.Stderr, "Pushing to %s:%s … ", target, destPath)
	if err := c.pushBinary(self, destPath); err != nil {
		fmt.Fprintf(os.Stderr, "failed: %v\n", err)
		return false
	}
	fmt.Fprintln(os.Stderr, "done. Retrying.")
	return true
}

// isSafeRemotePath conservatively accepts paths made of alphanumerics
// and `/ . _ - ~ $`. The `~` is left intact so the remote shell
// expands it; the `$` is left intact so `$HOME/...` expressions work.
// Anything else (shell metacharacters, quotes, backticks) is rejected
// so a user-supplied custom path can't smuggle a command into the
// remote shell invocation.
func isSafeRemotePath(p string) bool {
	if p == "" {
		return false
	}
	for _, r := range p {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9':
		case r == '/', r == '.', r == '_', r == '-', r == '~', r == '$':
		default:
			return false
		}
	}
	return true
}

// pushBinary scp-equivalents the local binary onto the remote via
// ssh stdin: `cat <local | ssh <args> 'mkdir -p $(dirname …) && cat > …tmp && chmod +x …tmp && mv …tmp …'`.
// The .tmp + atomic mv keeps any existing binary intact on partial
// transfer.
func (c *client) pushBinary(localBinary, destPath string) error {
	f, err := os.Open(localBinary)
	if err != nil {
		return err
	}
	defer f.Close()
	// destPath is either one of our defaults (safe by construction)
	// or a user input that passed isSafeRemotePath, so unquoted
	// substitution into the shell command is safe and lets the
	// remote shell expand ~ and $HOME.
	remoteScript := fmt.Sprintf(
		"mkdir -p $(dirname %s) && cat > %s.tmp && chmod +x %s.tmp && mv %s.tmp %s",
		destPath, destPath, destPath, destPath, destPath,
	)
	args := []string{"-T"}
	args = append(args, c.sshArgs...)
	args = append(args, remoteScript)
	cmd := exec.Command("ssh", args...)
	cmd.Stdin = f
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// extractSSHTarget pulls the user@host (or host, or alias) out of
// the ssh args list, for display in the install prompt. Picks the
// last non-flag, non-flag-value token — same heuristic as the
// per-invocation log filename builder.
func extractSSHTarget(sshArgs []string) string {
	for i := len(sshArgs) - 1; i >= 0; i-- {
		t := sshArgs[i]
		if strings.HasPrefix(t, "-") {
			break
		}
		return t
	}
	return "remote"
}

// humanBytesShort formats a byte count concisely (e.g. "8.7 MB").
// Inline here to avoid pulling in the sessions package just for this.
func humanBytesShort(n int64) string {
	const k = 1024
	switch {
	case n < k:
		return fmt.Sprintf("%d B", n)
	case n < k*k:
		return fmt.Sprintf("%.1f KB", float64(n)/k)
	case n < k*k*k:
		return fmt.Sprintf("%.1f MB", float64(n)/(k*k))
	default:
		return fmt.Sprintf("%.1f GB", float64(n)/(k*k*k))
	}
}
