// Package client is the local user-facing process. It manages the local TTY,
// spawns the ssh subprocess that brings up the remote attach, runs the
// silent infinite reconnect loop, and handles the `~.`/`~~` escape sequences.
package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/term"

	"github.com/zziigguurraatt/continuous-ssh/internal/buffer"
	"github.com/zziigguurraatt/continuous-ssh/internal/chunk"
	"github.com/zziigguurraatt/continuous-ssh/internal/dlog"
	"github.com/zziigguurraatt/continuous-ssh/internal/proto"
)

// Run is the client entry point. argv excludes the program name. Anything
// other than our own `--debug` flag is forwarded to `ssh` as arguments. The
// remote always runs the user's login shell.
func Run(argv []string) int {
	sshArgs, debug, err := parseArgs(argv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuous-ssh: %v\n", err)
		return 2
	}

	stdinFd := int(os.Stdin.Fd())
	if !term.IsTerminal(stdinFd) {
		fmt.Fprintln(os.Stderr, "continuous-ssh: stdin must be a terminal (piped stdin is not supported)")
		return 1
	}

	logPath, err := clientLogPath()
	if err == nil {
		_ = os.MkdirAll(filepath.Dir(logPath), 0o700)
	}
	var stderrSink io.Writer
	if debug {
		stderrSink = dlog.CRLFWriter{W: os.Stderr}
	}
	_ = dlog.Setup(logPath, debug, stderrSink)
	dlog.E("client starting debug=%v sshArgs=%v", debug, sshArgs)

	// No on-disk buffer on the client — sliding 10 MiB RAM window only.
	// We ACK every 5 MiB so the daemon trims its own buffer in lockstep;
	// past bytes that fall off the window are still tracked by the byte
	// counter (Buffer.Len) so dedup on reconnect works by offset alone.
	const clientBufRAM uint64 = 10 << 20
	outputBuf, err := buffer.New("", clientBufRAM, clientBufRAM, clientBufRAM)
	if err != nil {
		fmt.Fprintf(os.Stderr, "continuous-ssh: %v\n", err)
		return 1
	}

	c := &client{
		sshArgs:   sshArgs,
		debug:     debug,
		outputBuf: outputBuf,
		stdinFd:   stdinFd,
		termOut:   &crlfTranslator{w: os.Stdout},
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
		case 130:
			msg = "continuous-ssh: session was not cleanly shut down; recovery aborted."
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

// parseArgs splits the client argv into (ssh-args, debug).
//
// Grammar:
//
//	xssh [--debug] [ssh-args...]
//
// Every argv element except our own `--debug` is forwarded to `ssh`
// verbatim. The remote always runs the user's login shell.
func parseArgs(argv []string) (sshArgs []string, debug bool, err error) {
	for _, t := range argv {
		if t == "--debug" {
			debug = true
			continue
		}
		sshArgs = append(sshArgs, t)
	}
	if len(sshArgs) == 0 {
		return nil, false, errors.New("ssh target required")
	}
	return sshArgs, debug, nil
}

func clientLogPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".continuous-ssh", "client.log"), nil
}

type client struct {
	sshArgs   []string
	debug     bool
	sessionID string
	outputBuf *buffer.Buffer

	stdinFd  int
	oldState *term.State // set after the first activate(); nil while still cooked
	termOut  io.Writer

	// currentPC points at the active session's proto.Conn so that the SIGWINCH
	// handler can fire RESIZE frames at it without coordinating through a
	// channel. Nil when no session is active.
	currentPC atomic.Pointer[proto.Conn]

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

	// altScanner watches the remote's output for alt-screen-buffer
	// enter/exit escape sequences so we know whether we're currently in
	// alt-screen mode at exit time. On abort while in alt-screen we emit
	// \e[?1049l (which restores the cursor from the running program's
	// fresh save); otherwise we emit \e[?1047l (no cursor restore, avoids
	// any stale save from a long-exited earlier program).
	altScanner  altScanner
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
		return nil
	}

	consecutiveFails := 0
	for {
		if ctx.Err() != nil {
			c.aborted = true
			return 130
		}
		first := c.sessionID == ""
		result := c.runOnce(ctx, first, stdinCh, activate)
		if result.exit {
			return result.exitCode
		}
		if ctx.Err() != nil {
			c.aborted = true
			return 130
		}
		// Safety net: if the session id is set (we've connected before)
		// and a sequence of reconnect attempts is failing without ever
		// producing an EXIT, assume the session is truly gone — the
		// replay daemon may have cleaned up the session dir already —
		// and bail with the standard "remote daemon stopped" notice.
		if c.sessionID != "" {
			consecutiveFails++
			if consecutiveFails >= 5 {
				dlog.E("giving up after %d failed reconnects to session %s", consecutiveFails, c.sessionID)
				return 129
			}
		}
		dlog.V("reconnect: backing off (consecutiveFails=%d)", consecutiveFails)
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

	var stderrWG sync.WaitGroup
	if c.debug {
		stderrPipe, perr := sshCmd.StderrPipe()
		if perr != nil {
			dlog.E("ssh stderr pipe: %v", perr)
			return runResult{}
		}
		stderrWG.Add(1)
		go func() {
			defer stderrWG.Done()
			scan := bufio.NewScanner(stderrPipe)
			for scan.Scan() {
				dlog.V("ssh stderr: %s", scan.Text())
			}
		}()
	} else {
		sshCmd.Stderr = io.Discard
	}

	if err := sshCmd.Start(); err != nil {
		dlog.E("ssh start: %v", err)
		return runResult{}
	}

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
		return runResult{}
	}
	dlog.V("sending HELLO mode=%d outputTotal=%d", mode, c.outputBuf.Len())
	if err := pc.WriteFrame(proto.Frame{Type: proto.Hello, Payload: helloPayload}); err != nil {
		dlog.E("hello write: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{}
	}

	ack, err := pc.ReadFrame()
	if err != nil {
		dlog.E("hello_ack read: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{}
	}
	if ack.Type != proto.HelloAck {
		dlog.E("expected HELLO_ACK got %s", ack.Type)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{}
	}
	ackHello, err := chunk.DecodeHello(ack.Payload)
	if err != nil {
		dlog.E("hello_ack decode: %v", err)
		c.killSSH(sshCmd)
		stderrWG.Wait()
		return runResult{}
	}
	if c.sessionID == "" {
		c.sessionID = ackHello.SessionID
		dlog.V("session id assigned: %s", c.sessionID)
	}
	dlog.V("HELLO_ACK daemonTotal=%d", ackHello.Output.Total)

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
		return runResult{}
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
		// Best-effort: tell the daemon to kill the remote shell and clean
		// up its session dir. The frame may fail to flush if the link is
		// already dead; that's fine, the daemon's keep-alive timer will
		// pick up the slack.
		_ = pc.WriteFrame(proto.Frame{Type: proto.Shutdown})
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

	// Prepend the common user-install locations to PATH so the remote shell
	// finds `xssh` whether it was installed system-wide
	// (`/usr/local/bin/`) or in any of the user-local spots: `~/bin/`,
	// `~/.local/bin/`, or `~/go/bin/` (where `go install` drops things).
	// $HOME and $PATH are intentionally left unquoted here so the remote
	// shell expands them; the value of TERM is shellQuote'd, the literal
	// path additions are not.
	tokens := []string{
		`PATH=$PATH:$HOME/bin:$HOME/.local/bin:$HOME/go/bin`,
		"TERM=" + shellQuote(termValue),
		shellQuote("xssh"),
		shellQuote("attach"),
	}
	if c.debug {
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

	if off+uint64(len(payload)) <= total {
		dlog.V("output overlap: off=%d len=%d total=%d (skipped)", off, len(payload), total)
		return
	}
	if off > total {
		dlog.E("output gap: off=%d total=%d (dropping frame)", off, total)
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
	dlog.V("ACK sent at offset %d", total)
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
		Total:  b.Len(),
		Hashes: b.ChunkHashes(),
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

// altScanner tracks alt-screen buffer enter/exit sequences in an
// arbitrary byte stream. State persists across Scan calls so that a
// sequence split across two writes is still recognised correctly.
//
// Recognised sequences (all DEC private modes — note the leading "?"):
//
//	\e[?47h   /  \e[?47l
//	\e[?1047h /  \e[?1047l
//	\e[?1049h /  \e[?1049l   (the modern, ncurses-default form)
//
// Multi-parameter forms like "\e[?25;1049h" are also handled.
type altScanner struct {
	state int    // 0 idle, 1 saw ESC, 2 saw [, 3 saw ?, accumulating digits
	parm  []byte // accumulated digits + ';' separators in state 3
}

// Scan feeds bytes through the state machine. The returned event byte is
// 'h' (entered alt-screen) or 'l' (exited alt-screen) for the *last* such
// transition observed in p, or 0 if none were observed.
func (a *altScanner) Scan(p []byte) byte {
	var ev byte
	for _, b := range p {
		switch a.state {
		case 0:
			if b == 0x1B {
				a.state = 1
			}
		case 1:
			if b == '[' {
				a.state = 2
			} else {
				a.state = 0
			}
		case 2:
			if b == '?' {
				a.state = 3
				a.parm = a.parm[:0]
			} else {
				a.state = 0
			}
		case 3:
			switch {
			case (b >= '0' && b <= '9') || b == ';':
				if len(a.parm) < 32 {
					a.parm = append(a.parm, b)
				}
			case b == 'h' || b == 'l':
				for _, p := range strings.Split(string(a.parm), ";") {
					if p == "47" || p == "1047" || p == "1049" {
						ev = b
						break
					}
				}
				a.state = 0
			default:
				a.state = 0
			}
		}
	}
	return ev
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
