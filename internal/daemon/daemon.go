// Package daemon is the long-lived remote process that owns the spawned
// command and the per-stream history buffer. The command runs attached to a
// PTY so that interactive programs (shells, vim, htop, …) behave correctly.
// stdout and stderr are merged into a single output stream by the PTY itself.
//
// Attach connections come and go over a Unix socket; at most one is served at
// a time (a new attach kicks the previous one).
package daemon

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/creack/pty"

	"github.com/zziigguurraatt/continuous-ssh/internal/altscreen"
	"github.com/zziigguurraatt/continuous-ssh/internal/buffer"
	"github.com/zziigguurraatt/continuous-ssh/internal/chunk"
	"github.com/zziigguurraatt/continuous-ssh/internal/dlog"
	"github.com/zziigguurraatt/continuous-ssh/internal/proto"
	"github.com/zziigguurraatt/continuous-ssh/internal/sessions"
)

// Run is the daemon subcommand entry point.
// Usage: xssh daemon [--debug] --session <id> [--replay]
// The user's $SHELL (or /etc/passwd, or /bin/sh) is launched as a login
// shell. With --replay, no shell is started; instead the existing
// output.log is served to one attaching client and the session dir is
// removed when the client disconnects.
func Run(argv []string) int {
	sessionID, replay, logLevel, err := parseArgs(argv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
		return 2
	}
	if replay {
		return ReplayRun(sessionID, logLevel)
	}

	sessionDir, err := SessionDir(sessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(sessionDir, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
		return 1
	}

	if err := dlog.Setup(filepath.Join(sessionDir, "daemon.log"), logLevel, nil); err != nil {
		fmt.Fprintf(os.Stderr, "daemon: %v\n", err)
		return 1
	}
	dlog.E("daemon starting session=%s pid=%d level=%d", sessionID, os.Getpid(), logLevel)

	pidPath := filepath.Join(sessionDir, "pid")
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		dlog.E("write pid: %v", err)
		return 1
	}
	infoPath := filepath.Join(sessionDir, "info")
	_ = os.WriteFile(infoPath, []byte(fmt.Sprintf("started=%s\n", time.Now().Format(time.RFC3339))), 0o600)

	outputBuf, err := buffer.New(filepath.Join(sessionDir, "output.log"), 0, 0, 0)
	if err != nil {
		dlog.E("output buffer: %v", err)
		return 1
	}

	// Disk-usage banner: a single line written into the output stream
	// before the shell starts. Sits at offset 0, becomes part of the
	// session's scrollback like any other byte. Reconnects don't see
	// it again — the client already has it from the first attach.
	if banner := loginBanner(sessionID); banner != "" {
		_ = outputBuf.Append([]byte(banner))
	}

	sockPath := filepath.Join(sessionDir, "sock")
	_ = os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		dlog.E("listen: %v", err)
		return 1
	}

	cmd, ptmx, err := buildAndStart(sessionID)
	if err != nil {
		dlog.E("start cmd: %v", err)
		return 1
	}
	dlog.V("cmd started pid=%d argv=%v", cmd.Process.Pid, cmd.Args)

	d := &daemon{
		sessionID:  sessionID,
		sessionDir: sessionDir,
		outputBuf:  outputBuf,
		ptmx:       ptmx,
	}
	d.cond = sync.NewCond(&d.mu)

	d.mu.Lock()
	d.touchKeepAlive()
	d.mu.Unlock()

	// Catch SIGTERM/SIGINT/SIGHUP and convert to a clean shutdown.
	//   - signalShutdown=true so the accept-stopper closes the listener
	//     (no new attaches) but the active attach can still drain.
	//   - preserveOnExit=true so the on-disk buffer is kept if there's no
	//     attach to deliver to.
	//   - Kill the remote shell, which closes the PTY, which causes
	//     pumpPTY to seal the buffer, which makes d.exited=true via
	//     waitCmd → serveAttach's existing exited-path runs the drain +
	//     EXIT. If that drain succeeds the cleanup notices d.exitDelivered
	//     and skips the on-disk preservation.
	sigCh := make(chan os.Signal, 4)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP)
	go func() {
		first := true
		for s := range sigCh {
			if !first {
				dlog.E("ignoring signal %v during shutdown", s)
				continue
			}
			first = false
			dlog.E("SIGHANDLER: received %v, setting signalShutdown=true", s)
			d.mu.Lock()
			d.signalShutdown = true
			d.preserveOnExit = true
			d.cond.Broadcast()
			d.mu.Unlock()
			if cmd.Process != nil {
				pgid := cmd.Process.Pid
				err := syscall.Kill(-pgid, syscall.SIGHUP)
				dlog.E("SIGHANDLER: kill -HUP pgid=%d err=%v", pgid, err)
			} else {
				dlog.E("SIGHANDLER: cmd.Process is nil")
			}
		}
	}()

	// One pump drains the PTY master into the buffer. Compared to the old
	// two-pump (stdout+stderr) design this is simpler: the kernel-side PTY
	// has already merged the streams for us.
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		d.pumpPTY()
	}()

	// Owner-goroutine for ptmx.Write so that readUpstream never blocks on
	// PTY-input backpressure. If the foreground remote program isn't
	// reading stdin (e.g. `seq 1 N` while running), the PTY input buffer
	// fills up and ptmx.Write blocks. With the dispatch decoupled,
	// readUpstream stays responsive — it can still process Shutdown / Ack
	// / Resize frames immediately so `~.` works even when input is
	// jammed. When the channel is full, we drop the Stdin frame on the
	// floor: input during a wedge is best-effort by design (the design
	// promise is "no stdin replay across reconnects" already).
	d.stdinCh = make(chan []byte, stdinChCapacity)
	stdinWriterDone := make(chan struct{})
	go func() {
		defer close(stdinWriterDone)
		for data := range d.stdinCh {
			if _, werr := d.ptmx.Write(data); werr != nil {
				dlog.E("pty stdin write: %v", werr)
				return
			}
		}
	}()

	go d.waitCmd(cmd, pumpDone)

	// Buffer-state heartbeat — only when verbose. Logs a snapshot every
	// statsHeartbeat seconds so post-mortem analysis of a hang/freeze
	// can see how held bytes, RAM tail, and disk file size evolved.
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		d.statsHeartbeat(pumpDone)
	}()

	// Disk-cap sweeper. Ticks every diskCapSweepInterval and shuts
	// the daemon down (preserving segments for replay) when the
	// host-wide usage trips the DiskBudget rule.
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		d.diskCapSweep(pumpDone)
	}()

	d.acceptLoop(listener)

	d.killCmd(cmd)
	_ = ptmx.Close()
	<-pumpDone
	d.attachesWG.Wait()
	close(d.stdinCh) // shut down the ptmx writer goroutine
	<-stdinWriterDone

	d.mu.Lock()
	// Preserve the session for a later replay daemon when the client
	// didn't already receive EXIT live, the user didn't abort with
	// `~.`, and either the signal/disk-cap path set preserveOnExit
	// or the shell exited on its own.
	preserve := !d.exitDelivered && (d.preserveOnExit || (d.exited && !d.shutdown))
	d.mu.Unlock()

	// Close(false) flushes the in-RAM tail to disk; Close(true) just
	// unlinks the file. Track the close error so we know whether the
	// preserved file actually contains the full buffer.
	closeErr := outputBuf.Close(!preserve)
	_ = listener.Close()

	dlog.E("daemon exit code=%d preserve=%v diskcap=%v closeErr=%v",
		d.exitCode, preserve, d.diskcap.Load(), closeErr)

	if preserve {
		_ = os.Remove(sockPath)
		_ = os.Remove(pidPath)
		_ = os.Remove(infoPath)
		// Write the clean marker ONLY if the RAM tail flushed without
		// error. A missing marker tells the replay daemon to refuse —
		// because the on-disk file may be missing the tail.
		if closeErr == nil {
			markerPath := filepath.Join(sessionDir, cleanMarkerName)
			if werr := os.WriteFile(markerPath, nil, 0o600); werr != nil {
				dlog.E("write clean marker: %v", werr)
			}
			// Diskcap marker — host-wide disk policy fired. The replay
			// daemon promotes EXIT(129) to EXIT(134) when it sees this.
			if d.diskcap.Load() {
				diskcapPath := filepath.Join(sessionDir, diskcapMarkerName)
				if werr := os.WriteFile(diskcapPath, nil, 0o600); werr != nil {
					dlog.E("write diskcap marker: %v", werr)
				}
			}
		} else {
			dlog.E("not writing clean marker: buffer flush failed")
		}
	} else {
		_ = os.RemoveAll(sessionDir)
	}
	return d.exitCode
}

// parseArgs returns (sessionID, replay, logLevel, err). logLevel maps
// to dlog levels: 0=errors only, 1=verbose, 2=trace.
func parseArgs(argv []string) (string, bool, int, error) {
	var sessionID string
	var replay bool
	logLevel := dlog.LevelError
	i := 0
	for i < len(argv) {
		switch argv[i] {
		case "--session":
			if i+1 >= len(argv) {
				return "", false, 0, errors.New("--session requires an argument")
			}
			sessionID = argv[i+1]
			i += 2
		case "--debug":
			if logLevel < dlog.LevelVerbose {
				logLevel = dlog.LevelVerbose
			}
			i++
		case "--trace":
			logLevel = dlog.LevelTrace
			i++
		case "--replay":
			replay = true
			i++
		default:
			return "", false, 0, fmt.Errorf("unknown flag %q", argv[i])
		}
	}
	if sessionID == "" {
		return "", false, 0, errors.New("--session is required")
	}
	return sessionID, replay, logLevel, nil
}

// buildAndStart launches the user's login shell inside a fresh PTY. The
// returned *exec.Cmd has its Process populated; the *os.File is the PTY
// master. The shell's environment gets XSSH_SESSION=<id> so processes
// inside the session (notably `xssh ls`) can detect which xssh session
// they're running under.
func buildAndStart(sessionID string) (*exec.Cmd, *os.File, error) {
	shell := defaultShell()
	dlog.V("default shell resolved to %q", shell)
	// Login-shell convention: argv[0] starts with '-'.
	cmd := &exec.Cmd{
		Path: shell,
		Args: []string{"-" + filepath.Base(shell)},
		Env:  append(os.Environ(), "XSSH_SESSION="+sessionID),
	}
	// Start with a reasonable default size; the client's first RESIZE frame
	// (sent right after HELLO_ACK) will replace it with the actual local
	// terminal dimensions. Starting at 0x0 makes some shells skip prompting.
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: 80, Rows: 24})
	if err != nil {
		return nil, nil, fmt.Errorf("pty.Start: %w", err)
	}
	return cmd, ptmx, nil
}

// defaultShell returns the user's preferred shell. SHELL is checked first
// (covers explicit user overrides), then /etc/passwd (authoritative for the
// user's login shell, populated by useradd/chsh and used by login(1) and
// sshd itself), with /bin/sh as a final fallback.
func defaultShell() string {
	if s := os.Getenv("SHELL"); s != "" {
		return s
	}
	u, err := user.Current()
	if err == nil {
		f, err := os.Open("/etc/passwd")
		if err == nil {
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				fields := strings.Split(scanner.Text(), ":")
				if len(fields) >= 7 && fields[0] == u.Username && fields[6] != "" {
					return fields[6]
				}
			}
		}
	}
	return "/bin/sh"
}

// SessionDir returns the on-disk directory for a session id.
func SessionDir(id string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locate home: %w", err)
	}
	return filepath.Join(home, ".continuous-ssh", "sessions", id), nil
}

type daemon struct {
	sessionID  string
	sessionDir string

	outputBuf *buffer.Buffer
	ptmx      *os.File

	// altScanner watches PTY output for alt-screen-buffer enter/exit
	// escape sequences and keeps `inAltScreen` current. Reported on
	// HELLO_ACK so a reattaching client can resync its local
	// terminal before output streams in. Read by serveAttach (one
	// goroutine), written by pumpPTY (a different goroutine) —
	// hence atomic.Bool for the published flag.
	altScanner  altscreen.Scanner
	inAltScreen atomic.Bool

	// stdinCh hands Stdin payloads from readUpstream to the
	// per-daemon ptmx-writer goroutine. Buffered so readUpstream can
	// drop frames non-blockingly when the PTY input buffer is full
	// (which happens when the remote foreground program isn't reading
	// stdin) — without it, readUpstream would block on ptmx.Write and
	// stop processing Shutdown / Ack / Resize frames.
	stdinCh chan []byte

	mu   sync.Mutex
	cond *sync.Cond

	activeConn net.Conn
	epoch      uint64

	exited   bool
	exitCode int
	diskcap  atomic.Bool

	// shutdown means "tear down immediately, drop the active attach
	// abruptly". Set by the SHUTDOWN frame (client abort) and by the
	// disk-cap sweeper firing.
	shutdown bool

	// signalShutdown means "tear down gracefully: drain the active attach
	// and send EXIT, *then* clean up". Set by the SIGTERM/SIGINT/SIGHUP
	// handler. The signal handler also kills the remote shell to drive
	// pumpPTY → buffer.Seal → d.exited=true, which lets serveAttach's
	// existing exited-path do the drain+EXIT.
	signalShutdown bool

	// preserveOnExit means "the user might want to recover this session;
	// don't delete output.log on the way out". Set on disk-cap shutdown
	// or on caught SIGTERM/SIGINT/SIGHUP. The cleanup path additionally writes
	// a `clean` marker file iff the buffer flushed without error — the
	// replay daemon refuses to serve a session that lacks the marker.
	// Skipped (no preservation) when exitDelivered is true, because the
	// client already got everything via the live connection.
	preserveOnExit bool

	keepAliveUntil time.Time
	exitDelivered  bool

	attachesWG sync.WaitGroup
}

// cleanMarkerName is the file written under the session directory when the
// daemon shuts down cleanly with its RAM tail successfully flushed to
// disk. The replay daemon refuses to serve a session that lacks it.
const cleanMarkerName = "clean"

// diskcapMarkerName is written next to the clean marker when the
// daemon shut down because the host-wide disk cap was exceeded
// (sum of all sessions' segment bytes > DiskBudget and this daemon
// was above its fair share). The replay daemon promotes EXIT(129)
// → EXIT(134) when it sees this.
const diskcapMarkerName = "diskcap"

// diskCapSweepInterval controls how often the daemon checks the
// global on-disk usage against DiskBudget. The exact value isn't
// load-bearing — fast growth that briefly overshoots the cap is
// fine, the cap is a soft ceiling on accumulated bytes, not a
// per-byte gate.
const diskCapSweepInterval = 60 * time.Second

// lastAttachName is the file whose mtime tracks the most recent
// attach or detach event. `xssh ls` displays "X ago" relative to it
// so users can see how long a stale session has been without a
// client (or, for an active session, how long the current client
// has been connected).
const lastAttachName = "lastattach"

const keepAliveGrace = 30 * time.Second

// stdinChCapacity is how many Stdin frames the ptmx writer can have
// queued before readUpstream starts dropping new ones. Large enough
// that brief PTY-input back-pressure doesn't drop bursts of typing,
// small enough that we don't accumulate megabytes of stale keystrokes
// across a long wedge.
const stdinChCapacity = 64

// touchKeepAlive bumps the keep-alive deadline forward and schedules a
// broadcast so the accept-stopper wakes when the new deadline elapses.
// Caller must hold d.mu.
func (d *daemon) touchKeepAlive() {
	d.keepAliveUntil = time.Now().Add(keepAliveGrace)
	time.AfterFunc(keepAliveGrace, func() {
		d.mu.Lock()
		d.cond.Broadcast()
		d.mu.Unlock()
	})
}

// touchLastAttach updates the mtime of the lastattach marker file
// to "now". Called on each attach/detach event so `xssh ls` can show
// how long ago a stale session lost its client. Caller doesn't need
// any particular lock — the file's mtime is the only thing observed.
func (d *daemon) touchLastAttach() {
	path := filepath.Join(d.sessionDir, lastAttachName)
	now := time.Now()
	if f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o600); err == nil {
		_ = f.Close()
	}
	_ = os.Chtimes(path, now, now)
}

func (d *daemon) wake() {
	d.mu.Lock()
	d.cond.Broadcast()
	d.mu.Unlock()
}

// firstFew returns up to n bytes of p as a short %q-safe slice for log lines.
// Used to correlate frame content with offsets without dumping kilobytes.
func firstFew(p []byte, n int) []byte {
	if len(p) > n {
		return p[:n]
	}
	return p
}

// statsHeartbeat logs a buffer-state snapshot every statsHeartbeat
// seconds while the PTY pump is running, and once more at exit. Only
// active in verbose mode — the gating happens inside dlog.V so the
// formatting cost is paid only when verbose.
//
// Useful for post-mortem: track held bytes vs disk file size to spot
// disk-spill bugs, see whether ACKs are arriving, and confirm RAM
// tail isn't drifting up unexpectedly.
func (d *daemon) statsHeartbeat(pumpDone <-chan struct{}) {
	const interval = 10 * time.Second
	t := time.NewTicker(interval)
	defer t.Stop()
	log := func() {
		s := d.outputBuf.Stats()
		dlog.V("stats: total=%d held=%d ram=%d disk=%d diskFile=%d trim=%d mem=%d chunks=%d",
			s.Total, s.HeldBytes, s.RAMBytes, s.DiskBytes,
			s.DiskFileSize, s.TrimOffset, s.MemOffset, s.NumChunks)
	}
	for {
		select {
		case <-pumpDone:
			log()
			return
		case <-t.C:
			log()
		}
	}
}

// loginBanner returns the CRLF-terminated lines written into the
// output stream before the shell starts: the session id and the
// current disk-cap utilization. Returns an empty string on any
// error so the daemon can still come up if /proc or the home
// directory is inaccessible.
func loginBanner(sessionID string) string {
	sess, err := sessions.List()
	if err != nil {
		dlog.E("login banner: sessions.List: %v", err)
		return ""
	}
	free, err := sessions.FreeDisk()
	if err != nil {
		dlog.E("login banner: FreeDisk: %v", err)
		return ""
	}
	cap := sessions.DiskBudget(sess, free)
	usage := sessions.TotalDiskUsage(sess)
	pct := 0
	if cap > 0 {
		pct = int(float64(usage) / float64(cap) * 100)
	}
	return fmt.Sprintf(
		"continuous-ssh: session %s\r\ncontinuous-ssh: total buffer disk usage: %s (%d%%) of %s (DiskBudget)\r\n",
		sessionID,
		sessions.HumanBytes(int64(usage)),
		pct,
		sessions.HumanBytes(int64(cap)))
}

// diskCapSweep ticks every diskCapSweepInterval and triggers a
// self-shutdown when the global disk-cap rule fires. Stops as soon
// as the PTY pump completes (so we don't keep evaluating after the
// shell has exited on its own).
func (d *daemon) diskCapSweep(pumpDone <-chan struct{}) {
	t := time.NewTicker(diskCapSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-pumpDone:
			return
		case <-t.C:
			if d.diskcap.Load() {
				return // already shutting down for this reason
			}
			d.checkDiskCap()
		}
	}
}

// checkDiskCap evaluates the global rule for this daemon. Decision:
//
//	sum_disk > DiskBudget  AND  my_size > DiskBudget / N_growing
//
// where N_growing = active + stale sessions and DiskBudget =
// min(2 GiB, 20% × free_disk) − 100 MiB × N_growing. When both
// clauses hit, we mark `diskcap`, flip the daemon into the
// preserve-on-exit shutdown path, and seal the buffer (which wakes
// pumpPTY's reader and lets serveAttach drain the buffer to a
// connected client).
func (d *daemon) checkDiskCap() {
	sess, err := sessions.List()
	if err != nil {
		dlog.E("disk-cap: sessions.List: %v", err)
		return
	}
	free, err := sessions.FreeDisk()
	if err != nil {
		dlog.E("disk-cap: FreeDisk: %v", err)
		return
	}
	cap := sessions.DiskBudget(sess, free)
	sum := sessions.TotalDiskUsage(sess)
	growing := sessions.GrowingCount(sess)
	if growing == 0 {
		return
	}
	share := cap / uint64(growing)
	mySize := uint64(sessions.SegmentBytes(d.sessionDir))
	dlog.V("disk-cap sweep: sum=%d cap=%d share=%d mySize=%d growing=%d",
		sum, cap, share, mySize, growing)
	if sum <= cap {
		return
	}
	if mySize <= share {
		return
	}
	dlog.E("disk-cap: shutting down (sum=%d > cap=%d, mySize=%d > share=%d)",
		sum, cap, mySize, share)
	d.diskcap.Store(true)
	d.mu.Lock()
	d.shutdown = true
	d.preserveOnExit = true
	d.cond.Broadcast()
	d.mu.Unlock()
	d.outputBuf.Seal()
}

func (d *daemon) pumpPTY() {
	p := make([]byte, 32*1024)
	for {
		n, err := d.ptmx.Read(p)
		if n > 0 {
			if e := d.outputBuf.Append(p[:n]); e != nil {
				dlog.E("output append: %v", e)
				d.outputBuf.Seal()
				d.wake()
				return
			}
			if ev := d.altScanner.Scan(p[:n]); ev != 0 {
				d.inAltScreen.Store(ev == 'h')
			}
			d.wake()
		}
		if err != nil {
			if err != io.EOF {
				dlog.V("pty read: %v", err)
			} else {
				dlog.V("pty EOF at %d bytes", d.outputBuf.Len())
			}
			d.outputBuf.Seal()
			d.wake()
			return
		}
	}
}

func (d *daemon) waitCmd(cmd *exec.Cmd, pumpDone <-chan struct{}) {
	<-pumpDone
	err := cmd.Wait()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			// ExitCode() returns -1 for signal-killed processes. Reconstruct
			// the shell-convention `128 + signal_number` instead, so the
			// client can tell signal deaths apart from a normal exit.
			if ws, ok := ee.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
				code = 128 + int(ws.Signal())
			} else {
				code = ee.ExitCode()
			}
		} else {
			code = 1
		}
	}
	d.mu.Lock()
	d.exited = true
	d.exitCode = code
	d.cond.Broadcast()
	d.mu.Unlock()
	dlog.E("command exited code=%d", code)
}

func (d *daemon) killCmd(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	// pty.Start sets Setsid → cmd is a session/pgroup leader. Send SIGHUP
	// to the whole pgroup, then SIGKILL after a grace period.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGHUP)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
}

func (d *daemon) acceptLoop(listener net.Listener) {
	go func() {
		d.mu.Lock()
		for {
			if d.shutdown || d.signalShutdown {
				break
			}
			if d.exited && d.activeConn == nil {
				if d.exitDelivered {
					break
				}
				if time.Now().After(d.keepAliveUntil) {
					break
				}
			}
			d.cond.Wait()
		}
		d.mu.Unlock()
		dlog.V("listener closing")
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		d.attachesWG.Add(1)
		go func(c net.Conn) {
			defer d.attachesWG.Done()
			d.serveAttach(c)
		}(conn)
	}
}

func (d *daemon) serveAttach(conn net.Conn) {
	defer conn.Close()
	pc := proto.NewConn(conn, conn)

	hf, err := pc.ReadFrame()
	if err != nil {
		dlog.E("hello read: %v", err)
		return
	}
	if hf.Type != proto.Hello {
		dlog.E("expected HELLO got %s", hf.Type)
		return
	}
	hello, err := chunk.DecodeHello(hf.Payload)
	if err != nil {
		dlog.E("hello decode: %v", err)
		return
	}
	dlog.V("hello proto=%d.%d mode=%d sid=%s outputTotal=%d outputChunks=%d",
		hello.Major, hello.Minor, hello.Mode, hello.SessionID,
		hello.Output.Total, len(hello.Output.Hashes))

	// Protocol-version check. We always send HELLO_ACK back so the
	// client can see our version too — but on major mismatch we close
	// the conn right after, refusing to set up a stream.
	majorMismatch := hello.Major != proto.ProtocolMajor
	if majorMismatch {
		dlog.E("protocol mismatch: client=%d.%d local=%d.%d",
			hello.Major, hello.Minor, proto.ProtocolMajor, proto.ProtocolMinor)
	} else if hello.Minor != proto.ProtocolMinor {
		dlog.V("protocol minor differs: client=%d.%d local=%d.%d",
			hello.Major, hello.Minor, proto.ProtocolMajor, proto.ProtocolMinor)
	}

	if !majorMismatch && hello.Mode == chunk.ModeAttach && hello.SessionID != d.sessionID {
		dlog.E("session id mismatch want=%s got=%s", d.sessionID, hello.SessionID)
		return
	}

	outputMan := manifestOf(d.outputBuf)
	resendFrom := chunk.ResendFrom(outputMan, hello.Output, d.outputBuf.ChunkSize())
	// Clamp to the buffer's trim point — bytes below it have been freed
	// by the ACK-based purge and are no longer readable.
	if t := d.outputBuf.TrimOffset(); resendFrom < t {
		resendFrom = t
	}

	ackPayload, err := (&chunk.Hello{
		Mode:      chunk.ModeAttach,
		SessionID: d.sessionID,
		Output:    outputMan,
		AltScreen: d.inAltScreen.Load(),
	}).Encode()
	if err != nil {
		dlog.E("ack encode: %v", err)
		return
	}
	if err := pc.WriteFrame(proto.Frame{Type: proto.HelloAck, Payload: ackPayload}); err != nil {
		dlog.E("ack write: %v", err)
		return
	}

	// After advertising our version, bail on major mismatch. The client
	// will see our HELLO_ACK, detect the mismatch on its side, and
	// surface a clear message to the user.
	if majorMismatch {
		return
	}

	d.mu.Lock()
	if d.activeConn != nil && d.activeConn != conn {
		_ = d.activeConn.Close()
	}
	d.activeConn = conn
	d.epoch++
	epoch := d.epoch
	d.touchLastAttach()
	d.touchKeepAlive()
	d.cond.Broadcast()
	d.mu.Unlock()
	dlog.V("attach epoch=%d resendFrom=%d daemonTotal=%d",
		epoch, resendFrom, outputMan.Total)

	streamCtx, streamCancel := context.WithCancel(context.Background())

	var streamerWG sync.WaitGroup
	streamerWG.Add(1)
	go func() {
		defer streamerWG.Done()
		d.streamLoop(streamCtx, pc, resendFrom)
	}()

	var stdinWG sync.WaitGroup
	stdinWG.Add(1)
	go func() {
		defer stdinWG.Done()
		d.readUpstream(pc)
	}()

	d.mu.Lock()
	for d.epoch == epoch && !d.shutdown && !d.exited {
		d.cond.Wait()
	}
	superseded := d.epoch != epoch
	shutdown := d.shutdown
	exited := d.exited
	exitCode := d.exitCode
	d.mu.Unlock()

	switch {
	case superseded || shutdown:
		streamCancel()
		_ = conn.Close()
		streamerWG.Wait()
		stdinWG.Wait()

	case exited:
		// Drain the buffer, then send EXIT. If our signal handler was the
		// reason the shell died, force the exit code to 129 ("killed by
		// SIGHUP") so the client can recognize the daemon-stopped case
		// regardless of how the shell actually handled the signal —
		// bash, for instance, sometimes catches SIGHUP and exits 0
		// through its EXIT trap, which would otherwise look identical
		// to the user typing `exit`.
		streamerWG.Wait()
		d.mu.Lock()
		sigShut := d.signalShutdown
		d.mu.Unlock()
		finalCode := exitCode
		if sigShut {
			finalCode = 129
		}
		dlog.E("EXIT branch: sigShut=%v cmdExitCode=%d finalCode=%d", sigShut, exitCode, finalCode)
		var p [4]byte
		binary.BigEndian.PutUint32(p[:], uint32(finalCode))
		if werr := pc.WriteFrame(proto.Frame{Type: proto.Exit, Payload: p[:]}); werr == nil {
			d.mu.Lock()
			d.exitDelivered = true
			d.cond.Broadcast()
			d.mu.Unlock()
			dlog.V("EXIT delivered to attach epoch=%d", epoch)
		} else {
			dlog.E("EXIT write failed: %v", werr)
		}
		streamCancel()
		_ = conn.Close()
		stdinWG.Wait()
	}

	d.mu.Lock()
	if d.activeConn == conn {
		d.activeConn = nil
	}
	d.touchKeepAlive()
	d.touchLastAttach()
	d.cond.Broadcast()
	d.mu.Unlock()
}

func (d *daemon) streamLoop(ctx context.Context, pc *proto.Conn, from uint64) {
	off := from
	send := make([]byte, 64*1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		for {
			n, err := d.outputBuf.ReadAt(send, off)
			if n > 0 {
				dlog.T("OUT off=%d n=%d firstBytes=%q", off, n, firstFew(send[:n], 16))
				if werr := pc.WriteFrame(proto.Frame{Type: proto.Output, Offset: off, Payload: send[:n]}); werr != nil {
					return
				}
				off += uint64(n)
			}
			if err == io.EOF {
				// Drained the buffer up to the current end. Fall through
				// to WaitFor; only WaitFor knows whether more bytes are
				// coming or the buffer is sealed for good.
				break
			}
			if err != nil {
				dlog.E("streamLoop ReadAt off=%d err=%v", off, err)
				return
			}
			if uint64(n) < uint64(len(send)) {
				break
			}
		}
		if _, err := d.outputBuf.WaitFor(ctx, off); err != nil {
			// Sealed (real EOF) or context canceled — done.
			return
		}
	}
}

func (d *daemon) readUpstream(pc *proto.Conn) {
	for {
		f, err := pc.ReadFrame()
		if err != nil {
			return
		}
		switch f.Type {
		case proto.Stdin:
			// Non-blocking dispatch. If the PTY input buffer is full
			// (foreground program not reading stdin), the writer
			// goroutine is stuck and this channel fills up — drop the
			// frame rather than blocking readUpstream, which must stay
			// responsive to Shutdown / Ack / Resize.
			select {
			case d.stdinCh <- f.Payload:
			default:
				dlog.V("stdin dropped: writer blocked, %d bytes", len(f.Payload))
			}
		case proto.Resize:
			rp, derr := chunk.DecodeResize(f.Payload)
			if derr != nil {
				dlog.E("resize decode: %v", derr)
				continue
			}
			if err := pty.Setsize(d.ptmx, &pty.Winsize{Cols: rp.Cols, Rows: rp.Rows}); err != nil {
				dlog.E("pty setsize: %v", err)
			} else {
				dlog.V("pty resized to %dx%d", rp.Cols, rp.Rows)
			}
		case proto.Shutdown:
			// Client aborted via ~. and asked us to clean up.
			dlog.E("shutdown requested by client")
			d.mu.Lock()
			d.shutdown = true
			d.cond.Broadcast()
			d.mu.Unlock()
			return
		case proto.Ack:
			// Client confirms it has received bytes through this offset;
			// we can free anything older.
			if len(f.Payload) < 8 {
				dlog.E("ACK payload too short: %d", len(f.Payload))
				continue
			}
			ackOff := binary.BigEndian.Uint64(f.Payload[:8])
			d.outputBuf.TrimTo(ackOff)
			dlog.T("ACK trim to offset %d", ackOff)
		case proto.Ping:
			// Wake-detect liveness probe from the client. Echo a
			// Pong with no payload; the client uses it purely as
			// "the round-trip completed within its timeout window."
			if werr := pc.WriteFrame(proto.Frame{Type: proto.Pong}); werr != nil {
				dlog.V("pong write: %v", werr)
			}
		default:
			dlog.V("ignoring frame %s from attach", f.Type)
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

// streamSegments writes OUTPUT frames for the byte range [from, last
// segment end) by opening segments in order and reading sequentially.
// Returns 129 on success, 1 on write/read error (mirrors the existing
// ReplayRun exit-code contract). The caller passes a reusable byte
// buffer to avoid per-segment allocation.
func streamSegments(pc *proto.Conn, segs []buffer.SegmentInfo, from uint64, send []byte) int {
	off := from
	for i := range segs {
		seg := segs[i]
		if seg.EndOff <= off {
			continue // entirely below where the client already is
		}
		f, err := os.Open(seg.Path)
		if err != nil {
			dlog.E("open segment %s: %v", seg.Path, err)
			return 1
		}
		// If `off` is inside this segment, skip to the right spot;
		// otherwise start at the beginning of the segment.
		var relOff int64
		if off > seg.StartOff {
			relOff = int64(off - seg.StartOff)
		} else {
			off = seg.StartOff
		}
		if relOff > 0 {
			if _, err := f.Seek(relOff, io.SeekStart); err != nil {
				dlog.E("seek segment %s: %v", seg.Path, err)
				_ = f.Close()
				return 1
			}
		}
		for off < seg.EndOff {
			chunkLen := uint64(len(send))
			if off+chunkLen > seg.EndOff {
				chunkLen = seg.EndOff - off
			}
			n, rerr := f.Read(send[:chunkLen])
			if n > 0 {
				if werr := pc.WriteFrame(proto.Frame{Type: proto.Output, Offset: off, Payload: send[:n]}); werr != nil {
					dlog.E("output write: %v", werr)
					_ = f.Close()
					return 1
				}
				off += uint64(n)
			}
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				dlog.E("segment read %s: %v", seg.Path, rerr)
				_ = f.Close()
				return 1
			}
		}
		_ = f.Close()
	}
	return 129
}

// ReplayRun serves the contents of a previous session's on-disk segment
// files to one attaching client, then removes the session directory
// and exits. Spawned by attach when it finds a stale session directory
// (segment files present but the regular daemon process is gone —
// e.g., after a remote reboot or a SIGKILL).
//
// Compared to the regular daemon: no PTY, no command, no in-memory buffer,
// no keep-alive grace. We listen exactly long enough to serve one bridge,
// stream the file, send EXIT(0), and clean up.
func ReplayRun(sessionID string, logLevel int) int {
	sessionDir, err := SessionDir(sessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "daemon-replay: %v\n", err)
		return 1
	}
	if _, err := os.Stat(sessionDir); err != nil {
		fmt.Fprintf(os.Stderr, "daemon-replay: session %s: %v\n", sessionID, err)
		return 1
	}
	if err := dlog.Setup(filepath.Join(sessionDir, "daemon.log"), logLevel, nil); err != nil {
		fmt.Fprintf(os.Stderr, "daemon-replay: %v\n", err)
		return 1
	}
	dlog.E("replay daemon starting session=%s pid=%d", sessionID, os.Getpid())

	// Check the clean-shutdown marker. Its presence means the previous
	// daemon flushed its RAM tail to disk successfully on its way out;
	// its absence means the daemon died abruptly (SIGKILL, OOM, power
	// loss) and the on-disk file may be missing trailing bytes. In that
	// case we refuse to replay so the user knows the recovery is
	// unreliable.
	markerPath := filepath.Join(sessionDir, cleanMarkerName)
	_, markerErr := os.Stat(markerPath)
	cleanShutdown := markerErr == nil
	diskcapPath := filepath.Join(sessionDir, diskcapMarkerName)
	_, diskcapErr := os.Stat(diskcapPath)
	diskcapShutdown := diskcapErr == nil

	// The buffer was persisted as a series of segment files at the
	// "output.log" prefix; enumerate them and treat the last segment's
	// end offset as the total stream size.
	prefix := filepath.Join(sessionDir, "output.log")
	segs, err := buffer.ScanSegments(prefix)
	if err != nil {
		dlog.E("scan segments: %v", err)
		return 1
	}
	var totalSize uint64
	if len(segs) > 0 {
		totalSize = segs[len(segs)-1].EndOff
	}
	dlog.V("replay segments=%d totalSize=%d cleanShutdown=%v", len(segs), totalSize, cleanShutdown)

	sockPath := filepath.Join(sessionDir, "sock")
	_ = os.Remove(sockPath)
	listener, err := net.Listen("unix", sockPath)
	if err != nil {
		dlog.E("listen: %v", err)
		return 1
	}
	defer listener.Close()

	pidPath := filepath.Join(sessionDir, "pid")
	_ = os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600)

	// One-shot accept: a single attach connects, we serve it, then exit.
	// Bound the wait so a leftover replay daemon can't linger if attach
	// never shows up.
	if uc, ok := listener.(*net.UnixListener); ok {
		_ = uc.SetDeadline(time.Now().Add(30 * time.Second))
	}
	conn, err := listener.Accept()
	if err != nil {
		dlog.E("accept: %v", err)
		_ = os.RemoveAll(sessionDir)
		return 1
	}
	defer conn.Close()
	_ = listener.Close()
	dlog.V("attach connected for replay")

	pc := proto.NewConn(conn, conn)
	hf, err := pc.ReadFrame()
	if err != nil || hf.Type != proto.Hello {
		dlog.E("hello read: type=%s err=%v", hf.Type, err)
		_ = os.RemoveAll(sessionDir)
		return 1
	}
	hello, err := chunk.DecodeHello(hf.Payload)
	if err != nil {
		dlog.E("hello decode: %v", err)
		_ = os.RemoveAll(sessionDir)
		return 1
	}
	dlog.V("replay HELLO clientTotal=%d", hello.Output.Total)

	// Advertise our knowledge of the stream size in the HELLO_ACK. If the
	// shutdown wasn't clean we report Total = client's own total so that
	// the chunk-reconciliation diff comes out empty; we'll deliver an
	// error message and exit instead of replaying.
	announcedTotal := totalSize
	if !cleanShutdown {
		announcedTotal = hello.Output.Total
	}
	ackPayload, err := (&chunk.Hello{
		Mode:      chunk.ModeAttach,
		SessionID: sessionID,
		Output:    chunk.Manifest{Total: announcedTotal},
	}).Encode()
	if err != nil {
		dlog.E("ack encode: %v", err)
		_ = os.RemoveAll(sessionDir)
		return 1
	}
	if err := pc.WriteFrame(proto.Frame{Type: proto.HelloAck, Payload: ackPayload}); err != nil {
		dlog.E("ack write: %v", err)
		_ = os.RemoveAll(sessionDir)
		return 1
	}

	// Exit codes are the contract with the client:
	//   130 = recovery refused — the previous daemon didn't shut down
	//         cleanly, so the on-disk buffer is suspect. Left in place
	//         for manual recovery.
	//   133 = the previous daemon stopped by signal earlier (while no
	//         client was connected) and we just replayed its preserved
	//         buffer. The replay-side counterpart to the live 129.
	//   134 = same as 133 but the previous shutdown was a host-wide
	//         disk-cap shutdown — the diskcap marker upgrades 133 to
	//         134 so the client can print a more specific message.
	// The client interprets these and prints a one-line notice AFTER its
	// terminal reset, so the message survives alt-screen exit (htop, etc.)
	// which would otherwise eat any inline OUTPUT-frame notice.
	exitCode := 0
	if !cleanShutdown {
		exitCode = 130
	} else {
		// Stream segments in order. Client dedups by offset, so we can
		// start from any point — but the client's HELLO already told us
		// how far it has, so honour that to avoid retransmitting bytes
		// it already holds (the client's offset is the lower bound;
		// anything below it is overlap we'd just throw away).
		off := hello.Output.Total
		send := make([]byte, 64*1024)
		exitCode = streamSegments(pc, segs, off, send)
		// streamSegments returns 129 on success. Promote it to 133
		// (replay-recovered) so the client can distinguish "daemon
		// stopped just now" (live 129) from "daemon stopped while
		// the client was disconnected, and we just replayed it"
		// (133). A diskcap marker further promotes 133 → 134.
		switch {
		case exitCode == 129 && diskcapShutdown:
			exitCode = 134
		case exitCode == 129:
			exitCode = 133
		}
		dlog.V("replay streamed from offset %d to %d", hello.Output.Total, totalSize)
	}

	var ec [4]byte
	binary.BigEndian.PutUint32(ec[:], uint32(exitCode))
	exitDelivered := pc.WriteFrame(proto.Frame{Type: proto.Exit, Payload: ec[:]}) == nil

	_ = conn.Close()
	// Only clean up the session dir when the replay actually completed
	// end-to-end: a successful stream (129 or 134) AND a successful
	// EXIT-frame delivery. If anything went wrong (mid-stream network
	// drop → exitCode=1, or EXIT write failed silently because the
	// link was already dead), leave the segments + clean marker in
	// place so a subsequent reconnect can spawn a fresh replay daemon
	// and resume from where the client got to. The client's
	// outputBuf.Len() advances monotonically across attempts, so each
	// retry covers strictly less ground until the buffered output is
	// fully delivered.
	streamSucceeded := exitCode == 133 || exitCode == 134
	if cleanShutdown && streamSucceeded && exitDelivered {
		_ = os.RemoveAll(sessionDir)
	}
	dlog.E("replay daemon exiting code=%d cleanShutdown=%v streamSucceeded=%v exitDelivered=%v",
		exitCode, cleanShutdown, streamSucceeded, exitDelivered)
	return exitCode
}
