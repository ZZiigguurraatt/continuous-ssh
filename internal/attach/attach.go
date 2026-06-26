// Package attach is the remote-side bridge process. It runs on the host the
// client ssh'd to, and forwards frames between the ssh stdio channel and the
// daemon's Unix socket. In --new mode it also forks a fresh daemon process.
//
// attach is deliberately silent: catastrophic errors are written to a log
// file under the session directory rather than to stderr, since stderr is
// surfaced to the user terminal. --debug enables verbose logging.
package attach

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"

	"github.com/zziigguurraatt/continuous-ssh/internal/buffer"
	"github.com/zziigguurraatt/continuous-ssh/internal/daemon"
	"github.com/zziigguurraatt/continuous-ssh/internal/dlog"
	"github.com/zziigguurraatt/continuous-ssh/internal/proto"
	"github.com/zziigguurraatt/continuous-ssh/internal/sessions"
)

// Run is the attach subcommand entry point.
//
//	xssh attach [--debug | --trace] --new
//	xssh attach [--debug | --trace] --session <id>
//
// In --new mode the daemon launches the user's login shell.
func Run(argv []string) int {
	newMode, sessionID, logLevel, err := parseArgs(argv)
	if err != nil {
		fmt.Fprintf(os.Stderr, "attach: %v\n", err)
		return 2
	}

	if newMode {
		// Pre-spawn disk-cap check. If the host-wide segment-byte
		// total is already at or above DiskBudget, refuse to start a
		// new session — there's no budget for the daemon to grow
		// into. Communicate the refusal back to the client as an
		// EXIT(135) frame written to ssh stdout, then exit.
		if refused := refuseIfDiskCapHit(); refused {
			return 135
		}
		id, err := generateSessionID()
		if err != nil {
			fmt.Fprintf(os.Stderr, "attach: %v\n", err)
			return 1
		}
		sessionID = id
	}

	sd, err := daemon.SessionDir(sessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "attach: %v\n", err)
		return 1
	}
	if err := os.MkdirAll(sd, 0o700); err != nil {
		fmt.Fprintf(os.Stderr, "attach: %v\n", err)
		return 1
	}

	// attach must NEVER mirror logs to its own stderr — that stderr is wired
	// back to the user's terminal via ssh, which would break the silence
	// guarantee. File-only.
	_ = dlog.Setup(filepath.Join(sd, "attach.log"), logLevel, nil)
	dlog.E("attach starting session=%s newMode=%v level=%d pid=%d", sessionID, newMode, logLevel, os.Getpid())

	if newMode {
		if err := spawnDaemon(sessionID, logLevel); err != nil {
			dlog.E("spawn daemon: %v", err)
			return 1
		}
		dlog.V("daemon spawned for session=%s", sessionID)
	}

	sockPath := filepath.Join(sd, "sock")
	var conn net.Conn
	if newMode {
		// We just spawned the daemon ourselves; wait up to 5 s for its
		// socket to come up.
		conn, err = dialWithRetry(sockPath, 5*time.Second)
	} else {
		// --session: a running daemon should already be listening. Try
		// once and decide quickly — no point spending the retry budget
		// on a socket whose owner is gone.
		conn, err = net.Dial("unix", sockPath)
	}
	if err != nil {
		// Dial failed. If the session directory still has any output.log
		// segment files, the daemon process is gone but a recoverable
		// session lives on disk — spawn a replay daemon to serve what's
		// there.
		segs, segErr := buffer.ScanSegments(filepath.Join(sd, "output.log"))
		if segErr == nil && len(segs) > 0 {
			dlog.V("dial failed (%v); %d segment(s) present, spawning replay daemon", err, len(segs))
			if serr := spawnReplayDaemon(sessionID, logLevel); serr != nil {
				dlog.E("spawn replay daemon: %v", serr)
				return 1
			}
			conn, err = dialWithRetry(sockPath, 5*time.Second)
			if err != nil {
				dlog.E("dial after replay spawn: %v", err)
				return 1
			}
		} else {
			dlog.E("dial: %v", err)
			return 1
		}
	}
	defer conn.Close()
	dlog.V("attach bridging session=%s sock=%s", sessionID, sockPath)
	return bridge(conn)
}

func spawnReplayDaemon(sessionID string, logLevel int) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate self: %w", err)
	}
	args := []string{"daemon", "--session", sessionID, "--replay"}
	args = append(args, logLevelFlags(logLevel)...)
	cmd := exec.Command(self, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn replay daemon: %w", err)
	}
	_ = cmd.Process.Release()
	return nil
}

// parseArgs returns (newMode, sessionID, logLevel, err).
func parseArgs(argv []string) (newMode bool, sessionID string, logLevel int, err error) {
	logLevel = dlog.LevelError
	i := 0
	for i < len(argv) {
		switch argv[i] {
		case "--new":
			newMode = true
			i++
		case "--debug":
			if logLevel < dlog.LevelVerbose {
				logLevel = dlog.LevelVerbose
			}
			i++
		case "--trace":
			logLevel = dlog.LevelTrace
			i++
		case "--session":
			if i+1 >= len(argv) {
				return false, "", 0, errors.New("--session requires an argument")
			}
			sessionID = argv[i+1]
			i += 2
		default:
			return false, "", 0, fmt.Errorf("unknown flag %q", argv[i])
		}
	}
	if newMode == (sessionID != "") {
		return false, "", 0, errors.New("exactly one of --new or --session is required")
	}
	return newMode, sessionID, logLevel, nil
}

func generateSessionID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("generate session id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// logLevelFlags returns the CLI flag(s) corresponding to a dlog level
// that get appended to a spawned subprocess's argv. Level 0 emits
// nothing; level 1 emits --debug; level 2 emits --trace.
func logLevelFlags(level int) []string {
	switch level {
	case dlog.LevelTrace:
		return []string{"--trace"}
	case dlog.LevelVerbose:
		return []string{"--debug"}
	default:
		return nil
	}
}

func spawnDaemon(sessionID string, logLevel int) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("locate self: %w", err)
	}
	args := []string{"daemon", "--session", sessionID}
	args = append(args, logLevelFlags(logLevel)...)
	cmd := exec.Command(self, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("spawn daemon: %w", err)
	}
	// Detach: don't wait for the daemon.
	_ = cmd.Process.Release()
	return nil
}

func dialWithRetry(path string, timeout time.Duration) (net.Conn, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		conn, err := net.Dial("unix", path)
		if err == nil {
			return conn, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("dial %s: %w", path, lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// refuseIfDiskCapHit returns true (and writes an EXIT(135) frame to
// stdout) when total session disk usage already meets or exceeds
// DiskBudget. The frame goes to ssh's stdout — the client reads it in
// place of HELLO_ACK and surfaces a "cap reached" message before
// exiting with code 135. Returns false (silently) on any error
// reading sessions/free-disk state; better to let the session start
// than to block on a flaky stat call.
func refuseIfDiskCapHit() bool {
	sess, err := sessions.List()
	if err != nil {
		return false
	}
	free, err := sessions.FreeDisk()
	if err != nil {
		return false
	}
	cap := sessions.DiskBudget(sess, free)
	usage := sessions.TotalDiskUsage(sess)
	if usage < cap {
		return false
	}
	var p [4]byte
	binary.BigEndian.PutUint32(p[:], 135)
	pc := proto.NewConn(nil, os.Stdout)
	_ = pc.WriteFrame(proto.Frame{Type: proto.Exit, Payload: p[:]})
	return true
}

func bridge(conn net.Conn) int {
	done := make(chan string, 2)

	go func() {
		n, err := io.Copy(os.Stdout, conn)
		dlog.E("BRIDGE: conn->stdout returned n=%d err=%v", n, err)
		done <- "conn->stdout"
	}()
	go func() {
		n, err := io.Copy(conn, os.Stdin)
		dlog.E("BRIDGE: stdin->conn returned n=%d err=%v", n, err)
		done <- "stdin->conn"
	}()

	first := <-done
	dlog.E("BRIDGE: first direction done: %s", first)
	// One side closed; tear down the other so the bridge exits promptly.
	_ = conn.Close()
	_ = os.Stdin.Close()

	// Best-effort wait for the second direction; the OS cleans up on exit.
	select {
	case second := <-done:
		dlog.E("BRIDGE: second direction done: %s", second)
	case <-time.After(500 * time.Millisecond):
		dlog.E("BRIDGE: second direction did not finish within 500ms")
	}
	return 0
}
