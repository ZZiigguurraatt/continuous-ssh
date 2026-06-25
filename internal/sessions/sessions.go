// Package sessions implements the `xssh ls` and `xssh kill` subcommands —
// management for session directories under ~/.continuous-ssh/sessions/ on
// the host where the daemon is running.
package sessions

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Status describes the runtime state of a session.
type Status int

const (
	StatusUnknown Status = iota
	StatusActive         // daemon alive, client currently connected
	StatusStale          // daemon alive, no client (waiting for reconnect)
	StatusDead           // session dir present, no daemon process
)

func (s Status) String() string {
	switch s {
	case StatusActive:
		return "active"
	case StatusStale:
		return "stale"
	case StatusDead:
		return "dead"
	default:
		return "?"
	}
}

// Session is one session's on-disk + runtime state.
type Session struct {
	ID      string
	Dir     string
	Status  Status
	Pid     int
	Started time.Time
	Clean   bool // a "clean" marker file is present
}

// List enumerates every session directory under ~/.continuous-ssh/sessions/.
func List() ([]Session, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	root := filepath.Join(home, ".continuous-ssh", "sessions")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	connected := connectedSocks()

	var out []Session
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		s := Session{ID: e.Name(), Dir: filepath.Join(root, e.Name())}
		s.Pid = readPid(s.Dir)
		s.Started = readStarted(s.Dir)
		if _, err := os.Stat(filepath.Join(s.Dir, "clean")); err == nil {
			s.Clean = true
		}
		alive := s.Pid > 0 && syscall.Kill(s.Pid, 0) == nil
		switch {
		case !alive:
			s.Status = StatusDead
		case connected[filepath.Join(s.Dir, "sock")]:
			s.Status = StatusActive
		default:
			s.Status = StatusStale
		}
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		// Sort by status (active first, then stale, then dead), then by start time.
		if out[i].Status != out[j].Status {
			return out[i].Status < out[j].Status
		}
		return out[i].Started.Before(out[j].Started)
	})
	return out, nil
}

func readPid(dir string) int {
	data, err := os.ReadFile(filepath.Join(dir, "pid"))
	if err != nil {
		return 0
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(data)))
	return pid
}

func readStarted(dir string) time.Time {
	data, err := os.ReadFile(filepath.Join(dir, "info"))
	if err != nil {
		return time.Time{}
	}
	for _, line := range strings.Split(string(data), "\n") {
		if v, ok := strings.CutPrefix(line, "started="); ok {
			t, _ := time.Parse(time.RFC3339, strings.TrimSpace(v))
			return t
		}
	}
	return time.Time{}
}

// connectedSocks returns the set of unix-socket paths that currently have an
// established peer connection. Backed by /proc/net/unix; an empty result is
// returned silently on platforms or kernels that don't expose it.
func connectedSocks() map[string]bool {
	out := map[string]bool{}
	f, err := os.Open("/proc/net/unix")
	if err != nil {
		return out
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Scan() // header
	// columns: Num RefCount Protocol Flags Type St Inode Path
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 8 {
			continue
		}
		// `St` (state) 03 == SS_CONNECTED — a peer is attached.
		if fields[5] == "03" {
			out[fields[7]] = true
		}
	}
	return out
}

// Render produces a human-readable table of sessions, one per line.
func Render(sessions []Session, w *os.File) {
	if len(sessions) == 0 {
		fmt.Fprintln(w, "(no sessions)")
		return
	}
	fmt.Fprintf(w, "%-34s  %-7s  %-7s  %s\n", "SESSION ID", "STATUS", "PID", "STARTED")
	now := time.Now()
	for _, s := range sessions {
		pid := "-"
		if s.Pid > 0 {
			pid = strconv.Itoa(s.Pid)
		}
		started := "-"
		if !s.Started.IsZero() {
			d := now.Sub(s.Started).Truncate(time.Second)
			started = fmt.Sprintf("%s (%s ago)", s.Started.Format("2006-01-02 15:04"), d)
		}
		fmt.Fprintf(w, "%-34s  %-7s  %-7s  %s\n", s.ID, s.Status, pid, started)
	}
}

// Kill terminates a session via SIGTERM. The daemon's signal handler runs
// the graceful-shutdown path: drains the active conn if any (sending EXIT
// so the client prints "remote daemon stopped."), kills the shell, and
// removes its own session directory on the way out. A *stale* session's
// directory is intentionally preserved — `output.log` and the `clean`
// marker are kept so the next reconnect to that session id can replay it.
//
// For a Dead session the on-disk directory is unlinked directly.
func Kill(s Session) error {
	switch s.Status {
	case StatusActive, StatusStale:
		if err := syscall.Kill(s.Pid, syscall.SIGTERM); err != nil {
			return fmt.Errorf("signal pid %d: %w", s.Pid, err)
		}
		return nil
	case StatusDead:
		if err := os.RemoveAll(s.Dir); err != nil {
			return fmt.Errorf("remove %s: %w", s.Dir, err)
		}
		return nil
	default:
		return fmt.Errorf("unknown status for %s", s.ID)
	}
}

// Rm removes a session's on-disk directory. By default it operates only
// on Dead sessions — a live daemon (Active or Stale) is left alone and
// the caller is expected to run `xssh kill` first.
//
// When kill is true, Rm additionally terminates the daemon before removing:
// SIGTERM, wait up to `rmGrace` for the daemon to exit on its own (so an
// active client still sees EXIT cleanly), SIGKILL if it doesn't, then
// RemoveAll. Equivalent to `xssh kill` immediately followed by `xssh rm`.
func Rm(s Session, kill bool) error {
	switch s.Status {
	case StatusDead:
		if err := os.RemoveAll(s.Dir); err != nil {
			return fmt.Errorf("remove %s: %w", s.Dir, err)
		}
		return nil
	case StatusActive, StatusStale:
		if !kill {
			return fmt.Errorf("session is %s (live daemon); pass --kill to terminate it first, or `xssh kill` it manually", s.Status)
		}
		if err := syscall.Kill(s.Pid, syscall.SIGTERM); err != nil && !errno(err, syscall.ESRCH) {
			return fmt.Errorf("signal pid %d: %w", s.Pid, err)
		}
		// Wait for the daemon to exit gracefully.
		deadline := time.Now().Add(rmGrace)
		for time.Now().Before(deadline) {
			if syscall.Kill(s.Pid, 0) != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		// Escalate if still around.
		if syscall.Kill(s.Pid, 0) == nil {
			_ = syscall.Kill(s.Pid, syscall.SIGKILL)
			time.Sleep(100 * time.Millisecond)
		}
		if err := os.RemoveAll(s.Dir); err != nil {
			return fmt.Errorf("remove %s: %w", s.Dir, err)
		}
		return nil
	default:
		return fmt.Errorf("unknown status for %s", s.ID)
	}
}

const rmGrace = 3 * time.Second

func errno(err error, target syscall.Errno) bool {
	for e := err; e != nil; {
		if eno, ok := e.(syscall.Errno); ok {
			return eno == target
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := e.(unwrapper)
		if !ok {
			return false
		}
		e = u.Unwrap()
	}
	return false
}
