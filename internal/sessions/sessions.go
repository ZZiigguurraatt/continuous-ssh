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
	ID         string
	Dir        string
	Status     Status
	Pid        int
	Started    time.Time
	LastAttach time.Time // mtime of the lastattach marker; zero if absent
	DiskBytes  int64     // sum of output.log.<offset> segment file sizes
	Clean      bool      // a "clean" marker file is present
	IsCurrent  bool      // true if XSSH_SESSION env var matches this session
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
		s.LastAttach = mtimeOrZero(filepath.Join(s.Dir, "lastattach"))
		s.DiskBytes = sumSegmentSizes(s.Dir)
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
	// Mark "current" session — the one this process is running inside.
	// The daemon exports XSSH_SESSION=<id> into the spawned shell's env,
	// so child processes (like `xssh ls`) inherit it.
	if cur := os.Getenv("XSSH_SESSION"); cur != "" {
		for i := range out {
			if out[i].ID == cur {
				out[i].IsCurrent = true
				break
			}
		}
	}
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
//
// Column layout:
//
//	[*] SESSION ID  STATUS  PID    DISK BUF  LAST CHANGE  STARTED
//
// The leading `*` marks the session this process is running inside
// (XSSH_SESSION env var match), if any. LAST CHANGE encodes both the
// kind of the most recent state-change event and how long ago it
// happened — "connected Xm" for an active session (still attached
// since X ago) or "disconnected Xm ago" for stale/dead (without a
// client since X ago).
func Render(sessions []Session, w *os.File) {
	if len(sessions) == 0 {
		fmt.Fprintln(w, "(no sessions)")
		return
	}
	fmt.Fprintf(w, "%-2s%-34s  %-7s  %-7s  %-10s  %-22s   %s\n",
		"", "SESSION ID", "STATUS", "PID", "DISK BUF", "LAST CHANGE", "STARTED")
	now := time.Now()
	for _, s := range sessions {
		pid := "-"
		if s.Pid > 0 {
			pid = strconv.Itoa(s.Pid)
		}
		started := "-"
		if !s.Started.IsZero() {
			ago := humanDur(now.Sub(s.Started))
			// %3s right-justifies short durations so the unit char
			// lines up vertically across rows ("12m" / " 5m" / "44m"),
			// without the extra leading space that %4s would add for
			// 3-char durations (the common case).
			started = fmt.Sprintf("%s (%3s ago)", s.Started.Format("2006-01-02 15:04"), ago)
		}
		change := "-"
		if !s.LastAttach.IsZero() {
			ago := humanDur(now.Sub(s.LastAttach))
			verb := "connected"
			if s.Status != StatusActive {
				verb = "disconnected"
			}
			// Verb padded to "disconnected" width; the time portion
			// is wrapped in parens with a single space separator,
			// matching STARTED's "<prefix> (X ago)" pattern.
			// Duration right-justified to 3 chars so the unit chars
			// line up vertically across rows.
			change = fmt.Sprintf("%-12s (%3s ago)", verb, ago)
		}
		marker := ""
		if s.IsCurrent {
			marker = "* "
		} else {
			marker = "  "
		}
		fmt.Fprintf(w, "%s%-34s  %-7s  %-7s  %-10s  %-22s   %s\n",
			marker, s.ID, s.Status, pid, humanBytes(s.DiskBytes), change, started)
	}
}

// mtimeOrZero returns the mtime of the file at path, or the zero time
// if it can't be stat'd.
func mtimeOrZero(path string) time.Time {
	fi, err := os.Stat(path)
	if err != nil {
		return time.Time{}
	}
	return fi.ModTime()
}

// sumSegmentSizes adds up the bytes of every output.log.<digits> file
// under dir. The disk-spill buffer is segmented (see internal/buffer
// for the format); we don't need the buffer package here — a simple
// pattern match on filenames does the job.
func sumSegmentSizes(dir string) int64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	var total int64
	for _, e := range entries {
		name := e.Name()
		rem, ok := strings.CutPrefix(name, "output.log.")
		if !ok || rem == "" {
			continue
		}
		if _, err := strconv.ParseUint(rem, 10, 64); err != nil {
			continue
		}
		if fi, err := e.Info(); err == nil {
			total += fi.Size()
		}
	}
	return total
}

// humanBytes formats a byte count as a short string ("0", "512 B",
// "3.2 KB", "5.0 MB", "1.2 GB"). Decimal units, one fractional digit.
func humanBytes(n int64) string {
	if n == 0 {
		return "0"
	}
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

// humanDur formats a duration as "1d", "3h", "12m", "47s" — coarsest
// non-zero unit only. Caller appends " ago" or similar.
func humanDur(d time.Duration) string {
	if d < time.Second {
		return "0s"
	}
	switch {
	case d >= 24*time.Hour:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	case d >= time.Hour:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d >= time.Minute:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
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
