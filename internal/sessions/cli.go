package sessions

import (
	"errors"
	"fmt"
	"os"
)

// RunLs is the `xssh ls` subcommand entry point.
//
//	xssh ls                  list all sessions
//	xssh ls --active         only those with a connected client
//	xssh ls --stale          only those without a connected client
//	xssh ls --dead           only directories whose daemon process is gone
func RunLs(argv []string) int {
	filter, err := parseFilter(argv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "xssh ls:", err)
		return 2
	}
	sessions, err := List()
	if err != nil {
		fmt.Fprintln(os.Stderr, "xssh ls:", err)
		return 1
	}
	sessions = applyFilter(sessions, filter)
	Render(sessions, os.Stdout)
	return 0
}

// RunKill is the `xssh kill` subcommand entry point.
//
//	xssh kill <id>           kill a specific session
//	xssh kill --all          kill every session (active, stale, dead)
//	xssh kill --active       only kill sessions with a connected client
//	xssh kill --stale        only kill sessions without a connected client
//	xssh kill --dead         only remove dead session directories
//
// `kill` is the graceful path: SIGTERM, lets the daemon do its own
// cleanup. Stale-session directories are preserved (output.log + clean
// marker stay on disk, so a later reconnect can replay them). Use
// `xssh rm` if you also want to remove the preserved data.
func RunKill(argv []string) int {
	return runDispose(argv, "kill", func(s Session) error { return Kill(s) })
}

// RunRm is the `xssh rm` subcommand entry point. By default it only
// removes Dead session directories; a live (Active or Stale) session is
// refused unless --kill is also passed, in which case Rm terminates the
// daemon (graceful SIGTERM, with SIGKILL fallback) before removing.
//
//	xssh rm <id>           remove a Dead session directory
//	xssh rm --kill <id>    kill the daemon AND remove the directory in one step
//	xssh rm --dead         remove all dead session directories
//	xssh rm --kill --all   kill + remove every session
//	xssh rm --kill --stale kill + remove stale sessions (drops their preserved
//	                       buffers — no replay possible afterwards)
func RunRm(argv []string) int {
	// Extract --kill flag, leaving the rest for the standard
	// selector parser.
	kill := false
	var rest []string
	for _, a := range argv {
		if a == "--kill" {
			kill = true
			continue
		}
		rest = append(rest, a)
	}
	return runDispose(rest, "rm", func(s Session) error { return Rm(s, kill) })
}

func runDispose(argv []string, verb string, fn func(Session) error) int {
	if len(argv) == 0 {
		fmt.Fprintf(os.Stderr, "xssh %s: expected session id or --all/--active/--stale/--dead\n", verb)
		return 2
	}
	if argv[0] == "--all" || argv[0] == "--active" || argv[0] == "--stale" || argv[0] == "--dead" {
		filter, err := parseFilter(argv)
		if err != nil {
			fmt.Fprintf(os.Stderr, "xssh %s: %v\n", verb, err)
			return 2
		}
		sessions, err := List()
		if err != nil {
			fmt.Fprintf(os.Stderr, "xssh %s: %v\n", verb, err)
			return 1
		}
		return disposeAll(applyFilter(sessions, filter), verb, fn)
	}
	id := argv[0]
	sessions, err := List()
	if err != nil {
		fmt.Fprintf(os.Stderr, "xssh %s: %v\n", verb, err)
		return 1
	}
	for _, s := range sessions {
		if s.ID == id {
			return disposeAll([]Session{s}, verb, fn)
		}
	}
	fmt.Fprintf(os.Stderr, "xssh %s: no such session: %s\n", verb, id)
	return 1
}

func disposeAll(sessions []Session, verb string, fn func(Session) error) int {
	if len(sessions) == 0 {
		fmt.Fprintln(os.Stderr, "(no matching sessions)")
		return 0
	}
	rc := 0
	for _, s := range sessions {
		if err := fn(s); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", s.ID, err)
			rc = 1
			continue
		}
		switch verb {
		case "kill":
			switch s.Status {
			case StatusActive, StatusCatchup, StatusStalled, StatusReplay, StatusStale:
				fmt.Fprintf(os.Stdout, "%s: signaled %s session (pid %d)\n", s.ID, s.Status, s.Pid)
			case StatusDead:
				fmt.Fprintf(os.Stdout, "%s: removed dead session directory\n", s.ID)
			}
		case "rm":
			switch s.Status {
			case StatusActive, StatusCatchup, StatusStalled, StatusReplay, StatusStale:
				fmt.Fprintf(os.Stdout, "%s: terminated and removed (was %s, pid %d)\n", s.ID, s.Status, s.Pid)
			case StatusDead:
				fmt.Fprintf(os.Stdout, "%s: removed dead session directory\n", s.ID)
			}
		}
	}
	return rc
}

type filter struct{ active, stale, dead bool }

func parseFilter(argv []string) (filter, error) {
	var f filter
	if len(argv) == 0 {
		return filter{active: true, stale: true, dead: true}, nil
	}
	for _, a := range argv {
		switch a {
		case "--all":
			f.active, f.stale, f.dead = true, true, true
		case "--active":
			f.active = true
		case "--stale":
			f.stale = true
		case "--dead":
			f.dead = true
		default:
			return filter{}, errors.New("unknown flag " + a)
		}
	}
	if !f.active && !f.stale && !f.dead {
		return filter{}, errors.New("no statuses selected")
	}
	return f, nil
}

func applyFilter(in []Session, f filter) []Session {
	var out []Session
	for _, s := range in {
		switch s.Status {
		case StatusActive, StatusCatchup, StatusStalled, StatusReplay:
			// All four of these have a live attach — group
			// them under the --active filter rather than
			// needing dedicated flags for transient variants.
			if f.active {
				out = append(out, s)
			}
		case StatusStale:
			if f.stale {
				out = append(out, s)
			}
		case StatusDead:
			if f.dead {
				out = append(out, s)
			}
		}
	}
	return out
}
