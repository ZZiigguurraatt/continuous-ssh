package main

import (
	"fmt"
	"os"

	"github.com/zziigguurraatt/continuous-ssh/internal/attach"
	"github.com/zziigguurraatt/continuous-ssh/internal/client"
	"github.com/zziigguurraatt/continuous-ssh/internal/daemon"
	"github.com/zziigguurraatt/continuous-ssh/internal/sessions"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	switch os.Args[1] {
	case "attach":
		os.Exit(attach.Run(os.Args[2:]))
	case "daemon":
		os.Exit(daemon.Run(os.Args[2:]))
	case "ls":
		os.Exit(sessions.RunLs(os.Args[2:]))
	case "kill":
		os.Exit(sessions.RunKill(os.Args[2:]))
	case "rm":
		os.Exit(sessions.RunRm(os.Args[2:]))
	case "-h", "--help", "help":
		usage()
		os.Exit(0)
	default:
		os.Exit(client.Run(os.Args[1:]))
	}
}

// usage prints the help text shown by `xssh -h`. The README's
// "## Usage" section embeds this verbatim — if you edit the text
// below, copy the new output into README.md too so the docs don't
// drift from the binary.
func usage() {
	fmt.Fprintln(os.Stderr, `xssh — continuous-ssh: interactive SSH that survives disconnects.

Usage:
  xssh [--debug | --debug-file | --trace-file] [ssh-args...] <target>

The remote always runs your login shell. ssh-args (flags + target) are
forwarded verbatim to the system ssh binary. On disconnect the wrapper
reconnects silently and retries forever until the remote delivers an
EXIT frame or you abort with ~.

Flags:
  --debug       verbose logging to a per-invocation file under
                ~/.continuous-ssh/clients/<date>-<target>-<pid>.log AND
                mirrored to stderr (CR-LF translated in raw mode).
                Propagated to the remote attach and daemon. ssh's own
                stderr is captured into the log instead of being discarded.
  --debug-file  same level as --debug but file-only — no stderr mirror.
                Prints the log path on startup so you can tail it from
                another shell.
  --trace-file  bumps the log level: also captures per-frame chatter
                (OUT/IN frames, every ACK sent, overlap drops). Always
                file-only — would flood the terminal otherwise. High
                volume: thousands of lines per session under load.

Key sequences (at start of line):
  ~.   abort and exit
  ~~   send a literal ~

Subcommands (run on the host where the daemon lives):
  ls    list sessions in ~/.continuous-ssh/sessions/
        flags: --active, --stale, --dead, --all
  kill  terminate sessions; preserves stale-session data for replay.
        usage: xssh kill <id> | --all | --active | --stale | --dead
  rm    remove a Dead session's on-disk directory. By default refuses
        to operate on a live daemon; pass --kill to terminate it first.
        usage: xssh rm [--kill] <id> | --all | --active | --stale | --dead

Internal subcommands (invoked remotely; not for direct use):
  attach   bridge ssh stdio to a session daemon
  daemon   run a session daemon`)
}
