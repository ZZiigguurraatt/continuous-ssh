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

func usage() {
	fmt.Fprintln(os.Stderr, `xssh — continuous-ssh: interactive SSH that survives disconnects.

Usage:
  xssh [--debug] [ssh-args...] <target>

The remote always runs your login shell. ssh-args (flags + target) are
forwarded verbatim to the system ssh binary. On disconnect the wrapper
reconnects silently; if the remote session is unrecoverable it gives up
after a few attempts.

Flags:
  --debug   verbose logging to ~/.continuous-ssh/client.log AND mirrored to
            stderr. Propagated to the remote attach and daemon. ssh's own
            stderr is captured into the log instead of being discarded.

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
