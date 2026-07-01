# continuous-ssh (`xssh`)

A thin interactive SSH wrapper that survives transient network disconnects.
The binary is named `xssh` for brevity at the command line; the project name
is `continuous-ssh`.
The remote login shell keeps running on the host while the local client
silently reconnects when it can and replays any output that arrived while
the link was down. Replay is driven by a SHA-256 **chunk-manifest
reconciliation**: each side hashes the output stream in 1 MiB chunks, and
on reconnect the daemon compares manifests and retransmits exactly the
range the client is missing — with a "last complete chunk always resent"
rule that closes the ragged-edge gap when a connection drops mid-chunk.
Only the shell's own stdio is shown — the wrapper is otherwise invisible.

## What it's for

The motivating use case is *laptop sleep*: you're running a long command
on a remote machine, you close the lid, and when you wake the laptop the
session is still there with all the output that was produced while you
were away. Same idea applies to network drops, switching APs, or other
breaks — the gap can be arbitrarily long; the only real limit is the
host-wide DiskBudget bounding how much unacked output piles up while
the link is down.

For best results, pair it with a **WireGuard tunnel** between the
laptop and the remote. The TCP connection that ssh rides on will then
survive NAT changes as you roam — the SSH session stays alive across
network changes rather than needing to be re-established by this tool.

## Goals

- Single binary, deployed identically on local and remote, that wraps the
  system `ssh` binary as-is — no separate listener, no extra daemon to
  install, no configuration file.
- Survive disconnects (laptop sleep, network outage, WiFi handoff)
  without the user noticing or losing output. Gap duration is
  bounded only by the host-wide DiskBudget, not by any per-event
  timer.
- **Remote processes keep running across disconnects.** Whatever you had
  going on the remote shell — a long build, a download, a TUI program —
  continues to run while the local link is down. The wrapper reconnects
  to the same already-running shell on the other side.
- Otherwise be invisible: only the remote shell's stdio is shown.
- **Your local terminal emulator's scrollback IS the scrollback.** The
  wrapper writes output straight to your terminal in the same order it
  arrived — once a byte is displayed, it goes into your terminal's
  scrollback and stays there. There is no separate per-session scrollback
  to scroll into, no special keybinding to view past output, no `tmux
  copy-mode` analogue. Once the session ends, every byte you ever saw
  is still right there in your terminal, scrollable as usual.
- Continue to honour everything in `~/.ssh/config` (aliases, keys, agent,
  `ProxyJump`, …) because we shell out to `ssh`.
- Handle the alt-screen buffer correctly across abrupt session ends: if
  the remote program (vim, htop, …) was in alt-screen and the daemon is
  killed or the user aborts, the terminal is properly exited from
  alt-screen and the cursor is restored — no half-rendered TUI frame
  left behind.

## Non-goals

- **Not a terminal multiplexer; not a replacement for tmux/screen.**
  No detach/reattach across local invocations, no per-session
  scrollback management, no multiple windows, no session sharing
  between users. The remote session is tied to the lifetime of the
  local client — when the client exits, the remote shell is killed.
  If you want multiplexer features, run `tmux` or `screen` *inside*
  the remote shell; they compose cleanly and the multiplexer's own
  session outlives the wrapper across reconnects exactly as it would
  over plain ssh.
- **Does nothing about latency or jitter.** No predictive local echo,
  no input prediction, no smoothing — every keystroke pays the full
  network round-trip exactly as with plain ssh. If your link is laggy,
  it'll *feel* laggy. This tool only solves the disconnect-and-reconnect
  problem.
- **No new ports.** Strictly piggybacks on the existing ssh transport.
  No TCP or UDP listener is opened by this wrapper at either end.
- **No stdin replay.** Anything you type during a disconnect is dropped
  on the floor; it is *not* re-sent to the remote when the link comes
  back. Only the remote shell's *output* is preserved and replayed.
- **Don't use X11 forwarding (or agent / port forwarding) with this
  wrapper, it won't survive restarts.**
- **No MOTD / login banner.** The message-of-the-day you'd normally see
  on `ssh host` is not printed. It isn't `-T` (no-PTY) that suppresses
  it — it's that the wrapper hands ssh a *remote command*
  (`ssh -T … xssh attach …`), so sshd runs a non-interactive exec rather
  than a login shell. sshd's `PrintMotd` path only fires for an
  interactive login shell, and PAM's `pam_motd` is likewise out of the
  picture: the daemon spawns your login shell directly (no `login(1)`/PAM
  in the chain). Even if either *did* emit, the bytes would land on the
  stdout of `xssh attach`, which the client consumes as its wire protocol
  — they'd corrupt the stream, not render in your terminal. If you rely on
  the MOTD, read it some other way (e.g. `cat /etc/motd` / `cat
  /run/motd.dynamic` once connected).

## Build & install

### Build dependencies (Ubuntu)

The project is pure Go — no cgo, no system C libraries — so cross-compiling
to ARM (Raspberry Pi) works out of the box without needing a cross-gcc
toolchain. You just need Go ≥ 1.22 and `make`.

On **Ubuntu 24.04 or newer**, the apt-packaged Go is recent enough:

```
sudo apt install golang-go make openssh-client
```

On **older Ubuntu releases** the apt-packaged Go is too old. Grab the
latest Go tarball from <https://go.dev/dl/> and put it in your PATH:

```
sudo apt install make openssh-client
# Then follow https://go.dev/doc/install
```

(`openssh-client` provides `ssh` and `scp`, which the wrapper and the
`make deploy*` targets shell out to.)

### Quick install (no clone)

Once Go is installed you can fetch and build directly with `go install`:

```
go install github.com/zziigguurraatt/continuous-ssh/cmd/xssh@latest
```

This drops the `xssh` binary in `$(go env GOBIN)` (which falls back to
`$GOPATH/bin`, usually `~/go/bin/`). Make sure that's in your `PATH`.

`go install` only produces a native binary; if you want a cross-compiled
build (e.g. for a Raspberry Pi), clone the repo and use the Makefile
targets below.

### Builds (from a clone)

A `Makefile` is provided. Run `make help` to list every target with a
short description:

```
$ make help

Usage:
  make <target>

General
  help               Display this help.

Build
  build              Build native binary into bin/xssh.
  pi64               Cross-compile for Raspberry Pi 3/4/5 (64-bit OS) into bin/xssh-arm64.
  pi32               Cross-compile for Pi 2 / Pi 3 (32-bit OS) into bin/xssh-armv7.
  pi-zero            Cross-compile for Pi 1 / Pi Zero (ARMv6) into bin/xssh-armv6.
  clean              Remove built binaries.

Install (local)
  install-user       Install xssh + shell completions into per-user locations (no sudo).
  install-system     Install xssh + shell completions into system locations (requires sudo).
  uninstall-user     Remove xssh + completions from per-user locations.
  uninstall-system   Remove xssh + completions from system locations (requires sudo).

Deploy (remote)
  deploy             Native build → scp to $(HOST):$(REMOTE_PATH)xssh.
  deploy-pi64        ARM64 cross-build → scp to $(HOST):$(REMOTE_PATH)xssh.
  deploy-pi32        ARMv7 cross-build → scp to $(HOST):$(REMOTE_PATH)xssh.
  deploy-pi-zero    ARMv6 cross-build → scp to $(HOST):$(REMOTE_PATH)xssh.
```

Native build:

```
make            # builds bin/xssh
```

Install locally — pick one:

```
make install-user      # → $GOBIN/xssh (same place `go install` uses; no sudo)
make install-system    # → /usr/local/bin/xssh                 (requires sudo)
```

If `GOBIN` isn't set, `make install-user` falls back to `$(go env GOPATH)/bin`,
which is typically `~/go/bin/`. Override with `make install-user
USER_BIN=$HOME/.local/bin` (or any other path).

### Tab completion

Both `make install-user` and `make install-system` also drop shell
completion scripts so `xssh <TAB>` works just like `ssh <TAB>`:
hostnames pulled from `~/.ssh/config`, `~/.ssh/known_hosts`,
`/etc/hosts`, plus flag completion. We do this by delegating to your
shell's existing `ssh` completer, so any improvements to that
completer apply to `xssh` automatically.

| Shell | Install path (user)                                 | Install path (system)                       |
|-------|-----------------------------------------------------|---------------------------------------------|
| bash  | `~/.local/share/bash-completion/completions/xssh`   | `/usr/share/bash-completion/completions/xssh` |
| zsh   | `~/.local/share/zsh/site-functions/_xssh`           | `/usr/share/zsh/site-functions/_xssh`         |

Bash picks up the user-local directory automatically as long as the
`bash-completion` package is installed (Ubuntu:
`apt install bash-completion`).

Zsh requires the `_xssh` file's parent directory to be in `$fpath`
**before** `compinit` runs. Add this to `~/.zshrc` above your `compinit`
call if you used `install-user`:

```
fpath=(~/.local/share/zsh/site-functions $fpath)
```

The system path is in zsh's default `$fpath` and needs no extra setup.

After installing, start a fresh shell (or `exec bash` / `exec zsh`) to
load the completion.

If you installed via `go install` instead of the Makefile, the
completion scripts aren't shipped automatically. Copy them out of a
clone:

```
git clone https://github.com/zziigguurraatt/continuous-ssh
install -m 0644 continuous-ssh/completions/bash/xssh ~/.local/share/bash-completion/completions/xssh
install -m 0644 continuous-ssh/completions/zsh/_xssh ~/.local/share/zsh/site-functions/_xssh
```

### Remote deploy

The `deploy*` targets build (cross-compiling when needed) and then `scp`
the resulting binary onto a remote host. They exist because every change
to the wrapper has to land on *both* sides — the local client and the
remote (where attach + daemon run) — and running build + scp by hand
gets repetitive during development.

```
make deploy        HOST=user@host        # native x86_64 build
make deploy-pi64   HOST=pi@pi.local      # Pi 3 / 4 / 5 (64-bit OS)
make deploy-pi32   HOST=pi@pi.local      # Pi 2 / Pi 3 (32-bit OS)
make deploy-pi-zero HOST=pi@pi.local     # Pi 1 / Pi Zero (ARMv6)
```

How each target works under the hood:

1. **Builds** the appropriate architecture into `bin/xssh*` (e.g.,
   `bin/xssh-arm64`). Cross-compilation is pure-Go — no cross-gcc
   needed.
2. **scp**s that binary onto `$(HOST)` as `$(REMOTE_PATH)xssh`.
   `REMOTE_PATH` is chosen based on the host string:
   - `HOST=root@…` → `/usr/local/bin/` (system install on the remote;
     only root can write there)
   - any other user → `~/go/bin/` (matches the local `USER_BIN`
     convention — same place `go install` drops things when `GOBIN`
     isn't set; no sudo, and already in the remote-PATH prefix the
     client uses to invoke `xssh attach`)
3. Uses your existing ssh config, agent, and keys — same as any other
   `scp`/`ssh` invocation. Pass `HOST=alias` and any settings in
   `~/.ssh/config` (`Port`, `IdentityFile`, `ProxyJump`, etc.) apply
   automatically.

Common overrides:

```
# Force a specific install path on the remote
make deploy HOST=user@host REMOTE_PATH=/opt/bin/

# Push the cross-compiled ARM64 binary to a Pi
make deploy-pi64 HOST=pi@pi.local
```

**Both sides need the same binary version** — the local client and the
remote attach + daemon all live in one executable, dispatched by
subcommand. After a `make deploy*`, also `make install-user` (or
`install-system`) locally so your local copy stays in sync.

The local client invokes the remote binary by name (`xssh attach …`)
and **prepends a handful of common install locations to the remote
`PATH`** before running it, so the remote shell finds `xssh` whether
it was installed system-wide or user-local. The static list is
`~/bin/`, `~/.local/bin/`, `~/go/bin/`. On top of that, if the
remote has Go installed the client dynamically appends its effective
Go bindir — `go env GOBIN`, or `$(go env GOPATH)/bin` when GOBIN is
unset — so a binary placed at a custom GOBIN or non-default GOPATH
by the auto-install feature (or `make deploy`) is still discoverable:

```
PATH="$PATH:$HOME/bin:$HOME/.local/bin:$HOME/go/bin$(command -v go >/dev/null 2>&1 && { g=$(go env GOBIN); g=${g:-$(go env GOPATH)/bin}; printf ':%s' "$g"; })" xssh attach …
```

So you don't have to add `~/bin/` (or `~/go/bin/`, or your custom
GOBIN) to the remote account's shell rc just to make `xssh`
discoverable when sshd starts a non-login shell.

## Usage

```
$ xssh -h
xssh — continuous-ssh: interactive SSH that survives disconnects.

Usage:
  xssh [flags] [ssh-args...] <target>

The remote always runs your login shell. ssh-args (flags + target) are
forwarded verbatim to the system ssh binary. On disconnect the wrapper
reconnects silently and retries forever until the remote delivers an
EXIT frame or you abort with `~.`.

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
  --session ID  reconnect to an existing session by id instead of
                starting a new one. Useful after a previous xssh
                invocation exited with code 137 (auth, host key, etc.):
                the remote daemon is still running, so this flag lets
                you reattach after fixing the underlying problem.
                Alt-screen continuity is preserved across reattach:
                if the foreground program (vim, htop, …) is in
                alt-screen mode, the local terminal enters alt-screen
                and a Ctrl-L is sent so the program redraws cleanly
                (Ctrl-L is byte 0x0C, the conventional "redraw the
                screen" keystroke that vim/htop/less and most TUI
                programs interpret as a repaint request — once the
                local terminal is in alt-screen, the redrawn content
                lands in the right buffer).

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
  daemon   run a session daemon
```

ssh-args (flags + target) are passed through verbatim to the system
`ssh` binary.

Stdin must be a TTY. Piped or redirected stdin is rejected at startup.

The remote shell runs attached to a PTY, so interactive programs
(`vim`, `htop`, etc.) behave exactly as they would over plain ssh.
stdout and stderr from the remote are merged into a single stream by
the PTY itself — same as `ssh user@host` and unlike `ssh -T user@host`.

## Auto-install on missing remote binary

When the connection fails because the remote shell can't find `xssh`
on `PATH` (`bash: xssh: command not found` and similar), the client
detects the pattern and offers to push a copy of the local binary
over.

The remote is probed once for `id -u`, `uname -s`/`uname -m`,
`$HOME`, and its preferred Go bindir (using the same
`GOBIN`/`GOPATH/bin`/`$HOME/go/bin` cascade as the `Makefile`'s
`do_deploy` recipe).

The default install path follows the same convention as
`make deploy`:

- `root` on the remote → `/usr/local/bin/xssh`
- non-root → the remote's `go env GOBIN` (else `GOPATH/bin`, else
  `$HOME/go/bin`)

The prompt accepts:

- `Y` / `yes` / empty → install at the default path
- `n` / `no` → cancel and exit
- any path → install at that path instead (e.g. `/opt/bin/xssh`)

Architecture is verified before any bytes are pushed — if the local
binary is `linux/amd64` but the remote reports `linux/arm64`, the
push is refused and the user is referred to manually using the
`make pi64`/`pi32`/`pi-zero` cross-compile targets.

On successful push the original connection is retried automatically.

## Key sequences

At the **start of a line** (immediately after a newline, or as the first
input):

| Sequence | Effect |
|----------|--------|
| `~.`     | Abort the wrapper. Sends a SHUTDOWN frame so the remote daemon kills the shell and removes the session directory. Prints `Connection aborted.` and exits with status `130`. |
| `~~`     | Sends a single literal `~` to the remote. |

Tildes anywhere other than the start of a line are forwarded unchanged.
Same convention as the underlying `ssh` client (which is bypassed by
this wrapper).

## Initial connect

Until the first `HELLO_ACK` lands, the local terminal stays in "cooked"
mode. **Ctrl-C** during this window exits the client cleanly — useful
if you don't want to wait for ssh's own `ConnectTimeout` /
`ConnectionAttempts` to expire, or to cancel an auth prompt.
If the first connect fails for any reason, the wrapper does **not** retry —
it prints ssh's own stderr (`Permission denied (publickey)`, `Could
not resolve hostname …`, etc.) and exits with status 1. Silent
retries here would mask real problems like a wrong host, missing
remote `xssh` binary, or auth failure. Once the first connection is
established the terminal switches to raw mode and Ctrl-C is
forwarded to the remote shell like any other keystroke; from then
on, `~.` is the only way out.

## Debug mode

Three verbosity levels, controlled by three mutually-exclusive flags
on the client. The level is propagated through the chain (client →
remote attach → daemon) automatically.

| Level | Flag | What's logged |
|-------|------|--------------|
| 0 (default) | (none) | Errors and event messages only — start/exit lines, signals, disk-cap shutdowns. A few lines per session. |
| 1 (verbose) | `--debug` | Above + session-level events: protocol negotiation, reconnects, ACK trims as a whole, signal handlers, buffer-state heartbeat every 10 s. Tens of lines per session. Also mirrors the log to stderr (CR-LF translated so it stays readable while the local terminal is in raw mode). |
| 1 (verbose) | `--debug-file` | Same level as `--debug`, but no stderr mirror. The log path is printed to stderr once on startup so you can `tail -f` it from another shell. |
| 2 (trace) | `--trace-file` | Above + per-frame chatter: every OUT/IN frame, every ACK sent, overlap drops. Thousands of lines per session under load. File-only (no stderr mirror — would flood the terminal). |

Log files:

- Client → `~/.continuous-ssh/clients/<date>-<target>-<pid>.log` (one
  file per invocation, so concurrent or back-to-back invocations
  don't interleave their output; only created when a debug/trace
  flag is set).
- Attach → `~/.continuous-ssh/sessions/<id>/attach.log` (file only —
  stderr would surface to the user terminal via ssh).
- Daemon → `~/.continuous-ssh/sessions/<id>/daemon.log` (file only).

ssh's own stderr (normally discarded) is captured into the
per-invocation client log when any debug/trace flag is set — useful
when diagnosing ssh-layer issues like authentication failures.

Without any debug/trace flag the remote attach/daemon log files are
still created but contain only error/event lines (the daemon-side
files live inside the session directory which gets cleaned up when
the session ends, so they don't accumulate).

## Reconnect behaviour

- **Disconnect detection** is left to ssh's own settings. Set
  `ServerAliveInterval` / `ServerAliveCountMax` in `~/.ssh/config` (or
  pass `-o ServerAliveInterval=N -o ServerAliveCountMax=M` on the command
  line) to tune how quickly a dead link is noticed.
- On a detected disconnect the wrapper retries silently — no banner, no
  log line — with a 500 ms backoff between attempts.
- **Reconnects retry forever.** Once a session has been established,
  the wrapper keeps trying to reconnect until the user aborts with
  `~.` or the remote delivers an EXIT frame (clean shell exit, replay
  done, disk-cap shutdown, etc.). This lets a session survive
  arbitrarily long network gaps — laptop suspended overnight, roaming
  between networks, waiting to join wifi at a new location. When the
  link finally comes back, the usual outcome is that the daemon is
  still running and the session resumes transparently. If instead the
  daemon is gone but a `clean` marker is on disk (host rebooted
  between sleep and resume, etc.), the replay daemon spawned by
  `attach` serves the preserved buffer and delivers EXIT(133) (or
  EXIT(134) for a disk-cap shutdown) which terminates the loop. If
  segments are on disk but the `clean` marker is missing (hard kill
  / power outage path), the replay daemon refuses with EXIT(130).
  If nothing is left to recover at all (someone ran `xssh rm` while
  the client was retrying), `attach` sends EXIT(136) on the next
  reconnect so the client prints a clear "session no longer exists"
  message and exits without looping.
- **The retry-forever loop is narrow on purpose.** It only retries
  when the failure looks like a TCP-level transient — ssh stderr
  matching `ssh: connect to host …` (connect refused, network
  unreachable, timed out) or `ssh: Could not resolve hostname …`
  (DNS blip) — OR when the attempt got as far as HELLO_ACK and
  died mid-session (the daemon is alive; just a blip). Anything
  else (auth failure, host key change, `xssh` binary missing on
  the remote, etc.) surfaces ssh's own stderr and exits 1 instead
  of looping forever silently.
- Window-size changes (`SIGWINCH`) propagate to the remote PTY via a
  `RESIZE` frame; the initial size is sent right after `HELLO_ACK`.
- **Wake-detect link probe.** When the laptop wakes from sleep, the
  local TCP socket is still `ESTABLISHED` from the kernel's
  perspective even if the link silently died during sleep (NAT entry
  evicted, etc.). Without a probe, the user would have to type a
  keystroke to provoke a TCP-level error and trigger reconnect. A
  background goroutine compares the wall clock against the monotonic
  clock once a second; if wall-time jumps forward 20 s or more
  between ticks (the only normal cause is suspend-to-RAM), a `Ping`
  frame is sent over the active session. If a `Pong` arrives within
  10 s the link is healthy and nothing else happens; if the timeout
  expires, the ssh subprocess is killed and the reconnect loop takes
  over. Ping/Pong are sent **only** on a detected wake, never as a
  continuous keepalive — ssh's own `ServerAliveInterval` is the
  right knob for that.

### Output buffering & reconciliation

The whole point of the wrapper is that the local terminal eventually
sees every byte the remote shell produced, even across disconnects.
The two sides cooperate via an ACK-purged ring on the client and a
correspondingly-sized buffer on the daemon.

**Buffer layout (same shape on both sides).** Each side conceptually
holds the output stream as a sequence of fixed-size hashed chunks
plus an unhashed trailing tail:

```
  chunk 0       chunk 1            chunk N-1    partial tail
+---------+   +---------+        +---------+   +----------+
|  1 MiB  |   |  1 MiB  |  ...   |  1 MiB  |   |  < 1 MiB |
|  hashed |   |  hashed |        |  hashed |   |  unhashed|
+---------+   +---------+        +---------+   +----------+
```

Each completed 1 MiB chunk has its SHA-256 computed incrementally as
bytes flow in; the partial tail (anything past the last 1 MiB
boundary) is unhashed because it isn't 1 MiB yet. The two sides
*store* this layout differently: the daemon keeps the newest 10 MiB
in RAM (≈ the last 10 chunks) and spills older chunks to disk; the
client keeps the newest 10 MiB in RAM and lets older chunks fall
off the back.

**Client side: 10 MiB RAM, no disk.** The client's chunk hash list
keeps growing as new chunks are completed, so the manifest sent in
`HELLO` always covers the full byte history — but only the trailing
10 MiB of actual content lives in RAM, and there is no on-disk copy.
A monotonic byte counter (`outputBuf.Len()`) tracks the cumulative
total received.

**Daemon side: 10 MiB RAM tail + segmented disk spill.** The daemon
holds unacked output: the newest 10 MiB in RAM, the rest spilled to
a sequence of 10 MiB segment files in
`~/.continuous-ssh/sessions/<id>/output.log.<startOff>`. As ACKs
arrive, `TrimTo(N)` deletes any segment whose entire range falls
below N — so disk usage tracks **unacked** held bytes, not
cumulative output. The host-wide DiskBudget (below) is therefore
really "how much output can pile up across all sessions during
their disconnects", not "how much output the sessions can produce
in total". There is no per-session synchronous cap; DiskBudget is
the only ceiling.

**ACK-based purge during a healthy connection.** As the client
displays output it sends `Ack(N)` frames back to the daemon (where N
is its current `outputBuf.Len()`) under two triggers: every 5 MiB of
newly-displayed bytes (the size trigger, which throttles ACK rate
during fast output), and every 1 s of idle time with any unacked
bytes pending (the time trigger, which prevents low-rate streams
like log tails from accumulating on the daemon side forever). The
daemon's `readUpstream` calls `outputBuf.TrimTo(N)` on each ACK —
bytes below N are dropped from RAM (and unreferenced on disk). In
steady state the daemon's held buffer stays under ~5–10 MiB.

**Disconnect → reconnect.** During a disconnect the daemon stops
receiving ACKs but the shell keeps producing output. The daemon's
held buffer grows; the disk-cap sweeper bounds long-term growth
across all sessions. When the client reconnects,
it sends `HELLO` with its current total + the SHA-256 hash list of
every complete 1 MiB chunk it has hashed. The daemon:

1. Compares chunk manifests to find the first divergent chunk; sets
   `resendFrom` to that chunk's start (or, if all match, to the
   start of its own last complete chunk — so the trailing 1 MiB is
   always resent).
2. Clamps `resendFrom` to its own `TrimOffset()`, because bytes
   below the trim point have been freed and are no longer in the
   buffer.
3. Streams `[resendFrom, daemonTotal)` over `OUTPUT` frames.

The client deduplicates incoming bytes by offset
(`handleOutputFrame`): anything below its current `Len()` is
silently dropped; the new tail is written to the terminal and
appended to the RAM window.

The "always resend the last complete chunk" rule covers the
**ragged-edge** case — when a connection dropped mid-chunk and the
client got a partial chunk N. There's no cheap way to know exactly
where the cut happened, so we just resend chunk N + the trailing
partial in full. The client's byte-offset dedup quietly discards the
overlap.

### Global disk cap

A host-wide budget shared across sessions, so a fleet of growing
sessions can't fill the disk:

```
DiskBudget = min(2 GiB, 20% × free_disk)
           − min(100 MiB, 5% × 20% × free_disk) × N_growing
```

`N_growing` counts sessions in `active` or `stale` status — dead
sessions don't grow and so don't reserve. The reserve per session
caps at 100 MiB on a roomy disk but scales down (5% of the raw
20% budget) on tight disks so the per-session reserve can never
dominate the budget itself.

Each daemon's 60 s sweeper shuts the daemon down when the sum of
all sessions' disk usage exceeds DiskBudget AND this session is
above its fair share. Evaluated locally with no coordination
between daemons, fast growers volunteer first; sessions already
under their share keep running. The rule:

```
self-shutdown iff:
  sum_of_all_session_disk > DiskBudget
  AND
  my_size > DiskBudget / N_growing
```

A shutdown via this path preserves on exit: segments stay on disk,
a `diskcap` marker file is written, and the next reconnect spawns
a replay daemon that streams what was buffered, delivers EXIT 134,
and removes the directory. The disk isn't reclaimed until that
reconnect (or an explicit `xssh rm`).

A new session (`xssh user@host` on a host already at or above
DiskBudget) is refused up-front. The remote `attach` writes an
EXIT 135 frame in place of HELLO_ACK; the client prints "cap
reached" and exits without retrying.

A two-line banner is appended to the output stream at session
start — the session id and the current disk-cap utilization:

```
continuous-ssh: session ab12cd34ef56789012ab34cd56ef7890
continuous-ssh: total buffer disk usage: 21.3 MB (1%) of 1.7 GB (DiskBudget)
```

Because the banner is part of the session's byte stream, it lands
once at offset 0 and isn't re-injected on reconnects (the client
already has those bytes).

### Tunable parameters

All of these are compile-time constants in the Go source — there are
no runtime flags or config files. Listed here so the trade-offs are
visible in one place.

| Parameter | Value | Defined in | What it controls |
|-----------|-------|------------|------------------|
| `diskCapAbsolute` | **2 GiB** | `internal/sessions/diskcap.go` | Hard ceiling on `min(2 GiB, 20% × free)` — the raw budget before the per-session reserve. |
| `diskCapFreeRatio` | **20%** | `internal/sessions/diskcap.go` | Fraction of free-disk that bounds the budget when the disk has less than 10 GiB free. |
| `diskCapReserveMax` | **100 MiB** | `internal/sessions/diskcap.go` | Upper bound on the per-growing-session subtraction from DiskBudget. |
| `diskCapReserveSubRatio` | **5%** | `internal/sessions/diskcap.go` | Fraction of the raw 20% budget used as the per-session reserve when the disk is small enough that 100 MiB would dominate. |
| `diskCapSweepInterval` | **60 s** | `internal/daemon/daemon.go` | How often each daemon evaluates the DiskBudget rule and decides whether to self-shutdown. |
| `DefaultRAMTail` | **10 MiB** | `internal/buffer/buffer.go` | Daemon's in-RAM window; older held bytes spill to segment files. |
| `DefaultSegmentSize` | **10 MiB** | `internal/buffer/buffer.go` | Size of each on-disk segment file. Rotated when full; deleted when fully trimmed by ACKs. |
| `clientBufRAM` | **10 MiB** | `internal/client/client.go` | Client's RAM-only sliding window; older bytes drop off the back (local terminal scrollback is the real history). |
| `DefaultChunkSize` | **1 MiB** | `internal/buffer/buffer.go` | Granularity of SHA-256 chunk hashes and the "always resend last complete chunk" rule for ragged-edge reconnects. |
| `ackInterval` | **5 MiB** | `internal/client/client.go` | Size trigger — client emits an ACK after this many newly-displayed bytes since the last ACK. |
| `ackIdleMax` | **1 s** | `internal/client/client.go` | Time trigger — client emits an ACK after this long with any unacked bytes pending. Keeps low-rate streams from accumulating on the daemon. |
| reconnect backoff | **500 ms** | `internal/client/client.go` | Wait between reconnect attempts. |
| `keepAliveGrace` | **30 s** | `internal/daemon/daemon.go` | After client disconnect, daemon keeps its listener open this long before closing it (so a quick reconnect doesn't lose the socket). |
| `wakeSleepThreshold` | **20 s** | `internal/client/client.go` | Wall-vs-monotonic skew that counts as a "we slept" event. Triggers the wake-detect Ping. |
| `wakePongTimeout` | **10 s** | `internal/client/client.go` | How long the wake-detect probe waits for a Pong before declaring the link dead and force-reconnecting. |
| SHUTDOWN drain | **2 s** | `internal/client/client.go` | After `~.`, client waits this long for ssh to deliver the SHUTDOWN frame before killing it. |

## Remote state

For each live session the daemon keeps state under
`~/.continuous-ssh/sessions/<id>/`:

```
sock          Unix socket the daemon listens on
pid           daemon PID
info          session metadata (start time)
output.log.<off>  disk spill — one or more 10 MiB segment files, deleted as
              ACKs advance past their end. Each name encodes the byte
              offset of its first byte (zero-padded to 20 digits).
daemon.log   daemon log file
attach.log   attach log file
clean         written only when the daemon shut down with a complete flush
diskcap       written next to `clean` when the daemon stopped because the
              host-wide disk cap was exceeded; replay delivers EXIT(134).
```

What the daemon does with this directory on exit depends on **why** it's
exiting, and (for the signal case) on whether a client is connected:

| Exit cause | Disk action |
|-----------|-------------|
| Remote shell exits cleanly (`exit`) | Whole directory removed. |
| Client sends `SHUTDOWN` (user hit `~.`) | Whole directory removed. |
| `SIGTERM`/`SIGINT`/`SIGHUP` *with* an active client | Daemon kills the remote shell, drains the buffer to the client, sends `EXIT(129)`. Client exits cleanly. Whole directory removed — the client has everything. |
| `SIGTERM`/`SIGINT`/`SIGHUP` *without* an active client | RAM tail flushed to a fresh segment; `sock`/`pid`/`info` removed; segment files + `clean` marker kept for the next reconnect to replay. |
| Host-wide disk cap exceeded (this session above fair share) | Preserved like the no-client signal case; a `diskcap` marker is dropped alongside `clean` so the replay daemon delivers EXIT(134). |
| Hard kill (`SIGKILL`, OOM, power loss) | Daemon never gets to flush. Segments are whatever spilled before death; **no `clean` marker** is written. |

## Recovery / replay

If the local client reconnects to a session whose daemon is gone (such
as due to a reboot of the remote machine) but whose session directory
still exists, the local-side `attach` silently spawns a **replay
daemon** on the remote. The replay daemon serves whatever's on disk to
exactly one attaching client, then removes the session directory and
exits.

Replay only succeeds when the `clean` marker is present — i.e. the
previous daemon shut down through one of the rows above marked "RAM
tail flushed to disk". If the marker is absent (hard-kill path), the
replay daemon refuses to serve and the client prints:

```
continuous-ssh: session was not cleanly shut down; recovery aborted.
```

A missing `clean` marker on a session whose daemon is gone usually
means the remote took an **unexpected power outage** (or some other
abrupt kill — kernel OOM, the VM being yanked, etc.) — the daemon
catches all the normal termination signals and writes the marker on
its way out, so its absence implies the daemon never got the chance.

The on-disk buffer is *left in place* in that case so you can grab it
by hand (e.g. `scp -r host:~/.continuous-ssh/sessions/<id> .` —
segment files concatenate in name order to reconstruct the stream).

When replay succeeds (clean marker present), the client prints:

```
continuous-ssh: remote daemon stopped while disconnected; buffered output replayed.
```

…and exits with status 133 (or 134 if a `diskcap` marker was alongside
the `clean` marker). The session id is now dead; further attempts to
reconnect to it would find nothing on disk.

## Managing sessions on the remote

Two subcommands run on the host where the daemon lives — useful for
inspecting and cleaning up leftover sessions.

### `xssh ls`

List every session directory under `~/.continuous-ssh/sessions/`:

```
$ xssh ls
  SESSION ID                          STATUS   PID      DISK BUF         LAST CHANGE              STARTED
* ab12cd34ef56789012ab34cd56ef7890    active   12345           0 (  0%)  connected    (12m ago)   2026-06-25 14:02 (12m ago)
  98fe76dc54ba32108765edcb432109af    stale    12380     21.0 MB (  1%)  disconnected ( 5m ago)   2026-06-25 14:09 ( 5m ago)
  deadc0de87654321fedcba0987654321    dead     -        100.0 MB (  6%)  disconnected (44m ago)   2026-06-25 13:30 (44m ago)

Total buffer disk usage: 121.0 MB (  7%) of   1.7 GB (DiskBudget)
Total disk usage:         50.0 GB ( 50%) of 100.0 GB (Total Disk)
```

Columns:

- **Leading `*`**: marks the session this command is being run inside,
  if any. Detected via the `XSSH_SESSION` environment variable that the
  daemon exports into the spawned shell.
- **SESSION ID**: directory name under `~/.continuous-ssh/sessions/`.
- **STATUS**: see table below.
- **PID**: daemon PID, or `-` for dead sessions.
- **DISK BUF**: total bytes across all `output.log.<offset>` segment
  files, followed by what percentage of DiskBudget that occupies.
  Normally `0` for a healthy active session (RAM tail isn't
  exceeded). Grows during a disconnect; shrinks again on reconnect as
  ACKs trim segments away. The percentage reflects the same
  denominator as the daemon's startup banner, so the numbers line up
  with the sweep rule.
- **LAST CHANGE**: the most recent attach/detach event and how long
  ago it happened. `connected (X ago)` for an active session means
  the current client connected X ago and is still attached;
  `disconnected (X ago)` for stale or dead means the session has been
  without a client for that long. The same `(X ago)` parenthesised
  style as the STARTED column.
- **STARTED**: absolute timestamp + relative age of the session
  directory.

Statuses:

| Status    | Daemon process | Client connected | Meaning |
|-----------|----------------|------------------|---------|
| `active`  | running        | yes              | A client is currently attached and exchanging frames. |
| `catchup` | running        | yes              | Daemon has a meaningful unacked backlog — either draining a reconnect gap (transient) or in chronic backpressure because the producer is outpacing the link (persistent). Hysteresis: enters when held bytes cross above 2 MiB, exits only when they drop below 1 MiB. Independent of ACK activity; a fast-producer session with normal ACK flow can stay in `catchup` for as long as the backlog persists. |
| `stalled` | running        | yes              | No ACK from the current attach for ≥30 s. The client's `ackIdleMax` of 1 s means ACKs normally flow continuously during output, so any 30-s silence is abnormal — typically a peer that's gone quiet on a socket the OS hasn't yet torn down. Takes precedence over `catchup` when both would apply. Cleared on any incoming ACK. |
| `replay`  | running (as replay daemon) | yes | Original daemon is gone; a replay daemon (spawned by `attach` for a reconnecting client) is currently streaming preserved segments. Transient — usually disappears within seconds of the client finishing the replay. |
| `stale`   | running        | no               | Daemon is idle, waiting for a reconnect. |
| `dead`    | gone           | n/a              | Session directory persists but the daemon process is no longer around. Eligible for replay-on-reconnect if a `clean` marker is present. |

Filters: `--active`, `--stale`, `--dead`, or `--all` (default). Combine
freely (e.g. `xssh ls --active --stale`).

### `xssh kill` vs `xssh rm`

Two clearly separated operations:

| Command | What it does | On-disk data |
|---------|--------------|---------------|
| `xssh kill` | Sends `SIGTERM` to the daemon. Active clients see `EXIT(129)` and print `continuous-ssh: remote daemon stopped.` | **Always preserved.** After `kill` the session shows up as `dead` in `xssh ls`. |
| `xssh rm` (no `--kill`) | **Refuses** to act on a live daemon. Use on dead sessions to GC the leftover directory. | **Removed** (dead session dirs only). |
| `xssh rm --kill` | `SIGTERM`, wait up to 3 s, `SIGKILL` if needed, then `RemoveAll` — equivalent to `xssh kill` immediately followed by `xssh rm`. | **Removed.** |

Both share the same selectors:

```
xssh kill <id> | --all | --active | --stale | --dead
xssh rm   [--kill] <id> | --all | --active | --stale | --dead
```

Examples:

```
# Stop in-progress connections cleanly (clients see EXIT), keep all data:
xssh kill --active

# Stop idle daemons but preserve their buffers for later replay:
xssh kill --stale

# Clean up directories whose daemons are already gone:
xssh rm --dead

# Drop the preserved buffers of stale sessions in one step:
xssh rm --kill --stale

# Stop everything AND wipe everything:
xssh rm --kill --all
```

The two-step idiom — politely end first, GC later — is
`xssh kill --active` followed at some later point by `xssh rm --dead`.
The one-step shortcut for the same effect is `xssh rm --kill --active`.

## Protocol versioning

The wire protocol carried inside the SSH transport has an explicit
`major.minor` version exchanged in HELLO/HELLO_ACK at session start.
Current version is **`1.2`** (see `ProtocolMajor`/`ProtocolMinor` in
`internal/proto/frame.go`). Version history:

- `1.0` → `1.1` added a trailing alt-screen state byte to HELLO_ACK
  (older peers omit it; decoders default to false).
- `1.1` → `1.2` added `Ping` and `Pong` frame types for the
  wake-detect liveness probe (older daemons that don't recognise
  Ping just log "ignoring frame" and never send Pong; the client
  treats that as a probe timeout and force-reconnects, gracefully
  degrading to "always reconnect on wake").

Compatibility rule:

- **Same major** → compatible. Minor differences are accepted
  silently; the session proceeds normally. With `--debug` or
  `--debug-file` on, both sides log a one-line note (`protocol
  negotiated: local=X.Y remote=A.B` on match, or `protocol minor
  differs: …` when minors differ).
- **Different major** → fatal. Client prints:
  ```
  continuous-ssh: incompatible protocol (local=X.Y, remote=A.B).
  Re-deploy the matching xssh binary to the remote.
  ```
  and exits with status **132**. The reconnect retry loop is skipped
  — no amount of retrying fixes a mismatched binary.

When to bump which:

- **Major** (`1.0` → `2.0`): wire-format changes, removed or
  semantically-changed frame types, or any change that breaks how
  the peer interprets existing frames. Forces re-deploying both
  sides.
- **Minor** (`1.0` → `1.1`): additive backward-compatible changes —
  new optional frame types, new exit codes the peer can safely
  ignore, new fields appended at the end of an existing payload.

Both sides check on receive. On mismatch the daemon still sends back
HELLO_ACK with its own version (so the client can read the daemon's
version and include both numbers in the error message), then closes
the connection without setting up any streams.

## Exit codes

| Code | Meaning |
|------|---------|
| `0`  | Remote shell exited cleanly (user typed `exit`, or your command finished). |
| `1`  | First-connect failure — ssh couldn't establish the initial session (wrong host, auth failure, remote `xssh` binary missing, etc). Client prints `continuous-ssh: initial connection failed` followed by ssh's own stderr. Not retried; re-run after fixing the underlying issue. |
| `129`| Remote daemon was stopped by signal while the client was connected (live signal-induced shutdown). Client prints `continuous-ssh: remote daemon stopped.` |
| `133`| Replay-recovered counterpart to 129: the daemon stopped by signal while no client was attached, and the replay daemon just streamed its preserved buffer to you. Client prints `continuous-ssh: remote daemon stopped while disconnected; buffered output replayed.` |
| `130`| User aborted with `~.` (prints `Connection aborted.`), **or** replay was refused because the previous daemon didn't shut down cleanly (prints the "not cleanly shut down" message above). |
| `132`| Protocol-version mismatch between local and remote xssh binaries. Client prints `continuous-ssh: incompatible protocol (local=X.Y, remote=A.B). Re-deploy the matching xssh binary to the remote.` Major-version differences are fatal; same-major minor differences are accepted silently. |
| `134`| Remote daemon shut itself down because the host-wide disk cap was exceeded and this session was above its fair share (typically a long disconnect with fast output). Buffer was preserved and replayed successfully. Client prints `continuous-ssh: remote daemon stopped because the host-wide disk cap was exceeded (long disconnect with fast output).` |
| `135`| New session refused before it could start: the host-wide disk cap is already at or above DiskBudget. Client prints `continuous-ssh: cannot start new session — the host-wide disk cap is reached.` |
| `136`| Reconnect refused: the remote session no longer exists (no daemon socket, no segments to replay — typically `xssh rm` ran while the client was retrying). Client prints `continuous-ssh: remote session no longer exists; nothing to reconnect to.` Stops the retry-forever loop. |
| `137`| Reconnect bailed at the ssh layer with a non-transient error — typically a class that **requires user intervention** to fix: an auth rejection (load a key, fix `authorized_keys`), a host-key mismatch (update `known_hosts`), `xssh` missing on the remote (redeploy the binary that existed before the disconnect), etc. **Not fatal to the session**: the remote daemon is presumably still running. Reattach after fixing the underlying issue with `xssh --session <id> [ssh-args...] <target>` (the id is printed in the 137 message). Client prints `continuous-ssh: reconnect failed` followed by ssh's own stderr. Distinct from a plain exit 1 so scripts can tell xssh's own no-retry decision apart from other failures. |
| other| Underlying ssh / command exit code, passed through. |

## Smoke tests

Two end-to-end scenarios exercise every buffer code path. `seq` is the
sole output generator — its output is predictable (you can see exactly
where if it ever stops) and one tool covers both tests.

**Test A — reconnect + reconcile (session continues)**

```
xssh user@host
# inside the session:
sleep 20; seq 1 5000000          # ~36 MiB once seq starts
```

The `sleep 20` gives you 20 s to switch to another shell and stage
the disable-network + `pkill` commands so they're ready to run the
moment `seq` starts. While `seq` is running, from **another local
shell**, do these four steps in order:

1. **Disable networking to the remote** (toggle WiFi off, `nmcli`,
   `iptables`, whatever's fastest).
2. **Kill the ssh subprocess**:
   ```
   pkill -P $(pgrep xssh) ssh
   ```
3. Wait 5–10 seconds (the daemon keeps producing into its buffer
   during this window; client's reconnect attempts fail because the
   network is down).
4. **Re-enable networking.**

The next reconnect attempt succeeds. The client sends `HELLO` with
its current chunk-hash manifest; the daemon compares and retransmits
everything from the first divergent chunk onward (plus the trailing
chunk, always). `seq`'s output should pick up where it left off and
complete with the final number visible.

Exercises: first spill (`total` crossing `ramTail`), sustained spill
cycles, chunk-hash reconciliation on reconnect with a non-trivial
gap, normal exit.

Notes on the recipe:
- The `pkill` is what actually triggers xssh's reconnect. Without
  `ServerAliveInterval` in `~/.ssh/config`, just disabling the link
  doesn't cause ssh to die — TCP retransmits invisibly until you
  restore the link, at which point the session resumes without ever
  going through reconnect.
- Disabling the network around the `pkill` is what makes the gap
  large enough to be a meaningful reconcile test. Without it,
  `pkill` triggers an immediate reconnect with only a few hundred
  bytes of lag.

**Test B — disk-cap shutdown + replay (destroys the session)**

```
xssh user@host
sleep 20; seq 1 250000000        # ~2.1 GiB once seq starts; trips DiskBudget
```

The `sleep 20` gives you 20 s to switch to another shell and stage
the disable-network command. As `seq` is running, **disable networking
to the remote and leave it down** long enough for the sweeper (60 s
interval) to detect that the session's disk usage has crossed its
fair-share line. The daemon writes the `diskcap` marker and exits
preserving on-disk state. Re-enable networking; the client
reconnects, the replay daemon serves the buffered output, and the
session ends with:

```
continuous-ssh: remote daemon stopped because the host-wide disk cap was exceeded.
```

and exit code `134`.

Exercises: disk-cap sweeper, `diskcap` marker, on-disk preservation,
replay daemon, `EXIT(134)` message, terminal restore on the 134 path.

**Minimal regression check**: just run **Test A**. It triggers the
case that motivated the buffer rewrite; Test B destroys the session
and verifies a code path that's hit much less often in practice.

## License

MIT — see [LICENSE](LICENSE).
