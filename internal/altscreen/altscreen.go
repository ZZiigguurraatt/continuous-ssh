// Package altscreen parses byte streams for alt-screen-buffer
// enter/exit escape sequences. Used by the client (to track whether
// the local terminal is in alt-screen at exit-cleanup time) and the
// daemon (to report current alt-screen state on HELLO_ACK so a
// `--session` reattach can resync the local terminal cleanly).
//
// Recognised sequences (all DEC private modes — note the leading "?"):
//
//	\e[?47h   /  \e[?47l
//	\e[?1047h /  \e[?1047l
//	\e[?1049h /  \e[?1049l   (the modern, ncurses-default form)
//
// Multi-parameter forms like "\e[?25;1049h" are also handled.
package altscreen

import "strings"

// Scanner is a one-direction byte-stream state machine that emits
// 'h' / 'l' events when it sees an alt-screen enter / exit
// sequence. State persists across Scan calls so a sequence split
// across two writes is still recognised correctly. Not safe for
// concurrent use.
type Scanner struct {
	state int    // 0 idle, 1 saw ESC, 2 saw [, 3 saw ?, accumulating digits
	parm  []byte // accumulated digits + ';' separators in state 3
}

// Scan feeds bytes through the state machine. The returned event
// byte is 'h' (entered alt-screen) or 'l' (exited alt-screen) for
// the *last* such transition observed in p, or 0 if none were
// observed.
func (a *Scanner) Scan(p []byte) byte {
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
				for _, q := range strings.Split(string(a.parm), ";") {
					if q == "47" || q == "1047" || q == "1049" {
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
