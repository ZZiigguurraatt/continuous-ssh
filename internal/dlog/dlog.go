// Package dlog is a process-wide debug logger with three verbosity
// levels. Writes go to a file (always, when configured) and optionally
// to an stderr mirror.
//
//	LevelError   — always logs. Bookend events, errors, signals.
//	LevelVerbose — adds session-level events: protocol negotiation,
//	               reconnects, signal handlers, ACK trims as a whole,
//	               buffer-state heartbeats every 10 s.
//	LevelTrace   — adds per-frame chatter: every OUT/IN frame, every
//	               ACK sent, every overlap drop. High volume —
//	               thousands of lines per session.
package dlog

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
)

// Level constants. Higher means more verbose.
const (
	LevelError   = 0
	LevelVerbose = 1
	LevelTrace   = 2
)

var (
	level  atomic.Int32
	logger atomic.Pointer[log.Logger]
)

// Setup configures the global logger. filePath, if non-empty, is opened
// O_APPEND|O_CREATE and used as the primary sink. stderrSink (typically
// os.Stderr or a CR/LF wrapper) is added when non-nil.
//
// lvl selects which of E/V/T calls actually emit; see Level constants.
// Calling Setup with lvl=LevelError makes the logger near-silent (only
// E lines write).
//
// Safe to call multiple times; later calls replace prior configuration.
func Setup(filePath string, lvl int, stderrSink io.Writer) error {
	var sinks []io.Writer
	if filePath != "" {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("dlog: open %s: %w", filePath, err)
		}
		sinks = append(sinks, f)
	}
	if stderrSink != nil {
		sinks = append(sinks, stderrSink)
	}
	var out io.Writer
	switch len(sinks) {
	case 0:
		out = io.Discard
	case 1:
		out = sinks[0]
	default:
		out = io.MultiWriter(sinks...)
	}
	l := log.New(out, "", log.LstdFlags|log.Lmicroseconds)
	logger.Store(l)
	level.Store(int32(lvl))
	return nil
}

// E logs unconditionally (errors and rare events).
func E(format string, args ...any) {
	if l := logger.Load(); l != nil {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

// V logs at verbose level or above (session-level events).
func V(format string, args ...any) {
	if level.Load() < LevelVerbose {
		return
	}
	if l := logger.Load(); l != nil {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

// T logs at trace level (per-frame chatter, high volume).
func T(format string, args ...any) {
	if level.Load() < LevelTrace {
		return
	}
	if l := logger.Load(); l != nil {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

// Level reports the current verbosity level.
func Level() int { return int(level.Load()) }

// IsVerbose reports whether verbose-or-above is on (i.e. V calls emit).
func IsVerbose() bool { return level.Load() >= LevelVerbose }

// IsTrace reports whether trace is on (i.e. T calls emit).
func IsTrace() bool { return level.Load() >= LevelTrace }

// CRLFWriter wraps w so that every '\n' is written as "\r\n" — useful
// when emitting log lines to a stderr that is currently in raw TTY mode.
type CRLFWriter struct {
	W io.Writer
}

func (c CRLFWriter) Write(p []byte) (int, error) {
	out := make([]byte, 0, len(p)+8)
	for _, b := range p {
		if b == '\n' {
			out = append(out, '\r', '\n')
		} else {
			out = append(out, b)
		}
	}
	if _, err := c.W.Write(out); err != nil {
		return 0, err
	}
	return len(p), nil
}
