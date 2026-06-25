// Package dlog is a process-wide debug logger. It writes structured log lines
// to a file (always, errors-only by default) and optionally mirrors them to
// stderr when verbose mode is on.
package dlog

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
)

var (
	verbose atomic.Bool
	logger  atomic.Pointer[log.Logger]
)

// Setup configures the global logger. filePath, if non-empty, is opened
// O_APPEND|O_CREATE and used as the primary sink. When verbose is true, the
// stderrSink is additionally written to. stderrSink is typically os.Stderr,
// but the client wraps it in a CR/LF translator when stdin is in raw mode.
// Safe to call multiple times; later calls replace prior configuration.
func Setup(filePath string, verboseMode bool, stderrSink io.Writer) error {
	var sinks []io.Writer
	if filePath != "" {
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			return fmt.Errorf("dlog: open %s: %w", filePath, err)
		}
		sinks = append(sinks, f)
	}
	if verboseMode && stderrSink != nil {
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
	verbose.Store(verboseMode)
	return nil
}

// V logs only when verbose mode is enabled.
func V(format string, args ...any) {
	if !verbose.Load() {
		return
	}
	if l := logger.Load(); l != nil {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

// E logs unconditionally (errors and rare events).
func E(format string, args ...any) {
	if l := logger.Load(); l != nil {
		l.Output(2, fmt.Sprintf(format, args...))
	}
}

// IsVerbose reports whether verbose mode is on.
func IsVerbose() bool {
	return verbose.Load()
}

// CRLFWriter wraps w so that every '\n' is written as "\r\n" — useful when
// emitting log lines to a stderr that is currently in raw TTY mode.
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
