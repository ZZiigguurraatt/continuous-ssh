// Package buffer is the append-only per-stream history used on both sides of
// the connection. The newest 10 MiB lives in RAM; everything older spills to
// a backing file. Total size is hard-capped (default 100 MiB); appends past
// the cap return ErrOverflow without mutating state.
//
// Chunks of fixed size (default 1 MiB) are hashed incrementally with SHA-256
// as bytes flow through, so chunk manifests are O(1) to produce on reconnect.
// Only complete chunks are hashed; the trailing partial chunk is the "tail"
// and is always retransmitted in full on reconnect.
package buffer

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
)

const (
	DefaultRAMTail   uint64 = 10 << 20
	DefaultChunkSize uint64 = 1 << 20
	DefaultMaxBytes  uint64 = 100 << 20
)

var ErrOverflow = errors.New("buffer: max size exceeded")

type Hash [sha256.Size]byte

type Buffer struct {
	mu sync.Mutex

	ramTail   uint64
	chunkSize uint64
	maxBytes  uint64

	mem        []byte
	memOffset  uint64
	total      uint64

	disk     *os.File
	diskPath string

	chunkHashes []Hash
	curHasher   hash.Hash
	curBytes    uint64

	// trimOffset is the lowest byte offset that's still logically held in
	// the buffer. Bytes below it have been Trim'd away (either acknowledged
	// by a downstream consumer and freed, or, in no-disk mode, fallen off
	// the back of the RAM sliding window). Overflow accounting is based on
	// (total - trimOffset), so the buffer's cap really means "max held
	// bytes" not "max bytes ever appended".
	trimOffset uint64

	notify chan struct{}
	sealed bool
}

// New creates a Buffer. When diskPath is non-empty, the file is created
// (truncating any existing content) and opened for read+write; bytes that
// exceed the RAM tail are spilled to it. When diskPath is empty the buffer
// is RAM-only: appends past the RAM tail silently drop the oldest bytes
// (trimOffset advances), which suits the client side where no on-disk
// state is wanted.
//
// Passing 0 for any limit selects the default. maxBytes is the cap on
// HELD bytes (total - trimOffset), not on cumulative throughput.
func New(diskPath string, ramTail, chunkSize, maxBytes uint64) (*Buffer, error) {
	if ramTail == 0 {
		ramTail = DefaultRAMTail
	}
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	b := &Buffer{
		ramTail:   ramTail,
		chunkSize: chunkSize,
		maxBytes:  maxBytes,
		mem:       make([]byte, 0, ramTail),
		curHasher: sha256.New(),
		notify:    make(chan struct{}),
	}
	if diskPath != "" {
		f, err := os.OpenFile(diskPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			return nil, fmt.Errorf("buffer: open spill file: %w", err)
		}
		b.disk = f
		b.diskPath = diskPath
	}
	return b, nil
}

// Append adds p to the buffer. Returns ErrOverflow if total would exceed the
// configured cap; on overflow the buffer is left unchanged so callers can
// react (e.g. the daemon exits, leaving the spill file intact for fallback
// recovery).
func (b *Buffer) Append(p []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return errors.New("buffer: sealed")
	}
	// Overflow is checked against HELD bytes, not cumulative — TrimTo
	// (or the no-disk sliding-window) can free space.
	if held := b.total - b.trimOffset; held+uint64(len(p)) > b.maxBytes {
		return ErrOverflow
	}

	rem := p
	for len(rem) > 0 {
		room := b.chunkSize - b.curBytes
		n := uint64(len(rem))
		if n > room {
			n = room
		}
		b.curHasher.Write(rem[:n])
		b.curBytes += n
		if b.curBytes == b.chunkSize {
			var h Hash
			b.curHasher.Sum(h[:0])
			b.chunkHashes = append(b.chunkHashes, h)
			b.curHasher.Reset()
			b.curBytes = 0
		}
		rem = rem[n:]
	}

	b.mem = append(b.mem, p...)
	b.total += uint64(len(p))

	if uint64(len(b.mem)) > b.ramTail {
		spill := uint64(len(b.mem)) - b.ramTail
		if b.disk != nil {
			if _, err := b.disk.Write(b.mem[:spill]); err != nil {
				return fmt.Errorf("buffer: spill write: %w", err)
			}
		} else {
			// No-disk mode: the oldest bytes simply fall off the back of
			// the RAM sliding window. trimOffset tracks that they're gone.
			b.trimOffset += spill
		}
		b.memOffset += spill
		copy(b.mem, b.mem[spill:])
		b.mem = b.mem[:uint64(len(b.mem))-spill]
	}

	old := b.notify
	b.notify = make(chan struct{})
	close(old)
	return nil
}

// ReadAt copies bytes starting at off into p. Returns io.EOF only when off is
// at or past Len(); short reads at the end produce (n, nil) like io.ReaderAt.
// Reads from offsets below trimOffset (i.e., already freed) error out: those
// bytes are no longer recoverable from this buffer.
func (b *Buffer) ReadAt(p []byte, off uint64) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if off >= b.total {
		return 0, io.EOF
	}
	if off < b.trimOffset {
		return 0, fmt.Errorf("buffer: offset %d below trim point %d", off, b.trimOffset)
	}
	want := uint64(len(p))
	if off+want > b.total {
		want = b.total - off
	}

	var nRead uint64
	if off < b.memOffset {
		diskWant := want
		if off+diskWant > b.memOffset {
			diskWant = b.memOffset - off
		}
		n, err := b.disk.ReadAt(p[:diskWant], int64(off))
		nRead += uint64(n)
		if err != nil && err != io.EOF {
			return int(nRead), fmt.Errorf("buffer: spill read: %w", err)
		}
		if uint64(n) < diskWant {
			return int(nRead), io.ErrUnexpectedEOF
		}
	}
	if nRead < want {
		memOff := off + nRead - b.memOffset
		copy(p[nRead:want], b.mem[memOff:memOff+(want-nRead)])
		nRead = want
	}
	return int(nRead), nil
}

// Len returns the total number of bytes appended so far (monotonic — not
// affected by TrimTo).
func (b *Buffer) Len() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total
}

// TrimOffset returns the lowest byte offset currently held by the buffer.
// Bytes below it have been trimmed and are no longer readable; readers
// must clamp their start to at least this value.
func (b *Buffer) TrimOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trimOffset
}

// TrimTo drops bytes below `off` from the buffer, freeing RAM (and
// logically freeing disk; the disk file isn't compacted but its leading
// region becomes unreferenced). Bytes at or above `off` remain readable.
// Calls with off <= current trim point are no-ops; calls beyond total are
// clamped.
func (b *Buffer) TrimTo(off uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if off <= b.trimOffset {
		return
	}
	if off > b.total {
		off = b.total
	}
	b.trimOffset = off
	// If we're trimming into the RAM region, slide the window forward.
	if off > b.memOffset {
		drop := off - b.memOffset
		if drop > uint64(len(b.mem)) {
			drop = uint64(len(b.mem))
		}
		copy(b.mem, b.mem[drop:])
		b.mem = b.mem[:uint64(len(b.mem))-drop]
		b.memOffset = off
	}
}

// ChunkSize reports the configured chunk size.
func (b *Buffer) ChunkSize() uint64 {
	return b.chunkSize
}

// ChunkHashes returns a copy of the hashes for every complete chunk, in order.
// The trailing partial chunk (if any) is not represented.
func (b *Buffer) ChunkHashes() []Hash {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Hash, len(b.chunkHashes))
	copy(out, b.chunkHashes)
	return out
}

// TailOffset returns the byte offset where the trailing partial chunk begins.
// Bytes in [TailOffset, Len) are the "always retransmit" tail.
func (b *Buffer) TailOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total - b.curBytes
}

// WaitFor blocks until Len() > offset, the buffer is sealed, or ctx is
// canceled. Returns the current total on wake; io.EOF if sealed with no new
// data; or ctx.Err on cancellation.
func (b *Buffer) WaitFor(ctx context.Context, offset uint64) (uint64, error) {
	for {
		b.mu.Lock()
		if b.total > offset {
			t := b.total
			b.mu.Unlock()
			return t, nil
		}
		if b.sealed {
			t := b.total
			b.mu.Unlock()
			return t, io.EOF
		}
		ch := b.notify
		b.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

// Seal marks the buffer as no longer accepting appends and wakes all waiters.
// Reads continue to work; the disk file stays open until Close.
func (b *Buffer) Seal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return
	}
	b.sealed = true
	close(b.notify)
}

// Close releases the spill file. When removeFile is false, any in-RAM tail is
// first flushed to disk so the persisted file contains the full buffer; this
// is the fallback-recovery path used after daemon overflow. When removeFile
// is true, the file is unlinked and no flush is performed.
func (b *Buffer) Close(removeFile bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sealed {
		b.sealed = true
		close(b.notify)
	}
	if b.disk == nil {
		return nil
	}
	var err error
	if !removeFile && len(b.mem) > 0 {
		if _, werr := b.disk.Write(b.mem); werr != nil {
			err = werr
		} else {
			b.memOffset += uint64(len(b.mem))
			b.mem = b.mem[:0]
		}
	}
	if cerr := b.disk.Close(); cerr != nil && err == nil {
		err = cerr
	}
	b.disk = nil
	if removeFile {
		if rerr := os.Remove(b.diskPath); rerr != nil && err == nil {
			err = rerr
		}
	}
	return err
}
