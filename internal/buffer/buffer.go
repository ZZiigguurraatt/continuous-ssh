// Package buffer is the append-only per-stream history used on both sides of
// the connection. The newest 10 MiB lives in RAM; everything older is
// spilled to a sequence of fixed-size segment files on disk (or, in no-disk
// mode, silently dropped from a sliding window).
//
// Segment files are named `<diskPath>.<startOff:020d>` so they sort
// lexicographically by offset. As `TrimTo` advances past a segment's end,
// that segment file is closed and deleted. This keeps disk usage bounded
// by held-bytes plus at most one segment of "trim waste" at the front of
// the oldest remaining segment.
//
// Chunks of fixed size (default 1 MiB) are hashed incrementally with
// SHA-256 as bytes flow through, so chunk manifests are O(1) to produce
// on reconnect. Only complete chunks are hashed; the trailing partial
// chunk is the "tail" and is always retransmitted in full on reconnect.
package buffer

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	DefaultRAMTail     uint64 = 10 << 20
	DefaultChunkSize   uint64 = 1 << 20
	DefaultSegmentSize uint64 = 10 << 20
	DefaultMaxBytes    uint64 = 100 << 20
)

// segmentFormat builds segment paths from the prefix + start offset.
// Twenty zero-padded decimal digits comfortably fits any uint64 byte
// count and keeps directory listings sortable by name.
const segmentFormat = "%s.%020d"

var ErrOverflow = errors.New("buffer: max size exceeded")

type Hash [sha256.Size]byte

// segment is one disk-resident slice of the stream. Bytes
// [startOff, endOff) live at file positions [0, endOff-startOff).
type segment struct {
	file     *os.File
	path     string
	startOff uint64
	endOff   uint64
}

type Buffer struct {
	mu sync.Mutex

	ramTail     uint64
	chunkSize   uint64
	segmentSize uint64
	maxBytes    uint64

	mem       []byte
	memOffset uint64
	total     uint64

	diskPrefix string     // empty in no-disk mode
	segments   []*segment // ordered oldest→newest; last is the active write target

	chunkHashes []Hash
	curHasher   hash.Hash
	curBytes    uint64

	// trimOffset is the lowest byte offset still logically held. Bytes
	// below it have been freed (segments deleted by TrimTo, RAM slid
	// forward, or — in no-disk mode — dropped off the back of the
	// window). Overflow accounting is (total - trimOffset).
	trimOffset uint64

	notify chan struct{}
	sealed bool
}

// New creates a Buffer. When diskPath is non-empty, it is the prefix
// for segment files (`<diskPath>.<startOff>`); any pre-existing files
// matching that pattern are removed at construction. When diskPath is
// empty the buffer is RAM-only: appends past the RAM tail silently
// drop the oldest bytes.
//
// Passing 0 for any limit selects the default. maxBytes caps HELD
// bytes (total - trimOffset), not cumulative throughput.
func New(diskPath string, ramTail, chunkSize, segmentSize, maxBytes uint64) (*Buffer, error) {
	if ramTail == 0 {
		ramTail = DefaultRAMTail
	}
	if chunkSize == 0 {
		chunkSize = DefaultChunkSize
	}
	if segmentSize == 0 {
		segmentSize = DefaultSegmentSize
	}
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytes
	}
	b := &Buffer{
		ramTail:     ramTail,
		chunkSize:   chunkSize,
		segmentSize: segmentSize,
		maxBytes:    maxBytes,
		mem:         make([]byte, 0, ramTail),
		curHasher:   sha256.New(),
		notify:      make(chan struct{}),
		diskPrefix:  diskPath,
	}
	if diskPath != "" {
		if err := removeExistingSegments(diskPath); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// SegmentInfo describes one segment file on disk. Used by the replay
// daemon, which enumerates and streams segments without instantiating
// a live Buffer.
type SegmentInfo struct {
	Path     string
	StartOff uint64
	EndOff   uint64 // StartOff + file size
}

// ScanSegments lists every segment file at the given prefix, sorted
// by start offset. Returns an empty slice (no error) if the directory
// doesn't exist.
func ScanSegments(prefix string) ([]SegmentInfo, error) {
	paths, err := segmentPaths(prefix)
	if err != nil {
		return nil, err
	}
	out := make([]SegmentInfo, 0, len(paths))
	for _, p := range paths {
		startOff, ok := parseSegmentOffset(p, prefix)
		if !ok {
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		out = append(out, SegmentInfo{
			Path:     p,
			StartOff: startOff,
			EndOff:   startOff + uint64(fi.Size()),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].StartOff < out[j].StartOff })
	return out, nil
}

// RemoveSegments unlinks every segment file at the given prefix. Used
// by the replay daemon for cleanup after a successful replay.
func RemoveSegments(prefix string) error {
	return removeExistingSegments(prefix)
}

func segmentPaths(prefix string) ([]string, error) {
	dir := filepath.Dir(prefix)
	base := filepath.Base(prefix)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		rem, ok := strings.CutPrefix(name, base+".")
		if !ok || rem == "" {
			continue
		}
		if _, err := strconv.ParseUint(rem, 10, 64); err != nil {
			continue
		}
		out = append(out, filepath.Join(dir, name))
	}
	return out, nil
}

func parseSegmentOffset(path, prefix string) (uint64, bool) {
	base := filepath.Base(prefix)
	name := filepath.Base(path)
	rem, ok := strings.CutPrefix(name, base+".")
	if !ok {
		return 0, false
	}
	v, err := strconv.ParseUint(rem, 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func removeExistingSegments(prefix string) error {
	paths, err := segmentPaths(prefix)
	if err != nil {
		return err
	}
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("buffer: remove leftover %s: %w", p, err)
		}
	}
	return nil
}

// rotate appends a new empty segment. Must be called with b.mu held.
func (b *Buffer) rotate() error {
	startOff := b.memOffset
	if len(b.segments) > 0 {
		startOff = b.segments[len(b.segments)-1].endOff
	}
	path := fmt.Sprintf(segmentFormat, b.diskPrefix, startOff)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("buffer: open segment %s: %w", path, err)
	}
	b.segments = append(b.segments, &segment{
		file:     f,
		path:     path,
		startOff: startOff,
		endOff:   startOff,
	})
	return nil
}

// spillToDisk writes data into segments, rotating as needed. Must be
// called with b.mu held.
func (b *Buffer) spillToDisk(data []byte) error {
	rem := data
	for len(rem) > 0 {
		if len(b.segments) == 0 ||
			b.segments[len(b.segments)-1].endOff-b.segments[len(b.segments)-1].startOff >= b.segmentSize {
			if err := b.rotate(); err != nil {
				return err
			}
		}
		seg := b.segments[len(b.segments)-1]
		room := b.segmentSize - (seg.endOff - seg.startOff)
		n := uint64(len(rem))
		if n > room {
			n = room
		}
		if _, err := seg.file.Write(rem[:n]); err != nil {
			return fmt.Errorf("buffer: spill write: %w", err)
		}
		seg.endOff += n
		rem = rem[n:]
	}
	return nil
}

// Append adds p to the buffer. Returns ErrOverflow if held bytes would
// exceed the cap; on overflow the buffer is left unchanged so callers
// can react (e.g. the daemon flips to shutdown).
func (b *Buffer) Append(p []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.sealed {
		return errors.New("buffer: sealed")
	}
	// Overflow only applies to disk-backed buffers (daemon side). In
	// no-disk mode the post-append spill loop unconditionally trims
	// `len(mem) - ramTail` bytes from the front of the sliding window,
	// so there's always room for the new write — and the maxBytes
	// check was incorrectly rejecting otherwise-valid writes the
	// moment held bytes first reached ramTail (e.g. on the client,
	// where ramTail == maxBytes == 10 MiB by design). That manifested
	// as the client's `outputBuf.Len()` getting permanently pinned at
	// ~10 MiB once total crossed that threshold, with every
	// subsequent Append returning ErrOverflow silently — see the bug
	// where seq's output stopped rendering near offset 10469101.
	if b.diskPrefix != "" {
		if held := b.total - b.trimOffset; held+uint64(len(p)) > b.maxBytes {
			return ErrOverflow
		}
	}

	// Hash incrementally so a manifest is always cheap to produce.
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
		if b.diskPrefix != "" {
			if err := b.spillToDisk(b.mem[:spill]); err != nil {
				return err
			}
		} else {
			// No-disk mode: bytes fall off the back of the RAM window.
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

// segmentContaining returns the segment covering off, or nil. The
// segment count is small (≈ maxBytes/segmentSize, default 10) so a
// linear scan is fine. Must be called with b.mu held.
func (b *Buffer) segmentContaining(off uint64) *segment {
	for _, s := range b.segments {
		if off >= s.startOff && off < s.endOff {
			return s
		}
	}
	return nil
}

// ReadAt copies bytes starting at off into p. Returns io.EOF only when
// off is at or past Len(); short reads at the end produce (n, nil) like
// io.ReaderAt. Reads from offsets below trimOffset error out, and so do
// reads from a gap in the segment list (e.g. a segment that was
// trim-deleted but the partial-prefix region above trimOffset was
// expected — won't happen with the current TrimTo policy which only
// deletes fully-trimmed segments).
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
	// Disk region first: anything below b.memOffset.
	for nRead < want && off+nRead < b.memOffset {
		readOff := off + nRead
		seg := b.segmentContaining(readOff)
		if seg == nil {
			return int(nRead), fmt.Errorf("buffer: offset %d not in any segment", readOff)
		}
		segReadOff := readOff - seg.startOff
		segRemaining := seg.endOff - readOff
		chunk := want - nRead
		if chunk > segRemaining {
			chunk = segRemaining
		}
		n, err := seg.file.ReadAt(p[nRead:nRead+chunk], int64(segReadOff))
		nRead += uint64(n)
		if err != nil && err != io.EOF {
			return int(nRead), fmt.Errorf("buffer: segment read: %w", err)
		}
		if uint64(n) < chunk {
			return int(nRead), io.ErrUnexpectedEOF
		}
	}
	// RAM tail: anything from b.memOffset upward.
	if nRead < want {
		memOff := (off + nRead) - b.memOffset
		copy(p[nRead:want], b.mem[memOff:memOff+(want-nRead)])
		nRead = want
	}
	return int(nRead), nil
}

// Len returns the cumulative byte count (monotonic, not affected by
// TrimTo).
func (b *Buffer) Len() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total
}

// TrimOffset returns the lowest byte offset currently held. Readers
// must clamp their start to at least this value.
func (b *Buffer) TrimOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.trimOffset
}

// TrimTo drops bytes below `off`, deleting any segments whose entire
// range falls below it and sliding the RAM window forward if needed.
// Bytes that fall *within* a still-held segment but below `off` are
// no longer readable (ReadAt errors below trimOffset) but their disk
// blocks remain until the whole segment ages out.
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

	// Delete segments entirely below the trim point.
	for len(b.segments) > 0 && b.segments[0].endOff <= off {
		seg := b.segments[0]
		_ = seg.file.Close()
		_ = os.Remove(seg.path)
		b.segments = b.segments[1:]
	}

	// Slide RAM window if trim landed inside it.
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

// ChunkHashes returns a copy of the hashes for every complete chunk.
// The trailing partial chunk (if any) is not represented.
func (b *Buffer) ChunkHashes() []Hash {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Hash, len(b.chunkHashes))
	copy(out, b.chunkHashes)
	return out
}

// TailOffset returns the offset where the trailing partial chunk starts.
// Bytes in [TailOffset, Len) are the "always retransmit" tail.
func (b *Buffer) TailOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.total - b.curBytes
}

// Stats is a point-in-time snapshot used by the daemon's debug
// heartbeat. HeldBytes is what the overflow cap is measured against;
// everything else helps diagnose where bytes physically sit.
type Stats struct {
	Total        uint64
	TrimOffset   uint64
	MemOffset    uint64
	HeldBytes    uint64
	RAMBytes     uint64
	DiskBytes    uint64 // logical disk-held = MemOffset - TrimOffset, ignoring trim-waste within segments
	DiskFileSize int64  // sum of segment file sizes on disk
	NumChunks    int
	NumSegments  int
}

// Stats returns a snapshot of the buffer's counters.
func (b *Buffer) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := Stats{
		Total:       b.total,
		TrimOffset:  b.trimOffset,
		MemOffset:   b.memOffset,
		HeldBytes:   b.total - b.trimOffset,
		RAMBytes:    uint64(len(b.mem)),
		DiskBytes:   b.memOffset - b.trimOffset,
		NumChunks:   len(b.chunkHashes),
		NumSegments: len(b.segments),
	}
	for _, seg := range b.segments {
		if fi, err := seg.file.Stat(); err == nil {
			s.DiskFileSize += fi.Size()
		}
	}
	return s
}

// WaitFor blocks until Len() > offset, the buffer is sealed, or ctx is
// canceled. Returns the current total on wake; io.EOF if sealed with
// no new data; or ctx.Err on cancellation.
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

// Seal marks the buffer as no longer accepting appends and wakes all
// waiters. Reads continue to work; segment files stay open until Close.
func (b *Buffer) Seal() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sealed {
		return
	}
	b.sealed = true
	close(b.notify)
}

// Close releases all segment files. When removeFile is false, the
// in-RAM tail is first flushed to disk so the persisted segments
// contain the full buffer; this is the fallback-recovery path used
// after daemon overflow or signal shutdown. When removeFile is true,
// every segment file is unlinked and no flush happens.
func (b *Buffer) Close(removeFile bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.sealed {
		b.sealed = true
		close(b.notify)
	}
	if b.diskPrefix == "" {
		return nil
	}

	var err error
	if !removeFile && len(b.mem) > 0 {
		if werr := b.spillToDisk(b.mem); werr != nil {
			err = werr
		} else {
			b.memOffset += uint64(len(b.mem))
			b.mem = b.mem[:0]
		}
	}
	for _, seg := range b.segments {
		if cerr := seg.file.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if removeFile {
		for _, seg := range b.segments {
			if rerr := os.Remove(seg.path); rerr != nil && err == nil {
				err = rerr
			}
		}
	}
	b.segments = nil
	return err
}
