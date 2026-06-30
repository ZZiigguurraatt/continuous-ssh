// Package chunk handles the chunk manifest exchanged in HELLO / HELLO_ACK to
// drive replay on reconnect. There is exactly one output stream per session
// (the merged stdout/stderr coming off the remote PTY).
//
// Reconciliation rule: the sender starts retransmitting at the lower of
//   (a) the first divergent chunk index, scaled by ChunkSize, and
//   (b) the start of its own last complete chunk.
// (b) guarantees the trailing ~1 MiB is always resent in full, covering the
// case where a connection dropped mid-chunk and the receiver has a ragged
// edge that no hash would detect.
package chunk

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/zziigguurraatt/continuous-ssh/internal/buffer"
	"github.com/zziigguurraatt/continuous-ssh/internal/proto"
)

type Mode uint8

const (
	ModeNew    Mode = 0
	ModeAttach Mode = 1
)

// Manifest is one side's view of the output stream for reconciliation.
// Hashes[i] is the hash of the absolute chunk (FirstIndex+i); each
// side may have trimmed a different prefix of its hashes, so the
// reconciliation has to align by absolute index.
type Manifest struct {
	Total      uint64
	FirstIndex uint64
	Hashes     []buffer.Hash
}

// Hello is the structured payload of a HELLO or HELLO_ACK frame.
//
// Major/Minor at the start let either side detect protocol-incompatible
// peers. DecodeHello populates them as-received; the caller is
// responsible for checking against proto.ProtocolMajor/Minor and
// deciding whether to proceed.
//
// AltScreen is meaningful on HELLO_ACK only (and only on protocol
// 1.1+): when set, the daemon's remote PTY is currently in the
// alt-screen buffer (vim, htop, less). A reattaching client uses
// this to enter alt-screen on the local terminal before output
// streams in and to send a Ctrl-L so the remote program redraws —
// cleanly handling a `--session` reattach into a TUI without
// scribbling onto the main screen. Older daemons (1.0) leave the
// trailing byte off the wire and the client decodes false, falling
// back to the pre-feature behavior.
type Hello struct {
	Major     uint8
	Minor     uint8
	Mode      Mode
	SessionID string
	Output    Manifest
	AltScreen bool
}

func (h *Hello) Encode() ([]byte, error) {
	if len(h.SessionID) > 255 {
		return nil, fmt.Errorf("chunk: session id too long: %d", len(h.SessionID))
	}
	major := h.Major
	if major == 0 {
		major = proto.ProtocolMajor
	}
	minor := h.Minor
	if minor == 0 && h.Major == 0 {
		// Only default minor when major also defaulted; an explicit
		// major with minor=0 should stay minor=0.
		minor = proto.ProtocolMinor
	}
	payload := []byte{major, minor, byte(h.Mode), byte(len(h.SessionID))}
	payload = append(payload, []byte(h.SessionID)...)
	// Manifest header: total (8) + firstIndex (8) + hashCount (4) = 20 bytes.
	var hdr [20]byte
	binary.BigEndian.PutUint64(hdr[0:8], h.Output.Total)
	binary.BigEndian.PutUint64(hdr[8:16], h.Output.FirstIndex)
	binary.BigEndian.PutUint32(hdr[16:20], uint32(len(h.Output.Hashes)))
	payload = append(payload, hdr[:]...)
	for _, hash := range h.Output.Hashes {
		payload = append(payload, hash[:]...)
	}
	// AltScreen trails the manifest as a single byte. Older
	// decoders that don't know about this field simply stop after
	// reading the manifest and ignore the trailing byte, so this
	// is a backward-compatible minor-version addition.
	if h.AltScreen {
		payload = append(payload, 1)
	} else {
		payload = append(payload, 0)
	}
	return payload, nil
}

// DecodeHello parses a HELLO/HELLO_ACK payload. It does NOT enforce
// protocol-version compatibility — Major/Minor are returned as-is and
// the caller decides what to do with them. This lets the daemon read
// a mismatched client's HELLO and still send back its own version in
// HELLO_ACK so the client can surface a clear error.
func DecodeHello(p []byte) (*Hello, error) {
	if len(p) < 4 {
		return nil, errors.New("chunk: hello payload too short")
	}
	h := &Hello{
		Major: p[0],
		Minor: p[1],
		Mode:  Mode(p[2]),
	}
	sidLen := int(p[3])
	p = p[4:]
	if len(p) < sidLen {
		return nil, errors.New("chunk: hello session id truncated")
	}
	h.SessionID = string(p[:sidLen])
	p = p[sidLen:]
	if len(p) < 20 {
		return nil, errors.New("chunk: hello manifest header truncated")
	}
	total := binary.BigEndian.Uint64(p[0:8])
	firstIndex := binary.BigEndian.Uint64(p[8:16])
	n := int(binary.BigEndian.Uint32(p[16:20]))
	p = p[20:]
	hashSize := len(buffer.Hash{})
	if len(p) < n*hashSize {
		return nil, errors.New("chunk: hello hashes truncated")
	}
	m := Manifest{Total: total, FirstIndex: firstIndex, Hashes: make([]buffer.Hash, n)}
	for j := 0; j < n; j++ {
		copy(m.Hashes[j][:], p[:hashSize])
		p = p[hashSize:]
	}
	h.Output = m
	// AltScreen — optional trailing byte added in protocol 1.1.
	// Older peers don't send it; we default to false in that case.
	if len(p) >= 1 {
		h.AltScreen = p[0] != 0
	}
	return h, nil
}

// ResendFrom returns the byte offset at which `own` should begin
// retransmitting data to `peer`. The result is the lower of:
//
//	(a) the first divergent absolute-chunk-index, scaled by chunkSize, and
//	(b) the start of `own`'s last complete chunk
//
// so that the trailing chunk + tail is always retransmitted (the
// ragged-edge guard).
//
// Each manifest carries a FirstIndex because trimming may have dropped
// a prefix of hashes from one or both sides. We only compare hashes
// in the absolute-index range that BOTH sides still have, which is
// [max(own.First, peer.First), min(own.End, peer.End)). Any divergence
// outside that range can't be detected — but in practice the trimmed
// prefix has already been ACKed by the client and is no longer
// retransmittable anyway, so it doesn't matter.
func ResendFrom(own, peer Manifest, chunkSize uint64) uint64 {
	ownStart := own.FirstIndex
	peerStart := peer.FirstIndex
	ownEnd := ownStart + uint64(len(own.Hashes))
	peerEnd := peerStart + uint64(len(peer.Hashes))

	startIndex := ownStart
	if peerStart > startIndex {
		startIndex = peerStart
	}
	endIndex := ownEnd
	if peerEnd < endIndex {
		endIndex = peerEnd
	}

	divergent := endIndex // assume all matched in the overlap
	for i := startIndex; i < endIndex; i++ {
		if own.Hashes[i-ownStart] != peer.Hashes[i-peerStart] {
			divergent = i
			break
		}
	}
	fromDivergent := divergent * chunkSize

	var lastChunkStart uint64
	if ownEnd > 0 {
		lastChunkStart = (ownEnd - 1) * chunkSize
	}
	if lastChunkStart < fromDivergent {
		return lastChunkStart
	}
	return fromDivergent
}

// ResizePayload represents the body of a RESIZE frame.
type ResizePayload struct {
	Cols uint16
	Rows uint16
}

func (r ResizePayload) Encode() []byte {
	var b [4]byte
	binary.BigEndian.PutUint16(b[0:2], r.Cols)
	binary.BigEndian.PutUint16(b[2:4], r.Rows)
	return b[:]
}

func DecodeResize(p []byte) (ResizePayload, error) {
	if len(p) < 4 {
		return ResizePayload{}, errors.New("chunk: resize payload too short")
	}
	return ResizePayload{
		Cols: binary.BigEndian.Uint16(p[0:2]),
		Rows: binary.BigEndian.Uint16(p[2:4]),
	}, nil
}
