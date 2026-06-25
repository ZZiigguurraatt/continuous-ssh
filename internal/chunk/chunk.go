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
)

type Mode uint8

const (
	ModeNew    Mode = 0
	ModeAttach Mode = 1
)

// Manifest is one side's view of the output stream for reconciliation.
type Manifest struct {
	Total  uint64
	Hashes []buffer.Hash
}

// Hello is the structured payload of a HELLO or HELLO_ACK frame.
type Hello struct {
	Mode      Mode
	SessionID string
	Output    Manifest
}

const helloVersion = 2

func (h *Hello) Encode() ([]byte, error) {
	if len(h.SessionID) > 255 {
		return nil, fmt.Errorf("chunk: session id too long: %d", len(h.SessionID))
	}
	payload := []byte{helloVersion, byte(h.Mode), byte(len(h.SessionID))}
	payload = append(payload, []byte(h.SessionID)...)
	if uint64(len(h.Output.Hashes))*buffer.DefaultChunkSize > h.Output.Total {
		return nil, fmt.Errorf("chunk: hash count exceeds total bytes")
	}
	var hdr [12]byte
	binary.BigEndian.PutUint64(hdr[0:8], h.Output.Total)
	binary.BigEndian.PutUint32(hdr[8:12], uint32(len(h.Output.Hashes)))
	payload = append(payload, hdr[:]...)
	for _, hash := range h.Output.Hashes {
		payload = append(payload, hash[:]...)
	}
	return payload, nil
}

func DecodeHello(p []byte) (*Hello, error) {
	if len(p) < 3 {
		return nil, errors.New("chunk: hello payload too short")
	}
	if p[0] != helloVersion {
		return nil, fmt.Errorf("chunk: unsupported hello version %d", p[0])
	}
	h := &Hello{Mode: Mode(p[1])}
	sidLen := int(p[2])
	p = p[3:]
	if len(p) < sidLen {
		return nil, errors.New("chunk: hello session id truncated")
	}
	h.SessionID = string(p[:sidLen])
	p = p[sidLen:]
	if len(p) < 12 {
		return nil, errors.New("chunk: hello manifest header truncated")
	}
	total := binary.BigEndian.Uint64(p[0:8])
	n := int(binary.BigEndian.Uint32(p[8:12]))
	p = p[12:]
	hashSize := len(buffer.Hash{})
	if len(p) < n*hashSize {
		return nil, errors.New("chunk: hello hashes truncated")
	}
	m := Manifest{Total: total, Hashes: make([]buffer.Hash, n)}
	for j := 0; j < n; j++ {
		copy(m.Hashes[j][:], p[:hashSize])
		p = p[hashSize:]
	}
	h.Output = m
	return h, nil
}

// ResendFrom returns the byte offset at which `own` should begin retransmitting
// data to `peer`. The result is min(first-divergent-chunk-offset,
// last-complete-chunk-offset) so that the trailing chunk + tail is always
// retransmitted.
func ResendFrom(own, peer Manifest, chunkSize uint64) uint64 {
	common := len(peer.Hashes)
	if common > len(own.Hashes) {
		common = len(own.Hashes)
	}
	divergent := common
	for i := 0; i < common; i++ {
		if own.Hashes[i] != peer.Hashes[i] {
			divergent = i
			break
		}
	}
	fromDivergent := uint64(divergent) * chunkSize

	var lastChunkStart uint64
	if n := len(own.Hashes); n > 0 {
		lastChunkStart = uint64(n-1) * chunkSize
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
