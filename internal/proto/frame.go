// Package proto carries the wire framing used between client/attach and the
// daemon. Frames are length-prefixed binary records:
//
//	[u8 type][u64 offset][u32 length][bytes...]
//
// `offset` is the stream's monotonic byte offset for OUTPUT frames, and is
// zero for all other frame types. All multi-byte fields are big-endian.
package proto

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

type FrameType byte

const (
	Hello    FrameType = 0x01
	HelloAck FrameType = 0x02
	Output   FrameType = 0x04
	Stdin    FrameType = 0x06
	Resize   FrameType = 0x08
	Exit     FrameType = 0x07
	Shutdown FrameType = 0x09 // client → daemon: abort, kill cmd, exit
	Ack      FrameType = 0x0A // client → daemon: u64 offset, "I've received through this byte; you may forget anything older"
)

func (t FrameType) String() string {
	switch t {
	case Hello:
		return "HELLO"
	case HelloAck:
		return "HELLO_ACK"
	case Output:
		return "OUTPUT"
	case Stdin:
		return "STDIN"
	case Resize:
		return "RESIZE"
	case Exit:
		return "EXIT"
	case Shutdown:
		return "SHUTDOWN"
	case Ack:
		return "ACK"
	default:
		return fmt.Sprintf("FrameType(0x%02x)", byte(t))
	}
}

const (
	headerSize     = 1 + 8 + 4
	MaxPayloadSize = 64 << 20
)

type Frame struct {
	Type    FrameType
	Offset  uint64
	Payload []byte
}

// Conn serialises frame writes against concurrent senders. Reads are
// unsynchronised; callers must read from a single goroutine.
type Conn struct {
	r   io.Reader
	w   io.Writer
	wmu sync.Mutex
}

func NewConn(r io.Reader, w io.Writer) *Conn {
	return &Conn{r: r, w: w}
}

func (c *Conn) WriteFrame(f Frame) error {
	if len(f.Payload) > MaxPayloadSize {
		return fmt.Errorf("proto: payload too large: %d > %d", len(f.Payload), MaxPayloadSize)
	}
	var hdr [headerSize]byte
	hdr[0] = byte(f.Type)
	binary.BigEndian.PutUint64(hdr[1:9], f.Offset)
	binary.BigEndian.PutUint32(hdr[9:13], uint32(len(f.Payload)))

	c.wmu.Lock()
	defer c.wmu.Unlock()
	if _, err := c.w.Write(hdr[:]); err != nil {
		return err
	}
	if len(f.Payload) > 0 {
		if _, err := c.w.Write(f.Payload); err != nil {
			return err
		}
	}
	return nil
}

func (c *Conn) ReadFrame() (Frame, error) {
	var hdr [headerSize]byte
	if _, err := io.ReadFull(c.r, hdr[:]); err != nil {
		return Frame{}, err
	}
	length := binary.BigEndian.Uint32(hdr[9:13])
	if length > MaxPayloadSize {
		return Frame{}, fmt.Errorf("proto: payload too large: %d > %d", length, MaxPayloadSize)
	}
	f := Frame{
		Type:   FrameType(hdr[0]),
		Offset: binary.BigEndian.Uint64(hdr[1:9]),
	}
	if length > 0 {
		f.Payload = make([]byte, length)
		if _, err := io.ReadFull(c.r, f.Payload); err != nil {
			return Frame{}, err
		}
	}
	return f, nil
}
