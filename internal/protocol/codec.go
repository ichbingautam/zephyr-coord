// Package protocol provides ZooKeeper-compatible binary protocol encoding/decoding.
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

// Protocol errors
var (
	ErrBufferTooSmall = errors.New("buffer too small")
	ErrInvalidData    = errors.New("invalid protocol data")
	ErrStringTooLong  = errors.New("string exceeds maximum length")
)

const (
	MaxStringLength = 10 * 1024 * 1024  // 10MB max string
	MaxBufferLength = 100 * 1024 * 1024 // 100MB max buffer
)

// Encoder provides Jute-compatible binary encoding.
// All integers are written in big-endian (network byte order).
type Encoder struct {
	buf []byte
	pos int
}

// NewEncoder creates a new encoder with the given capacity.
func NewEncoder(capacity int) *Encoder {
	return &Encoder{
		buf: make([]byte, 0, capacity),
	}
}

// Reset resets the encoder for reuse.
func (e *Encoder) Reset() {
	e.buf = e.buf[:0]
	e.pos = 0
}

// Bytes returns the encoded data.
func (e *Encoder) Bytes() []byte {
	return e.buf
}

// Len returns the current length of encoded data.
func (e *Encoder) Len() int {
	return len(e.buf)
}

// grow ensures there's room for n more bytes.
func (e *Encoder) grow(n int) {
	if cap(e.buf)-len(e.buf) < n {
		newBuf := make([]byte, len(e.buf), 2*cap(e.buf)+n)
		copy(newBuf, e.buf)
		e.buf = newBuf
	}
}

// WriteBool writes a boolean (1 byte: 0 or 1).
func (e *Encoder) WriteBool(v bool) {
	e.grow(1)
	if v {
		e.buf = append(e.buf, 1)
	} else {
		e.buf = append(e.buf, 0)
	}
}

// WriteByte writes a single byte.
func (e *Encoder) WriteByte(v byte) {
	e.grow(1)
	e.buf = append(e.buf, v)
}

// WriteInt writes a 32-bit signed integer.
func (e *Encoder) WriteInt(v int32) {
	e.grow(4)
	e.buf = append(e.buf, 0, 0, 0, 0)
	binary.BigEndian.PutUint32(e.buf[len(e.buf)-4:], uint32(v))
}

// WriteLong writes a 64-bit signed integer.
func (e *Encoder) WriteLong(v int64) {
	e.grow(8)
	e.buf = append(e.buf, 0, 0, 0, 0, 0, 0, 0, 0)
	binary.BigEndian.PutUint64(e.buf[len(e.buf)-8:], uint64(v))
}

// WriteFloat writes a 32-bit float.
func (e *Encoder) WriteFloat(v float32) {
	e.WriteInt(int32(math.Float32bits(v)))
}

// WriteDouble writes a 64-bit double.
func (e *Encoder) WriteDouble(v float64) {
	e.WriteLong(int64(math.Float64bits(v)))
}

// WriteString writes a length-prefixed UTF-8 string.
// Format: [4-byte length][string bytes]
// A nil/empty string is written as length -1.
func (e *Encoder) WriteString(s string) {
	if s == "" {
		e.WriteInt(-1)
		return
	}
	e.WriteInt(int32(len(s)))
	e.grow(len(s))
	e.buf = append(e.buf, s...)
}

// WriteBuffer writes a length-prefixed byte buffer.
// Format: [4-byte length][bytes]
// A nil buffer is written as length -1.
func (e *Encoder) WriteBuffer(b []byte) {
	if b == nil {
		e.WriteInt(-1)
		return
	}
	e.WriteInt(int32(len(b)))
	e.grow(len(b))
	e.buf = append(e.buf, b...)
}

// WriteVector writes a length-prefixed vector.
// The caller provides a function to write each element.
func (e *Encoder) WriteVector(count int, writeElem func(i int)) {
	e.WriteInt(int32(count))
	for i := 0; i < count; i++ {
		writeElem(i)
	}
}

// Decoder provides Jute-compatible binary decoding.
type Decoder struct {
	buf []byte
	pos int
}

// NewDecoder creates a decoder from a byte slice.
func NewDecoder(buf []byte) *Decoder {
	return &Decoder{buf: buf}
}

// Reset resets the decoder with new data.
func (d *Decoder) Reset(buf []byte) {
	d.buf = buf
	d.pos = 0
}

// Remaining returns the number of unread bytes.
func (d *Decoder) Remaining() int {
	return len(d.buf) - d.pos
}

// ReadBool reads a boolean.
func (d *Decoder) ReadBool() (bool, error) {
	if d.Remaining() < 1 {
		return false, ErrBufferTooSmall
	}
	v := d.buf[d.pos] != 0
	d.pos++
	return v, nil
}

// ReadByte reads a single byte.
func (d *Decoder) ReadByte() (byte, error) {
	if d.Remaining() < 1 {
		return 0, ErrBufferTooSmall
	}
	v := d.buf[d.pos]
	d.pos++
	return v, nil
}

// ReadInt reads a 32-bit signed integer.
func (d *Decoder) ReadInt() (int32, error) {
	if d.Remaining() < 4 {
		return 0, ErrBufferTooSmall
	}
	v := int32(binary.BigEndian.Uint32(d.buf[d.pos:]))
	d.pos += 4
	return v, nil
}

// ReadLong reads a 64-bit signed integer.
func (d *Decoder) ReadLong() (int64, error) {
	if d.Remaining() < 8 {
		return 0, ErrBufferTooSmall
	}
	v := int64(binary.BigEndian.Uint64(d.buf[d.pos:]))
	d.pos += 8
	return v, nil
}

// ReadFloat reads a 32-bit float.
func (d *Decoder) ReadFloat() (float32, error) {
	v, err := d.ReadInt()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(uint32(v)), nil
}

// ReadDouble reads a 64-bit double.
func (d *Decoder) ReadDouble() (float64, error) {
	v, err := d.ReadLong()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(uint64(v)), nil
}

// ReadString reads a length-prefixed string.
func (d *Decoder) ReadString() (string, error) {
	length, err := d.ReadInt()
	if err != nil {
		return "", err
	}
	if length < 0 {
		return "", nil // nil/empty string
	}
	if length > MaxStringLength {
		return "", ErrStringTooLong
	}
	if d.Remaining() < int(length) {
		return "", ErrBufferTooSmall
	}
	s := string(d.buf[d.pos : d.pos+int(length)])
	d.pos += int(length)
	return s, nil
}

// ReadBuffer reads a length-prefixed byte buffer.
func (d *Decoder) ReadBuffer() ([]byte, error) {
	length, err := d.ReadInt()
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, nil // nil buffer
	}
	if length > MaxBufferLength {
		return nil, ErrInvalidData
	}
	if d.Remaining() < int(length) {
		return nil, ErrBufferTooSmall
	}
	b := make([]byte, length)
	copy(b, d.buf[d.pos:d.pos+int(length)])
	d.pos += int(length)
	return b, nil
}

// ReadVector reads a length-prefixed vector.
// Returns the count; caller should read each element.
func (d *Decoder) ReadVector() (int, error) {
	count, err := d.ReadInt()
	if err != nil {
		return 0, err
	}
	if count < 0 {
		return 0, nil
	}
	return int(count), nil
}

// FrameReader reads length-prefixed frames from an io.Reader.
type FrameReader struct {
	r         io.Reader
	maxSize   int
	headerBuf [4]byte
}

// NewFrameReader creates a new frame reader.
func NewFrameReader(r io.Reader, maxSize int) *FrameReader {
	return &FrameReader{r: r, maxSize: maxSize}
}

// ReadFrame reads the next frame.
func (fr *FrameReader) ReadFrame() ([]byte, error) {
	if _, err := io.ReadFull(fr.r, fr.headerBuf[:]); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(fr.headerBuf[:]))
	if length < 0 || length > fr.maxSize {
		return nil, ErrInvalidData
	}
	frame := make([]byte, length)
	if _, err := io.ReadFull(fr.r, frame); err != nil {
		return nil, err
	}
	return frame, nil
}

// FrameWriter writes length-prefixed frames to an io.Writer.
type FrameWriter struct {
	w         io.Writer
	headerBuf [4]byte
}

// NewFrameWriter creates a new frame writer.
func NewFrameWriter(w io.Writer) *FrameWriter {
	return &FrameWriter{w: w}
}

// WriteFrame writes a frame with length prefix.
func (fw *FrameWriter) WriteFrame(data []byte) error {
	binary.BigEndian.PutUint32(fw.headerBuf[:], uint32(len(data)))
	if _, err := fw.w.Write(fw.headerBuf[:]); err != nil {
		return err
	}
	_, err := fw.w.Write(data)
	return err
}
