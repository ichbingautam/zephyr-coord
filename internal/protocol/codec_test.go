package protocol

import (
	"bytes"
	"testing"
)

func TestEncoder_Primitives(t *testing.T) {
	e := NewEncoder(256)

	e.WriteBool(true)
	e.WriteBool(false)
	e.WriteByte(0x42)
	e.WriteInt(12345678)
	e.WriteLong(9876543210)

	d := NewDecoder(e.Bytes())

	b1, _ := d.ReadBool()
	if !b1 {
		t.Error("Expected true")
	}

	b2, _ := d.ReadBool()
	if b2 {
		t.Error("Expected false")
	}

	by, _ := d.ReadByte()
	if by != 0x42 {
		t.Errorf("Byte = %x, want 0x42", by)
	}

	i, _ := d.ReadInt()
	if i != 12345678 {
		t.Errorf("Int = %d, want 12345678", i)
	}

	l, _ := d.ReadLong()
	if l != 9876543210 {
		t.Errorf("Long = %d, want 9876543210", l)
	}
}

func TestEncoder_StringBuffer(t *testing.T) {
	e := NewEncoder(256)

	e.WriteString("hello")
	e.WriteString("") // Empty string
	e.WriteBuffer([]byte{1, 2, 3, 4})
	e.WriteBuffer(nil) // Nil buffer

	d := NewDecoder(e.Bytes())

	s1, _ := d.ReadString()
	if s1 != "hello" {
		t.Errorf("String = %s, want hello", s1)
	}

	s2, _ := d.ReadString()
	if s2 != "" {
		t.Errorf("String = %s, want empty", s2)
	}

	buf, _ := d.ReadBuffer()
	if !bytes.Equal(buf, []byte{1, 2, 3, 4}) {
		t.Errorf("Buffer = %v, want [1 2 3 4]", buf)
	}

	nilBuf, _ := d.ReadBuffer()
	if nilBuf != nil {
		t.Errorf("NilBuffer = %v, want nil", nilBuf)
	}
}

func TestEncoder_Vector(t *testing.T) {
	e := NewEncoder(256)

	items := []string{"a", "b", "c"}
	e.WriteVector(len(items), func(i int) {
		e.WriteString(items[i])
	})

	d := NewDecoder(e.Bytes())

	count, _ := d.ReadVector()
	if count != 3 {
		t.Errorf("Count = %d, want 3", count)
	}

	for i := 0; i < count; i++ {
		s, _ := d.ReadString()
		if s != items[i] {
			t.Errorf("Item %d = %s, want %s", i, s, items[i])
		}
	}
}

func TestEncoder_Reset(t *testing.T) {
	e := NewEncoder(64)
	e.WriteInt(123)

	if e.Len() != 4 {
		t.Errorf("Len = %d, want 4", e.Len())
	}

	e.Reset()

	if e.Len() != 0 {
		t.Errorf("After reset, Len = %d, want 0", e.Len())
	}
}

func TestFrameReadWrite(t *testing.T) {
	var buf bytes.Buffer

	fw := NewFrameWriter(&buf)
	data := []byte("test frame data")
	fw.WriteFrame(data)

	fr := NewFrameReader(&buf, 1024)
	frame, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("ReadFrame failed: %v", err)
	}

	if !bytes.Equal(frame, data) {
		t.Errorf("Frame = %v, want %v", frame, data)
	}
}

func TestDecoder_Errors(t *testing.T) {
	d := NewDecoder([]byte{1, 2}) // Only 2 bytes

	_, err := d.ReadInt()
	if err != ErrBufferTooSmall {
		t.Errorf("Expected ErrBufferTooSmall, got %v", err)
	}
}

func BenchmarkEncoder_Int(b *testing.B) {
	e := NewEncoder(4096)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Reset()
		for j := 0; j < 100; j++ {
			e.WriteInt(int32(j))
		}
	}
}

func BenchmarkEncoder_String(b *testing.B) {
	e := NewEncoder(4096)
	s := "test string"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Reset()
		for j := 0; j < 100; j++ {
			e.WriteString(s)
		}
	}
}

func BenchmarkDecoder_Int(b *testing.B) {
	e := NewEncoder(4096)
	for j := 0; j < 100; j++ {
		e.WriteInt(int32(j))
	}
	data := e.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDecoder(data)
		for j := 0; j < 100; j++ {
			d.ReadInt()
		}
	}
}

func BenchmarkDecoder_String(b *testing.B) {
	e := NewEncoder(4096)
	for j := 0; j < 100; j++ {
		e.WriteString("test string")
	}
	data := e.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDecoder(data)
		for j := 0; j < 100; j++ {
			d.ReadString()
		}
	}
}
