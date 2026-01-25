package zk

import (
	"testing"
)

func TestZXID_EpochCounter(t *testing.T) {
	tests := []struct {
		name    string
		epoch   uint32
		counter uint32
	}{
		{"zero", 0, 0},
		{"epoch only", 5, 0},
		{"counter only", 0, 100},
		{"both", 10, 500},
		{"max counter", 1, 0xFFFFFFFF},
		{"max epoch", 0xFFFFFFFF, 0},
		{"max both", 0xFFFFFFFF, 0xFFFFFFFF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zxid := NewZXID(tt.epoch, tt.counter)
			if got := zxid.Epoch(); got != tt.epoch {
				t.Errorf("Epoch() = %d, want %d", got, tt.epoch)
			}
			if got := zxid.Counter(); got != tt.counter {
				t.Errorf("Counter() = %d, want %d", got, tt.counter)
			}
		})
	}
}

func TestZXID_Next(t *testing.T) {
	zxid := NewZXID(1, 0)
	next := zxid.Next()

	if next.Epoch() != 1 {
		t.Errorf("Next().Epoch() = %d, want 1", next.Epoch())
	}
	if next.Counter() != 1 {
		t.Errorf("Next().Counter() = %d, want 1", next.Counter())
	}
}

func TestZXID_Compare(t *testing.T) {
	z1 := NewZXID(1, 100)
	z2 := NewZXID(1, 200)
	z3 := NewZXID(2, 50)
	z4 := NewZXID(1, 100)

	if z1.Compare(z2) != -1 {
		t.Error("z1 should be less than z2")
	}
	if z2.Compare(z1) != 1 {
		t.Error("z2 should be greater than z1")
	}
	if z1.Compare(z3) != -1 {
		t.Error("z1 should be less than z3 (different epoch)")
	}
	if z1.Compare(z4) != 0 {
		t.Error("z1 should equal z4")
	}
}

func TestZXIDGenerator_ThreadSafe(t *testing.T) {
	gen := NewZXIDGenerator(NewZXID(1, 0))
	done := make(chan struct{})
	count := 1000

	// Spawn multiple goroutines incrementing concurrently
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < count; j++ {
				gen.Next()
			}
			done <- struct{}{}
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	expected := uint32(10 * count)
	if got := gen.Current().Counter(); got != expected {
		t.Errorf("After concurrent increments, Counter() = %d, want %d", got, expected)
	}
}

func TestZXIDGenerator_SetEpoch(t *testing.T) {
	gen := NewZXIDGenerator(NewZXID(1, 500))
	gen.SetEpoch(2)

	if got := gen.Current().Epoch(); got != 2 {
		t.Errorf("After SetEpoch(2), Epoch() = %d, want 2", got)
	}
	if got := gen.Current().Counter(); got != 0 {
		t.Errorf("After SetEpoch(2), Counter() = %d, want 0", got)
	}
}

func BenchmarkZXID_Next(b *testing.B) {
	gen := NewZXIDGenerator(NewZXID(1, 0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		gen.Next()
	}
}
