package storage

import (
	"sync"
	"testing"
)

func TestMemPool_Alloc(t *testing.T) {
	pool := NewMemPool()

	tests := []struct {
		name    string
		size    int
		wantCap int
	}{
		{"tiny", 10, 64},
		{"exact 64", 64, 64},
		{"small", 100, 256},
		{"1KB", 1024, 1024},
		{"slightly over 1KB", 1025, 4096},
		{"1MB", 1048576, 1048576},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := pool.Alloc(tt.size)
			if len(buf) != tt.size {
				t.Errorf("len(buf) = %d, want %d", len(buf), tt.size)
			}
			if cap(buf) != tt.wantCap {
				t.Errorf("cap(buf) = %d, want %d", cap(buf), tt.wantCap)
			}
			pool.Free(buf[:cap(buf)])
		})
	}
}

func TestMemPool_LargeAlloc(t *testing.T) {
	pool := NewMemPool()

	// Allocate larger than max size class
	size := 2 * 1048576 // 2MB
	buf := pool.Alloc(size)
	if len(buf) != size {
		t.Errorf("len(buf) = %d, want %d", len(buf), size)
	}
	pool.Free(buf) // Should not panic
}

func TestMemPool_Reuse(t *testing.T) {
	pool := NewMemPool()

	// Allocate and free
	buf1 := pool.Alloc(100)
	buf1Cap := cap(buf1)
	pool.Free(buf1[:buf1Cap])

	// Allocate again - should reuse
	buf2 := pool.Alloc(100)

	// The slice should come from the same pool
	if cap(buf2) != buf1Cap {
		t.Errorf("Expected reuse, got different capacity: %d vs %d", cap(buf2), buf1Cap)
	}
}

func TestMemPool_ClearsOnFree(t *testing.T) {
	pool := NewMemPool()

	buf := pool.Alloc(64)
	for i := range buf {
		buf[i] = byte(i)
	}
	pool.Free(buf[:cap(buf)])

	// Get the same buffer back (likely)
	buf2 := pool.Alloc(64)
	for _, b := range buf2 {
		if b != 0 {
			t.Error("Buffer was not cleared on free")
			break
		}
	}
}

func TestMemPool_Stats(t *testing.T) {
	pool := NewMemPool()

	_ = pool.Alloc(100)  // Gets 256B
	_ = pool.Alloc(1000) // Gets 1KB

	stats := pool.Stats()
	if stats.Allocated != 256+1024 {
		t.Errorf("Allocated = %d, want %d", stats.Allocated, 256+1024)
	}
}

func BenchmarkMemPool_Alloc64(b *testing.B) {
	pool := NewMemPool()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Alloc(64)
		pool.Free(buf[:cap(buf)])
	}
}

func BenchmarkMemPool_Alloc1KB(b *testing.B) {
	pool := NewMemPool()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Alloc(1024)
		pool.Free(buf[:cap(buf)])
	}
}

func BenchmarkMemPool_AllocParallel(b *testing.B) {
	pool := NewMemPool()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf := pool.Alloc(256)
			pool.Free(buf[:cap(buf)])
		}
	})
}

func BenchmarkMake_Alloc64(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_ = make([]byte, 64)
	}
}

func BenchmarkSyncPool_Alloc64(b *testing.B) {
	pool := sync.Pool{
		New: func() interface{} { return make([]byte, 64) },
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := pool.Get().([]byte)
		pool.Put(buf)
	}
}
