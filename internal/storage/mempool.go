// Package storage provides high-performance storage primitives for ZephyrCoord.
package storage

import (
	"sync"
	"sync/atomic"
)

// MemPool is a slab allocator that reduces GC pressure by reusing byte slices.
// It maintains separate free lists for different size classes.
type MemPool struct {
	slabs     [8]*slab
	allocated atomic.Int64 // Total bytes allocated (for metrics)
	freed     atomic.Int64 // Total bytes freed (for metrics)
}

// Size classes: 64B, 256B, 1KB, 4KB, 16KB, 64KB, 256KB, 1MB
var sizeClasses = [8]int{64, 256, 1024, 4096, 16384, 65536, 262144, 1048576}

type slab struct {
	size     int
	freeList sync.Pool
}

// NewMemPool creates a new memory pool.
func NewMemPool() *MemPool {
	p := &MemPool{}
	for i := range p.slabs {
		size := sizeClasses[i]
		p.slabs[i] = &slab{
			size: size,
			freeList: sync.Pool{
				New: func() interface{} {
					return make([]byte, size)
				},
			},
		}
	}
	return p
}

// sizeClass returns the index of the smallest size class that fits n bytes.
// Returns -1 if the size exceeds the maximum.
func sizeClass(n int) int {
	for i, size := range sizeClasses {
		if n <= size {
			return i
		}
	}
	return -1
}

// Alloc returns a byte slice of at least size bytes.
// The returned slice may be larger than requested.
// For sizes exceeding 1MB, a new slice is allocated directly.
func (p *MemPool) Alloc(size int) []byte {
	if size <= 0 {
		return nil
	}

	class := sizeClass(size)
	if class < 0 {
		// Exceeds max size class, allocate directly
		p.allocated.Add(int64(size))
		return make([]byte, size)
	}

	buf := p.slabs[class].freeList.Get().([]byte)
	p.allocated.Add(int64(len(buf)))
	return buf[:size] // Return slice of exact requested size
}

// Free returns a byte slice to the pool for reuse.
// Only slices originally obtained from Alloc should be freed.
// The slice is cleared before being returned to the pool.
func (p *MemPool) Free(b []byte) {
	if b == nil {
		return
	}

	size := cap(b)
	class := sizeClass(size)
	if class < 0 || sizeClasses[class] != size {
		// Not from our pool, let GC handle it
		p.freed.Add(int64(len(b)))
		return
	}

	// Clear the slice to prevent data leaks
	clear(b[:size])

	p.freed.Add(int64(size))
	p.slabs[class].freeList.Put(b[:size])
}

// AllocSize returns the actual allocation size for a requested size.
// Useful for knowing how much memory will actually be used.
func AllocSize(requested int) int {
	class := sizeClass(requested)
	if class < 0 {
		return requested
	}
	return sizeClasses[class]
}

// Stats returns memory pool statistics.
func (p *MemPool) Stats() MemPoolStats {
	return MemPoolStats{
		Allocated: p.allocated.Load(),
		Freed:     p.freed.Load(),
	}
}

// MemPoolStats contains memory pool statistics.
type MemPoolStats struct {
	Allocated int64
	Freed     int64
}

// InUse returns the approximate bytes currently in use.
func (s MemPoolStats) InUse() int64 {
	return s.Allocated - s.Freed
}
