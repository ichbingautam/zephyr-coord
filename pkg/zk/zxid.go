// Package zk provides core ZooKeeper-compatible data types for the ZephyrCoord
// distributed coordination service.
package zk

import (
	"fmt"
	"sync/atomic"
)

// ZXID is a 64-bit ZooKeeper Transaction ID.
// Structure: epoch (high 32 bits) + counter (low 32 bits)
// Provides total ordering for all state changes across leader elections.
type ZXID uint64

// ZXIDGenerator provides thread-safe ZXID generation.
type ZXIDGenerator struct {
	current atomic.Uint64
}

// NewZXID creates a ZXID from epoch and counter components.
func NewZXID(epoch uint32, counter uint32) ZXID {
	return ZXID(uint64(epoch)<<32 | uint64(counter))
}

// Epoch returns the high 32 bits (leader election generation).
func (z ZXID) Epoch() uint32 {
	return uint32(z >> 32)
}

// Counter returns the low 32 bits (transaction counter within epoch).
func (z ZXID) Counter() uint32 {
	return uint32(z & 0xFFFFFFFF)
}

// Next returns the next ZXID (incremented counter).
// If counter overflows, it wraps to 0 (new epoch should be set externally).
func (z ZXID) Next() ZXID {
	return z + 1
}

// WithEpoch returns a new ZXID with the specified epoch and counter reset to 0.
func (z ZXID) WithEpoch(epoch uint32) ZXID {
	return NewZXID(epoch, 0)
}

// Compare returns -1 if z < other, 0 if z == other, 1 if z > other.
func (z ZXID) Compare(other ZXID) int {
	if z < other {
		return -1
	}
	if z > other {
		return 1
	}
	return 0
}

// IsZero returns true if the ZXID is uninitialized.
func (z ZXID) IsZero() bool {
	return z == 0
}

// String returns a hex representation of the ZXID.
func (z ZXID) String() string {
	return fmt.Sprintf("0x%x", uint64(z))
}

// NewZXIDGenerator creates a new generator starting from the given ZXID.
func NewZXIDGenerator(start ZXID) *ZXIDGenerator {
	g := &ZXIDGenerator{}
	g.current.Store(uint64(start))
	return g
}

// Current returns the current ZXID without incrementing.
func (g *ZXIDGenerator) Current() ZXID {
	return ZXID(g.current.Load())
}

// Next atomically increments and returns the next ZXID.
func (g *ZXIDGenerator) Next() ZXID {
	return ZXID(g.current.Add(1))
}

// SetEpoch atomically sets a new epoch and resets counter to 0.
// This should only be called during leader election.
func (g *ZXIDGenerator) SetEpoch(epoch uint32) ZXID {
	newZxid := NewZXID(epoch, 0)
	g.current.Store(uint64(newZxid))
	return newZxid
}

// Restore sets the generator to a specific ZXID (used during recovery).
func (g *ZXIDGenerator) Restore(zxid ZXID) {
	g.current.Store(uint64(zxid))
}
