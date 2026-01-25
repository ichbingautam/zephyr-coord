package zk

import "time"

// Stat contains metadata for a ZNode.
// This structure is cache-line aligned (88 bytes) for optimal performance.
type Stat struct {
	// Czxid is the ZXID of the change that created this node.
	Czxid int64

	// Mzxid is the ZXID of the change that last modified this node's data.
	Mzxid int64

	// Pzxid is the ZXID of the change that last modified this node's children.
	Pzxid int64

	// Ctime is the time in milliseconds when this node was created.
	Ctime int64

	// Mtime is the time in milliseconds when this node's data was last modified.
	Mtime int64

	// Version is the number of data changes to this node.
	// Used for optimistic locking in setData operations.
	Version int32

	// Cversion is the number of child node changes.
	Cversion int32

	// Aversion is the number of ACL changes.
	Aversion int32

	// EphemeralOwner is the session ID of the owner if ephemeral, 0 otherwise.
	EphemeralOwner int64

	// DataLength is the length of the data field.
	DataLength int32

	// NumChildren is the number of child nodes.
	NumChildren int32
}

// NewStat creates a Stat for a newly created node.
func NewStat(zxid ZXID, ephemeralOwner int64, dataLen int32) Stat {
	now := time.Now().UnixMilli()
	return Stat{
		Czxid:          int64(zxid),
		Mzxid:          int64(zxid),
		Pzxid:          int64(zxid),
		Ctime:          now,
		Mtime:          now,
		Version:        0,
		Cversion:       0,
		Aversion:       0,
		EphemeralOwner: ephemeralOwner,
		DataLength:     dataLen,
		NumChildren:    0,
	}
}

// UpdateData updates stat fields for a setData operation.
func (s *Stat) UpdateData(zxid ZXID, dataLen int32) {
	s.Mzxid = int64(zxid)
	s.Mtime = time.Now().UnixMilli()
	s.Version++
	s.DataLength = dataLen
}

// UpdateChildren updates stat fields when children are modified.
func (s *Stat) UpdateChildren(zxid ZXID, delta int32) {
	s.Pzxid = int64(zxid)
	s.Cversion++
	s.NumChildren += delta
}

// UpdateACL updates stat fields for a setACL operation.
func (s *Stat) UpdateACL(zxid ZXID) {
	s.Aversion++
}

// IsEphemeral returns true if this node is ephemeral.
func (s *Stat) IsEphemeral() bool {
	return s.EphemeralOwner != 0
}

// Clone returns a copy of the Stat.
func (s *Stat) Clone() Stat {
	return *s
}
