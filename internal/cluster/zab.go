// Package cluster - ZAB protocol message types.
package cluster

import (
	"encoding/binary"
	"errors"
)

// MessageType identifies the type of cluster message.
type MessageType uint8

const (
	// Election messages
	MsgVote MessageType = iota + 1

	// Discovery phase
	MsgFollowerInfo
	MsgLeaderInfo
	MsgAckEpoch

	// Synchronization
	MsgNewLeader
	MsgAckNewLeader
	MsgDiff
	MsgSnap
	MsgTrunc

	// Broadcast phase
	MsgProposal
	MsgAck
	MsgCommit

	// Heartbeat
	MsgPing
	MsgPong
)

func (m MessageType) String() string {
	switch m {
	case MsgVote:
		return "VOTE"
	case MsgFollowerInfo:
		return "FOLLOWER_INFO"
	case MsgLeaderInfo:
		return "LEADER_INFO"
	case MsgAckEpoch:
		return "ACK_EPOCH"
	case MsgNewLeader:
		return "NEW_LEADER"
	case MsgAckNewLeader:
		return "ACK_NEW_LEADER"
	case MsgDiff:
		return "DIFF"
	case MsgSnap:
		return "SNAP"
	case MsgTrunc:
		return "TRUNC"
	case MsgProposal:
		return "PROPOSAL"
	case MsgAck:
		return "ACK"
	case MsgCommit:
		return "COMMIT"
	case MsgPing:
		return "PING"
	case MsgPong:
		return "PONG"
	default:
		return "UNKNOWN"
	}
}

// Message is the base interface for all cluster messages.
type Message interface {
	Type() MessageType
	Encode() []byte
	Decode([]byte) error
}

// Vote represents an election vote.
type Vote struct {
	LeaderID int64 // Proposed leader
	ZXID     int64 // Last transaction seen by voter
	Epoch    int64 // Election epoch
	ServerID int64 // Who cast this vote
}

func (v *Vote) Type() MessageType { return MsgVote }

func (v *Vote) Encode() []byte {
	buf := make([]byte, 1+32) // type + 4 int64s
	buf[0] = byte(MsgVote)
	binary.BigEndian.PutUint64(buf[1:9], uint64(v.LeaderID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(v.ZXID))
	binary.BigEndian.PutUint64(buf[17:25], uint64(v.Epoch))
	binary.BigEndian.PutUint64(buf[25:33], uint64(v.ServerID))
	return buf
}

func (v *Vote) Decode(buf []byte) error {
	if len(buf) < 33 {
		return errors.New("vote buffer too small")
	}
	v.LeaderID = int64(binary.BigEndian.Uint64(buf[1:9]))
	v.ZXID = int64(binary.BigEndian.Uint64(buf[9:17]))
	v.Epoch = int64(binary.BigEndian.Uint64(buf[17:25]))
	v.ServerID = int64(binary.BigEndian.Uint64(buf[25:33]))
	return nil
}

// IsBetterThan returns true if v is a better vote than other.
// Higher ZXID wins; if equal, higher ServerID wins.
func (v *Vote) IsBetterThan(other *Vote) bool {
	if v.ZXID > other.ZXID {
		return true
	}
	if v.ZXID == other.ZXID && v.LeaderID > other.LeaderID {
		return true
	}
	return false
}

// Proposal represents a write proposal from the leader.
type Proposal struct {
	ZXID   int64
	Epoch  int64
	OpType uint8
	Path   string
	Data   []byte
}

func (p *Proposal) Type() MessageType { return MsgProposal }

func (p *Proposal) Encode() []byte {
	pathBytes := []byte(p.Path)
	size := 1 + 8 + 8 + 1 + 4 + len(pathBytes) + 4 + len(p.Data)
	buf := make([]byte, size)

	offset := 0
	buf[offset] = byte(MsgProposal)
	offset++

	binary.BigEndian.PutUint64(buf[offset:], uint64(p.ZXID))
	offset += 8

	binary.BigEndian.PutUint64(buf[offset:], uint64(p.Epoch))
	offset += 8

	buf[offset] = p.OpType
	offset++

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(pathBytes)))
	offset += 4
	copy(buf[offset:], pathBytes)
	offset += len(pathBytes)

	binary.BigEndian.PutUint32(buf[offset:], uint32(len(p.Data)))
	offset += 4
	copy(buf[offset:], p.Data)

	return buf
}

func (p *Proposal) Decode(buf []byte) error {
	if len(buf) < 22 {
		return errors.New("proposal buffer too small")
	}

	offset := 1 // Skip type byte
	p.ZXID = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	p.Epoch = int64(binary.BigEndian.Uint64(buf[offset:]))
	offset += 8

	p.OpType = buf[offset]
	offset++

	pathLen := int(binary.BigEndian.Uint32(buf[offset:]))
	offset += 4
	if offset+pathLen > len(buf) {
		return errors.New("invalid path length")
	}
	p.Path = string(buf[offset : offset+pathLen])
	offset += pathLen

	if offset+4 > len(buf) {
		return errors.New("missing data length")
	}
	dataLen := int(binary.BigEndian.Uint32(buf[offset:]))
	offset += 4
	if offset+dataLen > len(buf) {
		return errors.New("invalid data length")
	}
	p.Data = make([]byte, dataLen)
	copy(p.Data, buf[offset:offset+dataLen])

	return nil
}

// Ack represents an acknowledgment of a proposal.
type Ack struct {
	ZXID     int64
	Epoch    int64
	ServerID int64
}

func (a *Ack) Type() MessageType { return MsgAck }

func (a *Ack) Encode() []byte {
	buf := make([]byte, 1+24)
	buf[0] = byte(MsgAck)
	binary.BigEndian.PutUint64(buf[1:9], uint64(a.ZXID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(a.Epoch))
	binary.BigEndian.PutUint64(buf[17:25], uint64(a.ServerID))
	return buf
}

func (a *Ack) Decode(buf []byte) error {
	if len(buf) < 25 {
		return errors.New("ack buffer too small")
	}
	a.ZXID = int64(binary.BigEndian.Uint64(buf[1:9]))
	a.Epoch = int64(binary.BigEndian.Uint64(buf[9:17]))
	a.ServerID = int64(binary.BigEndian.Uint64(buf[17:25]))
	return nil
}

// Commit signals that a proposal has been committed.
type Commit struct {
	ZXID  int64
	Epoch int64
}

func (c *Commit) Type() MessageType { return MsgCommit }

func (c *Commit) Encode() []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(MsgCommit)
	binary.BigEndian.PutUint64(buf[1:9], uint64(c.ZXID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(c.Epoch))
	return buf
}

func (c *Commit) Decode(buf []byte) error {
	if len(buf) < 17 {
		return errors.New("commit buffer too small")
	}
	c.ZXID = int64(binary.BigEndian.Uint64(buf[1:9]))
	c.Epoch = int64(binary.BigEndian.Uint64(buf[9:17]))
	return nil
}

// FollowerInfo sent by follower during discovery.
type FollowerInfo struct {
	ServerID      int64
	AcceptedEpoch int64
	LastZXID      int64
}

func (f *FollowerInfo) Type() MessageType { return MsgFollowerInfo }

func (f *FollowerInfo) Encode() []byte {
	buf := make([]byte, 1+24)
	buf[0] = byte(MsgFollowerInfo)
	binary.BigEndian.PutUint64(buf[1:9], uint64(f.ServerID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(f.AcceptedEpoch))
	binary.BigEndian.PutUint64(buf[17:25], uint64(f.LastZXID))
	return buf
}

func (f *FollowerInfo) Decode(buf []byte) error {
	if len(buf) < 25 {
		return errors.New("follower info buffer too small")
	}
	f.ServerID = int64(binary.BigEndian.Uint64(buf[1:9]))
	f.AcceptedEpoch = int64(binary.BigEndian.Uint64(buf[9:17]))
	f.LastZXID = int64(binary.BigEndian.Uint64(buf[17:25]))
	return nil
}

// LeaderInfo sent by leader during discovery.
type LeaderInfo struct {
	Epoch int64
}

func (l *LeaderInfo) Type() MessageType { return MsgLeaderInfo }

func (l *LeaderInfo) Encode() []byte {
	buf := make([]byte, 1+8)
	buf[0] = byte(MsgLeaderInfo)
	binary.BigEndian.PutUint64(buf[1:9], uint64(l.Epoch))
	return buf
}

func (l *LeaderInfo) Decode(buf []byte) error {
	if len(buf) < 9 {
		return errors.New("leader info buffer too small")
	}
	l.Epoch = int64(binary.BigEndian.Uint64(buf[1:9]))
	return nil
}

// AckEpoch sent by follower to accept leader's epoch.
type AckEpoch struct {
	ServerID int64
	Epoch    int64
	LastZXID int64
}

func (a *AckEpoch) Type() MessageType { return MsgAckEpoch }

func (a *AckEpoch) Encode() []byte {
	buf := make([]byte, 1+24)
	buf[0] = byte(MsgAckEpoch)
	binary.BigEndian.PutUint64(buf[1:9], uint64(a.ServerID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(a.Epoch))
	binary.BigEndian.PutUint64(buf[17:25], uint64(a.LastZXID))
	return buf
}

func (a *AckEpoch) Decode(buf []byte) error {
	if len(buf) < 25 {
		return errors.New("ack epoch buffer too small")
	}
	a.ServerID = int64(binary.BigEndian.Uint64(buf[1:9]))
	a.Epoch = int64(binary.BigEndian.Uint64(buf[9:17]))
	a.LastZXID = int64(binary.BigEndian.Uint64(buf[17:25]))
	return nil
}

// Ping is a heartbeat request.
type Ping struct {
	ServerID  int64
	Timestamp int64
}

func (p *Ping) Type() MessageType { return MsgPing }

func (p *Ping) Encode() []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(MsgPing)
	binary.BigEndian.PutUint64(buf[1:9], uint64(p.ServerID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(p.Timestamp))
	return buf
}

func (p *Ping) Decode(buf []byte) error {
	if len(buf) < 17 {
		return errors.New("ping buffer too small")
	}
	p.ServerID = int64(binary.BigEndian.Uint64(buf[1:9]))
	p.Timestamp = int64(binary.BigEndian.Uint64(buf[9:17]))
	return nil
}

// Pong is a heartbeat response.
type Pong struct {
	ServerID  int64
	Timestamp int64
}

func (p *Pong) Type() MessageType { return MsgPong }

func (p *Pong) Encode() []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(MsgPong)
	binary.BigEndian.PutUint64(buf[1:9], uint64(p.ServerID))
	binary.BigEndian.PutUint64(buf[9:17], uint64(p.Timestamp))
	return buf
}

func (p *Pong) Decode(buf []byte) error {
	if len(buf) < 17 {
		return errors.New("pong buffer too small")
	}
	p.ServerID = int64(binary.BigEndian.Uint64(buf[1:9]))
	p.Timestamp = int64(binary.BigEndian.Uint64(buf[9:17]))
	return nil
}

// DecodeMessage decodes a message from bytes.
func DecodeMessage(buf []byte) (Message, error) {
	if len(buf) == 0 {
		return nil, errors.New("empty message")
	}

	msgType := MessageType(buf[0])
	var msg Message

	switch msgType {
	case MsgVote:
		msg = &Vote{}
	case MsgProposal:
		msg = &Proposal{}
	case MsgAck:
		msg = &Ack{}
	case MsgCommit:
		msg = &Commit{}
	case MsgFollowerInfo:
		msg = &FollowerInfo{}
	case MsgLeaderInfo:
		msg = &LeaderInfo{}
	case MsgAckEpoch:
		msg = &AckEpoch{}
	case MsgPing:
		msg = &Ping{}
	case MsgPong:
		msg = &Pong{}
	default:
		return nil, errors.New("unknown message type")
	}

	if err := msg.Decode(buf); err != nil {
		return nil, err
	}
	return msg, nil
}
