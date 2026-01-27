package cluster

import (
	"testing"
)

func TestVote_IsBetterThan(t *testing.T) {
	tests := []struct {
		name     string
		v1       Vote
		v2       Vote
		expected bool
	}{
		{
			name:     "higher ZXID wins",
			v1:       Vote{LeaderID: 1, ZXID: 100, Epoch: 1, ServerID: 1},
			v2:       Vote{LeaderID: 2, ZXID: 50, Epoch: 1, ServerID: 2},
			expected: true,
		},
		{
			name:     "equal ZXID, higher leader ID wins",
			v1:       Vote{LeaderID: 3, ZXID: 100, Epoch: 1, ServerID: 3},
			v2:       Vote{LeaderID: 2, ZXID: 100, Epoch: 1, ServerID: 2},
			expected: true,
		},
		{
			name:     "lower ZXID loses",
			v1:       Vote{LeaderID: 1, ZXID: 50, Epoch: 1, ServerID: 1},
			v2:       Vote{LeaderID: 2, ZXID: 100, Epoch: 1, ServerID: 2},
			expected: false,
		},
		{
			name:     "equal ZXID, lower leader ID loses",
			v1:       Vote{LeaderID: 1, ZXID: 100, Epoch: 1, ServerID: 1},
			v2:       Vote{LeaderID: 2, ZXID: 100, Epoch: 1, ServerID: 2},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.v1.IsBetterThan(&tc.v2)
			if result != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, result)
			}
		})
	}
}

func TestVote_EncodeDecode(t *testing.T) {
	original := Vote{
		LeaderID: 123,
		ZXID:     456789,
		Epoch:    5,
		ServerID: 42,
	}

	data := original.Encode()

	decoded := &Vote{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.LeaderID != original.LeaderID {
		t.Errorf("LeaderID mismatch: %d != %d", decoded.LeaderID, original.LeaderID)
	}
	if decoded.ZXID != original.ZXID {
		t.Errorf("ZXID mismatch: %d != %d", decoded.ZXID, original.ZXID)
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch mismatch: %d != %d", decoded.Epoch, original.Epoch)
	}
	if decoded.ServerID != original.ServerID {
		t.Errorf("ServerID mismatch: %d != %d", decoded.ServerID, original.ServerID)
	}
}

func TestProposal_EncodeDecode(t *testing.T) {
	original := Proposal{
		ZXID:   (5 << 32) | 100,
		Epoch:  5,
		OpType: 1,
		Path:   "/test/node",
		Data:   []byte("hello world"),
	}

	data := original.Encode()

	decoded := &Proposal{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ZXID != original.ZXID {
		t.Errorf("ZXID mismatch: %d != %d", decoded.ZXID, original.ZXID)
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch mismatch: %d != %d", decoded.Epoch, original.Epoch)
	}
	if decoded.OpType != original.OpType {
		t.Errorf("OpType mismatch: %d != %d", decoded.OpType, original.OpType)
	}
	if decoded.Path != original.Path {
		t.Errorf("Path mismatch: %s != %s", decoded.Path, original.Path)
	}
	if string(decoded.Data) != string(original.Data) {
		t.Errorf("Data mismatch: %s != %s", decoded.Data, original.Data)
	}
}

func TestAck_EncodeDecode(t *testing.T) {
	original := Ack{
		ZXID:     12345,
		Epoch:    3,
		ServerID: 7,
	}

	data := original.Encode()

	decoded := &Ack{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ZXID != original.ZXID {
		t.Errorf("ZXID mismatch")
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch mismatch")
	}
	if decoded.ServerID != original.ServerID {
		t.Errorf("ServerID mismatch")
	}
}

func TestCommit_EncodeDecode(t *testing.T) {
	original := Commit{
		ZXID:  67890,
		Epoch: 10,
	}

	data := original.Encode()

	decoded := &Commit{}
	if err := decoded.Decode(data); err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ZXID != original.ZXID {
		t.Errorf("ZXID mismatch")
	}
	if decoded.Epoch != original.Epoch {
		t.Errorf("Epoch mismatch")
	}
}

func TestDecodeMessage(t *testing.T) {
	vote := Vote{LeaderID: 1, ZXID: 100, Epoch: 1, ServerID: 1}
	data := vote.Encode()

	msg, err := DecodeMessage(data)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	decodedVote, ok := msg.(*Vote)
	if !ok {
		t.Fatalf("expected *Vote, got %T", msg)
	}

	if decodedVote.LeaderID != vote.LeaderID {
		t.Errorf("LeaderID mismatch")
	}
}

func TestPeer_Basic(t *testing.T) {
	peer := NewPeer(1, "127.0.0.1:2888")

	if peer.ID != 1 {
		t.Errorf("ID mismatch: %d != 1", peer.ID)
	}
	if peer.Address != "127.0.0.1:2888" {
		t.Errorf("Address mismatch")
	}
	if peer.State() != PeerDisconnected {
		t.Errorf("expected disconnected state")
	}
}

func TestPeerState_String(t *testing.T) {
	if PeerDisconnected.String() != "disconnected" {
		t.Error("wrong string for disconnected")
	}
	if PeerConnecting.String() != "connecting" {
		t.Error("wrong string for connecting")
	}
	if PeerConnected.String() != "connected" {
		t.Error("wrong string for connected")
	}
}

func TestElectionState_String(t *testing.T) {
	if Looking.String() != "LOOKING" {
		t.Error("wrong string for looking")
	}
	if Following.String() != "FOLLOWING" {
		t.Error("wrong string for following")
	}
	if Leading.String() != "LEADING" {
		t.Error("wrong string for leading")
	}
}

func TestClusterState_String(t *testing.T) {
	if ClusterStopped.String() != "STOPPED" {
		t.Error("wrong string for stopped")
	}
	if ClusterElecting.String() != "ELECTING" {
		t.Error("wrong string for electing")
	}
	if ClusterLeading.String() != "LEADING" {
		t.Error("wrong string for leading")
	}
	if ClusterFollowing.String() != "FOLLOWING" {
		t.Error("wrong string for following")
	}
}

func BenchmarkVote_Encode(b *testing.B) {
	vote := Vote{LeaderID: 1, ZXID: 100, Epoch: 1, ServerID: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = vote.Encode()
	}
}

func BenchmarkVote_Decode(b *testing.B) {
	vote := Vote{LeaderID: 1, ZXID: 100, Epoch: 1, ServerID: 1}
	data := vote.Encode()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v := &Vote{}
		v.Decode(data)
	}
}

func BenchmarkProposal_Encode(b *testing.B) {
	proposal := Proposal{
		ZXID:   12345,
		Epoch:  1,
		OpType: 1,
		Path:   "/test/node",
		Data:   []byte("hello world this is test data"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = proposal.Encode()
	}
}
