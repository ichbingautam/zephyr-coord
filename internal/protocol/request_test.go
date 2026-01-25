package protocol

import (
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestRequestHeader_EncodeDecode(t *testing.T) {
	orig := &RequestHeader{Xid: 123, OpCode: OpCreate}

	e := NewEncoder(64)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &RequestHeader{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Xid != orig.Xid || decoded.OpCode != orig.OpCode {
		t.Errorf("Decoded = %+v, want %+v", decoded, orig)
	}
}

func TestReplyHeader_EncodeDecode(t *testing.T) {
	orig := &ReplyHeader{Xid: 456, Zxid: 789, Err: 0}

	e := NewEncoder(64)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &ReplyHeader{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Xid != orig.Xid || decoded.Zxid != orig.Zxid || decoded.Err != orig.Err {
		t.Errorf("Decoded = %+v, want %+v", decoded, orig)
	}
}

func TestConnectRequest_EncodeDecode(t *testing.T) {
	orig := &ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    100,
		Timeout:         30000,
		SessionID:       0,
		Password:        []byte("secret"),
		ReadOnly:        false,
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &ConnectRequest{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Timeout != orig.Timeout {
		t.Errorf("Timeout = %d, want %d", decoded.Timeout, orig.Timeout)
	}
}

func TestCreateRequest_EncodeDecode(t *testing.T) {
	orig := &CreateRequest{
		Path:  "/test/node",
		Data:  []byte("node data"),
		ACL:   zk.WorldACL(),
		Flags: 1, // Ephemeral
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &CreateRequest{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Path != orig.Path {
		t.Errorf("Path = %s, want %s", decoded.Path, orig.Path)
	}
	if string(decoded.Data) != string(orig.Data) {
		t.Errorf("Data = %s, want %s", decoded.Data, orig.Data)
	}
	if len(decoded.ACL) != len(orig.ACL) {
		t.Errorf("ACL count = %d, want %d", len(decoded.ACL), len(orig.ACL))
	}
}

func TestGetDataRequest_EncodeDecode(t *testing.T) {
	orig := &GetDataRequest{
		Path:  "/data/path",
		Watch: true,
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &GetDataRequest{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Path != orig.Path || decoded.Watch != orig.Watch {
		t.Errorf("Decoded = %+v, want %+v", decoded, orig)
	}
}

func TestGetDataResponse_EncodeDecode(t *testing.T) {
	orig := &GetDataResponse{
		Data: []byte("response data"),
		Stat: &Stat{
			Czxid:      100,
			Mzxid:      200,
			Ctime:      1000,
			Mtime:      2000,
			Version:    5,
			DataLength: 13,
		},
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &GetDataResponse{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if string(decoded.Data) != string(orig.Data) {
		t.Errorf("Data = %s, want %s", decoded.Data, orig.Data)
	}
	if decoded.Stat.Version != orig.Stat.Version {
		t.Errorf("Stat.Version = %d, want %d", decoded.Stat.Version, orig.Stat.Version)
	}
}

func TestGetChildrenResponse_EncodeDecode(t *testing.T) {
	orig := &GetChildrenResponse{
		Children: []string{"child1", "child2", "child3"},
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &GetChildrenResponse{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if len(decoded.Children) != len(orig.Children) {
		t.Errorf("Children count = %d, want %d", len(decoded.Children), len(orig.Children))
	}
	for i, c := range decoded.Children {
		if c != orig.Children[i] {
			t.Errorf("Child %d = %s, want %s", i, c, orig.Children[i])
		}
	}
}

func TestWatcherEvent_EncodeDecode(t *testing.T) {
	orig := &WatcherEvent{
		Type:  3, // NodeDataChanged
		State: 3, // SyncConnected
		Path:  "/watched/path",
	}

	e := NewEncoder(256)
	orig.Encode(e)

	d := NewDecoder(e.Bytes())
	decoded := &WatcherEvent{}
	if err := decoded.Decode(d); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.Type != orig.Type || decoded.State != orig.State || decoded.Path != orig.Path {
		t.Errorf("Decoded = %+v, want %+v", decoded, orig)
	}
}

func BenchmarkCreateRequest_Encode(b *testing.B) {
	req := &CreateRequest{
		Path:  "/benchmark/test",
		Data:  make([]byte, 1024),
		ACL:   zk.WorldACL(),
		Flags: 0,
	}
	e := NewEncoder(2048)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Reset()
		req.Encode(e)
	}
}

func BenchmarkCreateRequest_Decode(b *testing.B) {
	req := &CreateRequest{
		Path:  "/benchmark/test",
		Data:  make([]byte, 1024),
		ACL:   zk.WorldACL(),
		Flags: 0,
	}
	e := NewEncoder(2048)
	req.Encode(e)
	data := e.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := NewDecoder(data)
		decoded := &CreateRequest{}
		decoded.Decode(d)
	}
}
