package protocol

import (
	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// OpCode represents a ZooKeeper operation code.
type OpCode int32

const (
	OpNotification    OpCode = 0
	OpCreate          OpCode = 1
	OpDelete          OpCode = 2
	OpExists          OpCode = 3
	OpGetData         OpCode = 4
	OpSetData         OpCode = 5
	OpGetACL          OpCode = 6
	OpSetACL          OpCode = 7
	OpGetChildren     OpCode = 8
	OpSync            OpCode = 9
	OpPing            OpCode = 11
	OpGetChildren2    OpCode = 12
	OpCheck           OpCode = 13
	OpMulti           OpCode = 14
	OpCreate2         OpCode = 15
	OpReconfig        OpCode = 16
	OpCheckWatches    OpCode = 17
	OpRemoveWatches   OpCode = 18
	OpCreateContainer OpCode = 19
	OpDeleteContainer OpCode = 20
	OpCreateTTL       OpCode = 21
	OpMultiRead       OpCode = 22
	OpAuth            OpCode = 100
	OpSetWatches      OpCode = 101
	OpSASL            OpCode = 102
	OpGetEphemerals   OpCode = 103
	OpGetAllChildren  OpCode = 104
	OpSetWatches2     OpCode = 105
	OpAddWatch        OpCode = 106
	OpWhoAmI          OpCode = 107
	OpCreateSession   OpCode = -10
	OpCloseSession    OpCode = -11
	OpError           OpCode = -1
)

func (o OpCode) String() string {
	switch o {
	case OpCreate:
		return "CREATE"
	case OpDelete:
		return "DELETE"
	case OpExists:
		return "EXISTS"
	case OpGetData:
		return "GET_DATA"
	case OpSetData:
		return "SET_DATA"
	case OpGetChildren:
		return "GET_CHILDREN"
	case OpGetChildren2:
		return "GET_CHILDREN2"
	case OpPing:
		return "PING"
	case OpCreateSession:
		return "CREATE_SESSION"
	case OpCloseSession:
		return "CLOSE_SESSION"
	default:
		return "UNKNOWN"
	}
}

// RequestHeader is the common header for all requests.
type RequestHeader struct {
	Xid    int32  // Client sequence number
	OpCode OpCode // Operation type
}

func (h *RequestHeader) Encode(e *Encoder) {
	e.WriteInt(h.Xid)
	e.WriteInt(int32(h.OpCode))
}

func (h *RequestHeader) Decode(d *Decoder) error {
	var err error
	h.Xid, err = d.ReadInt()
	if err != nil {
		return err
	}
	op, err := d.ReadInt()
	if err != nil {
		return err
	}
	h.OpCode = OpCode(op)
	return nil
}

// ReplyHeader is the common header for all responses.
type ReplyHeader struct {
	Xid  int32 // Matches request Xid
	Zxid int64 // Transaction ID
	Err  int32 // Error code (0 = success)
}

func (h *ReplyHeader) Encode(e *Encoder) {
	e.WriteInt(h.Xid)
	e.WriteLong(h.Zxid)
	e.WriteInt(h.Err)
}

func (h *ReplyHeader) Decode(d *Decoder) error {
	var err error
	h.Xid, err = d.ReadInt()
	if err != nil {
		return err
	}
	h.Zxid, err = d.ReadLong()
	if err != nil {
		return err
	}
	h.Err, err = d.ReadInt()
	return err
}

// ConnectRequest is sent to establish a session.
type ConnectRequest struct {
	ProtocolVersion int32
	LastZxidSeen    int64
	Timeout         int32
	SessionID       int64
	Password        []byte
	ReadOnly        bool
}

func (r *ConnectRequest) Encode(e *Encoder) {
	e.WriteInt(r.ProtocolVersion)
	e.WriteLong(r.LastZxidSeen)
	e.WriteInt(r.Timeout)
	e.WriteLong(r.SessionID)
	e.WriteBuffer(r.Password)
	e.WriteBool(r.ReadOnly)
}

func (r *ConnectRequest) Decode(d *Decoder) error {
	var err error
	r.ProtocolVersion, err = d.ReadInt()
	if err != nil {
		return err
	}
	r.LastZxidSeen, err = d.ReadLong()
	if err != nil {
		return err
	}
	r.Timeout, err = d.ReadInt()
	if err != nil {
		return err
	}
	r.SessionID, err = d.ReadLong()
	if err != nil {
		return err
	}
	r.Password, err = d.ReadBuffer()
	if err != nil {
		return err
	}
	// ReadOnly is optional in older clients
	if d.Remaining() > 0 {
		r.ReadOnly, err = d.ReadBool()
	}
	return err
}

// ConnectResponse is returned after session establishment.
type ConnectResponse struct {
	ProtocolVersion int32
	Timeout         int32
	SessionID       int64
	Password        []byte
	ReadOnly        bool
}

func (r *ConnectResponse) Encode(e *Encoder) {
	e.WriteInt(r.ProtocolVersion)
	e.WriteInt(r.Timeout)
	e.WriteLong(r.SessionID)
	e.WriteBuffer(r.Password)
	e.WriteBool(r.ReadOnly)
}

func (r *ConnectResponse) Decode(d *Decoder) error {
	var err error
	r.ProtocolVersion, err = d.ReadInt()
	if err != nil {
		return err
	}
	r.Timeout, err = d.ReadInt()
	if err != nil {
		return err
	}
	r.SessionID, err = d.ReadLong()
	if err != nil {
		return err
	}
	r.Password, err = d.ReadBuffer()
	if err != nil {
		return err
	}
	if d.Remaining() > 0 {
		r.ReadOnly, err = d.ReadBool()
	}
	return err
}

// CreateRequest is sent to create a new node.
type CreateRequest struct {
	Path  string
	Data  []byte
	ACL   []zk.ACL
	Flags int32
}

func (r *CreateRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteBuffer(r.Data)
	encodeACLs(e, r.ACL)
	e.WriteInt(r.Flags)
}

func (r *CreateRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Data, err = d.ReadBuffer()
	if err != nil {
		return err
	}
	r.ACL, err = decodeACLs(d)
	if err != nil {
		return err
	}
	r.Flags, err = d.ReadInt()
	return err
}

// CreateResponse is returned after node creation.
type CreateResponse struct {
	Path string
}

func (r *CreateResponse) Encode(e *Encoder) {
	e.WriteString(r.Path)
}

func (r *CreateResponse) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	return err
}

// DeleteRequest is sent to delete a node.
type DeleteRequest struct {
	Path    string
	Version int32
}

func (r *DeleteRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteInt(r.Version)
}

func (r *DeleteRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Version, err = d.ReadInt()
	return err
}

// ExistsRequest checks if a node exists.
type ExistsRequest struct {
	Path  string
	Watch bool
}

func (r *ExistsRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteBool(r.Watch)
}

func (r *ExistsRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Watch, err = d.ReadBool()
	return err
}

// ExistsResponse is returned from exists check.
type ExistsResponse struct {
	Stat *Stat
}

func (r *ExistsResponse) Encode(e *Encoder) {
	encodeStat(e, r.Stat)
}

func (r *ExistsResponse) Decode(d *Decoder) error {
	var err error
	r.Stat, err = decodeStat(d)
	return err
}

// GetDataRequest retrieves node data.
type GetDataRequest struct {
	Path  string
	Watch bool
}

func (r *GetDataRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteBool(r.Watch)
}

func (r *GetDataRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Watch, err = d.ReadBool()
	return err
}

// GetDataResponse is returned with node data.
type GetDataResponse struct {
	Data []byte
	Stat *Stat
}

func (r *GetDataResponse) Encode(e *Encoder) {
	e.WriteBuffer(r.Data)
	encodeStat(e, r.Stat)
}

func (r *GetDataResponse) Decode(d *Decoder) error {
	var err error
	r.Data, err = d.ReadBuffer()
	if err != nil {
		return err
	}
	r.Stat, err = decodeStat(d)
	return err
}

// SetDataRequest updates node data.
type SetDataRequest struct {
	Path    string
	Data    []byte
	Version int32
}

func (r *SetDataRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteBuffer(r.Data)
	e.WriteInt(r.Version)
}

func (r *SetDataRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Data, err = d.ReadBuffer()
	if err != nil {
		return err
	}
	r.Version, err = d.ReadInt()
	return err
}

// SetDataResponse is returned after data update.
type SetDataResponse struct {
	Stat *Stat
}

func (r *SetDataResponse) Encode(e *Encoder) {
	encodeStat(e, r.Stat)
}

func (r *SetDataResponse) Decode(d *Decoder) error {
	var err error
	r.Stat, err = decodeStat(d)
	return err
}

// GetChildrenRequest retrieves child names.
type GetChildrenRequest struct {
	Path  string
	Watch bool
}

func (r *GetChildrenRequest) Encode(e *Encoder) {
	e.WriteString(r.Path)
	e.WriteBool(r.Watch)
}

func (r *GetChildrenRequest) Decode(d *Decoder) error {
	var err error
	r.Path, err = d.ReadString()
	if err != nil {
		return err
	}
	r.Watch, err = d.ReadBool()
	return err
}

// GetChildrenResponse is returned with child names.
type GetChildrenResponse struct {
	Children []string
}

func (r *GetChildrenResponse) Encode(e *Encoder) {
	e.WriteInt(int32(len(r.Children)))
	for _, c := range r.Children {
		e.WriteString(c)
	}
}

func (r *GetChildrenResponse) Decode(d *Decoder) error {
	count, err := d.ReadInt()
	if err != nil {
		return err
	}
	r.Children = make([]string, count)
	for i := int32(0); i < count; i++ {
		r.Children[i], err = d.ReadString()
		if err != nil {
			return err
		}
	}
	return nil
}

// GetChildren2Response includes stat in response.
type GetChildren2Response struct {
	Children []string
	Stat     *Stat
}

func (r *GetChildren2Response) Encode(e *Encoder) {
	e.WriteInt(int32(len(r.Children)))
	for _, c := range r.Children {
		e.WriteString(c)
	}
	encodeStat(e, r.Stat)
}

func (r *GetChildren2Response) Decode(d *Decoder) error {
	count, err := d.ReadInt()
	if err != nil {
		return err
	}
	r.Children = make([]string, count)
	for i := int32(0); i < count; i++ {
		r.Children[i], err = d.ReadString()
		if err != nil {
			return err
		}
	}
	r.Stat, err = decodeStat(d)
	return err
}

// Stat is the wire-format stat structure.
type Stat struct {
	Czxid          int64
	Mzxid          int64
	Ctime          int64
	Mtime          int64
	Version        int32
	Cversion       int32
	Aversion       int32
	EphemeralOwner int64
	DataLength     int32
	NumChildren    int32
	Pzxid          int64
}

// FromZKStat converts from pkg/zk.Stat.
func (s *Stat) FromZKStat(zkStat *zk.Stat) {
	if zkStat == nil {
		return
	}
	s.Czxid = zkStat.Czxid
	s.Mzxid = zkStat.Mzxid
	s.Pzxid = zkStat.Pzxid
	s.Ctime = zkStat.Ctime
	s.Mtime = zkStat.Mtime
	s.Version = zkStat.Version
	s.Cversion = zkStat.Cversion
	s.Aversion = zkStat.Aversion
	s.EphemeralOwner = zkStat.EphemeralOwner
	s.DataLength = zkStat.DataLength
	s.NumChildren = zkStat.NumChildren
}

func encodeStat(e *Encoder, s *Stat) {
	if s == nil {
		s = &Stat{}
	}
	e.WriteLong(s.Czxid)
	e.WriteLong(s.Mzxid)
	e.WriteLong(s.Ctime)
	e.WriteLong(s.Mtime)
	e.WriteInt(s.Version)
	e.WriteInt(s.Cversion)
	e.WriteInt(s.Aversion)
	e.WriteLong(s.EphemeralOwner)
	e.WriteInt(s.DataLength)
	e.WriteInt(s.NumChildren)
	e.WriteLong(s.Pzxid)
}

func decodeStat(d *Decoder) (*Stat, error) {
	s := &Stat{}
	var err error
	s.Czxid, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	s.Mzxid, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	s.Ctime, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	s.Mtime, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	s.Version, err = d.ReadInt()
	if err != nil {
		return nil, err
	}
	s.Cversion, err = d.ReadInt()
	if err != nil {
		return nil, err
	}
	s.Aversion, err = d.ReadInt()
	if err != nil {
		return nil, err
	}
	s.EphemeralOwner, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	s.DataLength, err = d.ReadInt()
	if err != nil {
		return nil, err
	}
	s.NumChildren, err = d.ReadInt()
	if err != nil {
		return nil, err
	}
	s.Pzxid, err = d.ReadLong()
	if err != nil {
		return nil, err
	}
	return s, nil
}

func encodeACLs(e *Encoder, acls []zk.ACL) {
	e.WriteInt(int32(len(acls)))
	for _, acl := range acls {
		e.WriteInt(int32(acl.Perms))
		e.WriteString(acl.Scheme)
		e.WriteString(acl.ID)
	}
}

func decodeACLs(d *Decoder) ([]zk.ACL, error) {
	count, err := d.ReadInt()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, nil
	}
	acls := make([]zk.ACL, count)
	for i := int32(0); i < count; i++ {
		perms, err := d.ReadInt()
		if err != nil {
			return nil, err
		}
		scheme, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		id, err := d.ReadString()
		if err != nil {
			return nil, err
		}
		acls[i] = zk.ACL{
			Perms:  zk.Permission(perms),
			Scheme: scheme,
			ID:     id,
		}
	}
	return acls, nil
}

// WatcherEvent is sent to notify about watch triggers.
type WatcherEvent struct {
	Type  int32
	State int32
	Path  string
}

func (e *WatcherEvent) Encode(enc *Encoder) {
	enc.WriteInt(e.Type)
	enc.WriteInt(e.State)
	enc.WriteString(e.Path)
}

func (e *WatcherEvent) Decode(d *Decoder) error {
	var err error
	e.Type, err = d.ReadInt()
	if err != nil {
		return err
	}
	e.State, err = d.ReadInt()
	if err != nil {
		return err
	}
	e.Path, err = d.ReadString()
	return err
}
