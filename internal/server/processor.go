package server

import (
	"time"

	"github.com/ichbingautam/zephyr-coord/internal/protocol"
	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// ZooKeeper error codes
const (
	ErrOk                      = 0
	ErrSystemError             = -1
	ErrRuntimeInconsistency    = -2
	ErrDataInconsistency       = -3
	ErrConnectionLoss          = -4
	ErrMarshallingError        = -5
	ErrUnimplemented           = -6
	ErrOperationTimeout        = -7
	ErrBadArguments            = -8
	ErrAPIError                = -100
	ErrNoNode                  = -101
	ErrNoAuth                  = -102
	ErrBadVersion              = -103
	ErrNoChildrenForEphemerals = -108
	ErrNodeExists              = -110
	ErrNotEmpty                = -111
	ErrSessionExpiredCode      = -112
	ErrInvalidCallback         = -113
	ErrInvalidACL              = -114
	ErrAuthFailed              = -115
	ErrSessionMovedCode        = -118
	ErrNotReadOnly             = -119
)

// RequestProcessor processes ZooKeeper requests.
type RequestProcessor struct {
	datastore      *Datastore
	sessionManager *SessionManager
	watchManager   *WatchManager
}

// NewRequestProcessor creates a new request processor.
func NewRequestProcessor(ds *Datastore, sm *SessionManager, wm *WatchManager) *RequestProcessor {
	return &RequestProcessor{
		datastore:      ds,
		sessionManager: sm,
		watchManager:   wm,
	}
}

// HandleConnect processes a connect request.
func (p *RequestProcessor) HandleConnect(conn *Connection, req *protocol.ConnectRequest) (*protocol.ConnectResponse, error) {
	// Create or resume session
	var session *Session
	if req.SessionID != 0 {
		// Try to resume existing session
		var ok bool
		session, ok = p.sessionManager.GetSession(req.SessionID)
		if !ok {
			// Session not found, create new
			session = p.sessionManager.CreateSession(msToTimeout(req.Timeout))
		} else {
			session.Touch()
		}
	} else {
		session = p.sessionManager.CreateSession(msToTimeout(req.Timeout))
	}

	return &protocol.ConnectResponse{
		ProtocolVersion: 0,
		Timeout:         int32(session.Timeout.Milliseconds()),
		SessionID:       session.ID,
		Password:        make([]byte, 16), // TODO: Generate password
		ReadOnly:        false,
	}, nil
}

// HandleRequest processes a request.
func (p *RequestProcessor) HandleRequest(conn *Connection, header *protocol.RequestHeader, data []byte) ([]byte, error) {
	decoder := protocol.NewDecoder(data)
	encoder := protocol.NewEncoder(1024)

	// Touch session
	p.sessionManager.TouchSession(conn.SessionID())

	zxid := int64(p.datastore.CurrentZXID())
	var errCode int32

	switch header.OpCode {
	case protocol.OpPing:
		errCode = p.handlePing(encoder)

	case protocol.OpCreate:
		errCode = p.handleCreate(conn, decoder, encoder)

	case protocol.OpDelete:
		errCode = p.handleDelete(decoder, encoder)

	case protocol.OpExists:
		errCode = p.handleExists(conn, decoder, encoder)

	case protocol.OpGetData:
		errCode = p.handleGetData(conn, decoder, encoder)

	case protocol.OpSetData:
		errCode = p.handleSetData(decoder, encoder)

	case protocol.OpGetChildren:
		errCode = p.handleGetChildren(conn, decoder, encoder)

	case protocol.OpGetChildren2:
		errCode = p.handleGetChildren2(conn, decoder, encoder)

	case protocol.OpCloseSession:
		errCode = p.handleCloseSession(conn)

	default:
		errCode = ErrUnimplemented
	}

	// Build response with header
	respEncoder := protocol.NewEncoder(encoder.Len() + 16)
	replyHeader := &protocol.ReplyHeader{
		Xid:  header.Xid,
		Zxid: zxid,
		Err:  errCode,
	}
	replyHeader.Encode(respEncoder)

	// Append response body only if no error
	if errCode == ErrOk {
		respEncoder.WriteBuffer(encoder.Bytes()[4:]) // Skip length prefix
	}

	return respEncoder.Bytes(), nil
}

func (p *RequestProcessor) handlePing(encoder *protocol.Encoder) int32 {
	// Ping has no body
	return ErrOk
}

func (p *RequestProcessor) handleCreate(conn *Connection, decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.CreateRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	nodeType := flagsToNodeType(req.Flags)
	stat, err := p.datastore.Create(req.Path, req.Data, nodeType, req.ACL, conn.SessionID())
	if err != nil {
		return zkErrorCode(err)
	}

	// Track ephemeral nodes
	if nodeType.IsEphemeral() {
		if session, ok := p.sessionManager.GetSession(conn.SessionID()); ok {
			session.AddEphemeral(req.Path)
		}
	}

	// Trigger watches
	parentPath, _ := zk.SplitPath(req.Path)
	p.watchManager.TriggerNodeCreated(req.Path, parentPath)

	resp := &protocol.CreateResponse{Path: req.Path}
	resp.Encode(encoder)
	_ = stat // TODO: Return stat for Create2

	return ErrOk
}

func (p *RequestProcessor) handleDelete(decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.DeleteRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	if err := p.datastore.Delete(req.Path, req.Version); err != nil {
		return zkErrorCode(err)
	}

	// Trigger watches
	parentPath, _ := zk.SplitPath(req.Path)
	p.watchManager.TriggerNodeDeleted(req.Path, parentPath)

	return ErrOk
}

func (p *RequestProcessor) handleExists(conn *Connection, decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.ExistsRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	stat, err := p.datastore.Exists(req.Path)
	if err != nil {
		return zkErrorCode(err)
	}

	// Register watch
	if req.Watch {
		p.watchManager.AddDataWatch(req.Path, conn.SessionID(), func(e WatchEvent) {
			event := &protocol.WatcherEvent{
				Type:  int32(e.Type),
				State: 3, // SyncConnected
				Path:  e.Path,
			}
			conn.SendNotification(event)
		})
	}

	if stat == nil {
		return ErrNoNode
	}

	resp := &protocol.ExistsResponse{Stat: toProtoStat(stat)}
	resp.Encode(encoder)
	return ErrOk
}

func (p *RequestProcessor) handleGetData(conn *Connection, decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.GetDataRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	data, stat, err := p.datastore.Get(req.Path)
	if err != nil {
		return zkErrorCode(err)
	}

	// Register watch
	if req.Watch {
		p.watchManager.AddDataWatch(req.Path, conn.SessionID(), func(e WatchEvent) {
			event := &protocol.WatcherEvent{
				Type:  int32(e.Type),
				State: 3,
				Path:  e.Path,
			}
			conn.SendNotification(event)
		})
	}

	resp := &protocol.GetDataResponse{
		Data: data,
		Stat: toProtoStat(stat),
	}
	resp.Encode(encoder)
	return ErrOk
}

func (p *RequestProcessor) handleSetData(decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.SetDataRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	stat, err := p.datastore.Set(req.Path, req.Data, req.Version)
	if err != nil {
		return zkErrorCode(err)
	}

	// Trigger watches
	p.watchManager.TriggerNodeDataChanged(req.Path)

	resp := &protocol.SetDataResponse{Stat: toProtoStat(stat)}
	resp.Encode(encoder)
	return ErrOk
}

func (p *RequestProcessor) handleGetChildren(conn *Connection, decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.GetChildrenRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	children, _, err := p.datastore.GetChildren(req.Path)
	if err != nil {
		return zkErrorCode(err)
	}

	// Register watch
	if req.Watch {
		p.watchManager.AddChildWatch(req.Path, conn.SessionID(), func(e WatchEvent) {
			event := &protocol.WatcherEvent{
				Type:  int32(e.Type),
				State: 3,
				Path:  e.Path,
			}
			conn.SendNotification(event)
		})
	}

	resp := &protocol.GetChildrenResponse{Children: children}
	resp.Encode(encoder)
	return ErrOk
}

func (p *RequestProcessor) handleGetChildren2(conn *Connection, decoder *protocol.Decoder, encoder *protocol.Encoder) int32 {
	req := &protocol.GetChildrenRequest{}
	if err := req.Decode(decoder); err != nil {
		return ErrMarshallingError
	}

	children, stat, err := p.datastore.GetChildren(req.Path)
	if err != nil {
		return zkErrorCode(err)
	}

	// Register watch
	if req.Watch {
		p.watchManager.AddChildWatch(req.Path, conn.SessionID(), func(e WatchEvent) {
			event := &protocol.WatcherEvent{
				Type:  int32(e.Type),
				State: 3,
				Path:  e.Path,
			}
			conn.SendNotification(event)
		})
	}

	resp := &protocol.GetChildren2Response{
		Children: children,
		Stat:     toProtoStat(stat),
	}
	resp.Encode(encoder)
	return ErrOk
}

func (p *RequestProcessor) handleCloseSession(conn *Connection) int32 {
	sessionID := conn.SessionID()

	// Remove watches
	p.watchManager.RemoveWatchesForSession(sessionID)

	// Delete ephemerals
	p.datastore.DeleteEphemeralsBySession(sessionID)

	// Close session
	p.sessionManager.CloseSession(sessionID)

	return ErrOk
}

// Helper functions

func flagsToNodeType(flags int32) zk.NodeType {
	ephemeral := flags&1 != 0
	sequential := flags&2 != 0

	switch {
	case ephemeral && sequential:
		return zk.NodeEphemeralSequential
	case ephemeral:
		return zk.NodeEphemeral
	case sequential:
		return zk.NodePersistentSequential
	default:
		return zk.NodePersistent
	}
}

func toProtoStat(zkStat *zk.Stat) *protocol.Stat {
	if zkStat == nil {
		return nil
	}
	return &protocol.Stat{
		Czxid:          zkStat.Czxid,
		Mzxid:          zkStat.Mzxid,
		Ctime:          zkStat.Ctime,
		Mtime:          zkStat.Mtime,
		Version:        zkStat.Version,
		Cversion:       zkStat.Cversion,
		Aversion:       zkStat.Aversion,
		EphemeralOwner: zkStat.EphemeralOwner,
		DataLength:     zkStat.DataLength,
		NumChildren:    zkStat.NumChildren,
		Pzxid:          zkStat.Pzxid,
	}
}

func zkErrorCode(err error) int32 {
	switch err {
	case nil:
		return ErrOk
	case zk.ErrNoNode:
		return ErrNoNode
	case zk.ErrNodeExists:
		return ErrNodeExists
	case zk.ErrBadVersion:
		return ErrBadVersion
	case zk.ErrNotEmpty:
		return ErrNotEmpty
	case zk.ErrNoAuth:
		return ErrNoAuth
	case zk.ErrInvalidACL:
		return ErrInvalidACL
	case zk.ErrEphemeralChild:
		return ErrNoChildrenForEphemerals
	default:
		return ErrSystemError
	}
}

func msToTimeout(ms int32) time.Duration {
	return time.Duration(ms) * time.Millisecond
}
