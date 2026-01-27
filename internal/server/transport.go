package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ichbingautam/zephyr-coord/internal/protocol"
)

// Transport errors
var (
	ErrServerClosed   = errors.New("server closed")
	ErrSessionExpired = errors.New("session expired")
)

// TransportConfig contains transport configuration.
type TransportConfig struct {
	ListenAddr      string
	MaxConnections  int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxFrameSize    int
	ReadBufferSize  int
	WriteBufferSize int
}

// DefaultTransportConfig returns default transport configuration.
func DefaultTransportConfig() TransportConfig {
	return TransportConfig{
		ListenAddr:      ":2181",
		MaxConnections:  10000,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		MaxFrameSize:    10 * 1024 * 1024, // 10MB
		ReadBufferSize:  64 * 1024,        // 64KB
		WriteBufferSize: 64 * 1024,        // 64KB
	}
}

// Transport handles TCP connections for the ZooKeeper protocol.
type Transport struct {
	config   TransportConfig
	listener net.Listener
	handler  RequestHandler

	connections sync.Map // net.Conn -> *Connection
	connCount   atomic.Int32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Metrics
	totalConnections  atomic.Int64
	activeConnections atomic.Int32
	bytesReceived     atomic.Int64
	bytesSent         atomic.Int64
}

// RequestHandler processes incoming requests.
type RequestHandler interface {
	HandleConnect(conn *Connection, req *protocol.ConnectRequest) (*protocol.ConnectResponse, error)
	HandleRequest(conn *Connection, header *protocol.RequestHeader, data []byte) ([]byte, error)
}

// NewTransport creates a new transport.
func NewTransport(config TransportConfig, handler RequestHandler) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		config:  config,
		handler: handler,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start begins listening for connections.
func (t *Transport) Start() error {
	listener, err := net.Listen("tcp", t.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	t.listener = listener

	t.wg.Add(1)
	go t.acceptLoop()

	return nil
}

// acceptLoop accepts incoming connections.
func (t *Transport) acceptLoop() {
	defer t.wg.Done()

	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.ctx.Done():
				return
			default:
				continue
			}
		}

		if t.connCount.Load() >= int32(t.config.MaxConnections) {
			conn.Close()
			continue
		}

		t.connCount.Add(1)
		t.totalConnections.Add(1)
		t.activeConnections.Add(1)

		connection := newConnection(t, conn)
		t.connections.Store(conn, connection)

		t.wg.Add(1)
		go t.handleConnection(connection)
	}
}

// handleConnection processes a single connection.
func (t *Transport) handleConnection(conn *Connection) {
	defer func() {
		t.wg.Done()
		t.connCount.Add(-1)
		t.activeConnections.Add(-1)
		t.connections.Delete(conn.netConn)
		conn.Close()
	}()

	// Read connect request first
	if err := conn.handleConnect(t.handler); err != nil {
		return
	}

	// Main request loop
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-conn.ctx.Done():
			return
		default:
		}

		if err := conn.handleRequest(t.handler); err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				// Log error
			}
			return
		}
	}
}

// Stop gracefully shuts down the transport.
func (t *Transport) Stop() error {
	t.cancel()

	if t.listener != nil {
		t.listener.Close()
	}

	// Close all connections
	t.connections.Range(func(key, value interface{}) bool {
		conn := value.(*Connection)
		conn.Close()
		return true
	})

	// Wait for all goroutines
	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		return errors.New("shutdown timeout")
	}

	return nil
}

// Stats returns transport statistics.
type TransportStats struct {
	TotalConnections  int64
	ActiveConnections int32
	BytesReceived     int64
	BytesSent         int64
}

func (t *Transport) Stats() TransportStats {
	return TransportStats{
		TotalConnections:  t.totalConnections.Load(),
		ActiveConnections: t.activeConnections.Load(),
		BytesReceived:     t.bytesReceived.Load(),
		BytesSent:         t.bytesSent.Load(),
	}
}

// Addr returns the listener address.
func (t *Transport) Addr() net.Addr {
	if t.listener == nil {
		return nil
	}
	return t.listener.Addr()
}

// Connection represents a client connection.
type Connection struct {
	transport *Transport
	netConn   net.Conn
	reader    *bufio.Reader
	writer    *bufio.Writer
	writeMu   sync.Mutex

	sessionID int64
	connected atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	// Request tracking
	lastXid atomic.Int32
}

func newConnection(t *Transport, netConn net.Conn) *Connection {
	ctx, cancel := context.WithCancel(t.ctx)
	return &Connection{
		transport: t,
		netConn:   netConn,
		reader:    bufio.NewReaderSize(netConn, t.config.ReadBufferSize),
		writer:    bufio.NewWriterSize(netConn, t.config.WriteBufferSize),
		ctx:       ctx,
		cancel:    cancel,
	}
}

// handleConnect processes the initial connect request.
func (c *Connection) handleConnect(handler RequestHandler) error {
	c.netConn.SetReadDeadline(time.Now().Add(c.transport.config.ReadTimeout))

	frameReader := protocol.NewFrameReader(c.reader, c.transport.config.MaxFrameSize)
	frame, err := frameReader.ReadFrame()
	if err != nil {
		return err
	}

	c.transport.bytesReceived.Add(int64(len(frame) + 4))

	// Decode connect request
	decoder := protocol.NewDecoder(frame)
	req := &protocol.ConnectRequest{}
	if err := req.Decode(decoder); err != nil {
		return err
	}

	// Handle connect
	resp, err := handler.HandleConnect(c, req)
	if err != nil {
		return err
	}

	// Send response
	encoder := protocol.NewEncoder(256)
	resp.Encode(encoder)

	if err := c.writeFrame(encoder.Bytes()); err != nil {
		return err
	}

	c.sessionID = resp.SessionID
	c.connected.Store(true)
	return nil
}

// handleRequest processes a single request.
func (c *Connection) handleRequest(handler RequestHandler) error {
	c.netConn.SetReadDeadline(time.Now().Add(c.transport.config.ReadTimeout))

	frameReader := protocol.NewFrameReader(c.reader, c.transport.config.MaxFrameSize)
	frame, err := frameReader.ReadFrame()
	if err != nil {
		return err
	}

	c.transport.bytesReceived.Add(int64(len(frame) + 4))

	// Decode request header
	decoder := protocol.NewDecoder(frame)
	header := &protocol.RequestHeader{}
	if err := header.Decode(decoder); err != nil {
		return err
	}

	c.lastXid.Store(header.Xid)

	// Handle request
	respData, err := handler.HandleRequest(c, header, frame[8:]) // Skip header
	if err != nil {
		// Send error response
		return c.sendError(header.Xid, err)
	}

	return c.writeFrame(respData)
}

// writeFrame writes a length-prefixed frame.
func (c *Connection) writeFrame(data []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.netConn.SetWriteDeadline(time.Now().Add(c.transport.config.WriteTimeout))

	frameWriter := protocol.NewFrameWriter(c.writer)
	if err := frameWriter.WriteFrame(data); err != nil {
		return err
	}

	if err := c.writer.Flush(); err != nil {
		return err
	}

	c.transport.bytesSent.Add(int64(len(data) + 4))
	return nil
}

// sendError sends an error response.
func (c *Connection) sendError(xid int32, err error) error {
	encoder := protocol.NewEncoder(64)

	reply := &protocol.ReplyHeader{
		Xid:  xid,
		Zxid: 0,
		Err:  errorCode(err),
	}
	reply.Encode(encoder)

	return c.writeFrame(encoder.Bytes())
}

// SendNotification sends a watch notification.
func (c *Connection) SendNotification(event *protocol.WatcherEvent) error {
	encoder := protocol.NewEncoder(256)

	// Notification header
	header := &protocol.ReplyHeader{
		Xid:  -1, // Notifications use xid = -1
		Zxid: 0,
		Err:  0,
	}
	header.Encode(encoder)
	event.Encode(encoder)

	return c.writeFrame(encoder.Bytes())
}

// SessionID returns the session ID.
func (c *Connection) SessionID() int64 {
	return c.sessionID
}

// RemoteAddr returns the remote address.
func (c *Connection) RemoteAddr() net.Addr {
	return c.netConn.RemoteAddr()
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.cancel()
	return c.netConn.Close()
}

// errorCode converts an error to a ZooKeeper error code.
func errorCode(err error) int32 {
	// TODO: Map to proper ZK error codes
	switch {
	case err == nil:
		return 0
	default:
		return -1 // SystemError
	}
}
