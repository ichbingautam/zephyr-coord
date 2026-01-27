// Package cluster provides distributed coordination via ZAB protocol.
package cluster

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Peer errors
var (
	ErrPeerNotFound     = errors.New("peer not found")
	ErrPeerDisconnected = errors.New("peer disconnected")
	ErrPeerConnecting   = errors.New("peer still connecting")
)

// PeerState represents the connection state of a peer.
type PeerState uint8

const (
	PeerDisconnected PeerState = iota
	PeerConnecting
	PeerConnected
)

func (s PeerState) String() string {
	switch s {
	case PeerDisconnected:
		return "disconnected"
	case PeerConnecting:
		return "connecting"
	case PeerConnected:
		return "connected"
	default:
		return "unknown"
	}
}

// Peer represents a remote cluster node.
type Peer struct {
	ID      int64
	Address string

	mu       sync.RWMutex
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	state    PeerState
	lastSeen time.Time

	// Send queue
	sendCh chan []byte

	ctx    context.Context
	cancel context.CancelFunc
}

// NewPeer creates a new peer.
func NewPeer(id int64, address string) *Peer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Peer{
		ID:      id,
		Address: address,
		state:   PeerDisconnected,
		sendCh:  make(chan []byte, 1000),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// State returns the current peer state.
func (p *Peer) State() PeerState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.state
}

// Connect establishes connection to the peer.
func (p *Peer) Connect() error {
	p.mu.Lock()
	if p.state == PeerConnected {
		p.mu.Unlock()
		return nil
	}
	p.state = PeerConnecting
	p.mu.Unlock()

	conn, err := net.DialTimeout("tcp", p.Address, 5*time.Second)
	if err != nil {
		p.mu.Lock()
		p.state = PeerDisconnected
		p.mu.Unlock()
		return fmt.Errorf("failed to connect to peer %d: %w", p.ID, err)
	}

	p.mu.Lock()
	p.conn = conn
	p.reader = bufio.NewReaderSize(conn, 64*1024)
	p.writer = bufio.NewWriterSize(conn, 64*1024)
	p.state = PeerConnected
	p.lastSeen = time.Now()
	p.mu.Unlock()

	// Start send loop
	go p.sendLoop()

	return nil
}

// Accept accepts an incoming connection for this peer.
func (p *Peer) Accept(conn net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close existing connection if any
	if p.conn != nil {
		p.conn.Close()
	}

	p.conn = conn
	p.reader = bufio.NewReaderSize(conn, 64*1024)
	p.writer = bufio.NewWriterSize(conn, 64*1024)
	p.state = PeerConnected
	p.lastSeen = time.Now()

	go p.sendLoop()
}

// Close closes the peer connection.
func (p *Peer) Close() error {
	p.cancel()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.state = PeerDisconnected
	return nil
}

// Send queues a message for sending.
func (p *Peer) Send(data []byte) error {
	p.mu.RLock()
	state := p.state
	p.mu.RUnlock()

	if state != PeerConnected {
		return ErrPeerDisconnected
	}

	select {
	case p.sendCh <- data:
		return nil
	default:
		return errors.New("send queue full")
	}
}

// sendLoop handles outgoing messages.
func (p *Peer) sendLoop() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case data := <-p.sendCh:
			p.mu.Lock()
			if p.writer == nil {
				p.mu.Unlock()
				continue
			}

			// Write length prefix
			lenBuf := make([]byte, 4)
			binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))
			if _, err := p.writer.Write(lenBuf); err != nil {
				p.handleWriteError(err)
				p.mu.Unlock()
				continue
			}

			// Write data
			if _, err := p.writer.Write(data); err != nil {
				p.handleWriteError(err)
				p.mu.Unlock()
				continue
			}

			if err := p.writer.Flush(); err != nil {
				p.handleWriteError(err)
				p.mu.Unlock()
				continue
			}
			p.mu.Unlock()
		}
	}
}

func (p *Peer) handleWriteError(err error) {
	p.state = PeerDisconnected
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}

// ReadMessage reads a length-prefixed message.
func (p *Peer) ReadMessage() ([]byte, error) {
	p.mu.RLock()
	reader := p.reader
	p.mu.RUnlock()

	if reader == nil {
		return nil, ErrPeerDisconnected
	}

	// Read length
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(reader, lenBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)

	if length > 10*1024*1024 { // 10MB max
		return nil, errors.New("message too large")
	}

	// Read data
	data := make([]byte, length)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}

	p.mu.Lock()
	p.lastSeen = time.Now()
	p.mu.Unlock()

	return data, nil
}

// PeerManager manages connections to all cluster peers.
type PeerManager struct {
	self     int64
	address  string
	peers    sync.Map // id -> *Peer
	listener net.Listener

	// Stats
	messagesSent     atomic.Int64
	messagesReceived atomic.Int64
	bytesReceived    atomic.Int64
	bytesSent        atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PeerManagerConfig configures peer management.
type PeerManagerConfig struct {
	ServerID   int64
	ListenAddr string
	Peers      map[int64]string // peerID -> address
}

// NewPeerManager creates a new peer manager.
func NewPeerManager(config PeerManagerConfig) *PeerManager {
	ctx, cancel := context.WithCancel(context.Background())
	pm := &PeerManager{
		self:    config.ServerID,
		address: config.ListenAddr,
		ctx:     ctx,
		cancel:  cancel,
	}

	// Initialize peers
	for id, addr := range config.Peers {
		if id != config.ServerID {
			pm.peers.Store(id, NewPeer(id, addr))
		}
	}

	return pm
}

// Start starts the peer manager.
func (pm *PeerManager) Start() error {
	// Start listener for incoming connections
	listener, err := net.Listen("tcp", pm.address)
	if err != nil {
		return fmt.Errorf("failed to start peer listener: %w", err)
	}
	pm.listener = listener

	pm.wg.Add(1)
	go pm.acceptLoop()

	// Connect to all peers
	pm.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		go pm.connectWithRetry(peer)
		return true
	})

	return nil
}

// acceptLoop accepts incoming peer connections.
func (pm *PeerManager) acceptLoop() {
	defer pm.wg.Done()

	for {
		conn, err := pm.listener.Accept()
		if err != nil {
			select {
			case <-pm.ctx.Done():
				return
			default:
				continue
			}
		}

		go pm.handleIncoming(conn)
	}
}

// handleIncoming handles an incoming peer connection.
func (pm *PeerManager) handleIncoming(conn net.Conn) {
	// First message should be server ID
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		conn.Close()
		return
	}

	idBuf := make([]byte, 8)
	if _, err := io.ReadFull(conn, idBuf); err != nil {
		conn.Close()
		return
	}
	peerID := int64(binary.BigEndian.Uint64(idBuf))

	// Find or create peer
	val, ok := pm.peers.Load(peerID)
	if !ok {
		// Unknown peer
		conn.Close()
		return
	}

	peer := val.(*Peer)
	peer.Accept(conn)
}

// connectWithRetry connects to a peer with exponential backoff.
func (pm *PeerManager) connectWithRetry(peer *Peer) {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-pm.ctx.Done():
			return
		default:
		}

		if peer.State() == PeerConnected {
			time.Sleep(5 * time.Second)
			continue
		}

		if err := peer.Connect(); err != nil {
			time.Sleep(backoff)
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		// Send our server ID
		idBuf := make([]byte, 12)
		binary.BigEndian.PutUint32(idBuf[0:4], 8)
		binary.BigEndian.PutUint64(idBuf[4:12], uint64(pm.self))
		peer.Send(idBuf[4:12])

		backoff = time.Second
	}
}

// Stop stops the peer manager.
func (pm *PeerManager) Stop() error {
	pm.cancel()

	if pm.listener != nil {
		pm.listener.Close()
	}

	// Close all peers
	pm.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		peer.Close()
		return true
	})

	pm.wg.Wait()
	return nil
}

// GetPeer returns a peer by ID.
func (pm *PeerManager) GetPeer(id int64) (*Peer, bool) {
	val, ok := pm.peers.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Peer), true
}

// Send sends a message to a specific peer.
func (pm *PeerManager) Send(peerID int64, data []byte) error {
	peer, ok := pm.GetPeer(peerID)
	if !ok {
		return ErrPeerNotFound
	}

	if err := peer.Send(data); err != nil {
		return err
	}

	pm.messagesSent.Add(1)
	pm.bytesSent.Add(int64(len(data)))
	return nil
}

// Broadcast sends a message to all connected peers.
func (pm *PeerManager) Broadcast(data []byte) error {
	var lastErr error
	pm.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if err := peer.Send(data); err != nil {
			lastErr = err
		} else {
			pm.messagesSent.Add(1)
			pm.bytesSent.Add(int64(len(data)))
		}
		return true
	})
	return lastErr
}

// ConnectedPeers returns the number of connected peers.
func (pm *PeerManager) ConnectedPeers() int {
	count := 0
	pm.peers.Range(func(key, value interface{}) bool {
		peer := value.(*Peer)
		if peer.State() == PeerConnected {
			count++
		}
		return true
	})
	return count
}

// AllPeerIDs returns all peer IDs.
func (pm *PeerManager) AllPeerIDs() []int64 {
	var ids []int64
	pm.peers.Range(func(key, value interface{}) bool {
		ids = append(ids, key.(int64))
		return true
	})
	return ids
}

// ServerID returns the local server ID.
func (pm *PeerManager) ServerID() int64 {
	return pm.self
}

// PeerStats returns peer manager statistics.
type PeerStats struct {
	TotalPeers       int
	ConnectedPeers   int
	MessagesSent     int64
	MessagesReceived int64
	BytesSent        int64
	BytesReceived    int64
}

func (pm *PeerManager) Stats() PeerStats {
	total := 0
	connected := 0
	pm.peers.Range(func(key, value interface{}) bool {
		total++
		if value.(*Peer).State() == PeerConnected {
			connected++
		}
		return true
	})

	return PeerStats{
		TotalPeers:       total,
		ConnectedPeers:   connected,
		MessagesSent:     pm.messagesSent.Load(),
		MessagesReceived: pm.messagesReceived.Load(),
		BytesSent:        pm.bytesSent.Load(),
		BytesReceived:    pm.bytesReceived.Load(),
	}
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
