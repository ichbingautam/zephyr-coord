// Package cluster - Leader state machine for ZAB broadcast.
package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Leader errors
var (
	ErrNotLeader       = errors.New("not the leader")
	ErrQuorumLost      = errors.New("lost quorum")
	ErrProposalTimeout = errors.New("proposal timeout")
)

// LeaderConfig configures leader behavior.
type LeaderConfig struct {
	ServerID          int64
	QuorumSize        int
	ProposalTimeout   time.Duration
	HeartbeatInterval time.Duration
}

// DefaultLeaderConfig returns default leader configuration.
func DefaultLeaderConfig(serverID int64, clusterSize int) LeaderConfig {
	return LeaderConfig{
		ServerID:          serverID,
		QuorumSize:        (clusterSize / 2) + 1,
		ProposalTimeout:   5 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
	}
}

// PendingProposal tracks a proposal awaiting quorum.
type PendingProposal struct {
	Proposal *Proposal
	Acks     map[int64]bool
	Done     chan error
	Timer    *time.Timer
}

// Leader manages the leader state machine.
type Leader struct {
	config LeaderConfig
	epoch  int64

	mu        sync.RWMutex
	proposals map[int64]*PendingProposal // zxid -> pending
	lastZXID  atomic.Int64
	commitIdx atomic.Int64

	// Followers
	followers sync.Map // followerID -> *FollowerHandler

	// Communication
	peerManager *PeerManager
	proposeCh   chan *proposalRequest
	ackCh       chan *Ack

	// Apply callback
	applyFn func(zxid int64, data []byte) error

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type proposalRequest struct {
	data   []byte
	opType uint8
	path   string
	respCh chan proposalResponse
}

type proposalResponse struct {
	zxid int64
	err  error
}

// NewLeader creates a new leader instance.
func NewLeader(config LeaderConfig, epoch int64, pm *PeerManager) *Leader {
	ctx, cancel := context.WithCancel(context.Background())

	return &Leader{
		config:      config,
		epoch:       epoch,
		proposals:   make(map[int64]*PendingProposal),
		peerManager: pm,
		proposeCh:   make(chan *proposalRequest, 1000),
		ackCh:       make(chan *Ack, 1000),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// SetApplyFn sets the callback for applying committed proposals.
func (l *Leader) SetApplyFn(fn func(zxid int64, data []byte) error) {
	l.applyFn = fn
}

// Start starts the leader.
func (l *Leader) Start() {
	l.wg.Add(2)
	go l.proposalLoop()
	go l.heartbeatLoop()
}

// Stop stops the leader.
func (l *Leader) Stop() {
	l.cancel()
	l.wg.Wait()
}

// Propose submits a new proposal.
func (l *Leader) Propose(opType uint8, path string, data []byte) (int64, error) {
	respCh := make(chan proposalResponse, 1)

	req := &proposalRequest{
		data:   data,
		opType: opType,
		path:   path,
		respCh: respCh,
	}

	select {
	case l.proposeCh <- req:
	case <-l.ctx.Done():
		return 0, ErrNotLeader
	}

	select {
	case resp := <-respCh:
		return resp.zxid, resp.err
	case <-l.ctx.Done():
		return 0, ErrNotLeader
	}
}

// ReceiveAck processes an acknowledgment.
func (l *Leader) ReceiveAck(ack *Ack) {
	select {
	case l.ackCh <- ack:
	default:
		// Ack channel full
	}
}

// proposalLoop handles proposals and acks.
func (l *Leader) proposalLoop() {
	defer l.wg.Done()

	for {
		select {
		case <-l.ctx.Done():
			return

		case req := <-l.proposeCh:
			l.handleProposal(req)

		case ack := <-l.ackCh:
			l.handleAck(ack)
		}
	}
}

// handleProposal creates and broadcasts a new proposal.
func (l *Leader) handleProposal(req *proposalRequest) {
	// Generate ZXID
	zxid := l.nextZXID()

	proposal := &Proposal{
		ZXID:   zxid,
		Epoch:  l.epoch,
		OpType: req.opType,
		Path:   req.path,
		Data:   req.data,
	}

	// Create pending proposal
	pending := &PendingProposal{
		Proposal: proposal,
		Acks:     make(map[int64]bool),
		Done:     make(chan error, 1),
		Timer:    time.NewTimer(l.config.ProposalTimeout),
	}

	// Leader implicitly acks its own proposal
	pending.Acks[l.config.ServerID] = true

	l.mu.Lock()
	l.proposals[zxid] = pending
	l.mu.Unlock()

	// Broadcast proposal
	data := proposal.Encode()
	l.peerManager.Broadcast(data)

	// Wait for quorum or timeout
	go func() {
		select {
		case err := <-pending.Done:
			req.respCh <- proposalResponse{zxid: zxid, err: err}
		case <-pending.Timer.C:
			l.mu.Lock()
			delete(l.proposals, zxid)
			l.mu.Unlock()
			req.respCh <- proposalResponse{zxid: 0, err: ErrProposalTimeout}
		case <-l.ctx.Done():
			req.respCh <- proposalResponse{zxid: 0, err: ErrNotLeader}
		}
		pending.Timer.Stop()
	}()
}

// handleAck processes an acknowledgment.
func (l *Leader) handleAck(ack *Ack) {
	// Verify epoch
	if ack.Epoch != l.epoch {
		return
	}

	l.mu.Lock()
	pending, ok := l.proposals[ack.ZXID]
	if !ok {
		l.mu.Unlock()
		return
	}

	// Record ack
	pending.Acks[ack.ServerID] = true
	ackCount := len(pending.Acks)
	l.mu.Unlock()

	// Check for quorum
	if ackCount >= l.config.QuorumSize {
		l.commitProposal(ack.ZXID)
	}
}

// commitProposal commits a proposal and broadcasts commit.
func (l *Leader) commitProposal(zxid int64) {
	l.mu.Lock()
	pending, ok := l.proposals[zxid]
	if !ok {
		l.mu.Unlock()
		return
	}
	delete(l.proposals, zxid)
	l.mu.Unlock()

	// Update commit index
	l.commitIdx.Store(zxid)

	// Apply locally
	if l.applyFn != nil {
		l.applyFn(zxid, pending.Proposal.Data)
	}

	// Broadcast commit
	commit := &Commit{
		ZXID:  zxid,
		Epoch: l.epoch,
	}
	l.peerManager.Broadcast(commit.Encode())

	// Signal completion
	select {
	case pending.Done <- nil:
	default:
	}
}

// nextZXID generates the next ZXID.
func (l *Leader) nextZXID() int64 {
	// ZXID format: epoch(32) | counter(32)
	counter := l.lastZXID.Add(1)
	return (l.epoch << 32) | (counter & 0xFFFFFFFF)
}

// heartbeatLoop sends periodic heartbeats to followers.
func (l *Leader) heartbeatLoop() {
	defer l.wg.Done()

	ticker := time.NewTicker(l.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			ping := &Ping{
				ServerID:  l.config.ServerID,
				Timestamp: time.Now().UnixNano(),
			}
			l.peerManager.Broadcast(ping.Encode())
		}
	}
}

// Epoch returns the current epoch.
func (l *Leader) Epoch() int64 {
	return l.epoch
}

// LastZXID returns the last proposed ZXID.
func (l *Leader) LastZXID() int64 {
	return l.lastZXID.Load()
}

// CommitIndex returns the last committed ZXID.
func (l *Leader) CommitIndex() int64 {
	return l.commitIdx.Load()
}

// PendingCount returns the number of pending proposals.
func (l *Leader) PendingCount() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.proposals)
}

// FollowerHandler tracks a follower's state.
type FollowerHandler struct {
	ID       int64
	LastZXID int64
	LastAck  time.Time
	Synced   bool
}
