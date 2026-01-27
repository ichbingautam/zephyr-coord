// Package cluster - Follower state machine.
package cluster

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// Follower errors
var (
	ErrLeaderDisconnected = errors.New("leader disconnected")
	ErrSyncFailed         = errors.New("sync with leader failed")
)

// FollowerConfig configures follower behavior.
type FollowerConfig struct {
	ServerID         int64
	LeaderID         int64
	SyncTimeout      time.Duration
	HeartbeatTimeout time.Duration
}

// DefaultFollowerConfig returns default follower configuration.
func DefaultFollowerConfig(serverID, leaderID int64) FollowerConfig {
	return FollowerConfig{
		ServerID:         serverID,
		LeaderID:         leaderID,
		SyncTimeout:      30 * time.Second,
		HeartbeatTimeout: 5 * time.Second,
	}
}

// Follower manages the follower state machine.
type Follower struct {
	config FollowerConfig
	epoch  int64

	mu        sync.RWMutex
	lastZXID  atomic.Int64
	commitIdx atomic.Int64
	lastPing  atomic.Int64

	// Pending proposals awaiting commit
	pending map[int64]*Proposal // zxid -> proposal

	// Communication
	peerManager *PeerManager
	leaderPeer  *Peer

	// Apply callback
	applyFn func(zxid int64, data []byte) error

	// Channels
	proposalCh chan *Proposal
	commitCh   chan *Commit
	pingCh     chan *Ping

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewFollower creates a new follower instance.
func NewFollower(config FollowerConfig, epoch int64, pm *PeerManager) *Follower {
	ctx, cancel := context.WithCancel(context.Background())

	f := &Follower{
		config:      config,
		epoch:       epoch,
		pending:     make(map[int64]*Proposal),
		peerManager: pm,
		proposalCh:  make(chan *Proposal, 1000),
		commitCh:    make(chan *Commit, 1000),
		pingCh:      make(chan *Ping, 100),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Get leader peer
	if peer, ok := pm.GetPeer(config.LeaderID); ok {
		f.leaderPeer = peer
	}

	return f
}

// SetApplyFn sets the callback for applying committed proposals.
func (f *Follower) SetApplyFn(fn func(zxid int64, data []byte) error) {
	f.applyFn = fn
}

// Start starts the follower.
func (f *Follower) Start() error {
	f.lastPing.Store(time.Now().UnixNano())

	f.wg.Add(2)
	go f.messageLoop()
	go f.heartbeatMonitor()

	return nil
}

// Stop stops the follower.
func (f *Follower) Stop() {
	f.cancel()
	f.wg.Wait()
}

// ReceiveProposal handles an incoming proposal.
func (f *Follower) ReceiveProposal(p *Proposal) {
	select {
	case f.proposalCh <- p:
	default:
		// Channel full
	}
}

// ReceiveCommit handles a commit message.
func (f *Follower) ReceiveCommit(c *Commit) {
	select {
	case f.commitCh <- c:
	default:
	}
}

// ReceivePing handles a ping from leader.
func (f *Follower) ReceivePing(p *Ping) {
	select {
	case f.pingCh <- p:
	default:
	}
}

// messageLoop processes incoming messages.
func (f *Follower) messageLoop() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			return

		case proposal := <-f.proposalCh:
			f.handleProposal(proposal)

		case commit := <-f.commitCh:
			f.handleCommit(commit)

		case ping := <-f.pingCh:
			f.handlePing(ping)
		}
	}
}

// handleProposal processes a proposal from the leader.
func (f *Follower) handleProposal(p *Proposal) {
	// Verify epoch
	if p.Epoch != f.epoch {
		return
	}

	// Store pending proposal
	f.mu.Lock()
	f.pending[p.ZXID] = p
	f.mu.Unlock()

	// Update last ZXID
	f.lastZXID.Store(p.ZXID)

	// Send ACK to leader
	ack := &Ack{
		ZXID:     p.ZXID,
		Epoch:    f.epoch,
		ServerID: f.config.ServerID,
	}

	if f.leaderPeer != nil {
		f.leaderPeer.Send(ack.Encode())
	}
}

// handleCommit applies a committed proposal.
func (f *Follower) handleCommit(c *Commit) {
	// Verify epoch
	if c.Epoch != f.epoch {
		return
	}

	f.mu.Lock()
	proposal, ok := f.pending[c.ZXID]
	if ok {
		delete(f.pending, c.ZXID)
	}
	f.mu.Unlock()

	if !ok {
		// Proposal not found - might need sync
		return
	}

	// Apply the proposal
	if f.applyFn != nil {
		f.applyFn(c.ZXID, proposal.Data)
	}

	// Update commit index
	f.commitIdx.Store(c.ZXID)
}

// handlePing responds to leader heartbeat.
func (f *Follower) handlePing(p *Ping) {
	f.lastPing.Store(time.Now().UnixNano())

	// Send pong
	pong := &Pong{
		ServerID:  f.config.ServerID,
		Timestamp: p.Timestamp,
	}

	if f.leaderPeer != nil {
		f.leaderPeer.Send(pong.Encode())
	}
}

// heartbeatMonitor checks for leader heartbeats.
func (f *Follower) heartbeatMonitor() {
	defer f.wg.Done()

	ticker := time.NewTicker(f.config.HeartbeatTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-f.ctx.Done():
			return
		case <-ticker.C:
			lastPing := f.lastPing.Load()
			elapsed := time.Since(time.Unix(0, lastPing))

			if elapsed > f.config.HeartbeatTimeout {
				// Leader timeout - trigger new election
				f.cancel()
				return
			}
		}
	}
}

// Sync synchronizes state with the leader.
func (f *Follower) Sync() error {
	// Send follower info to leader
	info := &FollowerInfo{
		ServerID:      f.config.ServerID,
		AcceptedEpoch: f.epoch,
		LastZXID:      f.lastZXID.Load(),
	}

	if f.leaderPeer == nil {
		return ErrLeaderDisconnected
	}

	if err := f.leaderPeer.Send(info.Encode()); err != nil {
		return err
	}

	// Wait for leader's sync response with timeout
	ctx, cancel := context.WithTimeout(f.ctx, f.config.SyncTimeout)
	defer cancel()

	select {
	case <-ctx.Done():
		if ctx.Err() == context.DeadlineExceeded {
			return ErrSyncFailed
		}
		return ctx.Err()
	default:
		// In a full implementation, we'd wait for DIFF/SNAP/TRUNC messages
		return nil
	}
}

// Epoch returns the current epoch.
func (f *Follower) Epoch() int64 {
	return f.epoch
}

// LastZXID returns the last received ZXID.
func (f *Follower) LastZXID() int64 {
	return f.lastZXID.Load()
}

// CommitIndex returns the last committed ZXID.
func (f *Follower) CommitIndex() int64 {
	return f.commitIdx.Load()
}

// PendingCount returns the number of pending proposals.
func (f *Follower) PendingCount() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.pending)
}

// IsLeaderAlive checks if the leader is responding.
func (f *Follower) IsLeaderAlive() bool {
	lastPing := f.lastPing.Load()
	elapsed := time.Since(time.Unix(0, lastPing))
	return elapsed < f.config.HeartbeatTimeout
}
