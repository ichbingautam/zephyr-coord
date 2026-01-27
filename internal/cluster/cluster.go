// Package cluster - Main cluster coordinator.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Cluster errors
var (
	ErrNotRunning = errors.New("cluster not running")
	ErrNoLeader   = errors.New("no leader available")
)

// ClusterState represents the cluster state.
type ClusterState uint8

const (
	ClusterStopped ClusterState = iota
	ClusterElecting
	ClusterLeading
	ClusterFollowing
)

func (s ClusterState) String() string {
	switch s {
	case ClusterStopped:
		return "STOPPED"
	case ClusterElecting:
		return "ELECTING"
	case ClusterLeading:
		return "LEADING"
	case ClusterFollowing:
		return "FOLLOWING"
	default:
		return "UNKNOWN"
	}
}

// ClusterConfig configures the cluster.
type ClusterConfig struct {
	ServerID          int64
	PeerListenAddr    string           // Address for peer communication
	Peers             map[int64]string // peerID -> address
	DataDir           string
	ElectionTimeout   time.Duration
	HeartbeatInterval time.Duration
}

// DefaultClusterConfig returns default cluster configuration.
func DefaultClusterConfig(serverID int64, peerAddr string) ClusterConfig {
	return ClusterConfig{
		ServerID:          serverID,
		PeerListenAddr:    peerAddr,
		Peers:             make(map[int64]string),
		ElectionTimeout:   2 * time.Second,
		HeartbeatInterval: 500 * time.Millisecond,
	}
}

// Cluster coordinates distributed consensus.
type Cluster struct {
	config ClusterConfig

	state    atomic.Value // ClusterState
	epoch    atomic.Int64
	lastZXID atomic.Int64
	leaderID atomic.Int64

	peerManager *PeerManager
	election    *Election
	leader      *Leader
	follower    *Follower

	// Apply callback
	applyFn func(zxid int64, data []byte) error

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCluster creates a new cluster instance.
func NewCluster(config ClusterConfig) *Cluster {
	ctx, cancel := context.WithCancel(context.Background())

	c := &Cluster{
		config: config,
		ctx:    ctx,
		cancel: cancel,
	}
	c.state.Store(ClusterStopped)

	return c
}

// SetApplyFn sets the callback for applying committed operations.
func (c *Cluster) SetApplyFn(fn func(zxid int64, data []byte) error) {
	c.applyFn = fn
}

// Start starts the cluster.
func (c *Cluster) Start() error {
	// Initialize peer manager
	pmConfig := PeerManagerConfig{
		ServerID:   c.config.ServerID,
		ListenAddr: c.config.PeerListenAddr,
		Peers:      c.config.Peers,
	}
	c.peerManager = NewPeerManager(pmConfig)

	if err := c.peerManager.Start(); err != nil {
		return fmt.Errorf("failed to start peer manager: %w", err)
	}

	// Give peers time to connect
	time.Sleep(500 * time.Millisecond)

	// Start election
	c.startElection()

	return nil
}

// startElection begins the leader election process.
func (c *Cluster) startElection() {
	c.state.Store(ClusterElecting)

	clusterSize := len(c.config.Peers) + 1
	electionConfig := DefaultElectionConfig(c.config.ServerID, clusterSize)
	electionConfig.InitialVoteZXID = c.lastZXID.Load()

	c.election = NewElection(electionConfig, c.peerManager)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runElection()
	}()
}

// runElection runs the election and transitions to leader/follower.
func (c *Cluster) runElection() {
	resultCh := c.election.Start()

	select {
	case leaderID := <-resultCh:
		c.handleElectionResult(leaderID)
	case <-c.ctx.Done():
		c.election.Stop()
	}
}

// handleElectionResult handles the election outcome.
func (c *Cluster) handleElectionResult(leaderID int64) {
	c.leaderID.Store(leaderID)
	newEpoch := c.epoch.Add(1)

	if leaderID == c.config.ServerID {
		c.becomeLeader(newEpoch)
	} else {
		c.becomeFollower(leaderID, newEpoch)
	}
}

// becomeLeader transitions to leader state.
func (c *Cluster) becomeLeader(epoch int64) {
	c.state.Store(ClusterLeading)

	clusterSize := len(c.config.Peers) + 1
	leaderConfig := DefaultLeaderConfig(c.config.ServerID, clusterSize)
	c.leader = NewLeader(leaderConfig, epoch, c.peerManager)
	c.leader.SetApplyFn(c.applyFn)
	c.leader.Start()

	// Start message handler for leader
	c.wg.Add(1)
	go c.leaderMessageLoop()
}

// becomeFollower transitions to follower state.
func (c *Cluster) becomeFollower(leaderID, epoch int64) {
	c.state.Store(ClusterFollowing)

	followerConfig := DefaultFollowerConfig(c.config.ServerID, leaderID)
	c.follower = NewFollower(followerConfig, epoch, c.peerManager)
	c.follower.SetApplyFn(c.applyFn)

	if err := c.follower.Start(); err != nil {
		// Failed to start follower, trigger re-election
		c.startElection()
		return
	}

	// Start message handler for follower
	c.wg.Add(1)
	go c.followerMessageLoop()
}

// leaderMessageLoop handles incoming messages as leader.
func (c *Cluster) leaderMessageLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			if c.leader != nil {
				c.leader.Stop()
			}
			return
		default:
		}

		// Read from all peers
		for _, peerID := range c.peerManager.AllPeerIDs() {
			peer, ok := c.peerManager.GetPeer(peerID)
			if !ok || peer.State() != PeerConnected {
				continue
			}

			// Non-blocking read with short timeout
			go c.readPeerMessage(peer, true)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// followerMessageLoop handles incoming messages as follower.
func (c *Cluster) followerMessageLoop() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			if c.follower != nil {
				c.follower.Stop()
			}
			return
		default:
		}

		// Check if leader is still alive
		if c.follower != nil && !c.follower.IsLeaderAlive() {
			// Leader timeout, start new election
			c.follower.Stop()
			c.startElection()
			return
		}

		// Read from leader
		if c.follower != nil && c.follower.leaderPeer != nil {
			go c.readPeerMessage(c.follower.leaderPeer, false)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

// readPeerMessage reads and dispatches a message from a peer.
func (c *Cluster) readPeerMessage(peer *Peer, isLeader bool) {
	data, err := peer.ReadMessage()
	if err != nil {
		return
	}

	msg, err := DecodeMessage(data)
	if err != nil {
		return
	}

	c.dispatchMessage(msg, peer.ID, isLeader)
}

// dispatchMessage routes a message to the appropriate handler.
func (c *Cluster) dispatchMessage(msg Message, fromPeerID int64, isLeader bool) {
	switch m := msg.(type) {
	case *Vote:
		if c.election != nil {
			c.election.ReceiveVote(m)
		}

	case *Proposal:
		if !isLeader && c.follower != nil {
			c.follower.ReceiveProposal(m)
		}

	case *Ack:
		if isLeader && c.leader != nil {
			c.leader.ReceiveAck(m)
		}

	case *Commit:
		if !isLeader && c.follower != nil {
			c.follower.ReceiveCommit(m)
		}

	case *Ping:
		if !isLeader && c.follower != nil {
			c.follower.ReceivePing(m)
		}
	}
}

// Stop gracefully stops the cluster.
func (c *Cluster) Stop() error {
	c.cancel()

	if c.leader != nil {
		c.leader.Stop()
	}
	if c.follower != nil {
		c.follower.Stop()
	}
	if c.election != nil {
		c.election.Stop()
	}
	if c.peerManager != nil {
		c.peerManager.Stop()
	}

	c.wg.Wait()
	c.state.Store(ClusterStopped)
	return nil
}

// Propose submits a write operation (only works on leader).
func (c *Cluster) Propose(opType uint8, path string, data []byte) (int64, error) {
	state := c.State()
	if state != ClusterLeading {
		return 0, ErrNotLeader
	}

	if c.leader == nil {
		return 0, ErrNotLeader
	}

	return c.leader.Propose(opType, path, data)
}

// State returns the current cluster state.
func (c *Cluster) State() ClusterState {
	return c.state.Load().(ClusterState)
}

// IsLeader returns true if this node is the leader.
func (c *Cluster) IsLeader() bool {
	return c.State() == ClusterLeading
}

// LeaderID returns the current leader ID.
func (c *Cluster) LeaderID() int64 {
	return c.leaderID.Load()
}

// ServerID returns this server's ID.
func (c *Cluster) ServerID() int64 {
	return c.config.ServerID
}

// Epoch returns the current epoch.
func (c *Cluster) Epoch() int64 {
	return c.epoch.Load()
}

// ClusterStats contains cluster statistics.
type ClusterStats struct {
	ServerID  int64
	State     ClusterState
	LeaderID  int64
	Epoch     int64
	LastZXID  int64
	PeerStats PeerStats
}

// Stats returns cluster statistics.
func (c *Cluster) Stats() ClusterStats {
	var peerStats PeerStats
	if c.peerManager != nil {
		peerStats = c.peerManager.Stats()
	}

	return ClusterStats{
		ServerID:  c.config.ServerID,
		State:     c.State(),
		LeaderID:  c.leaderID.Load(),
		Epoch:     c.epoch.Load(),
		LastZXID:  c.lastZXID.Load(),
		PeerStats: peerStats,
	}
}
