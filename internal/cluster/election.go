// Package cluster - FastLeaderElection implementation.
package cluster

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ElectionState represents the current election state.
type ElectionState uint8

const (
	Looking   ElectionState = iota // Searching for leader
	Following                      // Following elected leader
	Leading                        // We are the leader
)

func (s ElectionState) String() string {
	switch s {
	case Looking:
		return "LOOKING"
	case Following:
		return "FOLLOWING"
	case Leading:
		return "LEADING"
	default:
		return "UNKNOWN"
	}
}

// ElectionConfig configures the election process.
type ElectionConfig struct {
	ServerID        int64
	InitialVoteZXID int64
	QuorumSize      int           // (n/2) + 1
	ElectionTimeout time.Duration // Time to wait before restarting
}

// DefaultElectionConfig returns default election configuration.
func DefaultElectionConfig(serverID int64, clusterSize int) ElectionConfig {
	return ElectionConfig{
		ServerID:        serverID,
		InitialVoteZXID: 0,
		QuorumSize:      (clusterSize / 2) + 1,
		ElectionTimeout: 2 * time.Second,
	}
}

// Election implements the FastLeaderElection algorithm.
type Election struct {
	config ElectionConfig

	mu            sync.RWMutex
	state         ElectionState
	currentVote   Vote            // Our current vote
	votes         map[int64]*Vote // serverID -> Vote
	epoch         atomic.Int64    // Election epoch
	electedLeader int64           // Result of election

	// Communication
	peerManager *PeerManager
	voteCh      chan *Vote

	// Result notification
	resultCh chan int64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewElection creates a new election instance.
func NewElection(config ElectionConfig, pm *PeerManager) *Election {
	ctx, cancel := context.WithCancel(context.Background())

	e := &Election{
		config:      config,
		state:       Looking,
		votes:       make(map[int64]*Vote),
		peerManager: pm,
		voteCh:      make(chan *Vote, 100),
		resultCh:    make(chan int64, 1),
		ctx:         ctx,
		cancel:      cancel,
	}

	// Initialize with vote for self
	e.currentVote = Vote{
		LeaderID: config.ServerID,
		ZXID:     config.InitialVoteZXID,
		Epoch:    1,
		ServerID: config.ServerID,
	}
	e.epoch.Store(1)

	return e
}

// Start begins the election process.
func (e *Election) Start() <-chan int64 {
	e.wg.Add(2)
	go e.electionLoop()
	go e.voteProcessor()

	// Broadcast initial vote
	e.broadcastVote()

	return e.resultCh
}

// Stop stops the election.
func (e *Election) Stop() {
	e.cancel()
	e.wg.Wait()
}

// State returns the current election state.
func (e *Election) State() ElectionState {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.state
}

// SetState sets the election state.
func (e *Election) SetState(state ElectionState) {
	e.mu.Lock()
	e.state = state
	e.mu.Unlock()
}

// Epoch returns the current election epoch.
func (e *Election) Epoch() int64 {
	return e.epoch.Load()
}

// ReceiveVote processes an incoming vote.
func (e *Election) ReceiveVote(v *Vote) {
	select {
	case e.voteCh <- v:
	default:
		// Vote channel full, drop
	}
}

// UpdateZXID updates our ZXID for voting.
func (e *Election) UpdateZXID(zxid int64) {
	e.mu.Lock()
	e.currentVote.ZXID = zxid
	e.mu.Unlock()
}

// electionLoop runs the main election loop.
func (e *Election) electionLoop() {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.ElectionTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			return

		case <-ticker.C:
			e.mu.RLock()
			state := e.state
			e.mu.RUnlock()

			if state == Looking {
				// Check if we have quorum
				if e.hasQuorum() {
					e.concludeElection()
				} else {
					// No quorum yet, increment epoch and retry
					e.incrementEpoch()
					e.broadcastVote()
				}
			}
		}
	}
}

// voteProcessor processes incoming votes.
func (e *Election) voteProcessor() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			return

		case vote := <-e.voteCh:
			e.processVote(vote)
		}
	}
}

// processVote handles an incoming vote.
func (e *Election) processVote(v *Vote) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Ignore votes from old epochs
	currentEpoch := e.epoch.Load()
	if v.Epoch < currentEpoch {
		return
	}

	// If vote is from a newer epoch, update our epoch
	if v.Epoch > currentEpoch {
		e.epoch.Store(v.Epoch)
		e.votes = make(map[int64]*Vote) // Clear old votes

		// If the incoming vote is better, adopt it
		if v.IsBetterThan(&e.currentVote) {
			e.currentVote = Vote{
				LeaderID: v.LeaderID,
				ZXID:     e.currentVote.ZXID, // Keep our ZXID
				Epoch:    v.Epoch,
				ServerID: e.config.ServerID,
			}
			go e.broadcastVote()
		}
	}

	// Record this vote
	e.votes[v.ServerID] = v

	// Also record our own vote
	e.votes[e.config.ServerID] = &e.currentVote

	// If this vote proposes a better leader than our current choice
	if v.Epoch == currentEpoch && v.IsBetterThan(&e.currentVote) {
		e.currentVote = Vote{
			LeaderID: v.LeaderID,
			ZXID:     e.currentVote.ZXID,
			Epoch:    currentEpoch,
			ServerID: e.config.ServerID,
		}
		go e.broadcastVote()
	}

	// Check for quorum
	if e.hasQuorumLocked() {
		e.concludeElectionLocked()
	}
}

// hasQuorum checks if we have a quorum of votes for the same leader.
func (e *Election) hasQuorum() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.hasQuorumLocked()
}

func (e *Election) hasQuorumLocked() bool {
	if len(e.votes) < e.config.QuorumSize {
		return false
	}

	// Count votes for each proposed leader
	voteCounts := make(map[int64]int)
	for _, v := range e.votes {
		voteCounts[v.LeaderID]++
	}

	// Check if any leader has quorum
	for _, count := range voteCounts {
		if count >= e.config.QuorumSize {
			return true
		}
	}

	return false
}

// findWinner returns the leader with quorum support.
func (e *Election) findWinner() int64 {
	voteCounts := make(map[int64]int)
	for _, v := range e.votes {
		voteCounts[v.LeaderID]++
	}

	for leader, count := range voteCounts {
		if count >= e.config.QuorumSize {
			return leader
		}
	}

	return -1
}

// concludeElection finalizes the election.
func (e *Election) concludeElection() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.concludeElectionLocked()
}

func (e *Election) concludeElectionLocked() {
	if e.state != Looking {
		return // Already concluded
	}

	winner := e.findWinner()
	if winner < 0 {
		return // No winner yet
	}

	e.electedLeader = winner

	if winner == e.config.ServerID {
		e.state = Leading
	} else {
		e.state = Following
	}

	// Notify result
	select {
	case e.resultCh <- winner:
	default:
	}
}

// incrementEpoch increments the election epoch and resets votes.
func (e *Election) incrementEpoch() {
	e.mu.Lock()
	defer e.mu.Unlock()

	newEpoch := e.epoch.Add(1)
	e.currentVote.Epoch = newEpoch
	e.votes = make(map[int64]*Vote)
}

// broadcastVote sends our current vote to all peers.
func (e *Election) broadcastVote() {
	e.mu.RLock()
	vote := e.currentVote
	e.mu.RUnlock()

	data := vote.Encode()
	e.peerManager.Broadcast(data)
}

// RestartElection starts a new election round.
func (e *Election) RestartElection() {
	e.mu.Lock()
	e.state = Looking
	e.incrementEpochLocked()
	e.votes = make(map[int64]*Vote)
	e.mu.Unlock()

	e.broadcastVote()
}

func (e *Election) incrementEpochLocked() {
	newEpoch := e.epoch.Add(1)
	e.currentVote.Epoch = newEpoch
}

// ElectedLeader returns the elected leader ID.
func (e *Election) ElectedLeader() int64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.electedLeader
}

// VoteCount returns the current number of votes.
func (e *Election) VoteCount() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.votes)
}

// CurrentVote returns our current vote.
func (e *Election) CurrentVote() Vote {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.currentVote
}
