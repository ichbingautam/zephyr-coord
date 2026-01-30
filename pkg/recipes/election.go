// Package recipes provides distributed coordination primitives.
package recipes

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Common errors for recipes.
var (
	ErrNotLeader       = errors.New("not the leader")
	ErrNoLeader        = errors.New("no leader elected")
	ErrSessionExpired  = errors.New("session expired")
	ErrElectionStopped = errors.New("election stopped")
)

// ZKClient is the interface required by recipes for ZooKeeper operations.
type ZKClient interface {
	// Create creates a node with the given data and flags.
	Create(path string, data []byte, flags int32) (string, error)

	// Delete deletes a node at the given path.
	Delete(path string, version int32) error

	// Exists checks if a node exists and optionally sets a watch.
	Exists(path string, watch bool) (bool, int32, error)

	// GetData gets the data at a path.
	GetData(path string, watch bool) ([]byte, int32, error)

	// GetChildren gets the children of a path.
	GetChildren(path string, watch bool) ([]string, error)

	// SessionID returns the current session ID.
	SessionID() int64

	// WatchEvents returns a channel for watch events.
	WatchEvents() <-chan WatchEvent
}

// WatchEvent represents a ZK watch notification.
type WatchEvent struct {
	Type int32
	Path string
}

// Node flags for ZooKeeper operations.
const (
	FlagEphemeral  int32 = 1
	FlagSequential int32 = 2
)

// LeaderElection implements leader election using ephemeral sequential nodes.
// Based on the ZooKeeper recipe for leader election.
type LeaderElection struct {
	client   ZKClient
	path     string // Election path (e.g., "/election")
	id       string // This participant's identifier
	data     []byte // Data to store in our election node
	nodePath string // Full path of our ephemeral node

	mu        sync.RWMutex
	isLeader  bool
	leaderID  string
	callbacks []func(isLeader bool)
	stopped   bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewLeaderElection creates a new leader election participant.
func NewLeaderElection(client ZKClient, path, id string, data []byte) *LeaderElection {
	ctx, cancel := context.WithCancel(context.Background())
	return &LeaderElection{
		client: client,
		path:   path,
		id:     id,
		data:   data,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins participating in the election.
func (le *LeaderElection) Start() error {
	le.mu.Lock()
	if le.stopped {
		le.mu.Unlock()
		return ErrElectionStopped
	}
	le.mu.Unlock()

	// Ensure election path exists
	exists, _, err := le.client.Exists(le.path, false)
	if err != nil {
		return fmt.Errorf("failed to check election path: %w", err)
	}
	if !exists {
		_, err = le.client.Create(le.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return fmt.Errorf("failed to create election path: %w", err)
		}
	}

	// Create ephemeral sequential node
	nodeName := fmt.Sprintf("%s/n_", le.path)
	createdPath, err := le.client.Create(nodeName, le.data, FlagEphemeral|FlagSequential)
	if err != nil {
		return fmt.Errorf("failed to create election node: %w", err)
	}

	le.mu.Lock()
	le.nodePath = createdPath
	le.mu.Unlock()

	// Start watching for election changes
	go le.watchElection()

	return nil
}

// watchElection monitors the election state.
func (le *LeaderElection) watchElection() {
	for {
		select {
		case <-le.ctx.Done():
			return
		default:
		}

		err := le.checkLeadership()
		if err != nil {
			// Retry after delay
			select {
			case <-le.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		// Wait for watch event
		select {
		case <-le.ctx.Done():
			return
		case event := <-le.client.WatchEvents():
			if strings.HasPrefix(event.Path, le.path) {
				// Election state may have changed
				continue
			}
		case <-time.After(5 * time.Second):
			// Periodic check
		}
	}
}

// checkLeadership determines if we are the leader.
func (le *LeaderElection) checkLeadership() error {
	children, err := le.client.GetChildren(le.path, true)
	if err != nil {
		return err
	}

	if len(children) == 0 {
		return nil
	}

	// Sort children to find the lowest sequence number
	sort.Strings(children)

	le.mu.Lock()
	defer le.mu.Unlock()

	// Extract our node name from full path
	ourNode := le.nodePath[strings.LastIndex(le.nodePath, "/")+1:]

	wasLeader := le.isLeader
	le.isLeader = children[0] == ourNode
	le.leaderID = children[0]

	// Notify callbacks if leadership changed
	if le.isLeader != wasLeader {
		for _, cb := range le.callbacks {
			go cb(le.isLeader)
		}
	}

	return nil
}

// IsLeader returns whether this participant is currently the leader.
func (le *LeaderElection) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

// LeaderID returns the current leader's node ID.
func (le *LeaderElection) LeaderID() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.leaderID
}

// OnLeadershipChange registers a callback for leadership changes.
func (le *LeaderElection) OnLeadershipChange(callback func(isLeader bool)) {
	le.mu.Lock()
	le.callbacks = append(le.callbacks, callback)
	le.mu.Unlock()
}

// Stop withdraws from the election.
func (le *LeaderElection) Stop() error {
	le.mu.Lock()
	le.stopped = true
	le.isLeader = false
	nodePath := le.nodePath
	le.mu.Unlock()

	le.cancel()

	if nodePath != "" {
		return le.client.Delete(nodePath, -1)
	}
	return nil
}

// ID returns this participant's identifier.
func (le *LeaderElection) ID() string {
	return le.id
}

// NodePath returns the full path of our election node.
func (le *LeaderElection) NodePath() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.nodePath
}
