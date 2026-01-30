// Package recipes - Barrier and Queue implementations.
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

// Barrier errors.
var (
	ErrBarrierBroken = errors.New("barrier broken")
)

// Barrier implements a distributed barrier that blocks until all parties arrive.
type Barrier struct {
	client ZKClient
	path   string
	size   int    // Number of parties required
	id     string // This party's identifier

	mu       sync.RWMutex
	nodePath string
	entered  bool
}

// NewBarrier creates a new distributed barrier.
func NewBarrier(client ZKClient, path string, size int, id string) *Barrier {
	return &Barrier{
		client: client,
		path:   path,
		size:   size,
		id:     id,
	}
}

// Enter enters the barrier and waits for all parties.
func (b *Barrier) Enter(ctx context.Context) error {
	b.mu.Lock()
	if b.entered {
		b.mu.Unlock()
		return nil
	}
	b.mu.Unlock()

	// Ensure barrier path exists
	exists, _, err := b.client.Exists(b.path, false)
	if err != nil {
		return err
	}
	if !exists {
		_, err = b.client.Create(b.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}

	// Create our ephemeral node
	nodeName := fmt.Sprintf("%s/b_", b.path)
	createdPath, err := b.client.Create(nodeName, []byte(b.id), FlagEphemeral|FlagSequential)
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.nodePath = createdPath
	b.entered = true
	b.mu.Unlock()

	// Wait for all parties
	return b.waitForParties(ctx)
}

// waitForParties waits until all parties have entered.
func (b *Barrier) waitForParties(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		children, err := b.client.GetChildren(b.path, true)
		if err != nil {
			return err
		}

		if len(children) >= b.size {
			return nil // All parties present
		}

		// Wait for watch event
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-b.client.WatchEvents():
			continue
		case <-time.After(time.Second):
			continue
		}
	}
}

// Leave leaves the barrier.
func (b *Barrier) Leave() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.nodePath != "" {
		err := b.client.Delete(b.nodePath, -1)
		b.nodePath = ""
		b.entered = false
		return err
	}
	return nil
}

// DoubleBarrier implements entry and exit barriers for coordinated work.
type DoubleBarrier struct {
	client ZKClient
	path   string
	size   int
	id     string

	mu       sync.RWMutex
	nodePath string
}

// NewDoubleBarrier creates a double barrier.
func NewDoubleBarrier(client ZKClient, path string, size int, id string) *DoubleBarrier {
	return &DoubleBarrier{
		client: client,
		path:   path,
		size:   size,
		id:     id,
	}
}

// Enter waits for all parties to enter.
func (db *DoubleBarrier) Enter(ctx context.Context) error {
	// Ensure path exists
	exists, _, err := db.client.Exists(db.path, false)
	if err != nil {
		return err
	}
	if !exists {
		_, err = db.client.Create(db.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}

	// Create our ephemeral node
	nodeName := fmt.Sprintf("%s/b_", db.path)
	createdPath, err := db.client.Create(nodeName, []byte(db.id), FlagEphemeral|FlagSequential)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.nodePath = createdPath
	db.mu.Unlock()

	// Wait for all to enter
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		children, err := db.client.GetChildren(db.path, true)
		if err != nil {
			return err
		}

		if len(children) >= db.size {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-db.client.WatchEvents():
		case <-time.After(time.Second):
		}
	}
}

// Leave waits for all parties to leave.
func (db *DoubleBarrier) Leave(ctx context.Context) error {
	db.mu.Lock()
	nodePath := db.nodePath
	db.mu.Unlock()

	if nodePath == "" {
		return nil
	}

	// Delete our node
	if err := db.client.Delete(nodePath, -1); err != nil {
		return err
	}

	db.mu.Lock()
	db.nodePath = ""
	db.mu.Unlock()

	// Wait for all to leave
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		children, err := db.client.GetChildren(db.path, true)
		if err != nil {
			return err
		}

		if len(children) == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-db.client.WatchEvents():
		case <-time.After(time.Second):
		}
	}
}

// Queue implements a distributed FIFO queue.
type Queue struct {
	client ZKClient
	path   string
}

// NewQueue creates a new distributed queue.
func NewQueue(client ZKClient, path string) *Queue {
	return &Queue{
		client: client,
		path:   path,
	}
}

// Offer adds an item to the queue. Returns immediately.
func (q *Queue) Offer(data []byte) error {
	// Ensure queue path exists
	exists, _, err := q.client.Exists(q.path, false)
	if err != nil {
		return err
	}
	if !exists {
		_, err = q.client.Create(q.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}

	nodeName := fmt.Sprintf("%s/q_", q.path)
	_, err = q.client.Create(nodeName, data, FlagSequential)
	return err
}

// Poll removes and returns the head of the queue.
// Returns nil, nil if queue is empty.
func (q *Queue) Poll() ([]byte, error) {
	children, err := q.client.GetChildren(q.path, false)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, nil
	}

	sort.Strings(children)

	for _, child := range children {
		childPath := q.path + "/" + child
		data, _, err := q.client.GetData(childPath, false)
		if err != nil {
			continue // Node might have been consumed by another client
		}

		err = q.client.Delete(childPath, -1)
		if err != nil {
			continue // Node might have been consumed by another client
		}

		return data, nil
	}

	return nil, nil
}

// Take blocks until an item is available.
func (q *Queue) Take(ctx context.Context) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		data, err := q.Poll()
		if err != nil {
			return nil, err
		}
		if data != nil {
			return data, nil
		}

		// Wait for children to appear
		_, err = q.client.GetChildren(q.path, true)
		if err != nil {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-q.client.WatchEvents():
		case <-time.After(time.Second):
		}
	}
}

// Size returns the number of items in the queue.
func (q *Queue) Size() (int, error) {
	children, err := q.client.GetChildren(q.path, false)
	if err != nil {
		return 0, err
	}
	return len(children), nil
}

// Priority Queue with ordering by priority value.
type PriorityQueue struct {
	client ZKClient
	path   string
}

// NewPriorityQueue creates a priority queue.
func NewPriorityQueue(client ZKClient, path string) *PriorityQueue {
	return &PriorityQueue{
		client: client,
		path:   path,
	}
}

// Offer adds an item with the given priority (lower = higher priority).
func (pq *PriorityQueue) Offer(priority int, data []byte) error {
	exists, _, err := pq.client.Exists(pq.path, false)
	if err != nil {
		return err
	}
	if !exists {
		_, err = pq.client.Create(pq.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}

	nodeName := fmt.Sprintf("%s/p%010d_", pq.path, priority)
	_, err = pq.client.Create(nodeName, data, FlagSequential)
	return err
}

// Poll removes and returns the highest priority item.
func (pq *PriorityQueue) Poll() ([]byte, error) {
	children, err := pq.client.GetChildren(pq.path, false)
	if err != nil {
		return nil, err
	}

	if len(children) == 0 {
		return nil, nil
	}

	// Sort by name (priority is encoded in name)
	sort.Strings(children)

	for _, child := range children {
		childPath := pq.path + "/" + child
		data, _, err := pq.client.GetData(childPath, false)
		if err != nil {
			continue
		}

		err = pq.client.Delete(childPath, -1)
		if err != nil {
			continue
		}

		return data, nil
	}

	return nil, nil
}
