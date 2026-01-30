// Package recipes - Distributed lock implementation.
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

// Lock errors.
var (
	ErrLockTimeout   = errors.New("lock acquisition timeout")
	ErrLockNotHeld   = errors.New("lock not held")
	ErrLockInterrupt = errors.New("lock acquisition interrupted")
)

// DistributedLock implements a fair distributed lock.
// Uses ephemeral sequential nodes to ensure fairness (FIFO ordering).
type DistributedLock struct {
	client   ZKClient
	path     string // Lock path (e.g., "/locks/mylock")
	id       string // Lock holder identifier
	nodePath string // Our ephemeral node path

	mu     sync.RWMutex
	held   bool
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDistributedLock creates a new distributed lock.
func NewDistributedLock(client ZKClient, path, id string) *DistributedLock {
	ctx, cancel := context.WithCancel(context.Background())
	return &DistributedLock{
		client: client,
		path:   path,
		id:     id,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Lock acquires the lock, blocking until it's obtained or context is cancelled.
func (l *DistributedLock) Lock(ctx context.Context) error {
	return l.LockWithTimeout(ctx, 0) // 0 means wait forever
}

// LockWithTimeout attempts to acquire the lock within the given timeout.
// A timeout of 0 means wait indefinitely.
func (l *DistributedLock) LockWithTimeout(ctx context.Context, timeout time.Duration) error {
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return nil // Already holding the lock
	}
	l.mu.Unlock()

	// Setup timeout context
	lockCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		lockCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	// Ensure lock path exists
	if err := l.ensurePath(); err != nil {
		return err
	}

	// Create ephemeral sequential node
	nodeName := fmt.Sprintf("%s/lock-", l.path)
	createdPath, err := l.client.Create(nodeName, []byte(l.id), FlagEphemeral|FlagSequential)
	if err != nil {
		return fmt.Errorf("failed to create lock node: %w", err)
	}

	l.mu.Lock()
	l.nodePath = createdPath
	l.mu.Unlock()

	// Try to acquire the lock
	return l.tryAcquire(lockCtx)
}

// ensurePath creates the lock path if it doesn't exist.
func (l *DistributedLock) ensurePath() error {
	exists, _, err := l.client.Exists(l.path, false)
	if err != nil {
		return err
	}
	if !exists {
		_, err = l.client.Create(l.path, nil, 0)
		if err != nil && !strings.Contains(err.Error(), "exists") {
			return err
		}
	}
	return nil
}

// tryAcquire attempts to acquire the lock.
func (l *DistributedLock) tryAcquire(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Clean up our node on timeout/cancel
			l.cleanup()
			if ctx.Err() == context.DeadlineExceeded {
				return ErrLockTimeout
			}
			return ErrLockInterrupt
		case <-l.ctx.Done():
			l.cleanup()
			return ErrLockInterrupt
		default:
		}

		children, err := l.client.GetChildren(l.path, true)
		if err != nil {
			return err
		}

		if len(children) == 0 {
			l.cleanup()
			return errors.New("unexpected: no children after creating node")
		}

		// Sort children by sequence number
		sort.Strings(children)

		// Get our node name
		ourNode := l.nodePath[strings.LastIndex(l.nodePath, "/")+1:]

		// Find our position
		position := -1
		for i, child := range children {
			if child == ourNode {
				position = i
				break
			}
		}

		if position == -1 {
			// Our node disappeared? Session might have expired
			return ErrSessionExpired
		}

		if position == 0 {
			// We have the lock!
			l.mu.Lock()
			l.held = true
			l.mu.Unlock()
			return nil
		}

		// Watch the node immediately before us
		predecessor := children[position-1]
		predecessorPath := l.path + "/" + predecessor

		exists, _, err := l.client.Exists(predecessorPath, true)
		if err != nil {
			return err
		}

		if !exists {
			// Predecessor is gone, try again
			continue
		}

		// Wait for predecessor to be deleted
		select {
		case <-ctx.Done():
			l.cleanup()
			if ctx.Err() == context.DeadlineExceeded {
				return ErrLockTimeout
			}
			return ErrLockInterrupt
		case <-l.ctx.Done():
			l.cleanup()
			return ErrLockInterrupt
		case event := <-l.client.WatchEvents():
			// Check if it's our predecessor being deleted
			if event.Path == predecessorPath {
				continue
			}
		case <-time.After(time.Second):
			// Periodic re-check
			continue
		}
	}
}

// TryLock attempts to acquire the lock without blocking.
// Returns true if the lock was acquired, false otherwise.
func (l *DistributedLock) TryLock() (bool, error) {
	l.mu.Lock()
	if l.held {
		l.mu.Unlock()
		return true, nil
	}
	l.mu.Unlock()

	if err := l.ensurePath(); err != nil {
		return false, err
	}

	// Create ephemeral sequential node
	nodeName := fmt.Sprintf("%s/lock-", l.path)
	createdPath, err := l.client.Create(nodeName, []byte(l.id), FlagEphemeral|FlagSequential)
	if err != nil {
		return false, fmt.Errorf("failed to create lock node: %w", err)
	}

	l.mu.Lock()
	l.nodePath = createdPath
	l.mu.Unlock()

	// Check if we're first
	children, err := l.client.GetChildren(l.path, false)
	if err != nil {
		l.cleanup()
		return false, err
	}

	sort.Strings(children)
	ourNode := createdPath[strings.LastIndex(createdPath, "/")+1:]

	if len(children) > 0 && children[0] == ourNode {
		l.mu.Lock()
		l.held = true
		l.mu.Unlock()
		return true, nil
	}

	// We're not first, clean up
	l.cleanup()
	return false, nil
}

// Unlock releases the lock.
func (l *DistributedLock) Unlock() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.held {
		return ErrLockNotHeld
	}

	l.held = false
	if l.nodePath != "" {
		err := l.client.Delete(l.nodePath, -1)
		l.nodePath = ""
		return err
	}
	return nil
}

// IsHeld returns whether this lock is currently held by us.
func (l *DistributedLock) IsHeld() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.held
}

// cleanup removes our lock node without marking as unheld
// (used when we failed to acquire).
func (l *DistributedLock) cleanup() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.nodePath != "" {
		_ = l.client.Delete(l.nodePath, -1)
		l.nodePath = ""
	}
}

// Close releases resources and the lock if held.
func (l *DistributedLock) Close() error {
	l.cancel()

	l.mu.Lock()
	defer l.mu.Unlock()

	if l.nodePath != "" {
		err := l.client.Delete(l.nodePath, -1)
		l.nodePath = ""
		l.held = false
		return err
	}
	return nil
}

// ReadWriteLock implements a distributed read-write lock.
type ReadWriteLock struct {
	client ZKClient
	path   string
	id     string

	mu        sync.RWMutex
	readLock  *DistributedLock
	writeLock *DistributedLock
}

// NewReadWriteLock creates a new distributed read-write lock.
func NewReadWriteLock(client ZKClient, path, id string) *ReadWriteLock {
	return &ReadWriteLock{
		client: client,
		path:   path,
		id:     id,
	}
}

// RLock acquires a read lock.
func (rw *ReadWriteLock) RLock(ctx context.Context) error {
	rw.mu.Lock()
	if rw.readLock == nil {
		rw.readLock = NewDistributedLock(rw.client, rw.path+"/read", rw.id)
	}
	lock := rw.readLock
	rw.mu.Unlock()

	return lock.Lock(ctx)
}

// RUnlock releases the read lock.
func (rw *ReadWriteLock) RUnlock() error {
	rw.mu.RLock()
	lock := rw.readLock
	rw.mu.RUnlock()

	if lock == nil {
		return ErrLockNotHeld
	}
	return lock.Unlock()
}

// Lock acquires a write lock.
func (rw *ReadWriteLock) Lock(ctx context.Context) error {
	rw.mu.Lock()
	if rw.writeLock == nil {
		rw.writeLock = NewDistributedLock(rw.client, rw.path+"/write", rw.id)
	}
	lock := rw.writeLock
	rw.mu.Unlock()

	return lock.Lock(ctx)
}

// Unlock releases the write lock.
func (rw *ReadWriteLock) Unlock() error {
	rw.mu.RLock()
	lock := rw.writeLock
	rw.mu.RUnlock()

	if lock == nil {
		return ErrLockNotHeld
	}
	return lock.Unlock()
}
