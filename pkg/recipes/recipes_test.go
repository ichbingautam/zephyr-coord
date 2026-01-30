package recipes

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

// MockZKClient implements ZKClient for testing.
type MockZKClient struct {
	mu        sync.RWMutex
	nodes     map[string][]byte
	children  map[string][]string
	sequence  int
	sessionID int64
	events    chan WatchEvent
}

// NewMockZKClient creates a mock client.
func NewMockZKClient() *MockZKClient {
	return &MockZKClient{
		nodes:     make(map[string][]byte),
		children:  make(map[string][]string),
		sessionID: 12345,
		events:    make(chan WatchEvent, 10),
	}
}

func (m *MockZKClient) Create(path string, data []byte, flags int32) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	actualPath := path
	if flags&FlagSequential != 0 {
		m.sequence++
		actualPath = path + fmt.Sprintf("%010d", m.sequence)
	}

	m.nodes[actualPath] = data

	// Add to parent's children
	parent := parentPath(actualPath)
	if parent != "" {
		m.children[parent] = append(m.children[parent], basename(actualPath))
	}

	return actualPath, nil
}

func (m *MockZKClient) Delete(path string, version int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodes, path)

	// Remove from parent's children
	parent := parentPath(path)
	if parent != "" {
		name := basename(path)
		children := m.children[parent]
		for i, c := range children {
			if c == name {
				m.children[parent] = append(children[:i], children[i+1:]...)
				break
			}
		}
	}

	return nil
}

func (m *MockZKClient) Exists(path string, watch bool) (bool, int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.nodes[path]
	return exists, 1, nil
}

func (m *MockZKClient) GetData(path string, watch bool) ([]byte, int32, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, exists := m.nodes[path]
	if !exists {
		return nil, 0, fmt.Errorf("no node")
	}
	return data, 1, nil
}

func (m *MockZKClient) GetChildren(path string, watch bool) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	children := m.children[path]
	result := make([]string, len(children))
	copy(result, children)
	sort.Strings(result)
	return result, nil
}

func (m *MockZKClient) SessionID() int64 {
	return m.sessionID
}

func (m *MockZKClient) WatchEvents() <-chan WatchEvent {
	return m.events
}

func (m *MockZKClient) TriggerEvent(path string) {
	select {
	case m.events <- WatchEvent{Path: path}:
	default:
	}
}

// Helper functions
func parentPath(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			if i == 0 {
				return "/"
			}
			return path[:i]
		}
	}
	return ""
}

func basename(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

func TestLeaderElection_SingleParticipant(t *testing.T) {
	client := NewMockZKClient()

	// Ensure path exists first
	client.Create("/election", nil, 0)

	le := NewLeaderElection(client, "/election", "node1", []byte("data1"))

	err := le.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Trigger check
	time.Sleep(50 * time.Millisecond)
	client.TriggerEvent("/election")
	time.Sleep(50 * time.Millisecond)

	if !le.IsLeader() {
		t.Error("single participant should be leader")
	}

	le.Stop()
}

func TestDistributedLock_TryLock(t *testing.T) {
	client := NewMockZKClient()

	// Ensure path exists
	client.Create("/locks", nil, 0)

	lock1 := NewDistributedLock(client, "/locks/test", "client1")
	lock2 := NewDistributedLock(client, "/locks/test", "client2")

	// First lock should succeed
	acquired, err := lock1.TryLock()
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if !acquired {
		t.Error("first TryLock should succeed")
	}

	// Second lock should fail
	acquired, err = lock2.TryLock()
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if acquired {
		t.Error("second TryLock should fail while first holds lock")
	}

	// Unlock first
	err = lock1.Unlock()
	if err != nil {
		t.Fatalf("Unlock failed: %v", err)
	}

	// Now second should succeed
	acquired, err = lock2.TryLock()
	if err != nil {
		t.Fatalf("TryLock failed: %v", err)
	}
	if !acquired {
		t.Error("TryLock should succeed after unlock")
	}

	lock2.Close()
}

func TestQueue_FIFO(t *testing.T) {
	client := NewMockZKClient()

	// Ensure path exists
	client.Create("/queue", nil, 0)

	q := NewQueue(client, "/queue")

	// Add items
	q.Offer([]byte("first"))
	q.Offer([]byte("second"))
	q.Offer([]byte("third"))

	// Poll should return in FIFO order
	data, err := q.Poll()
	if err != nil {
		t.Fatalf("Poll failed: %v", err)
	}
	if string(data) != "first" {
		t.Errorf("expected 'first', got '%s'", data)
	}

	data, _ = q.Poll()
	if string(data) != "second" {
		t.Errorf("expected 'second', got '%s'", data)
	}

	data, _ = q.Poll()
	if string(data) != "third" {
		t.Errorf("expected 'third', got '%s'", data)
	}

	// Queue should be empty
	data, _ = q.Poll()
	if data != nil {
		t.Error("queue should be empty")
	}
}

func TestBarrier_Enter(t *testing.T) {
	client := NewMockZKClient()

	// Ensure path exists
	client.Create("/barrier", nil, 0)

	// Create barrier waiting for 2 parties
	b1 := NewBarrier(client, "/barrier", 2, "party1")
	b2 := NewBarrier(client, "/barrier", 2, "party2")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	errors := make(chan error, 2)

	go func() {
		defer wg.Done()
		if err := b1.Enter(ctx); err != nil {
			errors <- err
		}
	}()

	// Small delay before second party enters
	time.Sleep(50 * time.Millisecond)

	go func() {
		defer wg.Done()
		if err := b2.Enter(ctx); err != nil {
			errors <- err
		}
		// Trigger watch to unblock first party
		client.TriggerEvent("/barrier")
	}()

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("barrier error: %v", err)
	}
}

func BenchmarkQueue_Offer(b *testing.B) {
	client := NewMockZKClient()
	client.Create("/queue", nil, 0)
	q := NewQueue(client, "/queue")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q.Offer([]byte("data"))
	}
}

func BenchmarkLock_TryLock(b *testing.B) {
	client := NewMockZKClient()
	client.Create("/locks", nil, 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock := NewDistributedLock(client, "/locks/bench", "client")
		lock.TryLock()
		lock.Unlock()
	}
}
