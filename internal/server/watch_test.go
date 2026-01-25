package server

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestWatchManager_DataWatch(t *testing.T) {
	wm := NewWatchManager()

	triggered := make(chan WatchEvent, 1)
	wm.AddDataWatch("/test", 1, func(e WatchEvent) {
		triggered <- e
	})

	stats := wm.Stats()
	if stats.DataWatchCount != 1 {
		t.Errorf("DataWatchCount = %d, want 1", stats.DataWatchCount)
	}

	wm.TriggerDataWatch("/test", EventNodeDataChanged)

	select {
	case event := <-triggered:
		if event.Type != EventNodeDataChanged {
			t.Errorf("Event type = %v, want NODE_DATA_CHANGED", event.Type)
		}
		if event.Path != "/test" {
			t.Errorf("Event path = %s, want /test", event.Path)
		}
	default:
		t.Error("Watch should have been triggered")
	}

	// Watch should be removed after trigger (one-shot)
	stats = wm.Stats()
	if stats.DataWatchCount != 0 {
		t.Errorf("DataWatchCount = %d, want 0 (one-shot)", stats.DataWatchCount)
	}
}

func TestWatchManager_ChildWatch(t *testing.T) {
	wm := NewWatchManager()

	triggered := make(chan WatchEvent, 1)
	wm.AddChildWatch("/parent", 1, func(e WatchEvent) {
		triggered <- e
	})

	wm.TriggerChildWatch("/parent")

	select {
	case event := <-triggered:
		if event.Type != EventNodeChildrenChanged {
			t.Errorf("Event type = %v, want NODE_CHILDREN_CHANGED", event.Type)
		}
	default:
		t.Error("Watch should have been triggered")
	}
}

func TestWatchManager_NodeCreated(t *testing.T) {
	wm := NewWatchManager()

	var events []WatchEvent
	var mu sync.Mutex

	// Watch for node creation (via exists)
	wm.AddDataWatch("/new", 1, func(e WatchEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	// Watch for children change on parent
	wm.AddChildWatch("/", 2, func(e WatchEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	wm.TriggerNodeCreated("/new", "/")

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(events))
	}
}

func TestWatchManager_NodeDeleted(t *testing.T) {
	wm := NewWatchManager()

	var events []WatchEvent
	var mu sync.Mutex

	wm.AddDataWatch("/node", 1, func(e WatchEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	wm.AddChildWatch("/", 2, func(e WatchEvent) {
		mu.Lock()
		events = append(events, e)
		mu.Unlock()
	})

	wm.TriggerNodeDeleted("/node", "/")

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Errorf("Expected 2 events, got %d", len(events))
	}

	// Check event types
	hasDeleted := false
	hasChildren := false
	for _, e := range events {
		if e.Type == EventNodeDeleted {
			hasDeleted = true
		}
		if e.Type == EventNodeChildrenChanged {
			hasChildren = true
		}
	}
	if !hasDeleted {
		t.Error("Expected NODE_DELETED event")
	}
	if !hasChildren {
		t.Error("Expected NODE_CHILDREN_CHANGED event")
	}
}

func TestWatchManager_RemoveWatchesForSession(t *testing.T) {
	wm := NewWatchManager()

	wm.AddDataWatch("/test1", 1, func(e WatchEvent) {})
	wm.AddDataWatch("/test2", 1, func(e WatchEvent) {})
	wm.AddChildWatch("/test1", 1, func(e WatchEvent) {})
	wm.AddDataWatch("/test3", 2, func(e WatchEvent) {}) // Different session

	stats := wm.Stats()
	if stats.DataWatchCount != 3 {
		t.Errorf("DataWatchCount = %d, want 3", stats.DataWatchCount)
	}

	wm.RemoveWatchesForSession(1)

	stats = wm.Stats()
	if stats.DataWatchCount != 1 {
		t.Errorf("DataWatchCount = %d, want 1 (session 2 only)", stats.DataWatchCount)
	}
	if stats.ChildWatchCount != 0 {
		t.Errorf("ChildWatchCount = %d, want 0", stats.ChildWatchCount)
	}
}

func TestWatchManager_MultipleSessionsSamePath(t *testing.T) {
	wm := NewWatchManager()

	var count atomic.Int32

	wm.AddDataWatch("/test", 1, func(e WatchEvent) { count.Add(1) })
	wm.AddDataWatch("/test", 2, func(e WatchEvent) { count.Add(1) })
	wm.AddDataWatch("/test", 3, func(e WatchEvent) { count.Add(1) })

	wm.TriggerDataWatch("/test", EventNodeDataChanged)

	if count.Load() != 3 {
		t.Errorf("Triggered %d times, want 3", count.Load())
	}
}

func BenchmarkWatchManager_AddWatch(b *testing.B) {
	wm := NewWatchManager()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wm.AddDataWatch("/test", int64(i), func(e WatchEvent) {})
	}
}

func BenchmarkWatchManager_TriggerWatch(b *testing.B) {
	wm := NewWatchManager()

	// Pre-populate watches
	for i := 0; i < 100; i++ {
		wm.AddDataWatch("/test", int64(i), func(e WatchEvent) {})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-add watches for each iteration
		for j := 0; j < 100; j++ {
			wm.AddDataWatch("/test", int64(j), func(e WatchEvent) {})
		}
		wm.TriggerDataWatch("/test", EventNodeDataChanged)
	}
}
