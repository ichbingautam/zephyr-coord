package server

import (
	"sync"
	"sync/atomic"
)

// WatchType indicates the type of watch.
type WatchType uint8

const (
	WatchData     WatchType = 1 // Watch for data changes
	WatchChildren WatchType = 2 // Watch for children changes
)

// EventType indicates the type of watch event.
type EventType int32

const (
	EventNone                EventType = 0
	EventNodeCreated         EventType = 1
	EventNodeDeleted         EventType = 2
	EventNodeDataChanged     EventType = 3
	EventNodeChildrenChanged EventType = 4
)

func (e EventType) String() string {
	switch e {
	case EventNone:
		return "NONE"
	case EventNodeCreated:
		return "NODE_CREATED"
	case EventNodeDeleted:
		return "NODE_DELETED"
	case EventNodeDataChanged:
		return "NODE_DATA_CHANGED"
	case EventNodeChildrenChanged:
		return "NODE_CHILDREN_CHANGED"
	default:
		return "UNKNOWN"
	}
}

// WatchEvent represents a watch notification.
type WatchEvent struct {
	Type      EventType
	Path      string
	SessionID int64
}

// WatchCallback is called when a watch fires.
type WatchCallback func(event WatchEvent)

// watchEntry represents a registered watch.
type watchEntry struct {
	sessionID int64
	callback  WatchCallback
}

// WatchManager manages watch registrations and notifications.
// Uses sync.Map for lock-free concurrent access.
type WatchManager struct {
	// path -> map[sessionID]*watchEntry
	dataWatches  sync.Map
	childWatches sync.Map

	// Metrics
	dataWatchCount  atomic.Int64
	childWatchCount atomic.Int64
	triggeredCount  atomic.Int64
}

// NewWatchManager creates a new watch manager.
func NewWatchManager() *WatchManager {
	return &WatchManager{}
}

// AddDataWatch registers a data watch on a path.
func (wm *WatchManager) AddDataWatch(path string, sessionID int64, callback WatchCallback) {
	entry := &watchEntry{sessionID: sessionID, callback: callback}

	sessionMap, _ := wm.dataWatches.LoadOrStore(path, &sync.Map{})
	sessionMap.(*sync.Map).Store(sessionID, entry)
	wm.dataWatchCount.Add(1)
}

// AddChildWatch registers a children watch on a path.
func (wm *WatchManager) AddChildWatch(path string, sessionID int64, callback WatchCallback) {
	entry := &watchEntry{sessionID: sessionID, callback: callback}

	sessionMap, _ := wm.childWatches.LoadOrStore(path, &sync.Map{})
	sessionMap.(*sync.Map).Store(sessionID, entry)
	wm.childWatchCount.Add(1)
}

// TriggerDataWatch fires watches for data changes.
// Watches are one-shot and removed after triggering.
func (wm *WatchManager) TriggerDataWatch(path string, eventType EventType) {
	val, ok := wm.dataWatches.LoadAndDelete(path)
	if !ok {
		return
	}

	sessionMap := val.(*sync.Map)
	sessionMap.Range(func(key, value interface{}) bool {
		entry := value.(*watchEntry)
		event := WatchEvent{
			Type:      eventType,
			Path:      path,
			SessionID: entry.sessionID,
		}
		entry.callback(event)
		wm.dataWatchCount.Add(-1)
		wm.triggeredCount.Add(1)
		return true
	})
}

// TriggerChildWatch fires watches for children changes.
func (wm *WatchManager) TriggerChildWatch(path string) {
	val, ok := wm.childWatches.LoadAndDelete(path)
	if !ok {
		return
	}

	sessionMap := val.(*sync.Map)
	sessionMap.Range(func(key, value interface{}) bool {
		entry := value.(*watchEntry)
		event := WatchEvent{
			Type:      EventNodeChildrenChanged,
			Path:      path,
			SessionID: entry.sessionID,
		}
		entry.callback(event)
		wm.childWatchCount.Add(-1)
		wm.triggeredCount.Add(1)
		return true
	})
}

// TriggerNodeCreated fires appropriate watches when a node is created.
func (wm *WatchManager) TriggerNodeCreated(path string, parentPath string) {
	// Trigger data watch on the created path (from exists())
	wm.TriggerDataWatch(path, EventNodeCreated)

	// Trigger child watch on the parent
	wm.TriggerChildWatch(parentPath)
}

// TriggerNodeDeleted fires appropriate watches when a node is deleted.
func (wm *WatchManager) TriggerNodeDeleted(path string, parentPath string) {
	// Trigger data watch on the deleted path
	wm.TriggerDataWatch(path, EventNodeDeleted)

	// Trigger child watch on the parent
	wm.TriggerChildWatch(parentPath)
}

// TriggerNodeDataChanged fires watches when node data changes.
func (wm *WatchManager) TriggerNodeDataChanged(path string) {
	wm.TriggerDataWatch(path, EventNodeDataChanged)
}

// RemoveWatchesForSession removes all watches for a session.
func (wm *WatchManager) RemoveWatchesForSession(sessionID int64) {
	wm.dataWatches.Range(func(path, value interface{}) bool {
		sessionMap := value.(*sync.Map)
		if _, ok := sessionMap.LoadAndDelete(sessionID); ok {
			wm.dataWatchCount.Add(-1)
		}
		return true
	})

	wm.childWatches.Range(func(path, value interface{}) bool {
		sessionMap := value.(*sync.Map)
		if _, ok := sessionMap.LoadAndDelete(sessionID); ok {
			wm.childWatchCount.Add(-1)
		}
		return true
	})
}

// Stats returns watch statistics.
type WatchStats struct {
	DataWatchCount  int64
	ChildWatchCount int64
	TriggeredCount  int64
}

func (wm *WatchManager) Stats() WatchStats {
	return WatchStats{
		DataWatchCount:  wm.dataWatchCount.Load(),
		ChildWatchCount: wm.childWatchCount.Load(),
		TriggeredCount:  wm.triggeredCount.Load(),
	}
}
