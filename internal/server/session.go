package server

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// SessionState represents the state of a client session.
type SessionState int32

const (
	SessionConnecting SessionState = iota
	SessionConnected
	SessionClosing
	SessionClosed
	SessionExpired
)

func (s SessionState) String() string {
	switch s {
	case SessionConnecting:
		return "CONNECTING"
	case SessionConnected:
		return "CONNECTED"
	case SessionClosing:
		return "CLOSING"
	case SessionClosed:
		return "CLOSED"
	case SessionExpired:
		return "EXPIRED"
	default:
		return "UNKNOWN"
	}
}

// Session represents a client session.
type Session struct {
	ID         int64
	Timeout    time.Duration
	lastActive atomic.Int64
	state      atomic.Int32
	ephemerals sync.Map // path -> struct{}
	watches    sync.Map // path -> WatchType

	// Internal fields
	wheelElem *list.Element // Position in timeout wheel
}

// NewSession creates a new session.
func NewSession(id int64, timeout time.Duration) *Session {
	s := &Session{
		ID:      id,
		Timeout: timeout,
	}
	s.state.Store(int32(SessionConnecting))
	s.Touch()
	return s
}

// Touch updates the last activity time.
func (s *Session) Touch() {
	s.lastActive.Store(time.Now().UnixNano())
}

// LastActive returns the last activity time.
func (s *Session) LastActive() time.Time {
	return time.Unix(0, s.lastActive.Load())
}

// IsExpired checks if the session has expired.
func (s *Session) IsExpired() bool {
	return time.Since(s.LastActive()) > s.Timeout
}

// State returns the current session state.
func (s *Session) State() SessionState {
	return SessionState(s.state.Load())
}

// SetState sets the session state.
func (s *Session) SetState(state SessionState) {
	s.state.Store(int32(state))
}

// AddEphemeral tracks an ephemeral node.
func (s *Session) AddEphemeral(path string) {
	s.ephemerals.Store(path, struct{}{})
}

// RemoveEphemeral removes tracking for an ephemeral node.
func (s *Session) RemoveEphemeral(path string) {
	s.ephemerals.Delete(path)
}

// Ephemerals returns all ephemeral paths for this session.
func (s *Session) Ephemerals() []string {
	var paths []string
	s.ephemerals.Range(func(key, _ interface{}) bool {
		paths = append(paths, key.(string))
		return true
	})
	return paths
}

// SessionManagerConfig contains session manager configuration.
type SessionManagerConfig struct {
	MinSessionTimeout time.Duration
	MaxSessionTimeout time.Duration
	TickInterval      time.Duration // Wheel resolution
	WheelBuckets      int
}

// DefaultSessionManagerConfig returns default configuration.
func DefaultSessionManagerConfig() SessionManagerConfig {
	return SessionManagerConfig{
		MinSessionTimeout: 2 * time.Second,
		MaxSessionTimeout: 40 * time.Second,
		TickInterval:      100 * time.Millisecond,
		WheelBuckets:      256,
	}
}

// SessionManager manages client sessions with efficient timeout detection.
type SessionManager struct {
	config   SessionManagerConfig
	sessions sync.Map // id -> *Session
	nextID   atomic.Int64

	// Timeout wheel for O(1) expiry checking
	wheel       []*list.List // Buckets
	wheelMu     sync.Mutex
	wheelPos    int
	wheelTicker *time.Ticker

	// Callbacks
	onExpire func(session *Session)

	closed atomic.Bool
}

// NewSessionManager creates a new session manager.
func NewSessionManager(config SessionManagerConfig, onExpire func(*Session)) *SessionManager {
	sm := &SessionManager{
		config:   config,
		wheel:    make([]*list.List, config.WheelBuckets),
		onExpire: onExpire,
	}

	for i := range sm.wheel {
		sm.wheel[i] = list.New()
	}

	sm.wheelTicker = time.NewTicker(config.TickInterval)
	go sm.tickLoop()

	return sm
}

// CreateSession creates a new session.
func (sm *SessionManager) CreateSession(requestedTimeout time.Duration) *Session {
	// Clamp timeout to allowed range
	timeout := requestedTimeout
	if timeout < sm.config.MinSessionTimeout {
		timeout = sm.config.MinSessionTimeout
	}
	if timeout > sm.config.MaxSessionTimeout {
		timeout = sm.config.MaxSessionTimeout
	}

	id := sm.nextID.Add(1)
	session := NewSession(id, timeout)
	session.SetState(SessionConnected)

	sm.sessions.Store(id, session)
	sm.addToWheel(session)

	return session
}

// GetSession returns a session by ID.
func (sm *SessionManager) GetSession(id int64) (*Session, bool) {
	val, ok := sm.sessions.Load(id)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}

// TouchSession updates session activity.
func (sm *SessionManager) TouchSession(id int64) bool {
	session, ok := sm.GetSession(id)
	if !ok {
		return false
	}
	session.Touch()
	sm.updateWheelPosition(session)
	return true
}

// CloseSession closes a session.
func (sm *SessionManager) CloseSession(id int64) {
	session, ok := sm.GetSession(id)
	if !ok {
		return
	}

	session.SetState(SessionClosed)
	sm.removeFromWheel(session)
	sm.sessions.Delete(id)
}

// addToWheel adds a session to the timeout wheel.
func (sm *SessionManager) addToWheel(session *Session) {
	sm.wheelMu.Lock()
	defer sm.wheelMu.Unlock()

	bucket := sm.bucketForSession(session)
	session.wheelElem = sm.wheel[bucket].PushBack(session)
}

// updateWheelPosition moves a session to its new bucket.
func (sm *SessionManager) updateWheelPosition(session *Session) {
	sm.wheelMu.Lock()
	defer sm.wheelMu.Unlock()

	if session.wheelElem != nil {
		// Find and remove from old bucket
		for _, bucket := range sm.wheel {
			for e := bucket.Front(); e != nil; e = e.Next() {
				if e == session.wheelElem {
					bucket.Remove(e)
					break
				}
			}
		}
	}

	bucket := sm.bucketForSession(session)
	session.wheelElem = sm.wheel[bucket].PushBack(session)
}

// removeFromWheel removes a session from the wheel.
func (sm *SessionManager) removeFromWheel(session *Session) {
	sm.wheelMu.Lock()
	defer sm.wheelMu.Unlock()

	if session.wheelElem != nil {
		for _, bucket := range sm.wheel {
			for e := bucket.Front(); e != nil; e = e.Next() {
				if e == session.wheelElem {
					bucket.Remove(e)
					return
				}
			}
		}
	}
}

// bucketForSession calculates the bucket for a session.
func (sm *SessionManager) bucketForSession(session *Session) int {
	// Calculate ticks until expiry
	ticksUntilExpiry := int(session.Timeout / sm.config.TickInterval)
	bucket := (sm.wheelPos + ticksUntilExpiry) % sm.config.WheelBuckets
	return bucket
}

// tickLoop processes the timeout wheel.
func (sm *SessionManager) tickLoop() {
	for range sm.wheelTicker.C {
		if sm.closed.Load() {
			return
		}
		sm.tick()
	}
}

// tick advances the wheel and checks for expired sessions.
func (sm *SessionManager) tick() {
	sm.wheelMu.Lock()
	sm.wheelPos = (sm.wheelPos + 1) % sm.config.WheelBuckets
	bucket := sm.wheel[sm.wheelPos]

	var expired []*Session
	for e := bucket.Front(); e != nil; {
		session := e.Value.(*Session)
		next := e.Next()

		if session.IsExpired() {
			bucket.Remove(e)
			expired = append(expired, session)
		}
		e = next
	}
	sm.wheelMu.Unlock()

	// Process expired sessions outside lock
	for _, session := range expired {
		session.SetState(SessionExpired)
		sm.sessions.Delete(session.ID)
		if sm.onExpire != nil {
			sm.onExpire(session)
		}
	}
}

// ActiveCount returns the number of active sessions.
func (sm *SessionManager) ActiveCount() int {
	count := 0
	sm.sessions.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

// Close shuts down the session manager.
func (sm *SessionManager) Close() {
	sm.closed.Store(true)
	sm.wheelTicker.Stop()
}
