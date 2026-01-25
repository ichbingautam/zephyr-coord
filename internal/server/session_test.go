package server

import (
	"testing"
	"time"
)

func TestSession_Basic(t *testing.T) {
	session := NewSession(1, 5*time.Second)

	if session.ID != 1 {
		t.Errorf("ID = %d, want 1", session.ID)
	}
	if session.State() != SessionConnecting {
		t.Errorf("State = %v, want CONNECTING", session.State())
	}

	session.SetState(SessionConnected)
	if session.State() != SessionConnected {
		t.Errorf("State = %v, want CONNECTED", session.State())
	}
}

func TestSession_Ephemerals(t *testing.T) {
	session := NewSession(1, 5*time.Second)

	session.AddEphemeral("/test1")
	session.AddEphemeral("/test2")

	paths := session.Ephemerals()
	if len(paths) != 2 {
		t.Errorf("len(Ephemerals()) = %d, want 2", len(paths))
	}

	session.RemoveEphemeral("/test1")
	paths = session.Ephemerals()
	if len(paths) != 1 {
		t.Errorf("len(Ephemerals()) = %d, want 1", len(paths))
	}
}

func TestSession_Expiry(t *testing.T) {
	session := NewSession(1, 50*time.Millisecond)

	if session.IsExpired() {
		t.Error("Session should not be expired immediately")
	}

	time.Sleep(60 * time.Millisecond)
	if !session.IsExpired() {
		t.Error("Session should be expired")
	}
}

func TestSessionManager_CreateSession(t *testing.T) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	session := sm.CreateSession(5 * time.Second)
	if session == nil {
		t.Fatal("CreateSession returned nil")
	}
	if session.State() != SessionConnected {
		t.Errorf("State = %v, want CONNECTED", session.State())
	}

	// Timeout clamping
	session2 := sm.CreateSession(1 * time.Second) // Below min
	if session2.Timeout != config.MinSessionTimeout {
		t.Errorf("Timeout = %v, want %v", session2.Timeout, config.MinSessionTimeout)
	}

	session3 := sm.CreateSession(60 * time.Second) // Above max
	if session3.Timeout != config.MaxSessionTimeout {
		t.Errorf("Timeout = %v, want %v", session3.Timeout, config.MaxSessionTimeout)
	}
}

func TestSessionManager_GetSession(t *testing.T) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	session := sm.CreateSession(5 * time.Second)

	got, ok := sm.GetSession(session.ID)
	if !ok {
		t.Fatal("GetSession should return existing session")
	}
	if got.ID != session.ID {
		t.Errorf("ID = %d, want %d", got.ID, session.ID)
	}

	_, ok = sm.GetSession(99999)
	if ok {
		t.Error("GetSession should return false for non-existent session")
	}
}

func TestSessionManager_TouchSession(t *testing.T) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	session := sm.CreateSession(5 * time.Second)
	initial := session.LastActive()

	time.Sleep(10 * time.Millisecond)
	sm.TouchSession(session.ID)

	if !session.LastActive().After(initial) {
		t.Error("LastActive should be updated after Touch")
	}
}

func TestSessionManager_CloseSession(t *testing.T) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	session := sm.CreateSession(5 * time.Second)
	id := session.ID

	sm.CloseSession(id)

	_, ok := sm.GetSession(id)
	if ok {
		t.Error("Session should be removed after Close")
	}
}

func TestSessionManager_SessionExpiry(t *testing.T) {
	config := DefaultSessionManagerConfig()
	config.TickInterval = 10 * time.Millisecond
	config.MinSessionTimeout = 30 * time.Millisecond

	expiredSessions := make(chan *Session, 10)
	sm := NewSessionManager(config, func(s *Session) {
		expiredSessions <- s
	})
	defer sm.Close()

	session := sm.CreateSession(30 * time.Millisecond)
	id := session.ID

	// Wait for expiry
	select {
	case expired := <-expiredSessions:
		if expired.ID != id {
			t.Errorf("Expired session ID = %d, want %d", expired.ID, id)
		}
		if expired.State() != SessionExpired {
			t.Errorf("State = %v, want EXPIRED", expired.State())
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Session should have expired")
	}
}

func TestSessionManager_ActiveCount(t *testing.T) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	if sm.ActiveCount() != 0 {
		t.Errorf("ActiveCount() = %d, want 0", sm.ActiveCount())
	}

	sm.CreateSession(5 * time.Second)
	sm.CreateSession(5 * time.Second)

	if sm.ActiveCount() != 2 {
		t.Errorf("ActiveCount() = %d, want 2", sm.ActiveCount())
	}
}

func BenchmarkSessionManager_CreateSession(b *testing.B) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.CreateSession(5 * time.Second)
	}
}

func BenchmarkSessionManager_TouchSession(b *testing.B) {
	config := DefaultSessionManagerConfig()
	sm := NewSessionManager(config, nil)
	defer sm.Close()

	session := sm.CreateSession(5 * time.Second)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sm.TouchSession(session.ID)
	}
}
