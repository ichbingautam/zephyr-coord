package server

import (
	"fmt"
	"time"
)

// ServerConfig contains all server configuration.
type ServerConfig struct {
	DataDir   string
	Transport TransportConfig
	Session   SessionManagerConfig
}

// DefaultServerConfig returns default server configuration.
func DefaultServerConfig(dataDir string) ServerConfig {
	return ServerConfig{
		DataDir:   dataDir,
		Transport: DefaultTransportConfig(),
		Session:   DefaultSessionManagerConfig(),
	}
}

// Server is the main ZephyrCoord server.
type Server struct {
	config         ServerConfig
	datastore      *Datastore
	sessionManager *SessionManager
	watchManager   *WatchManager
	processor      *RequestProcessor
	transport      *Transport
}

// NewServer creates a new server.
func NewServer(config ServerConfig) (*Server, error) {
	// Create datastore
	dsConfig := DefaultDatastoreConfig(config.DataDir)
	datastore, err := NewDatastore(dsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create datastore: %w", err)
	}

	// Create watch manager
	watchManager := NewWatchManager()

	// Create session manager with callback for expiry
	sessionManager := NewSessionManager(config.Session, func(session *Session) {
		// Clean up on session expiry
		watchManager.RemoveWatchesForSession(session.ID)
		datastore.DeleteEphemeralsBySession(session.ID)
	})

	// Create request processor
	processor := NewRequestProcessor(datastore, sessionManager, watchManager)

	// Create transport
	transport := NewTransport(config.Transport, processor)

	return &Server{
		config:         config,
		datastore:      datastore,
		sessionManager: sessionManager,
		watchManager:   watchManager,
		processor:      processor,
		transport:      transport,
	}, nil
}

// Start starts the server.
func (s *Server) Start() error {
	// Recover from snapshot + WAL
	if err := s.datastore.Recover(); err != nil {
		return fmt.Errorf("recovery failed: %w", err)
	}

	// Start transport
	if err := s.transport.Start(); err != nil {
		return fmt.Errorf("failed to start transport: %w", err)
	}

	return nil
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	// Stop accepting new connections
	if err := s.transport.Stop(); err != nil {
		return err
	}

	// Close session manager
	s.sessionManager.Close()

	// Close datastore (takes final snapshot)
	return s.datastore.Close()
}

// Addr returns the server listen address.
func (s *Server) Addr() string {
	addr := s.transport.Addr()
	if addr == nil {
		return ""
	}
	return addr.String()
}

// Stats returns server statistics.
type ServerStats struct {
	Datastore DatastoreStats
	Transport TransportStats
	Sessions  int
	Watches   WatchStats
}

func (s *Server) Stats() ServerStats {
	return ServerStats{
		Datastore: s.datastore.Stats(),
		Transport: s.transport.Stats(),
		Sessions:  s.sessionManager.ActiveCount(),
		Watches:   s.watchManager.Stats(),
	}
}

// WaitForReady blocks until the server is ready.
func (s *Server) WaitForReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.transport.Addr() != nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server not ready after %v", timeout)
}
