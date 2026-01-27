package server

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/ichbingautam/zephyr-coord/internal/protocol"
)

func TestTransport_StartStop(t *testing.T) {
	// Create temp dir for datastore
	tmpDir := t.TempDir()

	config := DefaultServerConfig(tmpDir)
	config.Transport.ListenAddr = "127.0.0.1:0"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// Wait for ready
	if err := server.WaitForReady(5 * time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	addr := server.Addr()
	if addr == "" {
		t.Fatal("expected non-empty address")
	}

	// Verify listening
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	conn.Close()

	// Stop server
	if err := server.Stop(); err != nil {
		t.Fatalf("failed to stop server: %v", err)
	}
}

func TestTransport_ConnectRequest(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultServerConfig(tmpDir)
	config.Transport.ListenAddr = "127.0.0.1:0"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	if err := server.WaitForReady(5 * time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	// Connect
	conn, err := net.DialTimeout("tcp", server.Addr(), time.Second)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// Send connect request (no header, just body with frame length)
	connectReq := &protocol.ConnectRequest{
		ProtocolVersion: 0,
		LastZxidSeen:    0,
		Timeout:         30000,
		SessionID:       0,
		Password:        make([]byte, 16),
		ReadOnly:        false,
	}

	encoder := protocol.NewEncoder(256)
	connectReq.Encode(encoder)
	frame := encoder.Bytes()

	// Write frame with length prefix
	buf := &bytes.Buffer{}
	fw := protocol.NewFrameWriter(buf)
	fw.WriteFrame(frame) // Frame writer already expects plain data

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write(buf.Bytes())
	if err != nil {
		t.Fatalf("failed to send connect request: %v", err)
	}

	// Read connect response
	fr := protocol.NewFrameReader(conn, 1024*1024)
	respFrame, err := fr.ReadFrame()
	if err != nil {
		t.Fatalf("failed to read connect response: %v", err)
	}

	// Decode response
	decoder := protocol.NewDecoder(respFrame)
	connectResp := &protocol.ConnectResponse{}
	if err := connectResp.Decode(decoder); err != nil {
		t.Fatalf("failed to decode connect response: %v", err)
	}

	if connectResp.SessionID == 0 {
		t.Error("expected non-zero session ID")
	}
	if connectResp.Timeout <= 0 {
		t.Errorf("expected positive timeout, got %d", connectResp.Timeout)
	}
}

func TestTransport_Stats(t *testing.T) {
	tmpDir := t.TempDir()

	config := DefaultServerConfig(tmpDir)
	config.Transport.ListenAddr = "127.0.0.1:0"

	server, err := NewServer(config)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	if err := server.WaitForReady(5 * time.Second); err != nil {
		t.Fatalf("server not ready: %v", err)
	}

	stats := server.Stats()
	if stats.Datastore.NodeCount != 1 { // Root node
		t.Errorf("expected 1 node (root), got %d", stats.Datastore.NodeCount)
	}
}

func BenchmarkConnect(b *testing.B) {
	tmpDir := b.TempDir()

	config := DefaultServerConfig(tmpDir)
	config.Transport.ListenAddr = "127.0.0.1:0"

	server, err := NewServer(config)
	if err != nil {
		b.Fatalf("failed to create server: %v", err)
	}

	if err := server.Start(); err != nil {
		b.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	server.WaitForReady(5 * time.Second)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			conn, err := net.Dial("tcp", server.Addr())
			if err != nil {
				b.Errorf("failed to connect: %v", err)
				continue
			}
			conn.Close()
		}
	})
}
