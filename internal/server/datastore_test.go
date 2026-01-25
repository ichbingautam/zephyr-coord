package server

import (
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestDatastore_CreateGet(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDatastoreConfig(dir)

	ds, err := NewDatastore(config)
	if err != nil {
		t.Fatalf("NewDatastore failed: %v", err)
	}
	defer ds.Close()

	// Create a node
	stat, err := ds.Create("/test", []byte("hello"), zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if stat == nil {
		t.Fatal("Expected non-nil stat")
	}

	// Get the node
	data, stat, err := ds.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Data = %s, want hello", data)
	}
}

func TestDatastore_SetDelete(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDatastoreConfig(dir)

	ds, err := NewDatastore(config)
	if err != nil {
		t.Fatalf("NewDatastore failed: %v", err)
	}
	defer ds.Close()

	ds.Create("/test", []byte("initial"), zk.NodePersistent, zk.WorldACL(), 0)

	// Set
	stat, err := ds.Set("/test", []byte("updated"), -1)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if stat.Version != 1 {
		t.Errorf("Version = %d, want 1", stat.Version)
	}

	// Delete
	err = ds.Delete("/test", -1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, _, err = ds.Get("/test")
	if err != zk.ErrNoNode {
		t.Errorf("Expected ErrNoNode, got %v", err)
	}
}

func TestDatastore_Recovery(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDatastoreConfig(dir)

	// Create datastore and add data
	ds, err := NewDatastore(config)
	if err != nil {
		t.Fatalf("NewDatastore failed: %v", err)
	}

	ds.Create("/test1", []byte("data1"), zk.NodePersistent, zk.WorldACL(), 0)
	ds.Create("/test2", []byte("data2"), zk.NodePersistent, zk.WorldACL(), 0)
	ds.Set("/test1", []byte("modified"), -1)
	ds.Close()

	// Create new datastore and recover
	ds2, err := NewDatastore(config)
	if err != nil {
		t.Fatalf("NewDatastore for recovery failed: %v", err)
	}
	defer ds2.Close()

	if err := ds2.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Verify data
	data, _, err := ds2.Get("/test1")
	if err != nil {
		t.Fatalf("Get /test1 failed: %v", err)
	}
	if string(data) != "modified" {
		t.Errorf("Data = %s, want modified", data)
	}

	data, _, err = ds2.Get("/test2")
	if err != nil {
		t.Fatalf("Get /test2 failed: %v", err)
	}
	if string(data) != "data2" {
		t.Errorf("Data = %s, want data2", data)
	}
}

func TestDatastore_EphemeralCleanup(t *testing.T) {
	dir := t.TempDir()
	config := DefaultDatastoreConfig(dir)

	ds, err := NewDatastore(config)
	if err != nil {
		t.Fatalf("NewDatastore failed: %v", err)
	}
	defer ds.Close()

	sessionID := int64(12345)
	ds.Create("/ephemeral", []byte("data"), zk.NodeEphemeral, zk.WorldACL(), sessionID)

	// Verify it exists
	stat, _ := ds.Exists("/ephemeral")
	if stat == nil {
		t.Fatal("Ephemeral node should exist")
	}

	// Delete ephemerals for session
	ds.DeleteEphemeralsBySession(sessionID)

	// Verify it's gone
	stat, _ = ds.Exists("/ephemeral")
	if stat != nil {
		t.Error("Ephemeral node should have been deleted")
	}
}

func BenchmarkDatastore_Create(b *testing.B) {
	dir := b.TempDir()
	config := DefaultDatastoreConfig(dir)

	ds, _ := NewDatastore(config)
	defer ds.Close()

	data := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := "/node" + string(rune(i))
		ds.Create(path, data, zk.NodePersistent, zk.WorldACL(), 0)
	}
}

func BenchmarkDatastore_Get(b *testing.B) {
	dir := b.TempDir()
	config := DefaultDatastoreConfig(dir)

	ds, _ := NewDatastore(config)
	defer ds.Close()

	ds.Create("/test", []byte("benchmark"), zk.NodePersistent, zk.WorldACL(), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ds.Get("/test")
	}
}
