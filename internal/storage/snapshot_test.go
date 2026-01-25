package storage

import (
	"fmt"
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestSnapshot_TakeAndLoad(t *testing.T) {
	dir := t.TempDir()

	// Create tree with data
	pool := NewMemPool()
	tree := NewTree(pool)
	tree.Create("/test1", []byte("data1"), zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/test2", []byte("data2"), zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/test1/child", []byte("child data"), zk.NodePersistent, zk.WorldACL(), 0)

	// Take snapshot
	config := DefaultSnapshotConfig(dir)
	mgr, err := NewSnapshotManager(config)
	if err != nil {
		t.Fatalf("NewSnapshotManager failed: %v", err)
	}

	if err := mgr.TakeSnapshot(tree); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	// Create new tree and load snapshot
	tree2 := NewTree(pool)
	zxid, err := mgr.LoadSnapshot(tree2)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	if zxid == 0 {
		t.Error("Expected non-zero ZXID from snapshot")
	}

	// Verify data
	data, _, err := tree2.Get("/test1")
	if err != nil {
		t.Fatalf("Get /test1 failed: %v", err)
	}
	if string(data) != "data1" {
		t.Errorf("Data = %s, want data1", data)
	}

	data, _, err = tree2.Get("/test1/child")
	if err != nil {
		t.Fatalf("Get /test1/child failed: %v", err)
	}
	if string(data) != "child data" {
		t.Errorf("Data = %s, want 'child data'", data)
	}
}

func TestSnapshot_NoCompression(t *testing.T) {
	dir := t.TempDir()

	pool := NewMemPool()
	tree := NewTree(pool)
	tree.Create("/test", []byte("data"), zk.NodePersistent, zk.WorldACL(), 0)

	config := DefaultSnapshotConfig(dir)
	config.Compression = false
	mgr, _ := NewSnapshotManager(config)

	if err := mgr.TakeSnapshot(tree); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	tree2 := NewTree(pool)
	_, err := mgr.LoadSnapshot(tree2)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	data, _, _ := tree2.Get("/test")
	if string(data) != "data" {
		t.Errorf("Data = %s, want data", data)
	}
}

func TestSnapshot_EmptyTree(t *testing.T) {
	dir := t.TempDir()

	pool := NewMemPool()
	tree := NewTree(pool)

	config := DefaultSnapshotConfig(dir)
	mgr, _ := NewSnapshotManager(config)

	if err := mgr.TakeSnapshot(tree); err != nil {
		t.Fatalf("TakeSnapshot failed: %v", err)
	}

	tree2 := NewTree(pool)
	_, err := mgr.LoadSnapshot(tree2)
	if err != nil {
		t.Fatalf("LoadSnapshot failed: %v", err)
	}

	// Should only have root
	if tree2.NodeCount() != 1 {
		t.Errorf("NodeCount = %d, want 1", tree2.NodeCount())
	}
}

func TestSnapshot_Cleanup(t *testing.T) {
	dir := t.TempDir()

	pool := NewMemPool()
	tree := NewTree(pool)
	tree.Create("/test", nil, zk.NodePersistent, zk.WorldACL(), 0)

	config := DefaultSnapshotConfig(dir)
	config.RetainCount = 2
	mgr, _ := NewSnapshotManager(config)

	// Take multiple snapshots
	for i := 0; i < 5; i++ {
		tree.Set("/test", []byte{byte(i)}, -1)
		if err := mgr.TakeSnapshot(tree); err != nil {
			t.Fatalf("TakeSnapshot %d failed: %v", i, err)
		}
	}

	// Should only have RetainCount snapshots
	snapshots, _ := mgr.listSnapshots()
	if len(snapshots) != config.RetainCount {
		t.Errorf("Snapshot count = %d, want %d", len(snapshots), config.RetainCount)
	}
}

func BenchmarkSnapshot_Take(b *testing.B) {
	dir := b.TempDir()
	pool := NewMemPool()
	tree := NewTree(pool)

	// Create 1000 nodes
	for i := 0; i < 1000; i++ {
		tree.Create(fmt.Sprintf("/node%d", i), []byte("data"), zk.NodePersistent, zk.WorldACL(), 0)
	}

	config := DefaultSnapshotConfig(dir)
	mgr, _ := NewSnapshotManager(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mgr.TakeSnapshot(tree)
	}
}

func BenchmarkSnapshot_Load(b *testing.B) {
	dir := b.TempDir()
	pool := NewMemPool()
	tree := NewTree(pool)

	// Create 1000 nodes
	for i := 0; i < 1000; i++ {
		tree.Create(fmt.Sprintf("/node%d", i), []byte("data"), zk.NodePersistent, zk.WorldACL(), 0)
	}

	config := DefaultSnapshotConfig(dir)
	mgr, _ := NewSnapshotManager(config)
	mgr.TakeSnapshot(tree)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree2 := NewTree(pool)
		mgr.LoadSnapshot(tree2)
	}
}
