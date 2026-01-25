package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestTree_Create(t *testing.T) {
	tree := NewTree(NewMemPool())

	stat, err := tree.Create("/test", []byte("data"), zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if stat == nil {
		t.Fatal("Expected non-nil stat")
	}
	if stat.DataLength != 4 {
		t.Errorf("DataLength = %d, want 4", stat.DataLength)
	}
}

func TestTree_CreateNested(t *testing.T) {
	tree := NewTree(NewMemPool())

	_, err := tree.Create("/a", nil, zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create /a failed: %v", err)
	}

	_, err = tree.Create("/a/b", nil, zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create /a/b failed: %v", err)
	}

	_, err = tree.Create("/a/b/c", []byte("nested"), zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create /a/b/c failed: %v", err)
	}

	data, _, err := tree.Get("/a/b/c")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "nested" {
		t.Errorf("Data = %s, want nested", data)
	}
}

func TestTree_CreateExisting(t *testing.T) {
	tree := NewTree(NewMemPool())

	_, err := tree.Create("/test", nil, zk.NodePersistent, zk.WorldACL(), 0)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	_, err = tree.Create("/test", nil, zk.NodePersistent, zk.WorldACL(), 0)
	if err != zk.ErrNodeExists {
		t.Errorf("Expected ErrNodeExists, got %v", err)
	}
}

func TestTree_CreateNoParent(t *testing.T) {
	tree := NewTree(NewMemPool())

	_, err := tree.Create("/a/b/c", nil, zk.NodePersistent, zk.WorldACL(), 0)
	if err != zk.ErrNoNode {
		t.Errorf("Expected ErrNoNode, got %v", err)
	}
}

func TestTree_Get(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", []byte("hello"), zk.NodePersistent, zk.WorldACL(), 0)

	data, stat, err := tree.Get("/test")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("Data = %s, want hello", data)
	}
	if stat.DataLength != 5 {
		t.Errorf("DataLength = %d, want 5", stat.DataLength)
	}
}

func TestTree_Set(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", []byte("initial"), zk.NodePersistent, zk.WorldACL(), 0)

	stat, err := tree.Set("/test", []byte("updated"), -1)
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if stat.Version != 1 {
		t.Errorf("Version = %d, want 1", stat.Version)
	}

	data, _, _ := tree.Get("/test")
	if string(data) != "updated" {
		t.Errorf("Data = %s, want updated", data)
	}
}

func TestTree_SetBadVersion(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", []byte("initial"), zk.NodePersistent, zk.WorldACL(), 0)

	_, err := tree.Set("/test", []byte("updated"), 999)
	if err != zk.ErrBadVersion {
		t.Errorf("Expected ErrBadVersion, got %v", err)
	}
}

func TestTree_Delete(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", nil, zk.NodePersistent, zk.WorldACL(), 0)

	err := tree.Delete("/test", -1)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, _, err = tree.Get("/test")
	if err != zk.ErrNoNode {
		t.Errorf("Expected ErrNoNode after delete, got %v", err)
	}
}

func TestTree_DeleteNotEmpty(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/parent", nil, zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/parent/child", nil, zk.NodePersistent, zk.WorldACL(), 0)

	err := tree.Delete("/parent", -1)
	if err != zk.ErrNotEmpty {
		t.Errorf("Expected ErrNotEmpty, got %v", err)
	}
}

func TestTree_Exists(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", nil, zk.NodePersistent, zk.WorldACL(), 0)

	stat, err := tree.Exists("/test")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if stat == nil {
		t.Error("Expected non-nil stat for existing node")
	}

	stat, err = tree.Exists("/nonexistent")
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if stat != nil {
		t.Error("Expected nil stat for non-existent node")
	}
}

func TestTree_GetChildren(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/parent", nil, zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/parent/child1", nil, zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/parent/child2", nil, zk.NodePersistent, zk.WorldACL(), 0)

	children, stat, err := tree.GetChildren("/parent")
	if err != nil {
		t.Fatalf("GetChildren failed: %v", err)
	}
	if len(children) != 2 {
		t.Errorf("len(children) = %d, want 2", len(children))
	}
	if stat.NumChildren != 2 {
		t.Errorf("NumChildren = %d, want 2", stat.NumChildren)
	}
}

func TestTree_EphemeralNode(t *testing.T) {
	tree := NewTree(NewMemPool())
	sessionID := int64(12345)

	_, err := tree.Create("/ephemeral", nil, zk.NodeEphemeral, zk.WorldACL(), sessionID)
	if err != nil {
		t.Fatalf("Create ephemeral failed: %v", err)
	}

	paths := tree.GetEphemeralsBySession(sessionID)
	if len(paths) != 1 || paths[0] != "/ephemeral" {
		t.Errorf("GetEphemeralsBySession = %v, want [/ephemeral]", paths)
	}

	tree.DeleteEphemeralsBySession(sessionID)

	stat, _ := tree.Exists("/ephemeral")
	if stat != nil {
		t.Error("Ephemeral node should have been deleted")
	}
}

func TestTree_SequentialNode(t *testing.T) {
	tree := NewTree(NewMemPool())
	tree.Create("/parent", nil, zk.NodePersistent, zk.WorldACL(), 0)

	// Create sequential nodes
	for i := 0; i < 3; i++ {
		_, err := tree.Create("/parent/seq", nil, zk.NodePersistentSequential, zk.WorldACL(), 0)
		if err != nil {
			t.Fatalf("Create sequential failed: %v", err)
		}
	}

	children, _, _ := tree.GetChildren("/parent")
	if len(children) != 3 {
		t.Errorf("len(children) = %d, want 3", len(children))
	}

	// Check sequential naming
	expected := []string{"seq0000000000", "seq0000000001", "seq0000000002"}
	for _, exp := range expected {
		found := false
		for _, child := range children {
			if child == exp {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected child %s not found in %v", exp, children)
		}
	}
}

func TestTree_NodeCount(t *testing.T) {
	tree := NewTree(NewMemPool())

	// Root node is always present
	if tree.NodeCount() != 1 {
		t.Errorf("NodeCount() = %d, want 1", tree.NodeCount())
	}

	tree.Create("/a", nil, zk.NodePersistent, zk.WorldACL(), 0)
	tree.Create("/b", nil, zk.NodePersistent, zk.WorldACL(), 0)

	if tree.NodeCount() != 3 {
		t.Errorf("NodeCount() = %d, want 3", tree.NodeCount())
	}
}

func BenchmarkTree_Create(b *testing.B) {
	tree := NewTree(NewMemPool())
	data := []byte("benchmark data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		path := fmt.Sprintf("/node%d", i)
		tree.Create(path, data, zk.NodePersistent, zk.WorldACL(), 0)
	}
}

func BenchmarkTree_Get(b *testing.B) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", []byte("benchmark data"), zk.NodePersistent, zk.WorldACL(), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Get("/test")
	}
}

func BenchmarkTree_GetParallel(b *testing.B) {
	tree := NewTree(NewMemPool())
	tree.Create("/test", []byte("benchmark data"), zk.NodePersistent, zk.WorldACL(), 0)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tree.Get("/test")
		}
	})
}

func BenchmarkTree_CreateParallel(b *testing.B) {
	tree := NewTree(NewMemPool())
	data := []byte("benchmark data")
	var counter int64 = 0
	var mu sync.Mutex

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mu.Lock()
			counter++
			path := fmt.Sprintf("/node%d", counter)
			mu.Unlock()
			tree.Create(path, data, zk.NodePersistent, zk.WorldACL(), 0)
		}
	})
}
