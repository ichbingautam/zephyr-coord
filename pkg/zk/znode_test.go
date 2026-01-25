package zk

import (
	"testing"
)

func TestZNode_Creation(t *testing.T) {
	data := []byte("test data")
	acl := WorldACL()
	zxid := NewZXID(1, 100)

	node := NewZNode("test", data, NodePersistent, acl, zxid, 0)

	if node.Name() != "test" {
		t.Errorf("Name() = %s, want test", node.Name())
	}

	gotData := node.Data()
	if string(gotData) != string(data) {
		t.Errorf("Data() = %s, want %s", gotData, data)
	}

	stat := node.Stat()
	if stat.Czxid != int64(zxid) {
		t.Errorf("Czxid = %d, want %d", stat.Czxid, zxid)
	}
	if stat.Version != 0 {
		t.Errorf("Version = %d, want 0", stat.Version)
	}
}

func TestZNode_SetData(t *testing.T) {
	node := NewZNode("test", []byte("initial"), NodePersistent, WorldACL(), NewZXID(1, 1), 0)

	newData := []byte("updated data")
	node.SetData(newData, NewZXID(1, 2))

	gotData := node.Data()
	if string(gotData) != string(newData) {
		t.Errorf("Data() = %s, want %s", gotData, newData)
	}

	stat := node.Stat()
	if stat.Version != 1 {
		t.Errorf("Version = %d, want 1", stat.Version)
	}
	if stat.DataLength != int32(len(newData)) {
		t.Errorf("DataLength = %d, want %d", stat.DataLength, len(newData))
	}
}

func TestZNode_Children(t *testing.T) {
	parent := NewZNode("parent", nil, NodePersistent, WorldACL(), NewZXID(1, 1), 0)
	child1 := NewZNode("child1", nil, NodePersistent, WorldACL(), NewZXID(1, 2), 0)
	child2 := NewZNode("child2", nil, NodePersistent, WorldACL(), NewZXID(1, 3), 0)

	if err := parent.AddChild(child1, NewZXID(1, 2)); err != nil {
		t.Fatalf("AddChild failed: %v", err)
	}
	if err := parent.AddChild(child2, NewZXID(1, 3)); err != nil {
		t.Fatalf("AddChild failed: %v", err)
	}

	if parent.ChildCount() != 2 {
		t.Errorf("ChildCount() = %d, want 2", parent.ChildCount())
	}

	children := parent.Children()
	if len(children) != 2 {
		t.Errorf("len(Children()) = %d, want 2", len(children))
	}

	got, ok := parent.GetChild("child1")
	if !ok || got != child1 {
		t.Error("GetChild(child1) failed")
	}

	// Test removal
	parent.RemoveChild("child1", NewZXID(1, 4))
	if parent.ChildCount() != 1 {
		t.Errorf("After removal, ChildCount() = %d, want 1", parent.ChildCount())
	}
}

func TestZNode_EphemeralCannotHaveChildren(t *testing.T) {
	ephemeral := NewZNode("ephemeral", nil, NodeEphemeral, WorldACL(), NewZXID(1, 1), 12345)
	child := NewZNode("child", nil, NodePersistent, WorldACL(), NewZXID(1, 2), 0)

	err := ephemeral.AddChild(child, NewZXID(1, 2))
	if err != ErrEphemeralChild {
		t.Errorf("Expected ErrEphemeralChild, got %v", err)
	}
}

func TestZNode_EphemeralOwner(t *testing.T) {
	sessionID := int64(0x123456789)
	node := NewZNode("test", nil, NodeEphemeral, WorldACL(), NewZXID(1, 1), sessionID)

	if !node.IsEphemeral() {
		t.Error("IsEphemeral() should return true")
	}
	if node.EphemeralOwner() != sessionID {
		t.Errorf("EphemeralOwner() = %d, want %d", node.EphemeralOwner(), sessionID)
	}
}

func TestValidatePath(t *testing.T) {
	tests := []struct {
		path  string
		valid bool
	}{
		{"/", true},
		{"/a", true},
		{"/a/b/c", true},
		{"", false},
		{"a", false},
		{"/a/", false},
		{"//a", false},
		{"/a//b", false},
	}

	for _, tt := range tests {
		err := ValidatePath(tt.path)
		if tt.valid && err != nil {
			t.Errorf("ValidatePath(%q) = %v, want nil", tt.path, err)
		}
		if !tt.valid && err == nil {
			t.Errorf("ValidatePath(%q) = nil, want error", tt.path)
		}
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path       string
		wantParent string
		wantName   string
	}{
		{"/", "", ""},
		{"/a", "/", "a"},
		{"/a/b", "/a", "b"},
		{"/a/b/c", "/a/b", "c"},
	}

	for _, tt := range tests {
		parent, name := SplitPath(tt.path)
		if parent != tt.wantParent || name != tt.wantName {
			t.Errorf("SplitPath(%q) = (%q, %q), want (%q, %q)",
				tt.path, parent, name, tt.wantParent, tt.wantName)
		}
	}
}

func BenchmarkZNode_GetData(b *testing.B) {
	data := make([]byte, 1024) // 1KB data
	node := NewZNode("test", data, NodePersistent, WorldACL(), NewZXID(1, 1), 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = node.Data()
	}
}

func BenchmarkZNode_GetChild(b *testing.B) {
	parent := NewZNode("parent", nil, NodePersistent, WorldACL(), NewZXID(1, 1), 0)
	child := NewZNode("child", nil, NodePersistent, WorldACL(), NewZXID(1, 2), 0)
	parent.AddChild(child, NewZXID(1, 2))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = parent.GetChild("child")
	}
}
