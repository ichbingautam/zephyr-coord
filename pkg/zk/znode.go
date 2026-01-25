package zk

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// NodeType represents the type of a ZNode.
type NodeType uint8

const (
	// NodePersistent is a standard persistent node.
	NodePersistent NodeType = iota

	// NodeEphemeral is deleted when the owning session ends.
	NodeEphemeral

	// NodePersistentSequential is persistent with an auto-incrementing suffix.
	NodePersistentSequential

	// NodeEphemeralSequential is ephemeral with an auto-incrementing suffix.
	NodeEphemeralSequential
)

// IsEphemeral returns true if the node type is ephemeral.
func (n NodeType) IsEphemeral() bool {
	return n == NodeEphemeral || n == NodeEphemeralSequential
}

// IsSequential returns true if the node type is sequential.
func (n NodeType) IsSequential() bool {
	return n == NodePersistentSequential || n == NodeEphemeralSequential
}

// String returns a string representation of the node type.
func (n NodeType) String() string {
	switch n {
	case NodePersistent:
		return "persistent"
	case NodeEphemeral:
		return "ephemeral"
	case NodePersistentSequential:
		return "persistent_sequential"
	case NodeEphemeralSequential:
		return "ephemeral_sequential"
	default:
		return "unknown"
	}
}

// Common errors
var (
	ErrNodeExists             = errors.New("node already exists")
	ErrNoNode                 = errors.New("node does not exist")
	ErrNotEmpty               = errors.New("node has children")
	ErrBadVersion             = errors.New("version mismatch")
	ErrNoAuth                 = errors.New("not authenticated")
	ErrInvalidACL             = errors.New("invalid ACL")
	ErrInvalidPath            = errors.New("invalid path")
	ErrSessionExpired         = errors.New("session expired")
	ErrSessionMoved           = errors.New("session moved")
	ErrEphemeralChild         = errors.New("ephemeral nodes cannot have children")
	ErrNoChildrenForEphemeral = errors.New("ephemeral nodes cannot have children")
)

// ZNode represents a node in the ZooKeeper data tree.
// It uses sync.Map for children to enable lock-free reads.
type ZNode struct {
	mu       sync.RWMutex
	stat     Stat
	data     []byte // COW: replaced entirely on update
	acl      []ACL
	children sync.Map // string -> *ZNode (lock-free for reads)
	parent   *ZNode
	name     string
	nodeType NodeType
}

// NewZNode creates a new ZNode.
func NewZNode(name string, data []byte, nodeType NodeType, acl []ACL, zxid ZXID, sessionID int64) *ZNode {
	var ephemeralOwner int64
	if nodeType.IsEphemeral() {
		ephemeralOwner = sessionID
	}

	// Copy data for immutability
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &ZNode{
		stat:     NewStat(zxid, ephemeralOwner, int32(len(data))),
		data:     dataCopy,
		acl:      CloneACLs(acl),
		name:     name,
		nodeType: nodeType,
	}
}

// Name returns the node's name (last path component).
func (n *ZNode) Name() string {
	return n.name
}

// Stat returns a copy of the node's stat.
func (n *ZNode) Stat() Stat {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stat.Clone()
}

// Data returns a copy of the node's data.
func (n *ZNode) Data() []byte {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.data == nil {
		return nil
	}
	result := make([]byte, len(n.data))
	copy(result, n.data)
	return result
}

// SetData updates the node's data (COW).
func (n *ZNode) SetData(data []byte, zxid ZXID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// COW: create new slice
	newData := make([]byte, len(data))
	copy(newData, data)
	n.data = newData
	n.stat.UpdateData(zxid, int32(len(data)))
}

// ACL returns a copy of the node's ACL.
func (n *ZNode) ACL() []ACL {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return CloneACLs(n.acl)
}

// SetACL updates the node's ACL.
func (n *ZNode) SetACL(acl []ACL, zxid ZXID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.acl = CloneACLs(acl)
	n.stat.UpdateACL(zxid)
}

// AddChild adds a child node.
func (n *ZNode) AddChild(child *ZNode, zxid ZXID) error {
	if n.nodeType.IsEphemeral() {
		return ErrEphemeralChild
	}

	child.parent = n
	n.children.Store(child.name, child)

	n.mu.Lock()
	n.stat.UpdateChildren(zxid, 1)
	n.mu.Unlock()

	return nil
}

// RemoveChild removes a child node by name.
func (n *ZNode) RemoveChild(name string, zxid ZXID) {
	n.children.Delete(name)

	n.mu.Lock()
	n.stat.UpdateChildren(zxid, -1)
	n.mu.Unlock()
}

// GetChild returns a child node by name.
func (n *ZNode) GetChild(name string) (*ZNode, bool) {
	val, ok := n.children.Load(name)
	if !ok {
		return nil, false
	}
	return val.(*ZNode), true
}

// Children returns a list of child names.
func (n *ZNode) Children() []string {
	var names []string
	n.children.Range(func(key, _ interface{}) bool {
		names = append(names, key.(string))
		return true
	})
	return names
}

// ChildCount returns the number of children.
func (n *ZNode) ChildCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return int(n.stat.NumChildren)
}

// IsEphemeral returns true if the node is ephemeral.
func (n *ZNode) IsEphemeral() bool {
	return n.nodeType.IsEphemeral()
}

// Parent returns the parent node.
func (n *ZNode) Parent() *ZNode {
	return n.parent
}

// NodeType returns the node type.
func (n *ZNode) NodeType() NodeType {
	return n.nodeType
}

// EphemeralOwner returns the session ID for ephemeral nodes, 0 otherwise.
func (n *ZNode) EphemeralOwner() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stat.EphemeralOwner
}

// Version returns the current data version.
func (n *ZNode) Version() int32 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.stat.Version
}

// Path utilities

// ValidatePath checks if a path is valid.
func ValidatePath(path string) error {
	if path == "" {
		return ErrInvalidPath
	}
	if path[0] != '/' {
		return ErrInvalidPath
	}
	if path != "/" && path[len(path)-1] == '/' {
		return ErrInvalidPath
	}
	if strings.Contains(path, "//") {
		return ErrInvalidPath
	}
	return nil
}

// SplitPath splits a path into parent path and node name.
func SplitPath(path string) (parentPath string, name string) {
	if path == "/" {
		return "", ""
	}
	idx := strings.LastIndexByte(path, '/')
	if idx == 0 {
		return "/", path[1:]
	}
	return path[:idx], path[idx+1:]
}

// JoinPath joins a parent path and node name.
func JoinPath(parent, name string) string {
	if parent == "/" {
		return "/" + name
	}
	return parent + "/" + name
}

// PathComponents returns the components of a path (excluding root).
func PathComponents(path string) []string {
	if path == "/" {
		return nil
	}
	return strings.Split(path[1:], "/")
}

// FormatSequence formats a sequence number as a 10-digit zero-padded string.
func FormatSequence(seq uint32) string {
	return fmt.Sprintf("%010d", seq)
}
