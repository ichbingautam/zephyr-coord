package storage

import (
	"hash/fnv"
	"sync"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

const (
	// NumShards is the number of shards for the tree.
	// Higher values reduce contention but increase memory overhead.
	NumShards = 256
)

// Tree is a sharded in-memory tree storage for ZNodes.
// It provides concurrent read access with minimal contention.
type Tree struct {
	root    *zk.ZNode
	shards  [NumShards]*shard
	zxidGen *zk.ZXIDGenerator
	pool    *MemPool
}

type shard struct {
	mu    sync.RWMutex
	nodes map[string]*zk.ZNode // Full path -> node
	seq   map[string]uint32    // Parent path -> next sequence number
}

// NewTree creates a new Tree with an initialized root node.
func NewTree(pool *MemPool) *Tree {
	t := &Tree{
		zxidGen: zk.NewZXIDGenerator(zk.NewZXID(0, 0)),
		pool:    pool,
	}

	// Initialize shards
	for i := range t.shards {
		t.shards[i] = &shard{
			nodes: make(map[string]*zk.ZNode),
			seq:   make(map[string]uint32),
		}
	}

	// Create root node
	t.root = zk.NewZNode("", nil, zk.NodePersistent, zk.WorldACL(), t.zxidGen.Next(), 0)
	t.getShard("/").nodes["/"] = t.root

	return t
}

// getShard returns the shard for a given path using FNV-1a hash.
func (t *Tree) getShard(path string) *shard {
	h := fnv.New32a()
	h.Write([]byte(path))
	return t.shards[h.Sum32()%NumShards]
}

// getShardIndex returns the shard index for a given path.
func getShardIndex(path string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(path))
	return h.Sum32() % NumShards
}

// Create creates a new node at the specified path.
func (t *Tree) Create(path string, data []byte, nodeType zk.NodeType, acl []zk.ACL, sessionID int64) (*zk.Stat, error) {
	if err := zk.ValidatePath(path); err != nil {
		return nil, err
	}
	if path == "/" {
		return nil, zk.ErrNodeExists
	}

	parentPath, name := zk.SplitPath(path)

	// Handle sequential nodes
	actualPath := path
	if nodeType.IsSequential() {
		parentShard := t.getShard(parentPath)
		parentShard.mu.Lock()
		seq := parentShard.seq[parentPath]
		parentShard.seq[parentPath] = seq + 1
		parentShard.mu.Unlock()
		// Append 10-digit sequence number
		name = name + zk.FormatSequence(seq)
		actualPath = zk.JoinPath(parentPath, name)
	}

	// Get parent node
	parent, err := t.getNode(parentPath)
	if err != nil {
		return nil, err
	}

	// Check if node already exists
	shard := t.getShard(actualPath)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	if _, exists := shard.nodes[actualPath]; exists {
		return nil, zk.ErrNodeExists
	}

	// Create the node
	zxid := t.zxidGen.Next()
	node := zk.NewZNode(name, data, nodeType, acl, zxid, sessionID)

	// Add to parent
	if err := parent.AddChild(node, zxid); err != nil {
		return nil, err
	}

	// Store in shard
	shard.nodes[actualPath] = node

	stat := node.Stat()
	return &stat, nil
}

// Get retrieves the data and stat for a node.
func (t *Tree) Get(path string) ([]byte, *zk.Stat, error) {
	node, err := t.getNode(path)
	if err != nil {
		return nil, nil, err
	}
	stat := node.Stat()
	return node.Data(), &stat, nil
}

// Set updates the data for a node.
func (t *Tree) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	node, err := t.getNode(path)
	if err != nil {
		return nil, err
	}

	// Optimistic locking
	if version != -1 && node.Version() != version {
		return nil, zk.ErrBadVersion
	}

	zxid := t.zxidGen.Next()
	node.SetData(data, zxid)

	stat := node.Stat()
	return &stat, nil
}

// Delete removes a node.
func (t *Tree) Delete(path string, version int32) error {
	if path == "/" {
		return zk.ErrInvalidPath
	}

	node, err := t.getNode(path)
	if err != nil {
		return err
	}

	// Check version
	if version != -1 && node.Version() != version {
		return zk.ErrBadVersion
	}

	// Check if node has children
	if node.ChildCount() > 0 {
		return zk.ErrNotEmpty
	}

	parentPath, name := zk.SplitPath(path)
	parent, err := t.getNode(parentPath)
	if err != nil {
		return err
	}

	// Remove from parent
	zxid := t.zxidGen.Next()
	parent.RemoveChild(name, zxid)

	// Remove from shard
	shard := t.getShard(path)
	shard.mu.Lock()
	delete(shard.nodes, path)
	shard.mu.Unlock()

	return nil
}

// Exists checks if a node exists and returns its stat.
func (t *Tree) Exists(path string) (*zk.Stat, error) {
	node, err := t.getNode(path)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, nil
		}
		return nil, err
	}
	stat := node.Stat()
	return &stat, nil
}

// GetChildren returns the list of child names.
func (t *Tree) GetChildren(path string) ([]string, *zk.Stat, error) {
	node, err := t.getNode(path)
	if err != nil {
		return nil, nil, err
	}
	stat := node.Stat()
	return node.Children(), &stat, nil
}

// getNode retrieves a node by path.
func (t *Tree) getNode(path string) (*zk.ZNode, error) {
	if err := zk.ValidatePath(path); err != nil {
		return nil, err
	}

	if path == "/" {
		return t.root, nil
	}

	shard := t.getShard(path)
	shard.mu.RLock()
	node, ok := shard.nodes[path]
	shard.mu.RUnlock()

	if !ok {
		return nil, zk.ErrNoNode
	}
	return node, nil
}

// CurrentZXID returns the current ZXID.
func (t *Tree) CurrentZXID() zk.ZXID {
	return t.zxidGen.Current()
}

// SetEpoch sets a new epoch (used during leader election).
func (t *Tree) SetEpoch(epoch uint32) zk.ZXID {
	return t.zxidGen.SetEpoch(epoch)
}

// NodeCount returns the total number of nodes.
func (t *Tree) NodeCount() int {
	count := 0
	for _, shard := range t.shards {
		shard.mu.RLock()
		count += len(shard.nodes)
		shard.mu.RUnlock()
	}
	return count
}

// GetEphemeralsBySession returns all ephemeral node paths for a session.
func (t *Tree) GetEphemeralsBySession(sessionID int64) []string {
	var paths []string
	for _, shard := range t.shards {
		shard.mu.RLock()
		for path, node := range shard.nodes {
			if node.EphemeralOwner() == sessionID {
				paths = append(paths, path)
			}
		}
		shard.mu.RUnlock()
	}
	return paths
}

// DeleteEphemeralsBySession deletes all ephemeral nodes for a session.
func (t *Tree) DeleteEphemeralsBySession(sessionID int64) error {
	paths := t.GetEphemeralsBySession(sessionID)
	for _, path := range paths {
		// Ignore errors - node may have been deleted already
		_ = t.Delete(path, -1)
	}
	return nil
}
