package storage

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// Snapshot errors
var (
	ErrSnapshotCorrupted = errors.New("snapshot is corrupted")
)

// SnapshotConfig contains snapshot configuration.
type SnapshotConfig struct {
	Dir          string
	TriggerCount int64 // Trigger snapshot after N transactions
	Compression  bool  // Enable gzip compression
	RetainCount  int   // Number of snapshots to retain
}

// DefaultSnapshotConfig returns default snapshot configuration.
func DefaultSnapshotConfig(dir string) SnapshotConfig {
	return SnapshotConfig{
		Dir:          dir,
		TriggerCount: 100000,
		Compression:  true,
		RetainCount:  3,
	}
}

// SnapshotHeader contains snapshot metadata.
type SnapshotHeader struct {
	Magic     uint32 // Magic number for validation
	Version   uint32 // Snapshot format version
	ZXID      int64  // Last ZXID included
	Timestamp int64  // Snapshot creation time
	NodeCount int64  // Number of nodes
	Checksum  uint32 // CRC32 of all node data
}

const (
	snapshotMagic   = 0x5A504852 // "ZPHR"
	snapshotVersion = 1
)

// SnapshotNode represents a serialized node.
type SnapshotNode struct {
	Path     string
	Data     []byte
	NodeType zk.NodeType
	ACLs     []zk.ACL
	Stat     zk.Stat
}

// SnapshotManager handles snapshot creation and loading.
type SnapshotManager struct {
	config   SnapshotConfig
	mu       sync.Mutex
	lastZXID zk.ZXID
	opsCount int64
}

// NewSnapshotManager creates a new snapshot manager.
func NewSnapshotManager(config SnapshotConfig) (*SnapshotManager, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot directory: %w", err)
	}

	return &SnapshotManager{
		config: config,
	}, nil
}

// ShouldTakeSnapshot returns true if it's time to take a snapshot.
func (m *SnapshotManager) ShouldTakeSnapshot(opsCount int64) bool {
	return opsCount >= m.config.TriggerCount
}

// TakeSnapshot creates a snapshot of the tree.
func (m *SnapshotManager) TakeSnapshot(tree *Tree) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	zxid := tree.CurrentZXID()
	nodes := m.collectNodes(tree)

	// Create temp file
	tmpPath := filepath.Join(m.config.Dir, fmt.Sprintf("snapshot.%016x.tmp", zxid))
	file, err := os.Create(tmpPath)
	if err != nil {
		return err
	}

	writer := bufio.NewWriterSize(file, 4*1024*1024) // 4MB buffer

	var compressor *gzip.Writer
	var w io.Writer = writer
	if m.config.Compression {
		compressor = gzip.NewWriter(writer)
		w = compressor
	}

	// Calculate checksum while writing
	checksummer := crc32.NewIEEE()
	mw := io.MultiWriter(w, checksummer)

	// Write header placeholder (will be updated at end)
	header := SnapshotHeader{
		Magic:     snapshotMagic,
		Version:   snapshotVersion,
		ZXID:      int64(zxid),
		Timestamp: time.Now().UnixMilli(),
		NodeCount: int64(len(nodes)),
	}

	if err := writeHeader(w, header); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}

	// Write nodes
	for _, node := range nodes {
		if err := writeNode(mw, node); err != nil {
			file.Close()
			os.Remove(tmpPath)
			return err
		}
	}

	if compressor != nil {
		compressor.Close()
	}
	writer.Flush()
	file.Sync()
	file.Close()

	// Rename to final path
	finalPath := filepath.Join(m.config.Dir, fmt.Sprintf("snapshot.%016x", zxid))
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return err
	}

	m.lastZXID = zxid

	// Cleanup old snapshots
	m.cleanup()

	return nil
}

// collectNodes collects all nodes from the tree.
func (m *SnapshotManager) collectNodes(tree *Tree) []SnapshotNode {
	var nodes []SnapshotNode

	// Traverse all shards
	for _, shard := range tree.shards {
		shard.mu.RLock()
		for path, node := range shard.nodes {
			nodes = append(nodes, SnapshotNode{
				Path:     path,
				Data:     node.Data(),
				NodeType: node.NodeType(),
				ACLs:     node.ACL(),
				Stat:     node.Stat(),
			})
		}
		shard.mu.RUnlock()
	}

	// Sort by path for deterministic ordering
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].Path < nodes[j].Path
	})

	return nodes
}

// LoadSnapshot loads a snapshot into Thettree.
func (m *SnapshotManager) LoadSnapshot(tree *Tree) (zk.ZXID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Find latest snapshot
	snapshots, err := m.listSnapshots()
	if err != nil {
		return 0, err
	}

	if len(snapshots) == 0 {
		return 0, nil // No snapshot to load
	}

	latestPath := snapshots[len(snapshots)-1]

	file, err := os.Open(latestPath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 4*1024*1024)

	var r io.Reader = reader
	if m.config.Compression {
		gr, err := gzip.NewReader(reader)
		if err != nil {
			// Try without compression
			file.Seek(0, 0)
			reader.Reset(file)
			r = reader
		} else {
			defer gr.Close()
			r = gr
		}
	}

	// Read header
	header, err := readHeader(r)
	if err != nil {
		return 0, err
	}

	if header.Magic != snapshotMagic {
		return 0, ErrSnapshotCorrupted
	}

	// Read nodes and reconstruct tree
	for i := int64(0); i < header.NodeCount; i++ {
		node, err := readNode(r)
		if err != nil {
			return 0, err
		}

		if node.Path == "/" {
			// Skip root, it's already created
			continue
		}

		// Recreate node in tree
		parentPath, _ := zk.SplitPath(node.Path)

		// Ensure parent exists (create placeholder if needed)
		if parentPath != "/" {
			if stat, _ := tree.Exists(parentPath); stat == nil {
				tree.Create(parentPath, nil, zk.NodePersistent, zk.WorldACL(), 0)
			}
		}

		tree.Create(node.Path, node.Data, node.NodeType, node.ACLs, node.Stat.EphemeralOwner)
	}

	m.lastZXID = zk.ZXID(header.ZXID)
	return zk.ZXID(header.ZXID), nil
}

// listSnapshots returns sorted list of snapshot paths.
func (m *SnapshotManager) listSnapshots() ([]string, error) {
	entries, err := os.ReadDir(m.config.Dir)
	if err != nil {
		return nil, err
	}

	var paths []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var zxid int64
		if _, err := fmt.Sscanf(entry.Name(), "snapshot.%016x", &zxid); err == nil {
			paths = append(paths, filepath.Join(m.config.Dir, entry.Name()))
		}
	}

	sort.Strings(paths)
	return paths, nil
}

// cleanup removes old snapshots.
func (m *SnapshotManager) cleanup() {
	snapshots, _ := m.listSnapshots()
	if len(snapshots) <= m.config.RetainCount {
		return
	}

	// Remove oldest snapshots
	for i := 0; i < len(snapshots)-m.config.RetainCount; i++ {
		os.Remove(snapshots[i])
	}
}

// LastZXID returns the ZXID of the last snapshot.
func (m *SnapshotManager) LastZXID() zk.ZXID {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastZXID
}

// writeHeader writes the snapshot header.
func writeHeader(w io.Writer, h SnapshotHeader) error {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint32(buf[0:], h.Magic)
	binary.BigEndian.PutUint32(buf[4:], h.Version)
	binary.BigEndian.PutUint64(buf[8:], uint64(h.ZXID))
	binary.BigEndian.PutUint64(buf[16:], uint64(h.Timestamp))
	binary.BigEndian.PutUint64(buf[24:], uint64(h.NodeCount))
	_, err := w.Write(buf)
	return err
}

// readHeader reads the snapshot header.
func readHeader(r io.Reader) (*SnapshotHeader, error) {
	buf := make([]byte, 32)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return &SnapshotHeader{
		Magic:     binary.BigEndian.Uint32(buf[0:]),
		Version:   binary.BigEndian.Uint32(buf[4:]),
		ZXID:      int64(binary.BigEndian.Uint64(buf[8:])),
		Timestamp: int64(binary.BigEndian.Uint64(buf[16:])),
		NodeCount: int64(binary.BigEndian.Uint64(buf[24:])),
	}, nil
}

// writeNode writes a snapshot node.
func writeNode(w io.Writer, node SnapshotNode) error {
	// [4: path len][path][4: data len][data][1: type][stat][acls]

	// Path
	if err := writeString(w, node.Path); err != nil {
		return err
	}

	// Data
	if err := writeBytes(w, node.Data); err != nil {
		return err
	}

	// Type
	if _, err := w.Write([]byte{byte(node.NodeType)}); err != nil {
		return err
	}

	// Stat (88 bytes)
	statBuf := make([]byte, 88)
	binary.BigEndian.PutUint64(statBuf[0:], uint64(node.Stat.Czxid))
	binary.BigEndian.PutUint64(statBuf[8:], uint64(node.Stat.Mzxid))
	binary.BigEndian.PutUint64(statBuf[16:], uint64(node.Stat.Pzxid))
	binary.BigEndian.PutUint64(statBuf[24:], uint64(node.Stat.Ctime))
	binary.BigEndian.PutUint64(statBuf[32:], uint64(node.Stat.Mtime))
	binary.BigEndian.PutUint32(statBuf[40:], uint32(node.Stat.Version))
	binary.BigEndian.PutUint32(statBuf[44:], uint32(node.Stat.Cversion))
	binary.BigEndian.PutUint32(statBuf[48:], uint32(node.Stat.Aversion))
	binary.BigEndian.PutUint64(statBuf[52:], uint64(node.Stat.EphemeralOwner))
	binary.BigEndian.PutUint32(statBuf[60:], uint32(node.Stat.DataLength))
	binary.BigEndian.PutUint32(statBuf[64:], uint32(node.Stat.NumChildren))
	if _, err := w.Write(statBuf[:68]); err != nil {
		return err
	}

	// ACLs
	aclBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(aclBuf, uint32(len(node.ACLs)))
	if _, err := w.Write(aclBuf); err != nil {
		return err
	}
	for _, acl := range node.ACLs {
		if _, err := w.Write([]byte{byte(acl.Perms)}); err != nil {
			return err
		}
		if err := writeString(w, acl.Scheme); err != nil {
			return err
		}
		if err := writeString(w, acl.ID); err != nil {
			return err
		}
	}

	return nil
}

// readNode reads a snapshot node.
func readNode(r io.Reader) (*SnapshotNode, error) {
	node := &SnapshotNode{}

	// Path
	path, err := readString(r)
	if err != nil {
		return nil, err
	}
	node.Path = path

	// Data
	data, err := readBytes(r)
	if err != nil {
		return nil, err
	}
	node.Data = data

	// Type
	typeBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typeBuf); err != nil {
		return nil, err
	}
	node.NodeType = zk.NodeType(typeBuf[0])

	// Stat
	statBuf := make([]byte, 68)
	if _, err := io.ReadFull(r, statBuf); err != nil {
		return nil, err
	}
	node.Stat = zk.Stat{
		Czxid:          int64(binary.BigEndian.Uint64(statBuf[0:])),
		Mzxid:          int64(binary.BigEndian.Uint64(statBuf[8:])),
		Pzxid:          int64(binary.BigEndian.Uint64(statBuf[16:])),
		Ctime:          int64(binary.BigEndian.Uint64(statBuf[24:])),
		Mtime:          int64(binary.BigEndian.Uint64(statBuf[32:])),
		Version:        int32(binary.BigEndian.Uint32(statBuf[40:])),
		Cversion:       int32(binary.BigEndian.Uint32(statBuf[44:])),
		Aversion:       int32(binary.BigEndian.Uint32(statBuf[48:])),
		EphemeralOwner: int64(binary.BigEndian.Uint64(statBuf[52:])),
		DataLength:     int32(binary.BigEndian.Uint32(statBuf[60:])),
		NumChildren:    int32(binary.BigEndian.Uint32(statBuf[64:])),
	}

	// ACLs
	aclCountBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, aclCountBuf); err != nil {
		return nil, err
	}
	aclCount := binary.BigEndian.Uint32(aclCountBuf)
	node.ACLs = make([]zk.ACL, aclCount)
	for i := uint32(0); i < aclCount; i++ {
		permBuf := make([]byte, 1)
		if _, err := io.ReadFull(r, permBuf); err != nil {
			return nil, err
		}
		scheme, err := readString(r)
		if err != nil {
			return nil, err
		}
		id, err := readString(r)
		if err != nil {
			return nil, err
		}
		node.ACLs[i] = zk.ACL{
			Perms:  zk.Permission(permBuf[0]),
			Scheme: scheme,
			ID:     id,
		}
	}

	return node, nil
}

func writeString(w io.Writer, s string) error {
	buf := make([]byte, 4+len(s))
	binary.BigEndian.PutUint32(buf, uint32(len(s)))
	copy(buf[4:], s)
	_, err := w.Write(buf)
	return err
}

func readString(r io.Reader) (string, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return "", err
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > 10*1024*1024 { // 10MB sanity check
		return "", ErrSnapshotCorrupted
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return "", err
	}
	return string(data), nil
}

func writeBytes(w io.Writer, b []byte) error {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(len(b)))
	if _, err := w.Write(buf); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func readBytes(r io.Reader) ([]byte, error) {
	lenBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBuf); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf)
	if length > 100*1024*1024 { // 100MB max
		return nil, ErrSnapshotCorrupted
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}
