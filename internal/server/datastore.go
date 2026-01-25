// Package server provides the core server components for ZephyrCoord.
package server

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ichbingautam/zephyr-coord/internal/storage"
	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// DatastoreConfig contains configuration for the datastore.
type DatastoreConfig struct {
	DataDir     string
	WALDir      string
	SnapshotDir string
	SnapCount   int64 // Transactions between snapshots
}

// DefaultDatastoreConfig returns default configuration.
func DefaultDatastoreConfig(dataDir string) DatastoreConfig {
	return DatastoreConfig{
		DataDir:     dataDir,
		WALDir:      dataDir + "/wal",
		SnapshotDir: dataDir + "/snap",
		SnapCount:   100000,
	}
}

// Datastore coordinates tree, WAL, and snapshot operations.
// It provides the primary interface for data operations with durability guarantees.
type Datastore struct {
	config   DatastoreConfig
	tree     *storage.Tree
	wal      *storage.WAL
	snapshot *storage.SnapshotManager
	pool     *storage.MemPool

	mu         sync.RWMutex
	opsCount   atomic.Int64
	recovering bool
}

// NewDatastore creates a new datastore.
func NewDatastore(config DatastoreConfig) (*Datastore, error) {
	pool := storage.NewMemPool()
	tree := storage.NewTree(pool)

	walConfig := storage.DefaultWALConfig(config.WALDir)
	wal, err := storage.NewWAL(walConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	snapConfig := storage.DefaultSnapshotConfig(config.SnapshotDir)
	snapConfig.TriggerCount = config.SnapCount
	snapMgr, err := storage.NewSnapshotManager(snapConfig)
	if err != nil {
		wal.Close()
		return nil, fmt.Errorf("failed to create snapshot manager: %w", err)
	}

	ds := &Datastore{
		config:   config,
		tree:     tree,
		wal:      wal,
		snapshot: snapMgr,
		pool:     pool,
	}

	return ds, nil
}

// Recover restores state from snapshot and WAL.
func (ds *Datastore) Recover() error {
	ds.mu.Lock()
	ds.recovering = true
	defer func() {
		ds.recovering = false
		ds.mu.Unlock()
	}()

	// Load latest snapshot
	snapshotZXID, err := ds.snapshot.LoadSnapshot(ds.tree)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	// Replay WAL from after snapshot
	err = ds.wal.Recover(func(entry storage.LogEntry) error {
		// Skip entries already in snapshot
		if entry.ZXID <= snapshotZXID {
			return nil
		}
		return ds.applyEntry(entry)
	})
	if err != nil {
		return fmt.Errorf("failed to replay WAL: %w", err)
	}

	return nil
}

// applyEntry applies a log entry to the tree (used during recovery).
func (ds *Datastore) applyEntry(entry storage.LogEntry) error {
	switch entry.Type {
	case storage.OpCreate:
		_, err := ds.tree.Create(entry.Path, entry.Data, entry.NodeType, entry.ACLs, entry.Session)
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	case storage.OpSetData:
		_, err := ds.tree.Set(entry.Path, entry.Data, -1)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	case storage.OpDelete:
		err := ds.tree.Delete(entry.Path, -1)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
	}
	return nil
}

// Create creates a new node.
func (ds *Datastore) Create(path string, data []byte, nodeType zk.NodeType, acl []zk.ACL, sessionID int64) (*zk.Stat, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Create in tree first to validate
	stat, err := ds.tree.Create(path, data, nodeType, acl, sessionID)
	if err != nil {
		return nil, err
	}

	// Log to WAL
	entry := storage.LogEntry{
		ZXID:     ds.tree.CurrentZXID(),
		Type:     storage.OpCreate,
		Path:     path,
		Data:     data,
		NodeType: nodeType,
		ACLs:     acl,
		Session:  sessionID,
	}
	if err := ds.wal.Append(entry); err != nil {
		return nil, fmt.Errorf("WAL append failed: %w", err)
	}

	ds.checkSnapshot()
	return stat, nil
}

// Get retrieves node data.
func (ds *Datastore) Get(path string) ([]byte, *zk.Stat, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.tree.Get(path)
}

// Set updates node data.
func (ds *Datastore) Set(path string, data []byte, version int32) (*zk.Stat, error) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	stat, err := ds.tree.Set(path, data, version)
	if err != nil {
		return nil, err
	}

	entry := storage.LogEntry{
		ZXID: ds.tree.CurrentZXID(),
		Type: storage.OpSetData,
		Path: path,
		Data: data,
	}
	if err := ds.wal.Append(entry); err != nil {
		return nil, fmt.Errorf("WAL append failed: %w", err)
	}

	ds.checkSnapshot()
	return stat, nil
}

// Delete removes a node.
func (ds *Datastore) Delete(path string, version int32) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if err := ds.tree.Delete(path, version); err != nil {
		return err
	}

	entry := storage.LogEntry{
		ZXID: ds.tree.CurrentZXID(),
		Type: storage.OpDelete,
		Path: path,
	}
	if err := ds.wal.Append(entry); err != nil {
		return fmt.Errorf("WAL append failed: %w", err)
	}

	ds.checkSnapshot()
	return nil
}

// Exists checks if a node exists.
func (ds *Datastore) Exists(path string) (*zk.Stat, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.tree.Exists(path)
}

// GetChildren returns child node names.
func (ds *Datastore) GetChildren(path string) ([]string, *zk.Stat, error) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.tree.GetChildren(path)
}

// DeleteEphemeralsBySession deletes all ephemeral nodes for a session.
func (ds *Datastore) DeleteEphemeralsBySession(sessionID int64) error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	paths := ds.tree.GetEphemeralsBySession(sessionID)
	for _, path := range paths {
		if err := ds.tree.Delete(path, -1); err != nil {
			continue // Best effort
		}
		entry := storage.LogEntry{
			ZXID: ds.tree.CurrentZXID(),
			Type: storage.OpDelete,
			Path: path,
		}
		ds.wal.Append(entry)
	}
	return nil
}

// checkSnapshot triggers a snapshot if needed.
func (ds *Datastore) checkSnapshot() {
	count := ds.opsCount.Add(1)
	if ds.snapshot.ShouldTakeSnapshot(count) {
		go func() {
			ds.mu.RLock()
			defer ds.mu.RUnlock()
			ds.snapshot.TakeSnapshot(ds.tree)
			ds.opsCount.Store(0)
		}()
	}
}

// CurrentZXID returns the current transaction ID.
func (ds *Datastore) CurrentZXID() zk.ZXID {
	return ds.tree.CurrentZXID()
}

// NodeCount returns the total number of nodes.
func (ds *Datastore) NodeCount() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return ds.tree.NodeCount()
}

// Stats returns datastore statistics.
type DatastoreStats struct {
	NodeCount int
	ZXID      zk.ZXID
	OpsCount  int64
	WALStats  storage.WALStats
	PoolStats storage.MemPoolStats
}

func (ds *Datastore) Stats() DatastoreStats {
	ds.mu.RLock()
	defer ds.mu.RUnlock()
	return DatastoreStats{
		NodeCount: ds.tree.NodeCount(),
		ZXID:      ds.tree.CurrentZXID(),
		OpsCount:  ds.opsCount.Load(),
		WALStats:  ds.wal.Stats(),
		PoolStats: ds.pool.Stats(),
	}
}

// Close closes the datastore.
func (ds *Datastore) Close() error {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	// Take final snapshot
	ds.snapshot.TakeSnapshot(ds.tree)

	return ds.wal.Close()
}
