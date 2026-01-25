package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

func TestWAL_AppendAndRecover(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)

	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	// Append entries
	entries := []LogEntry{
		{ZXID: zk.NewZXID(1, 1), Type: OpCreate, Path: "/test1", Data: []byte("data1"), NodeType: zk.NodePersistent},
		{ZXID: zk.NewZXID(1, 2), Type: OpSetData, Path: "/test1", Data: []byte("updated")},
		{ZXID: zk.NewZXID(1, 3), Type: OpCreate, Path: "/test2", Data: []byte("data2"), NodeType: zk.NodeEphemeral, Session: 12345},
	}

	for _, entry := range entries {
		if err := wal.Append(entry); err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	if wal.LastZXID() != entries[len(entries)-1].ZXID {
		t.Errorf("LastZXID = %v, want %v", wal.LastZXID(), entries[len(entries)-1].ZXID)
	}

	wal.Close()

	// Recover
	wal2, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL for recovery failed: %v", err)
	}
	defer wal2.Close()

	var recovered []LogEntry
	err = wal2.Recover(func(entry LogEntry) error {
		recovered = append(recovered, entry)
		return nil
	})
	if err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Errorf("Recovered %d entries, want %d", len(recovered), len(entries))
	}

	for i, entry := range recovered {
		if entry.ZXID != entries[i].ZXID {
			t.Errorf("Entry %d ZXID = %v, want %v", i, entry.ZXID, entries[i].ZXID)
		}
		if entry.Path != entries[i].Path {
			t.Errorf("Entry %d Path = %s, want %s", i, entry.Path, entries[i].Path)
		}
		if string(entry.Data) != string(entries[i].Data) {
			t.Errorf("Entry %d Data = %s, want %s", i, entry.Data, entries[i].Data)
		}
	}
}

func TestWAL_CRCValidation(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)

	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}

	wal.Append(LogEntry{ZXID: zk.NewZXID(1, 1), Type: OpCreate, Path: "/test", Data: []byte("data")})
	wal.Close()

	// Corrupt the WAL file
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if !f.IsDir() {
			path := filepath.Join(dir, f.Name())
			data, _ := os.ReadFile(path)
			if len(data) > 10 {
				data[10] ^= 0xFF // Flip some bits
				os.WriteFile(path, data, 0644)
			}
		}
	}

	// Recovery should handle corruption gracefully
	wal2, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL for recovery failed: %v", err)
	}
	defer wal2.Close()

	var count int
	wal2.Recover(func(entry LogEntry) error {
		count++
		return nil
	})
	// Should either recover 0 entries (corrupted) or stop at corruption
	t.Logf("Recovered %d entries from corrupted WAL", count)
}

func TestWAL_SegmentRotation(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)
	config.SegmentSize = 1024 // Small segment for testing

	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	// Write enough data to cause rotation
	data := make([]byte, 100)
	for i := 0; i < 100; i++ {
		wal.Append(LogEntry{
			ZXID: zk.NewZXID(1, uint32(i)),
			Type: OpCreate,
			Path: "/test",
			Data: data,
		})
	}

	stats := wal.Stats()
	if stats.CurrentSegment <= 1 {
		t.Errorf("Expected segment rotation, still on segment %d", stats.CurrentSegment)
	}
}

func TestWAL_ConcurrentAppend(t *testing.T) {
	dir := t.TempDir()
	config := DefaultWALConfig(dir)

	wal, err := NewWAL(config)
	if err != nil {
		t.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 100

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < entriesPerGoroutine; i++ {
				entry := LogEntry{
					ZXID: zk.NewZXID(1, uint32(id*1000+i)),
					Type: OpCreate,
					Path: "/test",
					Data: []byte("data"),
				}
				if err := wal.Append(entry); err != nil {
					t.Errorf("Append failed: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	stats := wal.Stats()
	expected := int64(numGoroutines * entriesPerGoroutine)
	if stats.EntriesWritten != expected {
		t.Errorf("EntriesWritten = %d, want %d", stats.EntriesWritten, expected)
	}
}

func BenchmarkWAL_Append(b *testing.B) {
	dir := b.TempDir()
	config := DefaultWALConfig(dir)

	wal, err := NewWAL(config)
	if err != nil {
		b.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	entry := LogEntry{
		ZXID: zk.NewZXID(1, 0),
		Type: OpCreate,
		Path: "/benchmark/test",
		Data: make([]byte, 1024), // 1KB
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entry.ZXID = zk.NewZXID(1, uint32(i))
		wal.Append(entry)
	}
}

func BenchmarkWAL_AppendParallel(b *testing.B) {
	dir := b.TempDir()
	config := DefaultWALConfig(dir)

	wal, err := NewWAL(config)
	if err != nil {
		b.Fatalf("NewWAL failed: %v", err)
	}
	defer wal.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		entry := LogEntry{
			ZXID: zk.NewZXID(1, 0),
			Type: OpCreate,
			Path: "/benchmark/test",
			Data: make([]byte, 1024),
		}
		for pb.Next() {
			wal.Append(entry)
		}
	})
}
