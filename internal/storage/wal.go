package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ichbingautam/zephyr-coord/pkg/zk"
)

// WAL errors
var (
	ErrWALClosed    = errors.New("WAL is closed")
	ErrWALCorrupted = errors.New("WAL is corrupted")
)

// OpType represents the type of operation in the WAL.
type OpType uint8

const (
	OpCreate OpType = iota + 1
	OpSetData
	OpDelete
	OpSetACL
)

func (o OpType) String() string {
	switch o {
	case OpCreate:
		return "CREATE"
	case OpSetData:
		return "SET_DATA"
	case OpDelete:
		return "DELETE"
	case OpSetACL:
		return "SET_ACL"
	default:
		return "UNKNOWN"
	}
}

// LogEntry represents a single entry in the WAL.
type LogEntry struct {
	ZXID     zk.ZXID
	Type     OpType
	Path     string
	Data     []byte
	NodeType zk.NodeType
	ACLs     []zk.ACL
	Session  int64
}

// WALConfig contains WAL configuration options.
type WALConfig struct {
	Dir          string        // Directory for WAL files
	SegmentSize  int64         // Max size per segment (default 64MB)
	SyncInterval time.Duration // Max time between syncs (default 1ms)
	BatchSize    int           // Max entries per batch (default 100)
	BufferSize   int           // Write buffer size (default 4MB)
}

// DefaultWALConfig returns default WAL configuration.
func DefaultWALConfig(dir string) WALConfig {
	return WALConfig{
		Dir:          dir,
		SegmentSize:  64 * 1024 * 1024, // 64MB
		SyncInterval: time.Millisecond,
		BatchSize:    100,
		BufferSize:   4 * 1024 * 1024, // 4MB
	}
}

// WAL is a write-ahead log with group commit support.
type WAL struct {
	config   WALConfig
	mu       sync.Mutex
	file     *os.File
	writer   *bufio.Writer
	segment  int64
	offset   int64
	lastZXID zk.ZXID

	// Group commit
	batch     []*pendingEntry
	batchCond *sync.Cond
	syncCh    chan struct{}
	closed    atomic.Bool

	// Metrics
	entriesWritten atomic.Int64
	bytesWritten   atomic.Int64
	syncs          atomic.Int64
}

type pendingEntry struct {
	entry LogEntry
	done  chan error
}

// NewWAL creates a new WAL.
func NewWAL(config WALConfig) (*WAL, error) {
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}

	w := &WAL{
		config: config,
		syncCh: make(chan struct{}, 1),
	}
	w.batchCond = sync.NewCond(&w.mu)

	// Find latest segment
	segment, zxid, err := w.findLatestSegment()
	if err != nil {
		return nil, err
	}
	w.segment = segment
	w.lastZXID = zxid

	// Open or create segment file
	if err := w.openSegment(segment); err != nil {
		return nil, err
	}

	// Start sync goroutine
	go w.syncLoop()

	return w, nil
}

// findLatestSegment finds the latest segment and its last ZXID.
func (w *WAL) findLatestSegment() (int64, zk.ZXID, error) {
	entries, err := os.ReadDir(w.config.Dir)
	if err != nil {
		return 0, 0, err
	}

	var segments []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var seg int64
		if _, err := fmt.Sscanf(entry.Name(), "wal.%016x", &seg); err == nil {
			segments = append(segments, seg)
		}
	}

	if len(segments) == 0 {
		return 1, 0, nil
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	latest := segments[len(segments)-1]

	// Read last ZXID from the segment
	lastZXID, err := w.readLastZXID(latest)
	if err != nil {
		return latest, 0, nil // Start fresh if corrupted
	}

	return latest, lastZXID, nil
}

// readLastZXID reads the last ZXID from a segment.
func (w *WAL) readLastZXID(segment int64) (zk.ZXID, error) {
	path := filepath.Join(w.config.Dir, fmt.Sprintf("wal.%016x", segment))
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var lastZXID zk.ZXID
	reader := bufio.NewReader(file)
	for {
		entry, err := readEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			break // Stop at corruption
		}
		lastZXID = entry.ZXID
	}
	return lastZXID, nil
}

// openSegment opens a segment file for appending.
func (w *WAL) openSegment(segment int64) error {
	path := filepath.Join(w.config.Dir, fmt.Sprintf("wal.%016x", segment))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, w.config.BufferSize)
	w.offset = info.Size()
	return nil
}

// Append appends an entry to the WAL.
// It blocks until the entry is synced to disk.
func (w *WAL) Append(entry LogEntry) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	pending := &pendingEntry{
		entry: entry,
		done:  make(chan error, 1),
	}

	w.mu.Lock()
	w.batch = append(w.batch, pending)
	shouldSignal := len(w.batch) >= w.config.BatchSize
	w.mu.Unlock()

	if shouldSignal {
		select {
		case w.syncCh <- struct{}{}:
		default:
		}
	}

	return <-pending.done
}

// syncLoop handles periodic sync.
func (w *WAL) syncLoop() {
	ticker := time.NewTicker(w.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.flush()
		case <-w.syncCh:
			w.flush()
		}

		if w.closed.Load() {
			w.flush()
			return
		}
	}
}

// flush writes and syncs pending entries.
func (w *WAL) flush() {
	w.mu.Lock()
	if len(w.batch) == 0 {
		w.mu.Unlock()
		return
	}

	batch := w.batch
	w.batch = nil
	w.mu.Unlock()

	var err error
	for _, pending := range batch {
		if err == nil {
			err = w.writeEntry(pending.entry)
		}
	}

	if err == nil {
		err = w.writer.Flush()
	}
	if err == nil {
		err = w.file.Sync()
		w.syncs.Add(1)
	}

	// Notify all waiters
	for _, pending := range batch {
		pending.done <- err
	}

	// Check if we need to rotate
	w.mu.Lock()
	if w.offset >= w.config.SegmentSize {
		w.rotateSegment()
	}
	w.mu.Unlock()
}

// writeEntry writes a single entry to the buffer.
func (w *WAL) writeEntry(entry LogEntry) error {
	// Entry format:
	// [4 bytes: total length]
	// [8 bytes: zxid]
	// [1 byte: op type]
	// [4 bytes: path length][path bytes]
	// [4 bytes: data length][data bytes]
	// [1 byte: node type]
	// [8 bytes: session]
	// [4 bytes: CRC32]

	var buf [4096]byte
	pos := 4 // Reserve space for length

	binary.BigEndian.PutUint64(buf[pos:], uint64(entry.ZXID))
	pos += 8

	buf[pos] = byte(entry.Type)
	pos++

	binary.BigEndian.PutUint32(buf[pos:], uint32(len(entry.Path)))
	pos += 4
	copy(buf[pos:], entry.Path)
	pos += len(entry.Path)

	binary.BigEndian.PutUint32(buf[pos:], uint32(len(entry.Data)))
	pos += 4
	copy(buf[pos:], entry.Data)
	pos += len(entry.Data)

	buf[pos] = byte(entry.NodeType)
	pos++

	binary.BigEndian.PutUint64(buf[pos:], uint64(entry.Session))
	pos += 8

	// CRC32 of everything after length
	crc := crc32.ChecksumIEEE(buf[4:pos])
	binary.BigEndian.PutUint32(buf[pos:], crc)
	pos += 4

	// Write length at the beginning
	binary.BigEndian.PutUint32(buf[0:], uint32(pos-4))

	n, err := w.writer.Write(buf[:pos])
	if err != nil {
		return err
	}

	w.offset += int64(n)
	w.lastZXID = entry.ZXID
	w.entriesWritten.Add(1)
	w.bytesWritten.Add(int64(n))

	return nil
}

// readEntry reads a single entry from a reader.
func readEntry(r *bufio.Reader) (*LogEntry, error) {
	var lengthBuf [4]byte
	if _, err := io.ReadFull(r, lengthBuf[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(lengthBuf[:])

	if length > 10*1024*1024 { // 10MB sanity check
		return nil, ErrWALCorrupted
	}

	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	// Verify CRC
	if len(data) < 4 {
		return nil, ErrWALCorrupted
	}
	storedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
	computedCRC := crc32.ChecksumIEEE(data[:len(data)-4])
	if storedCRC != computedCRC {
		return nil, ErrWALCorrupted
	}

	// Parse entry
	pos := 0
	entry := &LogEntry{}

	entry.ZXID = zk.ZXID(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	entry.Type = OpType(data[pos])
	pos++

	pathLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	entry.Path = string(data[pos : pos+int(pathLen)])
	pos += int(pathLen)

	dataLen := binary.BigEndian.Uint32(data[pos:])
	pos += 4
	entry.Data = make([]byte, dataLen)
	copy(entry.Data, data[pos:pos+int(dataLen)])
	pos += int(dataLen)

	entry.NodeType = zk.NodeType(data[pos])
	pos++

	entry.Session = int64(binary.BigEndian.Uint64(data[pos:]))

	return entry, nil
}

// rotateSegment creates a new segment.
func (w *WAL) rotateSegment() {
	w.writer.Flush()
	w.file.Sync()
	w.file.Close()

	w.segment++
	w.openSegment(w.segment)
}

// Recover replays WAL entries to rebuild state.
func (w *WAL) Recover(handler func(entry LogEntry) error) error {
	entries, err := os.ReadDir(w.config.Dir)
	if err != nil {
		return err
	}

	var segments []int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var seg int64
		if _, err := fmt.Sscanf(entry.Name(), "wal.%016x", &seg); err == nil {
			segments = append(segments, seg)
		}
	}

	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })

	for _, seg := range segments {
		if err := w.recoverSegment(seg, handler); err != nil {
			return err
		}
	}

	return nil
}

// recoverSegment replays a single segment.
func (w *WAL) recoverSegment(segment int64, handler func(entry LogEntry) error) error {
	path := filepath.Join(w.config.Dir, fmt.Sprintf("wal.%016x", segment))
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		entry, err := readEntry(reader)
		if err == io.EOF {
			break
		}
		if err == ErrWALCorrupted {
			// Stop at corruption but don't fail
			break
		}
		if err != nil {
			return err
		}
		if err := handler(*entry); err != nil {
			return err
		}
	}
	return nil
}

// LastZXID returns the last written ZXID.
func (w *WAL) LastZXID() zk.ZXID {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lastZXID
}

// Stats returns WAL statistics.
type WALStats struct {
	EntriesWritten int64
	BytesWritten   int64
	Syncs          int64
	CurrentSegment int64
	CurrentOffset  int64
}

func (w *WAL) Stats() WALStats {
	w.mu.Lock()
	defer w.mu.Unlock()
	return WALStats{
		EntriesWritten: w.entriesWritten.Load(),
		BytesWritten:   w.bytesWritten.Load(),
		Syncs:          w.syncs.Load(),
		CurrentSegment: w.segment,
		CurrentOffset:  w.offset,
	}
}

// Close closes the WAL.
func (w *WAL) Close() error {
	w.closed.Store(true)

	// Signal sync loop to exit
	select {
	case w.syncCh <- struct{}{}:
	default:
	}

	// Give sync loop time to flush
	time.Sleep(10 * time.Millisecond)

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		w.file.Sync()
		return w.file.Close()
	}
	return nil
}

// Truncate removes WAL segments before the given ZXID.
func (w *WAL) Truncate(beforeZXID zk.ZXID) error {
	entries, err := os.ReadDir(w.config.Dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var seg int64
		if _, err := fmt.Sscanf(entry.Name(), "wal.%016x", &seg); err == nil {
			lastZXID, _ := w.readLastZXID(seg)
			if lastZXID < beforeZXID && seg < w.segment {
				path := filepath.Join(w.config.Dir, entry.Name())
				os.Remove(path)
			}
		}
	}
	return nil
}
