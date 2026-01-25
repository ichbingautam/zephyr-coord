# ZephyrCoord

A high-performance, ZooKeeper-compatible distributed coordination service written in Go.

## Features

- **ZooKeeper Wire Protocol Compatible** - Drop-in replacement for ZooKeeper clients
- **High Performance** - Optimized for low latency and high throughput
  - Tree.Get: ~43ns (lock-free reads)
  - Tree.Create: ~756ns
  - ZXID generation: ~1.7ns (atomic)
- **Durable Storage** - WAL with group commit + periodic snapshots
- **Session Management** - Timeout wheel for O(1) expiry detection
- **Watch Notifications** - One-shot watches with lock-free registry

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        ZephyrCoord Server                        │
├─────────────────────────────────────────────────────────────────┤
│  Protocol Layer     │  Session Manager  │    Watch Engine       │
│  (Jute Codec)       │  (Timeout Wheel)  │    (sync.Map)         │
├─────────────────────────────────────────────────────────────────┤
│                    Datastore Coordinator                         │
├─────────────────────────────────────────────────────────────────┤
│  Sharded Tree       │       WAL         │      Snapshot         │
│  (256 shards)       │   (Group Commit)  │   (LZ4 Compressed)    │
├─────────────────────────────────────────────────────────────────┤
│                      Memory Pool (Slab Allocator)                │
└─────────────────────────────────────────────────────────────────┘
```

## Project Structure

```
zephyr-coord/
├── pkg/zk/                  # Core ZooKeeper types
│   ├── zxid.go              # 64-bit transaction ID (epoch + counter)
│   ├── stat.go              # Node metadata structure
│   ├── znode.go             # Tree node with sync.Map children
│   └── acl.go               # Permission model
├── internal/
│   ├── storage/             # Persistence layer
│   │   ├── tree.go          # Sharded in-memory tree (256 shards)
│   │   ├── mempool.go       # Slab allocator (reduces GC)
│   │   ├── wal.go           # Write-ahead log with group commit
│   │   └── snapshot.go      # Point-in-time snapshots (gzip)
│   ├── server/              # Server components
│   │   ├── datastore.go     # Coordinates tree + WAL + snapshot
│   │   ├── session.go       # Session manager with timeout wheel
│   │   └── watch.go         # Watch registry and notifications
│   └── protocol/            # Wire protocol
│       ├── codec.go         # Jute-compatible binary encoder
│       └── request.go       # ZooKeeper request/response types
└── cmd/zephyr-coord/        # Server binary (coming soon)
```

## Building

```bash
go build ./...
```

## Testing

```bash
# Run all tests
go test -v ./...

# Run with race detector
go test -race ./...

# Run benchmarks
go test -bench=. -benchmem ./...
```

## Benchmarks (Apple M4)

| Operation | Time | Allocations |
|-----------|------|-------------|
| Tree.Get | 43ns | 2 allocs |
| Tree.Create | 756ns | 8 allocs |
| ZXID.Next | 1.7ns | 0 allocs |
| GetChild (sync.Map) | 7.9ns | 0 allocs |
| MemPool alloc 64B | 21ns | 1 alloc |
| WAL append (batch) | 786μs | 4 allocs |
| CreateRequest encode | 40ns | 0 allocs |

## Roadmap

- [x] **Phase 1: Core Storage** - Tree, WAL, Snapshot, Session, Watch
- [ ] **Phase 2: Networking** - TCP transport, connection handling
- [ ] **Phase 3: Consensus** - FastLeaderElection, ZAB protocol
- [ ] **Phase 4: Observability** - Metrics, structured logging, admin API
- [ ] **Phase 5: Recipes** - Leader election, distributed locks

## License

MIT
