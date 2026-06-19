# DRedis

A distributed Redis-compatible in-memory key-value store with consistent hashing, vector clock CRDTs, gossip-based failure detection, and quorum replication.

## Quick Start

### Single node
```bash
docker build -t dredis .
docker run -p 6379:6379 dredis
redis-cli -p 6379 PING
```

### Multi-node cluster (3 nodes)
```bash
./scripts/start_cluster.sh 3 6380
redis-cli -p 6380 CLUSTER INFO
redis-cli -p 6380 CLUSTER NODES
```

Stop cluster:
```bash
./scripts/kill_cluster.sh 3
```

### Run tests
```bash
# Start a cluster first, then:
./tests/test_basic.sh 6380
./tests/test_cluster.sh 6380
```

## Architecture

### Consistent Hashing
- 16384 hash slots, CRC32c-64 based tokens
- 150 virtual nodes per physical node
- `std::map<uint64_t, NodeID>` ring for O(log N) key-to-node lookup

### Vector Clocks
- Every key-value entry carries a vector clock
- Writes bump the local node's counter
- CRDT merge: `max(local, remote)` per node
- Concurrent writes resolved by deterministic LWW (lexicographic serialized value)

### Gossip Protocol
- PING/PONG exchange every 200ms
- SUSPECT after `failure_timeout_ms` (3s default)
- DEAD after SUSPECT for `failure_timeout_ms * 3`
- State delta propagation with version-based merge
- Defense-in-depth: DEAD gated on socket health

### Replication
- Synchronous replication to `replication_factor - 1` replicas
- Write quorum: `+OK` returned after W confirmations
- Read quorum: latest version among R responses via vector clock comparison
- Anti-entropy: 1024-slot Merkle tree with subtree binary search

### Persistence
- Append-Only File (AOF) with everysec/always/no fsync
- AOF replay on startup restores state including vector clocks
- BGREWRITEAOF for compaction (async)

## Configuration

Via environment variables:
- `DREDIS_PORT` (default: 6379)
- `DREDIS_NODE_ID` (auto-generated, persisted to node.id)
- `DREDIS_IP` (default: 127.0.0.1)
- `DREDIS_SEEDS` (comma-separated host:port list)
- `DREDIS_REPL_FACTOR` (default: 3)
- `DREDIS_WRITE_QUORUM` (default: 2)
- `DREDIS_READ_QUORUM` (default: 2)

Or via `dredis.conf`:
```
client_port 6380
replication_factor 3
write_quorum 2
read_quorum 2
gossip_interval_ms 200
failure_timeout_ms 3000
aof_fsync everysec
maxmemory 256mb
seed 127.0.0.1:6380
seed 127.0.0.1:6381
```

## Commands

**String (14):** GET, SET, MGET, MSET, GETSET, SETNX, SETEX, DEL, EXISTS, APPEND, STRLEN, INCR, INCRBY, DECR, DECRBY

**Hash (10):** HSET, HGET, HDEL, HGETALL, HEXISTS, HLEN, HMSET, HKEYS, HVALS, HINCRBY

**List (10):** LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN, LINDEX, LINSERT, LREM, LSET

**Set (6):** SADD, SREM, SMEMBERS, SCARD, SISMEMBER, SPOP

**Sorted Set (9):** ZADD, ZREM, ZRANGE, ZREVRANGE, ZRANK, ZSCORE, ZCARD, ZPOPMIN, ZINCRBY, ZRANGEBYSCORE

**Expiry (7):** EXPIRE, PEXPIRE, EXPIRETIME, PEXPIRETIME, TTL, PTTL, PERSIST

**Server (5):** PING, INFO, DBSIZE, FLUSHDB, TYPE

**Cluster (4):** CLUSTER INFO, CLUSTER NODES, CLUSTER KEYSLOT, CLUSTER SLOTS

**Stream (1):** XADD

## Distributed Protocol

Binary frame header (30 bytes):
```
magic(4) | version(1) | type(1) | msg_id(8) | sender_id(8) | payload_len(4) | checksum(4)
```

Frame types: GOSSIP_PING/PONG, REPLICATE_PUT/ACK/DEL, READ_REQUEST/RESPONSE, ANTIENTROPY_HASH/SYNC, CLUSTER_JOIN/ACK, PROXY_REQUEST/RESPONSE, FULL_SYNC_REQUEST/CHUNK


