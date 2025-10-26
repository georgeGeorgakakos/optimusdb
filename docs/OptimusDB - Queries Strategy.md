# OptimusDB Query Strategies and Decentralized Query Mechanism

**Version:** 1.0
**Date:** October 26, 2025

---

## Executive Summary

OptimusDB implements an advanced decentralized query mechanism enabling efficient data retrieval across a peer-to-peer network without centralized coordination. This documentation covers query strategies, architecture, and implementation details of the decentralized database system.

---

## 1. Introduction

### 1.1 Background

OptimusDB represents a paradigm shift from centralized/distributed databases to fully decentralized architecture using libp2p and IPFS technologies.

### 1.2 Core Principles (DCS Triad)

- **Decentralization (D)**: No single point of failure
- **Consistency (C)**: Tunable consistency levels
- **Scalability (S)**: Horizontal scaling through peer addition

---

## 2. Architecture Overview

### System Components

```
HTTP API (8089) ← → Query Engine ← → Storage (OrbitDB/IPFS)
↓
libp2p Transport Layer (4001)
↓
P2P Network Mesh (Peers)
```

### Data Flow
1. Query Initiation → 2. Local Processing → 3. Peer Propagation →
4. Result Aggregation → 5. Unified Response

---

## 3. Query Strategies

### 3.1 LOCAL_ONLY
- **Latency**: 5-10ms
- **Use Case**: Low latency, local data sufficient
- **Network**: No overhead

### 3.2 REMOTE_ONLY
- **Latency**: 100-500ms
- **Use Case**: Distributed computation, load distribution
- **Network**: Moderate-High overhead

### 3.3 LOCAL_THEN_REMOTE_MERGE (Default)
- **Latency**: 10-500ms variable
- **Use Case**: Balanced performance and completeness
- **Network**: Adaptive overhead

### 3.4 PARALLEL_MERGE
- **Latency**: 50-200ms optimized
- **Use Case**: Real-time analytics, maximum completeness
- **Network**: High overhead

### 3.5 QUORUM
- **Latency**: Variable (depends on quorum_n)
- **Use Case**: Consistency-critical operations
- **Network**: Bounded by quorum requirement

---

## 4. Decentralized Query Mechanism

### 8-Step Process

1. **Peer Aggregation**: Combine connected + discovered peers
2. **Fan-out Limit**: Apply MaxPeers constraint
3. **Concurrent Propagation**: Launch goroutine per peer
4. **Loop Prevention**: Check tracePath for cycles
5. **Connection Attempt**: Dial discovered but unconnected peers
6. **Provenance Tagging**: Add _source and _trace metadata
7. **Merge Results**: Aggregate peer responses
8. **Timeout Handling**: Respect time budget

### Query Protocol

**libp2p Stream**: `/query/1.0.0`

**Message Format**:
```json
{
"criteria": [{"field": "value"}],
"trace_id": "uuid",
"trace_path": ["peer-1", "peer-2"],
"options": {
"strategy": "PARALLEL_MERGE",
"time_budget_ms": 2000,
"max_peers": 10
}
}
```

---

## 5. Query Engine Components

### Optimized Engine
```go
type OptimizedEngine struct {
workerPool *WorkerPoolEngine  // Bounded concurrency
cache      *SimpleCache        // TTL-based caching
}
```

### Performance Benchmarks (8-node network)

| Strategy | P50 Latency | P95 Latency | Throughput |
|----------|-------------|-------------|------------|
| LOCAL_ONLY | 8ms | 15ms | 5,000 qps |
| LOCAL_THEN_REMOTE | 120ms | 350ms | 500 qps |
| PARALLEL_MERGE | 95ms | 280ms | 650 qps |
| REMOTE_ONLY | 180ms | 450ms | 300 qps |
| QUORUM (n=3) | 210ms | 500ms | 250 qps |

---

## 6. Implementation Highlights

### Deduplication

```go
func dedupeByID(rows []map[string]interface{}) []map[string]interface{} {
seen := make(map[string]bool)
out := make([]map[string]interface{}, 0, len(rows))
for _, r := range rows {
id, _ := r["_id"].(string)
if id != "" && !seen[id] {
seen[id] = true
out = append(out, r)
} else if id == "" {
out = append(out, r)
}
}
return out
}
```

### Query Options

```go
type QueryOptions struct {
Strategy       QueryStrategy    // Query execution strategy
TimeBudgetMs   int              // Maximum duration
QuorumN        int              // Required responses
MaxPeers       int              // Fan-out limit
IncludeLocal   bool             // Include local results
AnnotateSource bool             // Add provenance tags
}
```

---

## 7. Performance Optimization

### Techniques
- **LRU Caching**: TTL-based result caching
- **Worker Pool**: Bounded concurrency (default: 10)
- **Connection Reuse**: libp2p stream persistence

### Scalability
- Query Complexity: O(log N) with smart routing
- Network Overhead: O(N) worst case (full broadcast)
- Storage per Node: O(1) (no full replication)

---

## 8. Technology Stack

### Core Technologies
- **libp2p**: P2P networking (https://libp2p.io)
- **IPFS**: Content addressing (https://ipfs.io)
- **OrbitDB**: Distributed database (https://orbitdb.org)

### Key Libraries
- `github.com/libp2p/go-libp2p`
- `berty.tech/go-orbit-db`
- `github.com/ipfs/interface-go-ipfs-core`

---

## 9. Future Research Directions

- Machine learning-based query planning
- Predictive peer selection
- Causal+ consistency implementation
- Privacy-preserving query protocols
- Zero-knowledge proof validation

---

## Conclusion

OptimusDB's decentralized query mechanism achieves a novel balance between performance, consistency, and decentralization through multiple query strategies, intelligent caching, and robust P2P communication.

---

**© 2025 OptimusDB Research Team**
**License**: MPL 2.0
**Repository**: https://github.com/georgegeorgakakos/optimusdb
