# OptimusDB Query Strategies & Decentralized Mechanism Documentation Package

## Package Contents

This documentation package provides comprehensive information about OptimusDB's decentralized query mechanism, query strategies, and implementation guides.

### 📄 Documents Included

1. **OptimusDB_Query_Strategies_Documentation.md** (5.4 KB)
- Complete technical documentation of query strategies
- Architecture overview and data flow diagrams
- Implementation details and code examples
- Performance benchmarks and optimization techniques

2. **OptimusDB_Query_Strategies_Presentation.pptx** (231 KB)
- Executive presentation for stakeholders
- Visual overview of architecture and strategies
- Key performance metrics and use cases

3. **SQLite_Metadata_OptimusDB_Guide.md** (19 KB)
- Comprehensive guide for SQLite metadata integration
- Complete database schema definitions
- Go implementation examples
- Performance monitoring queries
- Deployment and maintenance procedures

---

## Quick Start Guide

### Understanding the Query Strategies

OptimusDB implements 5 query strategies:

1. **LOCAL_ONLY** - Query local node only (fastest, limited scope)
2. **REMOTE_ONLY** - Query peer nodes only (distributed workload)
3. **LOCAL_THEN_REMOTE_MERGE** - Query local first, then peers if needed (balanced, default)
4. **PARALLEL_MERGE** - Query local and peers concurrently (maximum completeness)
5. **QUORUM** - Continue until quorum achieved (consistency-critical)

### Architecture Highlights

```
Client Request
↓
HTTP API (8089) → Query Engine → Strategy Selection
↓                            ↓
Local Query ←------------------→ Peer Queries (via libp2p)
↓                            ↓
Result Merge & Deduplication
↓
Response to Client
```

### Key Metrics (8-node network)

| Strategy | Latency (P50) | Throughput |
|----------|---------------|------------|
| LOCAL_ONLY | 8ms | 5,000 qps |
| PARALLEL_MERGE | 95ms | 650 qps |
| QUORUM (n=3) | 210ms | 250 qps |

---

## Implementing SQLite Metadata

The SQLite metadata layer enhances OptimusDB with:

- **Smart Peer Selection**: Choose optimal peers based on reputation and response time
- **Performance Tracking**: Monitor query execution and cache effectiveness
- **Network Analysis**: Track connection quality and latency
- **Data Distribution**: Know which peers have which data ranges

### Quick Setup

```go
// Initialize metadata store
metadataStore, err := metadata.NewMetadataStore("./optimusdb_metadata.db")
if err != nil {
log.Fatal(err)
}

// Use in query engine
engine := NewOptimizedEngine(10, 2*time.Second, 30*time.Second)
engine.SetMetadataStore(metadataStore)
```

---

## Use Cases by Strategy

### LOCAL_ONLY
- ✅ Development and testing
- ✅ Privacy-sensitive queries
- ✅ Known local data sufficiency
- ❌ Distributed data requirements

### PARALLEL_MERGE
- ✅ Real-time analytics
- ✅ Maximum data completeness needed
- ✅ High-availability requirements
- ❌ Bandwidth-constrained environments

### QUORUM
- ✅ Financial transactions
- ✅ Consistency-critical operations
- ✅ Byzantine fault tolerance scenarios
- ❌ Low-latency requirements

---

## Performance Optimization Tips

1. **Enable Caching**: 40-50% performance improvement with 30-second TTL
2. **Worker Pool Tuning**: Set to CPU cores × 2 for optimal concurrency
3. **Smart Peer Selection**: Use metadata to route queries to best-performing peers
4. **Strategy Selection**: Choose based on consistency vs. performance trade-offs

### Recommended Configuration

```json
{
"query_engine": {
"max_workers": 20,
"query_timeout_ms": 2000,
"cache_ttl_seconds": 30
},
"default_options": {
"strategy": "PARALLEL_MERGE",
"time_budget_ms": 2000,
"max_peers": 50,
"include_local": true
}
}
```

---

## Technology Stack

### Core Components
- **libp2p**: P2P networking layer
- **IPFS**: Content-addressed storage
- **OrbitDB**: Decentralized database
- **SQLite**: Metadata and analytics

### Go Libraries
- `github.com/libp2p/go-libp2p`
- `berty.tech/go-orbit-db`
- `github.com/mattn/go-sqlite3`

---

## Decentralized Query Mechanism: 8-Step Process

1. **Peer Aggregation** - Gather all connected and discovered peers
2. **Fan-out Limit** - Apply MaxPeers constraint to control propagation
3. **Concurrent Propagation** - Launch goroutine for each target peer
4. **Loop Prevention** - Check tracePath to avoid query cycles
5. **Connection Attempt** - Dial discovered but not yet connected peers
6. **Provenance Tagging** - Add _source and _trace metadata to results
7. **Merge Results** - Aggregate and deduplicate all peer responses
8. **Timeout Handling** - Respect time budget to prevent deadlocks

---

## API Example

### Query with Strategy

```bash
curl -X POST http://localhost:8089/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "querykbdata"},
"dstype": "dsswres",
"criteria": [{"status": "active", "region": "eu"}],
"options": {
"strategy": "PARALLEL_MERGE",
"time_budget_ms": 2000,
"max_peers": 10,
"include_local": true,
"annotate_source": true
}
}'
```

### Response with Provenance

```json
{
"results": [
{
"_id": "record1",
"status": "active",
"region": "eu",
"_source": {
"type": "peer",
"peer_id": "QmPeer1...",
"path": ["QmInitiator...", "QmPeer1..."]
},
"_trace": {
"id": "uuid-trace-123",
"path": ["QmInitiator...", "QmPeer1..."]
}
}
]
}
```

---

## Research Foundations

### Theoretical Background
- CAP Theorem (Brewer, 2000)
- PACELC Framework (Abadi, 2012)
- Conflict-Free Replicated Data Types (Shapiro et al., 2011)
- Practical Byzantine Fault Tolerance (Castro & Liskov, 1999)

### Future Research Directions
- Machine learning-based query planning
- Predictive peer selection and caching
- Causal+ consistency implementation
- Privacy-preserving query protocols
- Zero-knowledge proof validation

---

## Monitoring and Analytics

### Key Metrics to Track

1. **Query Performance**
- Execution time by strategy
- Cache hit rates
- Peer response times

2. **Network Health**
- Peer availability
- Connection quality
- Latency distribution

3. **Data Distribution**
- Records per peer
- Replication factor
- Data skew analysis

### Sample Monitoring Query

```sql
SELECT
strategy,
COUNT(*) as queries,
ROUND(AVG(execution_time_ms), 2) as avg_time_ms,
ROUND(AVG(peers_responded), 1) as avg_peers,
ROUND(SUM(CASE WHEN cache_hit THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as cache_hit_rate
FROM query_statistics
WHERE initiated_at >= datetime('now', '-24 hours')
GROUP BY strategy;
```

---

## Troubleshooting Common Issues

### Slow Query Performance
- ✓ Check network connectivity between peers
- ✓ Review cache hit rates (target: >40%)
- ✓ Adjust worker pool size (CPU cores × 2)
- ✓ Consider switching to LOCAL_THEN_REMOTE_MERGE strategy

### Incomplete Results
- ✓ Verify peer connectivity and online status
- ✓ Increase time_budget_ms
- ✓ Check quorum_n settings
- ✓ Review peer discovery logs

### High Network Overhead
- ✓ Reduce max_peers setting
- ✓ Enable and tune caching
- ✓ Use LOCAL_THEN_REMOTE_MERGE instead of PARALLEL_MERGE
- ✓ Implement request batching

---

## Additional Resources

### Documentation
- **Main Repository**: https://github.com/georgegeorgakakos/optimusdb
- **API Reference**: `docs/INTERFACE.md` in repository
- **Architecture**: See `README.md` in repository

### Community
- GitHub Issues for bug reports and feature requests
- Contributions welcome via pull requests

### Support
- Technical questions: Open GitHub issue
- Research collaboration: Contact OptimusDB team

---

## License

All documentation and code examples are licensed under MPL 2.0.

**© 2025 OptimusDB Research ICCS Team**

---
