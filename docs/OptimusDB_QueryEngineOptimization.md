# OptimusDB Query Engine and Optimization

## Overview

The Query Engine is a critical component of OptimusDB that enables distributed query execution across peer networks with optimization strategies, caching, and intelligent peer selection.

## Directory Structure

```
queryengine/
├── engine.go       # Main query engine with optimization
├── worker_pool.go  # Worker pool for parallel queries
└── cache.go        # Query result caching
```

## Architecture

### Component Overview

```
┌──────────────────────────────────────────────┐
│           Query Request                       │
└──────────────────────────────────────────────┘
↓
┌──────────────────────────────────────────────┐
│        OptimizedEngine                        │
│  • Strategy Selection                         │
│  • Cache Lookup                              │
│  • Peer Selection                            │
└──────────────────────────────────────────────┘
↓                ↓                ↓
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ Local Query │  │ WorkerPool  │  │    Cache    │
└─────────────┘  └─────────────┘  └─────────────┘
↓
┌─────────────────┐
│ Peer 1 Query    │
│ Peer 2 Query    │
│ Peer 3 Query    │
│    ...          │
└─────────────────┘
↓
┌─────────────────┐
│ Result Merger   │
└─────────────────┘
↓
┌─────────────────┐
│ Filtered Results│
└─────────────────┘
```

## 1. Optimized Engine (engine.go)

### Structure

```go
type OptimizedEngine struct {
workerPool *WorkerPoolEngine
cache      *SimpleCache
}
```

### Initialization

```go
func NewOptimizedEngine(
maxWorkers int,           // Number of concurrent workers
timeout time.Duration,    // Query timeout
cacheTTL time.Duration,   // Cache entry lifetime
) *OptimizedEngine
```

**Example**:
```go
engine := NewOptimizedEngine(
10,                    // 10 concurrent workers
30 * time.Second,      // 30s timeout
5 * time.Minute,       // 5min cache TTL
)
```

### Query Execution

```go
func (oe *OptimizedEngine) Query(
ctx context.Context,
hostNode host.Host,
selfID peer.ID,
criteria []map[string]interface{},
) ([]map[string]interface{}, error)
```

**Execution Flow**:

1. **Cache Lookup**
```go
if cached, found := oe.cache.Get(criteria); found {
return cached, nil
}
```

2. **Peer Selection**
```go
allPeers := hostNode.Peerstore().Peers()
var peers []peer.ID
for _, p := range allPeers {
if p != selfID {
peers = append(peers, p)
}
}
```

3. **Parallel Query Execution**
```go
results, err := oe.workerPool.QueryWithWorkerPool(
ctx,
hostNode,
selfID.String(),
criteria,
peers,
)
```

4. **Result Caching**
```go
if len(results) > 0 {
oe.cache.Set(criteria, results)
}
```

### Performance Metrics

The engine tracks:
- Query execution time
- Cache hit rate
- Number of peers queried
- Result count per peer
- Error rate

## 2. Worker Pool (worker_pool.go)

### Purpose

Manages parallel execution of queries across multiple peers with concurrency control and timeout handling.

### Structure

```go
type WorkerPoolEngine struct {
maxWorkers int
timeout    time.Duration
}

type QueryJob struct {
peerID   peer.ID
criteria []map[string]interface{}
}

type QueryResult struct {
peerID  peer.ID
results []map[string]interface{}
err     error
}
```

### Worker Pool Pattern

```go
func (wp *WorkerPoolEngine) QueryWithWorkerPool(
ctx context.Context,
hostNode host.Host,
selfID string,
criteria []map[string]interface{},
peers []peer.ID,
) ([]map[string]interface{}, error)
```

**Implementation**:

1. **Create Channels**
```go
jobs := make(chan QueryJob, len(peers))
results := make(chan QueryResult, len(peers))
```

2. **Start Workers**
```go
workerCount := min(wp.maxWorkers, len(peers))
var wg sync.WaitGroup

for i := 0; i < workerCount; i++ {
wg.Add(1)
go wp.worker(ctx, hostNode, jobs, results, &wg)
}
```

3. **Distribute Jobs**
```go
for _, peerID := range peers {
jobs <- QueryJob{
peerID:   peerID,
criteria: criteria,
}
}
close(jobs)
```

4. **Collect Results**
```go
go func() {
wg.Wait()
close(results)
}()

allResults := []map[string]interface{}{}
for result := range results {
if result.err == nil {
allResults = append(allResults, result.results...)
}
}
```

### Worker Function

```go
func (wp *WorkerPoolEngine) worker(
ctx context.Context,
hostNode host.Host,
jobs <-chan QueryJob,
results chan<- QueryResult,
wg *sync.WaitGroup,
) {
defer wg.Done()

for job := range jobs {
// Create timeout context
queryCtx, cancel := context.WithTimeout(ctx, wp.timeout)

// Execute query on peer
peerResults, err := wp.queryPeer(queryCtx, hostNode, job)

// Send result
results <- QueryResult{
peerID:  job.peerID,
results: peerResults,
err:     err,
}

cancel()
}
}
```

### Peer Query Protocol

```go
func (wp *WorkerPoolEngine) queryPeer(
ctx context.Context,
hostNode host.Host,
job QueryJob,
) ([]map[string]interface{}, error) {
// Open stream to peer
stream, err := hostNode.NewStream(ctx, job.peerID, QueryProtocol)
if err != nil {
return nil, err
}
defer stream.Close()

// Send query request
request := QueryRequest{
Criteria: job.criteria,
RequestID: generateRequestID(),
}

if err := json.NewEncoder(stream).Encode(request); err != nil {
return nil, err
}

// Read response
var response QueryResponse
if err := json.NewDecoder(stream).Decode(&response); err != nil {
return nil, err
}

return response.Results, nil
}
```

### Timeout Handling

Each worker query has individual timeout:
```go
queryCtx, cancel := context.WithTimeout(ctx, wp.timeout)
defer cancel()
```

Graceful handling of timeouts:
```go
select {
case <-queryCtx.Done():
log.Printf("[WORKER] Query to %s timed out", job.peerID)
results <- QueryResult{
peerID: job.peerID,
err:    context.DeadlineExceeded,
}
case result := <-resultChan:
results <- result
}
```

### Concurrency Control

**Maximum Workers**:
Limits concurrent queries to prevent resource exhaustion:
```go
workerCount := min(wp.maxWorkers, len(peers))
```

**Buffered Channels**:
Prevents goroutine blocking:
```go
jobs := make(chan QueryJob, len(peers))       // Buffer for all jobs
results := make(chan QueryResult, len(peers)) // Buffer for all results
```

**WaitGroup**:
Ensures all workers complete:
```go
var wg sync.WaitGroup
wg.Add(workerCount)
// ... start workers
wg.Wait()
```

## 3. Query Cache (cache.go)

### Purpose

Caches query results to reduce latency and network traffic for frequently repeated queries.

### Structure

```go
type SimpleCache struct {
store map[string]*CacheEntry
ttl   time.Duration
mu    sync.RWMutex
}

type CacheEntry struct {
results   []map[string]interface{}
timestamp time.Time
}
```

### Operations

#### Get from Cache

```go
func (c *SimpleCache) Get(
criteria []map[string]interface{},
) ([]map[string]interface{}, bool) {
c.mu.RLock()
defer c.mu.RUnlock()

key := c.generateKey(criteria)
entry, exists := c.store[key]

if !exists {
return nil, false
}

// Check if expired
if time.Since(entry.timestamp) > c.ttl {
return nil, false
}

return entry.results, true
}
```

#### Set in Cache

```go
func (c *SimpleCache) Set(
criteria []map[string]interface{},
results []map[string]interface{},
) {
c.mu.Lock()
defer c.mu.Unlock()

key := c.generateKey(criteria)
c.store[key] = &CacheEntry{
results:   results,
timestamp: time.Now(),
}
}
```

#### Cache Key Generation

```go
func (c *SimpleCache) generateKey(
criteria []map[string]interface{},
) string {
// Convert criteria to JSON
jsonBytes, _ := json.Marshal(criteria)

// Hash for compact key
hash := sha256.Sum256(jsonBytes)
return hex.EncodeToString(hash[:])
}
```

#### Cache Cleanup

Periodic cleanup of expired entries:
```go
func (c *SimpleCache) cleanup() {
c.mu.Lock()
defer c.mu.Unlock()

now := time.Now()
for key, entry := range c.store {
if now.Sub(entry.timestamp) > c.ttl {
delete(c.store, key)
}
}
}

// Run cleanup periodically
go func() {
ticker := time.NewTicker(1 * time.Minute)
for range ticker.C {
c.cleanup()
}
}()
```

### Cache Statistics

```go
type CacheStats struct {
Hits      int
Misses    int
Size      int
HitRate   float64
}

func (c *SimpleCache) GetStats() CacheStats {
c.mu.RLock()
defer c.mu.RUnlock()

totalRequests := c.hits + c.misses
hitRate := 0.0
if totalRequests > 0 {
hitRate = float64(c.hits) / float64(totalRequests)
}

return CacheStats{
Hits:    c.hits,
Misses:  c.misses,
Size:    len(c.store),
HitRate: hitRate,
}
}
```

## Query Strategies

### Strategy Selection

OptimusDB supports multiple query execution strategies:

```go
type QueryStrategy string

const (
LOCAL_ONLY             QueryStrategy = "LOCAL_ONLY"
REMOTE_ONLY            QueryStrategy = "REMOTE_ONLY"
LOCAL_THEN_REMOTE      QueryStrategy = "LOCAL_THEN_REMOTE_MERGE"
PARALLEL_MERGE         QueryStrategy = "PARALLEL_MERGE"
QUORUM                 QueryStrategy = "QUORUM"
)
```

### Strategy Implementation

#### 1. LOCAL_ONLY

Query only local data stores.

**Use Cases**:
- Known local data
- Offline operation
- Fastest response time

**Implementation**:
```go
if strategy == LOCAL_ONLY {
return queryLocalStore(criteria)
}
```

**Performance**: O(n) where n is local data size

#### 2. REMOTE_ONLY

Query only remote peers, excluding local data.

**Use Cases**:
- Fresh data required
- Verify distributed state
- Load distribution

**Implementation**:
```go
if strategy == REMOTE_ONLY {
return workerPool.QueryWithWorkerPool(
ctx, hostNode, selfID, criteria, allPeers,
)
}
```

**Performance**: O(p × m) where p is peer count, m is network latency

#### 3. LOCAL_THEN_REMOTE_MERGE

Try local first, query remote if insufficient results.

**Use Cases**:
- Optimize for common cases
- Minimize network usage
- Progressive enhancement

**Implementation**:
```go
localResults := queryLocalStore(criteria)

if len(localResults) >= minRows {
return localResults, nil
}

remoteResults := workerPool.QueryWithWorkerPool(
ctx, hostNode, selfID, criteria, allPeers,
)

return mergeResults(localResults, remoteResults), nil
```

**Performance**: Best case O(n), worst case O(n + p × m)

#### 4. PARALLEL_MERGE

Query local and remote simultaneously, merge all results.

**Use Cases**:
- Maximize completeness
- Balanced latency
- High availability

**Implementation**:
```go
var wg sync.WaitGroup
var localResults, remoteResults []map[string]interface{}

// Query local
wg.Add(1)
go func() {
defer wg.Done()
localResults = queryLocalStore(criteria)
}()

// Query remote
wg.Add(1)
go func() {
defer wg.Done()
remoteResults = workerPool.QueryWithWorkerPool(
ctx, hostNode, selfID, criteria, allPeers,
)
}()

wg.Wait()
return mergeResults(localResults, remoteResults), nil
```

**Performance**: O(max(n, p × m))

#### 5. QUORUM

Require responses from N peers for consistency.

**Use Cases**:
- Strong consistency
- Data verification
- Consensus queries

**Implementation**:
```go
requiredResponses := quorumN
responses := [][]map[string]interface{}{}

results := workerPool.QueryWithWorkerPool(
ctx, hostNode, selfID, criteria, allPeers,
)

if len(results) < requiredResponses {
return nil, ErrQuorumNotReached
}

// Verify consistency across responses
return verifyAndMerge(results, requiredResponses), nil
```

**Performance**: O(q × m) where q is quorum size

## Peer Selection Strategies

### Reputation-Based Selection

Select peers based on reputation scores:

```go
func selectTopPeers(peers []peer.ID, maxPeers int) []peer.ID {
// Get reputation scores
scores := make(map[peer.ID]float64)
for _, p := range peers {
scores[p] = getReputationScore(p)
}

// Sort by reputation
sort.Slice(peers, func(i, j int) bool {
return scores[peers[i]] > scores[peers[j]]
})

// Return top N
if len(peers) > maxPeers {
return peers[:maxPeers]
}
return peers
}
```

### Latency-Based Selection

Prefer peers with lower latency:

```go
func selectLowLatencyPeers(peers []peer.ID, maxPeers int) []peer.ID {
// Measure latency to each peer
latencies := make(map[peer.ID]time.Duration)
for _, p := range peers {
latencies[p] = measureLatency(p)
}

// Sort by latency
sort.Slice(peers, func(i, j int) bool {
return latencies[peers[i]] < latencies[peers[j]]
})

return peers[:min(maxPeers, len(peers))]
}
```

### Load-Based Selection

Avoid overloaded peers:

```go
func selectLightlyLoadedPeers(peers []peer.ID, maxPeers int) []peer.ID {
// Check peer load
loads := make(map[peer.ID]float64)
for _, p := range peers {
loads[p] = getPeerLoad(p)
}

// Filter and sort by load
available := []peer.ID{}
for _, p := range peers {
if loads[p] < loadThreshold {
available = append(available, p)
}
}

sort.Slice(available, func(i, j int) bool {
return loads[available[i]] < loads[available[j]]
})

return available[:min(maxPeers, len(available))]
}
```

## Result Merging and Deduplication

### Merge Algorithm

```go
func mergeResults(
resultSets ...[]map[string]interface{},
) []map[string]interface{} {
seen := make(map[string]bool)
merged := []map[string]interface{}{}

for _, results := range resultSets {
for _, result := range results {
// Generate unique key
key := generateResultKey(result)

if !seen[key] {
merged = append(merged, result)
seen[key] = true
}
}
}

return merged
}
```

### Deduplication

```go
func generateResultKey(result map[string]interface{}) string {
// Use _id if available
if id, ok := result["_id"]; ok {
return fmt.Sprintf("%v", id)
}

// Otherwise hash the entire document
jsonBytes, _ := json.Marshal(result)
hash := sha256.Sum256(jsonBytes)
return hex.EncodeToString(hash[:])
}
```

### Conflict Resolution

When multiple peers return different versions:

```go
func resolveConflict(versions []map[string]interface{}) map[string]interface{} {
// Use last-write-wins based on timestamp
var latest map[string]interface{}
var latestTime time.Time

for _, v := range versions {
if ts, ok := v["_timestamp"].(time.Time); ok {
if ts.After(latestTime) {
latest = v
latestTime = ts
}
}
}

return latest
}
```

## Performance Optimization

### 1. Early Termination

Stop querying when sufficient results found:

```go
if len(results) >= minRows {
cancel()  // Cancel remaining queries
return results, nil
}
```

### 2. Batch Processing

Process results in batches:

```go
batchSize := 100
for i := 0; i < len(results); i += batchSize {
batch := results[i:min(i+batchSize, len(results))]
processBatch(batch)
}
```

### 3. Streaming Results

Stream results as they arrive:

```go
resultChan := make(chan map[string]interface{}, 100)

go func() {
for result := range resultChan {
sendToClient(result)
}
}()

// Query peers
for _, peer := range peers {
go func(p peer.ID) {
results := queryPeer(p, criteria)
for _, r := range results {
resultChan <- r
}
}(peer)
}
```

### 4. Connection Pooling

Reuse connections to peers:

```go
type ConnectionPool struct {
connections map[peer.ID]network.Stream
mu          sync.Mutex
}

func (cp *ConnectionPool) GetConnection(p peer.ID) network.Stream {
cp.mu.Lock()
defer cp.mu.Unlock()

if conn, exists := cp.connections[p]; exists {
return conn
}

conn := establishConnection(p)
cp.connections[p] = conn
return conn
}
```

## Monitoring and Metrics

### Query Metrics

```go
type QueryMetrics struct {
TotalQueries      int
CacheHits         int
CacheMisses       int
AvgLatency        time.Duration
PeerResponseTimes map[peer.ID]time.Duration
ErrorRate         float64
}

func (qm *QueryMetrics) RecordQuery(
startTime time.Time,
cacheHit bool,
err error,
) {
duration := time.Since(startTime)

qm.TotalQueries++

if cacheHit {
qm.CacheHits++
} else {
qm.CacheMisses++
}

// Update average latency
qm.AvgLatency = (qm.AvgLatency*time.Duration(qm.TotalQueries-1) + duration) /
time.Duration(qm.TotalQueries)

if err != nil {
qm.ErrorRate = float64(qm.Errors) / float64(qm.TotalQueries)
}
}
```

### Performance Profiling

```go
import "runtime/pprof"

func profileQuery(query func()) {
f, _ := os.Create("query.prof")
defer f.Close()

pprof.StartCPUProfile(f)
defer pprof.StopCPUProfile()

query()
}
```

## Error Handling

### Error Types

```go
var (
ErrQueryTimeout      = errors.New("query timeout")
ErrPeerUnavailable   = errors.New("peer unavailable")
ErrQuorumNotReached  = errors.New("quorum not reached")
ErrInvalidCriteria   = errors.New("invalid query criteria")
ErrCacheCorrupted    = errors.New("cache corrupted")
)
```

### Error Recovery

```go
func (oe *OptimizedEngine) QueryWithRetry(
ctx context.Context,
criteria []map[string]interface{},
maxRetries int,
) ([]map[string]interface{}, error) {
var lastErr error

for i := 0; i < maxRetries; i++ {
results, err := oe.Query(ctx, hostNode, selfID, criteria)

if err == nil {
return results, nil
}

lastErr = err

// Exponential backoff
backoff := time.Duration(1<<uint(i)) * time.Second
time.Sleep(backoff)
}

return nil, fmt.Errorf("query failed after %d retries: %w",
maxRetries, lastErr)
}
```

## Testing

### Unit Tests

```go
func TestWorkerPool(t *testing.T) {
engine := NewWorkerPoolEngine(5, 10*time.Second)

// Test with mock peers
results, err := engine.QueryWithWorkerPool(
context.Background(),
mockHost,
"self",
testCriteria,
mockPeers,
)

assert.NoError(t, err)
assert.NotEmpty(t, results)
}

func TestCache(t *testing.T) {
cache := NewSimpleCache(1 * time.Minute)

// Test set/get
cache.Set(criteria, results)
cached, found := cache.Get(criteria)

assert.True(t, found)
assert.Equal(t, results, cached)

// Test expiration
time.Sleep(2 * time.Minute)
_, found = cache.Get(criteria)
assert.False(t, found)
}
```

### Integration Tests

```bash
cd repoScript
./testQueryOptimization.ps1
```

## Best Practices

1. **Choose Appropriate Strategy**
- Use LOCAL_ONLY for known local data
- Use PARALLEL_MERGE for maximum completeness
- Use QUORUM for strong consistency

2. **Configure Worker Pool**
- Set maxWorkers based on available resources
- Adjust timeout based on network conditions
- Monitor worker utilization

3. **Optimize Cache Usage**
- Set appropriate TTL for your data freshness needs
- Monitor cache hit rate
- Clear cache on significant updates

4. **Monitor Performance**
- Track query latencies
- Monitor peer response times
- Watch for timeouts and errors

## Related Documentation

- See `01_README_CORE_APP.md` for application integration
- See `02_README_API.md` for query API
- See `05_README_ELECTION.md` for reputation-based peer selection
- See `/docs/OptimusDB_QueriesStrategy.md` for query strategies
- See `/docs/OptimusDB_QueriesMechanisms.md` for query mechanisms