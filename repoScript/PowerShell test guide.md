# PowerShell Test Script Usage Guide

## Quick Start

### Basic Usage (8 agents starting at port 18001)

```powershell
# Run the test script
.\Test-OptimusDBOptimizations.ps1
```

### Custom Configuration

```powershell
# Test with different base port
.\Test-OptimusDBOptimizations.ps1 -BasePort 18001

# Test with different number of agents
.\Test-OptimusDBOptimizations.ps1 -Agents 8

# Both parameters
.\Test-OptimusDBOptimizations.ps1 -BasePort 18001 -Agents 8
```

---

## Prerequisites

1. **8 OptimusDB agents running**
- Use your cluster script: `.\repoScript\run_optimusdb_cluster.ps1`
- Wait 30 seconds for agents to start

2. **PowerShell 5.1 or higher**
- Check version: `$PSVersionTable.PSVersion`

3. **Network access to localhost ports**
- Default: 18001-18008

---

## Test Suite

The script runs 8 comprehensive tests:

### Test 1: Insert Test Data ✓
- Inserts data to Agents 1, 3, 5, 7
- Verifies data insertion works
- Waits for data replication

### Test 2: Query Cache Miss ✓
- Queries from Agent 8 (doesn't have local data)
- Tests peer aggregation
- Measures latency (expect 50-100ms)

### Test 3: Query Cache Hit ✓
- Same query as Test 2
- Tests cache performance
- Measures latency (expect 2-10ms)
- Verifies cache is 10-100x faster

### Test 4: Cache Statistics ✓
- Queries cache stats endpoint
- Verifies monitoring is working
- Shows hit rate and entries

### Test 5: Complex Query ✓
- Tests multiple criteria filtering
- Verifies correct results returned
- Tests query accuracy

### Test 6: Performance Test ✓
- Runs 10 queries in succession
- Calculates average latency
- Verifies performance target (<100ms avg)

### Test 7: Deduplication ✓
- Inserts duplicate data to multiple agents
- Queries and verifies only 1 result returned
- Tests deduplication logic

### Test 8: Multi-Agent Queries ✓
- Queries from 4 agents simultaneously
- Tests concurrent query handling
- Verifies system stability

---

## Expected Output

```
=========================================
OptimusDB Optimized Query Test Suite
=========================================

Checking if all 8 agents are running...
Agent 1 (port 18001): ✓ Running
Agent 2 (port 18002): ✓ Running
Agent 3 (port 18003): ✓ Running
Agent 4 (port 18004): ✓ Running
Agent 5 (port 18005): ✓ Running
Agent 6 (port 18006): ✓ Running
Agent 7 (port 18007): ✓ Running
Agent 8 (port 18008): ✓ Running
[PASS] All 8 agents are running

Test 1: Inserting test data to agents...
Agent 1: Inserted Solar Panel Alpha
Agent 3: Inserted Wind Turbine Beta
Agent 5: Inserted Hydro Plant Gamma
Agent 7: Inserted Geothermal Delta
[PASS] Test data inserted

Waiting 3 seconds for data replication...

Test 2: Query from Agent 8 (should aggregate from other agents)...
[PASS] Query returned 4 results in 58ms
Query latency (cache miss): 58ms

Test 3: Same query again from Agent 8 (should hit cache)...
[PASS] Query returned 4 results in 3ms
Query latency (cache hit): 3ms
[PASS] Cache optimization working (< 100ms)

Test 4: Checking cache statistics...
[PASS] Cache stats available
Stats: {"entries":1,"ttl":"10m0s","hits":1,"misses":1,"hit_rate":"50.00%"}

Test 5: Complex query (type + power filter)...
[PASS] Complex query returned correct results in 45ms

Test 6: Performance test (10 queries)...
10 queries completed
Average latency: 8.5ms
Total time: 85ms
[PASS] Performance is good (avg < 100ms)

Test 7: Testing deduplication...
[PASS] Deduplication working (only 1 result returned)

Test 8: Querying from multiple agents simultaneously...
[PASS] All 4 agents returned results successfully

=========================================
Test Summary
=========================================
Tests Passed: 8
Tests Failed: 0

All tests passed! ✓

Optimizations are working correctly:
• Parallel query execution is active
• Cache is functioning properly
• Deduplication is working
• Performance improvements verified
```

---

## Interpreting Results

### ✅ Success Indicators

**Cache Miss (First Query)**
- Latency: 50-100ms is good
- Shows parallel execution working

**Cache Hit (Repeat Query)**
- Latency: 2-10ms is excellent
- Should be 10-100x faster than cache miss
- Proves caching is working

**Average Performance**
- < 50ms = Excellent
- 50-100ms = Good
- > 100ms = Needs investigation

### ⚠️ Warning Signs

**Slow Cache Miss (>200ms)**
- Peers may be slow to respond
- Network latency issues
- Too many peer failures

**Slow Cache Hit (>50ms)**
- Cache not working properly
- Check if QueryEngine initialized
- Verify cache implementation

**High Failure Rate**
- Check agent connectivity
- Verify all agents running
- Check logs for errors

---

## Troubleshooting

### All Agents Not Running

```powershell
# Check which agents are running
1..8 | ForEach-Object {
$port = 18000 + $_
try {
Invoke-WebRequest "http://localhost:$port" -TimeoutSec 2
Write-Host "Agent $_ (port $port): Running" -ForegroundColor Green
}
catch {
Write-Host "Agent $_ (port $port): Not running" -ForegroundColor Red
}
}

# Start agents
.\repoScript\run_optimusdb_cluster.ps1
Start-Sleep -Seconds 30
```

### Tests Failing

```powershell
# Check agent logs
docker logs optimusdb1
docker logs optimusdb2
# ... etc

# Look for:
# - [ENGINE] Initializing optimized query engine
# - [CACHE] Hit! or [CACHE] Stored
# - [INFO] Query completed
```

### Cache Not Working

```powershell
# Verify optimized function is being used
docker logs optimusdb1 | Select-String "optimized query engine"

# Should see:
# [INFO] Initializing optimized query engine...

# If not, check that you're using queryPeersOptimized
```

### Slow Performance

```powershell
# Check CPU usage
docker stats --no-stream

# Check network connectivity
docker exec optimusdb1 ipfs swarm peers

# Check if many peers are failing
docker logs optimusdb1 | Select-String "Query to peer.*failed"
```

---

## Advanced Usage

### Run Specific Tests Only

Modify the script to comment out tests you don't want:

```powershell
# In Main function, comment out tests:
# Test-InsertData
$firstQueryTime = Test-QueryCacheMiss
Test-QueryCacheHit -FirstQueryTime $firstQueryTime
# Test-CacheStats
# Test-ComplexQuery
# Test-Performance
# Test-Deduplication
# Test-MultipleAgentQueries
```

### Custom Test Data

Edit the `Test-InsertData` function to insert different data:

```powershell
$json1 = @{
method = @{
cmd = "crudput"
argcnt = 1
}
args = @("")
dstype = "SWres"
UpdateData = @(
@{
id = "custom-1"
name = "My Custom Data"
status = "active"
# ... your fields ...
}
)
} | ConvertTo-Json -Depth 10
```

### Continuous Monitoring

Run tests in a loop for continuous monitoring:

```powershell
while ($true) {
.\Test-OptimusDBOptimizations.ps1
Write-Host "`nWaiting 60 seconds before next test..."
Start-Sleep -Seconds 60
}
```

---

## Performance Benchmarks

### Expected Timings (8 agents)

| Test | Expected | Excellent | Poor |
|------|----------|-----------|------|
| Cache Miss | 50-100ms | <50ms | >200ms |
| Cache Hit | 2-10ms | <5ms | >50ms |
| Complex Query | 40-80ms | <40ms | >150ms |
| Average (10 queries) | 20-60ms | <20ms | >100ms |

### Comparison with Old Implementation

| Metric | Old | Optimized | Improvement |
|--------|-----|-----------|-------------|
| First Query | 400-500ms | 50-100ms | **5-8x faster** |
| Cached Query | 400-500ms | 2-10ms | **50-200x faster** |
| 10 Queries | 4-5 seconds | 0.2-0.6s | **7-20x faster** |

---

## Cleaning Up Test Data

After running tests, you may want to clean up test data:

```powershell
# Delete test records (if DELETE is implemented)
$deleteJson = @{
method = @{
cmd = "delete"
argcnt = 0
}
criteria = @(
@{
id = @{
'$regex' = "^opt-test-|^dedup-test-"
}
}
)
} | ConvertTo-Json -Depth 10

1..8 | ForEach-Object {
$port = 18000 + $_
Invoke-RestMethod -Uri "http://localhost:$port/optimusdb/command" `
-Method Post `
-ContentType "application/json" `
-Body $deleteJson
}
```

---

## Integration with CI/CD

### Azure DevOps

```yaml
steps:
- powershell: |
.\Test-OptimusDBOptimizations.ps1
displayName: 'Test OptimusDB Optimizations'
continueOnError: false
```

### GitHub Actions

```yaml
- name: Test OptimusDB Optimizations
run: |
pwsh Test-OptimusDBOptimizations.ps1
shell: pwsh
```

---

## Tips for Best Results

1. **Wait for Startup**
- Let agents fully start (30 seconds)
- Verify all agents responding

2. **Run Twice**
- First run: System warm-up
- Second run: True performance

3. **Check Logs**
- Look for optimization messages
- Verify cache is being used

4. **Monitor Resources**
- Watch CPU usage during tests
- Check memory consumption

5. **Network Quality**
- Ensure low latency between agents
- Check for network congestion

---

## Exit Codes

- `0` - All tests passed
- `1` - One or more tests failed
- Error - Agent connectivity issues

---

## Support

If tests fail, check:

1. **IMPLEMENTATION_GUIDE.md** - Verify changes were made correctly
2. **Agent logs** - Look for errors or warnings
3. **Docker status** - Ensure all containers running
4. **Network** - Verify localhost connectivity

For detailed troubleshooting, see the main documentation files.

---

**Happy Testing! 🚀**