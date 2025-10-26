# Debug-PeerConnectivity.ps1
# Diagnoses peer-to-peer connectivity issues in OptimusDB

param(
    [int]$BasePort = 18001,
    [int]$Agents = 8
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "OptimusDB Peer Connectivity Diagnostic" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Test 1: Check if agents are running
Write-Host "Test 1: Checking if all agents are accessible..." -ForegroundColor Yellow
$runningAgents = @()

for ($i = 0; $i -lt $Agents; $i++) {
    $port = $BasePort + $i
    $agentNum = $i + 1
    try {
        $response = Invoke-WebRequest "http://localhost:$port" -TimeoutSec 2 -UseBasicParsing -ErrorAction Stop
        Write-Host "  Agent $agentNum (port $port): " -NoNewline
        Write-Host "Accessible ✓" -ForegroundColor Green
        $runningAgents += @{Port = $port; Agent = $agentNum}
    } catch {
        Write-Host "  Agent $agentNum (port $port): " -NoNewline
        Write-Host "Not accessible ✗" -ForegroundColor Red
    }
}
Write-Host ""

if ($runningAgents.Count -eq 0) {
    Write-Host "ERROR: No agents are accessible. Start your cluster first." -ForegroundColor Red
    exit 1
}

# Test 2: Check peer discovery (IPFS swarm)
Write-Host "Test 2: Checking peer discovery (IPFS swarm peers)..." -ForegroundColor Yellow

foreach ($agent in $runningAgents) {
    $agentName = "optimusdb$($agent.Agent)"
    Write-Host "  Checking $agentName..." -ForegroundColor Cyan

    try {
        $peers = docker exec $agentName ipfs swarm peers 2>$null
        if ($peers) {
            $peerCount = ($peers | Measure-Object).Count
            Write-Host "    Connected to $peerCount peers" -ForegroundColor Green

            # Show first 3 peers
            $peers | Select-Object -First 3 | ForEach-Object {
                Write-Host "      - $_" -ForegroundColor Gray
            }
        } else {
            Write-Host "    No peers connected!" -ForegroundColor Red
        }
    } catch {
        Write-Host "    Could not check peers: $_" -ForegroundColor Red
    }
}
Write-Host ""

# Test 3: Insert test data to Agent 1
Write-Host "Test 3: Inserting test data to Agent 1..." -ForegroundColor Yellow

$testData = @{
    method = @{
        cmd = "crudput"
        argcnt = 1
    }
    args = @("")
    dstype = "SWres"
    UpdateData = @(
        @{
            id = "debug-test-1"
            name = "Debug Test Item"
            status = "active"
            timestamp = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
        }
    )
} | ConvertTo-Json -Depth 10

try {
    $insertResult = Invoke-RestMethod -Uri "http://localhost:$BasePort/optimusdb/command" `
        -Method Post `
        -ContentType "application/json" `
        -Body $testData `
        -TimeoutSec 10

    Write-Host "  Data inserted successfully to Agent 1" -ForegroundColor Green
    Write-Host "  Response: $($insertResult | ConvertTo-Json -Compress)" -ForegroundColor Gray
} catch {
    Write-Host "  Failed to insert data: $_" -ForegroundColor Red
}
Write-Host ""

# Wait for potential replication
Write-Host "Waiting 3 seconds for data replication..." -ForegroundColor Yellow
Start-Sleep -Seconds 3
Write-Host ""

# Test 4: Query from Agent 1 (local query - should work)
Write-Host "Test 4: Query from Agent 1 (where data was inserted)..." -ForegroundColor Yellow

$queryData = @{
    method = @{
        cmd = "query"
        argcnt = 0
    }
    criteria = @(
        @{
            id = "debug-test-1"
        }
    )
} | ConvertTo-Json -Depth 10

try {
    $localResult = Invoke-RestMethod -Uri "http://localhost:$BasePort/optimusdb/command" `
        -Method Post `
        -ContentType "application/json" `
        -Body $queryData `
        -TimeoutSec 10

    $resultJson = $localResult | ConvertTo-Json -Depth 10

    if ($resultJson -match "debug-test-1") {
        Write-Host "  ✓ Local query SUCCESS - found data" -ForegroundColor Green
        Write-Host "  Response: $($resultJson.Substring(0, [Math]::Min(200, $resultJson.Length)))..." -ForegroundColor Gray
    } else {
        Write-Host "  ✗ Local query returned no results" -ForegroundColor Red
        Write-Host "  Response: $resultJson" -ForegroundColor Gray
    }
} catch {
    Write-Host "  ✗ Local query failed: $_" -ForegroundColor Red
}
Write-Host ""

# Test 5: Query from Agent 8 (peer query - tests decentralization)
Write-Host "Test 5: Query from Agent 8 (should query peers)..." -ForegroundColor Yellow
Write-Host "  This tests if Agent 8 can query data from Agent 1" -ForegroundColor Gray

$agent8Port = $BasePort + 7

try {
    $peerResult = Invoke-RestMethod -Uri "http://localhost:${agent8Port}/optimusdb/command" `
        -Method Post `
        -ContentType "application/json" `
        -Body $queryData `
        -TimeoutSec 10

    $resultJson = $peerResult | ConvertTo-Json -Depth 10

    if ($resultJson -match "debug-test-1") {
        Write-Host "  ✓ Peer query SUCCESS - decentralization working!" -ForegroundColor Green
        Write-Host "  Response: $($resultJson.Substring(0, [Math]::Min(200, $resultJson.Length)))..." -ForegroundColor Gray
    } else {
        Write-Host "  ✗ Peer query FAILED - returned no results" -ForegroundColor Red
        Write-Host "  This means agents cannot query each other!" -ForegroundColor Red
        Write-Host "  Response: $resultJson" -ForegroundColor Gray
    }
} catch {
    Write-Host "  ✗ Peer query failed with error: $_" -ForegroundColor Red
}
Write-Host ""

# Test 6: Check agent logs for query activity
Write-Host "Test 6: Checking Agent 8 logs for query activity..." -ForegroundColor Yellow

try {
    $logs = docker logs --tail 20 optimusdb8 2>&1

    # Look for query-related messages
    $queryLogs = $logs | Select-String -Pattern "query|Query|QUERY|peer|Peer" -CaseSensitive

    if ($queryLogs) {
        Write-Host "  Recent query-related log entries:" -ForegroundColor Cyan
        $queryLogs | Select-Object -First 10 | ForEach-Object {
            Write-Host "    $_" -ForegroundColor Gray
        }
    } else {
        Write-Host "  No query-related log entries found" -ForegroundColor Yellow
    }
} catch {
    Write-Host "  Could not retrieve logs: $_" -ForegroundColor Red
}
Write-Host ""

# Test 7: Check for optimization messages
Write-Host "Test 7: Checking for optimization engine initialization..." -ForegroundColor Yellow

try {
    $logs = docker logs optimusdb8 2>&1

    if ($logs -match "ENGINE.*Initializing optimized query engine" -or
            $logs -match "optimized query engine") {
        Write-Host "  ✓ Optimized query engine is initialized" -ForegroundColor Green

        # Show relevant logs
        $engineLogs = $logs | Select-String -Pattern "ENGINE|CACHE|optimized"
        if ($engineLogs) {
            Write-Host "  Optimization logs:" -ForegroundColor Cyan
            $engineLogs | Select-Object -First 5 | ForEach-Object {
                Write-Host "    $_" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "  ⚠ Optimized query engine may not be initialized" -ForegroundColor Yellow
        Write-Host "  This is OK if you haven't implemented the optimization yet" -ForegroundColor Gray
    }
} catch {
    Write-Host "  Could not check logs: $_" -ForegroundColor Red
}
Write-Host ""

# Test 8: Check libp2p stream handlers
Write-Host "Test 8: Checking if query stream handler is registered..." -ForegroundColor Yellow

try {
    $logs = docker logs optimusdb8 2>&1

    if ($logs -match "/query/1.0.0" -or $logs -match "stream.*handler") {
        Write-Host "  ✓ Query stream handler appears to be registered" -ForegroundColor Green
    } else {
        Write-Host "  ⚠ Query stream handler may not be registered" -ForegroundColor Yellow
        Write-Host "  Check if SetStreamHandler is called for /query/1.0.0" -ForegroundColor Gray
    }
} catch {
    Write-Host "  Could not check logs: $_" -ForegroundColor Red
}
Write-Host ""

# Summary and recommendations
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Diagnostic Summary" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

# Determine the issue
$issueFound = $false

# Check peer connectivity
$agent8Peers = docker exec optimusdb8 ipfs swarm peers 2>$null
$peerCount = if ($agent8Peers) { ($agent8Peers | Measure-Object).Count } else { 0 }

if ($peerCount -eq 0) {
    Write-Host "❌ ISSUE: No peer connections" -ForegroundColor Red
    Write-Host "   Agents are not connected to each other via libp2p/IPFS" -ForegroundColor White
    Write-Host "   Solution: Check bootstrap nodes and network configuration" -ForegroundColor Yellow
    $issueFound = $true
} elseif ($peerCount -lt ($Agents - 1)) {
    Write-Host "⚠️  WARNING: Only $peerCount peers connected (expected $($Agents - 1))" -ForegroundColor Yellow
    Write-Host "   Some agents may not be fully connected" -ForegroundColor White
}

# Check if data exists locally
Write-Host ""
Write-Host "Likely Issues:" -ForegroundColor Yellow
Write-Host ""

Write-Host "1. Stream Handler Not Registered" -ForegroundColor Cyan
Write-Host "   The /query/1.0.0 stream handler may not be set up" -ForegroundColor White
Write-Host "   Check: app/service.go - ensure SetStreamHandler is called" -ForegroundColor Gray
Write-Host ""

Write-Host "2. Query Function Not Using Peer Protocol" -ForegroundColor Cyan
Write-Host "   queryPeersOptimized might not be using libp2p streams" -ForegroundColor White
Write-Host "   Check: query/worker_pool.go - queryPeer function" -ForegroundColor Gray
Write-Host ""

Write-Host "3. Protocol Mismatch" -ForegroundColor Cyan
Write-Host "   Client and server using different protocol IDs" -ForegroundColor White
Write-Host "   Check: Both should use '/query/1.0.0'" -ForegroundColor Gray
Write-Host ""

Write-Host "4. Bootstrap/Discovery Issues" -ForegroundColor Cyan
Write-Host "   Agents can't discover each other" -ForegroundColor White
Write-Host "   Check: IPFS swarm peers shows connections" -ForegroundColor Gray
Write-Host ""

# Specific code to check
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Code to Verify" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "In query/worker_pool.go, verify queryPeer function uses:" -ForegroundColor Yellow
Write-Host '  stream, err := hostNode.NewStream(ctx, peerID, "/query/1.0.0")' -ForegroundColor Cyan
Write-Host ""

Write-Host "In app/service.go or node setup, verify handler is registered:" -ForegroundColor Yellow
Write-Host '  hostNode.SetStreamHandler("/query/1.0.0", handleQueryStream)' -ForegroundColor Cyan
Write-Host ""

Write-Host "Check that both use the SAME protocol ID: /query/1.0.0" -ForegroundColor Yellow
Write-Host ""

# Next steps
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "Recommended Next Steps" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "1. Check if stream handler is registered:" -ForegroundColor White
Write-Host "   docker logs optimusdb1 | Select-String '/query'" -ForegroundColor Cyan
Write-Host ""

Write-Host "2. Check Agent 8 trying to query peers:" -ForegroundColor White
Write-Host "   docker logs optimusdb8 | Select-String 'query|peer'" -ForegroundColor Cyan
Write-Host ""

Write-Host "3. Manually test peer communication:" -ForegroundColor White
Write-Host "   docker exec optimusdb8 ipfs swarm connect <peer-address>" -ForegroundColor Cyan
Write-Host ""

Write-Host "4. Review the implementation files:" -ForegroundColor White
Write-Host "   - query/worker_pool.go (queryPeer function)" -ForegroundColor Cyan
Write-Host "   - app/service.go (stream handler registration)" -ForegroundColor Cyan
Write-Host ""

Write-Host "Run this diagnostic again after making changes to verify the fix." -ForegroundColor Yellow
Write-Host ""