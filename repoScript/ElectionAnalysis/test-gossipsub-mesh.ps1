# test-gossipsub-mesh-corrected.ps1
# Complete test suite for GossipSub mesh formation
# Corrected based on actual log patterns

param(
    [int]$AgentCount = 8,
    [string]$BasePort = 4001,
    [string]$ContainerPrefix = "optimusdb-agent-",  # Updated to match actual naming
    [int]$WaitTime = 30
)

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GossipSub Mesh Formation Test Suite" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Function to check if Docker is running
function Test-Docker {
    try {
        docker version | Out-Null
        return $true
    } catch {
        Write-Host "Docker is not running!" -ForegroundColor Red
        return $false
    }
}

# Function to get container logs
function Get-ContainerLogs {
    param([string]$ContainerName, [int]$Lines = 50)

    $logs = docker logs $ContainerName --tail $Lines 2>&1
    return $logs
}

# Function to check mesh formation - CORRECTED
function Test-MeshFormation {
    param([string]$ContainerName)

    Write-Host "`nChecking mesh status for $ContainerName..." -ForegroundColor Yellow

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 100

    # Look for the MESH-STATUS pattern
    $meshStatus = $logs | Select-String "\[MESH-STATUS\] Mesh peers:" | Select-Object -Last 1
    if ($meshStatus) {
        # Extract the number after "Mesh peers:"
        $peerCount = [regex]::Match($meshStatus, "Mesh peers:\s*(\d+)").Groups[1].Value
        Write-Host "  ✓ Mesh peers: $peerCount" -ForegroundColor Green

        # Also show connected peers
        $connectedPeers = $logs | Select-String "\[MESH-STATUS\] Connected peers:" | Select-Object -Last 1
        if ($connectedPeers) {
            $connected = [regex]::Match($connectedPeers, "Connected peers:\s*(\d+)").Groups[1].Value
            Write-Host "  ✓ Connected peers: $connected" -ForegroundColor Green
        }

        return [int]$peerCount
    } else {
        Write-Host "  ✗ No mesh information found" -ForegroundColor Red
        return 0
    }
}

# Function to check discovered peers - CORRECTED
function Test-PeerDiscovery {
    param([string]$ContainerName)

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 150

    # Look for DISCOVERY patterns
    $foundPeers = $logs | Select-String "\[DISCOVERY\] Found peer:"
    $connectedPeers = $logs | Select-String "\[DISCOVERY\] Connected to peer:"

    Write-Host "  Discovery Status:" -ForegroundColor Cyan
    Write-Host "    - Peers found: $($foundPeers.Count)" -ForegroundColor Green
    Write-Host "    - Peers connected: $($connectedPeers.Count)" -ForegroundColor Green

    # Get unique peer IDs
    $uniquePeers = $foundPeers | ForEach-Object {
        [regex]::Match($_, "Found peer: (Qm\w+)").Groups[1].Value
    } | Select-Object -Unique

    Write-Host "    - Unique peers discovered: $($uniquePeers.Count)" -ForegroundColor Cyan

    return $connectedPeers.Count
}

# Function to check for GRAFT/ADD_PEER events - CORRECTED
function Test-GraftEvents {
    param([string]$ContainerName)

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 200

    # Look for GRAFT events (mesh joining)
    $grafts = $logs | Select-String "\[MESH\] 🌿 GRAFT:"

    # Look for ADD_PEER events
    $addPeers = $logs | Select-String "\[MESH\] 👥 ADD_PEER:"

    # Look for JOIN events
    $joins = $logs | Select-String "\[MESH\] ➕ JOIN:"

    Write-Host "  Mesh Events:" -ForegroundColor Cyan
    Write-Host "    - GRAFT events: $($grafts.Count)" -ForegroundColor Green
    Write-Host "    - ADD_PEER events: $($addPeers.Count)" -ForegroundColor Green
    Write-Host "    - JOIN events: $($joins.Count)" -ForegroundColor Green

    return $grafts.Count + $addPeers.Count
}

# Function to check election status - CORRECTED
function Test-Election {
    param([string]$ContainerName)

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 300

    # Check for election activities
    $elections = $logs | Select-String "\[ELECTION\] Starting Term"
    $votes = $logs | Select-String "\[VOTE-RX\]"
    $noWinner = $logs | Select-String "\[ELECTION\] No winner"
    $failures = $logs | Select-String "\[FAILURE\] Leader dead"

    Write-Host "  Election Status:" -ForegroundColor Cyan
    Write-Host "    - Elections started: $($elections.Count)" -ForegroundColor Yellow
    Write-Host "    - Votes cast: $($votes.Count)" -ForegroundColor Cyan
    Write-Host "    - Failed elections: $($noWinner.Count)" -ForegroundColor $(if($noWinner.Count -gt 0){"Red"}else{"Green"})
    Write-Host "    - Leader failures detected: $($failures.Count)" -ForegroundColor $(if($failures.Count -gt 0){"Red"}else{"Green"})

    # Get latest term
    if ($elections.Count -gt 0) {
        $latestElection = $elections | Select-Object -Last 1
        $term = [regex]::Match($latestElection, "Term (\d+)").Groups[1].Value
        Write-Host "    - Current term: $term" -ForegroundColor Cyan
    }

    # Determine status based on patterns
    if ($noWinner.Count -gt 5) {
        Write-Host "  ⚠️  Status: ELECTION ISSUES - Multiple failed elections" -ForegroundColor Red
        return "ElectionIssues"
    } elseif ($votes.Count -gt 0) {
        Write-Host "  ✓ Status: PARTICIPATING in elections" -ForegroundColor Yellow
        return "Participating"
    } else {
        Write-Host "  ? Status: Unknown" -ForegroundColor Gray
        return "Unknown"
    }
}

# Function to check message flow - CORRECTED
function Test-MessageFlow {
    param([string]$ContainerName)

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 200

    # Look for publish and receive patterns
    $published = $logs | Select-String "\[PUBLISH\] ✅.*published"
    $received = $logs | Select-String "\[MSG-RX-\d+\]"

    # Look for reputation messages
    $reputation = $logs | Select-String "\[REPUTATION\] Published"

    Write-Host "  Message Flow:" -ForegroundColor Cyan
    Write-Host "    - Messages published: $($published.Count)" -ForegroundColor Green
    Write-Host "    - Messages received: $($received.Count)" -ForegroundColor Green
    Write-Host "    - Reputation updates: $($reputation.Count)" -ForegroundColor Cyan

    return @{Published=$published.Count; Received=$received.Count; Reputation=$reputation.Count}
}

# Function to check connectivity issues - NEW
function Test-ConnectivityIssues {
    param([string]$ContainerName)

    $logs = Get-ContainerLogs -ContainerName $ContainerName -Lines 100

    # Check for missed heartbeats
    $missedHeartbeats = $logs | Select-String "\[WARN\] Missed.*heartbeats"

    # Check for EMS connection failures (expected, not critical)
    $emsFailures = $logs | Select-String "\[EMS\] Connect failed"

    Write-Host "  Connectivity Check:" -ForegroundColor Cyan
    if ($missedHeartbeats.Count -gt 0) {
        Write-Host "    ⚠️  Missed heartbeats detected: $($missedHeartbeats.Count)" -ForegroundColor Yellow
        # Show last one
        $last = $missedHeartbeats | Select-Object -Last 1
        Write-Host "      Last: $($last.Line)" -ForegroundColor Gray
    } else {
        Write-Host "    ✓ No missed heartbeats" -ForegroundColor Green
    }

    return $missedHeartbeats.Count
}

# Main test execution
Write-Host "`n1. Checking Docker..." -ForegroundColor White
if (-not (Test-Docker)) {
    exit 1
}
Write-Host "   ✓ Docker is running" -ForegroundColor Green

# Check if containers are running
Write-Host "`n2. Detecting OptimusDB containers..." -ForegroundColor White
# Look for any optimusdb containers
$allContainers = docker ps --format "{{.Names}}" | Select-String "optimusdb"
$runningContainers = @($allContainers)

if ($runningContainers.Count -eq 0) {
    Write-Host "   ✗ No OptimusDB containers found!" -ForegroundColor Red
    Write-Host "   Looking for containers with pattern: *optimusdb*" -ForegroundColor Yellow
    Write-Host "`n   Available containers:" -ForegroundColor Gray
    docker ps --format "table {{.Names}}\t{{.Status}}"
    exit 1
}

Write-Host "   ✓ Found $($runningContainers.Count) OptimusDB containers:" -ForegroundColor Green
$runningContainers | ForEach-Object { Write-Host "     - $_" -ForegroundColor Cyan }

Write-Host "`n3. Waiting for mesh stabilization ($WaitTime seconds)..." -ForegroundColor White
Start-Sleep -Seconds $WaitTime

# Test mesh formation for each container
Write-Host "`n4. Testing Mesh Formation:" -ForegroundColor White
Write-Host "============================" -ForegroundColor White

$meshStats = @{}
$totalMeshPeers = 0
$totalDiscovered = 0
$totalMeshEvents = 0

foreach ($container in $runningContainers) {
    Write-Host "`n[$container]" -ForegroundColor Cyan -BackgroundColor DarkBlue

    # Discovery test
    $discovered = Test-PeerDiscovery -ContainerName $container
    $totalDiscovered += $discovered

    # Mesh test
    $meshPeers = Test-MeshFormation -ContainerName $container
    $totalMeshPeers += $meshPeers

    # Mesh events
    $meshEvents = Test-GraftEvents -ContainerName $container
    $totalMeshEvents += $meshEvents

    # Election status
    $electionStatus = Test-Election -ContainerName $container

    # Message flow
    $messages = Test-MessageFlow -ContainerName $container

    # Connectivity check
    $missedHeartbeats = Test-ConnectivityIssues -ContainerName $container

    $meshStats[$container] = @{
        Discovered = $discovered
        MeshPeers = $meshPeers
        MeshEvents = $meshEvents
        ElectionStatus = $electionStatus
        Messages = $messages
        MissedHeartbeats = $missedHeartbeats
    }

    Write-Host ""
}

# Summary Report
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "SUMMARY REPORT" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

# Mesh health check
$healthyNodes = $meshStats.Values | Where-Object { $_.MeshPeers -gt 0 } | Measure-Object
$avgMeshPeers = if ($runningContainers.Count -gt 0) {
    ($meshStats.Values | ForEach-Object { $_.MeshPeers } | Measure-Object -Average).Average
} else { 0 }

Write-Host "`nMesh Statistics:" -ForegroundColor White
Write-Host "  - Total containers: $($runningContainers.Count)" -ForegroundColor Cyan
Write-Host "  - Nodes with mesh: $($healthyNodes.Count)/$($runningContainers.Count)" -ForegroundColor $(if($healthyNodes.Count -eq $runningContainers.Count){"Green"}else{"Yellow"})
Write-Host "  - Average mesh peers: $([math]::Round($avgMeshPeers, 2))" -ForegroundColor Cyan
Write-Host "  - Total mesh events: $totalMeshEvents" -ForegroundColor Cyan
Write-Host "  - Total discovered: $totalDiscovered" -ForegroundColor Cyan

Write-Host "`nElection Analysis:" -ForegroundColor White
$electionIssues = ($meshStats.Values | Where-Object { $_.ElectionStatus -eq "ElectionIssues" }).Count
$participating = ($meshStats.Values | Where-Object { $_.ElectionStatus -eq "Participating" }).Count

Write-Host "  - Nodes with election issues: $electionIssues" -ForegroundColor $(if($electionIssues -eq 0){"Green"}else{"Red"})
Write-Host "  - Nodes participating: $participating" -ForegroundColor Cyan

# Connectivity issues
$nodesWithIssues = ($meshStats.Values | Where-Object { $_.MissedHeartbeats -gt 0 }).Count
Write-Host "`nConnectivity:" -ForegroundColor White
Write-Host "  - Nodes with missed heartbeats: $nodesWithIssues" -ForegroundColor $(if($nodesWithIssues -eq 0){"Green"}else{"Yellow"})

# Health verdict
Write-Host "`nCluster Health:" -ForegroundColor White
if ($healthyNodes.Count -eq $runningContainers.Count -and $avgMeshPeers -gt 0 -and $electionIssues -lt $runningContainers.Count) {
    Write-Host "  ✅ HEALTHY - Mesh formed successfully!" -ForegroundColor Green -BackgroundColor DarkGreen
    Write-Host "     All nodes are connected and participating in the mesh." -ForegroundColor Green
} elseif ($healthyNodes.Count -gt ($runningContainers.Count / 2) -and $avgMeshPeers -gt 0) {
    Write-Host "  ⚠️  PARTIAL - Mesh partially formed" -ForegroundColor Yellow
    Write-Host "     Most nodes connected but some may have issues." -ForegroundColor Yellow
} else {
    Write-Host "  ❌ UNHEALTHY - Mesh formation failed!" -ForegroundColor Red -BackgroundColor DarkRed
    Write-Host "     Critical issues detected in mesh formation." -ForegroundColor Red
}

# Detailed logs for debugging
Write-Host "`n5. Detailed Diagnostics:" -ForegroundColor White
Write-Host "========================" -ForegroundColor White

# Get latest mesh status from first container
if ($runningContainers.Count -gt 0) {
    $sampleContainer = $runningContainers[0]
    Write-Host "`nLatest Mesh Status from $sampleContainer`:" -ForegroundColor Cyan
    $latestLogs = docker logs $sampleContainer --tail 30 2>&1 | Select-String "MESH-STATUS"
    if ($latestLogs) {
        $latestLogs | Select-Object -Last 5 | ForEach-Object { Write-Host "  $($_.Line)" -ForegroundColor Gray }
    } else {
        Write-Host "  No recent mesh status found" -ForegroundColor Yellow
    }
}

# Check for errors
Write-Host "`n6. Error Check:" -ForegroundColor White
$errorCount = 0
foreach ($container in $runningContainers) {
    $errors = docker logs $container --tail 100 2>&1 | Select-String "ERROR|FATAL|panic" | Where-Object { $_ -notmatch "EMS" }
    if ($errors -and $errors.Count -gt 0) {
        Write-Host "  ✗ $container has $($errors.Count) errors" -ForegroundColor Red
        $errorCount += $errors.Count
        # Show first error
        Write-Host "    First error: $($errors[0].Line)" -ForegroundColor Gray
    } else {
        Write-Host "  ✓ $container - No critical errors" -ForegroundColor Green
    }
}

# Export results to JSON (convert to PSCustomObject to avoid serialization issues)
$timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$reportFile = "mesh-test-results-$timestamp.json"

# Convert meshStats to array of objects
$detailsArray = @()
foreach ($key in $meshStats.Keys) {
    $detailsArray += [PSCustomObject]@{
        Container = $key
        Discovered = $meshStats[$key].Discovered
        MeshPeers = $meshStats[$key].MeshPeers
        MeshEvents = $meshStats[$key].MeshEvents
        ElectionStatus = $meshStats[$key].ElectionStatus
        MessagesPublished = $meshStats[$key].Messages.Published
        MessagesReceived = $meshStats[$key].Messages.Received
        ReputationUpdates = $meshStats[$key].Messages.Reputation
        MissedHeartbeats = $meshStats[$key].MissedHeartbeats
    }
}

$reportData = [PSCustomObject]@{
    Timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    ContainerCount = $runningContainers.Count
    HealthyNodes = $healthyNodes.Count
    AverageMeshPeers = $avgMeshPeers
    TotalMeshEvents = $totalMeshEvents
    ElectionIssues = $electionIssues
    ErrorCount = $errorCount
    Details = $detailsArray
}

try {
    $reportData | ConvertTo-Json -Depth 4 | Out-File $reportFile
    Write-Host "`n📊 Full report saved to: $reportFile" -ForegroundColor Cyan
} catch {
    Write-Host "`n⚠️  Could not save JSON report: $($_.Exception.Message)" -ForegroundColor Yellow
    # Try CSV as fallback
    $csvFile = "mesh-test-results-$timestamp.csv"
    $detailsArray | Export-Csv -Path $csvFile -NoTypeInformation
    Write-Host "📊 Report saved as CSV: $csvFile" -ForegroundColor Cyan
}

# Final recommendation
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

if ($avgMeshPeers -lt 1) {
    Write-Host "⚠️  Mesh not forming properly. Check:" -ForegroundColor Yellow
    Write-Host "  1. Network connectivity between containers (docker network ls)" -ForegroundColor White
    Write-Host "  2. MDNS discovery is enabled ([DISCOVERY] Using MDNS)" -ForegroundColor White
    Write-Host "  3. Topic subscription is correct (topic 'optimusdb')" -ForegroundColor White
    Write-Host "  4. Firewall rules allowing multicast" -ForegroundColor White
} elseif ($electionIssues -gt ($runningContainers.Count / 2)) {
    Write-Host "⚠️  Election issues detected. This is EXPECTED behavior:" -ForegroundColor Yellow
    Write-Host "  - The system requires minimum quorum (2 nodes) to elect a leader" -ForegroundColor White
    Write-Host "  - With insufficient participation, elections will keep retrying" -ForegroundColor White
    Write-Host "  - This prevents split-brain scenarios" -ForegroundColor White
    Write-Host "`n  Current situation:" -ForegroundColor Cyan
    Write-Host "  - Nodes are discovering each other ✓" -ForegroundColor Green
    Write-Host "  - Mesh is forming ✓" -ForegroundColor Green
    Write-Host "  - But quorum not reached for leader election" -ForegroundColor Yellow
} elseif ($nodesWithIssues -gt ($runningContainers.Count / 2)) {
    Write-Host "⚠️  Connectivity issues detected:" -ForegroundColor Yellow
    Write-Host "  - Multiple nodes missing heartbeats" -ForegroundColor White
    Write-Host "  - Check network latency and stability" -ForegroundColor White
} else {
    Write-Host "✅ System is healthy! Mesh formed successfully." -ForegroundColor Green
    Write-Host "   All nodes are connected and communicating properly." -ForegroundColor Green
}

Write-Host "`n📝 Note: EMS connection failures are expected if EMS broker is not deployed." -ForegroundColor Gray
Write-Host "   These can be safely ignored for mesh testing." -ForegroundColor Gray

Write-Host "`nTest completed at $(Get-Date)" -ForegroundColor Gray
Write-Host "========================================`n" -ForegroundColor Cyan