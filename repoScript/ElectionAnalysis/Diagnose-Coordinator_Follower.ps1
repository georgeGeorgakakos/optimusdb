# diagnose_cluster.ps1 - Comprehensive diagnostic for OptimusDB cluster
# This will tell you exactly what state your cluster is in

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║        OptimusDB Cluster Diagnostic Tool                      ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Step 1: Check if containers are running
Write-Host "[1/7] Checking Container Status..." -ForegroundColor Yellow
$containers = docker ps --format "{{.Names}}" | Where-Object { $_ -match "optimusdb" }

if ($containers.Count -eq 0) {
    Write-Host "❌ No OptimusDB containers found!" -ForegroundColor Red
    Write-Host "   Make sure your containers are running." -ForegroundColor Red
    exit
}

Write-Host "✅ Found $($containers.Count) running containers" -ForegroundColor Green
foreach ($c in $containers) {
    Write-Host "   - $c" -ForegroundColor White
}
Write-Host ""

# Step 2: Check if containers are using NEW code
Write-Host "[2/7] Checking if NEW code is deployed..." -ForegroundColor Yellow
$sample = $containers[0]
$newCodeMarker = docker logs $sample 2>&1 | Select-String "Using unified libp2p host"

if ($newCodeMarker) {
    Write-Host "✅ NEW CODE detected (unified host)" -ForegroundColor Green
    Write-Host "   $newCodeMarker" -ForegroundColor Gray
} else {
    Write-Host "⚠️  OLD CODE still running (dual host)" -ForegroundColor Red
    Write-Host "   You need to rebuild and redeploy with the NEW main.go!" -ForegroundColor Red
    Write-Host ""
    Write-Host "   The fix won't work with the old code." -ForegroundColor Red
}
Write-Host ""

# Step 3: Check peer discovery
Write-Host "[3/7] Checking Peer Discovery..." -ForegroundColor Yellow
$discovered = docker logs $sample 2>&1 | Select-String "DISCOVERY.*Found peer"
$connectedCount = (docker logs $sample 2>&1 | Select-String "DISCOVERY.*Connected to peer").Count

if ($discovered.Count -gt 0) {
    Write-Host "✅ Peer discovery working: $connectedCount peers connected" -ForegroundColor Green
    Write-Host "   Last 3 discoveries:" -ForegroundColor Gray
    $discovered | Select-Object -Last 3 | ForEach-Object {
        Write-Host "   $_" -ForegroundColor Gray
    }
} else {
    Write-Host "❌ No peer discovery activity!" -ForegroundColor Red
    Write-Host "   Peers are not finding each other." -ForegroundColor Red
}
Write-Host ""

# Step 4: Check mesh formation
Write-Host "[4/7] Checking GossipSub Mesh Formation..." -ForegroundColor Yellow
$meshLogs = docker logs $sample 2>&1 | Select-String "Mesh peers:"
$latestMesh = $meshLogs | Select-Object -Last 1

if ($latestMesh) {
    Write-Host "   Latest mesh status:" -ForegroundColor Gray
    Write-Host "   $latestMesh" -ForegroundColor White

    if ($latestMesh -match "Mesh peers: 0") {
        Write-Host "❌ CRITICAL: Mesh not formed (peers: 0)" -ForegroundColor Red
        Write-Host "   This is why elections are failing!" -ForegroundColor Red
        Write-Host ""
        Write-Host "   Possible causes:" -ForegroundColor Yellow
        Write-Host "   1. Old code still running (check step 2)" -ForegroundColor Yellow
        Write-Host "   2. Network isolation between containers" -ForegroundColor Yellow
        Write-Host "   3. Firewall blocking GossipSub" -ForegroundColor Yellow
    } else {
        Write-Host "✅ Mesh formed successfully" -ForegroundColor Green
    }
} else {
    Write-Host "⚠️  No mesh formation logs found" -ForegroundColor Yellow
    Write-Host "   Election system may not have started yet" -ForegroundColor Yellow
}
Write-Host ""

# Step 5: Check if election started
Write-Host "[5/7] Checking Election Status..." -ForegroundColor Yellow
$electionStarted = docker logs $sample 2>&1 | Select-String "Starting Term"
$electionResults = docker logs $sample 2>&1 | Select-String "Results for Term"

if ($electionStarted) {
    Write-Host "✅ Election started" -ForegroundColor Green
    $electionStarted | Select-Object -Last 1 | ForEach-Object {
        Write-Host "   $_" -ForegroundColor Gray
    }

    # Check for results
    if ($electionResults) {
        Write-Host ""
        Write-Host "   Last election results:" -ForegroundColor Gray
        $electionResults | Select-Object -Last 1 | ForEach-Object {
            Write-Host "   $_" -ForegroundColor Gray
        }

        # Get the context around results
        $fullLog = docker logs $sample 2>&1
        $resultIndex = $fullLog.IndexOf(($electionResults | Select-Object -Last 1).Line)
        if ($resultIndex -ge 0) {
            $context = $fullLog[($resultIndex)..($resultIndex + 5)]
            $context | ForEach-Object {
                Write-Host "   $_" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "⏳ Election in progress (no results yet)" -ForegroundColor Yellow
    }
} else {
    Write-Host "❌ Election not started yet" -ForegroundColor Red
    Write-Host "   Waiting for peers and mesh formation..." -ForegroundColor Red
}
Write-Host ""

# Step 6: Check vote propagation
Write-Host "[6/7] Checking Vote Propagation..." -ForegroundColor Yellow
$votes = docker logs $sample 2>&1 | Select-String "MSG-RX.*vote"

if ($votes.Count -gt 0) {
    Write-Host "✅ Receiving votes: $($votes.Count) total" -ForegroundColor Green

    # Check for unique voters
    $voters = @()
    foreach ($vote in $votes) {
        if ($vote -match "From: (\S+)") {
            $voters += $matches[1]
        }
    }
    $uniqueVoters = $voters | Sort-Object -Unique

    Write-Host "   Unique voters: $($uniqueVoters.Count)" -ForegroundColor Gray

    if ($uniqueVoters.Count -gt 1) {
        Write-Host "   ✅ Cross-peer voting working!" -ForegroundColor Green
    } else {
        Write-Host "   ❌ Only self-votes (mesh problem!)" -ForegroundColor Red
    }

    Write-Host ""
    Write-Host "   Last 3 votes received:" -ForegroundColor Gray
    $votes | Select-Object -Last 3 | ForEach-Object {
        Write-Host "   $_" -ForegroundColor Gray
    }
} else {
    Write-Host "⚠️  No votes received yet" -ForegroundColor Yellow
    Write-Host "   Election hasn't reached voting phase" -ForegroundColor Yellow
}
Write-Host ""

# Step 7: Check for role assignments
Write-Host "[7/7] Checking Role Assignments..." -ForegroundColor Yellow

$hasCoordinator = $false
$hasFollowers = $false

foreach ($container in $containers) {
    $coordLog = docker logs $container 2>&1 | Select-String "I AM COORDINATOR"
    $followerLog = docker logs $container 2>&1 | Select-String "Following"

    if ($coordLog) {
        $hasCoordinator = $true
        Write-Host "✅ Found coordinator: $container" -ForegroundColor Green
        Write-Host "   $coordLog" -ForegroundColor Gray
    }
    elseif ($followerLog) {
        $hasFollowers = $true
        if (-not $hasCoordinator) {
            Write-Host "✅ Found follower: $container" -ForegroundColor Blue
        }
    }
}

if (-not $hasCoordinator -and -not $hasFollowers) {
    Write-Host "❌ No roles assigned yet" -ForegroundColor Red
    Write-Host "   Election hasn't completed successfully" -ForegroundColor Red
}
Write-Host ""

# Summary and recommendations
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                    DIAGNOSIS SUMMARY                          ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Determine the issue
if (-not $newCodeMarker) {
    Write-Host "🔴 ROOT CAUSE: OLD CODE STILL RUNNING" -ForegroundColor Red
    Write-Host ""
    Write-Host "ACTION REQUIRED:" -ForegroundColor Yellow
    Write-Host "1. Stop all containers" -ForegroundColor White
    Write-Host "2. Replace main.go with the NEW version (unified host)" -ForegroundColor White
    Write-Host "3. Rebuild Docker image" -ForegroundColor White
    Write-Host "4. Redeploy containers" -ForegroundColor White
    Write-Host "5. Wait 60 seconds and run this diagnostic again" -ForegroundColor White
}
elseif ($latestMesh -and ($latestMesh -match "Mesh peers: 0")) {
    Write-Host "🔴 ROOT CAUSE: MESH NOT FORMING" -ForegroundColor Red
    Write-Host ""
    Write-Host "Possible causes:" -ForegroundColor Yellow
    Write-Host "1. Network isolation between containers" -ForegroundColor White
    Write-Host "2. Containers on different Docker networks" -ForegroundColor White
    Write-Host "3. Firewall blocking connections" -ForegroundColor White
    Write-Host ""
    Write-Host "Check network:" -ForegroundColor Yellow
    Write-Host "  docker network inspect <network-name>" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Test connectivity:" -ForegroundColor Yellow
    Write-Host "  docker exec $($containers[0]) ping <another-container-ip>" -ForegroundColor Gray
}
elseif (-not $electionStarted) {
    Write-Host "🟡 STATUS: WAITING FOR ELECTION TO START" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Cluster is still initializing..." -ForegroundColor White
    Write-Host "Wait time: Up to 60 seconds from container start" -ForegroundColor White
    Write-Host ""
    Write-Host "Current wait time: 15 minutes ago (check if something is stuck)" -ForegroundColor Red
}
elseif (-not $hasCoordinator) {
    Write-Host "🟡 STATUS: ELECTION IN PROGRESS" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "Election started but no leader elected yet..." -ForegroundColor White
    Write-Host "This might indicate:" -ForegroundColor Yellow
    Write-Host "- Not enough votes received (check mesh)" -ForegroundColor White
    Write-Host "- Election timing out and retrying" -ForegroundColor White
    Write-Host ""
    Write-Host "Wait another 30 seconds and check again" -ForegroundColor White
}
else {
    Write-Host "🟢 STATUS: HEALTHY" -ForegroundColor Green
    Write-Host ""
    Write-Host "Coordinator elected and cluster operational!" -ForegroundColor White
}

Write-Host ""
Write-Host "Run this diagnostic again in 30-60 seconds to check progress" -ForegroundColor Gray
Write-Host ""