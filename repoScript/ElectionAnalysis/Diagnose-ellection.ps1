# check-leader.ps1 - Find current coordinator and followers

param(
    [string]$LogPath = ".",
    [string]$LogPattern = "*.log"
)

Clear-Host

Write-Host "╔═══════════════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                                                                           ║" -ForegroundColor Cyan
Write-Host "║         OptimusDB Leader/Follower Status Check                           ║" -ForegroundColor Cyan
Write-Host "║                                                                           ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

# Check if running against Docker containers or log files
$useDocker = $false
$containers = @()

# Try Docker first
try {
    $dockerRunning = docker ps 2>&1
    if ($LASTEXITCODE -eq 0) {
        $containers = docker ps --filter "name=optimusdb" --format "{{.Names}}" | Sort-Object
        if ($containers.Count -gt 0) {
            $useDocker = $true
            Write-Host "✓ Found $($containers.Count) running Docker containers" -ForegroundColor Green
            Write-Host ""
        }
    }
} catch {
    # Docker not available, use log files
}

if (-not $useDocker) {
    # Find log files
    $logFiles = Get-ChildItem -Path $LogPath -Filter $LogPattern -ErrorAction SilentlyContinue | Where-Object { $_.Length -gt 0 }

    if ($logFiles.Count -eq 0) {
        Write-Host "❌ No Docker containers or log files found!" -ForegroundColor Red
        Write-Host ""
        Write-Host "Usage:" -ForegroundColor Yellow
        Write-Host "  1. For Docker: Ensure containers are running with 'optimusdb' in their names" -ForegroundColor White
        Write-Host "  2. For log files: Run from the directory containing *.log files" -ForegroundColor White
        Write-Host "  3. Or specify: .\check-leader.ps1 -LogPath 'C:\path\to\logs'" -ForegroundColor White
        Write-Host ""
        exit 1
    }

    Write-Host "✓ Found $($logFiles.Count) log file(s)" -ForegroundColor Green
    Write-Host ""
}

# Data structures
$coordinator = $null
$coordinatorTerm = 0
$followers = @()
$noRole = @()
$nodeInfo = @{}

Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "NODE STATUS ANALYSIS" -ForegroundColor White
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# Process Docker containers
if ($useDocker) {
    foreach ($container in $containers) {
        Write-Host "[$container]" -ForegroundColor Yellow

        # Get recent logs
        $logs = docker logs $container --tail 200 2>&1 | Out-String

        # Extract node ID
        $nodeIdMatch = $logs | Select-String -Pattern "Libp2p Node ID: ([A-Za-z0-9]+)" | Select-Object -Last 1
        $nodeId = "unknown"
        if ($nodeIdMatch) {
            $nodeId = $nodeIdMatch.Matches[0].Groups[1].Value
            if ($nodeId.Length -gt 12) {
                $nodeId = $nodeId.Substring(0, 12) + "..."
            }
        }

        # Look for STATUS messages (from LogRoleStatus)
        $statusMatches = $logs | Select-String -Pattern "\[STATUS\]" | Select-Object -Last 1

        # Look for ROLE messages (from handleAnnouncement)
        $roleMatches = $logs | Select-String -Pattern "\[ROLE\]" | Select-Object -Last 1

        $latestMessage = $null
        if ($statusMatches -and $roleMatches) {
            # Use whichever is more recent
            $statusMatches = $statusMatches | Select-Object -Last 1
            $roleMatches = $roleMatches | Select-Object -Last 1
            $latestMessage = $statusMatches
        } elseif ($statusMatches) {
            $latestMessage = $statusMatches
        } elseif ($roleMatches) {
            $latestMessage = $roleMatches
        }

        if ($latestMessage) {
            $line = $latestMessage.Line
            Write-Host "  Latest status: $line" -ForegroundColor White

            # Check if coordinator
            if ($line -match "I AM.*COORDINATOR|👑") {
                $term = 0
                if ($line -match "term (\d+)") {
                    $term = [int]$matches[1]
                }

                $coordinator = $container
                $coordinatorTerm = $term
                $nodeInfo[$container] = @{
                    Role = "Coordinator"
                    Term = $term
                    NodeId = $nodeId
                }

                Write-Host "  ✅ COORDINATOR (Term $term)" -ForegroundColor Green

                # Check heartbeat activity
                $heartbeats = $logs | Select-String -Pattern "\[HEARTBEAT\] Sent" | Select-Object -Last 1
                if ($heartbeats) {
                    Write-Host "  ✓ Sending heartbeats" -ForegroundColor Green
                } else {
                    Write-Host "  ⚠️  No recent heartbeats sent" -ForegroundColor Yellow
                }

            } elseif ($line -match "FOLLOWER|Following|📋") {
                $leaderId = "unknown"
                if ($line -match "following ([A-Za-z0-9]+)") {
                    $leaderId = $matches[1]
                    if ($leaderId.Length -gt 12) {
                        $leaderId = $leaderId.Substring(0, 12) + "..."
                    }
                }

                $term = 0
                if ($line -match "term (\d+)") {
                    $term = [int]$matches[1]
                }

                $followers += $container
                $nodeInfo[$container] = @{
                    Role = "Follower"
                    Leader = $leaderId
                    Term = $term
                    NodeId = $nodeId
                }

                Write-Host "  → Follower (following $leaderId, term $term)" -ForegroundColor Cyan

                # Check heartbeat reception
                $hbReceived = $logs | Select-String -Pattern "\[HB-RX\]" | Select-Object -Last 1
                if ($hbReceived) {
                    Write-Host "  ✓ Receiving heartbeats" -ForegroundColor Green
                } else {
                    Write-Host "  ⚠️  Not receiving heartbeats recently" -ForegroundColor Yellow
                }
            } else {
                $noRole += $container
                $nodeInfo[$container] = @{
                    Role = "Unknown"
                    NodeId = $nodeId
                }
                Write-Host "  ⚠️  Role unclear from status" -ForegroundColor Yellow
            }
        } else {
            $noRole += $container
            $nodeInfo[$container] = @{
                Role = "No Status"
                NodeId = $nodeId
            }
            Write-Host "  ⚠️  No [STATUS] or [ROLE] messages found" -ForegroundColor Yellow
        }

        # Check for recent election activity
        $electionLogs = $logs | Select-String -Pattern "\[ELECTION\].*Starting" | Select-Object -Last 1
        if ($electionLogs) {
            Write-Host "  Election: $($electionLogs.Line.Trim())" -ForegroundColor Gray
        }

        Write-Host "  Node ID: $nodeId" -ForegroundColor Gray
        Write-Host ""
    }
} else {
    # Process log files
    foreach ($log in $logFiles) {
        $nodeName = $log.BaseName
        Write-Host "[$nodeName]" -ForegroundColor Yellow

        $content = Get-Content $log.FullName -Raw

        # Extract node ID
        $nodeIdMatch = $content | Select-String -Pattern "Libp2p Node ID: ([A-Za-z0-9]+)" | Select-Object -Last 1
        $nodeId = "unknown"
        if ($nodeIdMatch) {
            $nodeId = $nodeIdMatch.Matches[0].Groups[1].Value
            if ($nodeId.Length -gt 12) {
                $nodeId = $nodeId.Substring(0, 12) + "..."
            }
        }

        # Look for STATUS or ROLE messages
        $statusMatches = $content | Select-String -Pattern "\[STATUS\]" | Select-Object -Last 1
        $roleMatches = $content | Select-String -Pattern "\[ROLE\]" | Select-Object -Last 1

        if ($statusMatches -is [array]) {
            $latestMessage = if ($statusMatches.Count) { $statusMatches } else { $roleMatches }
        } else {
            $latestMessage = if ([string]::IsNullOrEmpty($statusMatches)) { $roleMatches } else { $statusMatches }
        }



        if ($latestMessage) {
            $line = $latestMessage.Line
            Write-Host "  Latest: $line" -ForegroundColor White

            if ($line -match "I AM.*COORDINATOR|👑") {
                $term = 0
                if ($line -match "term (\d+)") {
                    $term = [int]$matches[1]
                }

                $coordinator = $nodeName
                $coordinatorTerm = $term
                $nodeInfo[$nodeName] = @{
                    Role = "Coordinator"
                    Term = $term
                    NodeId = $nodeId
                }

                Write-Host "  ✅ COORDINATOR (Term $term)" -ForegroundColor Green

            } elseif ($line -match "FOLLOWER|Following|📋") {
                $leaderId = "unknown"
                if ($line -match "following ([A-Za-z0-9]+)") {
                    $leaderId = $matches[1]
                    if ($leaderId.Length -gt 12) {
                        $leaderId = $leaderId.Substring(0, 12) + "..."
                    }
                }

                $term = 0
                if ($line -match "term (\d+)") {
                    $term = [int]$matches[1]
                }

                $followers += $nodeName
                $nodeInfo[$nodeName] = @{
                    Role = "Follower"
                    Leader = $leaderId
                    Term = $term
                    NodeId = $nodeId
                }

                Write-Host "  → Follower (following $leaderId, term $term)" -ForegroundColor Cyan
            }
        } else {
            $noRole += $nodeName
            $nodeInfo[$nodeName] = @{
                Role = "No Status"
                NodeId = $nodeId
            }
            Write-Host "  ⚠️  No status found" -ForegroundColor Yellow
        }

        Write-Host "  Node ID: $nodeId" -ForegroundColor Gray
        Write-Host ""
    }
}

# Summary Section
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "CLUSTER LEADERSHIP SUMMARY" -ForegroundColor White
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

if ($coordinator) {
    Write-Host "👑 COORDINATOR:" -ForegroundColor Green
    Write-Host "   Node: $coordinator" -ForegroundColor White
    Write-Host "   Term: $coordinatorTerm" -ForegroundColor White
    if ($nodeInfo[$coordinator].NodeId) {
        Write-Host "   ID:   $($nodeInfo[$coordinator].NodeId)" -ForegroundColor Gray
    }
} else {
    Write-Host "⚠️  NO COORDINATOR ELECTED!" -ForegroundColor Red
    Write-Host "   Possible causes:" -ForegroundColor Yellow
    Write-Host "   - Election in progress" -ForegroundColor Yellow
    Write-Host "   - Election failed" -ForegroundColor Yellow
    Write-Host "   - Nodes just started" -ForegroundColor Yellow
}

Write-Host ""

if ($followers.Count -gt 0) {
    Write-Host "📋 FOLLOWERS ($($followers.Count)):" -ForegroundColor Cyan
    foreach ($f in $followers) {
        $info = $nodeInfo[$f]
        $leader = if ($info.Leader) { "→ $($info.Leader)" } else { "" }
        $term = if ($info.Term) { "term $($info.Term)" } else { "" }
        Write-Host "   - $f $leader $term" -ForegroundColor White
    }
} else {
    Write-Host "📋 FOLLOWERS: None" -ForegroundColor Yellow
}

Write-Host ""

if ($noRole.Count -gt 0) {
    Write-Host "⚠️  NO ROLE ASSIGNED ($($noRole.Count)):" -ForegroundColor Yellow
    foreach ($n in $noRole) {
        Write-Host "   - $n" -ForegroundColor White
    }
    Write-Host ""
}

# Health Check
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host "CLUSTER HEALTH" -ForegroundColor White
Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

$totalNodes = if ($useDocker) { $containers.Count } else { $logFiles.Count }

if ($coordinator -and $followers.Count -gt 0) {
    Write-Host "✅ HEALTHY CLUSTER" -ForegroundColor Green
    Write-Host "   Status: 1 Coordinator, $($followers.Count) Follower(s)" -ForegroundColor White
    Write-Host "   Total:  $totalNodes node(s)" -ForegroundColor White

} elseif ($coordinator -and $followers.Count -eq 0) {
    Write-Host "⚠️  SINGLE NODE CLUSTER" -ForegroundColor Yellow
    Write-Host "   Only coordinator exists, no followers yet" -ForegroundColor Yellow
    Write-Host "   This is normal if other nodes just started" -ForegroundColor White

} elseif (!$coordinator -and $followers.Count -gt 0) {
    Write-Host "❌ INCONSISTENT STATE" -ForegroundColor Red
    Write-Host "   Followers exist but no coordinator!" -ForegroundColor Red
    Write-Host "   This indicates a serious issue" -ForegroundColor Red

} elseif (!$coordinator -and $noRole.Count -eq $totalNodes) {
    Write-Host "⚠️  ELECTION NOT STARTED" -ForegroundColor Yellow
    Write-Host "   No nodes have roles yet" -ForegroundColor Yellow
    Write-Host "   Nodes may still be initializing" -ForegroundColor White
    Write-Host "   Wait 30-60 seconds and check again" -ForegroundColor White

} else {
    Write-Host "❌ UNHEALTHY - No coordinator elected" -ForegroundColor Red
    Write-Host "   Check logs for election failures" -ForegroundColor Yellow
}

Write-Host ""

# Term consistency check
if ($coordinator -and $followers.Count -gt 0) {
    $mismatchedTerms = $followers | Where-Object {
        $nodeInfo[$_].Term -ne $coordinatorTerm
    }

    if ($mismatchedTerms.Count -gt 0) {
        Write-Host "⚠️  WARNING: Term mismatch detected!" -ForegroundColor Yellow
        Write-Host "   Some followers are on different terms:" -ForegroundColor Yellow
        foreach ($node in $mismatchedTerms) {
            Write-Host "   - $node is on term $($nodeInfo[$node].Term), coordinator is on term $coordinatorTerm" -ForegroundColor Yellow
        }
        Write-Host ""
    }
}

Write-Host "═══════════════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

if ($useDocker) {
    Write-Host "💡 Tip: Watch live status updates with:" -ForegroundColor Gray
    Write-Host "   docker logs -f $($containers[0]) | Select-String '\[STATUS\]'" -ForegroundColor White
} else {
    Write-Host "💡 Tip: For live updates, use Docker mode" -ForegroundColor Gray
}

Write-Host ""