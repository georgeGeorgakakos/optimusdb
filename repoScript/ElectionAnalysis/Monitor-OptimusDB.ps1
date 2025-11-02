# ============================================================================
# OptimusDB Cluster Monitor - Comprehensive Real-Time Analysis
# Save as: Monitor-OptimusDB.ps1
# Usage: .\Monitor-OptimusDB.ps1 [-Duration 300]
# ============================================================================

param(
    [int]$Duration = 180,  # Run for 3 minutes by default
    [int]$Interval = 15,   # Check every 15 seconds
    [string[]]$Containers = @("optimusdb1", "optimusdb2", "optimusdb3", "optimusdb4", "optimusdb5", "optimusdb6", "optimusdb7", "optimusdb8")
)

$script:StartTime = Get-Date
$script:Snapshots = @()

# ============================================================================
# Helper Functions
# ============================================================================

function Write-Header {
    param([string]$Text, [string]$Color = "Cyan")
    Write-Host ""
    Write-Host ("═" * 80) -ForegroundColor $Color
    Write-Host " $Text" -ForegroundColor $Color
    Write-Host ("═" * 80) -ForegroundColor $Color
}

function Write-SubHeader {
    param([string]$Text, [string]$Color = "Yellow")
    Write-Host ""
    Write-Host ("─" * 80) -ForegroundColor $Color
    Write-Host " $Text" -ForegroundColor $Color
    Write-Host ("─" * 80) -ForegroundColor $Color
}

function Get-Timestamp {
    return (Get-Date).ToString("HH:mm:ss")
}

function Get-ElapsedTime {
    $elapsed = (Get-Date) - $script:StartTime
    return "{0:mm}:{0:ss}" -f $elapsed
}

# ============================================================================
# Data Collection Functions
# ============================================================================

function Get-NodePeerID {
    param([string]$Container)

    $peerIdLine = docker logs $Container 2>&1 | Select-String "node ident string|Node ID:" | Select-Object -Last 1

    if ($peerIdLine -match "(Qm[A-Za-z0-9]{44,46})") {
        return $matches[1]
    }
    return "Unknown"
}

function Get-DiscoveryStatus {
    param([string]$Container)

    $discovered = docker logs $Container 2>&1 | Select-String "\[DISCOVERY\].*Connected to peer" | Measure-Object
    $discoveredPeers = docker logs $Container 2>&1 | Select-String "Discovered Peers:" -Context 0,10 | Select-Object -Last 1

    return @{
        PeerCount = $discovered.Count
        LastDiscovery = $discoveredPeers
    }
}

function Get-MeshStatus {
    param([string]$Container)

    $meshLog = docker logs $Container 2>&1 | Select-String "Election topic has (\d+) peers in mesh" | Select-Object -Last 1

    if ($meshLog -match "(\d+) peers in mesh") {
        return [int]$matches[1]
    }
    return 0
}

function Get-ReputationStatus {
    param([string]$Container)

    # Check for reputation collection
    $repCollected = docker logs $Container 2>&1 | Select-String "Collected (\d+)/(\d+) reputation entries" | Select-Object -Last 1
    $repPublished = docker logs $Container 2>&1 | Select-String "Published reputation update" | Measure-Object
    $repStored = docker logs $Container 2>&1 | Select-String "Stored updated reputation.*NodeID:(Qm[A-Za-z0-9]+)" | Select-Object -Last 10

    $collected = 0
    $total = 0
    if ($repCollected -match "Collected (\d+)/(\d+)") {
        $collected = [int]$matches[1]
        $total = [int]$matches[2]
    }

    # Extract unique peer IDs from stored reputation
    $uniquePeers = @()
    foreach ($line in $repStored) {
        if ($line -match "NodeID:(Qm[A-Za-z0-9]+)") {
            $peerId = $matches[1]
            if ($uniquePeers -notcontains $peerId) {
                $uniquePeers += $peerId
            }
        }
    }

    return @{
        Collected = $collected
        Total = $total
        Published = $repPublished.Count
        UniqueReputations = $uniquePeers.Count
        PeerIDs = $uniquePeers
    }
}

function Get-ElectionStatus {
    param([string]$Container)

    # Get current state
    $stateLog = docker logs $Container 2>&1 | Select-String "\[DEBUG STATE\] Role=" | Select-Object -Last 1

    $role = "Unknown"
    $term = 0
    $phase = "Unknown"
    $voteCount = 0
    $votedNodesCount = 0

    if ($stateLog -match "Role=(\w+).*Term=(\d+).*Phase=(\w+).*Votes=map\[([^\]]*)\].*VotedNodes=map\[([^\]]*)\]") {
        $role = $matches[1]
        $term = [int]$matches[2]
        $phase = $matches[3]

        # Count votes
        $votesStr = $matches[4]
        if ($votesStr -ne "") {
            $voteCount = ($votesStr -split " ").Count
        }

        # Count voted nodes
        $votedNodesStr = $matches[5]
        if ($votedNodesStr -ne "") {
            $votedNodesCount = ($votedNodesStr -split " ").Count
        }
    }

    # Check if elected as coordinator
    $electedLog = docker logs $Container 2>&1 | Select-String "Elected as coordinator|ELECTION.*SUCCESSFUL|is now acting as leader" | Select-Object -Last 1
    $isElected = $electedLog -ne $null

    # Check for election attempts
    $electionAttempts = docker logs $Container 2>&1 | Select-String "Starting election.*term (\d+)" | Measure-Object

    # Check candidates
    $candidatesLog = docker logs $Container 2>&1 | Select-String "Starting initial election with (\d+) candidates" | Select-Object -Last 1
    $candidateCount = 0
    if ($candidatesLog -match "(\d+) candidates") {
        $candidateCount = [int]$matches[1]
    }

    return @{
        Role = $role
        Term = $term
        Phase = $phase
        VoteCount = $voteCount
        VotedNodesCount = $votedNodesCount
        IsElected = $isElected
        ElectionAttempts = $electionAttempts.Count
        CandidateCount = $candidateCount
        LastState = $stateLog
    }
}

function Get-HeartbeatStatus {
    param([string]$Container)

    # Sending heartbeats (coordinator behavior)
    $sending = docker logs $Container 2>&1 | Select-String "\[HEARTBEAT\] Sent heartbeat" | Select-Object -Last 1

    # Receiving heartbeats (follower behavior)
    $receiving = docker logs $Container 2>&1 | Select-String "Received.*heartbeat|heartbeat from" | Select-Object -Last 1

    # Missed heartbeats
    $missed = docker logs $Container 2>&1 | Select-String "Missed heartbeat.*time" | Select-Object -Last 1
    $missedCount = 0
    if ($missed -match "Missed heartbeat (\d+) time") {
        $missedCount = [int]$matches[1]
    }

    return @{
        SendingHeartbeats = $sending -ne $null
        ReceivingHeartbeats = $receiving -ne $null
        LastSent = $sending
        LastReceived = $receiving
        MissedCount = $missedCount
        LastMissed = $missed
    }
}

function Get-MessageFlow {
    param([string]$Container, [string]$NodePeerID)

    # Get received messages
    $receivedMessages = docker logs $Container 2>&1 | Select-String "Received message from network.*from: (Qm[A-Za-z0-9]+)" | Select-Object -Last 20

    $selfMessages = 0
    $peerMessages = 0
    $uniquePeers = @()

    foreach ($msg in $receivedMessages) {
        if ($msg -match "from: (Qm[A-Za-z0-9]+)") {
            $fromPeer = $matches[1]
            if ($fromPeer -eq $NodePeerID) {
                $selfMessages++
            } else {
                $peerMessages++
                if ($uniquePeers -notcontains $fromPeer) {
                    $uniquePeers += $fromPeer
                }
            }
        }
    }

    # Get published messages
    $published = docker logs $Container 2>&1 | Select-String "\[PUBSUB\] Published message type" | Measure-Object

    return @{
        SelfMessages = $selfMessages
        PeerMessages = $peerMessages
        UniquePeers = $uniquePeers.Count
        Published = $published.Count
        PeerList = $uniquePeers
    }
}

function Get-ErrorStatus {
    param([string]$Container)

    # Check for crashes
    $crash = docker logs $Container 2>&1 | Select-String "panic:|runtime error:" | Select-Object -Last 1

    # Check for errors
    $errors = docker logs $Container 2>&1 | Select-String "\[ERROR\]|\[CRITICAL\]|failed|error" | Select-Object -Last 10

    return @{
        HasCrash = $crash -ne $null
        CrashLog = $crash
        ErrorCount = $errors.Count
        Errors = $errors
    }
}

# ============================================================================
# Snapshot Function - Collect All Data
# ============================================================================

function Get-ClusterSnapshot {
    $timestamp = Get-Date
    $snapshot = @{
        Timestamp = $timestamp
        Elapsed = Get-ElapsedTime
        Nodes = @{}
    }

    Write-Host "[$(Get-Timestamp)] Taking cluster snapshot..." -ForegroundColor Cyan

    foreach ($container in $Containers) {
        $peerId = Get-NodePeerID -Container $container
        $discovery = Get-DiscoveryStatus -Container $container
        $mesh = Get-MeshStatus -Container $container
        $reputation = Get-ReputationStatus -Container $container
        $election = Get-ElectionStatus -Container $container
        $heartbeat = Get-HeartbeatStatus -Container $container
        $messages = Get-MessageFlow -Container $container -NodePeerID $peerId
        $errors = Get-ErrorStatus -Container $container

        $snapshot.Nodes[$container] = @{
            PeerID = $peerId
            Discovery = $discovery
            MeshSize = $mesh
            Reputation = $reputation
            Election = $election
            Heartbeat = $heartbeat
            Messages = $messages
            Errors = $errors
        }
    }

    return $snapshot
}

# ============================================================================
# Analysis Functions
# ============================================================================

function Show-SnapshotSummary {
    param($Snapshot)

    Write-Header "SNAPSHOT at $(Get-Timestamp) (Elapsed: $($Snapshot.Elapsed))" -Color "Magenta"

    # Count roles
    $coordinators = @()
    $followers = @()
    $unknown = @()

    foreach ($node in $Snapshot.Nodes.Keys) {
        $role = $Snapshot.Nodes[$node].Election.Role
        if ($role -eq "Coordinator") {
            $coordinators += $node
        } elseif ($role -eq "Follower") {
            $followers += $node
        } else {
            $unknown += $node
        }
    }

    Write-Host ""
    Write-Host "CLUSTER STATUS:" -ForegroundColor Yellow
    Write-Host "  Coordinators: $($coordinators.Count)" -ForegroundColor $(if($coordinators.Count -eq 1){"Green"}elseif($coordinators.Count -eq 0){"Yellow"}else{"Red"})
    if ($coordinators.Count -gt 0) {
        Write-Host "    → $($coordinators -join ', ')" -ForegroundColor White
    }

    Write-Host "  Followers: $($followers.Count)" -ForegroundColor Cyan
    if ($followers.Count -gt 0 -and $followers.Count -le 3) {
        Write-Host "    → $($followers -join ', ')" -ForegroundColor White
    }

    Write-Host "  Unknown: $($unknown.Count)" -ForegroundColor Gray

    # Mesh connectivity
    Write-SubHeader "MESH CONNECTIVITY"
    $totalMesh = 0
    $minMesh = 999
    $maxMesh = 0

    foreach ($node in $Snapshot.Nodes.Keys) {
        $meshSize = $Snapshot.Nodes[$node].MeshSize
        $totalMesh += $meshSize
        if ($meshSize -lt $minMesh) { $minMesh = $meshSize }
        if ($meshSize -gt $maxMesh) { $maxMesh = $meshSize }
    }

    $avgMesh = [math]::Round($totalMesh / $Containers.Count, 1)

    Write-Host "  Average mesh size: $avgMesh peers" -ForegroundColor White
    Write-Host "  Range: $minMesh - $maxMesh peers" -ForegroundColor Gray

    if ($avgMesh -lt 3) {
        Write-Host "  ⚠️  PROBLEM: Mesh too small! Need at least 4-5 peers" -ForegroundColor Red
    } elseif ($avgMesh -ge 5) {
        Write-Host "  ✅ Good mesh connectivity" -ForegroundColor Green
    } else {
        Write-Host "  ⚠️  Marginal mesh size" -ForegroundColor Yellow
    }

    # Reputation propagation
    Write-SubHeader "REPUTATION PROPAGATION"
    $goodRep = 0
    $badRep = 0

    foreach ($node in $Snapshot.Nodes.Keys) {
        $rep = $Snapshot.Nodes[$node].Reputation
        if ($rep.UniqueReputations -ge 5) {
            $goodRep++
        } else {
            $badRep++
            Write-Host "  ⚠️  $node only has $($rep.UniqueReputations) reputations (collected $($rep.Collected)/$($rep.Total))" -ForegroundColor Yellow
        }
    }

    if ($goodRep -ge 6) {
        Write-Host "  ✅ $goodRep/$($Containers.Count) nodes have sufficient reputation data" -ForegroundColor Green
    } elseif ($goodRep -gt 0) {
        Write-Host "  ⚠️  Only $goodRep/$($Containers.Count) nodes have sufficient reputation data" -ForegroundColor Yellow
    } else {
        Write-Host "  ❌ NO nodes have sufficient reputation data!" -ForegroundColor Red
        Write-Host "     This will cause elections to fail!" -ForegroundColor Red
    }

    # Message flow
    Write-SubHeader "MESSAGE FLOW"
    $goodFlow = 0
    $isolatedNodes = @()

    foreach ($node in $Snapshot.Nodes.Keys) {
        $msg = $Snapshot.Nodes[$node].Messages
        if ($msg.PeerMessages -ge 10) {
            $goodFlow++
        } else {
            $isolatedNodes += "$node (only $($msg.PeerMessages) peer msgs)"
        }
    }

    if ($goodFlow -ge 6) {
        Write-Host "  ✅ $goodFlow/$($Containers.Count) nodes receiving messages from peers" -ForegroundColor Green
    } else {
        Write-Host "  ⚠️  Only $goodFlow/$($Containers.Count) nodes receiving peer messages" -ForegroundColor Yellow
        if ($isolatedNodes.Count -gt 0 -and $isolatedNodes.Count -le 5) {
            Write-Host "     Isolated: $($isolatedNodes -join ', ')" -ForegroundColor Red
        }
    }

    # Election status
    Write-SubHeader "ELECTION STATUS"
    foreach ($node in $Snapshot.Nodes.Keys | Select-Object -First 3) {
        $elec = $Snapshot.Nodes[$node].Election
        Write-Host "  $node : Role=$($elec.Role), Term=$($elec.Term), Phase=$($elec.Phase), Candidates=$($elec.CandidateCount), Votes=$($elec.VoteCount)" -ForegroundColor Cyan
    }

    # Errors
    $totalErrors = 0
    $hasCrash = $false
    foreach ($node in $Snapshot.Nodes.Keys) {
        $err = $Snapshot.Nodes[$node].Errors
        $totalErrors += $err.ErrorCount
        if ($err.HasCrash) {
            $hasCrash = $true
            Write-Host "  ❌ $node has CRASHED!" -ForegroundColor Red
        }
    }

    if ($hasCrash) {
        Write-Host ""
        Write-Host "  ⚠️  CRITICAL: Crashes detected!" -ForegroundColor Red
    } elseif ($totalErrors -gt 20) {
        Write-Host ""
        Write-Host "  ⚠️  High error count: $totalErrors total errors" -ForegroundColor Yellow
    }
}

function Show-DetailedNodeInfo {
    param($Snapshot, [string]$NodeName)

    if (-not $Snapshot.Nodes.ContainsKey($NodeName)) {
        Write-Host "Node $NodeName not found!" -ForegroundColor Red
        return
    }

    $node = $Snapshot.Nodes[$NodeName]

    Write-Header "DETAILED INFO: $NodeName" -Color "Green"

    Write-Host ""
    Write-Host "IDENTITY:" -ForegroundColor Yellow
    Write-Host "  Peer ID: $($node.PeerID)" -ForegroundColor White

    Write-Host ""
    Write-Host "DISCOVERY:" -ForegroundColor Yellow
    Write-Host "  Discovered peers: $($node.Discovery.PeerCount)" -ForegroundColor White

    Write-Host ""
    Write-Host "MESH:" -ForegroundColor Yellow
    Write-Host "  Peers in mesh: $($node.MeshSize)" -ForegroundColor White

    Write-Host ""
    Write-Host "REPUTATION:" -ForegroundColor Yellow
    Write-Host "  Collected: $($node.Reputation.Collected)/$($node.Reputation.Total)" -ForegroundColor White
    Write-Host "  Unique reputations stored: $($node.Reputation.UniqueReputations)" -ForegroundColor White
    Write-Host "  Published: $($node.Reputation.Published) times" -ForegroundColor White

    if ($node.Reputation.PeerIDs.Count -gt 0 -and $node.Reputation.PeerIDs.Count -le 8) {
        Write-Host "  Known peers:" -ForegroundColor Gray
        foreach ($peer in $node.Reputation.PeerIDs) {
            $shortPeer = $peer.Substring(0, [math]::Min(15, $peer.Length)) + "..."
            Write-Host "    - $shortPeer" -ForegroundColor Gray
        }
    }

    Write-Host ""
    Write-Host "ELECTION:" -ForegroundColor Yellow
    Write-Host "  Role: $($node.Election.Role)" -ForegroundColor $(if($node.Election.Role -eq "Coordinator"){"Green"}else{"Cyan"})
    Write-Host "  Term: $($node.Election.Term)" -ForegroundColor White
    Write-Host "  Phase: $($node.Election.Phase)" -ForegroundColor White
    Write-Host "  Candidates: $($node.Election.CandidateCount)" -ForegroundColor White
    Write-Host "  Votes collected: $($node.Election.VoteCount)" -ForegroundColor White
    Write-Host "  Voted nodes: $($node.Election.VotedNodesCount)" -ForegroundColor White
    Write-Host "  Election attempts: $($node.Election.ElectionAttempts)" -ForegroundColor White
    Write-Host "  Elected as coordinator: $(if($node.Election.IsElected){"YES"}else{"NO"})" -ForegroundColor $(if($node.Election.IsElected){"Green"}else{"Gray"})

    Write-Host ""
    Write-Host "HEARTBEAT:" -ForegroundColor Yellow
    Write-Host "  Sending: $(if($node.Heartbeat.SendingHeartbeats){"YES"}else{"NO"})" -ForegroundColor White
    Write-Host "  Receiving: $(if($node.Heartbeat.ReceivingHeartbeats){"YES"}else{"NO"})" -ForegroundColor White
    Write-Host "  Missed: $($node.Heartbeat.MissedCount)" -ForegroundColor White

    Write-Host ""
    Write-Host "MESSAGES:" -ForegroundColor Yellow
    Write-Host "  From self: $($node.Messages.SelfMessages)" -ForegroundColor White
    Write-Host "  From peers: $($node.Messages.PeerMessages)" -ForegroundColor $(if($node.Messages.PeerMessages -gt 0){"Green"}else{"Red"})
    Write-Host "  Unique peers: $($node.Messages.UniquePeers)" -ForegroundColor White
    Write-Host "  Published: $($node.Messages.Published)" -ForegroundColor White

    if ($node.Errors.HasCrash) {
        Write-Host ""
        Write-Host "CRASH DETECTED:" -ForegroundColor Red
        Write-Host "  $($node.Errors.CrashLog)" -ForegroundColor Red
    }

    if ($node.Errors.ErrorCount -gt 0) {
        Write-Host ""
        Write-Host "ERRORS ($($node.Errors.ErrorCount)):" -ForegroundColor Yellow
        $node.Errors.Errors | Select-Object -First 3 | ForEach-Object {
            Write-Host "  $_" -ForegroundColor Red
        }
    }
}

function Show-Timeline {
    param($Snapshots)

    Write-Header "TIMELINE ANALYSIS" -Color "Blue"

    if ($Snapshots.Count -lt 2) {
        Write-Host "Not enough snapshots for timeline" -ForegroundColor Yellow
        return
    }

    Write-Host ""
    Write-Host "Time  | Coordinators | Avg Mesh | Avg Rep | Problem" -ForegroundColor Cyan
    Write-Host ("─" * 80) -ForegroundColor Gray

    foreach ($snap in $Snapshots) {
        $coordCount = 0
        $totalMesh = 0
        $totalRep = 0
        $nodes = $snap.Nodes.Keys.Count

        foreach ($node in $snap.Nodes.Keys) {
            if ($snap.Nodes[$node].Election.Role -eq "Coordinator") {
                $coordCount++
            }
            $totalMesh += $snap.Nodes[$node].MeshSize
            $totalRep += $snap.Nodes[$node].Reputation.UniqueReputations
        }

        $avgMesh = if($nodes -gt 0) { [math]::Round($totalMesh / $nodes, 1) } else { 0 }
        $avgRep = if($nodes -gt 0) { [math]::Round($totalRep / $nodes, 1) } else { 0 }

        $problem = ""
        if ($coordCount -eq 0) { $problem = "No coordinator" }
        elseif ($coordCount -gt 1) { $problem = "SPLIT-BRAIN!" }
        elseif ($avgMesh -lt 3) { $problem = "Low mesh" }
        elseif ($avgRep -lt 2) { $problem = "Low reputation" }

        $color = "White"
        if ($problem -ne "") { $color = "Yellow" }
        if ($problem -eq "SPLIT-BRAIN!") { $color = "Red" }
        if ($coordCount -eq 1 -and $avgMesh -ge 5 -and $avgRep -ge 5) { $color = "Green" }

        Write-Host ("{0,-5} | {1,12} | {2,8} | {3,7} | {4}" -f $snap.Elapsed, $coordCount, $avgMesh, $avgRep, $problem) -ForegroundColor $color
    }
}

function Show-FinalDiagnosis {
    param($Snapshots)

    Write-Header "FINAL DIAGNOSIS" -Color "Red"

    $lastSnap = $Snapshots[-1]

    # Count coordinators
    $coordCount = 0
    $coordNames = @()
    foreach ($node in $lastSnap.Nodes.Keys) {
        if ($lastSnap.Nodes[$node].Election.Role -eq "Coordinator") {
            $coordCount++
            $coordNames += $node
        }
    }

    Write-Host ""
    Write-Host "ELECTION OUTCOME:" -ForegroundColor Yellow
    if ($coordCount -eq 0) {
        Write-Host "  ❌ NO COORDINATOR ELECTED" -ForegroundColor Red
        Write-Host "     Elections are failing to complete" -ForegroundColor Red
    } elseif ($coordCount -eq 1) {
        Write-Host "  ✅ SINGLE COORDINATOR: $($coordNames[0])" -ForegroundColor Green
        Write-Host "     Cluster is healthy!" -ForegroundColor Green
    } else {
        Write-Host "  ❌ SPLIT-BRAIN DETECTED: $coordCount coordinators" -ForegroundColor Red
        Write-Host "     Coordinators: $($coordNames -join ', ')" -ForegroundColor Red
    }

    # Analyze trends
    Write-Host ""
    Write-Host "TREND ANALYSIS:" -ForegroundColor Yellow

    # Check mesh growth
    $firstMesh = 0
    $lastMesh = 0
    foreach ($node in $Snapshots[0].Nodes.Keys) {
        $firstMesh += $Snapshots[0].Nodes[$node].MeshSize
    }
    foreach ($node in $lastSnap.Nodes.Keys) {
        $lastMesh += $lastSnap.Nodes[$node].MeshSize
    }
    $avgFirstMesh = [math]::Round($firstMesh / $Containers.Count, 1)
    $avgLastMesh = [math]::Round($lastMesh / $Containers.Count, 1)

    Write-Host "  Mesh size: $avgFirstMesh → $avgLastMesh" -ForegroundColor White
    if ($avgLastMesh -ge 5) {
        Write-Host "    ✅ Mesh is well-formed" -ForegroundColor Green
    } elseif ($avgLastMesh -ge 3) {
        Write-Host "    ⚠️  Mesh is marginal" -ForegroundColor Yellow
    } else {
        Write-Host "    ❌ Mesh is too small" -ForegroundColor Red
    }

    # Check reputation
    $totalRep = 0
    $goodRep = 0
    foreach ($node in $lastSnap.Nodes.Keys) {
        $rep = $lastSnap.Nodes[$node].Reputation.UniqueReputations
        $totalRep += $rep
        if ($rep -ge 5) { $goodRep++ }
    }
    $avgRep = [math]::Round($totalRep / $Containers.Count, 1)

    Write-Host "  Reputation: $avgRep avg unique reputations" -ForegroundColor White
    if ($goodRep -ge 6) {
        Write-Host "    ✅ Reputation propagating well" -ForegroundColor Green
    } elseif ($goodRep -gt 0) {
        Write-Host "    ⚠️  Reputation propagation incomplete" -ForegroundColor Yellow
    } else {
        Write-Host "    ❌ Reputation NOT propagating" -ForegroundColor Red
    }

    # Root cause
    Write-Host ""
    Write-Host "ROOT CAUSE:" -ForegroundColor Yellow

    if ($coordCount -eq 0) {
        if ($avgLastMesh -lt 3) {
            Write-Host "  → Mesh connectivity too low" -ForegroundColor Red
            Write-Host "     Messages cannot propagate to all nodes" -ForegroundColor Red
        } elseif ($avgRep -lt 2) {
            Write-Host "  → Reputation not propagating" -ForegroundColor Red
            Write-Host "     Nodes don't have enough data for elections" -ForegroundColor Red
        } else {
            Write-Host "  → Election logic may have issues" -ForegroundColor Red
            Write-Host "     Mesh and reputation OK but no election" -ForegroundColor Red
        }
    } elseif ($coordCount -gt 1) {
        Write-Host "  → Split-brain: Messages not reaching all nodes" -ForegroundColor Red
        Write-Host "     Each node running independent election" -ForegroundColor Red
    } else {
        Write-Host "  → System is working correctly!" -ForegroundColor Green
    }

    Write-Host ""
    Write-Host "RECOMMENDATIONS:" -ForegroundColor Yellow

    if ($avgLastMesh -lt 4) {
        Write-Host "  1. Increase GossipSub mesh parameters (D, Dhi)" -ForegroundColor White
    }
    if ($avgRep -lt 3) {
        Write-Host "  2. Check reputation message publishing/receiving" -ForegroundColor White
    }
    if ($coordCount -eq 0) {
        Write-Host "  3. Check election trigger conditions" -ForegroundColor White
    }
    if ($coordCount -gt 1) {
        Write-Host "  4. Check peer discovery and libp2p connectivity" -ForegroundColor White
    }
}

# ============================================================================
# Main Monitoring Loop
# ============================================================================

Write-Header "OptimusDB Cluster Monitor Starting" -Color "Cyan"
Write-Host "Duration: $Duration seconds (checking every $Interval seconds)" -ForegroundColor White
Write-Host "Containers: $($Containers -join ', ')" -ForegroundColor Gray
Write-Host ""
Write-Host "Press Ctrl+C to stop early" -ForegroundColor Yellow
Write-Host ""

$iterations = [math]::Ceiling($Duration / $Interval)
$currentIteration = 0

while ($currentIteration -lt $iterations) {
    $currentIteration++

    # Take snapshot
    $snapshot = Get-ClusterSnapshot
    $script:Snapshots += $snapshot

    # Show summary
    Show-SnapshotSummary -Snapshot $snapshot

    # Show detailed info for first node every 3rd iteration
    if ($currentIteration % 3 -eq 0) {
        Show-DetailedNodeInfo -Snapshot $snapshot -NodeName $Containers[0]
    }

    # Wait for next interval (unless last iteration)
    if ($currentIteration -lt $iterations) {
        Write-Host ""
        Write-Host ("[{0}] Waiting {1} seconds until next check..." -f (Get-Timestamp), $Interval) -ForegroundColor Gray
        Start-Sleep -Seconds $Interval
    }
}

# ============================================================================
# Final Analysis
# ============================================================================

Write-Host ""
Write-Host ""
Show-Timeline -Snapshots $script:Snapshots
Write-Host ""
Show-FinalDiagnosis -Snapshots $script:Snapshots

Write-Host ""
Write-Host ("═" * 80) -ForegroundColor Cyan
Write-Host " Monitoring Complete" -ForegroundColor Cyan
Write-Host ("═" * 80) -ForegroundColor Cyan
Write-Host ""
Write-Host "Total snapshots collected: $($script:Snapshots.Count)" -ForegroundColor White
Write-Host "Total duration: $(Get-ElapsedTime)" -ForegroundColor White
Write-Host ""