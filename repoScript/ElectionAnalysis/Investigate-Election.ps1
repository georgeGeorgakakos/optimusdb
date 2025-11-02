# ============================================================================
# Election Investigation Script - Find the Real Problem
# Save as: Investigate-Election.ps1
# Usage: .\Investigate-Election.ps1
# ============================================================================

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║           ELECTION INVESTIGATION - Deep Dive                  ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$Containers = docker ps --format "{{.Names}}"

# ============================================================================
# INVESTIGATION 1: Check Initial State (Should be FOLLOWER)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 1: Checking Initial State (All should start as FOLLOWER)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    Write-Host "Container: $container" -ForegroundColor Cyan

    # Check if node starts as follower
    $follower = docker logs $container 2>&1 | Select-String "State.*FOLLOWER|Starting as follower|Initial state.*FOLLOWER" | Select-Object -First 5
    if ($follower) {
        Write-Host "  ✅ Started as FOLLOWER" -ForegroundColor Green
        $follower | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
    } else {
        Write-Host "  ⚠️  No FOLLOWER state detected" -ForegroundColor Yellow
    }
    Write-Host ""
}

# ============================================================================
# INVESTIGATION 2: Track Election Timeline
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 2: Election Timeline (What happens during election?)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    Write-Host "Container: $container" -ForegroundColor Cyan

    # Get election-related logs in chronological order
    $electionLogs = docker logs $container 2>&1 | Select-String "election|vote|candidate|coordinator|follower|leader" | Select-Object -First 20

    if ($electionLogs) {
        $electionLogs | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    } else {
        Write-Host "  ⚠️  No election logs found" -ForegroundColor Yellow
    }
    Write-Host ""
}

# ============================================================================
# INVESTIGATION 3: Count Total Coordinators Ever Elected
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 3: How Many Times Was 'Elected as coordinator' Logged?" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

$totalElections = 0
$electionsByContainer = @{}

foreach ($container in $Containers) {
    $elections = docker logs $container 2>&1 | Select-String "Elected as coordinator"
    $count = $elections.Count
    $totalElections += $count
    $electionsByContainer[$container] = $count

    if ($count -gt 0) {
        Write-Host "Container: $container" -ForegroundColor Cyan
        Write-Host "  Times elected: $count" -ForegroundColor $(if($count -eq 1){"Green"}else{"Red"})

        # Show timestamps of elections
        $elections | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
        Write-Host ""
    }
}

Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TOTAL 'Elected as coordinator' messages: $totalElections" -ForegroundColor $(if($totalElections -eq 1){"Green"}elseif($totalElections -eq 0){"Yellow"}else{"Red"})
Write-Host ""

# ============================================================================
# INVESTIGATION 4: Current Active Coordinators
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 4: Which Nodes Think They Are Coordinator NOW?" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

$currentCoordinators = @()

foreach ($container in $Containers) {
    # Look for the LAST state transition
    $lastCoordLog = docker logs $container 2>&1 | Select-String "Elected as coordinator|became coordinator|coordinator state" | Select-Object -Last 1
    $lastFollowerLog = docker logs $container 2>&1 | Select-String "became follower|stepping down|demoted" | Select-Object -Last 1

    Write-Host "Container: $container" -ForegroundColor Cyan

    if ($lastCoordLog) {
        Write-Host "  Last coordinator log:" -ForegroundColor Yellow
        Write-Host "    $lastCoordLog" -ForegroundColor Gray
        $currentCoordinators += $container
    }

    if ($lastFollowerLog) {
        Write-Host "  Last follower log:" -ForegroundColor Yellow
        Write-Host "    $lastFollowerLog" -ForegroundColor Gray
    }

    if (-not $lastCoordLog -and -not $lastFollowerLog) {
        Write-Host "  ⚠️  No state information found" -ForegroundColor Yellow
    }
    Write-Host ""
}

# ============================================================================
# INVESTIGATION 5: Check for Multiple Elections (Re-elections)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 5: Are There Multiple Elections Happening?" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    $allElections = docker logs $container 2>&1 | Select-String "Elected as coordinator"

    if ($allElections.Count -gt 1) {
        Write-Host "Container: $container - ELECTED MULTIPLE TIMES!" -ForegroundColor Red
        Write-Host "  Election count: $($allElections.Count)" -ForegroundColor Red
        $allElections | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
        Write-Host ""
    }
}

# ============================================================================
# INVESTIGATION 6: Check Election Service Initialization
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 6: Is Election Service Starting Properly?" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    Write-Host "Container: $container" -ForegroundColor Cyan

    $electionStart = docker logs $container 2>&1 | Select-String "RunFullNode|Election.*start|Starting election service" | Select-Object -First 3

    if ($electionStart) {
        Write-Host "  ✅ Election service started" -ForegroundColor Green
        $electionStart | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
    } else {
        Write-Host "  ❌ NO election service initialization found!" -ForegroundColor Red
        Write-Host "     This is the problem - election code may not be running!" -ForegroundColor Red
    }
    Write-Host ""
}

# ============================================================================
# INVESTIGATION 7: Check for Heartbeats (Coordinator Proof of Life)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 7: Are Coordinators Sending Heartbeats?" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    $heartbeats = docker logs $container 2>&1 | Select-String "sending heartbeat|heartbeat.*coordinator" | Select-Object -Last 5

    if ($heartbeats) {
        Write-Host "Container: $container" -ForegroundColor Cyan
        Write-Host "  ✅ Sending heartbeats (is coordinator)" -ForegroundColor Green
        $heartbeats | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
        Write-Host ""
    }
}

# ============================================================================
# INVESTIGATION 8: Check Who Receives Heartbeats (Followers)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host "INVESTIGATION 8: Who Is Receiving Heartbeats? (Should be followers)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Yellow
Write-Host ""

foreach ($container in $Containers) {
    $receivedHeartbeats = docker logs $container 2>&1 | Select-String "received heartbeat|heartbeat from" | Select-Object -Last 5

    if ($receivedHeartbeats) {
        Write-Host "Container: $container" -ForegroundColor Cyan
        Write-Host "  ✅ Receiving heartbeats (is follower)" -ForegroundColor Green
        $receivedHeartbeats | ForEach-Object { Write-Host "     $_" -ForegroundColor Gray }
        Write-Host ""
    }
}

# ============================================================================
# FINAL DIAGNOSIS
# ============================================================================
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                        DIAGNOSIS                              ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

Write-Host "Total 'Elected as coordinator' messages found: $totalElections" -ForegroundColor $(if($totalElections -eq 1){"Green"}elseif($totalElections -eq 0){"Yellow"}else{"Red"})
Write-Host "Containers that were elected: $($electionsByContainer.Keys.Count)" -ForegroundColor $(if($electionsByContainer.Keys.Count -le 1){"Green"}else{"Red"})
Write-Host ""

if ($totalElections -eq 0) {
    Write-Host "❌ PROBLEM: No elections happening!" -ForegroundColor Red
    Write-Host "   → Election service may not be starting" -ForegroundColor Red
    Write-Host "   → Check INVESTIGATION 6 above" -ForegroundColor Red
} elseif ($totalElections -eq 1) {
    Write-Host "✅ PERFECT: Single coordinator elected" -ForegroundColor Green
    Write-Host "   Split-brain is SOLVED!" -ForegroundColor Green
} elseif ($totalElections -gt 1 -and $totalElections -le 8) {
    Write-Host "❌ SPLIT-BRAIN CONFIRMED: Multiple coordinators elected" -ForegroundColor Red
    Write-Host "   → $totalElections different nodes were elected" -ForegroundColor Red
    Write-Host "   → This means GossipSub is NOT propagating messages properly" -ForegroundColor Red
} else {
    Write-Host "⚠️  UNUSUAL: More than 8 election events detected" -ForegroundColor Yellow
    Write-Host "   → Multiple re-elections may be occurring" -ForegroundColor Yellow
    Write-Host "   → Network may be unstable" -ForegroundColor Yellow
}

Write-Host ""
Write-Host "Press any key to exit..."
$null = $Host.UI.RawUI.ReadKey("NoEcho,IncludeKeyDown")