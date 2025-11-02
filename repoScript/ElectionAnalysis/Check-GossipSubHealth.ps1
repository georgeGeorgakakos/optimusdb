# ============================================================================
# GossipSub Mesh Health Check Script - Windows PowerShell Version
# Save as: Check-GossipSubHealth.ps1
# Usage: .\Check-GossipSubHealth.ps1
# ============================================================================

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         GossipSub Mesh Health Check for OptimusDB            ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$PassCount = 0
$FailCount = 0
$WarnCount = 0

# ============================================================================
# Test 1: GossipSub Initialization
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 1: Checking GossipSub Initialization" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$InitCount = 0
$NodeDirs = Get-ChildItem -Directory -Filter "node-*" -ErrorAction SilentlyContinue

foreach ($nodeDir in $NodeDirs) {
    $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
    if (Test-Path $logFile) {
        $matches = Select-String -Path $logFile -Pattern "GossipSub created with.*mesh" -ErrorAction SilentlyContinue
        if ($matches) {
            $InitCount++
        }
    }
}

Write-Host "Nodes with mesh initialization: $InitCount/8"

if ($InitCount -eq 8) {
    Write-Host "✅ PASS: All nodes initialized GossipSub mesh" -ForegroundColor Green
    $PassCount++
} elseif ($InitCount -gt 0) {
    Write-Host "⚠️  WARNING: Only $InitCount nodes have mesh initialization" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: No nodes have mesh initialization" -ForegroundColor Red
    Write-Host "   → Check if new code is deployed" -ForegroundColor Red
    $FailCount++
}

# Show which nodes initialized
if ($InitCount -gt 0) {
    foreach ($nodeDir in $NodeDirs) {
        $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
        if (Test-Path $logFile) {
            $matches = Select-String -Path $logFile -Pattern "GossipSub created with.*mesh" -ErrorAction SilentlyContinue
            if ($matches) {
                Write-Host "   ✓ $($nodeDir.Name)" -ForegroundColor Gray
            }
        }
    }
}
Write-Host ""

# ============================================================================
# Test 2: Mesh Formation (GRAFT messages)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 2: Checking Mesh Formation (GRAFT messages)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$GraftCount = 0
foreach ($nodeDir in $NodeDirs) {
    $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
    if (Test-Path $logFile) {
        $matches = Select-String -Path $logFile -Pattern "graft" -AllMatches -ErrorAction SilentlyContinue |
                Where-Object { $_.Line -notmatch "OpportunisticGraft" }
        $GraftCount += $matches.Count
    }
}

Write-Host "Total GRAFT messages detected: $GraftCount"

if ($GraftCount -gt 15) {
    Write-Host "✅ PASS: Mesh formation is active" -ForegroundColor Green
    $PassCount++
} elseif ($GraftCount -gt 5) {
    Write-Host "⚠️  WARNING: Low GRAFT activity ($GraftCount messages)" -ForegroundColor Yellow
    Write-Host "   → Mesh may still be forming, wait 60 seconds" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: No mesh formation detected" -ForegroundColor Red
    Write-Host "   → Enable debug logging: `$env:GOLOG_LOG_LEVEL='debug'" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# Test 3: Coordinator Election (CRITICAL!)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 3: Checking Coordinator Election (MOST IMPORTANT!)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$CoordinatorCount = 0
$CoordinatorNodes = @()

foreach ($nodeDir in $NodeDirs) {
    $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
    if (Test-Path $logFile) {
        $matches = Select-String -Path $logFile -Pattern "Elected as coordinator" -ErrorAction SilentlyContinue
        if ($matches) {
            $CoordinatorCount++
            $CoordinatorNodes += $nodeDir.Name
        }
    }
}

Write-Host "Coordinators elected: $CoordinatorCount"

if ($CoordinatorCount -eq 1) {
    Write-Host "✅ PASS: Single coordinator elected (NO SPLIT-BRAIN!)" -ForegroundColor Green
    Write-Host "   → Coordinator: $($CoordinatorNodes[0])" -ForegroundColor Green
    $PassCount++
} elseif ($CoordinatorCount -eq 0) {
    Write-Host "⚠️  WARNING: No coordinator elected yet" -ForegroundColor Yellow
    Write-Host "   → Election may still be in progress, wait 30 seconds" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: SPLIT-BRAIN DETECTED! ($CoordinatorCount coordinators)" -ForegroundColor Red
    Write-Host "   Multiple coordinators found:" -ForegroundColor Red
    foreach ($node in $CoordinatorNodes) {
        Write-Host "   - $node" -ForegroundColor Red
    }
    Write-Host "   → This is the problem you're trying to fix!" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# Test 4: Peer Connectivity
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 4: Checking Peer Connectivity" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$LowPeerCount = 0
for ($i = 1; $i -le 8; $i++) {
    $logFile = "node-$i\logs\latest.log"
    if (Test-Path $logFile) {
        $peerMatches = Select-String -Path $logFile -Pattern "Connected to peer|Discovered peer" -ErrorAction SilentlyContinue
        $peerCount = $peerMatches.Count

        if ($peerCount -ge 3) {
            Write-Host "   ✓ node-$i : $peerCount peers" -ForegroundColor Green
        } elseif ($peerCount -gt 0) {
            Write-Host "   ⚠ node-$i : $peerCount peers (low)" -ForegroundColor Yellow
            $LowPeerCount++
        } else {
            Write-Host "   ✗ node-$i : $peerCount peers (isolated!)" -ForegroundColor Red
            $LowPeerCount++
        }
    } else {
        Write-Host "   ✗ node-$i : Log file not found" -ForegroundColor Red
        $LowPeerCount++
    }
}

if ($LowPeerCount -eq 0) {
    Write-Host "✅ PASS: All nodes have good peer connectivity" -ForegroundColor Green
    $PassCount++
} elseif ($LowPeerCount -le 2) {
    Write-Host "⚠️  WARNING: $LowPeerCount nodes have low peer count" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: $LowPeerCount nodes have connectivity issues" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# Test 5: Message Propagation
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 5: Checking Message Propagation" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$MsgCount = 0
foreach ($nodeDir in $NodeDirs) {
    $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
    if (Test-Path $logFile) {
        $matches = Select-String -Path $logFile -Pattern "Received.*from peer" -ErrorAction SilentlyContinue
        $MsgCount += $matches.Count
    }
}

Write-Host "Total messages received from peers: $MsgCount"

if ($MsgCount -gt 30) {
    Write-Host "✅ PASS: Messages are propagating across network" -ForegroundColor Green
    $PassCount++
} elseif ($MsgCount -gt 10) {
    Write-Host "⚠️  WARNING: Low message activity ($MsgCount messages)" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: Very low or no message propagation" -ForegroundColor Red
    Write-Host "   → Messages may not be reaching other nodes" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# Test 6: Discovery PubSub (Optional)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 6: Checking Discovery PubSub (Optional)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$DiscoveryPubSubCount = 0
$DiscoveryMeshCount = 0

foreach ($nodeDir in $NodeDirs) {
    $logFile = Join-Path $nodeDir.FullName "logs\latest.log"
    if (Test-Path $logFile) {
        $discoveryMatches = Select-String -Path $logFile -Pattern "Enabling PubSub-based discovery" -ErrorAction SilentlyContinue
        if ($discoveryMatches) {
            $DiscoveryPubSubCount++
        }

        $meshMatches = Select-String -Path $logFile -Pattern "DISCOVERY.*GossipSub created" -ErrorAction SilentlyContinue
        if ($meshMatches) {
            $DiscoveryMeshCount++
        }
    }
}

if ($DiscoveryPubSubCount -gt 0) {
    Write-Host "PubSub discovery is ENABLED on $DiscoveryPubSubCount nodes" -ForegroundColor Cyan
    if ($DiscoveryMeshCount -gt 0) {
        Write-Host "✅ Discovery mesh initialized on $DiscoveryMeshCount nodes" -ForegroundColor Green
    } else {
        Write-Host "⚠️  Discovery enabled but mesh not initialized" -ForegroundColor Yellow
    }
} else {
    Write-Host "ℹ️  PubSub discovery is DISABLED (this is normal)" -ForegroundColor Cyan
    Write-Host "   Most setups use mDNS discovery only" -ForegroundColor Gray
}
Write-Host ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                        FINAL SUMMARY                          ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$TotalTests = $PassCount + $FailCount + $WarnCount
Write-Host "Tests Run: $TotalTests"
Write-Host "Passed: $PassCount" -ForegroundColor Green
Write-Host "Warnings: $WarnCount" -ForegroundColor Yellow
Write-Host "Failed: $FailCount" -ForegroundColor Red
Write-Host ""

if ($FailCount -eq 0 -and $PassCount -ge 4) {
    Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║  ✅ ALL CRITICAL TESTS PASSED!                               ║" -ForegroundColor Green
    Write-Host "║  Your GossipSub mesh is working correctly!                   ║" -ForegroundColor Green
    Write-Host "║  Split-brain problem is SOLVED! 🎉                           ║" -ForegroundColor Green
    Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Green
    exit 0
} elseif ($FailCount -eq 0 -and $WarnCount -gt 0) {
    Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Yellow
    Write-Host "║  ⚠️  TESTS PASSED WITH WARNINGS                              ║" -ForegroundColor Yellow
    Write-Host "║  System is functional but may need attention                 ║" -ForegroundColor Yellow
    Write-Host "║  Review warnings above                                       ║" -ForegroundColor Yellow
    Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Yellow
    exit 0
} else {
    Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Red
    Write-Host "║  ❌ SOME TESTS FAILED                                        ║" -ForegroundColor Red
    Write-Host "║  Review failed tests above and apply fixes                   ║" -ForegroundColor Red
    Write-Host "║  Most critical: Coordinator election test                    ║" -ForegroundColor Red
    Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Red
    exit 1
}