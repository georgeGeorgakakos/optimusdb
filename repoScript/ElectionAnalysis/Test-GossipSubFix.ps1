# ============================================================================
# GossipSub Ultra-Safe Fix Testing Script - Windows PowerShell
# Save as: Test-GossipSubFix.ps1
# Usage: .\Test-GossipSubFix.ps1
# ============================================================================

Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║        Testing GossipSub Ultra-Safe Fix                      ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$PassCount = 0
$FailCount = 0
$WarnCount = 0

# ============================================================================
# TEST 1: Check for Panic Crashes
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 1: Checking for Panic Crashes (divide by zero)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$PanicCount = 0
$Containers = docker ps -q 2>$null

if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "panic.*divide by zero"
        $PanicCount += $logs.Count
    }
}

Write-Host "Panic crashes found: $PanicCount"

if ($PanicCount -eq 0) {
    Write-Host "✅ PASS: No divide-by-zero crashes detected!" -ForegroundColor Green
    $PassCount++
} else {
    Write-Host "❌ FAIL: Found $PanicCount crash(es)" -ForegroundColor Red
    Write-Host "   → Code was not updated correctly" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# TEST 2: Check GossipSub Initialization
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 2: Checking GossipSub Initialization" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$GossipCount = 0
if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "GossipSub created with default"
        if ($logs) {
            $GossipCount++
        }
    }
}

$TotalContainers = ($Containers | Measure-Object).Count
Write-Host "Nodes with GossipSub initialized: $GossipCount/$TotalContainers"

if ($GossipCount -eq $TotalContainers -and $TotalContainers -gt 0) {
    Write-Host "✅ PASS: All nodes initialized GossipSub with defaults" -ForegroundColor Green
    $PassCount++
} elseif ($GossipCount -gt 0) {
    Write-Host "⚠️  WARNING: Only $GossipCount/$TotalContainers nodes initialized" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: No GossipSub initialization detected" -ForegroundColor Red
    $FailCount++
}

# Show which containers initialized
if ($GossipCount -gt 0) {
    foreach ($container in $Containers) {
        $containerName = (docker inspect --format='{{.Name}}' $container 2>$null).TrimStart('/')
        $logs = docker logs $container 2>&1 | Select-String "GossipSub created with default"
        if ($logs) {
            Write-Host "   ✓ $containerName" -ForegroundColor Gray
        }
    }
}
Write-Host ""

# ============================================================================
# TEST 3: Check Coordinator Election (CRITICAL!)
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 3: Checking Coordinator Election (MOST IMPORTANT!)" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$CoordinatorCount = 0
$CoordinatorContainers = @()

if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "Elected as coordinator"
        if ($logs) {
            $CoordinatorCount++
            $containerName = (docker inspect --format='{{.Name}}' $container 2>$null).TrimStart('/')
            $CoordinatorContainers += $containerName
        }
    }
}

Write-Host "Coordinators elected: $CoordinatorCount"

if ($CoordinatorCount -eq 1) {
    Write-Host "✅ PASS: Single coordinator elected (NO SPLIT-BRAIN!)" -ForegroundColor Green
    Write-Host "   → Coordinator: $($CoordinatorContainers[0])" -ForegroundColor Green
    $PassCount++
} elseif ($CoordinatorCount -eq 0) {
    Write-Host "⚠️  WARNING: No coordinator elected yet" -ForegroundColor Yellow
    Write-Host "   → Election may still be in progress, wait 30s and re-run" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: SPLIT-BRAIN DETECTED! ($CoordinatorCount coordinators)" -ForegroundColor Red
    Write-Host "   Multiple coordinators found:" -ForegroundColor Red
    foreach ($coord in $CoordinatorContainers) {
        Write-Host "   - $coord" -ForegroundColor Red
    }
    Write-Host "   → The ultra-safe fix did not solve split-brain" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# TEST 4: Check Peer Connectivity
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 4: Checking Peer Connectivity" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$PeerCount = 0
if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "discovered peer|connected to peer"
        $PeerCount += $logs.Count
    }
}

Write-Host "Total peer connections detected: $PeerCount"

if ($PeerCount -gt 20) {
    Write-Host "✅ PASS: Nodes discovering peers successfully" -ForegroundColor Green
    $PassCount++
} elseif ($PeerCount -gt 5) {
    Write-Host "⚠️  WARNING: Low peer connectivity ($PeerCount connections)" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: Very low or no peer connectivity" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# TEST 5: Check Message Propagation
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 5: Checking Message Propagation" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$MessageCount = 0
if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "Received.*from peer|vote"
        $MessageCount += $logs.Count
    }
}

Write-Host "Total messages propagated: $MessageCount"

if ($MessageCount -gt 10) {
    Write-Host "✅ PASS: Messages propagating across network" -ForegroundColor Green
    $PassCount++
} elseif ($MessageCount -gt 3) {
    Write-Host "⚠️  WARNING: Low message activity ($MessageCount messages)" -ForegroundColor Yellow
    $WarnCount++
} else {
    Write-Host "❌ FAIL: Very low or no message propagation" -ForegroundColor Red
    $FailCount++
}
Write-Host ""

# ============================================================================
# TEST 6: Check for Default Mesh Parameters
# ============================================================================
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "TEST 6: Verifying Ultra-Safe Configuration" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray

$DefaultMeshCount = 0
if ($Containers) {
    foreach ($container in $Containers) {
        $logs = docker logs $container 2>&1 | Select-String "default mesh.*D=6|crash-safe"
        if ($logs) {
            $DefaultMeshCount++
        }
    }
}

Write-Host "Nodes using ultra-safe defaults: $DefaultMeshCount/$TotalContainers"

if ($DefaultMeshCount -gt 0) {
    Write-Host "✅ Ultra-safe configuration detected" -ForegroundColor Green
} else {
    Write-Host "ℹ️  Cannot confirm ultra-safe config from logs" -ForegroundColor Cyan
}
Write-Host ""

# ============================================================================
# FINAL SUMMARY
# ============================================================================
Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║                     FINAL RESULTS                             ║" -ForegroundColor Cyan
Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
Write-Host ""

$TotalTests = $PassCount + $FailCount + $WarnCount
Write-Host "Tests Run: $TotalTests"
Write-Host "Passed: $PassCount" -ForegroundColor Green
Write-Host "Warnings: $WarnCount" -ForegroundColor Yellow
Write-Host "Failed: $FailCount" -ForegroundColor Red
Write-Host ""

if ($FailCount -eq 0 -and $PassCount -ge 4 -and $CoordinatorCount -eq 1) {
    Write-Host "╔═══════════════════════════════════════════════════════════════╗" -ForegroundColor Green
    Write-Host "║  ✅ ALL CRITICAL TESTS PASSED!                               ║" -ForegroundColor Green
    Write-Host "║                                                               ║" -ForegroundColor Green
    Write-Host "║  - No crashes detected                                        ║" -ForegroundColor Green
    Write-Host "║  - GossipSub initialized with safe defaults                  ║" -ForegroundColor Green
    Write-Host "║  - Single coordinator elected                                ║" -ForegroundColor Green
    Write-Host "║  - Split-brain problem is SOLVED! 🎉                         ║" -ForegroundColor Green
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
    Write-Host "║                                                               ║" -ForegroundColor Red
    if ($PanicCount -gt 0) {
        Write-Host "║  → Still seeing crashes: Code not updated correctly          ║" -ForegroundColor Red
    }
    if ($CoordinatorCount -gt 1) {
        Write-Host "║  → Split-brain still exists: $CoordinatorCount coordinators detected    ║" -ForegroundColor Red
    }
    Write-Host "╚═══════════════════════════════════════════════════════════════╝" -ForegroundColor Red
    exit 1
}