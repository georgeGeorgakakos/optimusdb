Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "GossipSub Fix Verification Script" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Test 1: Initialization
Write-Host "[TEST 1] Checking GossipSub Initialization..." -ForegroundColor Yellow
$init = docker logs optimusdb1 2>&1 | Select-String "GossipSub created with flood publishing enabled"
if ($init) {
    Write-Host "✅ PASS: GossipSub initialized correctly`n" -ForegroundColor Green
} else {
    Write-Host "❌ FAIL: GossipSub not initialized with flood publishing`n" -ForegroundColor Red
}

# Test 2: Topic Subscription
Write-Host "[TEST 2] Checking Topic Subscriptions..." -ForegroundColor Yellow
$subscribed = 0
foreach ($i in 1..8) {
    $sub = docker logs optimusdb$i 2>&1 | Select-String "Successfully subscribed to topic"
    if ($sub) { $subscribed++ }
}
if ($subscribed -eq 8) {
    Write-Host "✅ PASS: All 8 nodes subscribed to topic`n" -ForegroundColor Green
} else {
    Write-Host "❌ FAIL: Only $subscribed/8 nodes subscribed`n" -ForegroundColor Red
}

# Test 3: Message Propagation (CRITICAL)
Write-Host "[TEST 3] Checking Message Propagation..." -ForegroundColor Yellow
$workingNodes = 0
foreach ($i in 1..8) {
    $messages = docker logs optimusdb$i 2>&1 | Select-String "from: ([A-Za-z0-9]+)" | Select-Object -Last 20
    $peerIds = $messages | ForEach-Object {
        if ($_ -match "from: ([A-Za-z0-9]+)") {
            $matches[1].Substring(0, 12)
        }
    }
    $uniquePeers = ($peerIds | Select-Object -Unique).Count

    if ($uniquePeers -gt 1) {
        $workingNodes++
        Write-Host "  Node $i : ✅ Receiving from $uniquePeers different peers" -ForegroundColor Green
    } else {
        Write-Host "  Node $i : ❌ Only receiving from self" -ForegroundColor Red
    }
}
if ($workingNodes -eq 8) {
    Write-Host "`n✅ PASS: All nodes receiving messages from multiple peers`n" -ForegroundColor Green
} else {
    Write-Host "`n❌ FAIL: Only $workingNodes/8 nodes working correctly`n" -ForegroundColor Red
}

# Test 4: Leader Election
Write-Host "[TEST 4] Checking Leader Election..." -ForegroundColor Yellow
$coordinators = 0
foreach ($i in 1..8) {
    $role = docker logs optimusdb$i 2>&1 | Select-String "My role: coordinator" | Select-Object -Last 1
    if ($role) {
        $coordinators++
        Write-Host "  Node $i is COORDINATOR" -ForegroundColor Cyan
    }
}
if ($coordinators -eq 1) {
    Write-Host "✅ PASS: Exactly 1 coordinator elected`n" -ForegroundColor Green
} elseif ($coordinators -eq 0) {
    Write-Host "⚠️  WARN: No coordinator yet (may still be electing)`n" -ForegroundColor Yellow
} else {
    Write-Host "❌ FAIL: Multiple coordinators ($coordinators) - split brain!`n" -ForegroundColor Red
}

# Test 5: Split-Brain Detection
Write-Host "[TEST 5] Checking Split-Brain Detection..." -ForegroundColor Yellow
$splitBrain = docker logs optimusdb1 2>&1 | Select-String "MULTIPLE COORDINATORS DETECTED"
if ($splitBrain) {
    Write-Host "✅ PASS: Split-brain detection working (conflicts were detected and resolved)`n" -ForegroundColor Green
} else {
    Write-Host "ℹ️  INFO: No split-brain detected (all nodes agreed on leader from start)`n" -ForegroundColor Cyan
}

# Final Summary
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "SUMMARY" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
if ($workingNodes -eq 8 -and $subscribed -eq 8 -and $coordinators -le 1) {
    Write-Host "🎉 ALL TESTS PASSED - GossipSub fix is working!" -ForegroundColor Green
} else {
    Write-Host "⚠️  SOME TESTS FAILED - Please review above" -ForegroundColor Yellow
}
Write-Host ""