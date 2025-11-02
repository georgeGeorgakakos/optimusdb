#!/usr/bin/env pwsh
# Emergency Split-Brain Diagnostic for Docker Setup

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Emergency Split-Brain Diagnostic" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$containers = @("optimusdb1", "optimusdb2", "optimusdb3", "optimusdb4", "optimusdb5", "optimusdb6", "optimusdb7", "optimusdb8")

Write-Host "[1] Checking if split-brain detection is triggering..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

$splitBrainCount = 0
foreach ($container in $containers) {
    $result = docker logs --tail 100 $container 2>$null | Select-String "SPLIT-BRAIN"
    if ($result) {
        Write-Host "✓ $container detected split-brain:" -ForegroundColor Green
        $result | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
        $splitBrainCount++
    }
}

if ($splitBrainCount -eq 0) {
    Write-Host "❌ CRITICAL: No split-brain detection found in ANY container!" -ForegroundColor Red
    Write-Host "   This means the split-brain detection code is not working." -ForegroundColor Red
    Write-Host ""
}

Write-Host ""
Write-Host "[2] Checking heartbeat messages and terms..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

foreach ($container in $containers) {
    Write-Host "`n$container heartbeats:" -ForegroundColor Cyan
    docker logs --tail 20 $container 2>$null | Select-String "HEARTBEAT" | Select-Object -Last 3 | ForEach-Object {
        Write-Host "  $_" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "[3] Checking election terms..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

foreach ($container in $containers) {
    $terms = docker logs --tail 50 $container 2>$null | Select-String "term [0-9]+" | Select-Object -Last 2
    if ($terms) {
        Write-Host "$container terms:" -ForegroundColor Cyan
        $terms | ForEach-Object { Write-Host "  $_" -ForegroundColor Gray }
    }
}

Write-Host ""
Write-Host "[4] Checking who thinks they're coordinator..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

foreach ($container in $containers) {
    $isCoord = docker logs --tail 100 $container 2>$null | Select-String "is now Coordinator" | Select-Object -Last 1
    if ($isCoord) {
        Write-Host "★ $container : $isCoord" -ForegroundColor Yellow
    }
}

Write-Host ""
Write-Host "[5] Checking message handling..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

Write-Host "Checking if TypeHeartbeat messages are being received..." -ForegroundColor Cyan
$hbReceived = 0
foreach ($container in $containers) {
    $result = docker logs --tail 50 $container 2>$null | Select-String "case TypeHeartbeat"
    if ($result) {
        $hbReceived++
    }
}
Write-Host "Containers processing TypeHeartbeat: $hbReceived/8" -ForegroundColor $(if($hbReceived -gt 0){"Green"}else{"Red"})

Write-Host ""
Write-Host "[DIAGNOSIS]" -ForegroundColor Magenta
Write-Host "============" -ForegroundColor Magenta

if ($splitBrainCount -eq 0) {
    Write-Host ""
    Write-Host "ROOT CAUSE: Split-brain detection is NOT triggering!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Possible reasons:" -ForegroundColor Yellow
    Write-Host "  1. The handleElectionMessage function is not processing TypeHeartbeat messages" -ForegroundColor Gray
    Write-Host "  2. Heartbeat messages are not including the term field" -ForegroundColor Gray
    Write-Host "  3. The split-brain detection code block is not being reached" -ForegroundColor Gray
    Write-Host "  4. Each node is only receiving its own heartbeats" -ForegroundColor Gray
    Write-Host ""
    Write-Host "IMMEDIATE ACTION REQUIRED:" -ForegroundColor Red
    Write-Host "  Run: Get-Content .\election_emergency_fix.go" -ForegroundColor Yellow
    Write-Host "  This will show the critical fix needed." -ForegroundColor Yellow
} else {
    Write-Host "Split-brain detection IS working, checking resolution..." -ForegroundColor Green
}

Write-Host ""
Write-Host "[6] Checking listener initialization..." -ForegroundColor Yellow
Write-Host "--------------------------------------------------------" -ForegroundColor Yellow

$listenerCount = 0
foreach ($container in $containers) {
    $result = docker logs --tail 200 $container 2>$null | Select-String "Starting unified election event listener"
    if ($result) {
        $listenerCount++
        Write-Host "✓ $container : Listener started" -ForegroundColor Green
    } else {
        Write-Host "❌ $container : NO LISTENER FOUND!" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "Listeners active: $listenerCount/8" -ForegroundColor $(if($listenerCount -eq 8){"Green"}else{"Red"})

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "1. Save detailed logs: .\Save-DetailedLogs.ps1" -ForegroundColor Yellow
Write-Host "2. Review the emergency fix below" -ForegroundColor Yellow
Write-Host "3. Apply the fix and rebuild" -ForegroundColor Yellow
Write-Host ""