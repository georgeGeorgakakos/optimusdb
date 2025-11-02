#!/usr/bin/env pwsh
# Find where to apply GossipSub fixes in your code

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Finding Files to Fix" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$projectRoot = "C:\Users\georg\GolandProjects\optimusdb-lsa"

Write-Host "[1] Finding where PubSub is created..." -ForegroundColor Yellow
Write-Host "    Look for: pubsub.NewGossipSub" -ForegroundColor Gray
Write-Host ""

$pubsubFiles = Get-ChildItem -Path $projectRoot -Recurse -Include *.go |
        Select-String "NewGossipSub" -List |
        Select-Object -ExpandProperty Path

if ($pubsubFiles) {
    foreach ($file in $pubsubFiles) {
        Write-Host "    ✓ Found in: $file" -ForegroundColor Green
        Write-Host "      Line(s):" -ForegroundColor Gray
        Get-Content $file | Select-String "NewGossipSub" -Context 2,2 | ForEach-Object {
            Write-Host "        $($_.Line)" -ForegroundColor Gray
        }
        Write-Host ""
    }
} else {
    Write-Host "    ❌ Not found! May be initialized differently" -ForegroundColor Red
    Write-Host "    Search manually for 'pubsub' creation" -ForegroundColor Yellow
}

Write-Host "[2] Finding ListenForElectionEvents function..." -ForegroundColor Yellow
Write-Host "    Look for: func (n *Node) ListenForElectionEvents" -ForegroundColor Gray
Write-Host ""

$listenerFiles = Get-ChildItem -Path $projectRoot -Recurse -Include *.go |
        Select-String "ListenForElectionEvents" -List |
        Select-Object -ExpandProperty Path

if ($listenerFiles) {
    foreach ($file in $listenerFiles) {
        Write-Host "    ✓ Found in: $file" -ForegroundColor Green
        Write-Host ""
    }
} else {
    Write-Host "    ❌ Not found!" -ForegroundColor Red
}

Write-Host "[3] Finding where messages are published..." -ForegroundColor Yellow
Write-Host "    Look for: topic.Publish" -ForegroundColor Gray
Write-Host ""

$publishFiles = Get-ChildItem -Path $projectRoot -Recurse -Include *.go |
        Select-String "\.Publish\(.*ctx" -List |
        Select-Object -ExpandProperty Path

if ($publishFiles) {
    foreach ($file in $publishFiles) {
        Write-Host "    ✓ Found in: $file" -ForegroundColor Green
    }
    Write-Host ""
} else {
    Write-Host "    ℹ No explicit Publish calls found" -ForegroundColor Yellow
    Write-Host "    Check for wrapper functions" -ForegroundColor Gray
    Write-Host ""
}

Write-Host "[4] Finding main.go or initialization file..." -ForegroundColor Yellow
Write-Host ""

$mainFile = Get-ChildItem -Path $projectRoot -Recurse -Include main.go -Depth 2 |
        Select-Object -First 1 -ExpandProperty FullName

if ($mainFile) {
    Write-Host "    ✓ Found: $mainFile" -ForegroundColor Green
} else {
    Write-Host "    ❌ main.go not found in expected location" -ForegroundColor Red
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Next Steps:" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "1. Open the files listed above" -ForegroundColor Yellow
Write-Host "2. Apply fixes from EXACT_GOSSIPSUB_FIX.md" -ForegroundColor Yellow
Write-Host "3. Focus on these priorities:" -ForegroundColor Yellow
Write-Host "   - Fix #1: PubSub initialization (add WithFloodPublish)" -ForegroundColor White
Write-Host "   - Fix #2: ListenForElectionEvents (add detailed logging)" -ForegroundColor White
Write-Host "   - Fix #3: Ensure topic.Subscribe() is called" -ForegroundColor White
Write-Host ""

Write-Host "Quick search commands:" -ForegroundColor Cyan
Write-Host "  # Find PubSub creation:" -ForegroundColor Gray
Write-Host "  Get-ChildItem -Recurse *.go | Select-String 'NewGossipSub'" -ForegroundColor White
Write-Host ""
Write-Host "  # Find listener function:" -ForegroundColor Gray
Write-Host "  Get-ChildItem -Recurse *.go | Select-String 'ListenForElectionEvents'" -ForegroundColor White
Write-Host ""