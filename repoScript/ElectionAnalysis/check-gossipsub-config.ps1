# check-gossipsub-config.ps1
# Examine GossipSub configuration in running containers

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GOSSIPSUB CONFIGURATION CHECK" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$nodes = 1..8 | ForEach-Object { "optimusdb$_" }

Write-Host "`n[STEP 1] Checking initialization logs..." -ForegroundColor Yellow
Write-Host "This shows if GossipSub was properly initialized" -ForegroundColor Gray
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node initialization:" -ForegroundColor White
    $init = docker logs $node 2>&1 | Select-String "GossipSub|pubsub.*start|libp2p.*init" -CaseSensitive:$false | Select-Object -First 10

    if ($init) {
        $init | ForEach-Object {
            if ($_ -match "GossipSub") {
                Write-Host "    ✅ $_" -ForegroundColor Green
            } else {
                Write-Host "    $_" -ForegroundColor Gray
            }
        }
    } else {
        Write-Host "    ❌ No GossipSub initialization found!" -ForegroundColor Red
        Write-Host "       This is the problem - GossipSub may not be enabled!" -ForegroundColor Red
    }
}

Write-Host "`n`n[STEP 2] Topic subscription patterns..." -ForegroundColor Yellow
Write-Host "All nodes must subscribe to the SAME topic" -ForegroundColor Gray
Write-Host "=" * 60

$allTopics = @{}

foreach ($node in $nodes) {
    Write-Host "`n  $node topics:" -ForegroundColor White
    $topics = docker logs $node 2>&1 | Select-String "topic.*:|subscrib.*topic|JOIN.*topic" -CaseSensitive:$false | Select-Object -First 10

    if ($topics) {
        $topics | ForEach-Object {
            $line = $_.ToString()
            Write-Host "    $_" -ForegroundColor Cyan

            # Extract topic names
            if ($line -match "topic[:\s]+([a-zA-Z0-9/_-]+)") {
                $topicName = $matches[1]
                if (-not $allTopics.ContainsKey($topicName)) {
                    $allTopics[$topicName] = @()
                }
                $allTopics[$topicName] += $node
            }
        }
    } else {
        Write-Host "    ❌ No topic subscriptions!" -ForegroundColor Red
    }
}

Write-Host "`n`n[STEP 3] Topic subscription summary..." -ForegroundColor Yellow
Write-Host "=" * 60

if ($allTopics.Count -gt 0) {
    foreach ($topic in $allTopics.Keys) {
        $nodeCount = $allTopics[$topic].Count
        Write-Host "`n  Topic: $topic" -ForegroundColor Cyan
        Write-Host "    Subscribed nodes: $nodeCount" -ForegroundColor White
        Write-Host "    Nodes: $($allTopics[$topic] -join ', ')" -ForegroundColor Gray

        if ($nodeCount -eq 8) {
            Write-Host "    ✅ All nodes subscribed!" -ForegroundColor Green
        } else {
            Write-Host "    ⚠️  Not all nodes subscribed!" -ForegroundColor Yellow
        }
    }
} else {
    Write-Host "  ❌ No topics found across any nodes!" -ForegroundColor Red
}

Write-Host "`n`n[STEP 4] Checking for common issues..." -ForegroundColor Yellow
Write-Host "=" * 60

# Check for FloodSub instead of GossipSub
Write-Host "`n  Checking if FloodSub is being used (should be GossipSub):" -ForegroundColor White
$floodsubFound = $false
foreach ($node in $nodes) {
    $floodsub = docker logs $node 2>&1 | Select-String "FloodSub" -CaseSensitive | Select-Object -First 1
    if ($floodsub) {
        Write-Host "    ⚠️  $node is using FloodSub: $floodsub" -ForegroundColor Yellow
        $floodsubFound = $true
    }
}
if (-not $floodsubFound) {
    Write-Host "    ✅ No FloodSub usage detected" -ForegroundColor Green
}

# Check for mesh formation
Write-Host "`n  Checking for mesh formation (GRAFT messages):" -ForegroundColor White
$meshFound = $false
foreach ($node in $nodes) {
    $graft = docker logs $node 2>&1 | Select-String "GRAFT|grafting" -CaseSensitive:$false | Select-Object -First 1
    if ($graft) {
        Write-Host "    ✅ $node has mesh activity: $graft" -ForegroundColor Green
        $meshFound = $true
    }
}
if (-not $meshFound) {
    Write-Host "    ⚠️  No GRAFT messages found - mesh may not be forming!" -ForegroundColor Yellow
}

# Check for validation failures
Write-Host "`n  Checking for message validation failures:" -ForegroundColor White
$validationErrors = $false
foreach ($node in $nodes) {
    $validation = docker logs $node 2>&1 | Select-String "validation.*fail|reject.*message|invalid.*message" -CaseSensitive:$false | Select-Object -First 3
    if ($validation) {
        Write-Host "    ⚠️  $node validation issues:" -ForegroundColor Yellow
        $validation | ForEach-Object { Write-Host "       $_" -ForegroundColor Red }
        $validationErrors = $true
    }
}
if (-not $validationErrors) {
    Write-Host "    ✅ No validation errors detected" -ForegroundColor Green
}

Write-Host "`n`n========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSIS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`nMost likely issues:" -ForegroundColor White
Write-Host "  1. GossipSub not initialized - Check if your code actually enables it" -ForegroundColor Yellow
Write-Host "  2. Nodes not subscribing to topics - Verify subscription code runs" -ForegroundColor Yellow
Write-Host "  3. Topic name mismatch - All nodes must use identical topic names" -ForegroundColor Yellow
Write-Host "  4. Application not publishing - Check if your vote/contribution code uses pubsub" -ForegroundColor Yellow

Write-Host "`nNext steps:" -ForegroundColor White
Write-Host "  1. Share your GossipSub initialization code" -ForegroundColor Cyan
Write-Host "  2. Share your topic subscription code" -ForegroundColor Cyan
Write-Host "  3. Share how you publish messages" -ForegroundColor Cyan
Write-Host "  4. Run: .\test-message-flow.ps1" -ForegroundColor Cyan