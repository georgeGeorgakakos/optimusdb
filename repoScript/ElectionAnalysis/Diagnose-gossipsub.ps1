# diagnose-gossipsub.ps1
# Comprehensive GossipSub and Message Propagation Diagnostics

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GOSSIPSUB MESSAGE PROPAGATION DIAGNOSIS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$nodes = 1..8 | ForEach-Object { "optimusdb$_" }

Write-Host "`n[CHECK 1] GossipSub Topic Subscriptions..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node subscriptions:" -ForegroundColor White
    $topics = docker logs $node 2>&1 | Select-String "Subscribed to topic|JOIN topic|topic.*subscribe" -CaseSensitive:$false | Select-Object -Last 5

    if ($topics) {
        $topics | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ❌ No topic subscriptions found!" -ForegroundColor Red
    }
}

Write-Host "`n`n[CHECK 2] GossipSub Publish Activity..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node publish activity:" -ForegroundColor White
    $publishes = docker logs $node 2>&1 | Select-String "Publish|Publishing|published" -CaseSensitive:$false | Select-Object -Last 5

    if ($publishes) {
        $publishes | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ⚠️  No publish activity found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n[CHECK 3] GossipSub Message Reception..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node message reception:" -ForegroundColor White
    $received = docker logs $node 2>&1 | Select-String "Received|incoming message|HandleIncoming" -CaseSensitive:$false | Select-Object -Last 10

    if ($received) {
        $received | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ❌ No messages received!" -ForegroundColor Red
    }
}

Write-Host "`n`n[CHECK 4] GossipSub Peer Mesh Status..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node mesh status:" -ForegroundColor White
    $mesh = docker logs $node 2>&1 | Select-String "GRAFT|PRUNE|mesh|peers in topic" -CaseSensitive:$false | Select-Object -Last 5

    if ($mesh) {
        $mesh | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ⚠️  No mesh information found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n[CHECK 5] OrbitDB Operation Logs..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node OrbitDB operations:" -ForegroundColor White
    $orbitOps = docker logs $node 2>&1 | Select-String "OrbitDB|Store.*add|Entry.*add|Replicate" -CaseSensitive:$false | Select-Object -Last 5

    if ($orbitOps) {
        $orbitOps | ForEach-Object { Write-Host "    $_" -ForegroundColor Cyan }
    } else {
        Write-Host "    ⚠️  No OrbitDB operations found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n[CHECK 6] Message/Event Flow..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node event flow:" -ForegroundColor White
    $events = docker logs $node 2>&1 | Select-String "vote|election|contribution|event" -CaseSensitive:$false | Select-Object -Last 5

    if ($events) {
        $events | ForEach-Object { Write-Host "    $_" -ForegroundColor Magenta }
    } else {
        Write-Host "    ⚠️  No vote/contribution events found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n[CHECK 7] Error Messages..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node errors:" -ForegroundColor White
    $errors = docker logs $node 2>&1 | Select-String "error|failed|panic|fatal" -CaseSensitive:$false | Select-Object -Last 5

    if ($errors) {
        $errors | ForEach-Object { Write-Host "    $_" -ForegroundColor Red }
    } else {
        Write-Host "    ✅ No recent errors" -ForegroundColor Green
    }
}

Write-Host "`n`n[CHECK 8] PubSub Router Type..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node PubSub configuration:" -ForegroundColor White
    $pubsubType = docker logs $node 2>&1 | Select-String "GossipSub|FloodSub|PubSub.*initializ|Router.*type" -CaseSensitive:$false | Select-Object -First 3

    if ($pubsubType) {
        $pubsubType | ForEach-Object { Write-Host "    $_" -ForegroundColor Cyan }
    } else {
        Write-Host "    ⚠️  No PubSub initialization found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n[CHECK 9] Peer Connection Quality..." -ForegroundColor Yellow
Write-Host "=" * 60

foreach ($node in $nodes) {
    Write-Host "`n  $node peer connections:" -ForegroundColor White
    $connections = docker logs $node 2>&1 | Select-String "Connected to peer|peer connected|establish.*connection" -CaseSensitive:$false | Select-Object -Last 5

    if ($connections) {
        $connections | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ⚠️  No connection logs found" -ForegroundColor Yellow
    }
}

Write-Host "`n`n========================================" -ForegroundColor Cyan
Write-Host "ANALYSIS & RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`nBased on the above checks, look for these issues:" -ForegroundColor White
Write-Host "  1. Are nodes subscribed to the SAME topic names?" -ForegroundColor Yellow
Write-Host "  2. Is GossipSub actually initialized (not FloodSub)?" -ForegroundColor Yellow
Write-Host "  3. Are there GRAFT messages showing mesh formation?" -ForegroundColor Yellow
Write-Host "  4. Are nodes publishing to topics they're subscribed to?" -ForegroundColor Yellow
Write-Host "  5. Are there any errors blocking message delivery?" -ForegroundColor Yellow

Write-Host "`nCommon issues:" -ForegroundColor White
Write-Host "  • Topic name mismatch between nodes" -ForegroundColor Cyan
Write-Host "  • GossipSub not enabled (using FloodSub instead)" -ForegroundColor Cyan
Write-Host "  • Mesh not forming (D/Dlo/Dhi parameters wrong)" -ForegroundColor Cyan
Write-Host "  • Messages published before subscription" -ForegroundColor Cyan
Write-Host "  • Firewall/network blocking GossipSub control messages" -ForegroundColor Cyan