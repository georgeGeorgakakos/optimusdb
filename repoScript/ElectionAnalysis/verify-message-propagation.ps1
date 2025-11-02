# verify-message-propagation.ps1

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "MESSAGE PROPAGATION VERIFICATION" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$nodes = 1..8 | ForEach-Object { "optimusdb$_" }

foreach ($node in $nodes) {
    Write-Host "`n  Checking $node message sources..." -ForegroundColor Yellow

    $output = docker logs $node 2>&1 | Select-String "Received.*from peer" | Select-Object -Last 10

    if ($output) {
        $output | ForEach-Object {
            if ($_ -match "from peer: (Qm\w+)") {
                $peerID = $matches[1]
                # Check if it's from another node
                $fromSelf = docker exec $node cat /tmp/peer-id 2>$null
                if ($peerID -ne $fromSelf) {
                    Write-Host "    ✅ Received message from OTHER peer: $peerID" -ForegroundColor Green
                } else {
                    Write-Host "    ℹ️  Own message: $peerID" -ForegroundColor Gray
                }
            }
        }
    } else {
        Write-Host "    ⚠️  No recent messages found" -ForegroundColor Yellow
    }
}