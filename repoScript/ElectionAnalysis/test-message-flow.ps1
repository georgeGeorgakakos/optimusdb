# test-message-flow.ps1
# Trigger test messages and verify propagation

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "TRIGGERING TEST MESSAGES" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

$nodes = 1..8 | ForEach-Object { "optimusdb$_" }

# Function to send a test vote/contribution
function Send-TestMessage {
    param (
        [string]$node,
        [int]$testId
    )

    Write-Host "`nSending test message from $node..." -ForegroundColor Yellow

    # Try multiple possible API endpoints
    $endpoints = @(
        "http://localhost:808$testId/api/vote",
        "http://localhost:808$testId/vote",
        "http://localhost:808$testId/api/contribution",
        "http://localhost:808$testId/contribution"
    )

    $testData = @{
        voter = "test-voter-$testId"
        candidate = "test-candidate"
        timestamp = (Get-Date).ToString("o")
    } | ConvertTo-Json

    foreach ($endpoint in $endpoints) {
        try {
            $response = Invoke-RestMethod -Uri $endpoint -Method Post -Body $testData -ContentType "application/json" -TimeoutSec 2 -ErrorAction SilentlyContinue
            Write-Host "  ✅ Message sent to $endpoint" -ForegroundColor Green
            return $true
        } catch {
            # Try next endpoint
        }
    }

    Write-Host "  ⚠️  Could not send via API, trying docker exec..." -ForegroundColor Yellow

    # Try sending via docker exec if API doesn't work
    try {
        docker exec $node sh -c "echo 'Test message $testId' > /tmp/test-trigger-$testId.txt"
        Write-Host "  ℹ️  Created test file in container" -ForegroundColor Cyan
    } catch {
        Write-Host "  ❌ Could not trigger test message" -ForegroundColor Red
    }

    return $false
}

# Send test messages from first 3 nodes
Write-Host "`nSending test messages from multiple nodes..." -ForegroundColor Cyan
for ($i = 1; $i -le 3; $i++) {
    Send-TestMessage -node "optimusdb$i" -testId $i
    Start-Sleep -Seconds 1
}

Write-Host "`n`nWaiting 5 seconds for message propagation..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "CHECKING FOR MESSAGE PROPAGATION" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

foreach ($node in $nodes) {
    Write-Host "`n  $node - Recent activity (last 20 lines):" -ForegroundColor White
    $recent = docker logs $node --tail 20 2>&1 | Select-String "test|vote|contribution|received|publish" -CaseSensitive:$false

    if ($recent) {
        $recent | ForEach-Object { Write-Host "    $_" -ForegroundColor Green }
    } else {
        Write-Host "    ⚠️  No recent activity related to test messages" -ForegroundColor Yellow
    }
}

Write-Host "`n`n========================================" -ForegroundColor Cyan
Write-Host "RECOMMENDATIONS" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`nIf messages still aren't propagating, check:" -ForegroundColor White
Write-Host "  1. Run: .\diagnose-gossipsub.ps1" -ForegroundColor Yellow
Write-Host "  2. Check if GossipSub is actually enabled in your code" -ForegroundColor Yellow
Write-Host "  3. Verify topic names are identical across all nodes" -ForegroundColor Yellow
Write-Host "  4. Check for errors in the docker logs" -ForegroundColor Yellow
Write-Host "  5. Verify your application is actually using pubsub" -ForegroundColor Yellow