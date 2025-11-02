#!/usr/bin/env pwsh
# Save detailed logs from all containers for analysis

$outputDir = ".\split-brain-logs-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
New-Item -ItemType Directory -Path $outputDir -Force | Out-Null

Write-Host "Saving logs to: $outputDir" -ForegroundColor Cyan
Write-Host ""

$containers = @("optimusdb1", "optimusdb2", "optimusdb3", "optimusdb4", "optimusdb5", "optimusdb6", "optimusdb7", "optimusdb8")

foreach ($container in $containers) {
    Write-Host "Saving logs from $container..." -ForegroundColor Yellow

    # Full logs
    docker logs $container 2>&1 | Out-File "$outputDir\$container-full.log"

    # Election-related only
    docker logs $container 2>&1 | Select-String "ELECTION|COORDINATOR|HEARTBEAT|SPLIT-BRAIN|VOTE" | Out-File "$outputDir\$container-election.log"

    # Terms and roles
    docker logs $container 2>&1 | Select-String "term|role" -CaseSensitive:$false | Out-File "$outputDir\$container-terms.log"
}

Write-Host ""
Write-Host "✓ Logs saved to $outputDir" -ForegroundColor Green
Write-Host ""
Write-Host "Creating analysis report..." -ForegroundColor Yellow

$report = @"
Split-Brain Analysis Report
Generated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')
========================================

COORDINATOR STATUS:
"@

foreach ($container in $containers) {
    $isCoord = Select-String -Path "$outputDir\$container-election.log" -Pattern "is now Coordinator" | Select-Object -Last 1
    if ($isCoord) {
        $report += "`n  ★ $container: COORDINATOR"
        $report += "`n    Last election: $($isCoord.Line)"
    } else {
        $report += "`n  ○ $container: Follower"
    }
}

$report += @"


SPLIT-BRAIN DETECTION:
"@

$splitBrainFound = $false
foreach ($container in $containers) {
    $splitBrain = Select-String -Path "$outputDir\$container-election.log" -Pattern "SPLIT-BRAIN"
    if ($splitBrain) {
        $splitBrainFound = $true
        $report += "`n  ✓ $container detected split-brain ($($splitBrain.Count) times)"
    }
}

if (-not $splitBrainFound) {
    $report += "`n  ❌ NO SPLIT-BRAIN DETECTION IN ANY CONTAINER!"
}

$report += @"


HEARTBEAT SUMMARY:
"@

foreach ($container in $containers) {
    $sent = (Select-String -Path "$outputDir\$container-election.log" -Pattern "Sent heartbeat").Count
    $received = (Select-String -Path "$outputDir\$container-election.log" -Pattern "Received from:").Count
    $report += "`n  $container - Sent: $sent, Received: $received"
}

$report += @"


TERM NUMBERS:
"@

foreach ($container in $containers) {
    $terms = Select-String -Path "$outputDir\$container-terms.log" -Pattern "term \d+" | Select-Object -Last 3
    if ($terms) {
        $report += "`n  $container:"
        $terms | ForEach-Object { $report += "`n    $($_.Line)" }
    }
}

$report | Out-File "$outputDir\ANALYSIS-REPORT.txt"

Write-Host ""
Write-Host "✓ Analysis report created: $outputDir\ANALYSIS-REPORT.txt" -ForegroundColor Green
Write-Host ""
Write-Host "Review the report with:" -ForegroundColor Cyan
Write-Host "  Get-Content $outputDir\ANALYSIS-REPORT.txt" -ForegroundColor Yellow
Write-Host ""