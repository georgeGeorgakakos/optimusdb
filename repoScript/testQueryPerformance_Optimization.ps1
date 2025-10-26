# Compare-Performance.ps1
# Compares performance before and after optimization

param(
    [int]$BasePort = 18001,
    [int]$Iterations = 20
)

Write-Host "=========================================" -ForegroundColor Cyan
Write-Host "OptimusDB Performance Comparison Tool" -ForegroundColor Cyan
Write-Host "=========================================" -ForegroundColor Cyan
Write-Host ""

function Measure-QueryPerformance {
    param(
        [int]$Port,
        [string]$JsonData,
        [int]$Count
    )

    $times = @()

    Write-Host "Running $Count queries..." -NoNewline

    for ($i = 1; $i -le $Count; $i++) {
        $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
        try {
            $null = Invoke-RestMethod -Uri "http://localhost:$Port/swarmkb/command" `
                -Method Post `
                -ContentType "application/json" `
                -Body $JsonData `
                -TimeoutSec 10
            $stopwatch.Stop()
            $times += $stopwatch.ElapsedMilliseconds

            if ($i % 5 -eq 0) {
                Write-Host "." -NoNewline
            }
        }
        catch {
            Write-Host "!" -NoNewline -ForegroundColor Red
        }
    }

    Write-Host " Done" -ForegroundColor Green

    return $times
}

function Show-Statistics {
    param(
        [double[]]$Times,
        [string]$Label
    )

    $stats = $Times | Measure-Object -Average -Minimum -Maximum
    $median = ($Times | Sort-Object)[[math]::Floor($Times.Count / 2)]
    $p95 = ($Times | Sort-Object)[[math]::Floor($Times.Count * 0.95)]

    Write-Host "`n$Label Statistics:" -ForegroundColor Yellow
    Write-Host "  Samples:     $($stats.Count)"
    Write-Host "  Average:     $([math]::Round($stats.Average, 2))ms"
    Write-Host "  Median:      $([math]::Round($median, 2))ms"
    Write-Host "  Min:         $([math]::Round($stats.Minimum, 2))ms"
    Write-Host "  Max:         $([math]::Round($stats.Maximum, 2))ms"
    Write-Host "  P95:         $([math]::Round($p95, 2))ms"

    return @{
        Average = $stats.Average
        Median = $median
        Min = $stats.Minimum
        Max = $stats.Maximum
        P95 = $p95
    }
}

function Show-Comparison {
    param(
        [hashtable]$Before,
        [hashtable]$After
    )

    Write-Host "`n=========================================" -ForegroundColor Cyan
    Write-Host "Performance Improvement Summary" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan

    $metrics = @("Average", "Median", "P95")

    foreach ($metric in $metrics) {
        $improvement = (($Before[$metric] - $After[$metric]) / $Before[$metric]) * 100
        $speedup = $Before[$metric] / $After[$metric]

        Write-Host "`n$metric Latency:" -ForegroundColor Yellow
        Write-Host "  Before:      $([math]::Round($Before[$metric], 2))ms"
        Write-Host "  After:       $([math]::Round($After[$metric], 2))ms"
        Write-Host "  Improvement: $([math]::Round($improvement, 1))%" -ForegroundColor Green
        Write-Host "  Speedup:     $([math]::Round($speedup, 2))x faster" -ForegroundColor Green
    }
}

function Main {
    # Test query
    $queryJson = @{
        method = @{
            cmd = "query"
            argcnt = 0
        }
        criteria = @(
            @{
                status = "active"
            }
        )
    } | ConvertTo-Json -Depth 10

    Write-Host "This tool measures query performance with $Iterations iterations" -ForegroundColor Cyan
    Write-Host "Port: $BasePort" -ForegroundColor Cyan
    Write-Host ""

    # Clear cache first
    Write-Host "Clearing cache..." -ForegroundColor Yellow
    $clearJson = @{
        method = @{
            cmd = "clearcache"
            argcnt = 0
        }
    } | ConvertTo-Json -Depth 10

    try {
        $null = Invoke-RestMethod -Uri "http://localhost:$BasePort/swarmkb/command" `
            -Method Post `
            -ContentType "application/json" `
            -Body $clearJson `
            -TimeoutSec 5
        Write-Host "Cache cleared successfully" -ForegroundColor Green
    }
    catch {
        Write-Host "Cache clear not available (optional)" -ForegroundColor Yellow
    }

    Start-Sleep -Seconds 2

    # Measure cache miss performance (simulates "before" or first query)
    Write-Host "`n--- Cache Miss Performance (Initial Queries) ---" -ForegroundColor Cyan
    $cacheMissTimes = Measure-QueryPerformance -Port $BasePort -JsonData $queryJson -Count 5
    $beforeStats = Show-Statistics -Times $cacheMissTimes -Label "Cache Miss"

    Start-Sleep -Seconds 2

    # Measure cache hit performance (shows optimization benefit)
    Write-Host "`n--- Cache Hit Performance (Subsequent Queries) ---" -ForegroundColor Cyan
    $cacheHitTimes = Measure-QueryPerformance -Port $BasePort -JsonData $queryJson -Count $Iterations
    $afterStats = Show-Statistics -Times $cacheHitTimes -Label "Cache Hit"

    # Show comparison
    Show-Comparison -Before $beforeStats -After $afterStats

    # Throughput calculation
    $throughputBefore = 1000 / $beforeStats.Average
    $throughputAfter = 1000 / $afterStats.Average

    Write-Host "`n=========================================" -ForegroundColor Cyan
    Write-Host "Throughput Comparison" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "Cache Miss:  $([math]::Round($throughputBefore, 1)) queries/second"
    Write-Host "Cache Hit:   $([math]::Round($throughputAfter, 1)) queries/second" -ForegroundColor Green
    Write-Host "Improvement: $([math]::Round(($throughputAfter - $throughputBefore), 1)) queries/second" -ForegroundColor Green

    # CPU efficiency estimate
    Write-Host "`n=========================================" -ForegroundColor Cyan
    Write-Host "Efficiency Gains" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan

    $timesSavedPerQuery = $beforeStats.Average - $afterStats.Average
    $totalTimeSaved = $timesSavedPerQuery * $Iterations

    Write-Host "Time saved per query: $([math]::Round($timesSavedPerQuery, 2))ms"
    Write-Host "Total time saved ($Iterations queries): $([math]::Round($totalTimeSaved, 0))ms" -ForegroundColor Green
    Write-Host ""

    # Recommendations
    Write-Host "=========================================" -ForegroundColor Cyan
    Write-Host "Recommendations" -ForegroundColor Cyan
    Write-Host "=========================================" -ForegroundColor Cyan

    $improvementPct = (($beforeStats.Average - $afterStats.Average) / $beforeStats.Average) * 100

    if ($improvementPct -gt 90) {
        Write-Host "✓ Excellent! Cache is working perfectly" -ForegroundColor Green
        Write-Host "  Your optimization is providing exceptional performance gains"
    }
    elseif ($improvementPct -gt 70) {
        Write-Host "✓ Good! Cache is working well" -ForegroundColor Green
        Write-Host "  Solid performance improvement detected"
    }
    elseif ($improvementPct -gt 50) {
        Write-Host "⚠ Fair cache performance" -ForegroundColor Yellow
        Write-Host "  Consider increasing cache TTL or checking for cache invalidation"
    }
    else {
        Write-Host "✗ Cache may not be working optimally" -ForegroundColor Red
        Write-Host "  Verify QueryEngine is initialized and cache is enabled"
        Write-Host "  Check logs for [CACHE] messages"
    }

    Write-Host ""
}

Main
