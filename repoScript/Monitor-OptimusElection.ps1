# OptimusDB Docker Dashboard - Optimized for Your Log Format
# Based on actual log analysis showing Role=Coordinator in DEBUG STATE
# Usage: .\Docker-Dashboard-v2.ps1

param(
    [Parameter(Mandatory=$false)]
    [int]$RefreshInterval = 5,

    [Parameter(Mandatory=$false)]
    [int]$LogLines = 200
)

$Colors = @{
    Success = 'Green'
    Warning = 'Yellow'
    Error = 'Red'
    Info = 'Cyan'
    Highlight = 'Magenta'
}

function Get-ContainerLogs {
    param([string]$Container, [int]$Lines = 200)

    try {
        $logs = docker logs --tail $Lines $Container 2>&1
        return $logs
    } catch {
        return @()
    }
}

function Get-ContainerStats {
    param([string]$Container)

    $logs = Get-ContainerLogs -Container $Container -Lines $script:LogLines

    $stats = @{
        Container = $Container
        Role = "UNKNOWN"
        LeaderID = $null
        IsElecting = $false
        HeartbeatsSent = 0
        HeartbeatsReceived = 0
        HeartbeatMissed = 0
        Errors = 0
        Status = "Unknown"
        LastDebugState = $null
    }

    # Parse logs
    foreach ($line in $logs) {
        # Primary pattern: DEBUG STATE with Role=
        # Example: [DEBUG STATE] Role=Coordinator, Leader=..., IsElecting=false, ...
        if ($line -match "\[DEBUG STATE\].*Role=(\w+)") {
            $stats.Role = $matches[1].ToUpper()

            # Extract additional info from debug state
            if ($line -match "Leader=([^,]+)") {
                $leaderFull = $matches[1]
                # Shorten leader ID for display
                $stats.LeaderID = if ($leaderFull.Length -gt 20) {
                    $leaderFull.Substring(0, 20) + "..."
                } else {
                    $leaderFull
                }
            }

            if ($line -match "IsElecting=(\w+)") {
                $stats.IsElecting = $matches[1] -eq "true"
            }

            if ($line -match "HeartbeatMissed=(\d+)") {
                $stats.HeartbeatMissed = [int]$matches[1]
            }

            $stats.LastDebugState = $line
        }

        # Count heartbeats
        if ($line -match "Sent heartbeat") {
            $stats.HeartbeatsSent++
        }
        elseif ($line -match "Received heartbeat") {
            $stats.HeartbeatsReceived++
        }

        # Count errors
        if ($line -match "\[ERROR\]") {
            $stats.Errors++
        }
    }

    # Determine status
    if ($stats.Role -ne "UNKNOWN") {
        if ($stats.IsElecting) {
            $stats.Status = "ELECTING"
        }
        elseif ($stats.Role -eq "COORDINATOR") {
            if ($stats.HeartbeatsSent -gt 0) {
                if ($stats.HeartbeatMissed -gt 0) {
                    $stats.Status = "HEALTHY (missed $($stats.HeartbeatMissed) HB)"
                } else {
                    $stats.Status = "HEALTHY"
                }
            } else {
                $stats.Status = "STARTING"
            }
        }
        else { # FOLLOWER
            if ($stats.HeartbeatsReceived -gt 0) {
                if ($stats.HeartbeatMissed -ge 3) {
                    $stats.Status = "DEGRADED (no leader)"
                }
                elseif ($stats.HeartbeatMissed -gt 0) {
                    $stats.Status = "HEALTHY (missed $($stats.HeartbeatMissed) HB)"
                }
                else {
                    $stats.Status = "HEALTHY"
                }
            } else {
                $stats.Status = "STARTING"
            }
        }
    } else {
        $stats.Status = "INITIALIZING"
    }

    return $stats
}

function Show-Dashboard {
    $containers = docker ps --format "{{.Names}}" 2>$null

    if (-not $containers) {
        Clear-Host
        Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor $Colors.Highlight
        Write-Host "║          OptimusDB Docker Dashboard v2                          ║" -ForegroundColor $Colors.Highlight
        Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor $Colors.Highlight

        Write-Host "❌ No Docker containers found!" -ForegroundColor $Colors.Error
        Write-Host ""
        Write-Host "Make sure you have OptimusDB containers running:" -ForegroundColor $Colors.Warning
        Write-Host "  docker ps" -ForegroundColor White
        Write-Host ""
        return
    }

    Clear-Host

    Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor $Colors.Highlight
    Write-Host "║          OptimusDB Docker Dashboard v2                          ║" -ForegroundColor $Colors.Highlight
    Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor $Colors.Highlight

    Write-Host "🔄 Last Updated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor $Colors.Info
    Write-Host "📦 Monitoring $($containers.Count) container(s) | Analyzing last $LogLines lines" -ForegroundColor $Colors.Info
    Write-Host ""

    Write-Host "⏳ Gathering data..." -ForegroundColor $Colors.Warning

    $allStats = @()

    foreach ($container in $containers) {
        $stats = Get-ContainerStats -Container $container
        $allStats += $stats
    }

    # Clear the "gathering" message
    $cursorPos = $host.UI.RawUI.CursorPosition
    $cursorPos.Y = $cursorPos.Y - 1
    $host.UI.RawUI.CursorPosition = $cursorPos
    Write-Host "✓ Data collected!                             " -ForegroundColor $Colors.Success
    Write-Host ""

    # Cluster overview
    $coordinators = ($allStats | Where-Object { $_.Role -eq "COORDINATOR" })
    $followers = ($allStats | Where-Object { $_.Role -eq "FOLLOWER" })
    $unknown = ($allStats | Where-Object { $_.Role -eq "UNKNOWN" })
    $electing = ($allStats | Where-Object { $_.IsElecting -eq $true })

    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "🌐 CLUSTER OVERVIEW" -ForegroundColor $Colors.Highlight
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info

    Write-Host "   Coordinators: " -NoNewline -ForegroundColor $Colors.Info
    if ($coordinators.Count -eq 1) {
        Write-Host "$($coordinators.Count) ✓" -ForegroundColor $Colors.Success
    } elseif ($coordinators.Count -gt 1) {
        Write-Host "$($coordinators.Count) ⚠️  SPLIT BRAIN!" -ForegroundColor $Colors.Error
    } else {
        Write-Host "$($coordinators.Count) ⚠️  NO LEADER!" -ForegroundColor $Colors.Warning
    }

    Write-Host "   Followers: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host $followers.Count -ForegroundColor $Colors.Success

    if ($unknown.Count -gt 0) {
        Write-Host "   Unknown: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host "$($unknown.Count) (check logs)" -ForegroundColor $Colors.Warning
    }

    if ($electing.Count -gt 0) {
        Write-Host "   🗳️  " -NoNewline -ForegroundColor $Colors.Warning
        Write-Host "$($electing.Count) container(s) currently electing" -ForegroundColor $Colors.Warning
    }

    # Show leader info if we have one
    if ($coordinators.Count -eq 1) {
        $leader = $coordinators[0]
        Write-Host ""
        Write-Host "   👑 Current Leader: " -NoNewline -ForegroundColor $Colors.Success
        Write-Host "$($leader.Container)" -ForegroundColor White
        if ($leader.LeaderID) {
            Write-Host "      Leader ID: " -NoNewline -ForegroundColor $Colors.Info
            Write-Host $leader.LeaderID -ForegroundColor Gray
        }
    }

    Write-Host ""

    # Show each container
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "📦 CONTAINER DETAILS" -ForegroundColor $Colors.Highlight
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host ""

    # Sort: Coordinators first, then Followers, then Unknown
    $sortedStats = @()
    $sortedStats += $allStats | Where-Object { $_.Role -eq "COORDINATOR" } | Sort-Object Container
    $sortedStats += $allStats | Where-Object { $_.Role -eq "FOLLOWER" } | Sort-Object Container
    $sortedStats += $allStats | Where-Object { $_.Role -eq "UNKNOWN" } | Sort-Object Container

    foreach ($stats in $sortedStats) {
        $roleIcon = switch ($stats.Role) {
            "COORDINATOR" { "★" }
            "FOLLOWER" { "○" }
            default { "?" }
        }

        $roleColor = switch ($stats.Role) {
            "COORDINATOR" { $Colors.Success }
            "FOLLOWER" { $Colors.Warning }
            default { $Colors.Info }
        }

        # Determine status color
        $statusColor = $Colors.Success
        if ($stats.Status -match "DEGRADED|NO LEADER") {
            $statusColor = $Colors.Error
        }
        elseif ($stats.Status -match "STARTING|INITIALIZING|missed") {
            $statusColor = $Colors.Warning
        }
        elseif ($stats.Status -match "ELECTING") {
            $statusColor = $Colors.Info
        }

        # Container header
        Write-Host "   $roleIcon " -NoNewline -ForegroundColor $roleColor
        Write-Host "$($stats.Container)" -ForegroundColor White

        # Role and status
        Write-Host "      Role: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.Role -NoNewline -ForegroundColor $roleColor
        Write-Host " | Status: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.Status -ForegroundColor $statusColor

        # Heartbeats - show relevant one based on role
        if ($stats.Role -eq "COORDINATOR") {
            Write-Host "      Heartbeats Sent: " -NoNewline -ForegroundColor $Colors.Info
            Write-Host $stats.HeartbeatsSent -ForegroundColor $Colors.Success
        } else {
            Write-Host "      Heartbeats Received: " -NoNewline -ForegroundColor $Colors.Info
            $hbColor = if ($stats.HeartbeatsReceived -gt 0) { $Colors.Success } else { $Colors.Warning }
            Write-Host $stats.HeartbeatsReceived -ForegroundColor $hbColor
        }

        # Heartbeat status
        if ($stats.HeartbeatMissed -gt 0) {
            $missedColor = if ($stats.HeartbeatMissed -ge 3) { $Colors.Error } else { $Colors.Warning }
            Write-Host "      ⚠️  Heartbeats Missed: " -NoNewline -ForegroundColor $Colors.Warning
            Write-Host $stats.HeartbeatMissed -ForegroundColor $missedColor
        }

        # Errors
        if ($stats.Errors -gt 0) {
            Write-Host "      ❌ Errors: " -NoNewline -ForegroundColor $Colors.Error
            Write-Host $stats.Errors -ForegroundColor $Colors.Error
        }

        Write-Host ""
    }

    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "Refreshing in $RefreshInterval seconds... (Ctrl+C to exit)" -ForegroundColor $Colors.Info
    Write-Host ""
    Write-Host "💡 Commands:" -ForegroundColor $Colors.Info
    Write-Host "   Monitor specific: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host ".\Monitor-Docker.ps1 -ContainerName <name>" -ForegroundColor White
    Write-Host "   View raw logs: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host "docker logs -f <name>" -ForegroundColor White
    Write-Host ""
}

# Check Docker
try {
    docker --version 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`n❌ Docker is not installed or not running!" -ForegroundColor Red
        Write-Host ""
        exit 1
    }
} catch {
    Write-Host "`n❌ Docker is not installed or not running!" -ForegroundColor Red
    Write-Host ""
    exit 1
}

Write-Host "`n🚀 Starting OptimusDB Docker Dashboard v2..." -ForegroundColor Cyan
Write-Host "   Optimized for your log format" -ForegroundColor Gray
Start-Sleep -Seconds 1

# Main loop
while ($true) {
    Show-Dashboard
    Start-Sleep -Seconds $RefreshInterval
}