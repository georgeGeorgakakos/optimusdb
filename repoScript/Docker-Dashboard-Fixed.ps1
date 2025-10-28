# Improved Docker Dashboard for OptimusDB
# Better log parsing to handle different formats
# Usage: .\Docker-Dashboard-Fixed.ps1

param(
    [Parameter(Mandatory=$false)]
    [int]$RefreshInterval = 5,

    [Parameter(Mandatory=$false)]
    [int]$LogLines = 500
)

$Colors = @{
    Success = 'Green'
    Warning = 'Yellow'
    Error = 'Red'
    Info = 'Cyan'
    Highlight = 'Magenta'
}

function Get-ContainerLogs {
    param([string]$Container, [int]$Lines = 500)

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
        LastElection = $null
        HeartbeatsSent = 0
        HeartbeatsReceived = 0
        Errors = 0
        Status = "Unknown"
        LastHeartbeat = $null
        DebugState = $null
    }

    # Parse logs - try multiple patterns
    foreach ($line in $logs) {
        # Pattern 1: "Node X is now Coordinator"
        if ($line -match "is now Coordinator") {
            $stats.Role = "COORDINATOR"
        }
        # Pattern 2: "Node X is now Follower"
        elseif ($line -match "is now Follower") {
            $stats.Role = "FOLLOWER"
        }
        # Pattern 3: "[COORDINATOR]" in log
        elseif ($line -match "\[COORDINATOR\]") {
            $stats.Role = "COORDINATOR"
        }
        # Pattern 4: "[FOLLOWER]" in log
        elseif ($line -match "\[FOLLOWER\]") {
            $stats.Role = "FOLLOWER"
        }
        # Pattern 5: "[ROLE]" message
        elseif ($line -match "\[ROLE\].*Role:\s*(Coordinator|Follower)") {
            $stats.Role = $matches[1].ToUpper()
        }
        # Pattern 6: DEBUG STATE with Role=
        elseif ($line -match "DEBUG STATE.*Role=(Coordinator|Follower)") {
            $stats.Role = $matches[1].ToUpper()
        }
        # Pattern 7: Published role message
        elseif ($line -match "Published role.*Role=(Coordinator|Follower)") {
            $stats.Role = $matches[1].ToUpper()
        }

        # Election
        if ($line -match "\[ELECTION\].*Winner") {
            $stats.LastElection = $line
        }

        # Heartbeats
        if ($line -match "Sent heartbeat") {
            $stats.HeartbeatsSent++
        }
        elseif ($line -match "Received heartbeat") {
            $stats.HeartbeatsReceived++
            $stats.LastHeartbeat = $line
        }

        # Errors
        if ($line -match "\[ERROR\]") {
            $stats.Errors++
        }

        # Debug state
        if ($line -match "DEBUG STATE") {
            $stats.DebugState = $line
        }
    }

    # Determine status based on role and activity
    if ($stats.Role -ne "UNKNOWN") {
        if ($stats.Role -eq "COORDINATOR") {
            $stats.Status = if ($stats.HeartbeatsSent -gt 0) { "HEALTHY" } else { "STARTING" }
        } else {
            $stats.Status = if ($stats.HeartbeatsReceived -gt 0) { "HEALTHY" } else { "STARTING" }
        }
    } else {
        # Still unknown - check if there's any activity
        if ($stats.HeartbeatsSent -gt 0 -or $stats.HeartbeatsReceived -gt 0) {
            $stats.Status = "ACTIVE (role unclear)"
        } else {
            $stats.Status = "INITIALIZING"
        }
    }

    return $stats
}

function Show-Dashboard {
    $containers = docker ps --format "{{.Names}}" 2>$null

    if (-not $containers) {
        Clear-Host
        Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor $Colors.Highlight
        Write-Host "║          OptimusDB Docker Dashboard                             ║" -ForegroundColor $Colors.Highlight
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
    Write-Host "║          OptimusDB Docker Dashboard (Enhanced)                  ║" -ForegroundColor $Colors.Highlight
    Write-Host "╚════════════════════════════════════════════════════════════════╝`n" -ForegroundColor $Colors.Highlight

    Write-Host "🔄 Last Updated: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor $Colors.Info
    Write-Host "📦 Monitoring $($containers.Count) container(s)" -ForegroundColor $Colors.Info
    Write-Host "📊 Analyzing last $LogLines log lines per container" -ForegroundColor $Colors.Info
    Write-Host ""

    Write-Host "⏳ Gathering data..." -ForegroundColor $Colors.Warning

    $allStats = @()

    foreach ($container in $containers) {
        $stats = Get-ContainerStats -Container $container
        $allStats += $stats
    }

    # Clear the "gathering" message
    $host.UI.RawUI.CursorPosition = @{X=0; Y=$host.UI.RawUI.CursorPosition.Y-1}
    Write-Host "✓ Data collected!    " -ForegroundColor $Colors.Success
    Write-Host ""

    # Show cluster overview
    $coordinators = ($allStats | Where-Object { $_.Role -eq "COORDINATOR" }).Count
    $followers = ($allStats | Where-Object { $_.Role -eq "FOLLOWER" }).Count
    $unknown = ($allStats | Where-Object { $_.Role -eq "UNKNOWN" }).Count

    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "🌐 CLUSTER OVERVIEW" -ForegroundColor $Colors.Highlight
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info

    Write-Host "   Coordinators: " -NoNewline -ForegroundColor $Colors.Info
    if ($coordinators -eq 1) {
        Write-Host $coordinators -ForegroundColor $Colors.Success
    } elseif ($coordinators -gt 1) {
        Write-Host "$coordinators (⚠️  SPLIT BRAIN!)" -ForegroundColor $Colors.Error
    } else {
        Write-Host "$coordinators (⚠️  NO LEADER!)" -ForegroundColor $Colors.Warning
    }

    Write-Host "   Followers: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host $followers -ForegroundColor $Colors.Success

    if ($unknown -gt 0) {
        Write-Host "   Unknown Role: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host "$unknown (⚠️  Check logs)" -ForegroundColor $Colors.Warning
    }

    Write-Host ""

    # Show each container
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "📦 CONTAINERS" -ForegroundColor $Colors.Highlight
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host ""

    foreach ($stats in $allStats) {
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

        $statusColor = switch ($stats.Status) {
            "HEALTHY" { $Colors.Success }
            "STARTING" { $Colors.Warning }
            default { $Colors.Info }
        }

        # Container name
        Write-Host "   $roleIcon " -NoNewline -ForegroundColor $roleColor
        Write-Host "$($stats.Container)" -ForegroundColor White

        # Role and status
        Write-Host "      Role: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.Role -NoNewline -ForegroundColor $roleColor
        Write-Host " | Status: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.Status -ForegroundColor $statusColor

        # Heartbeats - show both
        Write-Host "      HB→: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.HeartbeatsSent -NoNewline -ForegroundColor $(if ($stats.HeartbeatsSent -gt 0) { $Colors.Success } else { $Colors.Info })
        Write-Host " | HB←: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.HeartbeatsReceived -ForegroundColor $(if ($stats.HeartbeatsReceived -gt 0) { $Colors.Success } else { $Colors.Info })

        # Errors
        $errorColor = if ($stats.Errors -eq 0) { $Colors.Success } else { $Colors.Error }
        Write-Host "      Errors: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host $stats.Errors -ForegroundColor $errorColor

        # Debug state if available
        if ($stats.DebugState) {
            $shortDebug = if ($stats.DebugState.Length -gt 80) {
                $stats.DebugState.Substring(0, 77) + "..."
            } else {
                $stats.DebugState
            }
            Write-Host "      Debug: " -NoNewline -ForegroundColor $Colors.Info
            Write-Host $shortDebug -ForegroundColor Gray
        }

        Write-Host ""
    }

    # Recommendations
    if ($unknown -gt 0) {
        Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
        Write-Host "💡 DIAGNOSTICS" -ForegroundColor $Colors.Highlight
        Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
        Write-Host ""
        Write-Host "   Some containers show UNKNOWN role. To diagnose:" -ForegroundColor $Colors.Warning
        Write-Host "   1. Check actual logs: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host "docker logs <container>" -ForegroundColor White
        Write-Host "   2. Run diagnostic: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host ".\Diagnose-DockerLogs.ps1 -ContainerName <n>" -ForegroundColor White
        Write-Host "   3. Increase log lines: " -NoNewline -ForegroundColor $Colors.Info
        Write-Host ".\Docker-Dashboard-Fixed.ps1 -LogLines 1000" -ForegroundColor White
        Write-Host ""
    }

    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor $Colors.Info
    Write-Host "Refreshing in $RefreshInterval seconds... (Ctrl+C to exit)" -ForegroundColor $Colors.Info
    Write-Host ""
    Write-Host "💡 Tips:" -ForegroundColor $Colors.Info
    Write-Host "   - Monitor specific container: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host ".\Monitor-Docker.ps1 -ContainerName <n>" -ForegroundColor White
    Write-Host "   - Run diagnostic: " -NoNewline -ForegroundColor $Colors.Info
    Write-Host ".\Diagnose-DockerLogs.ps1 -ContainerName <n>" -ForegroundColor White
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

# Main loop
while ($true) {
    Show-Dashboard
    Start-Sleep -Seconds $RefreshInterval
}