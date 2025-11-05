# OptimusDB Election System Test Suite
# PowerShell Script for Windows Testing

param(
    [Parameter(Mandatory=$false)]
    [int]$NodeCount = 3,

    [Parameter(Mandatory=$false)]
    [string]$TestScenario = "basic",

    [Parameter(Mandatory=$false)]
    [int]$TestDuration = 120,

    [Parameter(Mandatory=$false)]
    [string]$LogDir = ".\test-logs",

    [Parameter(Mandatory=$false)]
    [switch]$Cleanup
)

$ErrorActionPreference = "Stop"

# Color output functions
function Write-Success { param($msg) Write-Host "✅ $msg" -ForegroundColor Green }
function Write-Info { param($msg) Write-Host "ℹ️  $msg" -ForegroundColor Cyan }
function Write-Warning { param($msg) Write-Host "⚠️  $msg" -ForegroundColor Yellow }
function Write-Error { param($msg) Write-Host "❌ $msg" -ForegroundColor Red }
function Write-Header { param($msg) Write-Host "`n========================================" -ForegroundColor Magenta; Write-Host $msg -ForegroundColor Magenta; Write-Host "========================================`n" -ForegroundColor Magenta }

# Test configuration
$script:TestConfig = @{
    Nodes = @()
    LogDir = $LogDir
    TestStart = Get-Date
    BootstrapNode = $null
}

# Node configuration
$script:BasePort = 4001
$script:BaseAPIPort = 5001

function Initialize-TestEnvironment {
    Write-Header "Initializing Test Environment"

    # Create log directory
    if (-not (Test-Path $LogDir)) {
        New-Item -ItemType Directory -Path $LogDir -Force | Out-Null
        Write-Success "Created log directory: $LogDir"
    }

    # Clean old logs
    Get-ChildItem -Path $LogDir -Filter "*.log" | Remove-Item -Force
    Write-Success "Cleaned old logs"

    # Check if optimusdb binary exists
    if (-not (Test-Path ".\optimusdb.exe")) {
        Write-Error "optimusdb.exe not found in current directory"
        Write-Info "Please build the project first: go build -o optimusdb.exe"
        exit 1
    }
    Write-Success "Found optimusdb.exe"

    # Verify Go installation
    try {
        $goVersion = go version
        Write-Success "Go installed: $goVersion"
    } catch {
        Write-Error "Go not found. Please install Go first."
        exit 1
    }
}

function Start-Node {
    param(
        [int]$NodeId,
        [string]$BootstrapAddr = ""
    )

    $port = $script:BasePort + $NodeId
    $apiPort = $script:BaseAPIPort + $NodeId
    $repo = "node$NodeId"
    $logFile = Join-Path $LogDir "node${NodeId}.log"

    Write-Info "Starting Node $NodeId (Port: $port, API: $apiPort)"

    # Build command arguments
    $args = @(
        "--repo", $repo,
        "--port", $port,
        "--api-port", $apiPort,
        "--enable-election"
    )

    if ($BootstrapAddr) {
        $args += "--bootstrap"
        $args += $BootstrapAddr
        Write-Info "  Bootstrapping to: $BootstrapAddr"
    }

    # Start process
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = ".\optimusdb.exe"
    $psi.Arguments = $args -join " "
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi

    # Capture output to log file
    $logStream = [System.IO.StreamWriter]::new($logFile, $false)

    Register-ObjectEvent -InputObject $process -EventName OutputDataReceived -Action {
        $data = $Event.SourceEventArgs.Data
        if ($data) {
            $logStream.WriteLine("$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [STDOUT] $data")
            $logStream.Flush()
        }
    } | Out-Null

    Register-ObjectEvent -InputObject $process -EventName ErrorDataReceived -Action {
        $data = $Event.SourceEventArgs.Data
        if ($data) {
            $logStream.WriteLine("$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') [STDERR] $data")
            $logStream.Flush()
        }
    } | Out-Null

    $process.Start() | Out-Null
    $process.BeginOutputReadLine()
    $process.BeginErrorReadLine()

    # Store node info
    $nodeInfo = @{
        Id = $NodeId
        Process = $process
        Port = $port
        APIPort = $apiPort
        Repo = $repo
        LogFile = $logFile
        LogStream = $logStream
        BootstrapAddr = $BootstrapAddr
        StartTime = Get-Date
    }

    $script:TestConfig.Nodes += $nodeInfo

    if (-not $BootstrapAddr) {
        $script:TestConfig.BootstrapNode = "/ip4/127.0.0.1/tcp/$port/p2p/PLACEHOLDER"
    }

    Write-Success "Node $NodeId started (PID: $($process.Id))"

    return $nodeInfo
}

function Stop-Node {
    param($NodeInfo)

    Write-Info "Stopping Node $($NodeInfo.Id)..."

    try {
        if (-not $NodeInfo.Process.HasExited) {
            $NodeInfo.Process.Kill()
            $NodeInfo.Process.WaitForExit(5000)
        }
        $NodeInfo.LogStream.Close()
        Write-Success "Node $($NodeInfo.Id) stopped"
    } catch {
        Write-Warning "Error stopping Node $($NodeInfo.Id): $_"
    }
}

function Get-NodeRole {
    param($NodeInfo)

    $logContent = Get-Content $NodeInfo.LogFile -Tail 50 -ErrorAction SilentlyContinue

    if ($logContent -match "I AM THE COORDINATOR") {
        return "COORDINATOR"
    } elseif ($logContent -match "FOLLOWER following") {
        return "FOLLOWER"
    } else {
        return "UNKNOWN"
    }
}

function Get-ElectionStats {
    param($NodeInfo)

    $logContent = Get-Content $NodeInfo.LogFile -ErrorAction SilentlyContinue

    $stats = @{
        ElectionsStarted = ($logContent | Select-String "STARTING ELECTION").Count
        ElectionsCompleted = ($logContent | Select-String "ELECTION COMPLETE").Count
        VotesCast = ($logContent | Select-String "I vote for").Count
        HeartbeatsSent = ($logContent | Select-String "Heartbeat.*Sent").Count
        HeartbeatsReceived = ($logContent | Select-String "HB-RX").Count
        ConsensusMessages = ($logContent | Select-String "CONSENSUS-RX").Count
        LeaseAcquisitions = ($logContent | Select-String "Acquired leadership lease").Count
        LeaseRenewals = ($logContent | Select-String "Renewed leadership lease").Count
    }

    return $stats
}

function Show-ClusterStatus {
    Write-Header "Cluster Status"

    $table = @()
    foreach ($node in $script:TestConfig.Nodes) {
        $role = Get-NodeRole -NodeInfo $node
        $running = -not $node.Process.HasExited
        $uptime = (Get-Date) - $node.StartTime

        $table += [PSCustomObject]@{
            Node = "Node $($node.Id)"
            PID = $node.Process.Id
            Port = $node.Port
            Role = $role
            Status = if ($running) { "Running" } else { "Stopped" }
            Uptime = "{0:hh\:mm\:ss}" -f $uptime
        }
    }

    $table | Format-Table -AutoSize
}

function Show-ElectionAnalytics {
    Write-Header "Election Analytics"

    foreach ($node in $script:TestConfig.Nodes) {
        $stats = Get-ElectionStats -NodeInfo $node

        Write-Host "`nNode $($node.Id) Statistics:" -ForegroundColor Yellow
        Write-Host "  Elections Started:    $($stats.ElectionsStarted)"
        Write-Host "  Elections Completed:  $($stats.ElectionsCompleted)"
        Write-Host "  Votes Cast:           $($stats.VotesCast)"
        Write-Host "  Heartbeats Sent:      $($stats.HeartbeatsSent)"
        Write-Host "  Heartbeats Received:  $($stats.HeartbeatsReceived)"
        Write-Host "  Consensus Messages:   $($stats.ConsensusMessages)"
        Write-Host "  Lease Acquisitions:   $($stats.LeaseAcquisitions)"
        Write-Host "  Lease Renewals:       $($stats.LeaseRenewals)"
    }
}

function Test-BasicElection {
    Write-Header "TEST: Basic Election ($NodeCount nodes)"

    # Start bootstrap node
    Write-Info "Starting bootstrap node..."
    $bootstrap = Start-Node -NodeId 1
    Start-Sleep -Seconds 5

    # Get bootstrap multiaddr (simplified - in real scenario parse from logs)
    $bootstrapAddr = "/ip4/127.0.0.1/tcp/$($bootstrap.Port)"

    # Start remaining nodes
    for ($i = 2; $i -le $NodeCount; $i++) {
        Start-Node -NodeId $i -BootstrapAddr $bootstrapAddr
        Start-Sleep -Seconds 3
    }

    Write-Info "Waiting for cluster formation and initial election..."
    Start-Sleep -Seconds 30

    Show-ClusterStatus

    # Check if we have a coordinator
    $coordinator = $script:TestConfig.Nodes | Where-Object { (Get-NodeRole $_) -eq "COORDINATOR" }

    if ($coordinator) {
        Write-Success "Election successful! Node $($coordinator.Id) is COORDINATOR"
    } else {
        Write-Error "No coordinator elected!"
        return $false
    }

    Write-Info "Running for $TestDuration seconds..."
    Start-Sleep -Seconds $TestDuration

    Show-ClusterStatus
    Show-ElectionAnalytics

    return $true
}

function Test-LeaderFailure {
    Write-Header "TEST: Leader Failure Recovery"

    # Start cluster
    Write-Info "Starting initial cluster..."
    $bootstrap = Start-Node -NodeId 1
    Start-Sleep -Seconds 5

    $bootstrapAddr = "/ip4/127.0.0.1/tcp/$($bootstrap.Port)"

    for ($i = 2; $i -le $NodeCount; $i++) {
        Start-Node -NodeId $i -BootstrapAddr $bootstrapAddr
        Start-Sleep -Seconds 3
    }

    Write-Info "Waiting for initial election..."
    Start-Sleep -Seconds 30

    Show-ClusterStatus

    # Find and kill coordinator
    $coordinator = $script:TestConfig.Nodes | Where-Object { (Get-NodeRole $_) -eq "COORDINATOR" }

    if (-not $coordinator) {
        Write-Error "No coordinator found!"
        return $false
    }

    Write-Warning "Killing coordinator Node $($coordinator.Id)..."
    Stop-Node -NodeInfo $coordinator

    Write-Info "Waiting for re-election..."
    Start-Sleep -Seconds 30

    Show-ClusterStatus

    # Check if new coordinator elected
    $newCoordinator = $script:TestConfig.Nodes | Where-Object {
        ($_.Id -ne $coordinator.Id) -and ((Get-NodeRole $_) -eq "COORDINATOR")
    }

    if ($newCoordinator) {
        Write-Success "Re-election successful! Node $($newCoordinator.Id) is new COORDINATOR"
    } else {
        Write-Error "Re-election failed!"
        return $false
    }

    Write-Info "Running for remaining test duration..."
    Start-Sleep -Seconds ($TestDuration - 60)

    Show-ClusterStatus
    Show-ElectionAnalytics

    return $true
}

function Test-NetworkPartition {
    Write-Header "TEST: Network Partition (Split-Brain)"

    Write-Warning "This test requires manual network manipulation"
    Write-Info "Starting cluster..."

    $bootstrap = Start-Node -NodeId 1
    Start-Sleep -Seconds 5

    $bootstrapAddr = "/ip4/127.0.0.1/tcp/$($bootstrap.Port)"

    for ($i = 2; $i -le $NodeCount; $i++) {
        Start-Node -NodeId $i -BootstrapAddr $bootstrapAddr
        Start-Sleep -Seconds 3
    }

    Write-Info "Waiting for initial election..."
    Start-Sleep -Seconds 30

    Show-ClusterStatus

    Write-Info "Cluster is running. Simulate partition by using Windows Firewall rules:"
    Write-Host @"

To create partition:
  netsh advfirewall firewall add rule name="Block_OptimusDB_Node1" dir=in action=block protocol=TCP localport=$($bootstrap.Port)

To restore partition:
  netsh advfirewall firewall delete rule name="Block_OptimusDB_Node1"

"@ -ForegroundColor Yellow

    Write-Info "Running for test duration. Monitor logs for partition behavior..."
    Start-Sleep -Seconds $TestDuration

    Show-ClusterStatus
    Show-ElectionAnalytics

    return $true
}

function Test-RapidLeaderChanges {
    Write-Header "TEST: Rapid Leader Changes"

    Write-Info "Starting cluster..."
    $bootstrap = Start-Node -NodeId 1
    Start-Sleep -Seconds 5

    $bootstrapAddr = "/ip4/127.0.0.1/tcp/$($bootstrap.Port)"

    for ($i = 2; $i -le $NodeCount; $i++) {
        Start-Node -NodeId $i -BootstrapAddr $bootstrapAddr
        Start-Sleep -Seconds 3
    }

    Write-Info "Waiting for initial election..."
    Start-Sleep -Seconds 30

    Show-ClusterStatus

    # Kill and restart coordinators multiple times
    for ($round = 1; $round -le 3; $round++) {
        Write-Info "`nRound $round : Killing coordinator..."

        $coordinator = $script:TestConfig.Nodes | Where-Object { (Get-NodeRole $_) -eq "COORDINATOR" }

        if ($coordinator) {
            Write-Warning "Killing Node $($coordinator.Id)..."
            Stop-Node -NodeInfo $coordinator

            Write-Info "Waiting for re-election..."
            Start-Sleep -Seconds 25

            Show-ClusterStatus
        }
    }

    Show-ElectionAnalytics

    return $true
}

function Test-StressTest {
    Write-Header "TEST: Stress Test (Many Nodes)"

    $stressNodeCount = 10
    Write-Info "Starting $stressNodeCount nodes..."

    $bootstrap = Start-Node -NodeId 1
    Start-Sleep -Seconds 5

    $bootstrapAddr = "/ip4/127.0.0.1/tcp/$($bootstrap.Port)"

    for ($i = 2; $i -le $stressNodeCount; $i++) {
        Start-Node -NodeId $i -BootstrapAddr $bootstrapAddr
        Start-Sleep -Seconds 2
    }

    Write-Info "Waiting for cluster stabilization..."
    Start-Sleep -Seconds 45

    Show-ClusterStatus

    Write-Info "Running stress test for $TestDuration seconds..."
    Start-Sleep -Seconds $TestDuration

    Show-ClusterStatus
    Show-ElectionAnalytics

    return $true
}

function Cleanup-Environment {
    Write-Header "Cleanup"

    Write-Info "Stopping all nodes..."
    foreach ($node in $script:TestConfig.Nodes) {
        Stop-Node -NodeInfo $node
    }

    if ($Cleanup) {
        Write-Info "Cleaning up repositories..."
        for ($i = 1; $i -le 20; $i++) {
            $repo = "node$i"
            if (Test-Path $repo) {
                Remove-Item -Recurse -Force $repo
                Write-Success "Removed $repo"
            }
        }
    }

    Write-Success "Cleanup complete"
}

function Show-TestMenu {
    Write-Host "`n==================================" -ForegroundColor Cyan
    Write-Host "OptimusDB Election Test Suite" -ForegroundColor Cyan
    Write-Host "==================================" -ForegroundColor Cyan
    Write-Host "1. Basic Election Test"
    Write-Host "2. Leader Failure Recovery Test"
    Write-Host "3. Network Partition Test"
    Write-Host "4. Rapid Leader Changes Test"
    Write-Host "5. Stress Test (10 nodes)"
    Write-Host "6. Custom Test (specify parameters)"
    Write-Host "7. View Logs"
    Write-Host "8. Cleanup and Exit"
    Write-Host "==================================" -ForegroundColor Cyan
}

# Main execution
try {
    Initialize-TestEnvironment

    if ($TestScenario -eq "interactive") {
        do {
            Show-TestMenu
            $choice = Read-Host "Select test (1-8)"

            switch ($choice) {
                "1" { Test-BasicElection }
                "2" { Test-LeaderFailure }
                "3" { Test-NetworkPartition }
                "4" { Test-RapidLeaderChanges }
                "5" { Test-StressTest }
                "6" {
                    $NodeCount = Read-Host "Number of nodes"
                    $TestDuration = Read-Host "Test duration (seconds)"
                    Test-BasicElection
                }
                "7" {
                    Write-Header "Recent Logs"
                    Get-ChildItem $LogDir -Filter "*.log" | ForEach-Object {
                        Write-Host "`n=== $($_.Name) ===" -ForegroundColor Yellow
                        Get-Content $_.FullName -Tail 20
                    }
                }
                "8" { break }
                default { Write-Warning "Invalid choice" }
            }

            if ($choice -ne "7" -and $choice -ne "8") {
                Cleanup-Environment
                Write-Host "`nPress Enter to continue..." -ForegroundColor Gray
                Read-Host
            }
        } while ($choice -ne "8")
    } else {
        # Run specified test scenario
        $success = switch ($TestScenario) {
            "basic" { Test-BasicElection }
            "failure" { Test-LeaderFailure }
            "partition" { Test-NetworkPartition }
            "rapid" { Test-RapidLeaderChanges }
            "stress" { Test-StressTest }
            default {
                Write-Error "Unknown test scenario: $TestScenario"
                $false
            }
        }

        Cleanup-Environment

        if ($success) {
            Write-Success "`n✅ TEST PASSED"
            exit 0
        } else {
            Write-Error "`n❌ TEST FAILED"
            exit 1
        }
    }
} catch {
    Write-Error "Test failed with error: $_"
    Write-Host $_.ScriptStackTrace
    Cleanup-Environment
    exit 1
} finally {
    # Ensure cleanup on exit
    if ($script:TestConfig.Nodes) {
        foreach ($node in $script:TestConfig.Nodes) {
            if ($node.Process -and -not $node.Process.HasExited) {
                try { $node.Process.Kill() } catch {}
            }
            if ($node.LogStream) {
                try { $node.LogStream.Close() } catch {}
            }
        }
    }
}

Write-Success "`nTest suite finished"