<#
Deploy-And-Test-OptimusDB.ps1
- Deploys N OptimusDB containers (optimusdb1..N) on 'swarmnet' with fixed port maps
- Waits for discovery/mesh + first reputation publish
- Collects logs safely (timeout + --since + --tail) and analyzes election
- PS 5.1 compatible

Examples:
  ./Deploy-And-Test-OptimusDB.ps1
  ./Deploy-And-Test-OptimusDB.ps1 -ContainerCount 6 -ImageName optimusdb:latest
  ./Deploy-And-Test-OptimusDB.ps1 -SkipDeploy
#>

[CmdletBinding()]
param(
    [string]$NetworkName    = "swarmnet",
    [string]$ImageName      = "optimusdb",
    [int]$ContainerCount    = 8,

# host → container ports
    [int]$BaseHttpPort      = 18000,
    [int]$BaseP2PPort       = 14000,
    [int]$BaseRPCPort       = 15000,
    [int]$InternalHttp      = 8089,
    [int]$InternalP2P       = 4001,
    [int]$InternalRPC       = 5001,

# timing
    [int]$DiscoveryWaitSec  = 25,
    [int]$ReputationWaitSec = 35,

# log capture knobs
    [int]$LogSinceSec       = 600,     # last 10 minutes
    [int]$LogTailLines      = 5000,    # at most 5k lines
    [int]$LogTimeoutSec     = 10,      # per-container timeout

# behavior
    [switch]$SkipDeploy,
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"

function Write-Info { param($m) Write-Host "[INFO]  $m" -ForegroundColor Cyan }
function Write-Ok   { param($m) Write-Host "[OK]    $m" -ForegroundColor Green }
function Write-Warn { param($m) Write-Host "[WARN]  $m" -ForegroundColor Yellow }
function Write-Err  { param($m) Write-Host "[ERROR] $m" -ForegroundColor Red }

# Safe docker logs capture (bounded + timeout). Falls back to cmd redirection if needed.
function Save-DockerLogs {
    param(
        [Parameter(Mandatory=$true)][string]$Container,
        [Parameter(Mandatory=$true)][string]$OutFile,
        [int]$SinceSec   = 300,
        [int]$TailLines  = 2000,
        [int]$TimeoutSec = 10
    )

    # Prefer bounded logs
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "docker"
    $psi.Arguments = "logs --since ${SinceSec}s --tail ${TailLines} $Container"
    $psi.UseShellExecute = $false
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError  = $true

    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi

    if (-not $p.Start()) {
        Write-Warn "Failed to start 'docker logs' for $Container, using fallback."
        & cmd /c "docker logs --since ${SinceSec}s --tail ${TailLines} $Container > `"$OutFile`" 2>&1"
        return 0
    }

    # Async readers (avoid deadlocks)
    $stdoutTask = [System.Threading.Tasks.Task[string]]::Factory.StartNew({ param($s) $s.ReadToEnd() }, $p.StandardOutput)
    $stderrTask = [System.Threading.Tasks.Task[string]]::Factory.StartNew({ param($s) $s.ReadToEnd() }, $p.StandardError)

    if (-not $p.WaitForExit($TimeoutSec * 1000)) {
        try { $p.Kill() } catch {}
        Write-Warn "docker logs timed out after $TimeoutSec s for $Container; using fallback."
        & cmd /c "docker logs --since ${SinceSec}s --tail ${TailLines} $Container > `"$OutFile`" 2>&1"
        return 0
    }

    $out = $stdoutTask.Result
    $err = $stderrTask.Result
            ($out + "`r`n" + $err) | Out-File -FilePath $OutFile -Encoding UTF8

    return $p.ExitCode
}

function Get-TargetContainers {
    param([int]$Count,[int]$BaseHttp,[int]$BaseP2P,[int]$BaseRPC)
    $list = @()
    for($i=1;$i -le $Count;$i++){
        $list += @{
            Name = "optimusdb$($i)"
            Http = $BaseHttp + $i
            P2P  = $BaseP2P  + $i
            RPC  = $BaseRPC  + $i
        }
    }
    return ,$list
}

# --- Setup working dirs
$ts = (Get-Date).ToString("yyyyMMdd_HHmmss")
$WorkDir = Join-Path $PWD "election_test_$ts"
$LogsDir = Join-Path $WorkDir "logs"
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Deploy + Election Test         " -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host ""

# --- Ensure network
if (-not $SkipDeploy) {
    $networkExists = docker network ls --format "{{.Name}}" 2>$null | Where-Object { $_ -eq $NetworkName }
    if (-not $networkExists) {
        Write-Info "Creating Docker network '$NetworkName'..."
        docker network create $NetworkName | Out-Null
    } else {
        Write-Info "Docker network '$NetworkName' already exists."
    }
}

$targets = Get-TargetContainers -Count $ContainerCount -BaseHttp $BaseHttpPort -BaseP2P $BaseP2PPort -BaseRPC $BaseRPCPort

# --- Deploy
if (-not $SkipDeploy) {
    Write-Host ""
    Write-Info "Starting $ContainerCount OptimusDB containers on '$NetworkName'..."
    Write-Host ""

    foreach($t in $targets){
        $containerName = $t.Name
        $httpPort = $t.Http
        $p2pPort  = $t.P2P
        $rpcPort  = $t.RPC

        $exists = docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $containerName }
        if ($exists) {
            Write-Warn "Container '$containerName' already exists. Removing old instance…"
            docker rm -f $containerName | Out-Null
        }

        Write-Info ("Launching {0} (HTTP={1}, P2P={2}, RPC={3})" -f $containerName,$httpPort,$p2pPort,$rpcPort)
        try {
            docker run -d `
        --network $NetworkName `
        --name   $containerName `
        -p "$($httpPort):$($InternalHttp)" `
        -p "$($p2pPort):$($InternalP2P)" `
        -p "$($rpcPort):$($InternalRPC)" `
        $ImageName | Out-Null

            if ($LASTEXITCODE -eq 0) {
                Write-Ok "$containerName started."
            } else {
                Write-Err "Failed to start $containerName (exit $LASTEXITCODE)."
            }
        } catch {
            Write-Err "Error launching $containerName : $_"
        }
    }

    Write-Host ""
    Write-Host "===========================================" -ForegroundColor Cyan
    Write-Host "Current status:" -ForegroundColor Cyan
    Write-Host "===========================================" -ForegroundColor Cyan
    docker ps --filter "network=$NetworkName"
}

# --- Waits
Write-Info ("Waiting {0}s for discovery + mesh formation…" -f $DiscoveryWaitSec)
Start-Sleep -Seconds $DiscoveryWaitSec

Write-Info ("Waiting {0}s for reputation cycle…" -f $ReputationWaitSec)
Start-Sleep -Seconds $ReputationWaitSec

# --- Collect logs snapshot (bounded, timeout, fallback)
Write-Info "Collecting logs snapshot…"
foreach($t in $targets){
    $logFile = Join-Path $LogsDir "$($t.Name).log"
    $exit = Save-DockerLogs -Container $t.Name -OutFile $logFile -SinceSec $LogSinceSec -TailLines $LogTailLines -TimeoutSec $LogTimeoutSec
    if ($exit -ne 0) {
        Write-Warn ("docker logs exited with code {0} for {1} — continuing" -f $exit, $t.Name)
    }
}

# --- Analyze logs
$rxLeaderAnnounce = [regex] 'Announcing new leader:\s+([A-Za-z0-9]+)'
$rxCoordNow       = [regex] '\[COORDINATOR\].*Node\s+([A-Za-z0-9]+)\s+is now acting as leader.*term\s+(\d+)'
$rxHeartbeatSent  = [regex] '\[HEARTBEAT\]\s+Sent heartbeat from leader:\s+([A-Za-z0-9]+)\s+\(term\s+(\d+)\)'
$rxHeartbeatRecv  = [regex] '\[HEARTBEAT\]\s+Received from:\s+([A-Za-z0-9]+)\s+\(term\s+(\d+)\)'
$rxSplitBrain     = [regex] '\[SPLIT-BRAIN\]'

$leaders = @{}
$hbSent  = @{}
$hbRecv  = @{}
$splitBrainHits = 0

foreach($t in $targets){
    $log = Join-Path $LogsDir "$($t.Name).log"
    if (-not (Test-Path $log)) { continue }
    $content = Get-Content -Raw -Path $log

    foreach($m in $rxLeaderAnnounce.Matches($content)){ $leaders[$m.Groups[1].Value] = $true }
    foreach($m in $rxCoordNow.Matches($content)){       $leaders[$m.Groups[1].Value] = $true }

    foreach($m in $rxHeartbeatSent.Matches($content)){
        $lid = $m.Groups[1].Value
        if (-not $hbSent.ContainsKey($lid)) { $hbSent[$lid] = 0 }
        $hbSent[$lid] = $hbSent[$lid] + 1
    }
    foreach($m in $rxHeartbeatRecv.Matches($content)){
        $lid = $m.Groups[1].Value
        if (-not $hbRecv.ContainsKey($lid)) { $hbRecv[$lid] = 0 }
        $hbRecv[$lid] = $hbRecv[$lid] + 1
    }

    if ($rxSplitBrain.IsMatch($content)) { $splitBrainHits = $splitBrainHits + 1 }
}

$uniqueLeaders = $leaders.Keys
if ($uniqueLeaders -and $uniqueLeaders.Count -ge 1) {
    Write-Info ("Leader(s) detected: " + ($uniqueLeaders -join ", "))
} else {
    Write-Info "Leader(s) detected: (none)"
}

# --- Evaluate
$pass = $true

if (-not $uniqueLeaders -or $uniqueLeaders.Count -eq 0) {
    Write-Err "No leader announcement found in logs."
    $pass = $false
} elseif ($uniqueLeaders.Count -gt 1) {
    Write-Err "Multiple leaders detected → split-brain suspected."
    $pass = $false
} else {
    Write-Ok  ("Exactly one leader detected: {0}" -f $uniqueLeaders[0])
}

if ($splitBrainHits -gt 0) {
    Write-Err ("Split-brain warnings found in logs ({0} occurrences)." -f $splitBrainHits)
    $pass = $false
}

if ($uniqueLeaders -and $uniqueLeaders.Count -ge 1) {
    $leaderId = $uniqueLeaders[0]
    $leaderSent = 0
    if ($hbSent.ContainsKey($leaderId)) { $leaderSent = $hbSent[$leaderId] }

    if ($leaderSent -lt 1) {
        Write-Err "No 'Sent heartbeat' lines for the elected leader."
        $pass = $false
    } else {
        Write-Ok  ("Leader heartbeat sent x{0}" -f $leaderSent)
    }

    $expectedFollowers = $ContainerCount - 1
    $recvCount = 0
    if ($hbRecv.ContainsKey($leaderId)) { $recvCount = $hbRecv[$leaderId] }

    if ($recvCount -lt $expectedFollowers) {
        Write-Warn ("Followers did not all confirm heartbeat reception (received count: {0}, expected ≥ {1})" -f $recvCount,$expectedFollowers)
    } else {
        Write-Ok  "Followers reported receiving heartbeats from the leader."
    }
}

# --- Summary
Write-Host ""
Write-Host "====================== SUMMARY =======================" -ForegroundColor White
"{0,-14} {1,-14} {2}" -f "Container","Ports","Log" | Write-Host
"{0,-14} {1,-14} {2}" -f "---------","-----","---" | Write-Host
foreach($t in $targets){
    $log = Join-Path $LogsDir "$($t.Name).log"
    $ports = "H:$($t.Http),P:$($t.P2P),R:$($t.RPC)"
    "{0,-14} {1,-14} {2}" -f $t.Name, $ports, $log | Write-Host
}

if ($pass) {
    Write-Ok "Election test PASSED."
} else {
    Write-Err "Election test FAILED. Inspect logs under: $LogsDir"
}

# --- Optional teardown
if (-not $KeepRunning -and -not $SkipDeploy) {
    Write-Info "Stopping and removing containers…"
    foreach($t in $targets){
        docker rm -f $($t.Name) | Out-Null
    }
    Write-Info "Done. Logs preserved under: $LogsDir"
} elseif ($KeepRunning) {
    Write-Warn "Keeping containers running. Logs under: $LogsDir"
} else {
    Write-Info "Analysis-only run complete. Logs under: $LogsDir"
}
