# ============================================================================
# laptop_portcheck.ps1  —  run this ON your Windows laptop (PowerShell)
#
# Two jobs:
#   1) INBOUND-TO-ZSTAKI: probe whether zstaki accepts connections on each port.
#      For a TRUE result you must first start a listener on zstaki for that port
#      (./zstaki_portcheck.sh listen <port>) — otherwise an open security group
#      but no listener still shows as closed.
#   2) LAPTOP OUTBOUND: confirm your laptop itself can dial those ports outward
#      (rules out YOUR network being the blocker).
#
# Usage (PowerShell):
#   # If scripts are blocked:  Set-ExecutionPolicy -Scope Process Bypass
#   .\laptop_portcheck.ps1                       # probe default ports on zstaki
#   .\laptop_portcheck.ps1 -Target 193.225.250.240 -Ports 443,4001,80
#   .\laptop_portcheck.ps1 -OutboundHost portquiz.net   # test laptop egress
#
# READS with the server script:
#   1. On zstaki:  sudo ./zstaki_portcheck.sh listen 443
#   2. On laptop:  .\laptop_portcheck.ps1 -Ports 443
#   3. TRUE  = inbound OPEN (security group allows it)
#      FALSE = blocked upstream, OR no listener running on zstaki for that port
# ============================================================================

param(
    [string]   $Target        = "193.225.250.240",   # zstaki public IP
    [int[]]    $Ports         = @(22, 80, 443, 4001, 8080, 9000, 30011),
    [string]   $OutboundHost  = "",                    # set to test laptop egress
    [int]      $TimeoutMs     = 4000
)

# NOTE: parameter is $RemoteHost, NOT $Host — $Host is a reserved automatic
# variable in PowerShell (the console host) and cannot be assigned to.
function Test-Port {
    param([string]$RemoteHost, [int]$Port, [int]$TimeoutMs)
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $iar = $client.BeginConnect($RemoteHost, $Port, $null, $null)
        $ok  = $iar.AsyncWaitHandle.WaitOne($TimeoutMs, $false)
        if ($ok -and $client.Connected) { $client.EndConnect($iar); return $true }
        return $false
    } catch { return $false }
    finally { $client.Close() }
}

Write-Host ""
Write-Host "=== Probing INBOUND reachability of $Target ===" -ForegroundColor Cyan
Write-Host "    (TRUE needs a matching listener running on zstaki for that port)" -ForegroundColor DarkGray
Write-Host ""
"{0,-8} {1}" -f "PORT", "RESULT"

foreach ($p in $Ports) {
    $open = Test-Port -RemoteHost $Target -Port $p -TimeoutMs $TimeoutMs
    if ($open) {
        Write-Host ("{0,-8} OPEN  (inbound allowed + listener up)" -f $p) -ForegroundColor Green
    } else {
        Write-Host ("{0,-8} closed/filtered (blocked, or no listener on zstaki)" -f $p) -ForegroundColor Red
    }
}

if ($OutboundHost -ne "") {
    Write-Host ""
    Write-Host "=== Testing LAPTOP OUTBOUND to $OutboundHost ===" -ForegroundColor Cyan
    Write-Host "    (portquiz.net answers on all ports; confirms YOUR network egress)" -ForegroundColor DarkGray
    Write-Host ""
    "{0,-8} {1}" -f "PORT", "RESULT"
    foreach ($p in $Ports) {
        $open = Test-Port -RemoteHost $OutboundHost -Port $p -TimeoutMs $TimeoutMs
        if ($open) {
            Write-Host ("{0,-8} OPEN  (laptop can dial out)" -f $p) -ForegroundColor Green
        } else {
            Write-Host ("{0,-8} blocked (your network filters this egress)" -f $p) -ForegroundColor Yellow
        }
    }
}

Write-Host ""
Write-Host "Tip: the two cells that decide your OptimusDB plan are :443 inbound" -ForegroundColor Cyan
Write-Host "     (enables the raw-TCP-proxy path) and :22 inbound (SSH tunnel path)." -ForegroundColor Cyan
Write-Host ""