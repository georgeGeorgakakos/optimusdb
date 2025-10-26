# Deploy-OptimusDB.ps1
# PowerShell script to deploy 8 OptimusDB containers on the 'swarmnet' network.

# ===========================================
# Configuration
# ===========================================
$networkName = "swarmnet"
$imageName   = "optimusdb"
$containerCount = 8

# Base ports (host → container)
$baseHttpPort = 18000
$baseP2PPort  = 14000
$baseRPCPort  = 15000
$internalHttp = 8089
$internalP2P  = 4001
$internalRPC  = 5001

# ===========================================
# Script Start
# ===========================================
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "  OptimusDB Deployment Script (PowerShell) " -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host ""

# Ensure Docker network exists
$networkExists = docker network ls --format '{{.Name}}' | Where-Object { $_ -eq $networkName }
if (-not $networkExists) {
    Write-Host "Creating Docker network '$networkName'..." -ForegroundColor Yellow
    docker network create $networkName | Out-Null
} else {
    Write-Host "Docker network '$networkName' already exists." -ForegroundColor Green
}

Write-Host ""
Write-Host "Starting $containerCount OptimusDB containers..." -ForegroundColor Cyan
Write-Host ""

# ===========================================
# Container Deployment Loop
# ===========================================
for ($i = 1; $i -le $containerCount; $i++) {

    $containerName = "optimusdb$i"
    $httpPort = $baseHttpPort + $i
    $p2pPort  = $baseP2PPort  + $i
    $rpcPort  = $baseRPCPort  + $i

    # Remove container if it already exists
    $exists = docker ps -a --format '{{.Names}}' | Where-Object { $_ -eq $containerName }
    if ($exists) {
        Write-Host "⚠️  Container '$containerName' already exists. Removing old instance..." -ForegroundColor Yellow
        docker rm -f $containerName | Out-Null
    }

    Write-Host "▶️  Starting $containerName ..." -ForegroundColor Cyan
    Write-Host "   Host Ports: HTTP=$httpPort, P2P=$p2pPort, RPC=$rpcPort"

    try {
        docker run -d `
            --network $networkName `
            --name $containerName `
            -p "$($httpPort):$($internalHttp)" `
            -p "$($p2pPort):$($internalP2P)" `
            -p "$($rpcPort):$($internalRPC)" `
            $imageName | Out-Null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ $containerName started successfully." -ForegroundColor Green
        } else {
            Write-Host "   ❌ Failed to start $containerName (exit code $LASTEXITCODE)." -ForegroundColor Red
        }
    }
    catch {
        Write-Host "   ⚠️ Error launching $containerName : $_" -ForegroundColor Red
    }

    Write-Host ""
}

# ===========================================
# Final Status Summary
# ===========================================
Write-Host "===========================================" -ForegroundColor Cyan
Write-Host "All containers processed. Current status:" -ForegroundColor Cyan
Write-Host "===========================================" -ForegroundColor Cyan

docker ps --filter "network=$networkName"

Write-Host "`n✅ Deployment complete!" -ForegroundColor Green
