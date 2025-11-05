<#
.SYNOPSIS
    Pre-flight check script for TOSCA upload to Docker containers

.DESCRIPTION
    Verifies that everything is ready before running the batch upload
#>

Write-Host @"
╔════════════════════════════════════════════════════════════════╗
║         Pre-Flight Check - Docker TOSCA Upload                ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

$allGood = $true

# Check 1: Current Directory
Write-Host "`n[1/6] Checking current directory..." -ForegroundColor Yellow
$currentDir = Get-Location
Write-Host "Current directory: $currentDir" -ForegroundColor Gray

# Check 2: Docker
Write-Host "`n[2/6] Checking Docker..." -ForegroundColor Yellow
try {
    $dockerVersion = docker --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Docker is installed: $dockerVersion" -ForegroundColor Green
    } else {
        Write-Host "✗ Docker command failed" -ForegroundColor Red
        $allGood = $false
    }
} catch {
    Write-Host "✗ Docker not found" -ForegroundColor Red
    $allGood = $false
}

# Check 3: Docker Running
Write-Host "`n[3/6] Checking if Docker is running..." -ForegroundColor Yellow
try {
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✓ Docker is running" -ForegroundColor Green
    } else {
        Write-Host "✗ Docker is not running - Please start Docker Desktop" -ForegroundColor Red
        $allGood = $false
    }
} catch {
    Write-Host "✗ Cannot connect to Docker" -ForegroundColor Red
    $allGood = $false
}

# Check 4: OptimusDB Containers
Write-Host "`n[4/6] Checking OptimusDB containers..." -ForegroundColor Yellow
$containers = docker ps --filter "name=optimusdb*" --format "{{.Names}}" 2>&1
if ($LASTEXITCODE -eq 0 -and $containers) {
    $containerCount = ($containers | Measure-Object).Count
    Write-Host "✓ Found $containerCount OptimusDB container(s)" -ForegroundColor Green
    $containers | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }
} else {
    Write-Host "✗ No OptimusDB containers found" -ForegroundColor Red
    Write-Host "  Run: docker ps" -ForegroundColor Yellow
    $allGood = $false
}

# Check 5: TOSCA Files
Write-Host "`n[5/6] Checking TOSCA sample files..." -ForegroundColor Yellow
$toscaFiles = @(
    "sample_1_application_description.yaml",
    "sample_2_capacity_description.yaml",
    "sample_3_opentofu_tosca_template.yaml",
    "sample_4_deployment_release_plan.yaml",
    "sample_5_application_requirements.yaml"
)

$missingFiles = @()
foreach ($file in $toscaFiles) {
    if (Test-Path $file) {
        Write-Host "✓ $file" -ForegroundColor Green
    } else {
        Write-Host "✗ $file - NOT FOUND" -ForegroundColor Red
        $missingFiles += $file
        $allGood = $false
    }
}

if ($missingFiles.Count -gt 0) {
    Write-Host "`nMissing files:" -ForegroundColor Red
    $missingFiles | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    Write-Host "`nMake sure you're in the correct directory!" -ForegroundColor Yellow
    Write-Host "Current directory: $currentDir" -ForegroundColor Yellow
}

# Check 6: Upload Scripts
Write-Host "`n[6/6] Checking upload scripts..." -ForegroundColor Yellow
$scripts = @(
    "TOSCA-Upload-Query-Docker.ps1",
    "Batch-Upload-TOSCA-Files-Docker.ps1"
)

foreach ($script in $scripts) {
    if (Test-Path $script) {
        Write-Host "✓ $script" -ForegroundColor Green
    } else {
        Write-Host "✗ $script - NOT FOUND" -ForegroundColor Red
        Write-Host "  Download from the outputs folder" -ForegroundColor Yellow
        $allGood = $false
    }
}

# Test Connectivity
Write-Host "`n[BONUS] Testing connectivity..." -ForegroundColor Yellow
try {
    $testUrl = "http://localhost:18001/swarmkb/peers"
    Write-Host "Testing: $testUrl" -ForegroundColor Gray
    $response = Invoke-RestMethod -Uri $testUrl -TimeoutSec 3 -ErrorAction Stop
    Write-Host "✓ API is reachable" -ForegroundColor Green
    if ($response.peers) {
        Write-Host "  Connected peers: $($response.peers.Count)" -ForegroundColor Gray
    }
} catch {
    Write-Host "✗ Cannot reach API at localhost:18001" -ForegroundColor Red
    Write-Host "  Check if containers are running: docker ps" -ForegroundColor Yellow
    $allGood = $false
}

# Summary
Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
if ($allGood) {
    Write-Host "║ ✓ ALL CHECKS PASSED - Ready to upload!                       ║" -ForegroundColor Green
    Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host "`nRun this command to upload:" -ForegroundColor Yellow
    Write-Host "  .\Batch-Upload-TOSCA-Files-Docker.ps1" -ForegroundColor White
} else {
    Write-Host "║ ✗ SOME CHECKS FAILED - Please fix issues above               ║" -ForegroundColor Red
    Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan
    Write-Host "`nFix the issues above before running the upload script" -ForegroundColor Yellow
}

Write-Host ""