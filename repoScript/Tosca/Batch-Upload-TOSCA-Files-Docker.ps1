<#
.SYNOPSIS
    Batch upload all TOSCA sample files to OptimusDB Docker containers

.DESCRIPTION
    This script uploads all 5 types of TOSCA files to OptimusDB containers
    running in Docker Desktop.

.PARAMETER ToscaDirectory
    Directory containing the TOSCA sample files (default: current directory)

.PARAMETER ContainerPattern
    Docker container name pattern (default: optimusdb*)

.PARAMETER DelayBetweenUploads
    Delay in seconds between uploads (default: 3)

.EXAMPLE
    .\Batch-Upload-TOSCA-Files-Docker.ps1

.EXAMPLE
    .\Batch-Upload-TOSCA-Files-Docker.ps1 -ToscaDirectory "C:\tosca-samples"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [string]$ToscaDirectory = ".",

    [Parameter(Mandatory=$false)]
    [string]$ContainerPattern = "optimusdb*",

    [Parameter(Mandatory=$false)]
    [int]$DelayBetweenUploads = 3
)

# TOSCA files configuration
$toscaFiles = @(
    @{
        File = "sample_1_application_description.yaml"
        Type = "ApplicationDescription"
        Description = "E-commerce web application with microservices"
        Datastore = "ADT Datastore"
    },
    @{
        File = "sample_2_capacity_description.yaml"
        Type = "CapacityDescription"
        Description = "Edge cluster infrastructure capacity"
        Datastore = "Capacity Descriptions Datastore"
    },
    @{
        File = "sample_3_opentofu_tosca_template.yaml"
        Type = "OpenTofuTemplate"
        Description = "Hybrid Kubernetes infrastructure provisioning"
        Datastore = "OpenTofu/TOSCA Templates Datastore"
    },
    @{
        File = "sample_4_deployment_release_plan.yaml"
        Type = "DeploymentPlan"
        Description = "Executable deployment plan with resource allocations"
        Datastore = "Deployment/Release Plans Datastore"
    },
    @{
        File = "sample_5_application_requirements.yaml"
        Type = "ApplicationRequirements"
        Description = "ML training workload requirements with GPU"
        Datastore = "N/A (Submitted)"
    }
)

Write-Host @"
╔════════════════════════════════════════════════════════════════╗
║   Swarmchestrate - Batch TOSCA Upload Script                  ║
║         Upload All Sample Files to Docker Containers          ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

Write-Host "`nConfiguration:" -ForegroundColor Yellow
Write-Host "  TOSCA Directory:    $ToscaDirectory" -ForegroundColor Gray
Write-Host "  Container Pattern:  $ContainerPattern" -ForegroundColor Gray
Write-Host "  Delay:              $DelayBetweenUploads seconds" -ForegroundColor Gray

# Check if upload script exists
$uploadScript = Join-Path $PSScriptRoot "TOSCA-Upload-Query-Docker.ps1"
if (-not (Test-Path $uploadScript)) {
    Write-Error "TOSCA-Upload-Query-Docker.ps1 not found in the same directory"
    Write-Host "`nExpected location: $uploadScript" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n✓ Upload script found: $uploadScript" -ForegroundColor Green

# Verify Docker is running
Write-Host "`nChecking Docker..." -ForegroundColor Yellow
$dockerInfo = docker info 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker is not running or not accessible"
    Write-Host "Please start Docker Desktop and try again" -ForegroundColor Red
    exit 1
}
Write-Host "✓ Docker is running" -ForegroundColor Green

# Check for OptimusDB containers
Write-Host "`nChecking for OptimusDB containers..." -ForegroundColor Yellow
$containers = docker ps --filter "name=$ContainerPattern" --format "{{.Names}}" 2>&1
if ($LASTEXITCODE -ne 0 -or -not $containers) {
    Write-Error "No OptimusDB containers found"
    Write-Host "`nExpected container pattern: $ContainerPattern" -ForegroundColor Yellow
    Write-Host "Running containers:" -ForegroundColor Yellow
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    exit 1
}

$containerCount = ($containers | Measure-Object).Count
Write-Host "✓ Found $containerCount OptimusDB container(s)" -ForegroundColor Green
$containers | ForEach-Object { Write-Host "  - $_" -ForegroundColor Gray }

# Initialize results tracking
$results = @()
$startTime = Get-Date

# Process each TOSCA file
Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║ Starting Batch Upload                                         ║" -ForegroundColor Magenta
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

$fileNumber = 1
foreach ($toscaConfig in $toscaFiles) {
    # Resolve the full path to handle relative paths correctly
    if ([System.IO.Path]::IsPathRooted($ToscaDirectory)) {
        $filePath = Join-Path $ToscaDirectory $toscaConfig.File
    } else {
        $fullDirectory = Resolve-Path $ToscaDirectory -ErrorAction SilentlyContinue
        if ($fullDirectory) {
            $filePath = Join-Path $fullDirectory $toscaConfig.File
        } else {
            $filePath = Join-Path (Get-Location).Path $toscaConfig.File
        }
    }

    Write-Host "`n[$fileNumber/$($toscaFiles.Count)] Processing: $($toscaConfig.File)" -ForegroundColor Yellow
    Write-Host "    Full Path:   $filePath" -ForegroundColor Gray
    Write-Host "    Type:        $($toscaConfig.Type)" -ForegroundColor Gray
    Write-Host "    Description: $($toscaConfig.Description)" -ForegroundColor Gray
    Write-Host "    Datastore:   $($toscaConfig.Datastore)" -ForegroundColor Gray

    if (-not (Test-Path $filePath)) {
        Write-Warning "File not found: $filePath"
        $results += @{
            File = $toscaConfig.File
            Type = $toscaConfig.Type
            Status = "Failed"
            Error = "File not found"
            TemplateId = $null
        }
        $fileNumber++
        continue
    }

    # Execute upload
    Write-Host "    Uploading..." -ForegroundColor Cyan

    try {
        # Call the main upload script
        $output = & $uploadScript `
            -ToscaFile $filePath `
            -ToscaType $toscaConfig.Type `
            -ContainerPattern $ContainerPattern `
            -ErrorAction Stop 2>&1

        # Parse output to extract template ID
        $templateIdMatch = $output | Select-String -Pattern "Template ID:\s+(\S+)"
        $templateId = if ($templateIdMatch) { $templateIdMatch.Matches[0].Groups[1].Value } else { "Unknown" }

        Write-Host "    ✓ Upload completed" -ForegroundColor Green

        $results += @{
            File = $toscaConfig.File
            Type = $toscaConfig.Type
            Status = "Success"
            Error = $null
            TemplateId = $templateId
        }

    } catch {
        Write-Warning "Upload failed: $($_.Exception.Message)"
        $results += @{
            File = $toscaConfig.File
            Type = $toscaConfig.Type
            Status = "Failed"
            Error = $_.Exception.Message
            TemplateId = $null
        }
    }

    # Delay before next upload
    if ($fileNumber -lt $toscaFiles.Count) {
        Write-Host "    Waiting $DelayBetweenUploads seconds before next upload..." -ForegroundColor Gray
        Start-Sleep -Seconds $DelayBetweenUploads
    }

    $fileNumber++
}

# Generate summary
$endTime = Get-Date
$duration = $endTime - $startTime

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Green
Write-Host "║ Batch Upload Summary                                          ║" -ForegroundColor Green
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Green

Write-Host "`nExecution Time: $($duration.TotalSeconds) seconds" -ForegroundColor Cyan

$successCount = ($results | Where-Object { $_.Status -eq "Success" }).Count
$failCount = ($results | Where-Object { $_.Status -eq "Failed" }).Count

Write-Host "`nResults:" -ForegroundColor Yellow
Write-Host "  Total Files:      $($toscaFiles.Count)" -ForegroundColor Gray
Write-Host "  Successful:       $successCount" -ForegroundColor Green
Write-Host "  Failed:           $failCount" -ForegroundColor $(if ($failCount -gt 0) { "Red" } else { "Gray" })

Write-Host "`nDetailed Results:" -ForegroundColor Yellow
Write-Host ("-" * 80) -ForegroundColor Gray

foreach ($result in $results) {
    $statusColor = if ($result.Status -eq "Success") { "Green" } else { "Red" }
    $statusIcon = if ($result.Status -eq "Success") { "✓" } else { "✗" }

    Write-Host "$statusIcon $($result.File)" -ForegroundColor $statusColor
    Write-Host "    Type:        $($result.Type)" -ForegroundColor Gray
    Write-Host "    Status:      $($result.Status)" -ForegroundColor $statusColor

    if ($result.TemplateId) {
        Write-Host "    Template ID: $($result.TemplateId)" -ForegroundColor Gray
    }

    if ($result.Error) {
        Write-Host "    Error:       $($result.Error)" -ForegroundColor Red
    }

    Write-Host ""
}

# Export results to JSON
$resultsFile = "batch-upload-results-$(Get-Date -Format 'yyyyMMdd-HHmmss').json"
$results | ConvertTo-Json -Depth 5 | Out-File $resultsFile -Encoding UTF8
Write-Host "Results exported to: $resultsFile" -ForegroundColor Cyan

# Generate verification queries
Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║ Verification Queries                                          ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`nTo verify all uploaded files, run these SQL queries:" -ForegroundColor Yellow

$sqlVerification = @"

-- Query 1: List all uploaded TOSCA files
SELECT filename, template_id, filesize_bytes, created_at, uploader
FROM toscametadata
ORDER BY created_at DESC
LIMIT 10;

-- Query 2: Count files by type
SELECT
    CASE
        WHEN filename LIKE '%application_description%' THEN 'Application Description'
        WHEN filename LIKE '%capacity_description%' THEN 'Capacity Description'
        WHEN filename LIKE '%opentofu%' THEN 'OpenTofu Template'
        WHEN filename LIKE '%deployment%' THEN 'Deployment Plan'
        WHEN filename LIKE '%requirements%' THEN 'Application Requirements'
        ELSE 'Other'
    END as tosca_type,
    COUNT(*) as file_count,
    SUM(filesize_bytes) as total_size_bytes
FROM toscametadata
GROUP BY tosca_type;

-- Query 3: Get files uploaded in this batch
SELECT filename, template_id, created_at
FROM toscametadata
WHERE datetime(created_at) > datetime('now', '-1 hour')
ORDER BY created_at DESC;

"@

Write-Host $sqlVerification -ForegroundColor Gray

Write-Host "`n✓ Batch upload completed!" -ForegroundColor Green
Write-Host "`nDocker Container Info:" -ForegroundColor Cyan
docker ps --filter "name=$ContainerPattern" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"