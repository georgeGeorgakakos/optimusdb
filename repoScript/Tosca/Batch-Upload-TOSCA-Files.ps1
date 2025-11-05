<#
.SYNOPSIS
    Batch upload all Swarmchestrate TOSCA sample files to OptimusDB Knowledge Base

.DESCRIPTION
    This script uploads all 5 types of TOSCA files created for the Swarmchestrate
    decentralized Knowledge Base system. It demonstrates the complete lifecycle of
    TOSCA file management across different datastores.

.PARAMETER Mode
    Discovery mode: 'lb' (LoadBalancer), 'pod' (Pod IPs), or 'headless' (Headless DNS)

.PARAMETER Namespace
    Kubernetes namespace where OptimusDB is deployed (default: default)

.PARAMETER ToscaDirectory
    Directory containing the TOSCA sample files (default: current directory)

.EXAMPLE
    .\Batch-Upload-TOSCA-Files.ps1

.EXAMPLE
    .\Batch-Upload-TOSCA-Files.ps1 -Mode pod -Namespace swarmkb-ns

.EXAMPLE
    .\Batch-Upload-TOSCA-Files.ps1 -ToscaDirectory "C:\tosca-samples"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("lb", "pod", "headless")]
    [string]$Mode = "lb",

    [Parameter(Mandatory=$false)]
    [string]$Namespace = "default",

    [Parameter(Mandatory=$false)]
    [string]$ToscaDirectory = ".",

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
║   Upload All Sample Files to Knowledge Base                   ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

Write-Host "`nConfiguration:" -ForegroundColor Yellow
Write-Host "  Mode:            $Mode" -ForegroundColor Gray
Write-Host "  Namespace:       $Namespace" -ForegroundColor Gray
Write-Host "  TOSCA Directory: $ToscaDirectory" -ForegroundColor Gray
Write-Host "  Delay:           $DelayBetweenUploads seconds" -ForegroundColor Gray

# Check if upload script exists
$uploadScript = Join-Path $PSScriptRoot "TOSCA-Upload-Query.ps1"
if (-not (Test-Path $uploadScript)) {
    Write-Error "TOSCA-Upload-Query.ps1 not found in the same directory"
    exit 1
}

Write-Host "`n✓ Upload script found: $uploadScript" -ForegroundColor Green

# Initialize results tracking
$results = @()
$startTime = Get-Date

# Process each TOSCA file
Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║ Starting Batch Upload                                         ║" -ForegroundColor Magenta
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

$fileNumber = 1
foreach ($toscaConfig in $toscaFiles) {
    $filePath = Join-Path $ToscaDirectory $toscaConfig.File

    Write-Host "`n[$fileNumber/$($toscaFiles.Count)] Processing: $($toscaConfig.File)" -ForegroundColor Yellow
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
            -Mode $Mode `
            -Namespace $Namespace `
            -ToscaType $toscaConfig.Type `
            -ErrorAction Stop 2>&1

        # Parse output to extract template ID (simplified - you may need to adjust)
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