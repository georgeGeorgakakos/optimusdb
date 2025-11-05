<#
.SYNOPSIS
    Advanced TOSCA Query and Retrieval Script for Swarmchestrate Knowledge Base

.DESCRIPTION
    Query and retrieve TOSCA files from the decentralized Knowledge Base without uploading.
    Provides advanced search, filtering, and analysis capabilities.

.PARAMETER Mode
    Discovery mode: 'lb', 'pod', or 'headless'

.PARAMETER Namespace
    Kubernetes namespace (default: default)

.PARAMETER QueryType
    Type of query to execute:
    - ByFilename: Search by filename
    - ByTemplateId: Search by template ID
    - ByType: Search by TOSCA type
    - ByUploader: Search by uploader
    - ByDateRange: Search by date range
    - Recent: Get recent uploads
    - Statistics: Get storage statistics
    - All: Get all TOSCA files

.PARAMETER SearchValue
    Search value (filename, template_id, uploader, etc.)

.PARAMETER ToscaType
    TOSCA type filter

.PARAMETER StartDate
    Start date for date range queries (format: yyyy-MM-dd)

.PARAMETER EndDate
    End date for date range queries (format: yyyy-MM-dd)

.PARAMETER Limit
    Maximum number of results to return (default: 10)

.EXAMPLE
    # Query by filename
    .\TOSCA-Query.ps1 -QueryType ByFilename -SearchValue "sample_1_application_description.yaml"

.EXAMPLE
    # Query by template ID
    .\TOSCA-Query.ps1 -QueryType ByTemplateId -SearchValue "template-abc123"

.EXAMPLE
    # Get recent uploads
    .\TOSCA-Query.ps1 -QueryType Recent -Limit 20

.EXAMPLE
    # Get statistics
    .\TOSCA-Query.ps1 -QueryType Statistics

.EXAMPLE
    # Query by TOSCA type
    .\TOSCA-Query.ps1 -QueryType ByType -ToscaType ApplicationDescription

.EXAMPLE
    # Query by date range
    .\TOSCA-Query.ps1 -QueryType ByDateRange -StartDate "2025-11-01" -EndDate "2025-11-05"

.EXAMPLE
    # Query by uploader
    .\TOSCA-Query.ps1 -QueryType ByUploader -SearchValue "admin"
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [ValidateSet("lb", "pod", "headless")]
    [string]$Mode = "lb",

    [Parameter(Mandatory=$false)]
    [string]$Namespace = "default",

    [Parameter(Mandatory=$true)]
    [ValidateSet("ByFilename", "ByTemplateId", "ByType", "ByUploader", "ByDateRange", "Recent", "Statistics", "All")]
    [string]$QueryType,

    [Parameter(Mandatory=$false)]
    [string]$SearchValue,

    [Parameter(Mandatory=$false)]
    [ValidateSet("ApplicationDescription", "CapacityDescription", "OpenTofuTemplate", "DeploymentPlan", "ApplicationRequirements")]
    [string]$ToscaType,

    [Parameter(Mandatory=$false)]
    [string]$StartDate,

    [Parameter(Mandatory=$false)]
    [string]$EndDate,

    [Parameter(Mandatory=$false)]
    [int]$Limit = 10,

    [Parameter(Mandatory=$false)]
    [int]$Port = 18001,

    [Parameter(Mandatory=$false)]
    [string]$Context = "swarmkb",

    [Parameter(Mandatory=$false)]
    [int]$ContainerPort = 8089
)

# ============================================================
# Global Variables
# ============================================================

$script:Targets = @()
$script:Results = @()

# ============================================================
# Helper Functions
# ============================================================

function Write-Section {
    param([string]$Message)
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Yellow
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Invoke-SqlEscape {
    param([string]$Text)
    return $Text -replace "'", "''"
}

function Invoke-SafeRestMethod {
    param(
        [string]$Uri,
        [string]$Method = "GET",
        [object]$Body = $null,
        [int]$TimeoutSec = 10
    )

    try {
        $headers = @{
            "Content-Type" = "application/json"
        }

        $params = @{
            Uri = $Uri
            Method = $Method
            Headers = $headers
            TimeoutSec = $TimeoutSec
            ErrorAction = "Stop"
        }

        if ($Body -ne $null) {
            if ($Body -is [string]) {
                $params.Body = $Body
            } else {
                $params.Body = ($Body | ConvertTo-Json -Depth 10 -Compress)
            }
        }

        $response = Invoke-RestMethod @params
        return $response

    } catch {
        Write-Warning "Request failed: $($_.Exception.Message)"
        return $null
    }
}

function Get-Targets {
    Write-Section "Discovering KB Agent Endpoints - Mode: $Mode"

    switch ($Mode) {
        "lb" {
            # Use JSON output and parse with PowerShell
            $nodesJson = kubectl get nodes -o json 2>&1
            if ($LASTEXITCODE -ne 0) { Write-Error "Failed to get node IPs"; exit 1 }

            $nodes = $nodesJson | ConvertFrom-Json
            $nodeIps = $nodes.items | ForEach-Object {
                $_.status.addresses | Where-Object { $_.type -eq "InternalIP" } | Select-Object -ExpandProperty address
            }
            if ($nodeIps.Count -eq 0) { Write-Error "No node IPs found"; exit 1 }

            $servicesJson = kubectl -n $Namespace get svc -o json 2>&1
            if ($LASTEXITCODE -ne 0) { Write-Error "Failed to get services"; exit 1 }

            $services = ($servicesJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb\d+$' }

            if ($services.Count -eq 0) { Write-Error "No optimusdb services found"; exit 1 }

            foreach ($svc in $services) {
                $svcPort = $svc.spec.ports[0].port
                foreach ($ip in $nodeIps) {
                    $script:Targets += "http://${ip}:${svcPort}"
                }
            }
        }

        "pod" {
            $podsJson = kubectl -n $Namespace get pods -o json 2>&1
            if ($LASTEXITCODE -ne 0) { Write-Error "Failed to get pods"; exit 1 }

            $pods = ($podsJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb\d+$' }

            foreach ($pod in $pods) {
                $podIp = $pod.status.podIP
                if ($podIp -match '^\d+\.\d+\.\d+\.\d+$') {
                    $script:Targets += "http://${podIp}:${ContainerPort}"
                }
            }
        }

        "headless" {
            $podsJson = kubectl -n $Namespace get pods -o json 2>&1
            if ($LASTEXITCODE -ne 0) { Write-Error "Failed to get pods"; exit 1 }

            $pods = ($podsJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb\d+$' } |
                    Sort-Object { [int]($_.metadata.name -replace '.*-', '') }

            foreach ($pod in $pods) {
                $podName = $pod.metadata.name
                $script:Targets += "http://${podName}.optimusdb-headless.${Namespace}.svc.cluster.local:${ContainerPort}"
            }
        }
    }

    Write-Host "Discovered $($script:Targets.Count) target(s)" -ForegroundColor Green
}

# ============================================================
# Query Functions
# ============================================================

function Invoke-Query {
    param(
        [string]$BaseUrl,
        [string]$Sql
    )

    $queryPayload = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $Sql
    }

    $commandUrl = "$BaseUrl/$Context/command"
    $response = Invoke-SafeRestMethod -Uri $commandUrl -Method POST -Body $queryPayload

    return $response
}

function Query-ByFilename {
    param([string]$BaseUrl)

    if (-not $SearchValue) {
        Write-Error "SearchValue parameter is required for ByFilename query"
        return
    }

    $filenameEscaped = Invoke-SqlEscape -Text $SearchValue

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename='$filenameEscaped'
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Searching for filename: $SearchValue" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-ByTemplateId {
    param([string]$BaseUrl)

    if (-not $SearchValue) {
        Write-Error "SearchValue parameter is required for ByTemplateId query"
        return
    }

    $templateIdEscaped = Invoke-SqlEscape -Text $SearchValue

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE template_id='$templateIdEscaped'
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Searching for template_id: $SearchValue" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-ByType {
    param([string]$BaseUrl)

    if (-not $ToscaType) {
        Write-Error "ToscaType parameter is required for ByType query"
        return
    }

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename LIKE '%$ToscaType%' OR description LIKE '%$ToscaType%'
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Searching for TOSCA type: $ToscaType" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-ByUploader {
    param([string]$BaseUrl)

    if (-not $SearchValue) {
        Write-Error "SearchValue parameter is required for ByUploader query"
        return
    }

    $uploaderEscaped = Invoke-SqlEscape -Text $SearchValue

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE uploader='$uploaderEscaped'
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Searching for uploader: $SearchValue" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-ByDateRange {
    param([string]$BaseUrl)

    if (-not $StartDate -or -not $EndDate) {
        Write-Error "StartDate and EndDate parameters are required for ByDateRange query"
        return
    }

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE DATE(created_at) BETWEEN '$StartDate' AND '$EndDate'
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Searching for date range: $StartDate to $EndDate" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-Recent {
    param([string]$BaseUrl)

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Fetching $Limit most recent TOSCA files" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-Statistics {
    param([string]$BaseUrl)

    $sql = @"
SELECT
    COUNT(*) as total_files,
    SUM(filesize_bytes) as total_size_bytes,
    AVG(filesize_bytes) as avg_size_bytes,
    MIN(created_at) as first_upload,
    MAX(created_at) as last_upload,
    COUNT(DISTINCT uploader) as unique_uploaders,
    COUNT(DISTINCT source_pod) as unique_pods
FROM toscametadata;
"@

    Write-Host "Calculating storage statistics" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

function Query-All {
    param([string]$BaseUrl)

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256,
       uploader, source_pod, created_at
FROM toscametadata
ORDER BY created_at DESC
LIMIT $Limit;
"@

    Write-Host "Fetching all TOSCA files (limit: $Limit)" -ForegroundColor Gray
    return Invoke-Query -BaseUrl $BaseUrl -Sql $sql
}

# ============================================================
# Result Processing
# ============================================================

function Format-Results {
    param([object]$Response)

    if ($Response -eq $null) {
        Write-Warning "No results returned"
        return
    }

    Write-Host "`nQuery Results:" -ForegroundColor Green

    # Pretty print JSON
    $Response | ConvertTo-Json -Depth 10 | Write-Host

    # Extract and display data if available
    if ($Response.data -and $Response.data.Count -gt 0) {
        Write-Host "`n--- Data Summary ---" -ForegroundColor Cyan
        Write-Host "Records returned: $($Response.data.Count)" -ForegroundColor Gray

        if ($QueryType -eq "Statistics") {
            # Special formatting for statistics
            $stats = $Response.data[0]
            Write-Host "`nStorage Statistics:" -ForegroundColor Yellow
            Write-Host "  Total Files:       $($stats.total_files)" -ForegroundColor Gray
            Write-Host "  Total Size:        $([math]::Round($stats.total_size_bytes / 1MB, 2)) MB" -ForegroundColor Gray
            Write-Host "  Average Size:      $([math]::Round($stats.avg_size_bytes / 1KB, 2)) KB" -ForegroundColor Gray
            Write-Host "  First Upload:      $($stats.first_upload)" -ForegroundColor Gray
            Write-Host "  Last Upload:       $($stats.last_upload)" -ForegroundColor Gray
            Write-Host "  Unique Uploaders:  $($stats.unique_uploaders)" -ForegroundColor Gray
            Write-Host "  Unique Pods:       $($stats.unique_pods)" -ForegroundColor Gray
        } else {
            # Display file records
            Write-Host "`nFiles Found:" -ForegroundColor Yellow
            foreach ($record in $Response.data) {
                Write-Host "`n  File: $($record.filename)" -ForegroundColor White
                Write-Host "    ID:          $($record.id)" -ForegroundColor Gray
                Write-Host "    Template ID: $($record.template_id)" -ForegroundColor Gray
                Write-Host "    Size:        $([math]::Round($record.filesize_bytes / 1KB, 2)) KB" -ForegroundColor Gray
                Write-Host "    Uploader:    $($record.uploader)" -ForegroundColor Gray
                Write-Host "    Created:     $($record.created_at)" -ForegroundColor Gray

                if ($record.ipfs_path) {
                    Write-Host "    IPFS Path:   $($record.ipfs_path)" -ForegroundColor Gray
                }

                if ($record.content_sha256) {
                    Write-Host "    SHA256:      $($record.content_sha256.Substring(0, 16))..." -ForegroundColor Gray
                }
            }
        }
    } else {
        Write-Host "No data returned" -ForegroundColor Yellow
    }
}

# ============================================================
# Main Execution
# ============================================================

function Main {
    Write-Host @"
╔════════════════════════════════════════════════════════════════╗
║    Swarmchestrate Knowledge Base - TOSCA Query Tool           ║
║               Advanced Query and Retrieval                     ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

    Write-Host "`nQuery Configuration:" -ForegroundColor Yellow
    Write-Host "  Query Type:      $QueryType" -ForegroundColor Gray
    Write-Host "  Namespace:       $Namespace" -ForegroundColor Gray
    Write-Host "  Mode:            $Mode" -ForegroundColor Gray
    Write-Host "  Limit:           $Limit" -ForegroundColor Gray

    if ($SearchValue) {
        Write-Host "  Search Value:    $SearchValue" -ForegroundColor Gray
    }
    if ($ToscaType) {
        Write-Host "  TOSCA Type:      $ToscaType" -ForegroundColor Gray
    }
    if ($StartDate) {
        Write-Host "  Start Date:      $StartDate" -ForegroundColor Gray
    }
    if ($EndDate) {
        Write-Host "  End Date:        $EndDate" -ForegroundColor Gray
    }

    # Get targets
    Get-Targets

    # Execute query on first available target
    $targetUrl = $script:Targets[0]

    Write-Section "Executing Query on: $targetUrl"

    $response = $null

    switch ($QueryType) {
        "ByFilename"    { $response = Query-ByFilename -BaseUrl $targetUrl }
        "ByTemplateId"  { $response = Query-ByTemplateId -BaseUrl $targetUrl }
        "ByType"        { $response = Query-ByType -BaseUrl $targetUrl }
        "ByUploader"    { $response = Query-ByUploader -BaseUrl $targetUrl }
        "ByDateRange"   { $response = Query-ByDateRange -BaseUrl $targetUrl }
        "Recent"        { $response = Query-Recent -BaseUrl $targetUrl }
        "Statistics"    { $response = Query-Statistics -BaseUrl $targetUrl }
        "All"           { $response = Query-All -BaseUrl $targetUrl }
    }

    # Format and display results
    Format-Results -Response $response

    # Export results
    if ($response -ne $null) {
        $exportFile = "tosca-query-results-$(Get-Date -Format 'yyyyMMdd-HHmmss').json"
        $response | ConvertTo-Json -Depth 10 | Out-File $exportFile -Encoding UTF8
        Write-Host "`n✓ Results exported to: $exportFile" -ForegroundColor Green
    }

    Write-Host "`n✓ Query completed" -ForegroundColor Green
}

# Execute
try {
    Main
} catch {
    Write-Error "Query execution failed: $($_.Exception.Message)"
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}