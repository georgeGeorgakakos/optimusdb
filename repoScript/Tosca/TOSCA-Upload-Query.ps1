<#
.SYNOPSIS
    OptimusDB TOSCA Upload and Query Script for Swarmchestrate Knowledge Base

.DESCRIPTION
    PowerShell script to upload TOSCA files to the decentralized Knowledge Base (OptimusDB)
    and query them afterwards. Supports multiple discovery modes: LoadBalancer, Pod IP, and Headless DNS.

    Based on the Swarmchestrate architecture, this script interacts with the KB agents to:
    - Upload TOSCA files (Application Descriptions, Capacity Descriptions, Deployment Plans, etc.)
    - Store them in OrbitDB and SQLite datastores
    - Retrieve and query the stored TOSCA templates
    - Verify replication across distributed KB agents

.PARAMETER ToscaFile
    Path to the TOSCA YAML file to upload

.PARAMETER Port
    Port number for the service (default: 18001 for LoadBalancer, 8089 for Pod/Headless)

.PARAMETER Context
    API context path (default: swarmkb)

.PARAMETER Namespace
    Kubernetes namespace where OptimusDB is deployed (default: default)

.PARAMETER Mode
    Discovery mode: 'lb' (LoadBalancer), 'pod' (Pod IPs), or 'headless' (Headless DNS)
    Default: lb

.PARAMETER ContainerPort
    Container port for pod/headless modes (default: 8089)

.PARAMETER ToscaType
    Type of TOSCA file being uploaded (for metadata):
    - ApplicationDescription
    - CapacityDescription
    - OpenTofuTemplate
    - DeploymentPlan
    - ApplicationRequirements

.EXAMPLE
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_1_application_description.yaml" -Mode lb

.EXAMPLE
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_2_capacity_description.yaml" -Port 8089 -Mode pod

.EXAMPLE
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_4_deployment_release_plan.yaml" -Mode headless -ToscaType DeploymentPlan

.EXAMPLE
    # Upload all sample TOSCA files
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_1_application_description.yaml" -ToscaType ApplicationDescription
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_2_capacity_description.yaml" -ToscaType CapacityDescription
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_3_opentofu_tosca_template.yaml" -ToscaType OpenTofuTemplate
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_4_deployment_release_plan.yaml" -ToscaType DeploymentPlan
    .\TOSCA-Upload-Query.ps1 -ToscaFile "sample_5_application_requirements.yaml" -ToscaType ApplicationRequirements

.NOTES
    Author: Swarmchestrate Team
    Version: 2.0
    Requires: kubectl, PowerShell 5.1+

    The script supports the Swarmchestrate KB architecture with:
    - Coordinator and Follower agents
    - OrbitDB for decentralized storage
    - SQLite for structured queries
    - IPFS for content-addressed storage
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [string]$ToscaFile = "sample_1_application_description.yaml",

    [Parameter(Mandatory=$false)]
    [int]$Port = 18001,

    [Parameter(Mandatory=$false)]
    [string]$Context = "swarmkb",

    [Parameter(Mandatory=$false)]
    [string]$Namespace = "default",

    [Parameter(Mandatory=$false)]
    [ValidateSet("lb", "pod", "headless")]
    [string]$Mode = "lb",

    [Parameter(Mandatory=$false)]
    [int]$ContainerPort = 8089,

    [Parameter(Mandatory=$false)]
    [ValidateSet("ApplicationDescription", "CapacityDescription", "OpenTofuTemplate", "DeploymentPlan", "ApplicationRequirements", "Unknown")]
    [string]$ToscaType = "Unknown"
)

# ============================================================
# Global Variables
# ============================================================

$script:Targets = @()
$script:TemplateId = $null
$script:FileName = [System.IO.Path]::GetFileName($ToscaFile)

# ============================================================
# Helper Functions
# ============================================================

function Write-Section {
    param([string]$Message)
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host $Message -ForegroundColor Yellow
    Write-Host "============================================================" -ForegroundColor Cyan
}

function Test-CommandExists {
    param([string]$Command)
    $exists = $null -ne (Get-Command $Command -ErrorAction SilentlyContinue)
    if (-not $exists) {
        Write-Error "Required command not found: $Command"
        Write-Host "Please ensure kubectl is installed and in your PATH" -ForegroundColor Red
        exit 1
    }
    return $exists
}

function ConvertTo-Base64 {
    param([string]$FilePath)

    if (-not (Test-Path $FilePath)) {
        Write-Error "File not found: $FilePath"
        exit 1
    }

    $bytes = [System.IO.File]::ReadAllBytes($FilePath)
    return [Convert]::ToBase64String($bytes)
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

# ============================================================
# Discovery Functions
# ============================================================

function Get-Targets {
    Write-Section "0) Discovering KB Agent Endpoints - Mode: $Mode"

    switch ($Mode) {
        "lb" {
            # Get Node IPs
            Write-Host "Discovering Kubernetes node IPs..." -ForegroundColor Green
            $nodeIpsJson = kubectl get nodes -o jsonpath='{range .items[*]}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{end}' 2>&1

            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to get node IPs"
                exit 1
            }

            $nodeIps = $nodeIpsJson -split "`n" | Where-Object { $_ -match '^\d+\.\d+\.\d+\.\d+$' }

            if ($nodeIps.Count -eq 0) {
                Write-Error "No node IPs found"
                exit 1
            }

            Write-Host "Found $($nodeIps.Count) node(s): $($nodeIps -join ', ')" -ForegroundColor Green

            # Get LoadBalancer Services
            Write-Host "Discovering OptimusDB LoadBalancer services in namespace: $Namespace..." -ForegroundColor Green
            $servicesJson = kubectl -n $Namespace get svc -o json 2>&1

            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to get services"
                exit 1
            }

            $services = ($servicesJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb-\d+$' }

            if ($services.Count -eq 0) {
                Write-Error "No optimusdb-* LoadBalancer services found in namespace: $Namespace"
                exit 1
            }

            Write-Host "Found $($services.Count) OptimusDB service(s)" -ForegroundColor Green

            # Build target URLs
            foreach ($svc in $services) {
                $svcName = $svc.metadata.name
                $svcPort = $svc.spec.ports[0].port

                foreach ($ip in $nodeIps) {
                    $target = "http://${ip}:${svcPort}"
                    $script:Targets += $target
                    Write-Host "  - $target ($svcName)" -ForegroundColor Gray
                }
            }
        }

        "pod" {
            Write-Host "Discovering OptimusDB pod IPs in namespace: $Namespace..." -ForegroundColor Green
            $podsJson = kubectl -n $Namespace get pods -o json 2>&1

            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to get pods"
                exit 1
            }

            $pods = ($podsJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb-\d+$' }

            if ($pods.Count -eq 0) {
                Write-Error "No optimusdb-* pods found in namespace: $Namespace"
                exit 1
            }

            foreach ($pod in $pods) {
                $podName = $pod.metadata.name
                $podIp = $pod.status.podIP

                if ($podIp -match '^\d+\.\d+\.\d+\.\d+$') {
                    $target = "http://${podIp}:${ContainerPort}"
                    $script:Targets += $target
                    Write-Host "  - $target ($podName)" -ForegroundColor Gray
                }
            }

            if ($script:Targets.Count -eq 0) {
                Write-Error "No valid pod IPs found"
                exit 1
            }
        }

        "headless" {
            Write-Host "Using headless DNS names in namespace: $Namespace..." -ForegroundColor Green
            $podsJson = kubectl -n $Namespace get pods -o json 2>&1

            if ($LASTEXITCODE -ne 0) {
                Write-Error "Failed to get pods"
                exit 1
            }

            $pods = ($podsJson | ConvertFrom-Json).items |
                    Where-Object { $_.metadata.name -match '^optimusdb-\d+$' } |
                    Sort-Object { [int]($_.metadata.name -replace '.*-', '') }

            if ($pods.Count -eq 0) {
                Write-Error "No optimusdb-* pods found in namespace: $Namespace"
                exit 1
            }

            foreach ($pod in $pods) {
                $podName = $pod.metadata.name
                $target = "http://${podName}.optimusdb-headless.${Namespace}.svc.cluster.local:${ContainerPort}"
                $script:Targets += $target
                Write-Host "  - $target" -ForegroundColor Gray
            }
        }
    }

    Write-Host "`nTotal targets discovered: $($script:Targets.Count)" -ForegroundColor Green
}

# ============================================================
# KB Agent Interaction Functions
# ============================================================

function Test-Connectivity {
    param([string]$BaseUrl)

    Write-Section "1) Checking Connectivity: $BaseUrl"

    $peersUrl = "$BaseUrl/$Context/peers"
    Write-Host "Testing: $peersUrl" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $peersUrl -TimeoutSec 5

    if ($response -ne $null) {
        Write-Host "✓ Reachable: $BaseUrl" -ForegroundColor Green

        if ($response.peers) {
            Write-Host "  Connected peers: $($response.peers.Count)" -ForegroundColor Gray
        }
        return $true
    } else {
        Write-Host "✗ Not reachable: $BaseUrl" -ForegroundColor Red
        return $false
    }
}

function Invoke-ToscaUpload {
    param([string]$BaseUrl)

    Write-Section "2) Uploading TOSCA File: $script:FileName"
    Write-Host "Target: $BaseUrl/$Context/upload" -ForegroundColor Gray
    Write-Host "TOSCA Type: $ToscaType" -ForegroundColor Gray

    if (-not (Test-Path $ToscaFile)) {
        Write-Error "TOSCA file not found: $ToscaFile"
        return
    }

    # Read file info
    $fileInfo = Get-Item $ToscaFile
    $fileSize = $fileInfo.Length
    Write-Host "File size: $([math]::Round($fileSize/1KB, 2)) KB" -ForegroundColor Gray

    # Convert to Base64
    Write-Host "Converting to Base64..." -ForegroundColor Gray
    $base64Content = ConvertTo-Base64 -FilePath $ToscaFile

    # Prepare upload payload
    $uploadPayload = @{
        file = $base64Content
        filename = $script:FileName
        tosca_type = $ToscaType
        uploaded_by = $env:USERNAME
        source_host = $env:COMPUTERNAME
    }

    # Upload
    Write-Host "Uploading to KB..." -ForegroundColor Gray
    $uploadUrl = "$BaseUrl/$Context/upload"
    $response = Invoke-SafeRestMethod -Uri $uploadUrl -Method POST -Body $uploadPayload

    if ($response -eq $null) {
        Write-Warning "Upload failed or returned no response"
        return
    }

    Write-Host "`nUpload Response:" -ForegroundColor Green
    $response | ConvertTo-Json -Depth 5 | Write-Host

    # Extract template_id
    $script:TemplateId = $null
    if ($response.data.template_id) {
        $script:TemplateId = $response.data.template_id
    } elseif ($response.template_id) {
        $script:TemplateId = $response.template_id
    } elseif ($response.data.templateId) {
        $script:TemplateId = $response.data.templateId
    } elseif ($response.templateId) {
        $script:TemplateId = $response.templateId
    }

    if ($script:TemplateId) {
        Write-Host "`n✓ Template ID: $script:TemplateId" -ForegroundColor Green
    } else {
        Write-Warning "Could not extract template_id from response"
    }
}

function Invoke-OrbitDBQuery {
    param([string]$BaseUrl)

    Write-Section "3) Querying OrbitDB: Template ID = $script:TemplateId"

    if (-not $script:TemplateId) {
        Write-Warning "No template_id available, skipping OrbitDB query"
        return
    }

    $queryPayload = @{
        method = @{
            cmd = "crudget"
            argcnt = 1
        }
        dstype = "tosca_imported"
        criteria = @(
            @{
                _id = $script:TemplateId
            }
        )
    }

    $commandUrl = "$BaseUrl/$Context/command"
    Write-Host "Querying: $commandUrl" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $commandUrl -Method POST -Body $queryPayload

    if ($response -ne $null) {
        Write-Host "`nOrbitDB Query Response:" -ForegroundColor Green
        $response | ConvertTo-Json -Depth 10 | Write-Host
    } else {
        Write-Warning "OrbitDB query returned no response"
    }
}

function Invoke-SQLiteQueryByFilename {
    param([string]$BaseUrl)

    Write-Section "4) Querying SQLite by Filename: $script:FileName"

    $filenameEscaped = Invoke-SqlEscape -Text $script:FileName

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename='$filenameEscaped'
ORDER BY id DESC
LIMIT 5;
"@

    $queryPayload = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $sql
    }

    $commandUrl = "$BaseUrl/$Context/command"
    Write-Host "Executing SQL query..." -ForegroundColor Gray
    Write-Host "SQL: $sql" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $commandUrl -Method POST -Body $queryPayload

    if ($response -ne $null) {
        Write-Host "`nSQLite Query Response (by filename):" -ForegroundColor Green
        $response | ConvertTo-Json -Depth 10 | Write-Host
    } else {
        Write-Warning "SQLite query returned no response"
    }
}

function Invoke-SQLiteQueryByTemplateId {
    param([string]$BaseUrl)

    Write-Section "5) Querying SQLite by Template ID: $script:TemplateId"

    if (-not $script:TemplateId) {
        Write-Warning "No template_id available, skipping SQLite query"
        return
    }

    $templateIdEscaped = Invoke-SqlEscape -Text $script:TemplateId

    $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE template_id='$templateIdEscaped'
ORDER BY id DESC
LIMIT 5;
"@

    $queryPayload = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $sql
    }

    $commandUrl = "$BaseUrl/$Context/command"
    Write-Host "Executing SQL query..." -ForegroundColor Gray
    Write-Host "SQL: $sql" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $commandUrl -Method POST -Body $queryPayload

    if ($response -ne $null) {
        Write-Host "`nSQLite Query Response (by template_id):" -ForegroundColor Green
        $response | ConvertTo-Json -Depth 10 | Write-Host
    } else {
        Write-Warning "SQLite query returned no response"
    }
}

function Get-PeersList {
    param([string]$BaseUrl)

    Write-Section "6) Listing KB Agent Peers"

    $peersUrl = "$BaseUrl/$Context/peers"
    Write-Host "Querying: $peersUrl" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $peersUrl

    if ($response -ne $null) {
        Write-Host "`nPeers List:" -ForegroundColor Green
        $response | ConvertTo-Json -Depth 10 | Write-Host

        if ($response.peers) {
            Write-Host "`nPeer Summary:" -ForegroundColor Cyan
            Write-Host "  Total Peers: $($response.peers.Count)" -ForegroundColor Gray

            foreach ($peer in $response.peers) {
                if ($peer.id) {
                    Write-Host "  - Peer ID: $($peer.id)" -ForegroundColor Gray
                }
            }
        }
    } else {
        Write-Warning "Failed to retrieve peers list"
    }
}

function Get-AgentLogs {
    param([string]$BaseUrl)

    Write-Section "7) Fetching KB Agent Logs"

    $today = Get-Date -Format "yyyy-MM-dd"
    $hour = Get-Date -Format "HH"

    $logUrl = "$BaseUrl/$Context/log?date=$today&hour=$hour"
    Write-Host "Querying: $logUrl" -ForegroundColor Gray

    $response = Invoke-SafeRestMethod -Uri $logUrl

    if ($response -ne $null) {
        Write-Host "`nAgent Logs:" -ForegroundColor Green
        $response | ConvertTo-Json -Depth 10 | Write-Host
    } else {
        Write-Warning "Failed to retrieve logs"
    }
}

# ============================================================
# Advanced Query Functions
# ============================================================

function Invoke-AdvancedQueries {
    param([string]$BaseUrl)

    Write-Section "8) Advanced Queries - TOSCA Metadata Analysis"

    # Query 1: Get all TOSCA files by type
    if ($ToscaType -ne "Unknown") {
        Write-Host "`nQuery: All TOSCA files of type '$ToscaType'" -ForegroundColor Cyan

        $sql = @"
SELECT filename, template_id, filesize_bytes, created_at, uploader
FROM toscametadata
WHERE description LIKE '%$ToscaType%' OR filename LIKE '%$ToscaType%'
ORDER BY created_at DESC
LIMIT 10;
"@

        $queryPayload = @{
            method = @{
                cmd = "sqldml"
                argcnt = 1
            }
            sqldml = $sql
        }

        $response = Invoke-SafeRestMethod -Uri "$BaseUrl/$Context/command" -Method POST -Body $queryPayload

        if ($response -ne $null) {
            $response | ConvertTo-Json -Depth 10 | Write-Host
        }
    }

    # Query 2: Get recent uploads
    Write-Host "`nQuery: Recent TOSCA uploads (last 24 hours)" -ForegroundColor Cyan

    $sql = @"
SELECT filename, template_id, filesize_bytes, created_at, uploader, source_pod
FROM toscametadata
WHERE datetime(created_at) > datetime('now', '-1 day')
ORDER BY created_at DESC
LIMIT 20;
"@

    $queryPayload = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $sql
    }

    $response = Invoke-SafeRestMethod -Uri "$BaseUrl/$Context/command" -Method POST -Body $queryPayload

    if ($response -ne $null) {
        $response | ConvertTo-Json -Depth 10 | Write-Host
    }

    # Query 3: Statistics
    Write-Host "`nQuery: TOSCA storage statistics" -ForegroundColor Cyan

    $sql = @"
SELECT
    COUNT(*) as total_files,
    SUM(filesize_bytes) as total_size_bytes,
    AVG(filesize_bytes) as avg_size_bytes,
    MIN(created_at) as first_upload,
    MAX(created_at) as last_upload
FROM toscametadata;
"@

    $queryPayload = @{
        method = @{
            cmd = "sqldml"
            argcnt = 1
        }
        sqldml = $sql
    }

    $response = Invoke-SafeRestMethod -Uri "$BaseUrl/$Context/command" -Method POST -Body $queryPayload

    if ($response -ne $null) {
        $response | ConvertTo-Json -Depth 10 | Write-Host
    }
}

# ============================================================
# Batch Operations
# ============================================================

function Invoke-BatchUpload {
    param(
        [string[]]$Files,
        [string]$BaseUrl
    )

    Write-Section "Batch Upload - Multiple TOSCA Files"

    $results = @()

    foreach ($file in $Files) {
        Write-Host "`n--- Processing: $file ---" -ForegroundColor Yellow

        if (Test-Path $file) {
            $script:ToscaFile = $file
            $script:FileName = [System.IO.Path]::GetFileName($file)

            Invoke-ToscaUpload -BaseUrl $BaseUrl

            $results += @{
                File = $file
                TemplateId = $script:TemplateId
                Success = ($script:TemplateId -ne $null)
            }

            Start-Sleep -Seconds 2
        } else {
            Write-Warning "File not found: $file"
            $results += @{
                File = $file
                TemplateId = $null
                Success = $false
            }
        }
    }

    Write-Host "`n--- Batch Upload Summary ---" -ForegroundColor Green
    $results | ForEach-Object {
        $status = if ($_.Success) { "✓" } else { "✗" }
        Write-Host "$status $($_.File) - Template ID: $($_.TemplateId)" -ForegroundColor $(if ($_.Success) { "Green" } else { "Red" })
    }
}

# ============================================================
# Main Execution
# ============================================================

function Main {
    Write-Host @"
╔════════════════════════════════════════════════════════════════╗
║    Swarmchestrate Knowledge Base - TOSCA Upload & Query       ║
║                    OptimusDB Integration                       ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

    Write-Host "`nConfiguration:" -ForegroundColor Yellow
    Write-Host "  TOSCA File:      $ToscaFile" -ForegroundColor Gray
    Write-Host "  TOSCA Type:      $ToscaType" -ForegroundColor Gray
    Write-Host "  Port:            $Port" -ForegroundColor Gray
    Write-Host "  Context:         $Context" -ForegroundColor Gray
    Write-Host "  Namespace:       $Namespace" -ForegroundColor Gray
    Write-Host "  Mode:            $Mode" -ForegroundColor Gray
    Write-Host "  Container Port:  $ContainerPort" -ForegroundColor Gray

    # Check dependencies
    Write-Host "`nChecking dependencies..." -ForegroundColor Yellow
    Test-CommandExists -Command "kubectl"
    Write-Host "✓ kubectl is available" -ForegroundColor Green

    # Discover targets
    Get-Targets

    if ($script:Targets.Count -eq 0) {
        Write-Error "No targets discovered"
        exit 1
    }

    # Process each target
    $successfulTargets = 0

    foreach ($target in $script:Targets) {
        Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
        Write-Host "║ Processing Target: $target" -ForegroundColor Magenta
        Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

        # Test connectivity first
        $isReachable = Test-Connectivity -BaseUrl $target

        if (-not $isReachable) {
            Write-Warning "Skipping unreachable target: $target"
            continue
        }

        $successfulTargets++

        # Execute operations
        Invoke-ToscaUpload -BaseUrl $target
        Invoke-OrbitDBQuery -BaseUrl $target
        Invoke-SQLiteQueryByFilename -BaseUrl $target
        Invoke-SQLiteQueryByTemplateId -BaseUrl $target
        Get-PeersList -BaseUrl $target
        Get-AgentLogs -BaseUrl $target
        Invoke-AdvancedQueries -BaseUrl $target

        Write-Host "`n--- Completed processing for: $target ---" -ForegroundColor Green
    }

    # Final summary
    Write-Section "Execution Summary"
    Write-Host "Total Targets Discovered:  $($script:Targets.Count)" -ForegroundColor Cyan
    Write-Host "Successful Connections:    $successfulTargets" -ForegroundColor Green
    Write-Host "Failed Connections:        $($script:Targets.Count - $successfulTargets)" -ForegroundColor $(if (($script:Targets.Count - $successfulTargets) -gt 0) { "Yellow" } else { "Gray" })

    if ($script:TemplateId) {
        Write-Host "`nFinal Template ID: $script:TemplateId" -ForegroundColor Green
    }

    Write-Host "`n✓ Script execution completed" -ForegroundColor Green
}

# ============================================================
# Script Entry Point
# ============================================================

try {
    Main
} catch {
    Write-Error "Script execution failed: $($_.Exception.Message)"
    Write-Host $_.ScriptStackTrace -ForegroundColor Red
    exit 1
}