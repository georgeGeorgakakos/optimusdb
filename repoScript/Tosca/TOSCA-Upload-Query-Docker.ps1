<#
.SYNOPSIS
    OptimusDB TOSCA Upload and Query Script for Docker Desktop

.DESCRIPTION
    PowerShell script to upload TOSCA files to OptimusDB containers running in Docker Desktop.
    Automatically discovers running OptimusDB containers and their ports.

.PARAMETER ToscaFile
    Path to the TOSCA YAML file to upload

.PARAMETER Context
    API context path (default: swarmkb)

.PARAMETER ToscaType
    Type of TOSCA file being uploaded (for metadata)

.PARAMETER ContainerPattern
    Docker container name pattern (default: optimusdb*)

.PARAMETER UseLocalhost
    Use localhost instead of container IPs (default: true for Docker Desktop)

.EXAMPLE
    .\TOSCA-Upload-Query-Docker.ps1 -ToscaFile "sample_1_application_description.yaml" -ToscaType ApplicationDescription

.EXAMPLE
    .\TOSCA-Upload-Query-Docker.ps1 -ToscaFile "sample_2_capacity_description.yaml" -ToscaType CapacityDescription
#>

[CmdletBinding()]
param(
    [Parameter(Mandatory=$false)]
    [string]$ToscaFile = "sample_1_application_description.yaml",

    [Parameter(Mandatory=$false)]
    [string]$Context = "swarmkb",

    [Parameter(Mandatory=$false)]
    [ValidateSet("ApplicationDescription", "CapacityDescription", "OpenTofuTemplate", "DeploymentPlan", "ApplicationRequirements", "Unknown")]
    [string]$ToscaType = "Unknown",

    [Parameter(Mandatory=$false)]
    [string]$ContainerPattern = "optimusdb*",

    [Parameter(Mandatory=$false)]
    [bool]$UseLocalhost = $true
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
        Write-Host "Please ensure Docker is installed and in your PATH" -ForegroundColor Red
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
# Docker Discovery Functions
# ============================================================

function Get-DockerTargets {
    Write-Section "0) Discovering OptimusDB Docker Containers"

    Write-Host "Searching for containers matching pattern: $ContainerPattern" -ForegroundColor Green

    # Get running containers
    $containersJson = docker ps --filter "name=$ContainerPattern" --format "{{json .}}" 2>&1

    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to get Docker containers. Is Docker running?"
        exit 1
    }

    # Parse container information
    $containers = $containersJson | ForEach-Object {
        if ($_ -match '^\{') {
            $_ | ConvertFrom-Json
        }
    }

    if (-not $containers -or $containers.Count -eq 0) {
        Write-Error "No OptimusDB containers found matching pattern: $ContainerPattern"
        Write-Host "`nTry running: docker ps" -ForegroundColor Yellow
        Write-Host "Make sure your OptimusDB containers are running" -ForegroundColor Yellow
        exit 1
    }

    Write-Host "Found $($containers.Count) running container(s)" -ForegroundColor Green

    # Extract ports and build targets
    foreach ($container in $containers) {
        $containerName = $container.Names
        $ports = $container.Ports

        # Parse port mappings (format: "0.0.0.0:18001->8089/tcp")
        if ($ports -match '0\.0\.0\.0:(\d+)->8089') {
            $hostPort = $Matches[1]

            if ($UseLocalhost) {
                $target = "http://localhost:${hostPort}"
            } else {
                # Get container IP
                $containerIp = docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $containerName 2>&1
                if ($LASTEXITCODE -eq 0 -and $containerIp) {
                    $target = "http://${containerIp}:8089"
                } else {
                    $target = "http://localhost:${hostPort}"
                }
            }

            $script:Targets += $target
            Write-Host "  - $target ($containerName)" -ForegroundColor Gray
        }
    }

    if ($script:Targets.Count -eq 0) {
        Write-Error "No valid container ports found"
        Write-Host "`nExpected port mapping format: 0.0.0.0:18001->8089/tcp" -ForegroundColor Yellow
        exit 1
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

function Invoke-AdvancedQueries {
    param([string]$BaseUrl)

    Write-Section "8) Advanced Queries - TOSCA Metadata Analysis"

    # Query: Get recent uploads
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

    # Query: Statistics
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
# Main Execution
# ============================================================

function Main {
    Write-Host @"
╔════════════════════════════════════════════════════════════════╗
║    Swarmchestrate Knowledge Base - TOSCA Upload & Query       ║
║              OptimusDB Docker Desktop Integration              ║
╚════════════════════════════════════════════════════════════════╝
"@ -ForegroundColor Cyan

    Write-Host "`nConfiguration:" -ForegroundColor Yellow
    Write-Host "  TOSCA File:      $ToscaFile" -ForegroundColor Gray
    Write-Host "  TOSCA Type:      $ToscaType" -ForegroundColor Gray
    Write-Host "  Context:         $Context" -ForegroundColor Gray
    Write-Host "  Container Pattern: $ContainerPattern" -ForegroundColor Gray
    Write-Host "  Use Localhost:   $UseLocalhost" -ForegroundColor Gray

    # Check dependencies
    Write-Host "`nChecking dependencies..." -ForegroundColor Yellow
    Test-CommandExists -Command "docker"
    Write-Host "✓ Docker is available" -ForegroundColor Green

    # Discover Docker containers
    Get-DockerTargets

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