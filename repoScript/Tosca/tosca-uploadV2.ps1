<#
.SYNOPSIS
  OptimusDB TOSCA Upload + Query Script (LB / Pod / Headless / Docker)

.EXAMPLES
  # Kubernetes - LB services on each node IP (namespace: default)
  .\Upload-ToscaV2.ps1 -File .\mytosca.yaml -Port 18001 -Context swarmkb -Namespace default -Mode lb

  # Kubernetes - Direct Pod IPs (container port 8089)
  .\Upload-ToscaV2.ps1 -File .\mytosca.yaml -Context swarmkb -Namespace default -Mode pod -ContainerPort 8089

  # Kubernetes - Headless DNS (container port 8089)
  .\Upload-ToscaV2.ps1 -File .\mytosca.yaml -Context swarmkb -Namespace default -Mode headless -ContainerPort 8089

  # Docker Desktop - sequential published ports (localhost:18001..18008)
  .\Upload-ToscaV2.ps1 -Mode docker -BasePort 18001 -Agents 8 -File .\mytosca.yaml -Context swarmkb

  # Docker Desktop - auto-discover published ports via container names
  .\Upload-ToscaV2.ps1 -Mode docker -NamePrefix "optimusdb" -File .\mytosca.yaml -Context swarmkb
#>

[CmdletBinding()]
param(
  [string]$File = ".\mytosca.yaml",

# For K8s LB mode only (kept for compatibility; discovery reads svc ports anyway)
  [int]$Port = 18001,

  [string]$Context = "swarmkb",
  [string]$Namespace = "default",

  [ValidateSet("lb","pod","headless","docker")]
  [string]$Mode = "lb",

# For pod/headless modes (container listener)
  [int]$ContainerPort = 8089,

# For Docker mode
  [int]$BasePort = 18001,      # first published host port (localhost)
  [int]$Agents = 8,            # number of agents/containers
  [string]$NamePrefix = "optimusdb"  # container name prefix for auto-discovery
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ---------------- Helpers ----------------
function Write-HR { "`n============================================================" | Write-Host }
function Need-Bin([string]$Name) {
  if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
    throw "Missing dependency: $Name"
  }
}
function Sql-Escape([string]$s) { $s -replace "'", "''" }
function Get-Base64File([string]$path) {
  if (-not (Test-Path -Path $path -PathType Leaf)) { throw "File not found: $path" }
  $bytes = [System.IO.File]::ReadAllBytes($path)
  [System.Convert]::ToBase64String($bytes)
}
function Get-TemplateIdFromResponse($obj) {
  if ($null -ne $obj) {
    if ($obj.data -and $obj.data.template_id) { return [string]$obj.data.template_id }
    if ($obj.template_id)                    { return [string]$obj.template_id }
    if ($obj.data -and $obj.data.templateId) { return [string]$obj.data.templateId }
    if ($obj.templateId)                     { return [string]$obj.templateId }
  }
  ""
}
function Invoke-KubectlJson([string[]]$Args) {
  $raw = & kubectl @Args -o json
  $raw | ConvertFrom-Json
}

# ---------------- Dependency Check ----------------
Need-Bin curl
switch ($Mode) {
  'docker'  { Need-Bin docker }
  default   { Need-Bin kubectl }
}

# ---------------- Target Discovery ----------------
$Targets = New-Object System.Collections.Generic.List[string]

switch ($Mode) {
  'lb' {
    Write-HR; Write-Host "0) Discovering node IPs and LB services (namespace: $Namespace)"
    $nodes = Invoke-KubectlJson @('get','nodes')
    $nodeIps = @()
    foreach ($n in $nodes.items) {
      foreach ($addr in $n.status.addresses) {
        if ($addr.type -eq 'InternalIP' -and $addr.address) { $nodeIps += $addr.address }
      }
    }
    if (-not $nodeIps) { throw "No node IPs found" }

    $svcs = Invoke-KubectlJson @('get','svc','-n', $Namespace)
    $svcItems = $svcs.items | Where-Object { $_.metadata.name -match '^optimusdb[0-9]+$' }
    if (-not $svcItems) { throw "No optimusdb* LB services found" }

    foreach ($svc in $svcItems) {
      $p = $svc.spec.ports[0].port
      foreach ($ip in $nodeIps) { $Targets.Add("http://$ip\:$p") }
    }
    Write-Host ("LB targets: " + ($Targets -join ' '))
  }
  'pod' {
    Write-HR; Write-Host "0) Discovering OptimusDB pod IPs (namespace: $Namespace) on port $ContainerPort"
    $pods = Invoke-KubectlJson @('get','pods','-n', $Namespace)
    $podIps = $pods.items |
            Where-Object { $_.metadata.name -match '^optimusdb[0-9]+$' -and $_.status.podIP -match '^\d+\.' } |
            ForEach-Object { $_.status.podIP }
    if (-not $podIps) { throw "No optimusdb pod IPs found" }
    foreach ($ip in $podIps) { $Targets.Add("http://$ip\:$ContainerPort") }
    Write-Host ("Pod targets: " + ($Targets -join ' '))
  }
  'headless' {
    Write-HR; Write-Host "0) Using headless DNS names on port $ContainerPort"
    $pods = Invoke-KubectlJson @('get','pods','-n', $Namespace)
    $podNames = $pods.items |
            ForEach-Object { $_.metadata.name } |
            Where-Object { $_ -match '^optimusdb[0-9]+$' } |
            Sort-Object { [int]($_ -replace '^[^\d]+','') }
    if (-not $podNames) { throw "No optimusdb pods found" }
    foreach ($p in $podNames) { $Targets.Add("http://$p.optimusdb-headless.$Namespace.svc.cluster.local:$ContainerPort") }
    Write-Host ("Headless targets: " + ($Targets -join ' '))
  }
  'docker' {
    Write-HR; Write-Host "0) Discovering Docker targets"
    # OPTION A: sequential published ports on localhost (fast path)
    for ($i = 0; $i -lt $Agents; $i++) {
      $hostPort = $BasePort + $i
      $Targets.Add("http://localhost:$hostPort")
    }
    # OPTION B: enrich by inspecting running containers that match $NamePrefix
    try {
      $ps = & docker ps --format '{{.ID}} {{.Names}}'
      $lines = $ps -split "`n" | Where-Object { $_ -match "$NamePrefix\d+" }
      foreach ($ln in $lines) {
        $parts = $ln -split ' ',2
        if ($parts.Count -lt 2) { continue }
        $id   = $parts[0]
        $ins  = & docker inspect $id | ConvertFrom-Json
        $ports = $ins[0].NetworkSettings.Ports
        if ($ports) {
          foreach ($k in $ports.PSObject.Properties.Name) {
            $bindings = $ports.$k
            if ($bindings -and $bindings.Count -gt 0 -and $bindings[0].HostPort) {
              $Targets.Add("http://localhost:{0}" -f $bindings[0].HostPort)
            }
          }
        }
      }
    } catch { Write-Verbose "docker inspect discovery failed: $($_.Exception.Message)" }
    # dedupe
    $Targets = [System.Collections.Generic.List[string]](
    [System.Linq.Enumerable]::ToList([System.Linq.Enumerable]::Distinct($Targets))
    )
    Write-Host ("Docker targets: " + ($Targets -join ' '))
  }
  default { throw "Unknown MODE: $Mode" }
}

# ---------------- Steps (HTTP actions) ----------------
$filename = [System.IO.Path]::GetFileName($File)
$templateId = ""

function Test-Reachability([string]$base) {
  Write-HR; Write-Host "1) Checking connectivity at $base/$Context/peers"
  try {
    $null = Invoke-RestMethod -Uri "$base/$Context/peers" -Method GET -TimeoutSec 4
    Write-Host "Reachable: $base"
  } catch {
    Write-Host "Not reachable or not JSON: $base"
  }
}

function Invoke-UploadTosca([string]$base) {
  Write-HR; Write-Host "2) Uploading TOSCA file: $File to $base/$Context/upload"
  $b64 = Get-Base64File $File
  $body = @{ file = $b64; filename = $filename } | ConvertTo-Json -Depth 5
  $raw = Invoke-RestMethod -Uri "$base/$Context/upload" -Method POST -ContentType "application/json" -Body $body -TimeoutSec 30 -ErrorAction Stop
  Write-Host "Upload response (parsed):"
  $raw | ConvertTo-Json -Depth 10 | Write-Output
  $script:templateId = Get-TemplateIdFromResponse $raw
  if ([string]::IsNullOrWhiteSpace($script:templateId)) { Write-Host "Upload returned no template_id" } else { Write-Host "Parsed template_id: $script:templateId" }
}

function Invoke-QueryOptimusDbByTemplate([string]$base) {
  Write-HR; Write-Host "3) Querying OptimusdB on $base for templateId: $script:templateId"
  if ([string]::IsNullOrWhiteSpace($script:templateId)) { Write-Host "No template_id; skipping OptimusdB query"; return }
  $payload = @{
    method   = @{ cmd = "crudget"; argcnt = 1 }
    dstype   = "tosca_imported"
    criteria = @(@{ _id = $script:templateId })
  } | ConvertTo-Json -Depth 10
  try {
    $resp = Invoke-RestMethod -Uri "$base/$Context/command" -Method POST -ContentType "application/json" -Body $payload -TimeoutSec 30
    Write-Host "OptimusdB response:"
    $resp | ConvertTo-Json -Depth 10 | Write-Output
  } catch { Write-Warning $_.Exception.Message }
}

function Invoke-QuerySqliteByFilename([string]$base) {
  Write-HR; Write-Host "4) Querying SQLite by filename: $filename"
  $fnameEsc = Sql-Escape $filename
  $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename='$fnameEsc'
ORDER BY id DESC
LIMIT 5;
"@
  $payload = @{ method = @{ cmd="sqldml"; argcnt=1 }; sqldml = $sql } | ConvertTo-Json -Depth 5
  try {
    $resp = Invoke-RestMethod -Uri "$base/$Context/command" -Method POST -ContentType "application/json" -Body $payload -TimeoutSec 30
    Write-Host "SQLDML-by-filename response:"
    $resp | ConvertTo-Json -Depth 10 | Write-Output
  } catch { Write-Warning $_.Exception.Message }
}

function Invoke-QuerySqliteByTemplate([string]$base) {
  Write-HR; Write-Host "5) Querying SQLite by template_id: $script:templateId"
  if ([string]::IsNullOrWhiteSpace($script:templateId)) { Write-Host "No template_id; skipping SQLite query"; return }
  $tidEsc = Sql-Escape $script:templateId
  $sql = @"
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE template_id='$tidEsc'
ORDER BY id DESC
LIMIT 5;
"@
  $payload = @{ method = @{ cmd="sqldml"; argcnt=1 }; sqldml = $sql } | ConvertTo-Json -Depth 5
  try {
    $resp = Invoke-RestMethod -Uri "$base/$Context/command" -Method POST -ContentType "application/json" -Body $payload -TimeoutSec 30
    Write-Host "SQLDML-by-template_id response:"
    $resp | ConvertTo-Json -Depth 10 | Write-Output
  } catch { Write-Warning $_.Exception.Message }
}

function Get-Peers([string]$base) {
  Write-HR; Write-Host "6) Listing peers from $base"
  try {
    $resp = Invoke-RestMethod -Uri "$base/$Context/peers" -Method GET -TimeoutSec 10
    $resp | ConvertTo-Json -Depth 10 | Write-Output
  } catch { Write-Warning $_.Exception.Message }
}

function Show-Logs([string]$base) {
  Write-HR; Write-Host "7) Fetching logs from $base"
  $today = Get-Date -Format "yyyy-MM-dd"
  $hour  = Get-Date -Format "HH"
  $url = "$base/$Context/log?date=$today&hour=$hour"
  try {
    $resp = Invoke-RestMethod -Uri $url -Method GET -TimeoutSec 15
    if ($resp -is [string]) { Write-Host "(raw logs)"; Write-Output $resp } else { $resp | ConvertTo-Json -Depth 10 | Write-Output }
  } catch { Write-Warning $_.Exception.Message }
}

# ---------------- Main ----------------
foreach ($base in $Targets) {
  Test-Reachability $base
  Invoke-UploadTosca $base
  Invoke-QueryOptimusDbByTemplate $base
  Invoke-QuerySqliteByFilename $base
  Invoke-QuerySqliteByTemplate $base
  Get-Peers $base
  Show-Logs $base
}
