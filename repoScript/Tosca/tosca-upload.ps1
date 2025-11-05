param(
  [string]$File    = "C:\Users\georg\Desktop\mytosca.yaml",
  [string]$Server  = "localhost",
  [int]   $Port    = 18001,
  [string]$Context = "swarmkb"
)

# ---- helpers ---------------------------------------------------------------

function Fail($msg) { throw $msg }

function Json($hashtableOrObject) {
  $hashtableOrObject | ConvertTo-Json -Depth 10
}

# Avoid string interpolation pitfalls with ":" by using -f
$Base = "http://{0}:{1}/{2}" -f $Server, $Port, $Context

Write-Host "=== Checking connectivity: $Base ==="
try {
  $probe = Test-NetConnection -ComputerName $Server -Port $Port -WarningAction SilentlyContinue
  if (-not $probe.TcpTestSucceeded) {
    Fail ("Cannot reach {0}:{1}. Is the service up and port open?" -f $Server, $Port) }

} catch { Fail $_ }

if (!(Test-Path $File)) { Fail "File not found: $File" }

# ----------------------------------------------------------------------------
# 1) UPLOAD TOSCA
# ----------------------------------------------------------------------------
Write-Host "`n=== 1) Upload TOSCA ==="
try {
  $bytes = [IO.File]::ReadAllBytes($File)
  $b64   = [Convert]::ToBase64String($bytes)
  $body  = Json @{ file = $b64 }
  $uri   = "$Base/upload"

  Write-Host "POST $uri"
  $resp  = Invoke-RestMethod -Method Post -Uri $uri -ContentType 'application/json' -Body $body

  # Handler may return either {status:200,data:{...}} or a flat object depending on your version.
  # Normalize:
  $payload = if ($resp.data) { $resp.data } else { $resp }

  if ($payload.message -and $payload.template_id) {
    Write-Host "✅ Upload OK: $($payload.message)"
    $TemplateId = $payload.template_id
    Write-Host "Template ID : $TemplateId"
    if ($payload.node_count) {
      Write-Host "Node Count  : $($payload.node_count)"
    }
  } else {
    Write-Host "⚠️ Upload returned unexpected shape:"
    $resp | Format-List
  }
} catch {
  Write-Host "❌ Upload failed:"
  Write-Host $_.Exception.Message
  if ($_.ErrorDetails.Message) { Write-Host $_.ErrorDetails.Message }
  exit 1
}

# ----------------------------------------------------------------------------
# 2) QUERY the tosca_imported store via /command (by id if we got one)
# ----------------------------------------------------------------------------
Write-Host "`n=== 2) Query tosca_imported via /command ==="
try {
  $cmdUri = "$Base/command"

  # If we have a TemplateId, query that specific doc; otherwise list all
	  if ($TemplateId) {
		$cmd = @{
			method   = @{ cmd = "crudget"; argcnt = 1 }
			dstype   = "tosca_imported"
			criteria = @(@{ _id = $TemplateId })
    } | ConvertTo-Json -Depth 6
	} else {
		$cmd = @{
			method   = @{ cmd = "crudget"; argcnt = 0 }
			dstype   = "tosca_imported"
			criteria = @()
		} | ConvertTo-Json -Depth 6
	}


  Write-Host "POST $cmdUri"
  $res = Invoke-RestMethod -Method Post -Uri $cmdUri -ContentType 'application/json' -Body $cmd

  if ($res.status -and $res.data) {
    Write-Host "✅ Command OK (wrapped):"
    $res.data | Format-List
  } else {
    Write-Host "✅ Command OK:"
    $res | Format-List
  }
} catch {
  Write-Host "❌ Command failed:"
  Write-Host $_.Exception.Message
  if ($_.ErrorDetails.Message) { Write-Host $_.ErrorDetails.Message }
}

# ----------------------------------------------------------------------------
# 3) /peers
# ----------------------------------------------------------------------------
Write-Host "`n=== 3) List peers ==="
try {
  $peers = Invoke-RestMethod -Method Get -Uri "$Base/peers"
  $peers | Format-Table -AutoSize
} catch {
  Write-Host "❌ Peers call failed:"
  Write-Host $_.Exception.Message
}

# ----------------------------------------------------------------------------
# 4) /log (today/current hour)
# ----------------------------------------------------------------------------
Write-Host "`n=== 4) Logs (today/current hour) ==="
try {
  $today = Get-Date -Format "yyyy-MM-dd"
  $hour  = (Get-Date).Hour.ToString("D2")
  $logs  = Invoke-RestMethod -Method Get -Uri "$Base/log?date=$today&hour=$hour"
  if ($logs) { $logs | Format-Table -AutoSize } else { Write-Host "(no logs)" }
} catch {
  Write-Host "❌ Log call failed:"
  Write-Host $_.Exception.Message
}

# ----------------------------------------------------------------------------
# 5) /benchmarks (if enabled)
# ----------------------------------------------------------------------------
Write-Host "`n=== 5) Benchmarks (if enabled) ==="
try {
  $bm = Invoke-RestMethod -Method Get -Uri "$Base/benchmarks"
  if ($bm) { $bm | Format-Table -AutoSize } else { Write-Host "(no benchmarks)" }
} catch {
  Write-Host "ℹ️ Benchmarks endpoint not available or disabled"
}
