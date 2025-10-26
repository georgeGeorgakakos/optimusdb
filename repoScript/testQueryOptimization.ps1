# Test-OptimusDB-FullBenchmark-Scenarios-Final.ps1
# Comprehensive decentralized benchmark for OptimusDB
# - ALL strategies & scenarios
# - Correct array-based criteria
# - Per-query trace_id
# - Payload echo + initiator info
# - Responder attribution
# - Adaptive quorum
# - Update propagation + cache tests
# - JSON + CSV exports
# - Full run transcript to file

param(
    [int]$BasePort = 18001,
    [int]$Agents = 8,
    [int]$RecordCount = 50,
    [int]$InitiatorIndex = 7,        # zero-based offset from BasePort (default uses last agent)
    [string]$TranscriptPath = "BenchmarkLog.txt"
)

$ErrorActionPreference = "Stop"

# ────────────────────────────────────────────────────────────────────────────────
# Transcript
# ────────────────────────────────────────────────────────────────────────────────
try { Stop-Transcript | Out-Null } catch {}
Start-Transcript -Path $TranscriptPath -Append | Out-Null

function Write-Info     { param([string]$m) Write-Host $m -ForegroundColor Cyan }
function Write-Success  { param([string]$m) Write-Host $m -ForegroundColor Green }
function Write-Failure  { param([string]$m) Write-Host $m -ForegroundColor Red }
function Write-WarningC { param([string]$m) Write-Host $m -ForegroundColor Yellow }

function As-Json($obj) { $obj | ConvertTo-Json -Depth 20 }

function Invoke-OptimusDBQuery {
    param([int]$Port, [object]$JsonData, [int]$TimeoutSec = 20)
    try {
        $uri = "http://localhost:$Port/swarmkb/command"
        $json = As-Json $JsonData
        return Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $json -TimeoutSec $TimeoutSec
    } catch {
        Write-Failure "❌ Request to port $Port failed: $_"
        return $null
    }
}

function New-Record {
    param([int]$Port)
    $types = @("solar","wind","hydro","geo")
    $status = @("active","inactive")
    $rand = Get-Random
    $recordId = "test-$Port-" + [guid]::NewGuid().ToString().Substring(0,8)
    return @{
        _id    = $recordId
        name   = "Node-$Port-$rand"
        type   = $types[$rand % $types.Count]
        status = $status[$rand % $status.Count]
        power  = (Get-Random -Minimum 50 -Maximum 500)
        origin = "agent-$Port"
    }
}

function Extract-ResponderStats {
    param($results)
    $stats = @{}  # peer_id -> count
    if (-not $results) { return $stats }
    foreach ($row in $results) {
        if ($row._source -and $row._source.peer_id) {
            $pid = [string]$row._source.peer_id
            if (-not $stats.ContainsKey($pid)) { $stats[$pid] = 0 }
            $stats[$pid] += 1
        }
    }
    return $stats
}

function Show-ResponderStats {
    param($stats)
    if (-not $stats.Keys.Count) { Write-Host "   ↳ Responders: (none tagged)"; return }
    $pairs = $stats.GetEnumerator() | Sort-Object -Property Value -Descending | ForEach-Object { "$($_.Key):$($_.Value)" }
    Write-Host ("   ↳ Responders: " + ($pairs -join ", "))
}

function Measure-Query {
    param(
        [string]$Label,
        [int]$InitiatorPort,
        [hashtable]$QueryJson
    )
    # attach trace_id
    $traceId = [guid]::NewGuid().ToString()
    $QueryJson["trace_id"] = $traceId

    Write-Info ("▶️  Running: {0}  (initiator: {1})  trace:{2}" -f $Label, $InitiatorPort, $traceId.Substring(0,8))
    Write-Host "   Payload:"
    $pretty = $QueryJson | ConvertTo-Json -Depth 20
    $pretty.Split("`n") | ForEach-Object { Write-Host ("   " + $_) }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $res = Invoke-OptimusDBQuery -Port $InitiatorPort -JsonData $QueryJson
    $sw.Stop()

    $count = if ($res) { ($res | ConvertTo-Json -Depth 20 | Select-String '"_id"\s*:').Matches.Count } else { 0 }
    $tracePeers = if ($res) { ($res | ConvertTo-Json -Depth 20 | Select-String '"peer_id"\s*:\s*"').Matches.Count } else { 0 }
    $stats = Extract-ResponderStats -results $res

    Write-Host ("⏱️  {0}: {1} ms | Results: {2} | Traced entries: {3}" -f $Label, $sw.ElapsedMilliseconds, $count, $tracePeers)
    Show-ResponderStats -stats $stats

    return @{
        Strategy = $Label
        Initiator = $InitiatorPort
        TimeMs   = $sw.ElapsedMilliseconds
        Count    = $count
        Traces   = $tracePeers
        Responders = $stats
        Results  = $res
        Payload  = $QueryJson
        TraceId  = $traceId
    }
}

# ────────────────────────────────────────────────────────────────────────────────
# 1) Agent Health
# ────────────────────────────────────────────────────────────────────────────────
Write-Info "Checking agent health..."
$peerMap = @{}
for ($i=0; $i -lt $Agents; $i++) {
    $port = $BasePort + $i
    $res = Invoke-OptimusDBQuery -Port $port -JsonData @{ method = @{ cmd = "introspect" } }
    if ($res -and $res.data) {
        $peerMap[$port] = $res.data.peer_id
        Write-Success ("✅ Agent port {0} → {1}" -f $port, $res.data.peer_id)
    } else {
        Write-Failure ("❌ Agent port {0} unreachable" -f $port)
        throw "Aborting: missing agent(s)."
    }
}

# ────────────────────────────────────────────────────────────────────────────────
# 2) Inserts
# ────────────────────────────────────────────────────────────────────────────────
Write-Info "`nSeeding $RecordCount records across $Agents agents..."
$records = @()
for ($n=1; $n -le $RecordCount; $n++) {
    $targetPort = $BasePort + (Get-Random -Minimum 0 -Maximum ($Agents - 1))
    $rec = New-Record -Port $targetPort
    Invoke-OptimusDBQuery -Port $targetPort -JsonData @{
        method = @{ cmd = "crudput" }
        dstype = "dsswres"
        criteria = @($rec)   # <- array of documents
    } | Out-Null
    $rec | Add-Member -NotePropertyName AgentPort -NotePropertyValue $targetPort
    $records += $rec
    Write-Host ("   + {0} → port {1}" -f $rec._id, $targetPort)
}
Write-Success "`n✅ Insert phase complete. Waiting 8s for replication..."
Start-Sleep -Seconds 8

# ────────────────────────────────────────────────────────────────────────────────
# 3) Scenarios
# ────────────────────────────────────────────────────────────────────────────────
$initiatorPort = $BasePort + $InitiatorIndex
$quorumN = [math]::Ceiling($Agents / 2.0)

# Common criteria as ARRAY
$critAll = @(@{ _id = @{ '$regex' = 'test-*' } })


# Define scenarios covering all strategies and knobs
$scenarios = @(
    @{
        Label = "LOCAL_ONLY (baseline local)"
        Options = @{
            strategy="LOCAL_ONLY"; consistency="BEST_EFFORT"; include_local=$true; annotate_source=$true; time_budget_ms=800
        }
    },
    @{
        Label = "REMOTE_ONLY (skip local)"
        Options = @{
            strategy="REMOTE_ONLY"; consistency="BEST_EFFORT"; include_local=$false; annotate_source=$true; max_peers=5; time_budget_ms=1000
        }
    },
    @{
        Label = "LOCAL_THEN_REMOTE_MERGE (sequential merge)"
        Options = @{
            strategy="LOCAL_THEN_REMOTE_MERGE"; consistency="BEST_EFFORT"; include_local=$true; annotate_source=$true; time_budget_ms=1500
        }
    },
    @{
        Label = "PARALLEL_MERGE (hedged)"
        Options = @{
            strategy="PARALLEL_MERGE"; consistency="BEST_EFFORT"; include_local=$true; annotate_source=$true; max_peers=5; time_budget_ms=1800
        }
    },
    @{
        Label = "QUORUM (majority peers)"
        Options = @{
            strategy="QUORUM"; consistency="QUORUM"; quorum_n=$quorumN; min_rows=2; include_local=$true; annotate_source=$true; time_budget_ms=2500
        }
    },
    @{
        Label = "REMOTE_ONLY + max_peers=2 (limited fanout)"
        Options = @{
            strategy="REMOTE_ONLY"; consistency="BEST_EFFORT"; max_peers=2; include_local=$false; annotate_source=$true; time_budget_ms=1200
        }
    },
    @{
        Label = "STALE_OK cache (TTL 10s)"
        Options = @{
            strategy="LOCAL_THEN_REMOTE_MERGE"; consistency="BEST_EFFORT"; stale_ok_ttl_ms=10000; include_local=$true; annotate_source=$true; time_budget_ms=1200
        }
    },
    @{
        Label = "FORCE_REMOTE (probe even if local hits)"
        Options = @{
            strategy="LOCAL_THEN_REMOTE_MERGE"; consistency="BEST_EFFORT"; include_local=$true; annotate_source=$true; time_budget_ms=2200; force_remote=$true
        }
    },
    @{
        Label = "Consistency=ALL (bounded wait)"
        Options = @{
            strategy="PARALLEL_MERGE"; consistency="ALL"; include_local=$true; annotate_source=$true; max_peers=10; time_budget_ms=3500
        }
    },
    @{
        Label = "Zero-hit control (query non-existent prefix)"
        Criteria = @(@{ _id = @{ '$regex' = 'no-such-id-*' } })
        Options = @{
            strategy="REMOTE_ONLY"; consistency="BEST_EFFORT"; include_local=$false; annotate_source=$true; time_budget_ms=1200
        }
    }
)

$results = @()

foreach ($sc in $scenarios) {
    $criteria = if ($sc.ContainsKey("Criteria")) { $sc.Criteria } else { $critAll }
    $payload = @{
        method = @{ cmd = "query" }
        dstype = "dsswres"
        criteria = @(@{ _id = @{ '$regex' = 'test-*' } })  # ← THIS LINE CHANGED
        options = $sc.Options
    }

    $r = Measure-Query -Label $sc.Label -InitiatorPort $initiatorPort -QueryJson $payload
    $results += $r
    Start-Sleep -Milliseconds 300
}

# ────────────────────────────────────────────────────────────────────────────────
# 4) Update Propagation
# ────────────────────────────────────────────────────────────────────────────────
Write-Info "`nTesting propagation with a random update..."
$randomRec = Get-Random -InputObject $records
Invoke-OptimusDBQuery -Port ($BasePort + 2) -JsonData @{
    method = @{ cmd = "crudput" }
    dstype = "dsswres"
    criteria = @(@{ _id = $randomRec._id; status = "inactive" })
} | Out-Null
Start-Sleep -Seconds 6

$verifyPayload = @{
    method = @{ cmd = "query" }
    dstype = "dsswres"
    criteria = @(@{ _id = @{ '$regex' = $randomRec._id } })
    options  = @{ strategy="PARALLEL_MERGE"; consistency="BEST_EFFORT"; time_budget_ms=2000; annotate_source=$true }
}
$verify = Measure-Query -Label "Propagation Verify" -InitiatorPort $BasePort -QueryJson $verifyPayload
if ($verify.Results -and ($verify.Results | ConvertTo-Json -Depth 20) -match '"inactive"') {
    Write-Success ("✅ Update for {0} propagated successfully." -f $randomRec._id)
} else {
    Write-Failure ("❌ Update for {0} not visible across peers." -f $randomRec._id)
}

# ────────────────────────────────────────────────────────────────────────────────
# 5) Cache Warm-up
# ────────────────────────────────────────────────────────────────────────────────
Write-Info "`nRe-running global query to test cache efficiency..."
$cachePayload = @{
    method = @{ cmd = "query" }
    dstype = "dsswres"
    criteria = $critAll
    options  = @{ strategy="LOCAL_THEN_REMOTE_MERGE"; consistency="BEST_EFFORT"; time_budget_ms=900; stale_ok_ttl_ms=10000; annotate_source=$true }
}
$cacheRun = Measure-Query -Label "Cache Requery" -InitiatorPort $initiatorPort -QueryJson $cachePayload
Write-Success "✅ Cache re-query finished."

# ────────────────────────────────────────────────────────────────────────────────
# 6) Summary
# ────────────────────────────────────────────────────────────────────────────────
Write-Host "`n==================== Benchmark Summary ====================" -ForegroundColor Cyan
Write-Host ("Agents: {0} | Records inserted: {1} | Initiator: {2} ({3})" -f $Agents, $RecordCount, $initiatorPort, $peerMap[$initiatorPort])
Write-Host "------------------------------------------------------------"
$expected = $RecordCount
foreach ($r in $results) {
    $ratio = if ($expected -gt 0) { [math]::Round(($r.Count / $expected) * 100, 1) } else { 0 }
    $resp = if ($r.Responders.Keys.Count -gt 0) { ($r.Responders.Keys -join ",") } else { "-" }
    Write-Host ("{0,-36} → {1,5} ms | {2,4} rec | {3,5}% | initiator {4} | responders: {5}" -f $r.Strategy, $r.TimeMs, $r.Count, $ratio, $r.Initiator, $resp)
}

# ────────────────────────────────────────────────────────────────────────────────
# 7) Export Artifacts
# ────────────────────────────────────────────────────────────────────────────────
$artifact = @{
    meta = @{
        base_port = $BasePort; agents = $Agents; records = $RecordCount; initiator_port = $initiatorPort; initiator_peer = $peerMap[$initiatorPort]
        timestamp = [DateTime]::UtcNow.ToString("o")
        transcript = (Resolve-Path $TranscriptPath).Path
    }
    results = $results
    cache   = $cacheRun
    verify  = $verify
}
$artifactPath = Join-Path -Path (Get-Location) -ChildPath "BenchmarkResults.json"
$artifact | ConvertTo-Json -Depth 20 | Out-File -Encoding utf8 $artifactPath
Write-Success ("📁 Results exported → {0}" -f (Resolve-Path $artifactPath))

# Also export a CSV of per-strategy responder tallies
$csv = @()
foreach ($r in $results) {
    if ($r.Responders.Keys.Count -eq 0) {
        $csv += [pscustomobject]@{ Strategy=$r.Strategy; Initiator=$r.Initiator; PeerId="(none)"; Count=0 }
    } else {
        foreach ($k in $r.Responders.Keys) {
            $csv += [pscustomobject]@{ Strategy=$r.Strategy; Initiator=$r.Initiator; PeerId=$k; Count=$r.Responders[$k] }
        }
    }
}
$csvPath = Join-Path (Get-Location) "BenchmarkResponders.csv"
$csv | Export-Csv -Path $csvPath -NoTypeInformation -Encoding UTF8
Write-Success ("📁 Responder CSV exported → {0}" -f (Resolve-Path $csvPath))

Write-Success "✅ Benchmark Completed"

# End transcript
try { Stop-Transcript | Out-Null } catch {}
