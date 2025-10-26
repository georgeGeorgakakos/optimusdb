<#
.SYNOPSIS
    Multi-agent decentralized query scenario for OptimusDB (Final Version)
.DESCRIPTION
    - Inserts multiple records into three separate agents.
    - Waits for decentralized replication.
    - Executes all query strategies from a fourth agent that did not perform inserts.
    - Shows peer connections before and after.
    - Accurately counts results from structured responses.
#>

param(
    [int]$BasePort = 18001,
    [int]$Agents = 8,
    [int]$InsertAgentsCount = 3,
    [int]$QueryAgentIndex = 4
)

$ErrorActionPreference = "Stop"

function As-Json($obj) { $obj | ConvertTo-Json -Depth 20 }

function Write-Header($text) {
    Write-Host "`n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
    Write-Host "🧭 $text" -ForegroundColor Cyan
    Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Cyan
}

function Write-Step($text) { Write-Host ("   ➤ " + $text) -ForegroundColor Yellow }
function Write-Success($text) { Write-Host ("   ✅ " + $text) -ForegroundColor Green }
function Write-Failure($text) { Write-Host ("   ❌ " + $text) -ForegroundColor Red }

function Invoke-OptimusDB {
    param([int]$Port, [object]$Body)
    $uri = "http://localhost:$Port/swarmkb/command"
    $json = As-Json $Body
    try {
        return Invoke-RestMethod -Uri $uri -Method Post -ContentType "application/json" -Body $json -TimeoutSec 20
    } catch {
        Write-Failure "Request failed on $uri"
        return $null
    }
}

function Get-Peers {
    param([int]$Port)
    try {
        $uri = "http://localhost:$Port/swarmkb/peers"
        $peers = Invoke-RestMethod -Uri $uri -Method Get -TimeoutSec 10
        Write-Host "   ↳ Connected Peers on $Port : $($peers.Count)"
        foreach ($p in $peers) { Write-Host "      - $p" }
    } catch {
        Write-Failure "   Could not retrieve peers for port $Port"
    }
}

# ╭─────────────────────────────────────────────────────────────╮
# │ 1️⃣ Peer Snapshot                                           │
# ╰─────────────────────────────────────────────────────────────╯
Write-Header "Peer Discovery Snapshot (Before)"
for ($i=0; $i -lt $Agents; $i++) {
    Get-Peers -Port ($BasePort + $i)
}

# ╭─────────────────────────────────────────────────────────────╮
# │ 2️⃣ Insert Phase                                            │
# ╰─────────────────────────────────────────────────────────────╯
Write-Header "Multi-Agent Insert Phase"

$records = @()
for ($i=0; $i -lt $InsertAgentsCount; $i++) {
    $port = $BasePort + $i
    for ($n=1; $n -le 2; $n++) {
        $recordId = "demo-" + $port + "-" + [guid]::NewGuid().ToString().Substring(0,8)
        $record = @{
            _id = $recordId
            name = "Record-$recordId"
            type = (Get-Random -InputObject @("solar","wind","hydro","geo"))
            status = "active"
            origin = "agent-$port"
            power = (Get-Random -Minimum 50 -Maximum 500)
        }
        $insertBody = @{
            method = @{ cmd = "crudput" }
            dstype = "dsswres"
            criteria = @($record)
        }

        Write-Step "Inserting record $recordId into Agent on port $port..."
        $insertBody | ConvertTo-Json -Depth 20 | ForEach-Object { Write-Host "   $_" }
        $result = Invoke-OptimusDB -Port $port -Body $insertBody
        if ($null -eq $result) {
            Write-Failure "Failed insert on $port"
        } else {
            Write-Success "Inserted record $recordId at Agent port $port"
            $record.AgentPort = $port
            $records += $record
        }
        Start-Sleep -Milliseconds 400
    }
}

Write-Success "✅ Insert phase complete: $($records.Count) records across $InsertAgentsCount agents."
Write-Step "Waiting 8 seconds for decentralized replication..."
Start-Sleep -Seconds 8

# ╭─────────────────────────────────────────────────────────────╮
# │ 3️⃣ Peer Snapshot (After Inserts)                           │
# ╰─────────────────────────────────────────────────────────────╯
Write-Header "Peer Discovery Snapshot (After Inserts)"
for ($i=0; $i -lt $Agents; $i++) {
    Get-Peers -Port ($BasePort + $i)
}

# ╭─────────────────────────────────────────────────────────────╮
# │ 4️⃣ Define Strategies                                       │
# ╰─────────────────────────────────────────────────────────────╯
$QueryPort = $BasePort + $QueryAgentIndex
$regexPattern = "demo-*"
$criteria = @(@{ _id = @{ '$regex' = $regexPattern } })

Write-Header "Query Initiator Agent: port $QueryPort"
Write-Host "This agent has not inserted any record previously." -ForegroundColor White
Write-Host "It will now execute all decentralized strategies." -ForegroundColor White

$strategies = @(
    @{
        Label = "LOCAL_ONLY"
        Description = "Queries only its local datastore (baseline, no peer fan-out)."
        Options = @{ strategy="LOCAL_ONLY"; include_local=$true; annotate_source=$true; consistency="BEST_EFFORT"; time_budget_ms=800 }
    },
    @{
        Label = "REMOTE_ONLY"
        Description = "Skips local store and queries all connected peers."
        Options = @{ strategy="REMOTE_ONLY"; include_local=$false; annotate_source=$true; consistency="BEST_EFFORT"; time_budget_ms=1200 }
    },
    @{
        Label = "LOCAL_THEN_REMOTE_MERGE"
        Description = "Executes local query first, then merges remote peer results sequentially."
        Options = @{ strategy="LOCAL_THEN_REMOTE_MERGE"; include_local=$true; annotate_source=$true; consistency="BEST_EFFORT"; time_budget_ms=1500 }
    },
    @{
        Label = "PARALLEL_MERGE"
        Description = "Runs local + remote queries in parallel (hedged) and merges all results."
        Options = @{ strategy="PARALLEL_MERGE"; include_local=$true; annotate_source=$true; consistency="BEST_EFFORT"; max_peers=5; time_budget_ms=1800 }
    },
    @{
        Label = "QUORUM"
        Description = "Waits until a majority of peers (quorum) respond before returning results."
        Options = @{ strategy="QUORUM"; include_local=$true; annotate_source=$true; consistency="QUORUM"; quorum_n=3; time_budget_ms=2500 }
    }
)

# ╭─────────────────────────────────────────────────────────────╮
# │ 5️⃣ Execute Strategies                                      │
# ╰─────────────────────────────────────────────────────────────╯
$results = @()

foreach ($s in $strategies) {
    $trace = [guid]::NewGuid().ToString().Substring(0,8)
    Write-Header "Executing Strategy: $($s.Label)"
    Write-Host $s.Description -ForegroundColor White
    Write-Step "Initiator Agent: port $QueryPort"
    Write-Step "Trace ID: $trace"
    Write-Step "Searching pattern: $regexPattern"

    $payload = @{
        method = @{ cmd = "query" }
        dstype = "dsswres"
        criteria = $criteria
        options = $s.Options
        trace_id = $trace
    }

    Write-Step "Payload being sent to Agent $QueryPort :"
    $payload | ConvertTo-Json -Depth 20 | ForEach-Object { Write-Host "   $_" }

    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $res = Invoke-OptimusDB -Port $QueryPort -Body $payload
    $sw.Stop()

    if ($null -eq $res) {
        Write-Failure "No response for strategy $($s.Label)"
        continue
    }

    # --- Fixed counting logic ---
    $count = 0
    if ($res -is [System.Collections.IEnumerable] -and -not ($res -is [string])) {
        $count = $res.Count
    }
    elseif ($res.PSObject.Properties.Name -contains "results") {
        $count = $res.results.Count
    }
    elseif ($res.PSObject.Properties.Name -contains "data") {
        $count = $res.data.Count
    }
    else {
        $jsonTmp = $res | ConvertTo-Json -Depth 20
        $count = ($jsonTmp | Select-String '"_id"').Matches.Count
    }

    $json = $res | ConvertTo-Json -Depth 20
    Write-Success ("Completed in {0} ms | Results: {1}" -f $sw.ElapsedMilliseconds, $count)

    Write-Step "Result Payload (first 20 lines):"
    $json.Split("`n") | Select-Object -First 20 | ForEach-Object { Write-Host "   $_" }

    $peerMatches = ($json | Select-String '"peer_id"\s*:\s*"([^"]+)"' -AllMatches).Matches
    if ($peerMatches.Count -gt 0) {
        $peers = ($peerMatches.Groups | Where-Object { $_.Name -eq 1 } | Select-Object -ExpandProperty Value | Sort-Object -Unique)
        Write-Host ("   ↳ Responders (peers): " + ($peers -join ", ")) -ForegroundColor Cyan
    } else {
        Write-Host "   ↳ Responders: (none tagged)" -ForegroundColor DarkGray
    }

    $results += [pscustomobject]@{
        Strategy = $s.Label
        TimeMs = $sw.ElapsedMilliseconds
        Records = $count
    }
}

# ╭─────────────────────────────────────────────────────────────╮
# │ 6️⃣ Summary                                                 │
# ╰─────────────────────────────────────────────────────────────╯
Write-Host "`n==================== Summary ====================" -ForegroundColor Cyan
$results | Format-Table Strategy, TimeMs, Records -AutoSize
Write-Success "🎯 Completed all decentralized query strategies across multi-agent setup."
