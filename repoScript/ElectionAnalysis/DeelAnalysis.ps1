# Deep Analysis - Why Messages Aren't Propagating

Write-Host "`n========================================" -ForegroundColor Red
Write-Host "DEEP DIVE: Message Propagation Analysis" -ForegroundColor Red
Write-Host "========================================`n" -ForegroundColor Red

# Step 1: Get each node's actual peer ID
Write-Host "[STEP 1] Identifying Each Node's Peer ID..." -ForegroundColor Yellow
$nodeIds = @{}
foreach ($i in 1..8) {
    $nodeIdLine = docker logs optimusdb$i 2>&1 | Select-String "Node ID: (Qm[A-Za-z0-9]+)" | Select-Object -First 1
    if ($nodeIdLine -match "Node ID: (Qm[A-Za-z0-9]+)") {
        $nodeIds["optimusdb$i"] = $matches[1]
        Write-Host "  optimusdb$i : $($matches[1])" -ForegroundColor Cyan
    }
}

# Step 2: Check who each node is receiving FROM
Write-Host "`n[STEP 2] Analyzing Message Sources..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    Write-Host "`n  Node optimusdb$i (ID: $($nodeIds["optimusdb$i"])):" -ForegroundColor Cyan

    $messages = docker logs optimusdb$i 2>&1 | Select-String "from: (Qm[A-Za-z0-9]+)" | Select-Object -Last 20
    $sources = @{}

    foreach ($msg in $messages) {
        if ($msg -match "from: (Qm[A-Za-z0-9]+)") {
            $sourceId = $matches[1]
            if ($sources.ContainsKey($sourceId)) {
                $sources[$sourceId]++
            } else {
                $sources[$sourceId] = 1
            }
        }
    }

    foreach ($source in $sources.Keys) {
        $count = $sources[$source]
        if ($source -eq $nodeIds["optimusdb$i"]) {
            Write-Host "    ✅ From SELF: $source (x$count)" -ForegroundColor Green
        } else {
            Write-Host "    🎉 From OTHER: $source (x$count)" -ForegroundColor Magenta

            # Try to identify which node this is
            foreach ($node in $nodeIds.Keys) {
                if ($nodeIds[$node] -eq $source) {
                    Write-Host "       └─> This is $node" -ForegroundColor Yellow
                    break
                }
            }
        }
    }
}

# Step 3: Check libp2p peer connections
Write-Host "`n[STEP 3] Checking LibP2P Peer Connections..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    Write-Host "`n  optimusdb$i connections:" -ForegroundColor Cyan

    # Look for connection logs
    $connections = docker logs optimusdb$i 2>&1 | Select-String "connected|connection established|peer added" -CaseSensitive:$false | Select-Object -Last 5

    if ($connections) {
        foreach ($conn in $connections) {
            Write-Host "    $conn" -ForegroundColor Gray
        }
    } else {
        Write-Host "    ⚠️  No connection logs found" -ForegroundColor Yellow
    }
}

# Step 4: Check if nodes are discovering each other
Write-Host "`n[STEP 4] Checking Peer Discovery..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    $discovered = docker logs optimusdb$i 2>&1 | Select-String "Discovered peer|found peer|peer discovery" -CaseSensitive:$false | Measure-Object
    Write-Host "  optimusdb$i : Discovered $($discovered.Count) peers" -ForegroundColor Cyan
}

# Step 5: Check GossipSub mesh
Write-Host "`n[STEP 5] Checking GossipSub Mesh Status..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    $mesh = docker logs optimusdb$i 2>&1 | Select-String "mesh|GRAFT|PRUNE" -CaseSensitive:$false | Select-Object -Last 3
    if ($mesh) {
        Write-Host "`n  optimusdb$i mesh activity:" -ForegroundColor Cyan
        foreach ($m in $mesh) {
            Write-Host "    $m" -ForegroundColor Gray
        }
    }
}

# Step 6: Check if messages are being PUBLISHED
Write-Host "`n[STEP 6] Checking Message Publishing..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    $published = docker logs optimusdb$i 2>&1 | Select-String "Published message type|Publish.*message" | Select-Object -Last 2
    if ($published) {
        Write-Host "  optimusdb$i : Publishing OK" -ForegroundColor Green
    } else {
        Write-Host "  optimusdb$i : ⚠️  No publish logs" -ForegroundColor Yellow
    }
}

# Step 7: Network connectivity test
Write-Host "`n[STEP 7] Testing Network Connectivity..." -ForegroundColor Yellow
Write-Host "  Testing if containers can reach each other..." -ForegroundColor Cyan
$ping = docker exec optimusdb1 ping -c 2 optimusdb2 2>&1 | Select-String "2 received"
if ($ping) {
    Write-Host "  ✅ Network connectivity OK (optimusdb1 → optimusdb2)" -ForegroundColor Green
} else {
    Write-Host "  ❌ Network connectivity FAILED" -ForegroundColor Red
}

# Step 8: The critical question
Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSIS" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "Key Observations:" -ForegroundColor Yellow
Write-Host ""

# Check if Node 2 received from someone else
$node2Others = docker logs optimusdb2 2>&1 | Select-String "from: (Qm[A-Za-z0-9]+)" | Select-Object -Last 20
$node2ReceiveFromOthers = $false
foreach ($msg in $node2Others) {
    if ($msg -match "from: (Qm[A-Za-z0-9]+)" -and $matches[1] -ne $nodeIds["optimusdb2"]) {
        $node2ReceiveFromOthers = $true
        break
    }
}

if ($node2ReceiveFromOthers) {
    Write-Host "✅ Node 2 DID receive from another peer" -ForegroundColor Green
    Write-Host "   This means GossipSub CAN propagate messages" -ForegroundColor Green
    Write-Host ""
    Write-Host "❌ BUT nodes mostly receive only their own messages" -ForegroundColor Red
    Write-Host "   This suggests a GossipSub mesh formation issue" -ForegroundColor Red
} else {
    Write-Host "❌ No node is receiving from others consistently" -ForegroundColor Red
    Write-Host "   This suggests:" -ForegroundColor Yellow
    Write-Host "   1. Nodes aren't connecting to each other via libp2p" -ForegroundColor White
    Write-Host "   2. GossipSub mesh isn't forming" -ForegroundColor White
    Write-Host "   3. Peer discovery isn't working" -ForegroundColor White
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "ROOT CAUSE INVESTIGATION" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "The issue is likely ONE of these:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. PEER DISCOVERY ISN'T WORKING" -ForegroundColor White
Write-Host "   → Nodes can't find each other on the network" -ForegroundColor Gray
Write-Host "   → Solution: Check bootstrap nodes, DHT, or mDNS config" -ForegroundColor Gray
Write-Host ""
Write-Host "2. LIBP2P CONNECTIONS AREN'T BEING ESTABLISHED" -ForegroundColor White
Write-Host "   → Nodes discover each other but can't connect" -ForegroundColor Gray
Write-Host "   → Solution: Check firewall, NAT, or Docker networking" -ForegroundColor Gray
Write-Host ""
Write-Host "3. GOSSIPSUB MESH ISN'T FORMING" -ForegroundColor White
Write-Host "   → Nodes connect but don't join the gossip mesh" -ForegroundColor Gray
Write-Host "   → Solution: Check topic subscription and mesh params" -ForegroundColor Gray
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "NEXT STEPS" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "Run these commands to gather more info:" -ForegroundColor Yellow
Write-Host ""
Write-Host "1. Check peer discovery:" -ForegroundColor Cyan
Write-Host "   docker logs optimusdb1 2>&1 | Select-String 'discover|peer.*found'" -ForegroundColor White
Write-Host ""
Write-Host "2. Check libp2p connections:" -ForegroundColor Cyan
Write-Host "   docker logs optimusdb1 2>&1 | Select-String 'connection|connected'" -ForegroundColor White
Write-Host ""
Write-Host "3. Check topic joins:" -ForegroundColor Cyan
Write-Host "   docker logs optimusdb1 2>&1 | Select-String 'JOIN|topic'" -ForegroundColor White
Write-Host ""
Write-Host "4. Check for errors:" -ForegroundColor Cyan
Write-Host "   docker logs optimusdb1 2>&1 | Select-String 'ERROR|error'" -ForegroundColor White
Write-Host ""