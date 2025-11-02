# Check Peer Discovery and Bootstrap Configuration

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "Peer Discovery & Bootstrap Check" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

# Check 1: Are nodes finding each other?
Write-Host "[CHECK 1] Peer Discovery Status..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    Write-Host "`n  Node optimusdb$i :" -ForegroundColor Cyan

    # Check for peer discovery
    $discovered = docker logs optimusdb$i 2>&1 | Select-String "Discovered|found.*peer|peer.*discovered" -CaseSensitive:$false | Select-Object -Last 5

    if ($discovered.Count -gt 0) {
        Write-Host "    ✅ Has peer discovery activity" -ForegroundColor Green
        $discovered | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    } else {
        Write-Host "    ❌ NO peer discovery activity" -ForegroundColor Red
    }

    # Check for bootstrap
    $bootstrap = docker logs optimusdb$i 2>&1 | Select-String "bootstrap|Bootstrap" | Select-Object -First 3
    if ($bootstrap) {
        Write-Host "    Bootstrap info:" -ForegroundColor Yellow
        $bootstrap | ForEach-Object { Write-Host "      $_" -ForegroundColor Gray }
    }
}

# Check 2: LibP2P addresses
Write-Host "`n[CHECK 2] Node Addresses (Multiaddrs)..." -ForegroundColor Yellow
foreach ($i in 1..8) {
    $addr = docker logs optimusdb$i 2>&1 | Select-String "/ip4.*tcp|multiaddr|listen.*address" -CaseSensitive:$false | Select-Object -First 2
    if ($addr) {
        Write-Host "  optimusdb$i :" -ForegroundColor Cyan
        $addr | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
    }
}

# Check 3: Connection attempts
Write-Host "`n[CHECK 3] Connection Attempts..." -ForegroundColor Yellow
Write-Host "  Checking optimusdb1 for connection attempts..." -ForegroundColor Cyan
$connAttempts = docker logs optimusdb1 2>&1 | Select-String "dial|connect|connection" -CaseSensitive:$false | Select-Object -Last 10
if ($connAttempts) {
    $connAttempts | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
} else {
    Write-Host "    ❌ No connection attempts found" -ForegroundColor Red
}

# Check 4: Docker network configuration
Write-Host "`n[CHECK 4] Docker Network Configuration..." -ForegroundColor Yellow
$network = docker network inspect bridge 2>&1 | Select-String "optimusdb"
if ($network) {
    Write-Host "  ✅ Nodes are on Docker network" -ForegroundColor Green
} else {
    Write-Host "  ⚠️  Checking other networks..." -ForegroundColor Yellow
    $allNetworks = docker network ls
    Write-Host $allNetworks
}

# Check 5: Auto-discovery flags
Write-Host "`n[CHECK 5] Auto-Discovery Configuration..." -ForegroundColor Yellow
$autoDiscovery = docker logs optimusdb1 2>&1 | Select-String "Auto Discovery|MDNS|DHT|autodiscovery"
if ($autoDiscovery) {
    Write-Host "  Auto-discovery status:" -ForegroundColor Cyan
    $autoDiscovery | Select-Object -First 5 | ForEach-Object { Write-Host "    $_" -ForegroundColor Gray }
} else {
    Write-Host "  ⚠️  No auto-discovery logs found" -ForegroundColor Yellow
    Write-Host "     This might be the problem!" -ForegroundColor Red
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "DIAGNOSIS" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "The problem is likely that nodes aren't discovering each other." -ForegroundColor Yellow
Write-Host ""
Write-Host "GossipSub fix is applied correctly, BUT:" -ForegroundColor Green
Write-Host "  - Nodes can't propagate messages to peers they don't know about" -ForegroundColor Red
Write-Host "  - Each node only knows about itself" -ForegroundColor Red
Write-Host "  - No libp2p connections are being established" -ForegroundColor Red
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "SOLUTION" -ForegroundColor Cyan
Write-Host "========================================`n" -ForegroundColor Cyan

Write-Host "You need to enable peer discovery. Check your docker-compose.yml:" -ForegroundColor Yellow
Write-Host ""
Write-Host "Option 1: Use Bootstrap Nodes" -ForegroundColor Cyan
Write-Host "  Add bootstrap peer addresses to connect nodes together" -ForegroundColor White
Write-Host ""
Write-Host "Option 2: Use mDNS (for local discovery)" -ForegroundColor Cyan
Write-Host "  Add flag: --autodiscovery --autodiscovery-mdns" -ForegroundColor White
Write-Host ""
Write-Host "Option 3: Use DHT" -ForegroundColor Cyan
Write-Host "  Add flag: --autodiscovery --autodiscovery-dht" -ForegroundColor White
Write-Host ""

Write-Host "Check your startup command in docker-compose.yml" -ForegroundColor Yellow
Write-Host "It should include auto-discovery flags!" -ForegroundColor Red
Write-Host ""

# Try to show current command
Write-Host "[INFO] Current startup command:" -ForegroundColor Cyan
$compose = Get-Content "docker-compose.yml" -ErrorAction SilentlyContinue
if ($compose) {
    $cmdLines = $compose | Select-String "command:" -Context 0,2
    if ($cmdLines) {
        $cmdLines | ForEach-Object { Write-Host $_ -ForegroundColor Gray }
    } else {
        Write-Host "  Could not find 'command:' in docker-compose.yml" -ForegroundColor Yellow
    }
} else {
    Write-Host "  Could not find docker-compose.yml in current directory" -ForegroundColor Yellow
}

Write-Host ""