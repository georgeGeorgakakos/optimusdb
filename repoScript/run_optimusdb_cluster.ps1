# Allow this script to run without requiring manual execution policy changes
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

# Stop on errors
$ErrorActionPreference = "Stop"

# Create the network if it doesn't exist
if (-not (docker network ls --format "{{.Name}}" | Select-String -Pattern "^swarmnet$")) {
    Write-Host "Creating Docker network 'swarmnet'..."
    docker network create swarmnet | Out-Null
}

# Docker image name
$image = "optimusdb"

# Generate nodes dynamically
$nodes = 1..8 | ForEach-Object {
    [PSCustomObject]@{
        Name   = "optimusdb$_"
        Http   = 18000 + $_
        RPC    = 14000 + $_
        Gossip = 15000 + $_
    }
}

# Launch containers
foreach ($n in $nodes) {
    Write-Host "Starting $($n.Name)..."
    # Remove existing container if it exists
    if (docker ps -a --format "{{.Names}}" | Select-String -Pattern "^$($n.Name)$") {
        Write-Host "Removing existing container $($n.Name)..."
        docker rm -f $($n.Name) | Out-Null
    }

    docker run -d `
        --network swarmnet `
        --name $($n.Name) `
        -p "$($n.Http):8089" `
        -p "$($n.RPC):4001" `
        -p "$($n.Gossip):5001" `
        $image | Out-Null
}

Write-Host "`nAll OptimusDB nodes launched successfully."
Write-Host "Use 'docker ps' to verify container status."
