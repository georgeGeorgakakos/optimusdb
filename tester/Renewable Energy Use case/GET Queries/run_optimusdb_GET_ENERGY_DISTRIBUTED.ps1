# Distributed OptimusDB query tester with CSV logging (18001–18008)
# Logs all results in query_results_log.csv

$Ports = 18001..18008
$ContentType = "application/json"
$LogFile = "query_results_log.csv"

# Initialize CSV header
"timestamp,query_file,optimusdb_port,status" | Out-File $LogFile -Encoding UTF8

Write-Host "=== Starting Distributed OptimusDB Query Tests ==="
$i = 0

Get-ChildItem -Filter *.json | ForEach-Object {
    $Port = $Ports[$i % $Ports.Count]
    $Url = "http://localhost:$Port/swarmkb/command"
    $Timestamp = (Get-Date).ToString("yyyy-MM-ddTHH:mm:ss")
    Write-Host "➡️ Sending $($_.Name) → OptimusDB:$Port"

    try {
        $Response = Invoke-RestMethod -Uri $Url -Method POST -ContentType $ContentType -InFile $_.FullName -ErrorAction Stop
        $OutFile = "response_$($_.BaseName)_$Port.txt"
        $Response | Out-File $OutFile
        $Status = "Success"
    } catch {
        $Status = "Fail"
        Write-Host "❌ Failed to send to OptimusDB:$Port — $($_.Exception.Message)"
    }

    "$Timestamp,$($_.Name),$Port,$Status" | Out-File $LogFile -Append -Encoding UTF8
    Write-Host "✅ $($_.Name) logged as $Status at $Timestamp"
    Write-Host "---------------------------------------------"
    $i++
}

Write-Host "=== All distributed queries completed ==="
Write-Host "📄 Log written to: $LogFile"
