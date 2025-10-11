# Send all JSON test queries in optimusdb_test_queries.zip to OptimusDB REST API

$BaseUrl = "http://localhost:18001/swarmkb/command"
$ContentType = "application/json"

Write-Host "=== Starting OptimusDB Query Tests ==="

Get-ChildItem -Filter *.json | ForEach-Object {
    $file = $_.FullName
    Write-Host "➡️ Sending $file ..."
    $response = Invoke-RestMethod -Uri $BaseUrl -Method POST -ContentType $ContentType -InFile $file
    $outFile = "response_$($_.BaseName).txt"
    $response | Out-File $outFile
    Write-Host "✅ Saved response to $outFile"
    Write-Host "---------------------------------------------"
}

Write-Host "=== All queries completed ==="
