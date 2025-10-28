# Diagnostic script to see actual log format
param(
    [Parameter(Mandatory=$true)]
    [string]$ContainerName
)

Write-Host "=== Log Format Diagnostic ===" -ForegroundColor Cyan
Write-Host "Container: $ContainerName`n" -ForegroundColor Yellow

Write-Host "Checking for role assignment patterns..." -ForegroundColor Cyan
Write-Host ""

# Get logs
$logs = docker logs --tail 200 $ContainerName 2>&1

Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Lines containing 'Coordinator':" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
$coordinator = $logs | Select-String -Pattern "Coordinator" | Select-Object -Last 5
if ($coordinator) {
    $coordinator | ForEach-Object { Write-Host $_.Line -ForegroundColor Green }
} else {
    Write-Host "  (none found)" -ForegroundColor Red
}

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Lines containing 'Follower':" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
$follower = $logs | Select-String -Pattern "Follower" | Select-Object -Last 5
if ($follower) {
    $follower | ForEach-Object { Write-Host $_.Line -ForegroundColor Yellow }
} else {
    Write-Host "  (none found)" -ForegroundColor Red
}

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Lines containing 'DEBUG STATE':" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
$debug = $logs | Select-String -Pattern "DEBUG STATE" | Select-Object -Last 3
if ($debug) {
    $debug | ForEach-Object { Write-Host $_.Line -ForegroundColor Cyan }
} else {
    Write-Host "  (none found)" -ForegroundColor Red
}

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Lines containing 'Role=' or 'is now':" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
$role = $logs | Select-String -Pattern "Role=|is now" | Select-Object -Last 5
if ($role) {
    $role | ForEach-Object { Write-Host $_.Line -ForegroundColor Magenta }
} else {
    Write-Host "  (none found)" -ForegroundColor Red
}

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Sample of last 10 lines:" -ForegroundColor Yellow
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
$logs | Select-Object -Last 10 | ForEach-Object { Write-Host $_ -ForegroundColor White }

Write-Host ""
Write-Host "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" -ForegroundColor Gray
Write-Host "Total lines in log:" -ForegroundColor Yellow
Write-Host "  $($logs.Count)" -ForegroundColor White
Write-Host ""