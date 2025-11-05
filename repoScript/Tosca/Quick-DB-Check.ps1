<#
.SYNOPSIS
    Test upload with explicit UTF8 encoding and different methods

.DESCRIPTION
    Tests multiple upload approaches to find what works
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$TestFile = "sample_1_application_description.yaml",

    [Parameter(Mandatory=$false)]
    [string]$TestUrl = "http://localhost:18001/swarmkb/upload"
)

Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         Upload Method Comparison Test                         ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

# Get full path
$fullPath = if ([System.IO.Path]::IsPathRooted($TestFile)) {
    $TestFile
} else {
    Join-Path (Get-Location).Path $TestFile
}

if (-not (Test-Path $fullPath)) {
    Write-Error "File not found: $fullPath"
    exit 1
}

# Read file
$fileBytes = [System.IO.File]::ReadAllBytes($fullPath)
$base64 = [Convert]::ToBase64String($fileBytes)
$filename = [System.IO.Path]::GetFileName($TestFile)

Write-Host "`nFile: $filename" -ForegroundColor Yellow
Write-Host "Size: $($fileBytes.Length) bytes" -ForegroundColor Gray
Write-Host "Base64: $($base64.Length) chars" -ForegroundColor Gray

# ============================================================
# Method 1: Invoke-RestMethod (recommended for APIs)
# ============================================================
Write-Host "`n[Method 1] Invoke-RestMethod with UTF8 encoding" -ForegroundColor Cyan

try {
    $payload = @{
        file = $base64
        filename = $filename
    }

    $response = Invoke-RestMethod `
        -Uri $TestUrl `
        -Method POST `
        -Body ($payload | ConvertTo-Json -Compress) `
        -ContentType "application/json; charset=utf-8" `
        -TimeoutSec 10 `
        -ErrorAction Stop

    Write-Host "✓ SUCCESS!" -ForegroundColor Green
    $response | ConvertTo-Json -Depth 10 | Write-Host

} catch {
    Write-Host "✗ Failed: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.Exception.Response) {
        $statusCode = $_.Exception.Response.StatusCode.value__
        Write-Host "  Status: $statusCode" -ForegroundColor Red
    }
}

# ============================================================
# Method 2: System.Net.WebClient
# ============================================================
Write-Host "`n[Method 2] System.Net.WebClient" -ForegroundColor Cyan

try {
    $webClient = New-Object System.Net.WebClient
    $webClient.Headers.Add("Content-Type", "application/json; charset=utf-8")

    $payload = @{
        file = $base64
        filename = $filename
    }

    $jsonBytes = [System.Text.Encoding]::UTF8.GetBytes(($payload | ConvertTo-Json -Compress))

    $response = $webClient.UploadData($TestUrl, "POST", $jsonBytes)
    $responseText = [System.Text.Encoding]::UTF8.GetString($response)

    Write-Host "✓ SUCCESS!" -ForegroundColor Green
    Write-Host $responseText

} catch {
    Write-Host "✗ Failed: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    if ($webClient) { $webClient.Dispose() }
}

# ============================================================
# Method 3: HttpClient (.NET)
# ============================================================
Write-Host "`n[Method 3] System.Net.Http.HttpClient" -ForegroundColor Cyan

try {
    $httpClient = New-Object System.Net.Http.HttpClient
    $httpClient.Timeout = [TimeSpan]::FromSeconds(30)

    $payload = @{
        file = $base64
        filename = $filename
    }

    $jsonContent = $payload | ConvertTo-Json -Compress
    $content = New-Object System.Net.Http.StringContent(
    $jsonContent,
    [System.Text.Encoding]::UTF8,
    "application/json"
    )

    $result = $httpClient.PostAsync($TestUrl, $content).Result
    $responseBody = $result.Content.ReadAsStringAsync().Result

    if ($result.IsSuccessStatusCode) {
        Write-Host "✓ SUCCESS!" -ForegroundColor Green
        Write-Host "Status: $($result.StatusCode)" -ForegroundColor Green
        Write-Host $responseBody
    } else {
        Write-Host "✗ Failed" -ForegroundColor Red
        Write-Host "Status: $($result.StatusCode)" -ForegroundColor Red
        Write-Host "Response: $responseBody" -ForegroundColor Red
    }

} catch {
    Write-Host "✗ Failed: $($_.Exception.Message)" -ForegroundColor Red
} finally {
    if ($httpClient) { $httpClient.Dispose() }
    if ($content) { $content.Dispose() }
}

# ============================================================
# Method 4: Minimal test with tiny payload
# ============================================================
Write-Host "`n[Method 4] Minimal test with tiny payload" -ForegroundColor Cyan

try {
    $tinyBase64 = [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes("test"))

    $payload = @{
        file = $tinyBase64
        filename = "test.yaml"
    }

    $response = Invoke-RestMethod `
        -Uri $TestUrl `
        -Method POST `
        -Body ($payload | ConvertTo-Json -Compress) `
        -ContentType "application/json; charset=utf-8" `
        -TimeoutSec 10 `
        -ErrorAction Stop

    Write-Host "✓ SUCCESS!" -ForegroundColor Green
    $response | ConvertTo-Json | Write-Host

} catch {
    Write-Host "✗ Failed: $($_.Exception.Message)" -ForegroundColor Red
}

# ============================================================
# Method 5: Check if endpoint exists
# ============================================================
Write-Host "`n[Method 5] Testing endpoint availability" -ForegroundColor Cyan

$endpoints = @(
    "/swarmkb/upload",
    "/swarmkb",
    "/upload",
    "/api/upload",
    "/swarmkb/tosca/upload"
)

foreach ($endpoint in $endpoints) {
    $testUrl = "http://localhost:18001$endpoint"
    try {
        $response = Invoke-WebRequest -Uri $testUrl -Method GET -TimeoutSec 3 -ErrorAction Stop
        Write-Host "  ✓ GET $endpoint - Status: $($response.StatusCode)" -ForegroundColor Green
    } catch {
        $status = if ($_.Exception.Response) { $_.Exception.Response.StatusCode.value__ } else { "N/A" }
        $color = if ($status -eq 405) { "Yellow" } else { "Red" }
        Write-Host "  ✗ GET $endpoint - Status: $status" -ForegroundColor $color
        if ($status -eq 405) {
            Write-Host "    (Method Not Allowed - endpoint exists but doesn't accept GET)" -ForegroundColor DarkYellow
        }
    }
}

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║ Next Steps                                                    ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`n1. Check container logs:" -ForegroundColor Yellow
Write-Host "   docker logs optimusdb1 --tail 100" -ForegroundColor Gray

Write-Host "`n2. Check if API is actually running:" -ForegroundColor Yellow
Write-Host "   docker exec optimusdb1 ps aux" -ForegroundColor Gray

Write-Host "`n3. Test from inside container:" -ForegroundColor Yellow
Write-Host "   docker exec optimusdb1 curl -X POST http://localhost:8089/swarmkb/upload -H 'Content-Type: application/json' -d '{\"file\":\"dGVzdA==\",\"filename\":\"test.yaml\"}'" -ForegroundColor Gray

Write-Host ""