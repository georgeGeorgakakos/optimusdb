<#
.SYNOPSIS
    Diagnostic script to test OptimusDB upload API

.DESCRIPTION
    Tests different payload formats to determine what the API expects
#>

param(
    [Parameter(Mandatory=$false)]
    [string]$TestUrl = "http://localhost:18001/swarmkb/upload",

    [Parameter(Mandatory=$false)]
    [string]$TestFile = "sample_1_application_description.yaml"
)

Write-Host "╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║         OptimusDB Upload API Diagnostic Tool                  ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

Write-Host "`nTest Configuration:" -ForegroundColor Yellow
Write-Host "  API Endpoint: $TestUrl" -ForegroundColor Gray
Write-Host "  Test File:    $TestFile" -ForegroundColor Gray

# Check if test file exists
if (-not (Test-Path $TestFile)) {
    Write-Error "Test file not found: $TestFile"
    exit 1
}

# Read file with absolute path
$absolutePath = if ([System.IO.Path]::IsPathRooted($TestFile)) {
    $TestFile
} else {
    Join-Path (Get-Location).Path $TestFile
}

Write-Host "`nResolved path: $absolutePath" -ForegroundColor Gray

if (-not (Test-Path $absolutePath)) {
    Write-Error "Test file not found: $absolutePath"
    exit 1
}

$fileBytes = [System.IO.File]::ReadAllBytes($absolutePath)
$base64Content = [Convert]::ToBase64String($fileBytes)
$fileContent = [System.IO.File]::ReadAllText($absolutePath)

Write-Host "`nFile Information:" -ForegroundColor Yellow
Write-Host "  Size:         $($fileBytes.Length) bytes" -ForegroundColor Gray
Write-Host "  Base64 Size:  $($base64Content.Length) characters" -ForegroundColor Gray

# Test different payload formats
$tests = @(
    @{
        Name = "Format 1: file + filename (CORRECT - from bash script)"
        Payload = @{
            file = $base64Content
            filename = $TestFile
        }
    },
    @{
        Name = "Format 2: file + filename + metadata"
        Payload = @{
            file = $base64Content
            filename = $TestFile
            tosca_type = "ApplicationDescription"
            uploaded_by = $env:USERNAME
            source_host = $env:COMPUTERNAME
        }
    },
    @{
        Name = "Format 3: content + name"
        Payload = @{
            content = $base64Content
            name = $TestFile
            type = "ApplicationDescription"
        }
    },
    @{
        Name = "Format 4: data + filename"
        Payload = @{
            data = $base64Content
            filename = $TestFile
        }
    },
    @{
        Name = "Format 5: raw content (not base64)"
        Payload = @{
            content = $fileContent
            filename = $TestFile
        }
    }
)

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Magenta
Write-Host "║ Testing Different Payload Formats                             ║" -ForegroundColor Magenta
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Magenta

$successfulFormat = $null

foreach ($test in $tests) {
    Write-Host "`n[$($tests.IndexOf($test) + 1)/$($tests.Count)] $($test.Name)" -ForegroundColor Yellow
    Write-Host "Payload keys: $($test.Payload.Keys -join ', ')" -ForegroundColor Gray

    try {
        $headers = @{
            "Content-Type" = "application/json"
        }

        $body = $test.Payload | ConvertTo-Json -Depth 10 -Compress

        # Show first 200 chars of payload
        $preview = if ($body.Length -gt 200) { $body.Substring(0, 200) + "..." } else { $body }
        Write-Host "Payload preview: $preview" -ForegroundColor DarkGray

        $response = Invoke-WebRequest `
            -Uri $TestUrl `
            -Method POST `
            -Headers $headers `
            -Body $body `
            -TimeoutSec 10 `
            -ErrorAction Stop

        Write-Host "✓ SUCCESS! Status Code: $($response.StatusCode)" -ForegroundColor Green
        Write-Host "Response:" -ForegroundColor Green
        $response.Content | Write-Host -ForegroundColor Gray

        $successfulFormat = $test
        break

    } catch {
        $statusCode = $_.Exception.Response.StatusCode.value__
        $statusDesc = $_.Exception.Response.StatusDescription
        Write-Host "✗ Failed: $statusCode $statusDesc" -ForegroundColor Red

        # Try to get error details
        try {
            $errorStream = $_.Exception.Response.GetResponseStream()
            $reader = New-Object System.IO.StreamReader($errorStream)
            $errorBody = $reader.ReadToEnd()
            if ($errorBody) {
                Write-Host "Error details: $errorBody" -ForegroundColor Red
            }
        } catch {
            # Ignore errors reading error stream
        }
    }
}

Write-Host "`n╔════════════════════════════════════════════════════════════════╗" -ForegroundColor Cyan
Write-Host "║ Diagnostic Summary                                            ║" -ForegroundColor Cyan
Write-Host "╚════════════════════════════════════════════════════════════════╝" -ForegroundColor Cyan

if ($successfulFormat) {
    Write-Host "`n✓ Found working format!" -ForegroundColor Green
    Write-Host "`nSuccessful payload format:" -ForegroundColor Yellow
    $successfulFormat.Payload.Keys | ForEach-Object {
        Write-Host "  - $_" -ForegroundColor Gray
    }

    Write-Host "`nUse this format in your upload scripts!" -ForegroundColor Green
} else {
    Write-Host "`n✗ No working format found" -ForegroundColor Red
    Write-Host "`nPossible issues:" -ForegroundColor Yellow
    Write-Host "  1. API endpoint is incorrect" -ForegroundColor Gray
    Write-Host "  2. API requires authentication" -ForegroundColor Gray
    Write-Host "  3. API expects a different content type" -ForegroundColor Gray
    Write-Host "  4. Container is not configured correctly" -ForegroundColor Gray

    Write-Host "`nNext steps:" -ForegroundColor Yellow
    Write-Host "  1. Check container logs: docker logs optimusdb1" -ForegroundColor Gray
    Write-Host "  2. Verify API documentation" -ForegroundColor Gray
    Write-Host "  3. Test with curl:" -ForegroundColor Gray
    Write-Host '     curl -X POST http://localhost:18001/swarmkb/upload -H "Content-Type: application/json" -d "{\"file\":\"test\"}"' -ForegroundColor DarkGray
}

Write-Host ""