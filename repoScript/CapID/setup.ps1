# Bootstrap for optimusCapacity (Windows / PowerShell).
$ErrorActionPreference = "Stop"
python -m pip install -r requirements.txt
Write-Host ""
Write-Host "Ready. Try:"
Write-Host "  python capacity_client.py --url http://localhost:18001 health"
