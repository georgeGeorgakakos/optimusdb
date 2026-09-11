# ==============================================================================
# setup.ps1 — bootstrap optimusCapacity on Windows / PowerShell.
#
#   .\setup.ps1              create .venv and install
#   .\setup.ps1 -Dev         also install pytest
#   .\setup.ps1 -NoVenv      install into the current environment instead
#
# If execution is blocked:  Set-ExecutionPolicy -Scope Process -Bypass
# ==============================================================================
param(
    [switch]$Dev,
    [switch]$NoVenv
)
$ErrorActionPreference = "Stop"

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Error "python not found on PATH"
}

$pyv = python -c "import sys; print('%d.%d' % sys.version_info[:2])"
Write-Host "Python $pyv"
python -c "import sys; sys.exit(0 if sys.version_info >= (3,8) else 1)"
if ($LASTEXITCODE -ne 0) { Write-Error "Python 3.8+ required, found $pyv" }

if (-not $NoVenv) {
    if (-not (Test-Path ".venv")) {
        Write-Host "Creating .venv ..."
        python -m venv .venv
    }
    & ".\.venv\Scripts\Activate.ps1"
}

python -m pip install --upgrade pip --quiet

if ($Dev) {
    Write-Host "Installing with dev extras ..."
    python -m pip install -e ".[dev]" --quiet
} else {
    Write-Host "Installing ..."
    python -m pip install -e ".[yaml]" --quiet
}

if (-not (Test-Path ".env")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example - edit OPTIMUSDB_URL"
}

Write-Host ""
Write-Host "Installed. The CLI is available two ways:"
Write-Host "  optimus-capacity health"
Write-Host "  python capacity_client.py health"
Write-Host ""
if (-not $NoVenv) { Write-Host "Activate later with:  .\.venv\Scripts\Activate.ps1" }
Write-Host 'Point at your agent:  $env:OPTIMUSDB_URL = "http://localhost:18001"'
if ($Dev) { Write-Host "Run the tests:        pytest" }
