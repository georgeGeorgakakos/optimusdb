#!/usr/bin/env bash
# ==============================================================================
# setup.sh — bootstrap optimusCapacity on Linux / macOS.
#
#   ./setup.sh              create .venv and install
#   ./setup.sh --dev        also install pytest
#   ./setup.sh --no-venv    install into the current environment instead
# ==============================================================================
set -euo pipefail

DEV=0
USE_VENV=1
for arg in "$@"; do
    case "$arg" in
        --dev)     DEV=1 ;;
        --no-venv) USE_VENV=0 ;;
        -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
        *) echo "unknown option: $arg" >&2; exit 2 ;;
    esac
done

command -v python3 >/dev/null || { echo "python3 not found"; exit 1; }

PYV=$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')
echo "Python $PYV"
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)' \
    || { echo "Python 3.8+ required, found $PYV"; exit 1; }

PY=python3
if [[ "$USE_VENV" == "1" ]]; then
    if [[ ! -d .venv ]]; then
        echo "Creating .venv ..."
        python3 -m venv .venv
    fi
    # shellcheck disable=SC1091
    source .venv/bin/activate
    PY=python
fi

$PY -m pip install --upgrade pip --quiet

if [[ "$DEV" == "1" ]]; then
    echo "Installing with dev extras ..."
    $PY -m pip install -e ".[dev]" --quiet
else
    echo "Installing ..."
    $PY -m pip install -e ".[yaml]" --quiet
fi

chmod +x capacity_client.py 2>/dev/null || true

[[ -f .env ]] || { cp .env.example .env; echo "Created .env from .env.example — edit OPTIMUSDB_URL"; }

echo
echo "Installed. The CLI is available two ways:"
echo "  optimus-capacity health"
echo "  python3 capacity_client.py health"
echo
[[ "$USE_VENV" == "1" ]] && echo "Activate later with:  source .venv/bin/activate"
echo "Point at your agent:  export OPTIMUSDB_URL=http://localhost:18001"
[[ "$DEV" == "1" ]] && echo "Run the tests:        pytest"
exit 0
