#!/usr/bin/env bash
# Bootstrap for optimusCapacity (Linux / macOS).
set -e
python3 -m pip install --user -r requirements.txt
chmod +x capacity_client.py
echo
echo "Ready. Try:"
echo "  python3 capacity_client.py --url http://localhost:18001 health"
