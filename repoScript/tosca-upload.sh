#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# OptimusDB TOSCA Upload + Query Script (Bash version)
# ============================================================

# --- Parameters ---
FILE="${1:-/home/ubuntu/mytosca.yaml}"   # Default file path, can override with $1
SERVER="${2:-localhost}"                 # Default server (arg 2)
PORT="${3:-18001}"                       # Default port (arg 3)
CONTEXT="${4:-swarmkb}"                  # Default context (arg 4)

BASE_URL="http://${SERVER}:${PORT}/${CONTEXT}"

# --- Functions ---
hr() { echo -e "\n============================================================"; }

check_connectivity() {
  hr
  echo "1) Checking connectivity: ${BASE_URL}"
  if curl -s --head "${BASE_URL}" | head -n 1 | grep -q "200"; then
    echo "✅ Service is reachable"
  else
    echo "❌ Cannot reach service at ${BASE_URL}"
    exit 1
  fi
}

upload_tosca() {
  hr
  echo "2) Uploading TOSCA file: $FILE"

  if [[ ! -f "$FILE" ]]; then
    echo "❌ File not found: $FILE"
    exit 1
  fi

  # Encode file to Base64 (single line, no wrapping)
  BASE64_CONTENT=$(base64 -w 0 "$FILE")

  # Upload
  RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"file\":\"${BASE64_CONTENT}\"}" \
    "${BASE_URL}/upload")

  echo "Response: $RESPONSE"

  TEMPLATE_ID=$(echo "$RESPONSE" | jq -r '.templateId // empty')

  if [[ -z "$TEMPLATE_ID" ]]; then
    echo "❌ Upload failed, no templateId returned."
    exit 1
  fi

  echo "✅ Upload OK, Template ID: $TEMPLATE_ID"
}

query_datastore() {
  hr
  echo "3) Querying datastore 'tosca_imported' for templateId: $TEMPLATE_ID"

  QUERY_PAYLOAD=$(jq -n \
    --arg id "$TEMPLATE_ID" \
    '{ method: {cmd: "crudget", argcnt: 1}, dstype: "tosca_imported", criteria: [{_id: $id}] }')

  RESPONSE=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "$QUERY_PAYLOAD" \
    "${BASE_URL}/command")

  echo "Response: $RESPONSE"
}

list_peers() {
  hr
  echo "4) Listing peers"
  curl -s "${BASE_URL}/peers" | jq
}

show_logs() {
  hr
  echo "5) Fetching logs"
  curl -s "${BASE_URL}/logs" | jq
}

# --- Main Execution ---
check_connectivity
upload_tosca
query_datastore
list_peers
show_logs
