
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# OptimusDB TOSCA Upload + Query Script (via Pod IPs)
# - Uploads to /{context}/upload (sends file + filename)
# - Queries OrbitDB by template_id (crudget on tosca_imported)
# - Queries SQLite toscametadata by filename and template_id via sqldml
# ============================================================

FILE="${1:-/home/ubuntu/mytosca.yaml}"   # Path to TOSCA file
PORT="${2:-8089}"                        # OptimusDB HTTP port
CONTEXT="${3:-swarmkb}"                  # API context
NAMESPACE="${4:-optimusdb}"              # Kubernetes namespace

TEMPLATE_ID=""
FILENAME="$(basename "$FILE")"

# ---------------------------
hr() { echo -e "\n============================================================"; }

need_bin(){
  command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 1; }
}

sql_escape(){
  printf "%s" "$1" | sed "s/'/''/g"
}

get_pod_ips() {
  hr
  echo "0) Getting Pod IPs in namespace: $NAMESPACE"
  mapfile -t POD_IPS < <(kubectl -n "$NAMESPACE" get pod -o jsonpath='{range .items[*]}{.status.podIP}{"\n"}{end}')
  if [[ ${#POD_IPS[@]} -eq 0 ]]; then
    echo "No pod IPs found"
    exit 1
  fi
  echo "Found pod IPs: ${POD_IPS[*]}"
}

check_connectivity() {
  local ip="$1"
  local url="http://${ip}:${PORT}/${CONTEXT}/peers"
  hr
  echo "1) Checking connectivity to pod ${ip} at ${url}"
  if curl -s -m 3 "$url" | jq . >/dev/null 2>&1; then
    echo "Pod ${ip} is reachable"
  else
    echo "Pod ${ip} is not reachable or did not return JSON"
  fi
}

upload_tosca() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"

  hr
  echo "2) Uploading TOSCA file: ${FILE} to pod ${ip}"

  if [[ ! -f "$FILE" ]]; then
    echo "File not found: $FILE"
    exit 1
  fi

  # Base64 (single line)
  local b64
  b64=$(base64 -w 0 "$FILE")

  # Send file AND filename so server can persist proper metadata
  local resp
  resp=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"file\":\"${b64}\",\"filename\":\"${FILENAME}\"}" \
    "${base}/upload")

  echo "Upload response (raw):"
  echo "$resp"

  # Extract template_id (supports multiple shapes)
  TEMPLATE_ID=$(echo "$resp" | jq -r '.data.template_id // .template_id // .data.templateId // .templateId // empty' 2>/dev/null || true)

  if [[ -z "$TEMPLATE_ID" || "$TEMPLATE_ID" == "null" ]]; then
    echo "Upload returned no template_id"
  else
    echo "Parsed template_id: ${TEMPLATE_ID}"
  fi
}

query_orbitdb_by_template() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"

  hr
  echo "3) Querying OrbitDB datastore 'tosca_imported' on pod ${ip} for templateId: ${TEMPLATE_ID}"

  if [[ -z "$TEMPLATE_ID" ]]; then
    echo "No template_id available; skipping OrbitDB query"
    return 0
  fi

  local payload
  payload=$(jq -n --arg id "$TEMPLATE_ID" \
    '{ method: {cmd: "crudget", argcnt: 1}, dstype: "tosca_imported", criteria: [{_id: $id}] }')

  local resp
  resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/command")

  echo "OrbitDB response:"
  echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

query_sqlite_by_filename() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"

  hr
  echo "4) Querying SQLite toscametadata by filename: ${FILENAME}"

  local fname_esc; fname_esc=$(sql_escape "$FILENAME")
  local sql="
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename='${fname_esc}'
ORDER BY id DESC
LIMIT 5;
"

  local payload
  payload=$(jq -n --arg sql "$sql" '{method:{cmd:"sqldml",argcnt:1}, sqldml:$sql }')

  local resp
  resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/command")

  echo "SQLDML-by-filename response:"
  echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

query_sqlite_by_template() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"

  hr
  echo "5) Querying SQLite toscametadata by template_id: ${TEMPLATE_ID}"

  if [[ -z "$TEMPLATE_ID" ]]; then
    echo "No template_id available; skipping SQLite query by template_id"
    return 0
  fi

  local tid_esc; tid_esc=$(sql_escape "$TEMPLATE_ID")
  local sql="
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE template_id='${tid_esc}'
ORDER BY id DESC
LIMIT 5;
"

  local payload
  payload=$(jq -n --arg sql "$sql" '{method:{cmd:"sqldml",argcnt:1}, sqldml:$sql }')

  local resp
  resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/command")

  echo "SQLDML-by-template_id response:"
  echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

list_peers() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"
  hr
  echo "6) Listing peers from pod ${ip}"
  curl -s "${base}/peers" | jq . 2>/dev/null || curl -s "${base}/peers"
}

show_logs() {
  local ip="$1"
  local base="http://${ip}:${PORT}/${CONTEXT}"
  hr
  echo "7) Fetching logs from pod ${ip}"

  local today hour resp
  today=$(date +"%Y-%m-%d")
  hour=$(date +"%H")

  resp=$(curl -s "${base}/log?date=${today}&hour=${hour}" || true)
  if echo "$resp" | jq . >/dev/null 2>&1; then
    echo "$resp" | jq .
  else
    echo "(raw logs)"
    echo "$resp"
  fi
}

# ---------------------------
# Main
# ---------------------------
need_bin kubectl
need_bin curl
need_bin jq
need_bin base64

get_pod_ips
for ip in "${POD_IPS[@]}"; do
  check_connectivity "$ip"
  upload_tosca "$ip"
  query_orbitdb_by_template "$ip"
  query_sqlite_by_filename "$ip"
  query_sqlite_by_template "$ip"
  list_peers "$ip"
  show_logs "$ip"
done
