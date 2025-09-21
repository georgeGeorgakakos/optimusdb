
#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# OptimusDB TOSCA Upload + Query Script (Node LB / Pod IP / Headless DNS)
# Usage examples:
#   sudo bash ./tosca-uploadV2.sh mytosca.yaml 18001 swarmkb default lb
#   sudo bash ./tosca-uploadV2.sh mytosca.yaml 8089  swarmkb default pod
#   sudo bash ./tosca-uploadV2.sh mytosca.yaml 8089  swarmkb default headless
# ============================================================

# -------- Args --------
FILE="${1:-/home/ubuntu/mytosca.yaml}"   # TOSCA file
PORT="${2:-18001}"                       # Service/LB or container port
CONTEXT="${3:-swarmkb}"                  # API context (e.g., swarmkb)
NAMESPACE="${4:-default}"                # Kubernetes namespace
MODE="${5:-lb}"                          # lb | pod | headless

# -------- Globals --------
CONTAINER_PORT="${CONTAINER_PORT:-8089}" # used for pod/headless if different than PORT
TEMPLATE_ID=""
FILENAME="$(basename "${FILE}")"

# -------- Helpers --------
hr(){ echo -e "\n============================================================"; }
need_bin(){ command -v "$1" >/dev/null 2>&1 || { echo "Missing dependency: $1"; exit 1; }; }
sql_escape(){ printf "%s" "$1" | sed "s/'/''/g"; }

# -------- Discovery --------
get_targets() {
  TARGETS=()
  case "$MODE" in
    lb)
      hr; echo "0) Discovering node IPs and LB services (namespace: $NAMESPACE)"
      mapfile -t NODE_IPS < <(kubectl get nodes -o jsonpath='{range .items[*]}{range .status.addresses[?(@.type=="InternalIP")]}{.address}{"\n"}{end}{end}')
      [[ ${#NODE_IPS[@]} -gt 0 ]] || { echo "No node IPs found"; exit 1; }

      # Services named optimusdb-0, optimusdb-1, optimusdb-2 ...
      mapfile -t SVCNAMES < <(kubectl -n "$NAMESPACE" get svc -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -E '^optimusdb-[0-9]+$' || true)
      [[ ${#SVCNAMES[@]} -gt 0 ]] || { echo "No optimusdb-* LB services found"; exit 1; }

      for svc in "${SVCNAMES[@]}"; do
        # Take first port unless you want to pick by name
        p=$(kubectl -n "$NAMESPACE" get svc "$svc" -o jsonpath='{.spec.ports[0].port}')
        for ip in "${NODE_IPS[@]}"; do
          TARGETS+=("http://${ip}:${p}")
        done
      done
      echo "LB targets: ${TARGETS[*]}"
      ;;

    pod)
      hr; echo "0) Discovering OptimusDB pod IPs (namespace: $NAMESPACE) on port ${CONTAINER_PORT}"
      mapfile -t POD_IPS < <(kubectl -n "$NAMESPACE" get pod -o jsonpath='{range .items[*]}{.metadata.name}{"|"}{.status.podIP}{"\n"}{end}' \
         | grep -E '^optimusdb-[0-9]+\|' | awk -F'|' '{print $2}' | grep -E '^[0-9]+\.' || true)
      [[ ${#POD_IPS[@]} -gt 0 ]] || { echo "No optimusdb pod IPs found"; exit 1; }
      for ip in "${POD_IPS[@]}"; do
        TARGETS+=("http://${ip}:${CONTAINER_PORT}")
      done
      echo "Pod targets: ${TARGETS[*]}"
      ;;

    headless)
      hr; echo "0) Using headless DNS names on port ${CONTAINER_PORT}"
      mapfile -t PODS < <(kubectl -n "$NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -E '^optimusdb-[0-9]+$' | sort -V)
      [[ ${#PODS[@]} -gt 0 ]] || { echo "No optimusdb pods found"; exit 1; }
      for p in "${PODS[@]}"; do
        TARGETS+=("http://${p}.optimusdb-headless.${NAMESPACE}.svc.cluster.local:${CONTAINER_PORT}")
      done
      echo "Headless targets: ${TARGETS[*]}"
      ;;

    *)
      echo "Unknown MODE: $MODE (use lb|pod|headless)"; exit 1;;
  esac
}

check_connectivity() {
  local base="$1"
  hr; echo "1) Checking connectivity at ${base}/${CONTEXT}/peers"
  if curl -s -m 4 "${base}/${CONTEXT}/peers" | jq . >/dev/null 2>&1; then
    echo "Reachable: ${base}"
  else
    echo "Not reachable or not JSON: ${base}"
  fi
}

upload_tosca() {
  local base="$1"
  hr; echo "2) Uploading TOSCA file: ${FILE} to ${base}/${CONTEXT}/upload"
  [[ -f "$FILE" ]] || { echo "File not found: $FILE"; exit 1; }
  local b64; b64=$(base64 -w 0 "$FILE")
  local resp
  resp=$(curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"file\":\"${b64}\",\"filename\":\"${FILENAME}\"}" \
    "${base}/${CONTEXT}/upload")
  echo "Upload response (raw):"
  echo "$resp"
  TEMPLATE_ID=$(echo "$resp" | jq -r '.data.template_id // .template_id // .data.templateId // .templateId // empty' 2>/dev/null || true)
  if [[ -n "$TEMPLATE_ID" && "$TEMPLATE_ID" != "null" ]]; then
    echo "Parsed template_id: ${TEMPLATE_ID}"
  else
    echo "Upload returned no template_id"
  fi
}

query_orbitdb_by_template() {
  local base="$1"
  hr; echo "3) Querying OrbitDB on ${base} for templateId: ${TEMPLATE_ID}"
  [[ -n "$TEMPLATE_ID" ]] || { echo "No template_id; skipping OrbitDB query"; return 0; }
  local payload; payload=$(jq -n --arg id "$TEMPLATE_ID" \
    '{ method: {cmd: "crudget", argcnt: 1}, dstype: "tosca_imported", criteria: [{_id: $id}] }')
  local resp; resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/${CONTEXT}/command")
  echo "OrbitDB response:"; echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

query_sqlite_by_filename() {
  local base="$1"
  hr; echo "4) Querying SQLite by filename: ${FILENAME}"
  local fname_esc; fname_esc=$(sql_escape "$FILENAME")
  local sql=$(
cat <<'SQL'
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE filename='%FILENAME%'
ORDER BY id DESC
LIMIT 5;
SQL
)
  sql="${sql//'%'FILENAME'%'/$fname_esc}"
  local payload; payload=$(jq -n --arg sql "$sql" '{method:{cmd:"sqldml",argcnt:1}, sqldml:$sql }')
  local resp; resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/${CONTEXT}/command")
  echo "SQLDML-by-filename response:"; echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

query_sqlite_by_template() {
  local base="$1"
  hr; echo "5) Querying SQLite by template_id: ${TEMPLATE_ID}"
  [[ -n "$TEMPLATE_ID" ]] || { echo "No template_id; skipping SQLite query"; return 0; }
  local tid_esc; tid_esc=$(sql_escape "$TEMPLATE_ID")
  local sql=$(
cat <<'SQL'
SELECT id, template_id, filename, filesize_bytes, content_sha256, ipfs_path,
       uploader, source_pod, source_ip, description, node_templates_count, created_at
FROM toscametadata
WHERE template_id='%TID%'
ORDER BY id DESC
LIMIT 5;
SQL
)
  sql="${sql//'%'TID'%'/$tid_esc}"
  local payload; payload=$(jq -n --arg sql "$sql" '{method:{cmd:"sqldml",argcnt:1}, sqldml:$sql }')
  local resp; resp=$(curl -s -X POST -H "Content-Type: application/json" -d "$payload" "${base}/${CONTEXT}/command")
  echo "SQLDML-by-template_id response:"; echo "$resp" | jq . 2>/dev/null || echo "$resp"
}

list_peers() {
  local base="$1"
  hr; echo "6) Listing peers from ${base}"
  curl -s "${base}/${CONTEXT}/peers" | jq . 2>/dev/null || curl -s "${base}/${CONTEXT}/peers"
}

show_logs() {
  local base="$1"
  hr; echo "7) Fetching logs from ${base}"
  local today hour resp
  today=$(date +"%Y-%m-%d"); hour=$(date +"%H")
  resp=$(curl -s "${base}/${CONTEXT}/log?date=${today}&hour=${hour}" || true)
  if echo "$resp" | jq . >/dev/null 2>&1; then echo "$resp" | jq .; else echo "(raw logs)"; echo "$resp"; fi
}

# -------- Main --------
need_bin kubectl; need_bin curl; need_bin jq; need_bin base64

get_targets
for base in "${TARGETS[@]}"; do
  check_connectivity "$base"
  upload_tosca "$base"
  query_orbitdb_by_template "$base"
  query_sqlite_by_filename "$base"
  query_sqlite_by_template "$base"
  list_peers "$base"
  show_logs "$base"
done
