#!/usr/bin/env bash
# ====================================================================
# optimusdb_BackupRestoreDemo.sh — End-to-end test for OptimusDB backup/restore
#                    across Ingress-exposed agents.
#
# Runs two phases:
#   Phase 1 — single-agent round-trip (seed, export, delete, re-import, verify)
#   Phase 2 — cross-agent (agent A → archive → agent B, verify arrival)
#
# Usage:
#   ./optimusdb_BackupRestoreDemo.sh                                    # defaults below
#   INGRESS=http://193.225.250.240 ./optimusdb_BackupRestoreDemo.sh     # only change the host
#   AGENT_A=optimusdb1 AGENT_B=optimusdb2 ./optimusdb_BackupRestoreDemo.sh
#
# Environment:
#   INGRESS       Ingress base URL (default: http://193.225.250.240)
#   AGENT_A       agent path prefix for phase 1 + 2 source (default: optimusdb1)
#   AGENT_B       agent path prefix for phase 2 destination (default: optimusdb2)
#                 set to "" to skip phase 2
#   CONTEXT       legacy API context (default: swarmkb)
#   STORE         OrbitDB docstore for seeding (default: dsswres)
#   TOKEN         optional bearer token (Keycloak); script injects Authorization
#   KEEP_TMP      set to 1 to leave /tmp artefacts
#
# URL pattern (Ingress-prefix mode):
#   ${INGRESS}/${AGENT_A}/${CONTEXT}/command          — legacy command API
#   ${INGRESS}/${AGENT_A}/api/v1/exchange/export      — new export endpoint
#   ${INGRESS}/${AGENT_A}/api/v1/exchange/import      — new import endpoint
# ====================================================================

set -u
set -o pipefail

# ────────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────────
INGRESS="${INGRESS:-http://193.225.250.240}"
AGENT_A="${AGENT_A:-optimusdb1}"
AGENT_B="${AGENT_B:-optimusdb2}"
CONTEXT="${CONTEXT:-swarmkb}"
STORE="${STORE:-dsswres}"
TOKEN="${TOKEN:-}"
KEEP_TMP="${KEEP_TMP:-0}"

BASE_A="${INGRESS}/${AGENT_A}"
BASE_B=""
if [[ -n "$AGENT_B" ]]; then
    BASE_B="${INGRESS}/${AGENT_B}"
fi

TMP_DIR="$(mktemp -d -t optimusdb-test.XXXXXX)"
ARCHIVE_A="${TMP_DIR}/${AGENT_A}_export.tar.gz"
EXTRACT_DIR="${TMP_DIR}/inspect"

RUN_ID="test-$(date -u +%Y%m%dT%H%M%S)-$$"
TEST_ID_1="exchange-test-${RUN_ID}-001"
TEST_ID_2="exchange-test-${RUN_ID}-002"
TEST_ID_3="exchange-test-${RUN_ID}-003"

# ────────────────────────────────────────────────────────────────────
# Output helpers
# ────────────────────────────────────────────────────────────────────
PASS=0
FAIL=0
FAILURES=()

say()  { printf '\n\033[1;34m==>\033[0m %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
ok()   { printf '    \033[0;32m✓\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
bad()  { printf '    \033[0;31m✗\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); FAILURES+=("$*"); }

assert_eq() {
    if [[ "$1" == "$2" ]]; then ok "$3: $1"; else bad "$3: got '$1', expected '$2'"; fi
}

assert_contains() {
    if printf '%s' "$1" | grep -qF "$2"; then
        ok "$3"
    else
        bad "$3 — '$2' not found"
    fi
}

cleanup() {
    if [[ "$KEEP_TMP" != "1" ]]; then
        rm -rf "$TMP_DIR"
    else
        info "Kept artefacts at $TMP_DIR"
    fi
}
trap cleanup EXIT

# ────────────────────────────────────────────────────────────────────
# curl wrapper — injects Authorization header if TOKEN set
# ────────────────────────────────────────────────────────────────────
CURL_AUTH=()
if [[ -n "$TOKEN" ]]; then
    CURL_AUTH=(-H "Authorization: Bearer ${TOKEN}")
    info "Auth: bearer token configured"
fi

curl_cmd() {
    curl -sS --max-time 30 "${CURL_AUTH[@]}" "$@"
}

# ────────────────────────────────────────────────────────────────────
# Dependency check
# ────────────────────────────────────────────────────────────────────
for cmd in curl jq tar sqlite3; do
    if ! command -v "$cmd" >/dev/null 2>&1; then
        printf '\033[0;31mError:\033[0m required tool "%s" not found in PATH\n' "$cmd" >&2
        exit 1
    fi
done

# ────────────────────────────────────────────────────────────────────
# Command API helpers
# ────────────────────────────────────────────────────────────────────
command_req() {
    local base="$1"; shift
    local payload="$1"; shift
    curl_cmd -X POST "${base}/${CONTEXT}/command" \
        -H 'Content-Type: application/json' \
        -d "$payload"
}

seed_data() {
    local base="$1"
    command_req "$base" "$(cat <<EOF
{
  "method": {"cmd": "crudput", "argcnt": 2},
  "args": ["seed", "exchange-test"],
  "dstype": "${STORE}",
  "sqlselect": "",
  "graph_traversal": [{}],
  "criteria": [
    {"_id": "${TEST_ID_1}", "resource_provider": "EXCHANGE-TEST",
     "resource_name": "canary-one", "run_id": "${RUN_ID}", "marker": "alpha"},
    {"_id": "${TEST_ID_2}", "resource_provider": "EXCHANGE-TEST",
     "resource_name": "canary-two", "run_id": "${RUN_ID}", "marker": "beta"},
    {"_id": "${TEST_ID_3}", "resource_provider": "EXCHANGE-TEST",
     "resource_name": "canary-three", "run_id": "${RUN_ID}", "marker": "gamma"}
  ]
}
EOF
)"
}

query_by_id() {
    local base="$1"; local doc_id="$2"
    command_req "$base" "$(cat <<EOF
{
  "method": {"cmd": "crudget", "argcnt": 1},
  "args": ["query"],
  "dstype": "${STORE}",
  "sqlselect": "",
  "graph_traversal": [{}],
  "criteria": [{"_id": "${doc_id}"}]
}
EOF
)"
}

delete_by_id() {
    local base="$1"; local doc_id="$2"
    command_req "$base" "$(cat <<EOF
{
  "method": {"cmd": "cruddelete", "argcnt": 1},
  "args": ["delete"],
  "dstype": "${STORE}",
  "sqlselect": "",
  "graph_traversal": [{}],
  "criteria": [{"_id": "${doc_id}"}]
}
EOF
)"
}

# ════════════════════════════════════════════════════════════════════
# Phase 0 — preflight
# ════════════════════════════════════════════════════════════════════
say "Phase 0 — preflight"
info "Ingress: $INGRESS"
info "Agent A: $AGENT_A → $BASE_A"
[[ -n "$BASE_B" ]] && info "Agent B: $AGENT_B → $BASE_B"
info "Store: $STORE"
info "Run ID: $RUN_ID"

# Ingress path-rewrite sanity check: mesh endpoint
mesh_code=$(curl_cmd -o /dev/null -w '%{http_code}' \
    "${BASE_A}/${CONTEXT}/debug/optimusdb/mesh" --max-time 10 || echo "000")
if [[ "$mesh_code" == "200" ]]; then
    ok "Mesh debug endpoint reachable (Ingress path prefix stripping confirmed)"
elif [[ "$mesh_code" == "401" || "$mesh_code" == "403" ]]; then
    bad "Mesh endpoint returns $mesh_code — Keycloak auth required"
    info "Set TOKEN=... env var with a valid bearer token and retry"
    exit 2
else
    bad "Mesh endpoint returned HTTP $mesh_code — cluster routing issue?"
    info "Try: curl -v ${BASE_A}/${CONTEXT}/debug/optimusdb/mesh"
    exit 2
fi

# Exchange endpoint preflight
exp_code=$(curl_cmd -o /dev/null -w '%{http_code}' \
    -X POST "${BASE_A}/api/v1/exchange/export" --max-time 10 || echo "000")

case "$exp_code" in
    200)
        ok "Exchange export endpoint live on agent A"
        ;;
    503)
        bad "Agent A returns 503 — kb.ExchangeService not initialized"
        info "Check pod logs: kubectl logs -n optimusddc optimusdb1-<hash> | grep EXCHANGE"
        exit 2
        ;;
    404)
        bad "Agent A returns 404 — routes not registered in api/http.go"
        info "Rebuild the image and kubectl rollout restart"
        exit 2
        ;;
    *)
        bad "Agent A exchange endpoint returned HTTP $exp_code"
        exit 2
        ;;
esac

# ════════════════════════════════════════════════════════════════════
# Phase 1 — single-agent round-trip
# ════════════════════════════════════════════════════════════════════
say "Phase 1.1 — seed test data on agent A"

seed_response=$(seed_data "$BASE_A")
info "Seed response: $(printf '%s' "$seed_response" | head -c 200)"
sleep 2

got=$(query_by_id "$BASE_A" "$TEST_ID_1")
assert_contains "$got" "$TEST_ID_1" "Seeded doc queryable via /command"

say "Phase 1.2 — export"

http_code=$(curl_cmd -o "$ARCHIVE_A" -w '%{http_code}' \
    -X POST "${BASE_A}/api/v1/exchange/export" --max-time 180)
assert_eq "$http_code" "200" "Export HTTP status"

if [[ -f "$ARCHIVE_A" ]]; then
    archive_size=$(stat -c '%s' "$ARCHIVE_A" 2>/dev/null || stat -f '%z' "$ARCHIVE_A")
    info "Archive: $ARCHIVE_A (${archive_size} bytes)"
    if [[ "$archive_size" -lt 100 ]]; then
        bad "Archive suspiciously small (${archive_size} bytes)"
        info "First 200 bytes: $(head -c 200 "$ARCHIVE_A")"
    else
        ok "Archive non-trivial size"
    fi
else
    bad "Archive file not created"
fi

say "Phase 1.3 — inspect archive structure"

archive_listing=$(tar -tzf "$ARCHIVE_A" 2>/dev/null || echo "")

if printf '%s' "$archive_listing" | grep -q '^manifest\.json$'; then
    ok "manifest.json present"
else
    bad "manifest.json missing"
fi

if printf '%s' "$archive_listing" | grep -q '^sqlite/optimusdb\.db$'; then
    ok "sqlite/optimusdb.db present"
else
    bad "sqlite/optimusdb.db missing"
fi

if printf '%s' "$archive_listing" | grep -q "^orbitdb/${STORE}\.jsonl$"; then
    ok "orbitdb/${STORE}.jsonl present"
else
    bad "orbitdb/${STORE}.jsonl missing"
    info "Archive contents:"
    printf '%s\n' "$archive_listing" | sed 's/^/      /'
fi

manifest_json=$(tar -xzOf "$ARCHIVE_A" manifest.json 2>/dev/null || echo '{}')
manifest_version=$(printf '%s' "$manifest_json" | jq -r '.version // empty')
assert_eq "$manifest_version" "1" "Manifest version"

manifest_has_sqlite=$(printf '%s' "$manifest_json" | jq -r '.has_sqlite // false')
assert_eq "$manifest_has_sqlite" "true" "Manifest has_sqlite flag"

manifest_store=$(printf '%s' "$manifest_json" | jq -r --arg s "$STORE" \
    '.stores // [] | index($s) // empty')
if [[ -n "$manifest_store" ]]; then
    ok "Manifest lists $STORE"
else
    bad "Manifest does NOT list $STORE"
    info "Manifest stores: $(printf '%s' "$manifest_json" | jq -c '.stores // []')"
fi

say "Phase 1.4 — verify seeded docs are in the archive"

seeded_count=$(tar -xzOf "$ARCHIVE_A" "orbitdb/${STORE}.jsonl" 2>/dev/null \
    | grep -c "\"run_id\":\"${RUN_ID}\"" || true)

if [[ "$seeded_count" -ge 3 ]]; then
    ok "All 3 seeded docs present in archive (found ${seeded_count})"
elif [[ "$seeded_count" -gt 0 ]]; then
    bad "Partial seed in archive: found ${seeded_count} of 3"
else
    bad "Zero seeded docs found in archive"
    info "Sample _ids in archive:"
    tar -xzOf "$ARCHIVE_A" "orbitdb/${STORE}.jsonl" 2>/dev/null \
        | head -3 | jq -c '{_id}' 2>/dev/null | sed 's/^/      /'
fi

say "Phase 1.5 — verify SQLite in archive is a valid database"

mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_A" -C "$EXTRACT_DIR" sqlite/optimusdb.db 2>/dev/null

if [[ -f "${EXTRACT_DIR}/sqlite/optimusdb.db" ]]; then
    table_count=$(sqlite3 "${EXTRACT_DIR}/sqlite/optimusdb.db" \
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table';" 2>/dev/null || echo "0")
    if [[ "$table_count" -gt 0 ]]; then
        ok "SQLite file valid, ${table_count} tables"
    else
        bad "SQLite file has no tables"
    fi
else
    bad "SQLite not extractable from archive"
fi

say "Phase 1.6 — destructive round-trip: delete then re-import"

info "Deleting ${TEST_ID_2:0:40}... from agent A"
del_response=$(delete_by_id "$BASE_A" "$TEST_ID_2")
info "Delete response: $(printf '%s' "$del_response" | head -c 200)"
sleep 2

gone=$(query_by_id "$BASE_A" "$TEST_ID_2")
if printf '%s' "$gone" | grep -qF "$TEST_ID_2"; then
    bad "Doc still present after delete — delete may not have worked"
else
    ok "Doc confirmed deleted before import"
fi

info "Re-importing archive into agent A..."
import_response=$(curl_cmd --max-time 300 \
    -X POST "${BASE_A}/api/v1/exchange/import" \
    -F "archive=@${ARCHIVE_A}")

info "Import response: $(printf '%s' "$import_response" | head -c 400)"

sqlite_restored=$(printf '%s' "$import_response" | jq -r '.sqlite_restored // false')
assert_eq "$sqlite_restored" "true" "Import set sqlite_restored=true"

import_errors=$(printf '%s' "$import_response" | jq -r '.errors // [] | length')
assert_eq "$import_errors" "0" "Import reported zero errors"

store_written=$(printf '%s' "$import_response" | jq -r --arg s "$STORE" \
    '.stores[$s] // 0')
if [[ "$store_written" -ge 3 ]]; then
    ok "Import wrote ≥3 docs to $STORE (actual: $store_written)"
else
    bad "Import wrote only $store_written docs (expected ≥3)"
fi

sleep 3

restored=$(query_by_id "$BASE_A" "$TEST_ID_2")
if printf '%s' "$restored" | grep -qF "$TEST_ID_2"; then
    ok "Deleted doc reappeared after import (round-trip confirmed)"
else
    bad "Deleted doc did NOT come back after import"
    info "Query response: $(printf '%s' "$restored" | head -c 200)"
fi

# ════════════════════════════════════════════════════════════════════
# Phase 2 — cross-agent
# ════════════════════════════════════════════════════════════════════
if [[ -z "$BASE_B" ]]; then
    say "Phase 2 — skipped (AGENT_B empty)"
else
    say "Phase 2.1 — preflight agent B"
    info "Agent B: $BASE_B"

    b_code=$(curl_cmd -o /dev/null -w '%{http_code}' \
        -X POST "${BASE_B}/api/v1/exchange/export" --max-time 10 || echo "000")

    case "$b_code" in
        200) ok "Agent B exchange endpoint reachable" ;;
        *)
            bad "Agent B exchange endpoint returned HTTP $b_code"
            info "Skipping remainder of Phase 2"
            b_code="skip"
            ;;
    esac

    if [[ "$b_code" == "200" ]]; then
        say "Phase 2.2 — check if data already exists on B (peer-replicated)"

        # Your mesh has GossipSub-based OrbitDB replication. A freshly seeded
        # doc on A may already be on B before we import — that tells us the
        # mesh is healthy but makes it hard to distinguish import effect.
        pre_b=$(query_by_id "$BASE_B" "$TEST_ID_3")
        if printf '%s' "$pre_b" | grep -qF "$TEST_ID_3"; then
            info "Note: ${TEST_ID_3:0:40}... already on B via OrbitDB peer replication"
            info "      Import effect cannot be cleanly isolated in this configuration"
            info "      — the import will be a no-op for that doc."
            peer_replicated=1
        else
            ok "Doc ${TEST_ID_3:0:40}... NOT yet on B — import effect will be visible"
            peer_replicated=0
        fi

        say "Phase 2.3 — import node-A's archive into B"
        b_import=$(curl_cmd --max-time 300 \
            -X POST "${BASE_B}/api/v1/exchange/import" \
            -F "archive=@${ARCHIVE_A}")
        info "B import response: $(printf '%s' "$b_import" | head -c 400)"

        b_errors=$(printf '%s' "$b_import" | jq -r '.errors // [] | length')
        assert_eq "$b_errors" "0" "Agent B import reported zero errors"

        b_sqlite=$(printf '%s' "$b_import" | jq -r '.sqlite_restored // false')
        assert_eq "$b_sqlite" "true" "Agent B sqlite_restored=true"

        sleep 3

        say "Phase 2.4 — verify data on agent B after import"
        b_got=$(query_by_id "$BASE_B" "$TEST_ID_3")
        if printf '%s' "$b_got" | grep -qF "$TEST_ID_3"; then
            if [[ "$peer_replicated" == "1" ]]; then
                ok "Doc on B (could be peer-replication or import — both valid)"
            else
                ok "Doc arrived on B via import (clean migration confirmed)"
            fi
        else
            bad "Doc NOT found on B after import"
            info "B response: $(printf '%s' "$b_got" | head -c 200)"
        fi
    fi
fi

# ════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════
printf '\n\033[1m====================================================================\033[0m\n'
printf '\033[1mResults:\033[0m  %d passed, %d failed\n' "$PASS" "$FAIL"
printf '\033[1m====================================================================\033[0m\n'

if [[ "$FAIL" -gt 0 ]]; then
    printf '\nFailures:\n'
    for f in "${FAILURES[@]}"; do
        printf '  \033[0;31m•\033[0m %s\n' "$f"
    done
    exit 1
fi

printf '\n\033[0;32mAll checks passed.\033[0m\n'
exit 0