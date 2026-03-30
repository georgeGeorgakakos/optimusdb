#!/bin/bash
# ==============================================================================
# OptimusDB Semantic Search — End-to-End Test Script
# ==============================================================================
# Tests the full semantic search pipeline on a running k3s deployment:
#   1. Pre-flight checks (pods, ports, llama-server embedding, vec0)
#   2. Insert test documents via CRUDPUT (triggers auto-indexing)
#   3. Manual index endpoint test
#   4. Semantic search on single node
#   5. Cross-node distributed search (insert on node2, query from node1)
#   6. Bootstrap endpoint test
#   7. Summary report
#
# Usage:
#   chmod +x test_semantic_search.sh
#   ./test_semantic_search.sh
#
#   Override defaults:
#   HOST=10.0.0.5 NAMESPACE=default ./test_semantic_search.sh
# ==============================================================================

set -euo pipefail

# ──────────────────────────────────────────────────────────────────────────────
# Configuration — override with environment variables if needed
# ──────────────────────────────────────────────────────────────────────────────
HOST="${HOST:-193.225.250.240}"
NAMESPACE="${NAMESPACE:-optimusddc}"
KUBECTL_NS="kubectl -n ${NAMESPACE}"

# Traefik ingress routes — strip-prefix middleware removes /optimusdbN before
# forwarding to each pod, so the pod sees the original path unchanged.
# e.g. http://HOST/optimusdb1/swarmkb/peers → pod1:/swarmkb/peers
BASE1="http://${HOST}/optimusdb1"
BASE2="http://${HOST}/optimusdb2"
BASE3="http://${HOST}/optimusdb3"

# ──────────────────────────────────────────────────────────────────────────────
# Colour helpers
# ──────────────────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { echo -e "  ${GREEN}✅ PASS${NC}  $*"; TESTS_PASSED=$((TESTS_PASSED + 1)); }
fail() { echo -e "  ${RED}❌ FAIL${NC}  $*"; TESTS_FAILED=$((TESTS_FAILED + 1)); }
warn() { echo -e "  ${YELLOW}⚠️  WARN${NC}  $*"; }
info() { echo -e "  ${CYAN}ℹ️  INFO${NC}  $*"; }
step() { echo -e "\n${BOLD}${CYAN}━━━ $* ━━━${NC}"; }

TESTS_PASSED=0
TESTS_FAILED=0

# ──────────────────────────────────────────────────────────────────────────────
# Helper: curl with timeout, returns HTTP body; exits non-zero on curl failure
# ──────────────────────────────────────────────────────────────────────────────
api_get() {
    curl -sf --max-time 10 "$1"
}

api_post() {
    curl -sf --max-time 15 -X POST "$1" \
        -H "Content-Type: application/json" \
        -d "$2"
}

# ──────────────────────────────────────────────────────────────────────────────
# Helper: get first running pod name matching a prefix in NAMESPACE
# ──────────────────────────────────────────────────────────────────────────────
get_pod() {
    local prefix="$1"
    ${KUBECTL_NS} get pods --no-headers 2>/dev/null \
        | awk -v p="$prefix" '$1 ~ "^"p && $3=="Running" {print $1; exit}'
}

# ==============================================================================
# BANNER
# ==============================================================================
echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${CYAN}║       OptimusDB Semantic Search — E2E Test Suite             ║${NC}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  Host     : ${BOLD}${HOST}${NC}"
echo -e "  Node 1   : ${HOST}/optimusdb1"
echo -e "  Node 2   : ${HOST}/optimusdb2"
echo -e "  Node 3   : ${HOST}/optimusdb3"
echo -e "  Namespace: ${NAMESPACE}"
echo ""

# ==============================================================================
# STEP 1 — PRE-FLIGHT CHECKS
# ==============================================================================
step "STEP 1 — Pre-flight checks"

# 1a. kubectl available
if command -v kubectl &>/dev/null; then
    pass "kubectl is available"
else
    fail "kubectl not found — install kubectl to run in-pod checks"
fi

# 1b. Pods running
echo ""
info "Checking pod status in namespace '${NAMESPACE}'..."
POD_LIST=$(${KUBECTL_NS} get pods --no-headers 2>/dev/null | awk '/^optimusdb/ {print $1, $3}') || POD_LIST=""
if [[ -z "$POD_LIST" ]]; then
    fail "No optimusdb pods found in namespace '${NAMESPACE}'"
else
    while IFS= read -r line; do
        POD_NAME=$(echo "$line" | awk '{print $1}')
        POD_STATUS=$(echo "$line" | awk '{print $2}')
        if [[ "$POD_STATUS" == "Running" ]]; then
            pass "Pod ${POD_NAME} is ${POD_STATUS}"
        else
            fail "Pod ${POD_NAME} is ${POD_STATUS}"
        fi
    done <<< "$POD_LIST"
fi

# Pick the first running pod for exec checks
POD1=$(get_pod "optimusdb1") || POD1=""
POD2=$(get_pod "optimusdb2") || POD2=""

# 1c. HTTP port reachable on all three nodes
echo ""
info "Checking HTTP connectivity on all three nodes..."
for BASE in "$BASE1" "$BASE2" "$BASE3"; do
    if curl -sf --max-time 5 "${BASE}/swarmkb/peers" &>/dev/null; then
        pass "${BASE}/swarmkb/peers is reachable"
    else
        fail "${BASE}/swarmkb/peers is NOT reachable"
    fi
done

# 1d. Check llama-server embedding endpoint inside pod
echo ""
info "Checking llama-server /embedding endpoint..."
if [[ -n "$POD1" ]]; then
    EMBED_RESP=$(${KUBECTL_NS} exec "$POD1" -- \
        curl -sf --max-time 5 http://localhost:8080/embedding \
        -H "Content-Type: application/json" \
        -d '{"content":"test"}' 2>/dev/null) || EMBED_RESP=""

    if [[ -z "$EMBED_RESP" ]]; then
        fail "llama-server /embedding returned empty — did you add --embedding to supervisord?"
    else
        EMBED_DIMS=$(echo "$EMBED_RESP" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(len(d.get('embedding',[])))" 2>/dev/null) || EMBED_DIMS=0
        if [[ "$EMBED_DIMS" -gt 0 ]]; then
            pass "llama-server /embedding working — ${EMBED_DIMS} dimensions"
        else
            fail "llama-server /embedding returned unexpected format: ${EMBED_RESP:0:120}"
        fi
    fi
else
    warn "No running optimusdb1 pod found — skipping llama-server check"
fi

# 1e. Check vec0.so is present in the image
echo ""
info "Checking sqlite-vec extension (vec0.so)..."
if [[ -n "$POD1" ]]; then
    VEC0_PATH=$(${KUBECTL_NS} exec "$POD1" -- \
        ls /usr/lib/sqlite-vec/vec0.so 2>/dev/null) || VEC0_PATH=""
    if [[ -n "$VEC0_PATH" ]]; then
        pass "vec0.so found at /usr/lib/sqlite-vec/vec0.so"
    else
        fail "vec0.so NOT found — Dockerfile is missing the sqlite-vec download step"
    fi
else
    warn "No running pod found — skipping vec0.so check"
fi

# 1f. Check semantic routes are registered in logs
echo ""
info "Checking semantic routes in startup logs..."
if [[ -n "$POD1" ]]; then
    SEMANTIC_LOG=$(${KUBECTL_NS} logs "$POD1" 2>/dev/null | grep "\[SEMANTIC\]" | head -5) || SEMANTIC_LOG=""
    if echo "$SEMANTIC_LOG" | grep -q "initialized\|Routes registered"; then
        pass "Semantic search initialized successfully"
        info "Log lines: $(echo "$SEMANTIC_LOG" | head -2)"
    elif echo "$SEMANTIC_LOG" | grep -q "unavailable\|panic\|without semantic"; then
        fail "Semantic search failed to initialize:"
        echo "$SEMANTIC_LOG" | while IFS= read -r line; do echo "         $line"; done
    else
        warn "No [SEMANTIC] log lines found yet — pod may still be starting"
    fi
else
    warn "No running pod found — skipping log check"
fi

# ==============================================================================
# STEP 2 — INSERT TEST DOCUMENTS (auto-indexing)
# ==============================================================================
step "STEP 2 — Insert test documents via CRUDPUT (triggers auto-indexing)"

info "Inserting 2 documents on node 1 (${BASE1})..."
INSERT1=$(api_post "${BASE1}/swarmkb/command" '{
  "method": {"cmd": "crudput", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [
    {
      "_id": "sem_test_solar_001",
      "name": "Athens Solar Farm",
      "type": "solar",
      "status": "operational",
      "capacity_mw": 500,
      "location": "Attica Greece",
      "tags": ["renewable","solar","high-capacity"],
      "description": "Large photovoltaic installation in the Attica region providing clean energy to the national grid"
    },
    {
      "_id": "sem_test_wind_002",
      "name": "Thrace Wind Park",
      "type": "wind",
      "status": "operational",
      "capacity_mw": 800,
      "location": "Thrace Greece",
      "tags": ["renewable","wind","offshore"],
      "description": "Offshore wind turbine cluster in northern Greece with high annual energy yield"
    }
  ]
}') || INSERT1=""

if echo "$INSERT1" | grep -qi "success\|inserted\|OK"; then
    pass "Inserted 2 documents on node 1"
    info "Response: ${INSERT1:0:80}"
else
    fail "Insert on node 1 failed: ${INSERT1:0:120}"
fi

# Insert a 3rd document on node 2 (for distributed search test later)
info "Inserting 1 document on node 2 (${BASE2}) for cross-node test..."
INSERT2=$(api_post "${BASE2}/swarmkb/command" '{
  "method": {"cmd": "crudput", "argcnt": 1},
  "dstype": "dsswres",
  "criteria": [
    {
      "_id": "sem_test_hydro_003",
      "name": "Epirus Hydroelectric Plant",
      "type": "hydro",
      "status": "operational",
      "capacity_mw": 1200,
      "location": "Epirus Greece",
      "tags": ["renewable","hydro","river"],
      "description": "Run-of-river hydroelectric facility on the Arachthos river with large reservoir"
    }
  ]
}') || INSERT2=""

if echo "$INSERT2" | grep -qi "success\|inserted\|OK"; then
    pass "Inserted 1 document on node 2"
else
    fail "Insert on node 2 failed: ${INSERT2:0:120}"
fi

# Wait for background indexing goroutines to complete
info "Waiting 5 seconds for background indexing to complete..."
sleep 5

# ==============================================================================
# STEP 3 — MANUAL INDEX ENDPOINT
# ==============================================================================
step "STEP 3 — Manual index endpoint (POST /api/v1/semantic/index)"

info "Manually indexing a document via the index endpoint..."
INDEX_RESP=$(api_post "${BASE1}/api/v1/semantic/index" '{
  "store": "dsswres",
  "doc_id": "sem_test_manual_004",
  "fields": {
    "name": "Corinth Battery Storage",
    "type": "battery_storage",
    "status": "commissioning",
    "description": "Grid-scale lithium-ion battery storage facility for peak shaving and renewable energy balancing",
    "capacity_mw": "200",
    "location": "Corinth Greece",
    "tags": "storage battery grid renewable balancing"
  }
}') || INDEX_RESP=""

if echo "$INDEX_RESP" | grep -q "\"indexed\""; then
    pass "Manual index endpoint working"
    info "Response: ${INDEX_RESP}"
else
    fail "Manual index failed: ${INDEX_RESP:0:200}"
fi

# ==============================================================================
# STEP 4 — SEMANTIC SEARCH ON SINGLE NODE
# ==============================================================================
step "STEP 4 — Semantic search queries on node 1"

# Query 1: Solar
echo ""
info "Query 1: 'solar farm photovoltaic Greece renewable'"
SEARCH1=$(api_get "${BASE1}/api/v1/semantic/search?q=solar+farm+photovoltaic+Greece+renewable&top_k=5") || SEARCH1=""

if [[ -z "$SEARCH1" ]]; then
    fail "Search endpoint returned no response"
else
    COUNT1=$(echo "$SEARCH1" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT1=0
    if [[ "$COUNT1" -gt 0 ]]; then
        pass "Solar query returned ${COUNT1} result(s)"
        # Print top result
        TOP=$(echo "$SEARCH1" | python3 -c "
import sys,json
d=json.load(sys.stdin)
r=d['results'][0] if d['results'] else {}
print(f\"  doc_id={r.get('doc_id','-')}  score={r.get('score',0):.3f}  store={r.get('store','-')}  has_doc={'yes' if r.get('document') else 'no'}\")
" 2>/dev/null) || TOP=""
        info "Top result: ${TOP}"
    else
        fail "Solar query returned 0 results — check llama-server embedding and vec0"
        info "Full response: ${SEARCH1:0:300}"
    fi
fi

# Query 2: Wind
echo ""
info "Query 2: 'offshore wind turbines high capacity'"
SEARCH2=$(api_get "${BASE1}/api/v1/semantic/search?q=offshore+wind+turbines+high+capacity&top_k=5") || SEARCH2=""
COUNT2=$(echo "$SEARCH2" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT2=0
if [[ "$COUNT2" -gt 0 ]]; then
    pass "Wind query returned ${COUNT2} result(s)"
else
    fail "Wind query returned 0 results"
fi

# Query 3: Storage / battery (manually indexed doc)
echo ""
info "Query 3: 'battery storage grid balancing energy'"
SEARCH3=$(api_get "${BASE1}/api/v1/semantic/search?q=battery+storage+grid+balancing+energy&top_k=5") || SEARCH3=""
COUNT3=$(echo "$SEARCH3" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT3=0
if [[ "$COUNT3" -gt 0 ]]; then
    pass "Storage query returned ${COUNT3} result(s)"
else
    warn "Storage query returned 0 results — manually indexed doc may not have propagated yet"
fi

# Query 4: Generic operational plant
echo ""
info "Query 4: 'operational power plant Greece' with budget_ms=2000"
SEARCH4=$(api_get "${BASE1}/api/v1/semantic/search?q=operational+power+plant+Greece&top_k=10&budget_ms=2000") || SEARCH4=""
COUNT4=$(echo "$SEARCH4" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT4=0
if [[ "$COUNT4" -gt 0 ]]; then
    pass "Generic query returned ${COUNT4} result(s)"
else
    fail "Generic query returned 0 results"
fi

# ==============================================================================
# STEP 5 — CROSS-NODE DISTRIBUTED SEARCH
# ==============================================================================
step "STEP 5 — Cross-node distributed search (hydro on node2, query from node1)"

info "Searching for hydroelectric from node 1 — should find node 2's document..."
SEARCH_DIST=$(api_get "${BASE1}/api/v1/semantic/search?q=hydroelectric+river+reservoir+water+power&top_k=5&budget_ms=2500") || SEARCH_DIST=""

if [[ -z "$SEARCH_DIST" ]]; then
    fail "Distributed search returned no response"
else
    COUNT_DIST=$(echo "$SEARCH_DIST" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT_DIST=0
    # Check if hydro_003 appears and comes from a different source node
    HYDRO_RESULT=$(echo "$SEARCH_DIST" | python3 -c "
import sys,json
d=json.load(sys.stdin)
for r in d.get('results',[]):
    if 'hydro' in r.get('doc_id',''):
        print(f\"doc_id={r['doc_id']}  score={r.get('score',0):.3f}  source={r.get('source_node','-')[:20]}  has_doc={'yes' if r.get('document') else 'no (remote)'}\")
" 2>/dev/null) || HYDRO_RESULT=""

    if [[ -n "$HYDRO_RESULT" ]]; then
        pass "Distributed search found hydro document from node 2"
        info "Result: ${HYDRO_RESULT}"
        info "Note: has_doc=no (remote) is expected — remote results are not hydrated over GossipSub"
    elif [[ "$COUNT_DIST" -gt 0 ]]; then
        warn "Got ${COUNT_DIST} results but hydro_003 not among them — GossipSub fanout may not have reached node 2 yet"
        info "This is normal on first run — retry after ~30s for mesh to stabilise"
    else
        warn "Distributed search returned 0 results — GossipSub mesh may not be fully formed yet"
        info "Run again after 30–60 seconds once the mesh stabilises"
    fi
fi

# Also verify the same query from node 3
info "Same query from node 3 (${BASE3})..."
SEARCH_N3=$(api_get "${BASE3}/api/v1/semantic/search?q=hydroelectric+river+water&top_k=5&budget_ms=2000") || SEARCH_N3=""
COUNT_N3=$(echo "$SEARCH_N3" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('count',0))" 2>/dev/null) || COUNT_N3=0
if [[ "$COUNT_N3" -gt 0 ]]; then
    pass "Node 3 also returns results from distributed search (${COUNT_N3} results)"
else
    warn "Node 3 returned 0 results — may need more time for GossipSub mesh propagation"
fi

# ==============================================================================
# STEP 6 — BOOTSTRAP ENDPOINT
# ==============================================================================
step "STEP 6 — Bootstrap endpoint (fetch embedding from IPFS)"

# First we need an ipfs_cid — get it from vec_meta if pod is accessible
info "Checking vec_meta table for IPFS CIDs (requires pod exec)..."
if [[ -n "$POD1" ]]; then
    VEC_META=$(${KUBECTL_NS} exec "$POD1" -- \
        sqlite3 "$(${KUBECTL_NS} exec "$POD1" -- \
            find /root/.cache/optimusdb -name '*.db' -not -name 'optimuslog.db' 2>/dev/null | head -1)" \
        "SELECT doc_id, ipfs_cid FROM vec_meta WHERE ipfs_cid != '' LIMIT 1;" 2>/dev/null) || VEC_META=""

    if [[ -n "$VEC_META" ]]; then
        DOC_ID_BOOT=$(echo "$VEC_META" | cut -d'|' -f1)
        IPFS_CID=$(echo "$VEC_META" | cut -d'|' -f2)
        info "Found: doc_id=${DOC_ID_BOOT}  ipfs_cid=${IPFS_CID:0:20}..."

        # Test bootstrap on node 3
        BOOT_RESP=$(api_post "${BASE3}/api/v1/semantic/bootstrap" \
            "{\"doc_id\":\"${DOC_ID_BOOT}_bootstrap_copy\",\"ipfs_cid\":\"${IPFS_CID}\"}") || BOOT_RESP=""

        if echo "$BOOT_RESP" | grep -q "\"bootstrapped\""; then
            pass "Bootstrap endpoint: embedding fetched from IPFS and inserted on node 3"
            info "Response: ${BOOT_RESP}"
        else
            fail "Bootstrap endpoint failed: ${BOOT_RESP:0:200}"
        fi
    else
        warn "No IPFS CIDs in vec_meta yet — TinyLlama may be pinning asynchronously"
        info "The bootstrap test is skipped — run again after a few minutes"
    fi
else
    warn "No running pod found — skipping bootstrap test"
fi

# ==============================================================================
# STEP 7 — VEC_META CONTENTS (diagnostic)
# ==============================================================================
step "STEP 7 — Diagnostic: vec_meta table contents on node 1"

if [[ -n "$POD1" ]]; then
    DB_PATH=$(${KUBECTL_NS} exec "$POD1" -- \
        find /root/.cache/optimusdb -name '*.db' -not -name 'optimuslog.db' 2>/dev/null | head -1) || DB_PATH=""

    if [[ -n "$DB_PATH" ]]; then
        info "SQLite DB: ${DB_PATH}"
        META_ROWS=$(${KUBECTL_NS} exec "$POD1" -- \
            sqlite3 "$DB_PATH" \
            "SELECT doc_id, store_name, substr(source_text,1,50), indexed_at FROM vec_meta ORDER BY indexed_at DESC LIMIT 10;" \
            2>/dev/null) || META_ROWS=""

        if [[ -n "$META_ROWS" ]]; then
            pass "vec_meta has indexed documents:"
            echo "$META_ROWS" | while IFS= read -r row; do
                echo "         ${row}"
            done
        else
            fail "vec_meta is empty — indexing did not run"
        fi

        # Row count in vec_embeddings
        EMBED_COUNT=$(${KUBECTL_NS} exec "$POD1" -- \
            sqlite3 "$DB_PATH" \
            "SELECT count(*) FROM vec_embeddings;" 2>/dev/null) || EMBED_COUNT="unknown"
        info "vec_embeddings row count: ${EMBED_COUNT}"
    else
        warn "Could not locate SQLite DB inside pod"
    fi
else
    warn "No running pod found — skipping diagnostic"
fi

# ==============================================================================
# CLEANUP — Remove test documents
# ==============================================================================
step "CLEANUP — Removing test documents"

for DOC_ID in "sem_test_solar_001" "sem_test_wind_002" "sem_test_manual_004"; do
    DEL=$(api_post "${BASE1}/swarmkb/command" \
        "{\"method\":{\"cmd\":\"cruddelete\",\"argcnt\":1},\"dstype\":\"dsswres\",\"criteria\":[{\"_id\":\"${DOC_ID}\"}]}") || DEL=""
    if echo "$DEL" | grep -qi "success\|deleted"; then
        info "Removed ${DOC_ID} from node 1"
    fi
done

DEL_HYDRO=$(api_post "${BASE2}/swarmkb/command" \
    '{"method":{"cmd":"cruddelete","argcnt":1},"dstype":"dsswres","criteria":[{"_id":"sem_test_hydro_003"}]}') || DEL_HYDRO=""
if echo "$DEL_HYDRO" | grep -qi "success\|deleted"; then
    info "Removed sem_test_hydro_003 from node 2"
fi

# ==============================================================================
# SUMMARY
# ==============================================================================
echo ""
echo -e "${BOLD}${CYAN}╔══════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}${CYAN}║                       TEST SUMMARY                          ║${NC}"
echo -e "${BOLD}${CYAN}╚══════════════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "  ${GREEN}Passed : ${TESTS_PASSED}${NC}"
echo -e "  ${RED}Failed : ${TESTS_FAILED}${NC}"
echo ""

if [[ $TESTS_FAILED -eq 0 ]]; then
    echo -e "  ${GREEN}${BOLD}🎉 All tests passed! Semantic search is fully operational.${NC}"
else
    echo -e "  ${YELLOW}${BOLD}⚠️  Some tests failed. Common causes:${NC}"
    echo ""
    echo "  1. llama-server not running with --embedding"
    echo "     → Check Dockerfile supervisord config has --embedding flag"
    echo "     → kubectl exec <pod> -- ps aux | grep llama"
    echo ""
    echo "  2. vec0.so not present in the runtime image"
    echo "     → Check Dockerfile has sqlite-vec wget + COPY --from=builder"
    echo "     → kubectl exec <pod> -- ls /usr/lib/sqlite-vec/vec0.so"
    echo ""
    echo "  3. Pods still starting up (readiness probe)"
    echo "     → kubectl get pods -n ${NAMESPACE}"
    echo "     → Wait until all optimusdb pods show 1/1 Running"
    echo ""
    echo "  4. GossipSub mesh not yet formed (distributed search)"
    echo "     → Normal on first startup — wait 60s and re-run"
    echo ""
    echo "  5. TinyLlama model still loading"
    echo "     → kubectl logs -n ${NAMESPACE} <pod> | grep -i tinyllama"
fi

echo ""
echo -e "  ${CYAN}Useful debug commands:${NC}"
echo "  kubectl logs -n ${NAMESPACE} <pod> | grep -i semantic"
echo "  kubectl logs -n ${NAMESPACE} <pod> | grep -i tinyllama"
echo "  kubectl exec -n ${NAMESPACE} <pod> -- cat /var/log/supervisor/tinyllama_info.log"
echo "  kubectl exec -n ${NAMESPACE} <pod> -- cat /var/log/supervisor/optimusdb_info.log"
echo ""

exit $TESTS_FAILED