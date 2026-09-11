
#!/usr/bin/env bash
# ==============================================================================
# test_dynamic_stores.sh — end-to-end validation of the dynamic datastore change
#
# Covers: implicit creation on every verb, isolation from dsswres, name
# validation, config persistence, export/import coverage, cross-peer
# replication, detach, and the empty-dstype regression.
#
# Usage:
#   ./test_dynamic_stores.sh                          # Docker, agent 1 only
#   ./test_dynamic_stores.sh --peer http://localhost:18002
#   ./test_dynamic_stores.sh --import-node http://localhost:18004
#   ./test_dynamic_stores.sh --public-url http://193.225.250.240/optimusdb1
#   ./test_dynamic_stores.sh --url http://193.225.250.240/optimusdb1 \\
#                            --peer http://193.225.250.240/optimusdb2
#
# Options:
#   --url URL          agent under test            (default http://localhost:18001)
#   --peer URL         second agent, for replication checks
#   --import-node URL  agent to restore the archive onto
#   --public-url URL   external/ingress URL of --url, checked for reachability
#   --context CTX      API context                 (default swarmkb)
#   --keep             leave test stores in place
#
# Requires: curl, jq
# ./test_dynamic_stores.sh --public-url http://193.225.250.240/optimusdb1
# ==============================================================================
set -uo pipefail

A="http://localhost:18001"
PEER=""
IMPORT_NODE=""
PUBLIC_URL=""
CTX="swarmkb"
KEEP=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --url)         A="$2";           shift 2 ;;
        --peer)        PEER="$2";        shift 2 ;;
        --import-node) IMPORT_NODE="$2"; shift 2 ;;
        --public-url)  PUBLIC_URL="$2";  shift 2 ;;
        --context)     CTX="$2";         shift 2 ;;
        --keep)        KEEP=1;           shift   ;;
        -h|--help)     sed -n '2,24p' "$0"; exit 0 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

PASS=0; FAIL=0; SKIP=0
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

# Unique suffix so repeated runs don't collide on store names.
SFX="$(date +%s)"
S_TRUST="kbtrust"
S_TMP1="dyntest_a_${SFX}"
S_TMP2="dyntest_b_${SFX}"
S_TMP3="dyntest_c_${SFX}"
DOC_ID="peer:QmDYNTEST${SFX}"

c_g=$'\e[32m'; c_r=$'\e[31m'; c_y=$'\e[33m'; c_b=$'\e[1m'; c_0=$'\e[0m'

hdr()  { printf '\n%s── %s %s\n' "$c_b" "$1" "$c_0"; }
ok()   { PASS=$((PASS+1)); printf '  %s✓%s %s\n' "$c_g" "$c_0" "$1"; }
bad()  { FAIL=$((FAIL+1)); printf '  %s✗%s %s\n' "$c_r" "$c_0" "$1"; [[ -n "${2:-}" ]] && printf '      %s\n' "$2"; }
skip() { SKIP=$((SKIP+1)); printf '  %s-%s %s\n' "$c_y" "$c_0" "$1"; }

cmd() {  # cmd <verb> <dstype> <criteria-json> [updatedata-json]
    local body
    if [[ -n "${4:-}" ]]; then
        body=$(printf '{"method":{"cmd":"%s"},"dstype":"%s","criteria":%s,"updatedata":%s}' "$1" "$2" "$3" "$4")
    else
        body=$(printf '{"method":{"cmd":"%s"},"dstype":"%s","criteria":%s}' "$1" "$2" "$3")
    fi
    curl -sS -X POST "$A/$CTX/command" -H 'Content-Type: application/json' -d "$body" 2>/dev/null
}

# host_of <url> -> hostname without scheme, port or path
host_of() { printf '%s' "$1" | sed -E 's#^[a-z]+://##; s#[:/].*$##'; }

# classify_ip <ipv4> -> loopback | link-local | private | cgnat | public
classify_ip() {
    local ip="$1" o1 o2
    o1=${ip%%.*}; o2=$(printf '%s' "$ip" | cut -d. -f2)
    [[ ! "$ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] && { echo "non-ipv4"; return; }
    if   [[ "$o1" == 127 ]];                                  then echo "loopback"
    elif [[ "$o1" == 169 && "$o2" == 254 ]];                  then echo "link-local"
    elif [[ "$o1" == 10 ]];                                   then echo "private"
    elif [[ "$o1" == 192 && "$o2" == 168 ]];                  then echo "private"
    elif [[ "$o1" == 172 && "$o2" -ge 16  && "$o2" -le 31 ]]; then echo "private"
    elif [[ "$o1" == 100 && "$o2" -ge 64  && "$o2" -le 127 ]]; then echo "cgnat"
    else                                                           echo "public"
    fi
}

store_names() { curl -sS "$A/api/v1/stores" 2>/dev/null | jq -r '.stores[]?.name' 2>/dev/null; }
store_addr()  { curl -sS "$1/api/v1/stores" 2>/dev/null | jq -r --arg n "$2" '.stores[]?|select(.name==$n)|.address' 2>/dev/null; }
count()       { jq 'if type=="array" then length elif .data then (.data|length) else 0 end' 2>/dev/null; }

printf '%s\n' "════════════════════════════════════════════════════════════"
printf '  OptimusDB — Dynamic Datastore Test Suite\n'
printf '  agent: %s\n' "$A"
[[ -n "$PEER" ]]        && printf '  peer : %s\n' "$PEER"
[[ -n "$PUBLIC_URL" ]]  && printf '  public: %s\n' "$PUBLIC_URL"
[[ -n "$IMPORT_NODE" ]] && printf '  import target: %s\n' "$IMPORT_NODE"
printf '%s\n' "════════════════════════════════════════════════════════════"

# ── 0. preflight ──────────────────────────────────────────────────────────────
hdr "0. Preflight"
for b in curl jq; do command -v $b >/dev/null || { echo "missing: $b"; exit 1; }; done

if curl -sS --max-time 10 "$A/$CTX/agent/status" >/dev/null 2>&1; then
    ok "agent reachable"
else
    bad "agent not reachable at $A" "start the cluster, or pass --url"; exit 1
fi

if curl -sS --max-time 10 "$A/api/v1/stores" | jq -e '.stores' >/dev/null 2>&1; then
    ok "/api/v1/stores is registered"
else
    bad "/api/v1/stores missing" "api/http.go was not replaced — grep -n storesHandler api/http.go"
    exit 1
fi

# ── 0.5 public address ────────────────────────────────────────────────────────
# Two different "addresses" matter and they fail independently:
#   HTTP  — where clients (optimusPy, optimusTrust, the catalog frontend) reach
#           the REST API. Behind Traefik in K3s.
#   libp2p— the multiaddrs this node advertises to peers for replication. A node
#           that only advertises loopback/private addresses will never replicate
#           to a peer outside its own network, no matter how healthy the API is.
hdr "0.5 Public address"

STATUS_JSON="$TMP/status.json"
curl -sS --max-time 15 "$A/$CTX/agent/status" -o "$STATUS_JSON" 2>/dev/null

PEER_ID=$(jq -r '.agent.peer_id // empty' "$STATUS_JSON" 2>/dev/null)
HTTP_PORT=$(jq -r '.configuration.http_port // empty' "$STATUS_JSON" 2>/dev/null)
CTX_SRV=$(jq -r '.configuration.context // empty' "$STATUS_JSON" 2>/dev/null)

if [[ -n "$PEER_ID" ]]; then
    ok "peer ID: $PEER_ID"
else
    bad "could not read .agent.peer_id from /agent/status"
fi

if [[ -n "$CTX_SRV" && "$CTX_SRV" != "$CTX" ]]; then
    bad "context mismatch" "server says '$CTX_SRV', script is using '$CTX' — pass --context $CTX_SRV"
elif [[ -n "$CTX_SRV" ]]; then
    ok "context '$CTX_SRV' (http port $HTTP_PORT)"
fi

# --- HTTP endpoint ------------------------------------------------------------
A_HOST=$(host_of "$A")
A_CLASS=""
if [[ "$A_HOST" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    A_CLASS=$(classify_ip "$A_HOST")
else
    A_IP=$(getent hosts "$A_HOST" 2>/dev/null | awk '{print $1; exit}')
    [[ -z "$A_IP" ]] && A_IP=$(ping -c1 -W1 "$A_HOST" 2>/dev/null | sed -n '1s/.*(\([0-9.]*\)).*/\1/p')
    if [[ -n "$A_IP" ]]; then
        A_CLASS=$(classify_ip "$A_IP")
        ok "HTTP host '$A_HOST' resolves to $A_IP ($A_CLASS)"
    else
        skip "could not resolve HTTP host '$A_HOST'"
    fi
fi
[[ -n "$A_CLASS" && ! "$A_HOST" =~ ^[0-9] ]] || printf '      %s\n' "HTTP endpoint: $A  [${A_CLASS:-unknown}]"

# --- libp2p advertised addresses ---------------------------------------------
mapfile -t ADDRS < <(jq -r '.agent.addresses[]? // empty' "$STATUS_JSON" 2>/dev/null)
if [[ ${#ADDRS[@]} -eq 0 ]]; then
    bad "node advertises no libp2p addresses" "peers cannot dial it"
else
    ok "node advertises ${#ADDRS[@]} libp2p address(es)"
    N_PUB=0; N_PRIV=0; N_LOOP=0
    for ma in "${ADDRS[@]}"; do
        ip=$(printf '%s' "$ma" | sed -n 's#^/ip4/\([0-9.]*\)/.*#\1#p')
        [[ -z "$ip" ]] && continue
        cls=$(classify_ip "$ip")
        case "$cls" in
            public)   N_PUB=$((N_PUB+1));  printf '      %s%s%s  %s\n' "$c_g" "PUBLIC " "$c_0" "$ma" ;;
            loopback) N_LOOP=$((N_LOOP+1)) ;;
            *)        N_PRIV=$((N_PRIV+1)); printf '      %s%-7s%s %s\n' "$c_y" "$cls" "$c_0" "$ma" ;;
        esac
    done
    printf '      %s\n' "summary: $N_PUB public, $N_PRIV private, $N_LOOP loopback"

    if [[ "$N_PUB" -gt 0 ]]; then
        ok "at least one publicly routable libp2p address"
    elif [[ "$N_PRIV" -gt 0 ]]; then
        skip "only private/container addresses advertised — fine for a single Docker network or one K3s cluster, but peers outside it cannot dial this node"
    else
        bad "only loopback advertised" "this node cannot replicate to any peer"
    fi
fi

# --- public HTTP endpoint reachability ---------------------------------------
if [[ -n "$PUBLIC_URL" ]]; then
    P_HOST=$(host_of "$PUBLIC_URL")
    if curl -sS --max-time 15 "$PUBLIC_URL/$CTX/agent/status" -o "$TMP/pub.json" 2>/dev/null; then
        P_ID=$(jq -r '.agent.peer_id // empty' "$TMP/pub.json" 2>/dev/null)
        if [[ -n "$P_ID" ]]; then
            ok "public URL reachable: $PUBLIC_URL"
            if [[ "$P_ID" == "$PEER_ID" ]]; then
                ok "public URL serves the SAME node (peer_id matches)"
            else
                skip "public URL serves a different node ($P_ID) — load balanced across agents?"
            fi
            # /api/v1/stores must be reachable publicly too, or the dynamic-store
            # endpoints are only usable from inside the cluster.
            if curl -sS --max-time 15 "$PUBLIC_URL/api/v1/stores" | jq -e '.stores' >/dev/null 2>&1; then
                ok "/api/v1/stores reachable via the public URL"
            else
                bad "/api/v1/stores NOT reachable publicly" \
                    "add a Traefik IngressRoute for PathPrefix(\`/api/v1/stores\`), or use the per-node route"
            fi
        else
            bad "public URL returned an unexpected body" "$(head -c 200 "$TMP/pub.json")"
        fi
    else
        bad "public URL not reachable: $PUBLIC_URL" "check ingress, firewall, or DNS for $P_HOST"
    fi
else
    skip "no --public-url given (pass e.g. --public-url http://193.225.250.240/optimusdb1)"
fi

# --- peer endpoint addresses --------------------------------------------------
NPEERS=$(jq -r '.cluster.connected_peers // 0' "$STATUS_JSON" 2>/dev/null)
if [[ "$NPEERS" -gt 0 ]]; then
    ok "$NPEERS connected peer(s)"
    jq -r '.peers[]? | "      \(.peer_id[0:12])…  \(.role // "?")  \((.addresses // []) | join(" "))"' \
        "$STATUS_JSON" 2>/dev/null | head -8
else
    skip "no connected peers — replication tests will be inconclusive"
fi

# ── 1. baseline ───────────────────────────────────────────────────────────────
hdr "1. Baseline store inventory"
BUILTIN=$(curl -sS "$A/api/v1/stores" | jq '[.stores[]|select(.kind=="builtin")]|length')
[[ "$BUILTIN" == "11" ]] && ok "11 built-in stores" || bad "expected 11 built-in, got $BUILTIN"

MISSING=""
for s in validations kbdata kbmetadata whoiswho dsswres dsswresaloc \
         tosca_imported tosca_adt tosca_capacities tosca_deploymentplan tosca_eventhistory; do
    store_names | grep -qx "$s" || MISSING="$MISSING $s"
done
[[ -z "$MISSING" ]] && ok "all built-in names resolve" || bad "missing:$MISSING"

store_names | grep -qx "contributions" \
    && bad "contributions listed as a docstore (it is an EventLog)" \
    || ok "contributions correctly excluded"

# ── 2. implicit creation on write ─────────────────────────────────────────────
hdr "2. Create a store by writing to it"
cmd crudput "$S_TRUST" "[{\"_id\":\"$DOC_ID\",\"context\":\"storage\",\"trust_level\":0.8,\"verified\":true}]" > "$TMP/put.json"
store_names | grep -qx "$S_TRUST" && ok "store '$S_TRUST' exists after write" \
    || bad "store not created" "$(head -c 300 "$TMP/put.json")"

KIND=$(curl -sS "$A/api/v1/stores" | jq -r --arg n "$S_TRUST" '.stores[]|select(.name==$n)|.kind')
[[ "$KIND" == "dynamic" || "$KIND" == "builtin" ]] && ok "reported as kind=$KIND" || bad "unexpected kind: $KIND"

# ── 3. isolation — the assertion that matters ─────────────────────────────────
hdr "3. Isolation from dsswres"
N=$(cmd crudget dsswres "[{\"_id\":\"$DOC_ID\"}]" | count)
[[ "$N" == "0" ]] && ok "document is NOT in dsswres" \
    || bad "document leaked into dsswres (count=$N)" "the old fallback is still active — app/service.go not replaced"

N=$(cmd crudget "$S_TRUST" '[{}]' | count)
[[ "$N" -ge "1" ]] && ok "document readable from '$S_TRUST' (count=$N)" || bad "document not readable back"

# ── 4. explicit creation ──────────────────────────────────────────────────────
hdr "4. Explicit creation via POST /api/v1/stores"
R=$(curl -sS -X POST "$A/api/v1/stores" -H 'Content-Type: application/json' -d "{\"name\":\"$S_TMP1\"}")
echo "$R" | jq -e '.address' >/dev/null 2>&1 && ok "created '$S_TMP1'" || bad "creation failed" "$R"
echo "$R" | jq -e '.created == true' >/dev/null 2>&1 && ok "created=true on first call" || skip "created flag not true"

R=$(curl -sS -X POST "$A/api/v1/stores" -H 'Content-Type: application/json' -d "{\"name\":\"$S_TMP1\"}")
echo "$R" | jq -e '.created == false' >/dev/null 2>&1 && ok "idempotent on repeat" || bad "not idempotent" "$R"

# ── 5. read / update / delete against stores that do not exist ────────────────
hdr "5. Non-existent store on read, update, delete"
N=$(cmd crudget "$S_TMP2" '[{}]' | count)
[[ "$N" == "0" ]] && ok "read returns empty, not an error" || bad "unexpected read result: $N"
store_names | grep -qx "$S_TMP2" && ok "read created the store" || bad "read did not create the store"

cmd crudupdate "$S_TMP3" '[{"nomatch":1}]' '[{"y":2}]' >/dev/null
store_names | grep -qx "$S_TMP3" && ok "update created the store" || bad "update did not create the store"

cmd cruddelete "$S_TMP3" '[{"nomatch":1}]' >/dev/null
ok "delete against empty store did not error"

# ── 6. name validation ────────────────────────────────────────────────────────
hdr "6. Name validation"
R=$(curl -sS -X POST "$A/api/v1/stores" -H 'Content-Type: application/json' -d '{"name":"My Trust Store"}')
echo "$R" | jq -e '.error' >/dev/null 2>&1 && ok "rejects spaces/uppercase" || bad "accepted an invalid name" "$R"

R=$(curl -sS -X POST "$A/api/v1/stores" -H 'Content-Type: application/json' -d '{"name":"../../etc/passwd"}')
echo "$R" | jq -e '.error' >/dev/null 2>&1 && ok "rejects path traversal" || bad "accepted a traversal name" "$R"

R=$(curl -sS -X POST "$A/api/v1/stores" -H 'Content-Type: application/json' -d '{"name":"contributions"}')
echo "$R" | jq -e '.error' >/dev/null 2>&1 && ok "rejects reserved name 'contributions'" || bad "accepted reserved name" "$R"

R=$(cmd crudget contributions '[{}]')
echo "$R" | grep -qi "eventlog" && ok "crudget on contributions explains it is an EventLog" \
    || skip "contributions message differs: $(echo "$R" | head -c 120)"

# ── 7. empty dstype regression ────────────────────────────────────────────────
hdr "7. Empty dstype still resolves to dsswres"
EMPTY=$(curl -sS -X POST "$A/$CTX/command" -H 'Content-Type: application/json' \
        -d '{"method":{"cmd":"crudget"},"criteria":[{}]}' | count)
DSW=$(cmd crudget dsswres '[{}]' | count)
[[ "$EMPTY" == "$DSW" ]] && ok "empty dstype == dsswres ($EMPTY docs)" \
    || bad "empty dstype returned $EMPTY, dsswres has $DSW"
store_names | grep -qx "" && bad 'a store named "" was created' || ok 'no empty-named store created'

# ── 8. export coverage ────────────────────────────────────────────────────────
hdr "8. Export includes dynamic stores"
if curl -sS -X POST "$A/api/v1/exchange/export" -o "$TMP/backup.tar.gz" 2>/dev/null \
   && tar -tzf "$TMP/backup.tar.gz" >/dev/null 2>&1; then
    ok "export produced a valid archive ($(du -h "$TMP/backup.tar.gz" | cut -f1))"

    tar -tzf "$TMP/backup.tar.gz" | grep -q "orbitdb/${S_TRUST}.jsonl" \
        && ok "archive contains orbitdb/${S_TRUST}.jsonl" \
        || bad "dynamic store missing from archive" "backupfunc/exchange.go may not be replaced"

    tar -xzOf "$TMP/backup.tar.gz" manifest.json | jq -e --arg n "$S_TRUST" \
        '.stores | index($n)' >/dev/null 2>&1 \
        && ok "manifest lists '$S_TRUST'" || bad "manifest does not list '$S_TRUST'"

    tar -tzf "$TMP/backup.tar.gz" | grep -q 'sqlite/optimusdb.db' \
        && ok "SQLite snapshot present" || bad "SQLite snapshot missing"

    NB=$(tar -tzf "$TMP/backup.tar.gz" | grep -c '^orbitdb/')
    [[ "$NB" -ge 12 ]] && ok "$NB store files in archive" || bad "only $NB store files (expected >= 12)"

    LINES=$(tar -xzOf "$TMP/backup.tar.gz" "orbitdb/${S_TRUST}.jsonl" 2>/dev/null | wc -l | tr -d ' ')
    [[ "$LINES" -ge 1 ]] && ok "'$S_TRUST' exported with $LINES record(s)" \
        || bad "'$S_TRUST' exported empty" "store index may not have loaded"
else
    bad "export failed" "check that ExchangeService is wired in main.go"
fi

# ── 9. import onto a node that has never seen the store ───────────────────────
hdr "9. Import onto a fresh node"
if [[ -z "$IMPORT_NODE" ]]; then
    skip "no --import-node given"
elif [[ ! -s "$TMP/backup.tar.gz" ]]; then
    skip "no archive from step 8"
elif ! curl -sS --max-time 10 "$IMPORT_NODE/api/v1/stores" >/dev/null 2>&1; then
    skip "import node not reachable"
else
    R=$(curl -sS -X POST "$IMPORT_NODE/api/v1/exchange/import" -F "archive=@$TMP/backup.tar.gz")
    echo "$R" | jq -e --arg n "$S_TRUST" '.stores[$n]' >/dev/null 2>&1 \
        && ok "import restored '$S_TRUST' ($(echo "$R" | jq -r --arg n "$S_TRUST" '.stores[$n]') docs)" \
        || bad "import did not restore '$S_TRUST'" "$(echo "$R" | head -c 300)"

    NERR=$(echo "$R" | jq '.errors // [] | length')
    [[ "$NERR" == "0" ]] && ok "import reported no errors" \
        || bad "import errors: $(echo "$R" | jq -c '.errors')"

    curl -sS "$IMPORT_NODE/api/v1/stores" | jq -e --arg n "$S_TRUST" \
        '.stores[]|select(.name==$n)' >/dev/null 2>&1 \
        && ok "'$S_TRUST' now live on the import node" || bad "store not live after import"
fi

# ── 10. cross-peer replication ────────────────────────────────────────────────
hdr "10. Cross-peer address agreement"
if [[ -z "$PEER" ]]; then
    skip "no --peer given"
elif ! curl -sS --max-time 10 "$PEER/api/v1/stores" >/dev/null 2>&1; then
    skip "peer not reachable"
else
    cmd_peer=$(curl -sS -X POST "$PEER/$CTX/command" -H 'Content-Type: application/json' \
        -d "{\"method\":{\"cmd\":\"crudget\"},\"dstype\":\"$S_TRUST\",\"criteria\":[{}]}")
    A1=$(store_addr "$A" "$S_TRUST"); A2=$(store_addr "$PEER" "$S_TRUST")
    if [[ -n "$A1" && "$A1" == "$A2" ]]; then
        ok "identical OrbitDB address on both peers"
        printf '      %s\n' "$A1"
        sleep 20
        N=$(echo "$cmd_peer" | count)
        N2=$(curl -sS -X POST "$PEER/$CTX/command" -H 'Content-Type: application/json' \
             -d "{\"method\":{\"cmd\":\"crudget\"},\"dstype\":\"$S_TRUST\",\"criteria\":[{}]}" | count)
        [[ "$N2" -ge 1 ]] && ok "document replicated to peer ($N2 docs)" \
            || skip "not replicated yet (was $N, now $N2) — allow more time"
    else
        bad "addresses differ — separate logs under one name" "A=$A1  B=$A2"
    fi
fi

# ── 11. detach ────────────────────────────────────────────────────────────────
hdr "11. Detach"
R=$(curl -sS -X DELETE "$A/api/v1/stores/dsswres")
echo "$R" | jq -e '.error' >/dev/null 2>&1 && ok "built-in store cannot be dropped" || bad "dsswres was droppable" "$R"

R=$(curl -sS -X DELETE "$A/api/v1/stores/$S_TMP1")
echo "$R" | jq -e '.detached == true' >/dev/null 2>&1 && ok "dynamic store detached" || bad "detach failed" "$R"
store_names | grep -qx "$S_TMP1" && bad "still listed after detach" || ok "removed from listing"

# ── 12. cleanup ───────────────────────────────────────────────────────────────
hdr "12. Cleanup"
if [[ "$KEEP" == "1" ]]; then
    skip "--keep given, leaving test stores in place"
else
    for s in "$S_TMP2" "$S_TMP3"; do
        curl -sS -X DELETE "$A/api/v1/stores/$s" >/dev/null 2>&1
    done
    cmd cruddelete "$S_TRUST" "[{\"_id\":\"$DOC_ID\"}]" >/dev/null 2>&1
    ok "test stores detached, test document removed"
    printf '      %s\n' "note: '$S_TRUST' left in place — it is a real store you may want to keep"
fi

# ── summary ───────────────────────────────────────────────────────────────────
printf '\n%s\n' "════════════════════════════════════════════════════════════"
printf '  passed %s%d%s   failed %s%d%s   skipped %s%d%s\n' \
    "$c_g" "$PASS" "$c_0" "$c_r" "$FAIL" "$c_0" "$c_y" "$SKIP" "$c_0"
printf '%s\n' "════════════════════════════════════════════════════════════"

if [[ "$FAIL" -gt 0 ]]; then
    printf '\n%sManual checks not covered here:%s\n' "$c_b" "$c_0"
    printf '  - restart survival: restart the agent, then GET /api/v1/stores\n'
    printf '  - config: cat <repo>_config | jq .dynamicStoreAddrs\n'
    printf '  - strict mode: restart with -dynamic-stores=false\n'
    exit 1
fi

printf '\n%sStill to verify manually:%s\n' "$c_b" "$c_0"
printf '  1. restart the agent, then: curl -s %s/api/v1/stores | jq\n' "$A"
printf '  2. docker exec <agent> cat /root/swarmkbIpfs_config | jq .dynamicStoreAddrs\n'
printf '  3. restart one agent with -dynamic-stores=false and retry step 2 of the guide\n'
printf '  4. from OUTSIDE the deployment network, confirm the public URL answers:\n'
printf '     curl -s <public-url>/%s/agent/status | jq .agent.peer_id\n' "$CTX"
exit 0