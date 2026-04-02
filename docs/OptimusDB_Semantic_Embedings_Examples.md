# OptimusDB — Semantic Search & Embedding Endpoints
## Testing Guide

**Cluster:** `http://193.225.250.240`
**Nodes:** optimusdb1 · optimusdb2 · optimusdb3
**Namespace:** `optimusddc`

---

## What was completed

The OptimusDB cluster is fully operational with the following capabilities:

| Capability | Status |
|---|---|
| 3-node distributed P2P cluster (LibP2P + GossipSub) | ✅ Running |
| TinyLlama-1.1B embedding model (2048 dimensions) | ✅ Running on all nodes |
| SQLite-vec vector index (semantic search) | ✅ Ready |
| Decentralized semantic search via GossipSub fan-out | ✅ Active |
| External embedding endpoint via Traefik (per node) | ✅ Live |
| IP allowlist protection on embedding endpoints | ✅ Active |
| Automated deploy script with model streaming | ✅ Active |

---

## 1. Health Check — Cluster Status

Verify that all three nodes are responding:

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
echo "=== $node ==="
curl -s http://193.225.250.240/${node}/swarmkb/agent/status | python3 -m json.tool
done
```

**Expected result:** HTTP 200 with a status JSON from each node.

---

## 2. Decentralized Embedding Endpoints

Each OptimusDB node runs its own TinyLlama-1.1B instance. The embedding endpoint is
exposed per node through Traefik with IP allowlist protection. All three nodes run the
same model and produce identical vectors for identical inputs — this is a prerequisite
for cross-node vector similarity comparisons to be meaningful.

### How it works

```
Client → Traefik → /optimusdbN/embedding  (priority 8, IP-restricted)
→ strips /optimusdbN prefix
→ optimusdbN-llama ClusterIP Service
→ llama-server :8080 inside the pod
→ returns 2048-dimensional float vector
```

The embedding computation is fully local to each node — no central model server,
no single point of failure. If node 1 is unavailable, nodes 2 and 3 continue serving
embeddings independently.

### Basic example — single node

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/embedding \
-H "Content-Type: application/json" \
-d '{"content":"solar farm photovoltaic Greece renewable energy"}'
```

**Expected result:**
```json
{"embedding": [0.0163, -0.0224, 0.0085, ...]}
```

### Verify embedding dimensions

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/embedding \
-H "Content-Type: application/json" \
-d '{"content":"hydroelectric power plant"}' | python3 -c \
"import json,sys; d=json.load(sys.stdin); print(f'Dimensions: {len(d[\"embedding\"])}')"
```

**Expected result:** `Dimensions: 2048`

### Verify all three nodes produce the same vector

This confirms model and embedding space consistency across the decentralized cluster:

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
VEC=$(curl -s -X POST http://193.225.250.240/${node}/embedding \
-H "Content-Type: application/json" \
-d '{"content":"wind turbine offshore energy"}' | python3 -c \
"import json,sys; d=json.load(sys.stdin); print(round(d['embedding'][0],6))" 2>/dev/null)
echo "$node → first dimension: $VEC"
done
```

**Expected result:** All three nodes return the same value for dimension[0]:

```
optimusdb1 → first dimension: 0.016397
optimusdb2 → first dimension: 0.016397
optimusdb3 → first dimension: 0.016397
```

### Node independence — embedding still available when a peer is down

Query node 2 directly — it serves embeddings regardless of node 1's state:

```bash
curl -s -X POST http://193.225.250.240/optimusdb2/embedding \
-H "Content-Type: application/json" \
-d '{"content":"battery storage grid balancing"}' | python3 -c \
"import json,sys; d=json.load(sys.stdin); print(f'Node 2 OK — {len(d[\"embedding\"])} dims')"
```

### IP Allowlist — verify access control

Run from an external machine to confirm the allowlist is active:

```bash
# Run from an external machine (not from the server)
curl -v -X POST http://193.225.250.240/optimusdb1/embedding \
-H "Content-Type: application/json" \
-d '{"content":"test"}'
# Expected: HTTP 403 Forbidden
```

---

## 3. Document Insert

Insert one document per node to set up the distributed search test.

### Node 1 — solar asset

```bash
curl -s -X PUT http://193.225.250.240/optimusdb1/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "energy_assets",
"records": [{
"id": "asset_solar_001",
"name": "Hellenic Solar Farm Alpha",
"description": "50MW photovoltaic installation in Thessaly Greece",
"type": "solar",
"status": "operational",
"country": "Greece",
"region": "Thessaly",
"capacity_mw": 50
}]
}'
```

**Expected result:**
```json
{"data": "OK: Successfully inserted records", "status": 200}
```

### Node 2 — hydroelectric asset

```bash
curl -s -X PUT http://193.225.250.240/optimusdb2/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "energy_assets",
"records": [{
"id": "asset_hydro_002",
"name": "Acheloos Hydroelectric Station",
"description": "Large hydroelectric dam on Acheloos river western Greece",
"type": "hydroelectric",
"status": "operational",
"country": "Greece",
"region": "Western Greece",
"capacity_mw": 120
}]
}'
```

### Node 3 — wind asset

```bash
curl -s -X PUT http://193.225.250.240/optimusdb3/swarmkb/crudput \
-H "Content-Type: application/json" \
-d '{
"store": "energy_assets",
"records": [{
"id": "asset_wind_003",
"name": "Aegean Wind Park Beta",
"description": "Offshore wind farm 200MW capacity north Aegean Sea",
"type": "wind",
"status": "operational",
"country": "Greece",
"region": "North Aegean",
"capacity_mw": 200
}]
}'
```

---

## 4. Manual Semantic Index

The manual index endpoint bypasses the background indexer and immediately embeds a
document into the local `vec_embeddings` SQLite-vec table. Use this when you need a
document to be searchable immediately after insert, without waiting for the asynchronous
background indexer. It calls the local TinyLlama instance, writes the vector to
SQLite-vec, and pins the blob to IPFS.

```bash
# Force immediate indexing of the solar document on node 1
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "energy_assets",
"doc_id": "asset_solar_001",
"fields": {
"name": "Hellenic Solar Farm Alpha",
"description": "50MW photovoltaic installation in Thessaly Greece",
"type": "solar",
"status": "operational",
"country": "Greece",
"region": "Thessaly"
}
}'
```

**Expected result:**
```json
{"status": "indexed", "doc_id": "asset_solar_001"}
```

Index the remaining documents on their respective nodes:

```bash
sleep 5

curl -s -X POST http://193.225.250.240/optimusdb2/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"energy_assets","doc_id":"asset_hydro_002","fields":{"name":"Acheloos Hydroelectric Station","description":"Large hydroelectric dam on Acheloos river western Greece","type":"hydroelectric","status":"operational","country":"Greece","region":"Western Greece"}}'

curl -s -X POST http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"energy_assets","doc_id":"asset_wind_003","fields":{"name":"Aegean Wind Park Beta","description":"Offshore wind farm 200MW capacity north Aegean Sea","type":"wind","status":"operational","country":"Greece","region":"North Aegean"}}'
```

---

## 5. Decentralized Semantic Search

This is the core distributed capability. When a query arrives at any node, it runs in
three steps in parallel:

1. **Local ANN** — queries its own `vec_embeddings` SQLite-vec index immediately
2. **GossipSub fan-out** — broadcasts the query vector to all peers on topic
`/optimusdb/semantic/search/1.0.0` with a deadline timestamp
3. **Result collection** — collects replies from peers on topic
`/optimusdb/semantic/results/1.0.0` within the `budget_ms` window, then merges,
deduplicates and re-ranks all results by cosine similarity score

No central search coordinator exists. Any node can initiate a cluster-wide search.

### How it works

```
Client → Node 1  GET /api/v1/semantic/search?q=...&budget_ms=2000
→ embed query locally via TinyLlama
→ run local ANN on vec_embeddings
→ GossipSub publish query vector to all peers
→ Node 2: receives, runs local ANN, publishes reply
→ Node 3: receives, runs local ANN, publishes reply
→ Node 1: collects peer replies within 2000ms deadline
→ merge + deduplicate + rank all results by score
→ return unified result set to client
```

### Local search — node 1 index only

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search?q=solar+farm+photovoltaic+Greece&top_k=5"
```

### Distributed search — node 1 queries the entire cluster

Node 1 fans out the query to nodes 2 and 3 and merges all results within 2 seconds:

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search?q=renewable+energy+operational+Greece&top_k=10&budget_ms=2000" \
| python3 -m json.tool
```

**Expected result:** Results from all three nodes merged and ranked by score:

```json
{
"query": "renewable energy operational Greece",
"count": 3,
"results": [
{"doc_id": "asset_solar_001", "score": 0.91, "store_name": "energy_assets", "source_node": "QmNode1PeerIdXXXXXXXXXXXX"},
{"doc_id": "asset_wind_003",  "score": 0.88, "store_name": "energy_assets", "source_node": "QmNode3PeerIdXXXXXXXXXXXX"},
{"doc_id": "asset_hydro_002", "score": 0.85, "store_name": "energy_assets", "source_node": "QmNode2PeerIdXXXXXXXXXXXX"}
]
}
```

The `source_node` field contains the LibP2P peer ID of the node that contributed each
result. When it differs from the node you queried, the result was retrieved from a
remote peer via GossipSub — not from local storage.

### Cross-node search — query from node 3, find document stored on node 2

This proves that any node can find documents stored exclusively on any other node:

```bash
curl -s "http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/search?q=hydroelectric+dam+river+western+Greece&top_k=5&budget_ms=2000" \
| python3 -m json.tool
```

**Expected result:** `asset_hydro_002` appears in results with `source_node` pointing
to node 2's peer ID, confirming cross-node retrieval via GossipSub.

### Symmetric search — verify any node can be the entry point

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
echo "=== Querying from $node ==="
curl -s "http://193.225.250.240/${node}/swarmkb/api/v1/semantic/search?q=offshore+wind+farm+Aegean&top_k=5&budget_ms=2000" \
| python3 -c "import json,sys; d=json.load(sys.stdin); print(f'count={d[\"count\"]} results')"
done
```

**Expected result:** All three nodes return the same count, confirming symmetric
distributed search regardless of which node receives the query.

### Budget window — observe latency vs completeness tradeoff

A tight budget returns only fast-responding peers:

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search?q=power+plant+Greece+operational&top_k=5&budget_ms=300" \
| python3 -c "import json,sys; d=json.load(sys.stdin); print(f'300ms budget: {d[\"count\"]} results')"
```

A generous budget gives all peers time to respond:

```bash
curl -s "http://193.225.250.240/optimusdb1/swarmkb/api/v1/semantic/search?q=power+plant+Greece+operational&top_k=5&budget_ms=3000" \
| python3 -c "import json,sys; d=json.load(sys.stdin); print(f'3000ms budget: {d[\"count\"]} results')"
```

Compare the counts to observe the GossipSub collection window in action.

---

## 6. Bootstrap Embedding from IPFS

When a node indexes a document it pins the raw embedding vector blob to IPFS and stores
the CID in `vec_meta`. A new or recovering node can fetch this blob directly from IPFS
without calling the embedding model — useful for cold-start without re-embedding.

First retrieve a CID from `vec_meta` on node 1:

```bash
kubectl exec -n optimusddc \
$(kubectl get pods -n optimusddc -l app=optimusdb,node=1 -o jsonpath='{.items[0].metadata.name}') -- \
sqlite3 /root/.cache/optimusdb/swarmkbIpfs/orbitdb/kbrdbms.db \
"SELECT doc_id, ipfs_cid FROM vec_meta LIMIT 5;"
```

Then bootstrap that embedding on node 3 using the IPFS CID:

```bash
curl -s -X POST http://193.225.250.240/optimusdb3/swarmkb/api/v1/semantic/bootstrap \
-H "Content-Type: application/json" \
-d '{
"doc_id": "asset_solar_001",
"ipfs_cid": "Qm... (from vec_meta query above)"
}'
```

**Expected result:**
```json
{"status": "bootstrapped", "doc_id": "asset_solar_001"}
```

---

## 7. Peers & Network Topology

### Show connected peers

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
echo "=== $node peers ==="
curl -s http://193.225.250.240/${node}/swarmkb/peers | python3 -m json.tool
done
```

### Asset inventory (load-balanced across all nodes)

```bash
# Traefik round-robins each call to a different node
curl -s http://193.225.250.240/swarmkb/agent/inventory | python3 -m json.tool
```

---

## 8. Cleanup

```bash
for node in optimusdb1 optimusdb2 optimusdb3; do
for id in asset_solar_001 asset_hydro_002 asset_wind_003; do
curl -s -X DELETE "http://193.225.250.240/${node}/swarmkb/crudput/${id}" > /dev/null
done
done
echo "Cleanup complete"
```

---

## Postman Collection

Import the following JSON into Postman (File → Import → Raw Text).
Change the `node` variable to switch the target between nodes:

```json
{
"info": {
"name": "OptimusDB — Decentralized Semantic Search & Embedding",
"schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
},
"variable": [
{"key": "host", "value": "193.225.250.240"},
{"key": "node", "value": "optimusdb1"}
],
"item": [
{
"name": "1. Health Check",
"request": {"method": "GET", "url": "http://{{host}}/{{node}}/swarmkb/agent/status"}
},
{
"name": "2a. Embedding — node 1",
"request": {
"method": "POST", "url": "http://{{host}}/optimusdb1/embedding",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"content\": \"solar farm photovoltaic Greece renewable energy\"}"}
}
},
{
"name": "2b. Embedding — node 2",
"request": {
"method": "POST", "url": "http://{{host}}/optimusdb2/embedding",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"content\": \"solar farm photovoltaic Greece renewable energy\"}"}
}
},
{
"name": "2c. Embedding — node 3",
"request": {
"method": "POST", "url": "http://{{host}}/optimusdb3/embedding",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"content\": \"solar farm photovoltaic Greece renewable energy\"}"}
}
},
{
"name": "3a. Insert — node 1 (solar)",
"request": {
"method": "PUT", "url": "http://{{host}}/optimusdb1/swarmkb/crudput",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"store\": \"energy_assets\", \"records\": [{\"id\": \"asset_solar_001\", \"name\": \"Hellenic Solar Farm Alpha\", \"description\": \"50MW photovoltaic installation in Thessaly Greece\", \"type\": \"solar\", \"status\": \"operational\", \"country\": \"Greece\", \"region\": \"Thessaly\"}]}"}
}
},
{
"name": "3b. Insert — node 2 (hydro)",
"request": {
"method": "PUT", "url": "http://{{host}}/optimusdb2/swarmkb/crudput",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"store\": \"energy_assets\", \"records\": [{\"id\": \"asset_hydro_002\", \"name\": \"Acheloos Hydroelectric Station\", \"description\": \"Large hydroelectric dam on Acheloos river western Greece\", \"type\": \"hydroelectric\", \"status\": \"operational\", \"country\": \"Greece\", \"region\": \"Western Greece\"}]}"}
}
},
{
"name": "3c. Insert — node 3 (wind)",
"request": {
"method": "PUT", "url": "http://{{host}}/optimusdb3/swarmkb/crudput",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"store\": \"energy_assets\", \"records\": [{\"id\": \"asset_wind_003\", \"name\": \"Aegean Wind Park Beta\", \"description\": \"Offshore wind farm 200MW capacity north Aegean Sea\", \"type\": \"wind\", \"status\": \"operational\", \"country\": \"Greece\", \"region\": \"North Aegean\"}]}"}
}
},
{
"name": "4. Manual Semantic Index",
"request": {
"method": "POST", "url": "http://{{host}}/{{node}}/swarmkb/api/v1/semantic/index",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"store\": \"energy_assets\", \"doc_id\": \"asset_solar_001\", \"fields\": {\"name\": \"Hellenic Solar Farm Alpha\", \"description\": \"50MW photovoltaic installation in Thessaly Greece\", \"type\": \"solar\", \"status\": \"operational\", \"country\": \"Greece\"}}"}
}
},
{
"name": "5a. Local Search — solar (node 1)",
"request": {
"method": "GET",
"url": {"raw": "http://{{host}}/optimusdb1/swarmkb/api/v1/semantic/search?q=solar+farm+photovoltaic+Greece&top_k=5",
"query": [{"key": "q", "value": "solar farm photovoltaic Greece"}, {"key": "top_k", "value": "5"}]}
}
},
{
"name": "5b. Distributed Search — all nodes via node 1",
"request": {
"method": "GET",
"url": {"raw": "http://{{host}}/optimusdb1/swarmkb/api/v1/semantic/search?q=renewable+energy+operational+Greece&top_k=10&budget_ms=2000",
"query": [{"key": "q", "value": "renewable energy operational Greece"}, {"key": "top_k", "value": "10"}, {"key": "budget_ms", "value": "2000"}]}
}
},
{
"name": "5c. Cross-node — hydro queried from node 3",
"request": {
"method": "GET",
"url": {"raw": "http://{{host}}/optimusdb3/swarmkb/api/v1/semantic/search?q=hydroelectric+dam+river+western+Greece&top_k=5&budget_ms=2000",
"query": [{"key": "q", "value": "hydroelectric dam river western Greece"}, {"key": "top_k", "value": "5"}, {"key": "budget_ms", "value": "2000"}]}
}
},
{
"name": "5d. Budget comparison — 300ms",
"request": {
"method": "GET",
"url": {"raw": "http://{{host}}/{{node}}/swarmkb/api/v1/semantic/search?q=power+plant+Greece+operational&top_k=5&budget_ms=300",
"query": [{"key": "q", "value": "power plant Greece operational"}, {"key": "top_k", "value": "5"}, {"key": "budget_ms", "value": "300"}]}
}
},
{
"name": "5e. Budget comparison — 3000ms",
"request": {
"method": "GET",
"url": {"raw": "http://{{host}}/{{node}}/swarmkb/api/v1/semantic/search?q=power+plant+Greece+operational&top_k=5&budget_ms=3000",
"query": [{"key": "q", "value": "power plant Greece operational"}, {"key": "top_k", "value": "5"}, {"key": "budget_ms", "value": "3000"}]}
}
},
{
"name": "6. Bootstrap Embedding from IPFS",
"request": {
"method": "POST", "url": "http://{{host}}/{{node}}/swarmkb/api/v1/semantic/bootstrap",
"header": [{"key": "Content-Type", "value": "application/json"}],
"body": {"mode": "raw", "raw": "{\"doc_id\": \"asset_solar_001\", \"ipfs_cid\": \"Qm... (from vec_meta query)\"}"}
}
},
{
"name": "7. List Peers",
"request": {"method": "GET", "url": "http://{{host}}/{{node}}/swarmkb/peers"}
},
{
"name": "8. Cleanup — Delete solar",
"request": {"method": "DELETE", "url": "http://{{host}}/optimusdb1/swarmkb/crudput/asset_solar_001"}
}
]
}
```

---

## Notes for evaluators

**Semantic search latency:** The first search after a restart may take 10–15 seconds due
to TinyLlama model loading. Subsequent searches are immediate.

**Cross-node propagation:** After inserting a document into a node, allow 5–10 seconds
for GossipSub mesh propagation before querying from a different node. This is normal P2P
behaviour — the mesh stabilises within the first minute after cluster startup.

**Decentralization proof:** The `source_node` field in search results contains the
LibP2P peer ID of the node that contributed each result. When this differs from the node
you queried, the result was retrieved from a remote peer via GossipSub — not from local
storage. This is the key observable evidence of decentralized search.

**Embedding consistency:** All three nodes run the same TinyLlama-1.1B-Chat Q4_K_M
model. Dimension[0] of the embedding vector for identical input text will be identical
across all nodes, confirming a shared embedding space — required for cross-node ANN
similarity to be valid.

**Embedding endpoint access:** The `/optimusdbN/embedding` endpoint is restricted to
cluster-internal IPs and the host server IP (`193.225.250.240`). External IPs receive
HTTP 403. To grant access to a research workstation, add its IP to the
`embedding-ipallowlist` middleware in the K3s manifest and `kubectl apply`.

**Model:** TinyLlama-1.1B-Chat Q4_K_M — 2048 dimensions, CPU-only inference (4 threads per node).
