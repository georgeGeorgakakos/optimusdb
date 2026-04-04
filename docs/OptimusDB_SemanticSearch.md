# Semantic Search for OptimusDB

Decentralized semantic search over all document stores, using the existing
`llama-server` for embeddings, `sqlite-vec` for local ANN, and GossipSub for peer
fan-out.

---

## How it works

```
Write path (non-blocking goroutine)
────────────────────────────────────────────────────────
crudPutDocStoreRev ──► go IndexDocument(store, docID, fields)
│
├─► POST /embedding        llama-server (--embedding flag required)
│         └─► float32[2048] vector
│
├─► INSERT OR REPLACE       sqlite-vec (vec_embeddings table)
│
└─► Unixfs().Add()          IPFS (via existing *Orbit API)
└─► CID stored in vec_meta

Search path
────────────────────────────────────────────────────────
GET /api/v1/semantic/search?q=...
│
├─► POST /embedding              embed query (same llama-server, no IPFS pin)
│
├─► sqlite-vec local ANN         top-K from this node
│
├─► GossipSub publish            /optimusdb/semantic/search/1.0.0
│       └─► each peer: local ANN ──► /optimusdb/semantic/results/1.0.0
│
└─► merge + deduplicate + rank   return top-K within budget_ms

Bootstrap path (peer replication, skip inference)
────────────────────────────────────────────────────────
POST /api/v1/semantic/bootstrap { doc_id, ipfs_cid }
└─► Unixfs().Get()               fetch raw float32 blob from IPFS
└─► sqlite-vec upsert            no llama-server call needed
```

---

## Architecture decisions

| Decision | Choice | Reason |
|---|---|---|
| Embedding source | Existing `llama-server` `/embedding` route | Already running via supervisord — zero new processes |
| Vector dimensions | 2048 | TinyLlama-1.1B-Chat produces 2048-dim embeddings (confirmed) |
| Vector storage (local) | `sqlite-vec` virtual table in `rdbms.DB` | Same `*sql.DB` as rest of OptimusDB — no extra file |
| Vector storage (remote) | IPFS blob via `(*Orbit).IPFS().Unixfs().Add()` | Already used in `service.go` — consistent API |
| Distributed search | GossipSub topic pair on `knowledgeBaseDB.PubSub` | Reuses the IPFS node's single pubsub — no double-instance conflict |
| Write path | Non-blocking goroutine | `crudPutDocStoreRev` returns immediately |
| Import boundary | `SemanticIdx interface{}` on `KnowledgeBaseDB` | Avoids `app → semantic → app` import cycle |

---

## End-to-end example

### 1. Insert three energy asset documents

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{
"_id": "solar_attica_001",
"name": "Athens Solar Farm",
"type": "solar",
"status": "operational",
"capacity_mw": 500,
"location": {"country": "Greece", "region": "Attica"},
"tags": ["renewable", "solar", "high-capacity"]
}]
}'
```

```bash
curl -s -X POST http://193.225.250.240/optimusdb2/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{
"_id": "wind_thrace_007",
"name": "Thrace Wind Park",
"type": "wind",
"status": "operational",
"capacity_mw": 800,
"location": {"country": "Greece", "region": "Thrace"},
"tags": ["renewable", "wind", "offshore-candidate"]
}]
}'
```

```bash
curl -s -X POST http://193.225.250.240/optimusdb3/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{
"_id": "solar_crete_012",
"name": "Crete Solar Installation",
"type": "solar",
"status": "maintenance",
"capacity_mw": 120,
"location": {"country": "Greece", "region": "Crete"},
"tags": ["solar", "island-grid"]
}]
}'
```

After each successful insert, `crudPutDocStoreRev` fires a goroutine:

```
"Athens Solar Farm solar operational Greece Attica renewable solar high-capacity"
│
├─► POST /embedding  →  float32[2048]
├─► sqlite-vec upsert
└─► IPFS pin  →  QmCid stored in vec_meta
```

The `crudput` HTTP response returns immediately. All three documents are indexed
concurrently in the background.

---

### 2. Index documents for semantic search

After insert, each document must be indexed. This can happen automatically via the
background goroutine in `crudPutDocStoreRev`, or manually via the index endpoint:

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "solar_attica_001",
"fields": {
"name": "Athens Solar Farm",
"type": "solar",
"status": "operational",
"capacity_mw": "500",
"location": "Greece Attica",
"tags": "renewable solar high-capacity"
}
}'
```

```bash
curl -s -X POST http://193.225.250.240/optimusdb2/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "wind_thrace_007",
"fields": {
"name": "Thrace Wind Park",
"type": "wind",
"status": "operational",
"capacity_mw": "800",
"location": "Greece Thrace",
"tags": "renewable wind offshore-candidate"
}
}'
```

```bash
curl -s -X POST http://193.225.250.240/optimusdb3/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store": "dsswres",
"doc_id": "solar_crete_012",
"fields": {
"name": "Crete Solar Installation",
"type": "solar",
"status": "maintenance",
"capacity_mw": "120",
"location": "Greece Crete",
"tags": "solar island-grid"
}
}'
```

---

### 3. Search

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=solar+farm+high+capacity+operational&top_k=5" \
| python3 -m json.tool
```

---

### 4. Response

Each result includes `doc_id`, `score`, `source_node`, and `store`. The `document`
field is populated when the result comes from the local node (OrbitDB lookup via
`DocumentStore.Get()`).

```json
{
"query": "solar farm high capacity operational",
"count": 2,
"results": [
{
"doc_id":      "solar_attica_001",
"score":       0.94,
"source_node": "QmNode1xYzAbCdEf...",
"store":       "dsswres",
"document": {
"name":        "Athens Solar Farm",
"type":        "solar",
"status":      "operational",
"capacity_mw": 500,
"location":    {"country": "Greece", "region": "Attica"},
"tags":        ["renewable", "solar", "high-capacity"]
}
},
{
"doc_id":      "wind_thrace_007",
"score":       0.81,
"source_node": "QmNode2xYzAbCdEf...",
"store":       "dsswres",
"document": {
"name":        "Thrace Wind Park",
"type":        "wind",
"status":      "operational",
"capacity_mw": 800,
"location":    {"country": "Greece", "region": "Thrace"},
"tags":        ["renewable", "wind", "offshore-candidate"]
}
}
]
}
```

`solar_crete_012` does not appear — its embedding
("Crete Solar Installation solar maintenance island-grid") is semantically distant
from "high capacity operational". The cosine similarity falls below both returned
results and it is trimmed from the top-K.

> **`document` field rules:**
> - Present when `source_node` matches the coordinator node (local OrbitDB lookup
>   via `DocumentStore.Get()`).
> - `null` for results from remote peers — document content is not serialised over
>   GossipSub to keep reply payloads small.
> - Internal fields `_id` and `_created_at` are stripped from the document payload.

---

### 5. Peer merging (multi-node cluster)

```
coordinator (node 1)
│
├─► local ANN  →  [solar_attica_001: 0.94, wind_thrace_007: 0.81]
│
└─► GossipSub broadcast  →  node 2, node 3
│
├─► node 2 local ANN  →  [wind_thrace_007: 0.79]   (replica)
└─► node 3 local ANN  →  []                         (no matching docs)

merge: deduplicate wind_thrace_007 (keep score 0.81 > 0.79), trim to top_k=5
final: [solar_attica_001: 0.94, wind_thrace_007: 0.81]
```

---

## REST API reference

### `GET /api/v1/semantic/search`

Search all indexed documents using natural language.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `q` | string | required | Natural language query |
| `top_k` | int | 10 | Max results to return |
| `budget_ms` | int | 1500 | Max time to wait for peer replies (ms) |

**Example:**

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search\
?q=solar+farm+high+capacity+operational&top_k=5"
```

---

### `POST /api/v1/semantic/index`

Manually (re-)index a document. Useful after schema changes or for documents
inserted before semantic search was enabled.

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{
"store":  "dsswres",
"doc_id": "solar_attica_001",
"fields": {
"name":        "Athens Solar Farm",
"type":        "solar",
"status":      "operational",
"capacity_mw": "500",
"location":    "Greece Attica"
}
}'
```

---

### `POST /api/v1/semantic/bootstrap`

Fetch a pre-computed embedding from IPFS instead of re-running `llama-server`
inference. Called automatically when a node replicates a document from a peer
and the CID is available in `vec_meta`.

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/bootstrap \
-H "Content-Type: application/json" \
-d '{"doc_id": "solar_attica_001", "ipfs_cid": "QmXyz..."}'
```

---

## New SQLite tables

Two tables are added to the existing `rdbms.DB` database on first startup:

```sql
-- ANN index (sqlite-vec virtual table)
-- Dimensions: 2048 (TinyLlama-1.1B-Chat)
CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings USING vec0(
doc_id    TEXT PRIMARY KEY,
embedding float[2048]
);

-- IPFS CID mapping for peer bootstrap
CREATE TABLE IF NOT EXISTS vec_meta (
doc_id      TEXT PRIMARY KEY,
ipfs_cid    TEXT,
store_name  TEXT,
indexed_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
source_text TEXT
);
```

---

## GossipSub topics

| Topic | Direction | Payload |
|---|---|---|
| `/optimusdb/semantic/search/1.0.0` | coordinator → all peers | `SearchQuery` — correlation ID, query vector, top_k, deadline |
| `/optimusdb/semantic/results/1.0.0` | each peer → coordinator | `SearchReply` — correlation ID, top-K results |

Both topics use `knowledgeBaseDB.PubSub` — the same single GossipSub instance used
by the election controller. No second pubsub instance is created on the host.

---

## Verification

```bash
# 1. Confirm llama-server /embedding is live and returns 2048 dims
curl -s -X POST http://193.225.250.240/optimusdb1/embedding \
-H "Content-Type: application/json" \
-d '{"content": "test"}' | python3 -c \
"import sys,json; d=json.load(sys.stdin); print(len(d['embedding']), 'dims')"
# Expected: 2048 dims

# 2. Insert a document
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
-H "Content-Type: application/json" \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"dstype": "dsswres",
"criteria": [{"_id":"test_001","name":"Test Solar Farm","type":"solar","status":"operational"}]
}'

# 3. Index the document
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/semantic/index \
-H "Content-Type: application/json" \
-d '{"store":"dsswres","doc_id":"test_001","fields":{"name":"Test Solar Farm","type":"solar","status":"operational"}}'

# 4. Search
curl -s "http://193.225.250.240/optimusdb1/api/v1/semantic/search?q=solar+operational&top_k=3"
```

---

## File map

```
optimusdb/
├── semantic/
│   ├── semantic_search.go    ← Index struct, embed(), localANN(), GossipSub, IPFS pin
│   ├── doc_fetch.go          ← DocFetcher interface, enrichResults(), SearchResult.Document
│   └── http_handlers.go      ← SearchHandler, IndexHandler, BootstrapHandler
├── main.go                   ← background semantic init goroutine, WithFetcher()
├── app/
│   ├── app.go                ← SemanticIdx field, sqlite3_vec_kb driver, FetchDocument()
│   ├── sqlite_ext_linux.go   ← SQLiteDriver.Extensions + EnableLoadExtension (Linux/CGO)
│   └── sqlite_ext_other.go   ← plain SQLiteDriver fallback (Windows/dev)
├── api/
│   └── http.go               ← 3 routes: /semantic/search, /semantic/index, /semantic/bootstrap
└── Dockerfile                ← patchelf libm.so.6 fix for vec0.so, --embedding on llama-server
```

---

## References

- Swarmchestrate project: EU Horizon Europe Grant #101135012