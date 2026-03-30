# Semantic Search for OptimusDB

Distributed semantic search over all OrbitDB document stores, using your existing `llama-server` for embeddings, `sqlite-vec` for local ANN, and GossipSub for peer fan-out.

---

## How it works

```
Write path (non-blocking goroutine)
────────────────────────────────────────────────────────
crudPutDocStoreRev ──► go IndexDocument(store, docID, fields)
│
├─► POST /embedding        llama-server (existing, add --embedding flag)
│         └─► float32[4096] vector
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
| Vector storage (local) | `sqlite-vec` virtual table in `rdbms.DB` | Same `*sql.DB` as rest of OptimusDB — no extra file |
| Vector storage (remote) | IPFS blob via `(*Orbit).IPFS().Unixfs().Add()` | Already used in `service.go:1646` — consistent API |
| Distributed search | GossipSub topic pair on `knowledgeBaseDB.PubSub` | Reuses the IPFS node's single pubsub — no double-instance conflict |
| Write path | Non-blocking goroutine | `crudPutDocStoreRev` returns immediately |
| Import boundary | `SemanticIdx interface{}` on `KnowledgeBaseDB` | Avoids `app → semantic → app` import cycle |

---

## End-to-end example

### 1. Insert three energy asset documents

```bash
curl -X POST http://optimusdb-agent-1:9091/api/v1/crudput \
-H 'Content-Type: application/json' \
-d '{
"method": {"cmd": "crudput", "argcnt": 1},
"dstype": "dsswres",
"criteria": [
{
"_id": "solar_attica_001",
"name": "Athens Solar Farm",
"type": "solar",
"status": "operational",
"capacity_mw": 500,
"location": { "country": "Greece", "region": "Attica" },
"tags": ["renewable", "solar", "high-capacity"]
},
{
"_id": "wind_thrace_007",
"name": "Thrace Wind Park",
"type": "wind",
"status": "operational",
"capacity_mw": 800,
"location": { "country": "Greece", "region": "Thrace" },
"tags": ["renewable", "wind", "offshore-candidate"]
},
{
"_id": "solar_crete_012",
"name": "Crete Solar Installation",
"type": "solar",
"status": "maintenance",
"capacity_mw": 120,
"location": { "country": "Greece", "region": "Crete" },
"tags": ["solar", "island-grid"]
}
]
}'
```

After each successful insert, `crudPutDocStoreRev` fires a goroutine:

```
"Athens Solar Farm solar operational Greece Attica renewable solar high-capacity"
│
├─► POST /embedding  →  float32[4096]
├─► sqlite-vec upsert
└─► IPFS pin  →  QmCid stored in vec_meta
```

The `crudput` HTTP response returns immediately. All three documents are indexed concurrently in the background.

---

### 2. Search from the OptimusDDC UI

The user types into the Semantic Search page search bar:

```
solar farm high capacity operational
```

The OptimusDDC frontend calls:

```
GET /api/v1/semantic/search?q=solar+farm+high+capacity+operational&top_k=5&budget_ms=1500
```

---

### 3. Response

Each result includes `document` — the full OrbitDB document content, fetched via `DocumentStore.Get()` after the ANN ranking is complete.

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
"location": {
"country": "Greece",
"region":  "Attica"
},
"tags": ["renewable", "solar", "high-capacity"]
}
},
{
"doc_id":      "wind_thrace_007",
"score":       0.81,
"source_node": "QmNode1xYzAbCdEf...",
"store":       "dsswres",
"document": {
"name":        "Thrace Wind Park",
"type":        "wind",
"status":      "operational",
"capacity_mw": 800,
"location": {
"country": "Greece",
"region":  "Thrace"
},
"tags": ["renewable", "wind", "offshore-candidate"]
}
}
]
}
```

`solar_crete_012` does not appear — its embedding ("Crete Solar Installation solar maintenance island-grid") is semantically distant from "high capacity operational". The cosine similarity falls below both returned results and it is trimmed from the top-K.

> **`document` field rules:**
> - Present when `source_node` matches the coordinator node (local OrbitDB lookup via `DocumentStore.Get()`).
> - `null` for results from remote peers — document content is not serialised over GossipSub to keep reply payloads small. The coordinator can still resolve remote docs if it holds a replica of the same OrbitDB store.
> - Internal fields `_id` and `_created_at` are stripped from the document payload.

---

### 4. Peer merging (multi-node cluster)

```
coordinator (agent-1)
│
├─► local ANN  →  [solar_attica_001: 0.94, wind_thrace_007: 0.81]
│
└─► GossipSub broadcast  →  agent-2, agent-3, agent-4 ...
│
├─► agent-2 local ANN  →  [wind_thrace_007: 0.79]   (replica)
└─► agent-3 local ANN  →  []                         (no matching docs)

merge: deduplicate wind_thrace_007 (keep score 0.81 > 0.79), trim to top_k=5
final: [solar_attica_001: 0.94, wind_thrace_007: 0.81]
```

---

## Integration changes

Four surgical edits across three existing files. One new package (`semantic/`) drops in alongside `chat/`, `contextualmetadata/`.

### §1 — `main.go`: load sqlite-vec + init SemanticIndex

**Add imports and a second `init()` for the driver:**

```go
import (
// existing imports ...
_ "github.com/asg017/sqlite-vec-go-bindings/cgo" // NEW
"optimusdb/semantic"                              // NEW
)

func init() {
app.InitAgentName() // existing
}

// NEW — register sqlite3 with vec0 extension
func init() {
sql.Register("sqlite3_vec", &sqlite3.SQLiteDriver{
Extensions: []string{"vec0"},
})
}
```

**Add a URL helper:**

```go
func llamaBaseURL(endpoint string) string {
// "http://host:8080/v1/completions" → "http://host:8080"
if idx := strings.Index(endpoint, "/v1/"); idx != -1 {
return endpoint[:idx]
}
return endpoint
}
```

**Init after GossipSub is ready** (immediately after `knowledgeBaseDB.PubSub = ps`):

```go
llamaURL := llamaBaseURL(os.Getenv("TINYLLAMA_ENDPOINT"))
if llamaURL == "" {
llamaURL = "http://localhost:8080"
}

semanticIdx, semanticErr := semantic.New(
rdbms.DB,                   // *sql.DB
llamaURL,                   // "http://localhost:8080"
knowledgeBaseDB.Orbit,      // *iface.OrbitDB
hostMain,                   // host.Host
ps,                         // *pubsub.PubSub
)
if semanticErr != nil {
logger.Warn("[SEMANTIC] Index unavailable: %v", semanticErr)
} else {
// Wire the fetcher — enables document content in search results.
semanticIdx.WithFetcher(&knowledgeBaseDB)
logger.Info("[SEMANTIC] Semantic search initialized (llama: %s)", llamaURL)
knowledgeBaseDB.SemanticIdx = semanticIdx
}
```

---

### §2b — `app/app.go`: add `FetchDocument` method

This implements `semantic.DocFetcher` — the interface that lets `semantic.Index` fetch full document content from OrbitDB after ranking, without creating an import cycle. Add alongside the other KB interface methods:

```go
func (kb *KnowledgeBaseDB) FetchDocument(ctx context.Context, storeName, docID string) (map[string]interface{}, error) {
var store iface.DocumentStore
switch strings.ToLower(storeName) {
case "dsswres":
if kb.DsSWres == nil { return nil, fmt.Errorf("store not init") }
store = *kb.DsSWres
case "dsswresaloc":
if kb.DsSWresaloc == nil { return nil, fmt.Errorf("store not init") }
store = *kb.DsSWresaloc
case "kbmetadata":
if kb.KBMetadata == nil { return nil, fmt.Errorf("store not init") }
store = *kb.KBMetadata
case "kbdata":
if kb.KBdata == nil { return nil, fmt.Errorf("store not init") }
store = *kb.KBdata
// ... add remaining stores: tosca_imported, tosca_adt, tosca_capacities,
//     tosca_deploymentplan, tosca_eventhistory, whoiswho
default:
return nil, fmt.Errorf("unknown store: %s", storeName)
}
opts := &iface.DocumentStoreGetOptions{CaseInsensitive: false, PartialMatches: false}
docs, err := store.Get(ctx, docID, opts)
if err != nil || len(docs) == 0 {
return nil, err
}
if m, ok := docs[0].(map[string]interface{}); ok {
return m, nil
}
return nil, fmt.Errorf("unexpected doc type")
}
```

The full switch with all stores is in `semantic/app_changes.go.txt`.



**Add one field to `KnowledgeBaseDB`** (after `Interceptor`):

```go
// *semantic.Index — interface{} avoids import cycle
SemanticIdx interface{}
```

**Change driver in `InitSQLite`:**

```go
// BEFORE:
db, err := sql.Open("sqlite3", rdbmsCache)

// AFTER:
db, err := sql.Open("sqlite3_vec", rdbmsCache)
```

---

### §3 — `app/service.go`: hook into `crudPutDocStoreRev`

Inside the `if insertSuccess` block (after the progress log, ~line 1295):

```go
if optimusdb.SemanticIdx != nil {
fields := docFieldsToStringMap(docMap)
go func(sn, id string, f map[string]string, sidx interface{}) {
type indexer interface {
IndexDocument(store, docID string, fields map[string]string) error
}
if idx, ok := sidx.(indexer); ok {
if err := idx.IndexDocument(sn, id, f); err != nil {
logger.Warn("[SEMANTIC] index failed %s/%s: %v", sn, id, err)
}
}
}(storeName, docID, fields, optimusdb.SemanticIdx)
}
```

Add the helper anywhere in `service.go`:

```go
func docFieldsToStringMap(doc map[string]interface{}) map[string]string {
out := make(map[string]string, len(doc))
for k, v := range doc {
if k == "_id" || k == "_created_at" {
continue
}
switch t := v.(type) {
case string:
out[k] = t
case []interface{}:
parts := make([]string, 0, len(t))
for _, item := range t {
if s, ok := item.(string); ok {
parts = append(parts, s)
}
}
out[k] = strings.Join(parts, " ")
default:
out[k] = fmt.Sprintf("%v", v)
}
}
return out
}
```

---

### §4 — `api/http.go`: register routes

Inside `RegisterMetadataRoutes`, after the chat endpoint block:

```go
if kb.SemanticIdx != nil {
type semanticRouter interface {
SearchHandler(http.ResponseWriter, *http.Request)
IndexHandler(http.ResponseWriter, *http.Request)
BootstrapHandler(http.ResponseWriter, *http.Request)
}
if sidx, ok := kb.SemanticIdx.(semanticRouter); ok {
apiV1.HandleFunc("/semantic/search",    sidx.SearchHandler).Methods("GET")
apiV1.HandleFunc("/semantic/index",     sidx.IndexHandler).Methods("POST")
apiV1.HandleFunc("/semantic/bootstrap", sidx.BootstrapHandler).Methods("POST")
logger.Info("[SEMANTIC] Routes registered at /api/v1/semantic")
}
}
```

---

### §5 — `Dockerfile`: enable the `/embedding` route

The `llama-server` ships with embedding support built in. Add `--embedding` to the startup command in the supervisord config section of the Dockerfile:

```dockerfile
# BEFORE:
command=/usr/local/bin/llama-server -m /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf \
-c 2048 --host 127.0.0.1 --port 8080 --n-gpu-layers 0

# AFTER:
command=/usr/local/bin/llama-server -m /models/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf \
-c 2048 --host 127.0.0.1 --port 8080 --n-gpu-layers 0 --embedding
```

For the multi-node compose setup, add `--embedding` to both `COORDINATOR_TINYLLAMA_TEMPLATE` and `FOLLOWER_TINYLLAMA_TEMPLATE` in `repoScript/Energy/generate-docker-compose.py`.

---

## New dependency

```bash
go get github.com/asg017/sqlite-vec-go-bindings/cgo
```

The C library ships with the Go module via CGo — no separate shared library install needed.

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
curl "http://localhost:9091/api/v1/semantic/search?\
q=solar+farm+high+capacity+operational&top_k=5&budget_ms=1500"
```

**Response:**

```json
{
"query":   "solar farm high capacity operational",
"count":   2,
"results": [
{
"doc_id":      "solar_attica_001",
"score":       0.94,
"source_node": "QmNode1...",
"store":       "dsswres",
"document": {
"name": "Athens Solar Farm",
"type": "solar",
"status": "operational",
"capacity_mw": 500,
"location": { "country": "Greece", "region": "Attica" },
"tags": ["renewable", "solar", "high-capacity"]
}
},
{
"doc_id":      "wind_thrace_007",
"score":       0.81,
"source_node": "QmNode1...",
"store":       "dsswres",
"document": {
"name": "Thrace Wind Park",
"type": "wind",
"status": "operational",
"capacity_mw": 800,
"location": { "country": "Greece", "region": "Thrace" },
"tags": ["renewable", "wind", "offshore-candidate"]
}
}
]
}
```

---

### `POST /api/v1/semantic/index`

Manually (re-)index a document. Useful after schema changes or for documents inserted before semantic search was enabled.

```bash
curl -X POST http://localhost:9091/api/v1/semantic/index \
-H 'Content-Type: application/json' \
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

Fetch a pre-computed embedding from IPFS instead of re-running `llama-server` inference. Called automatically when a node replicates a document from a peer and the CID is available in `vec_meta`.

```bash
curl -X POST http://localhost:9091/api/v1/semantic/bootstrap \
-H 'Content-Type: application/json' \
-d '{ "doc_id": "solar_attica_001", "ipfs_cid": "QmXyz..." }'
```

---

## New SQLite tables

Two tables are added to the existing `rdbms.DB` database on first startup:

```sql
-- ANN index (sqlite-vec virtual table)
CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings USING vec0(
doc_id    TEXT PRIMARY KEY,
embedding float[4096]
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

Both topics use `knowledgeBaseDB.PubSub` — the same single GossipSub instance used by the election controller. No second pubsub instance is created on the host.

---

## Verification

```bash
# 1. Confirm llama-server /embedding is live
curl -s http://localhost:8080/embedding \
-H 'Content-Type: application/json' \
-d '{"content": "test"}' | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d['embedding']), 'dims')"
# 4096 dims

# 2. Insert a document (triggers background indexing)
curl -X POST http://localhost:9091/api/v1/crudput \
-H 'Content-Type: application/json' \
-d '{"method":{"cmd":"crudput","argcnt":1},"dstype":"dsswres","criteria":[
{"_id":"test_001","name":"Test Solar Farm","type":"solar","status":"operational"}
]}'

# 3. Wait ~2s for indexing goroutine, then search
curl "http://localhost:9091/api/v1/semantic/search?q=solar+operational&top_k=3"
```

---

## File map

```
optimusdb-lsa/
├── semantic/
│   ├── semantic_search.go    ← Index struct, embed(), localANN(), GossipSub, IPFS pin
│   ├── doc_fetch.go          ← DocFetcher interface, enrichResults(), SearchResult.Document
│   └── http_handlers.go      ← SearchHandler, IndexHandler, BootstrapHandler
├── main.go                   ← +init() for sqlite-vec, +semantic.New(), +WithFetcher()
├── app/
│   ├── app.go                ← +SemanticIdx field, driver change, +FetchDocument() method
│   └── service.go            ← +goroutine in crudPutDocStoreRev, +docFieldsToStringMap()
├── api/
│   └── http.go               ← +3 routes in RegisterMetadataRoutes
└── Dockerfile                ← +--embedding flag to llama-server command
```
