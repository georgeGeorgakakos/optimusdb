# OptimusDB — Embedding Endpoint

## Overview

Each OptimusDB node runs a `llama-server` process (llama.cpp b3790) that serves a local TinyLlama-1.1B model. The server exposes a `/embedding` endpoint that converts any text input into a 2048-dimensional float vector using the model's internal representation layer.

This endpoint is used internally by the semantic search subsystem to index documents and process queries. It can also be exposed externally through the Traefik ingress for a range of additional use cases described below.

**Internal address (always available):**
```
POST http://127.0.0.1:8080/embedding
```

**Request format:**
```json
{ "content": "your text here" }
```

**Response format:**
```json
{ "embedding": [0.016, -0.012, 0.008, ... ] }
```

The response vector has exactly **2048 dimensions**, matching the hidden size of TinyLlama-1.1B. All nodes in the cluster run the same model and produce identical vectors for identical inputs.

---

## Use Cases

### 1. Cross-Node Semantic Bootstrap

When a new node joins the cluster it has no local embeddings. Without an exposed embedding endpoint, the new node must wait for GossipSub propagation or retrieve embedding blobs from IPFS — both of which introduce latency on cold start.

With the endpoint exposed, a joining node can call any peer's `/embedding` route directly over HTTP to generate vectors for its local documents before the mesh stabilises. This accelerates index population on new nodes significantly.

**Relevant code path:** `semantic/semantic_search.go → BootstrapFromIPFS()`

---

### 2. Catalog Frontend Query Expansion

The OptimusDDC catalog frontend currently sends user queries to the full `/swarmkb/semantic/search` route, which embeds the query internally and runs ANN search in one step.

Exposing the embedding endpoint separately allows the frontend to:

- Pre-compute the query vector client-side before issuing a search
- Apply filters or transformations to the vector before search
- Re-rank results using a secondary scoring function
- Display query vector similarity scores directly in the UI

This separation of embedding from search gives the frontend more control over result presentation without modifying the Go backend.

---

### 3. External Data Ingestion Pipelines

External systems — such as SCADA platforms, IoT sensor gateways, or energy management systems — may want to push asset records into OptimusDB and pre-compute embeddings at ingestion time rather than relying on the background indexer.

Exposing the endpoint allows external producers to:

- Call the embedding endpoint before a `CRUDPUT` insert
- Attach the pre-computed vector to the document payload
- Bypass the async indexing delay for time-sensitive records

This is particularly useful when ingestion volume is high and the background indexer would otherwise introduce a lag between insert and searchability.

---

### 4. Federated Search Across Clusters

When two independent OptimusDB deployments need to perform cross-cluster semantic search, both clusters must use the same embedding space — otherwise cosine similarity comparisons between vectors from different models are meaningless.

Exposing the endpoint allows a remote cluster to:

- Normalise its document vectors by calling this cluster's TinyLlama instance
- Ensure vector compatibility before running distributed ANN queries
- Avoid deploying a separate model instance for small or resource-constrained nodes

This is the correct approach when clusters are deployed in different environments but must share a common semantic index.

---

### 5. Batch Re-indexing After Model Upgrades

When the underlying model is upgraded (e.g. from TinyLlama-1.1B to a larger or domain-specific model), all existing embeddings in `vec_embeddings` are invalidated because the vector space changes.

Exposing the endpoint makes batch re-indexing scriptable without requiring `kubectl exec` access:

```bash
# Example: re-embed all documents via external endpoint
curl -s -X POST http://<node>/embedding \
    -H "Content-Type: application/json" \
    -d '{"content": "'"$DOC_TEXT"'"}'
    ```

    An external script can iterate over documents in the SQLite database, call the endpoint for each, and repopulate `vec_embeddings` and `vec_meta` without modifying or redeploying the Go application.

    ---

    ### 6. Research and Benchmarking

    For academic evaluation of the distributed semantic search system, being able to call the embedding endpoint externally simplifies benchmark tooling significantly.

    Standard IR benchmark datasets (e.g. BEIR, MS MARCO subsets) can be fed through the endpoint to:

    - Measure retrieval quality (NDCG, MRR, Recall@K) against ground truth
    - Compare latency and throughput across cluster sizes
    - Validate that distributed ANN results match single-node baselines
    - Generate reproducible evaluation results without modifying the Go codebase

    This is directly relevant for Swarmchestrate deliverables and academic paper evaluation sections.

    ---

    ## Security Considerations

    The `/embedding` endpoint performs CPU-bound inference. Exposing it without access control allows any client that can reach the node to consume CPU resources indefinitely.

    Before exposing through Traefik, apply one of the following:

    | Approach | Traefik Middleware | Suitable For |
    |---|---|---|
    | Static API key | `BasicAuth` or custom header middleware | Internal tools, scripts |
    | IP allowlist | `IPWhiteList` | Cluster-internal or VPN-only access |
    | Rate limiting | `RateLimit` | Public-facing demo environments |
    | No auth | — | Air-gapped / isolated lab environments |

    For the current Swarmchestrate demo environment, an IP allowlist restricted to cluster-internal addresses is the minimum recommended control.

    ---

    ## Exposing via Traefik

    Add an `IngressRoute` for the embedding endpoint in the K3s manifest:

    ```yaml
    apiVersion: traefik.containo.us/v1alpha1
    kind: IngressRoute
    metadata:
    name: optimusdb1-embedding
    namespace: optimusddc
    spec:
    entryPoints:
    - web
    routes:
    - match: PathPrefix(`/optimusdb1/embedding`)
    kind: Rule
    services:
    - name: optimusdb1
    port: 8080
    middlewares:
    - name: embedding-stripprefix
    ---
    apiVersion: traefik.containo.us/v1alpha1
    kind: Middleware
    metadata:
    name: embedding-stripprefix
    namespace: optimusddc
    spec:
    stripPrefix:
    prefixes:
    - /optimusdb1/embedding
    ```

    Repeat for `optimusdb2` and `optimusdb3`. After applying, verify:

    ```bash
    curl -s -X POST http://193.225.250.240/optimusdb1/embedding \
    -H "Content-Type: application/json" \
    -d '{"content":"solar farm photovoltaic Greece"}' | python3 -c \
    "import json,sys; d=json.load(sys.stdin); print(f'Dimensions: {len(d[\"embedding\"])}')"
    ```

    Expected output: `Dimensions: 2048`