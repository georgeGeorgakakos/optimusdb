# OptimusDB — Trace Log & Additions (2026)

**Project:** OptimusDB — Decentralized Data Catalog for Renewable Energy Metadata
**Programme:** EU Horizon Europe, Grant 101135012 — *Swarmchestrate*
**Institution:** Athens University of Economics and Business
**Snapshot:** `optimusdb-lsa_09112026`
**Covers:** December 2025 → July 2026

> This document is a companion to the main [README.md](../README.md). It records **what changed and what was added** across the 2026 development cycle, in chronological order, with the affected source files and the rationale behind each change.
>
> Dates are derived from source-file timestamps in the release snapshot. Where several files changed on the same day they are grouped as a single work item.

---

## Table of Contents

- [1. Snapshot at a Glance](#1-snapshot-at-a-glance)
- [2. What Is New in 2026 — Executive Summary](#2-what-is-new-in-2026--executive-summary)
- [3. Chronological Trace Log](#3-chronological-trace-log)
- [4. New Subsystems in Detail](#4-new-subsystems-in-detail)
- [5. Data Plane Changes — From 6 to 12 Stores](#5-data-plane-changes--from-6-to-12-stores)
- [6. API Surface Added in 2026](#6-api-surface-added-in-2026)
- [7. Persistence Layer — SQLite Schema](#7-persistence-layer--sqlite-schema)
- [8. Deployment & Runtime Changes](#8-deployment--runtime-changes)
- [9. Configuration & Environment Additions](#9-configuration--environment-additions)
- [10. Test & Automation Assets](#10-test--automation-assets)
- [11. Known Issues and Open Items](#11-known-issues-and-open-items)
- [12. Verification Checklist](#12-verification-checklist)

---

## 1. Snapshot at a Glance

| Metric | Nov 2025 baseline | Sep 2026 snapshot | Delta |
|---|---:|---:|---:|
| Go source files (excl. `binding/`) | 62 | 102 | **+40** |
| Total Go lines (excl. `binding/`) | ~24,651 | 45,893 | **+21,242** |
| Non-test Go lines | — | 37,634 | — |
| OrbitDB stores initialised at boot | 6 | 12 | **+6** |
| Top-level Go packages | 14 | 18 | **+4** |
| Automation / test scripts (`repoScript/`) | ~80 | 135 | **+55** |
| K3s manifest size | ~5 KB (EMS variant) | 78,916 bytes | **full stack** |

**New top-level packages:** `semantic/`, `chat/`, `backupfunc/`, `queryengine/` (promoted to active use).

---

## 2. What Is New in 2026 — Executive Summary

| # | Capability | Package(s) | Status |
|---|---|---|---|
| 1 | **Distributed semantic search** — sqlite-vec ANN + GossipSub fan-out + IPFS embedding exchange | `semantic/` | Implemented |
| 2 | **Natural-language query pipeline** — NL → JSON criteria via TinyLlama, with deterministic fallback | `chat/` | Implemented |
| 3 | **Backup / restore & catalog exchange** — tar.gz bundle of all stores + SQLite snapshot | `backupfunc/` | Implemented |
| 4 | **Data lineage tracking** — persistence interceptor, metadata extractor, upstream/downstream edges | `app/` | Implemented |
| 5 | **TOSCA 2.0 store specialisation** — five dedicated docstores with field-level queryability | `app/`, `tosca/` | Implemented |
| 6 | **Production K3s stack** — Keycloak SSO, Traefik IngressRoutes, catalog frontend/search/metadata services | `k3smanifest/` | Deployed |
| 7 | **Election hardening** — rate limiting, term-divergence detection, mesh self-healing | `election/` | Implemented |
| 8 | **EMS/STOMP resilience** — auto-reconnecting client, sensor→catalog mapping | `mq/`, `app/` | Implemented |
| 9 | **Observability** — Loki telemetry sink, structured agent inventory | `logger/`, `api/` | Implemented |
| 10 | **W3C Verifiable Credentials** — issue/query/revoke/verify endpoints | `credentials/` | Partial (see §11) |

---

## 3. Chronological Trace Log

### December 2025 — Lineage foundations and the ICCS hackathon scenario

| Date | Area | Files | Change |
|---|---|---|---|
| 2025-12-01 | Catalog schema | `repoScript/CatalogScripts/setupSchema.{sql,ps1}` | Added the relational catalog bootstrap script (users, badges, dashboards, relations tables) used to seed the catalog front end. |
| 2025-12-05 | IPFS / MQ | `ipfs/ipfsNode.go`, `ipfs/ipfsNodeAct.go`, `mq/reconnect_stomp.go` | Kubo node lifecycle cleanups; introduced `ReconnectingClient` so a STOMP broker restart no longer kills the agent's EMS subscription. |
| 2025-12-13 | Metadata | `contextualmetadata/chat_handler.go` | First conversational entry point over the metadata catalog (precursor to the `chat/` package). |
| 2025-12-19 | TOSCA | `tosca/query_helpers.go` | Helper layer for querying nested TOSCA structures by field path. |
| 2025-12-20 → 12-25 | Scenario assets | `repoScript/Tosca/scenarioHackathon/**` | Full end-to-end hackathon scenario: five TOSCA sample artifacts (ADT, capacity profile, deployment plan, OpenTofu hybrid, app requirements), upload clients in PowerShell / Bash / Python / Go, and `testingGuideICCS.md`. |
| **2025-12-28** | **Lineage** | `app/lineage_manager.go`, `app/metadata_extractor.go`, `app/persistence_interceptor.go` | **New.** Write-path interception with automatic metadata extraction and lineage edge maintenance. See [§4.4](#44-data-lineage-app). |

### January 2026 — Query routing and agent introspection

| Date | Area | Files | Change |
|---|---|---|---|
| 2026-01-02 | Query routing | `app/query_handler.go` | Dedicated libp2p stream handler for distributed queries: per-request options, trace IDs, trace paths, hop limits, `queryPeersLimited` and `queryPeersUntilQuorum`. Decouples query fan-out from `service.go`. |
| 2026-01-02 | API | `api/inventory.go`, `api/host.go`, `api/shell.go` | Agent inventory endpoint — reports stores, peers, roles and capabilities of each agent. |
| 2026-01-04 | SQL / metrics | `app/sql_helpers.go`, `utilities/metricUtil.go` | Shared SQL result-shaping helpers; CPU/RAM/disk metric collection used by the reputation scorer. |
| 2026-01-06 | Docs | `api/ArchitectureStack.md` | Architecture stack reference for the API layer. |

### February 2026 — EMS integration and metadata model

| Date | Area | Files | Change |
|---|---|---|---|
| 2026-02-14 | Tooling | `git-push-optimusdb.ps1` | Repository push automation. |
| 2026-02-17 | Discovery | `api/discovery.go` | Peer-discovery reworked around the unified libp2p host; mDNS / DHT / IPFS-PubSub strategies selectable by flag. |
| 2026-02-18 | EMS | `mq/ems_service.go` | `EMSService` wrapper — lifecycle, topic subscription and dispatch. |
| 2026-02-19 | EMS | `app/ems_subscriber.go` | `StartEMSSubscriber` with health-gated startup and clean shutdown hook; sensor messages mapped into catalog entries (`ProcessEMSSensor`, `sensorToCatalogEntry`). |
| 2026-02-22 | Data model | `datamodel/kbmetadata.go` | Extended metadata entry model — access-control expressions, provenance and TOSCA-aware typing (now ~41 KB). |

### March 2026 — TOSCA parser, simulation, semantic groundwork

| Date | Area | Files | Change |
|---|---|---|---|
| 2026-03-07 | TOSCA | `tosca/toscaparser.go`, `tosca/toscaparser_test.go` | Parser rewrite with unit-test coverage: full-structure JSON storage alongside legacy YAML blob, plus queryable field-path extraction. |
| 2026-03-10 | Simulation | `repoScript/Simulations/simulate_agents.sh` | Multi-agent workload simulator for benchmark runs. |
| 2026-03-30 | Core service | `app/service.go` | Semantic auto-indexing hooked into `crudput`; store dispatch extended to all 12 stores; query-strategy consolidation (`LOCAL_ONLY`, `REMOTE_ONLY`, `PARALLEL_MERGE`, `QUORUM`). |
| 2026-03-30 | Semantic | `semantic/doc_fetch.go` | `DocFetcher` interface + `SearchResult` type — allows hydration of search hits without an `app` ↔ `semantic` import cycle. |
| 2026-03-31 | Observability | `logger/logger.go` | Loki telemetry sink with `-LokiIsDisabled` escape hatch; structured level-based logging. |
| 2026-03-31 | Deployment | `repoScript/Deployment/**` | Scheduled deploy/undeploy scripts and the first full K3s manifest. |

### April 2026 — Semantic search, chat, backup/restore, production stack

| Date | Area | Files | Change |
|---|---|---|---|
| 2026-04-01 | Election | `election/reputationBasedElection.go` | Hardening pass — see [§4.5](#45-election-hardening-election). |
| 2026-04-01 | Deployment | `k3smanifest/deploy-optimusdb-scheduled.sh`, `undeploy-…` | Model injection into running pods, group-qualified `supervisorctl` restart (`optimusdb-suite:tinyllama`), readiness verification. |
| 2026-04-01 | Semantic | `semantic/http_handlers.go` | `/search`, `/index`, `/bootstrap` HTTP handlers. |
| 2026-04-04 | SQLite | `app/sqlite_ext_linux.go`, `app/sqlite_ext_other.go` | **New.** Build-tagged registration of the `sqlite3_vec_kb` driver with `vec0` pre-loaded per connection on Linux+CGO; transparent no-op fallback on Windows. |
| **2026-04-05** | **Semantic** | `semantic/semantic_search.go` | **New.** Full distributed semantic index. See [§4.1](#41-distributed-semantic-search-semantic). |
| 2026-04-18 | Core | `app/app.go` | `KnowledgeBaseDB` extended: `SemanticIdx`, `ExchangeService`, `Interceptor`, `QueryEngine`, `EMSService`, `PubSub`/`ElectionTopic`; `FetchDocument` implements `semantic.DocFetcher`. |
| 2026-04-18 | Core | `app/initPeer.go`, `config/config.go` | Twelve stores opened at boot; new store addresses persisted in the node config file. |
| **2026-04-18** | **Exchange** | `backupfunc/exchange.go` | **New.** Export/import of the whole catalog. See [§4.3](#43-backup--restore-backupfunc). |
| 2026-04-18 | Bootstrap | `main.go` | Non-blocking semantic-index bootstrapper that polls llama-server `/health` and retries every 5 s; EMS subscriber lifecycle; exchange-service wiring. |
| **2026-04-19** | **Chat** | `chat/handler.go`, `chat/adapter.go` | **New.** NL→query pipeline. See [§4.2](#42-natural-language-query-pipeline-chat). |
| 2026-04-19 | API | `api/http.go` | `/api/v1` router: chat, semantic, exchange and metadata routes; TOSCA upload rewritten with interceptor hooks and store targeting. |
| 2026-04-19 | Deployment | `k3smanifest/optimusddc-k3s-manifest.yaml`, `Dockerfile` | Production stack. See [§8](#8-deployment--runtime-changes). |
| 2026-04-19 | Demo | `repoScript/BackupRestore/optimusdb_BackupRestoreDemo.sh` | Scripted export→wipe→import demonstration. |

### July 2026 — Network diagnostics

| Date | Area | Files | Change |
|---|---|---|---|
| 2026-07-14 | Networking | `repoScript/Networking/zstaki_portcheck.sh`, `laptop_portcheck.ps1` | Host-side reachability checks for the HTTP, libp2p and embedding ports across the deployment hosts. |

---

## 4. New Subsystems in Detail

### 4.1 Distributed Semantic Search (`semantic/`)

The largest addition of the cycle. It gives OptimusDB vector retrieval **without a central index** — each agent keeps its own embeddings and the network is queried by broadcast.

**Components**

| File | Responsibility |
|---|---|
| `semantic_search.go` | Index lifecycle, schema migration, GossipSub topics, embed / index / search |
| `doc_fetch.go` | `DocFetcher` interface, `SearchResult` type, result hydration |
| `http_handlers.go` | REST surface |

**Storage.** A `vec0` virtual table created through sqlite-vec:

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings USING vec0(
    doc_id    TEXT PRIMARY KEY,
    embedding float[2048]
);
CREATE TABLE IF NOT EXISTS vec_meta (
    doc_id TEXT PRIMARY KEY, ipfs_cid TEXT, store_name TEXT,
    indexed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    source_text TEXT
);
```

`EmbedDim = 2048` matches TinyLlama-1.1B's hidden size. Because `vec0` virtual tables do not support `ON CONFLICT` / `INSERT OR REPLACE`, upserts are implemented as **DELETE-then-INSERT inside a transaction** — a documented sqlite-vec limitation, not a version bug.

**Search protocol.** Two GossipSub topics:

| Topic | Direction | Payload |
|---|---|---|
| `/optimusdb/semantic/search/1.0.0` | originator → all peers | `{cid, requester, vector, top_k, deadline}` |
| `/optimusdb/semantic/results/1.0.0` | peer → originator | `{cid, results[]}` |

Flow: embed query → **local ANN** → publish `SearchQuery` → collect replies until the budget expires (default **1500 ms**) → merge, dedupe, rank, trim to `topK` → hydrate documents that belong to this node via `DocFetcher`.

Peers return **only** `doc_id + score + store_name`. Content never crosses the wire during fan-out; hydration happens at the coordinator against its own OrbitDB stores. This keeps the fan-out payload small and avoids replicating documents a requester may not be authorised to read.

**Embedding exchange over IPFS.** Every indexed vector is added to IPFS and pinned; the CID is stored in `vec_meta`. `BootstrapFromIPFS(ctx, docID, cid)` lets a fresh or recovering node pull a peer's embeddings instead of re-running inference.

**Auto-indexing.** Hooked into the `crudput` write path (`app/service.go`), so documents become searchable as they land.

**Endpoints:** `POST /api/v1/semantic/search`, `POST /api/v1/semantic/index`, `POST /api/v1/semantic/bootstrap`.

---

### 4.2 Natural-Language Query Pipeline (`chat/`)

Turns a plain question into OptimusDB query criteria and executes it.

**`handler.go`** — conversational layer:

- Intent classification: `greeting`, `help`, `schema`, `list_datasets`, `data_query`
- Dataset-type inference from the message, with history-based fallback (`inferFromHistory`)
- Result formatting and a confidence estimate per answer
- Configurable timeouts, max history and result caps (`HandlerConfig`)

**`adapter.go`** — translation layer:

- `translateWithTinyLlama` posts to the OpenAI-compatible `/v1/chat/completions` (`temperature 0.1`, `max_tokens 256`)
- Response parsing accepts **both** llama-server (`choices[].message.content`) and Ollama (`message.content`) shapes
- `parseTranslationResponse` extracts the JSON criteria block
- **`fallbackTranslation`** — deterministic pattern matching used whenever the model output is unusable, so the endpoint degrades rather than fails
- `getDefaultSchema` supplies per-store field hints to the prompt

Target grammar produced by the model:

```json
{"command":"get|query",
 "criteria":[{"field":"num_cpus","operator":">=","value":2}]}
```

Supported operators: `==`, `!=`, `>`, `>=`, `<`, `<=`, `contains`.

**Endpoints:** `POST /api/v1/chat`, `GET /api/v1/chat/health`.

---

### 4.3 Backup / Restore (`backupfunc/`)

Full catalog portability in a single artifact.

**Export** (`GET /api/v1/exchange/export`) produces a `tar.gz` containing:

- `manifest.json` — peer ID, timestamp, store inventory, record counts
- one **JSONL** file per docstore (all 12 stores enumerated via `storeRegistry()`)
- a consistent SQLite snapshot taken with `VACUUM INTO` into a temp file

**Import** (`POST /api/v1/exchange/import`) extracts the bundle, reads the manifest, replays each JSONL file document-by-document into the matching store, and restores the SQLite file — re-opening it through `app.InitSQLite` so the `sqlite3_vec_kb` driver hook re-attaches and `vec0` stays available. Returns an `ImportReport` with per-store counts.

---

### 4.4 Data Lineage (`app/`)

Three cooperating files turn ordinary writes into catalog + lineage records.

```
CRUD write ──► PersistenceInterceptor ──► MetadataExtractor ──► LineageManager
                (OnDocumentPut /            (classify + extract      (datacatalog rows +
                 Update / Delete)            references)              upstream/downstream edges)
```

- **`persistence_interceptor.go`** — `OnDocumentPut` / `OnDocumentUpdate` / `OnDocumentDelete`, with `Enable()` / `Disable()` for bulk loads.
- **`metadata_extractor.go`** — detects TOSCA vs SQL vs generic documents (`isTOSCA`, `isSQL`), maps TOSCA types to metadata types, counts fields, and discovers cross-document references (`detectTOSCAReferences`, `detectJSONReferences`).
- **`lineage_manager.go`** — writes `datacatalog` entries, resolves references to table URIs, and maintains bidirectional edges (`addToDownstream`, `removeFromUpstream`), including cleanup on delete (`RemoveLineageForDeletedDocument`).

Wiring: instantiated in `app/initPeer.go`; invoked from `api/http.go` (TOSCA upload) and `app/service.go` (update/delete paths).

---

### 4.5 Election Hardening (`election/`)

Extensions to the reputation-based coordinator election in the April pass:

| Addition | Purpose |
|---|---|
| `MessageRateLimiter` | Per-peer, per-message-type throttling — protects against election-message storms |
| `hashPeerList` + `selectInitiatorDeterministic` | All nodes independently agree on who starts a round, removing simultaneous-initiation races |
| `checkTermDivergence` | Detects and reconciles split terms across the mesh |
| `validateStartupTerm` | Prevents a restarted node from re-entering with a stale term |
| `emergencyMeshHealing`, `MonitorAndHealMesh`, `checkMeshHealth` | Active GossipSub mesh repair when peer degree drops below threshold |
| `promoteAsDefaultCoordinator`, `isDefaultCoordinatorNode` | Deterministic bootstrap coordinator derived from container name |
| `CreateBetterGossipSubParams` | Tuned GossipSub parameters (self-delivery + flood publish) for small clusters |
| `fallbackElection` | Last-resort path when quorum cannot be reached |

Reputation and election history persist in the `reputation` and `election_log` SQLite tables.

---

## 5. Data Plane Changes — From 6 to 12 Stores

Stores opened at boot in `app/initPeer.go`:

| # | Store | Type | Added | Purpose |
|---|---|---|---|---|
| 1 | `contributions` | EventLog | pre-2026 | Append-only contribution log |
| 2 | `validations` | Document | pre-2026 | Validation records |
| 3 | `kbdata` | Document | pre-2026 | Primary data |
| 4 | `kbmetadata` | Document | pre-2026 | Metadata catalog |
| 5 | `dsswres` | Document | pre-2026 | Swarm resources |
| 6 | `dsswresaloc` | Document | pre-2026 | Resource allocations |
| 7 | `tosca_imported` | Document | **2026** | Raw imported TOSCA artifacts |
| 8 | `whoiswho` | Document | **2026** | Agent/identity registry |
| 9 | `tosca_adt` | Document | **2026** | Application Description Templates |
| 10 | `tosca_capacities` | Document | **2026** | Capacity profiles |
| 11 | `tosca_deploymentplan` | Document | **2026** | Deployment / release plans |
| 12 | `tosca_eventhistory` | Document | **2026** | Deployment event history |

All six new stores are threaded through `resolveDocStoreByType`, the CRUD put/get/update/delete dispatch, and `KnowledgeBaseDB.FetchDocument`. Their OrbitDB addresses are persisted in `config.Config` so they survive restarts.

---

## 6. API Surface Added in 2026

Base context defaults to `swarmkb`; the versioned routes live under `/api/v1`.

### Semantic search
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/semantic/search` | Hybrid local-ANN + network semantic query |
| `POST` | `/api/v1/semantic/index` | Index a document into the local vector store |
| `POST` | `/api/v1/semantic/bootstrap` | Pull a peer's embedding blob by CID |

### Conversational query
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/chat` | Natural-language question → executed query + formatted answer |
| `GET` | `/api/v1/chat/health` | LLM reachability probe |

### Catalog exchange
| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/exchange/export` | Download full catalog bundle (`tar.gz`) |
| `POST` | `/api/v1/exchange/import` | Restore from a bundle; returns per-store counts |

### Metadata enrichment
| Method | Path | Description |
|---|---|---|
| `POST` | `/api/v1/metadata/enrich` | Enrich a single dataset |
| `POST` | `/api/v1/metadata/enrich-batch` | Batch enrichment |
| `GET` | `/api/v1/metadata/profile` | Dataset profiling |
| `GET` | `/api/v1/metadata/metrics` | LLM/enrichment metrics |
| `GET` | `/api/v1/metadata/health` | Enrichment service health |
| `DELETE` | `/api/v1/metadata/cache` | Clear enrichment cache |

### Verifiable Credentials
| Method | Path | Description |
|---|---|---|
| `POST` | `/{ctx}/credentials` | Issue / store a credential |
| `GET` | `/{ctx}/credentials/get/{id}` | Retrieve by ID |
| `POST` | `/{ctx}/credentials/query` | Query by criteria |
| `GET` | `/{ctx}/credentials/issuer/{did}` | List by issuer |
| `GET` | `/{ctx}/credentials/subject/{did}` | List by subject |
| `POST` | `/{ctx}/credentials/revoke` | Revoke |
| `POST` | `/{ctx}/credentials/verify` | Verify (see §11) |

### Operations & EMS
| Method | Path | Description |
|---|---|---|
| `GET` | `/{ctx}/agent/status` | Role, term, leader, peers, store health |
| `GET` | `/{ctx}/agent/inventory` | Full agent capability inventory |
| `GET` | `/{ctx}/debug/optimusdb/mesh` | GossipSub mesh introspection |
| `GET` | `/{ctx}/benchmarks` | Cluster benchmark collection |
| `GET/POST` | `/{ctx}/ems`, `/ems/logs`, `/ems/events`, `/ems/sql` | EMS bridge surfaces |

---

## 7. Persistence Layer — SQLite Schema

Tables created by the current code base (SQLite is used for the catalog, reputation, vectors and logs; OrbitDB remains the decentralized document plane):

**Catalog & lineage:** `datacatalog`, `metadata_catalog`, `column_metadata`, `type_metadata`, `toscametadata`, `resource_dependencies`
**Semantic:** `vec_embeddings` (virtual, `vec0`), `vec_meta`, `search_cache`
**Consensus:** `reputation`, `election_log`
**Identity & access:** `users`, `credentials_metadata`, `user_table_relations`, `user_resource_relations`, `user_dashboard_relations`
**Front end:** `dashboards`, `badges`, `blocks`, `table_dashboard_relations`
**Operations:** `optimusLogger`, `access_log`, `ems_events`

---

## 8. Deployment & Runtime Changes

### 8.1 Container image (`Dockerfile`, 2026-04-19)

- **Builder:** `golang:1.19.13`; build with `CGO_ENABLED=1 -tags allow_load_extension`.
- **llama.cpp:** pre-built `llama-server` from release **b3790** (confirmed compatible with Q4_K_M on the target CPU) — no longer compiled from source.
- **sqlite-vec:** `v0.1.6` loadable extension installed to `/usr/lib/sqlite-vec/vec0.so`.
- **`patchelf --add-needed libm.so.6 vec0.so`** — fixes the `undefined symbol: sqrtf` failure that occurred when `vec0` was loaded at driver-init time rather than lazily.
- **Supervisor:** both `tinyllama` (priority 1) and `optimusdb` (priority 2) run under `supervisord` in group `optimusdb-suite`, with `[unix_http_server]` + `[supervisorctl]` enabled so `supervisorctl restart optimusdb-suite:tinyllama` works inside the pod.
- **Model:** `models/*.gguf` is a **0-byte placeholder** in git; the real weights are injected into running pods by the deploy script.
- **Ports exposed:** `4001 4002 5001 8080 8089 9001`.

### 8.2 K3s production stack (`k3smanifest/optimusddc-k3s-manifest.yaml`, ~79 KB)

| Object class | Instances |
|---|---|
| Namespace | `optimusddc` |
| OptimusDB nodes | `optimusdb1/2/3` — Deployment + Service + NodePort + PVC each, plus `optimusdb-headless` |
| Identity | **Keycloak 23.0.7** — Deployment, Service, PVC, admin Secret, realm ConfigMap, custom login template + SCSS |
| Catalog application | `catalogfrontend`, `catalogsearch`, `catalogmetadata` — Deployment + Service each |
| LLM services | `optimusdb1/2/3-llama` (port 8080) |
| Traefik middlewares | per-node strip-prefix, `embedding-ipallowlist`, per-node embedding strip-prefix |
| IngressRoutes | keycloak-auth, command-compat, agent-status/inventory/log/swarmkb load-balanced routes, per-node routes, chat API, semantic API, catalog APIs, frontend root, per-node `/optimusdbN/embedding` |

The embedding routes are protected by an **IP allowlist** because `/embedding` runs CPU-bound inference and must not be exposed to arbitrary external traffic.

### 8.3 Scheduled deployment (`deploy-optimusdb-scheduled.sh`)

Copies `tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf` from `/opt/iccs/libs/` into each pod, verifies the file size, then starts or restarts the LLM using the **group-qualified** supervisor name (`optimusdb-suite:tinyllama`) — the bare name returns *"no such process"* once the program belongs to a group. Waits for readiness before declaring success.

---

## 9. Configuration & Environment Additions

### New / notable flags (`config/flags.go`)

| Flag | Default | Purpose |
|---|---|---|
| `-metrics` | `true` | Enable CPU/RAM metric collection |
| `-autodis` | `true` | Multi-swarm autodiscovery |
| `-dismDNS` | `true` | mDNS discovery |
| `-disIpfsPubSub` | `false` | IPFS PubSub discovery |
| `-disDHT` | `false` | DHT discovery |
| `-election-retry-limit` | `1` | Max election retry attempts |
| `-election-retry-delay` | `3s` | Initial retry backoff |
| `-logfile` | `logs/optimusdb.log` | Log path |
| `-LokiIsDisabled` | `false` | Disable Loki telemetry |
| `-mq-url` / `-mq-user` / `-mq-pass` / `-mq-topic` | env-backed | STOMP broker settings |
| `-Swarmchestrate` | `""` | Swarm name for this agent |

### Environment variables

| Variable | Consumed by | Notes |
|---|---|---|
| `TINYLLAMA_ENDPOINT` | `main.go`, `chat/`, `contextualmetadata/` | Base URL is derived from this via `llamaBaseURL()` and reused for `/embedding` |
| `TINYLLAMA_URL` | metadata enrichment | Chat-completions URL |
| `TINYLLAMA_MODEL` | runtime | Model path |
| `METADATA_ENRICHMENT_ENABLED` | enrichment worker | `true` by default |
| `METADATA_CACHE_TTL` | enrichment cache | `24h` by default |
| `MQ_URL` / `MQ_USER` / `MQ_PASS` / `MQ_TOPIC` | EMS | Override the corresponding flags |

---

## 10. Test & Automation Assets

`repoScript/` now holds 135 files across 17 categories:

| Directory | Contents |
|---|---|
| `semantic/` | `test_semantic_search.sh` — end-to-end semantic search validation |
| `BackupRestore/` | `optimusdb_BackupRestoreDemo.sh` — export → wipe → import demo |
| `Deployment/`, `k3s/` | Scheduled deploy/undeploy, manifests |
| `Simulations/` | `simulate_agents.sh` — multi-agent workload generator |
| `Tosca/scenarioHackathon/` | ICCS end-to-end scenario: samples, clients (PS/Bash/Python/Go), Postman collections, testing guide |
| `ElectionAnalysis/`, `Monitoring/`, `Networking/` | Election tracing, cluster health, port reachability |
| `Metadata/`, `CatalogScripts/`, `DID/`, `EMS/`, `Energy/` | Feature-specific test suites |
| `debugCompilation/` | Docker cluster deployment and fallback-scenario tests |

---

## 11. Known Issues and Open Items

| # | Severity | Item | Location | Detail |
|---|---|---|---|---|
| 1 | **High** | `--embedding` flag is not passed to `llama-server` | `Dockerfile` supervisord block | The command is `llama-server -m … -c 512 --host 0.0.0.0 --port 8080 --n-gpu-layers 0`. Without `--embedding`, `/embedding` returns no vector and `semantic.embed()` fails with *"empty embedding — add --embedding flag to llama-server"*. Semantic search cannot work on images built from this Dockerfile. |
| 2 | **High** | Single llama-server may not serve both chat **and** embeddings | `Dockerfile`, `chat/`, `semantic/` | On llama.cpp b3790, embedding mode and generation are mutually exclusive on one instance. If confirmed, run a **second** `llama-server` on a separate port with `--embedding` and point `semantic` at it. |
| 3 | Medium | `TINYLLAMA_EMBEDDING_ENDPOINT` is set but never read | `Dockerfile` env vs. Go sources | `semantic` derives its URL from `TINYLLAMA_ENDPOINT`. Either consume the variable or remove it to avoid misleading operators. |
| 4 | Medium | Context window is tight | `Dockerfile` (`-c 512`), `chat/adapter.go` | System prompt (~150 tokens) + user message + `max_tokens: 256` approaches 512. Longer questions truncate silently. Consider `-c 1024`. |
| 5 | Medium | VC proof verification is a stub | `credentials/cred_service.go:187` | `VerifyProof` checks that proof fields are present but performs **no cryptographic verification**. Required before any security claim is published. |
| 6 | Low | `dstype` switch duplicated ~8× | `app/service.go` | The 12-store switch is repeated across put/get/update/delete/resolve paths. A single `storeRegistry()` map (as already used in `backupfunc`) would remove the drift risk. |
| 7 | Low | Dead code | `app/service.go` | Large commented-out bodies of the previous `queryPeers` and `ConvertMetadataToMap` remain, inflating the file to 4,417 lines. |
| 8 | Low | Documentation folder absent from snapshot | `docs/` | The main README links ~30 documents under `docs/` that are not present in this archive. |
| 9 | Low | Default MQ credentials | `config/flags.go` | `admin`/`admin` defaults; ensure the deployment always overrides via `MQ_USER`/`MQ_PASS` secrets. |

---

## 12. Verification Checklist

Before the next demonstration or release, confirm:

- [ ] `llama-server` starts with `--embedding` (or a dedicated embedding instance exists)
- [ ] `POST /api/v1/semantic/search` returns non-empty results on a seeded cluster
- [ ] `POST /api/v1/chat` succeeds **and** the fallback path is exercised by forcing an LLM failure
- [ ] `vec0` loads cleanly in-container: `SELECT vec_version();`
- [ ] Export → import round-trip preserves all 12 stores and the SQLite catalog
- [ ] Lineage edges appear in `datacatalog` after a TOSCA upload with cross-references
- [ ] Coordinator election converges on a 3-node K3s deployment and survives a leader kill
- [ ] EMS subscriber reconnects after a broker restart
- [ ] Keycloak-protected IngressRoutes reject unauthenticated requests
- [ ] `/optimusdbN/embedding` is unreachable from outside the allowlisted range

---

*Trace log compiled from the `optimusdb-lsa_09112026` source snapshot. Dates reflect source-file modification timestamps.*
