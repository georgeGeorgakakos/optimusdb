# OptimusDB — HTTP Interface & Usage

This document describes **how to interact with OptimusDB over HTTP** and the related configuration knobs.

---

## Overview

OptimusDB exposes:
- **HTTP API** on port **8089** (you may map externally, e.g. 1800x)
- **P2P/libp2p** on port **4001** (e.g. 1400x outside)
- **Gateway** (IPFS-style) on port **5001** (e.g. 1500x outside)

> Exact endpoints are defined in the source; this guide lists the current surface.

---

## Run

### Docker (3 instances)
```bash
docker network create swarmnet || true

docker run -d --network=swarmnet --name=optimusdb1   -p 18001:8089 -p 14001:4001 -p 15001:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb2   -p 18002:8089 -p 14002:4001 -p 15002:5001   ghcr.io/georgegeorgakakos/optimusdb:latest

docker run -d --network=swarmnet --name=optimusdb3   -p 18003:8089 -p 14003:4001 -p 15003:5001   ghcr.io/georgegeorgakakos/optimusdb:latest
```

### K3s (3 pods via StatefulSet)
Point your manifest to the image and apply:
```yaml
# in your K8s workload spec
image: ghcr.io/georgegeorgakakos/optimusdb:latest
```
Then:
```bash
kubectl apply -f k3smanifest/optimusdb-k3s.yaml
```

---

## Base

- **HTTP Port:** `8089` (flag: `-http-port`)
- **Path Prefix (context):** `swarmkb` (flag: `-swarmkb`)
- **Base URL:** `http://{host}:8089/swarmkb`
- **CORS:** `Access-Control-Allow-Origin: *`, `Access-Control-Allow-Methods: *`, `Access-Control-Allow-Headers: Content-Type`
- **Content-Type:** all JSON endpoints expect `Content-Type: application/json`

> You can change the port and context via flags; see **Flags** below.

---

## Configuration

### Environment variables
- `OPTIMUSDB_API_PORT` (default `8089`)
- `OPTIMUSDB_P2P_PORT` (default `4001`)
- `OPTIMUSDB_GATEWAY_PORT` (default `5001`)
- `OPTIMUSDB_LOG_LEVEL` (e.g., `info`, `debug`)
- `OPTIMUSDB_SEED_PEERS` (comma-separated `host:port` seed list; optional)

### Flags (CLI)
- `-http` (default `true`) — enable HTTP interface
- `-http-port` (default `8089`)
- `-ipfs-port` (default `4001`)
- `-swarmkb` (default `swarmkb`) — HTTP path prefix / context
- `-benchmark` (default `false`) — enable `/benchmarks`

> Combine env vars and flags as needed; env vars typically apply at container/pod level, flags are passed to the binary entrypoint.

---
# 🔍 OptimusDB — Request & Criteria Examples

This document illustrates how to construct query requests for OptimusDB, including all supported criteria patterns, strategies, and operators.

---

## 🧱 1. Basic Equality Match

```json
{
"method": {"cmd": "query"},
"dstype": "dsswres",
"criteria": [
{"status": "active"},
{"type": "solar"}
]
}
```

---

## ⚡ 2. Compound AND Condition

```json
{
"method": {"cmd": "query"},
"dstype": "dsswres",
"criteria": [
{"$and": [
{"type": "solar"},
{"status": "active"}
]}
]
}
```

---

## 🔁 3. Regular Expression Match

```json
{
"criteria": [
{"_id": {"$regex": "test-1800.*"}}
]
}
```

---

## 🔢 4. Numeric Range Query

```json
{
"criteria": [
{"power": {"$gte": 100, "$lte": 300}}
]
}
```

---

## 🧮 5. Logical Combinations

### `$or`
```json
{"criteria":[{"$or":[{"type":"wind"},{"type":"solar"}]}]}
```

### `$not`
```json
{"criteria":[{"$not":{"status":"inactive"}}]}
```

---

## 🧠 6. Nested Field Query

```json
{
"criteria": [
{"metadata.location.country": "Greece"},
{"metadata.tags": {"$regex": "energy"}}
]
}
```

---

## 🌐 7. Decentralized Query with Strategy

```json
{
"method": {"cmd": "query"},
"dstype": "dsswres",
"criteria": [{"type": "solar", "status": "active"}],
"options": {
"strategy": "PARALLEL_MERGE",
"consistency": "BEST_EFFORT",
"time_budget_ms": 2000,
"annotate_source": true
}
}
```

---

## 🔗 8. Quorum-Based Query

```json
{
"method": {"cmd": "query"},
"dstype": "dsswres",
"criteria": [{"status": "active"}],
"options": {
"strategy": "QUORUM",
"consistency": "QUORUM",
"quorum_n": 3,
"min_rows": 10,
"time_budget_ms": 3000
}
}
```

---

## 🧩 9. Operator Reference Matrix

| Operator | Description | Type | Example |
|-----------|--------------|------|----------|
| `$eq` | Equals | Any | `{"status":{"$eq":"active"}}` |
| `$ne` | Not equal | Any | `{"status":{"$ne":"inactive"}}` |
| `$gt` | Greater than | Numeric | `{"power":{"$gt":200}}` |
| `$gte` | Greater or equal | Numeric | `{"power":{"$gte":100}}` |
| `$lt` | Less than | Numeric | `{"power":{"$lt":500}}` |
| `$lte` | Less or equal | Numeric | `{"power":{"$lte":400}}` |
| `$regex` | Regular expression match | String | `{"name":{"$regex":"^A.*"}}` |
| `$and` | Logical AND | Array | `{"$and":[{"type":"solar"},{"status":"active"}]}` |
| `$or` | Logical OR | Array | `{"$or":[{"type":"solar"},{"type":"wind"}]}` |
| `$not` | Negation | Object | `{"$not":{"status":"inactive"}}` |
| `$in` | Value in list | Array | `{"type":{"$in":["solar","wind"]}}` |
| `$nin` | Value not in list | Array | `{"status":{"$nin":["inactive","deprecated"]}}` |
| `$exists` | Field existence | Boolean | `{"metadata":{"$exists":true}}` |
| `$between` | Range shorthand | Array | `{"power":{"$between":[100,300]}}` |

---

## 🧮 10. Combined Workflow Example

```bash
# Insert
curl -X POST http://localhost:18001/swarmkb/command -d '{"method":{"cmd":"crudput"},"dstype":"dsswres","criteria":[{"_id":"asset-1","type":"solar","status":"active"}]}'

# Query with merge
curl -X POST http://localhost:18003/swarmkb/command -d '{"method":{"cmd":"query"},"dstype":"dsswres","criteria":[{"type":"solar"}],"options":{"strategy":"LOCAL_THEN_REMOTE_MERGE","annotate_source":true}}'

# Trace
curl -X POST http://localhost:18003/swarmkb/command -d '{"method":{"cmd":"trace"},"args":["asset-1"]}'
```

---

**Last updated:** October 2025
Reflects new decentralized query strategies, operators, and provenance fields.

---

## Versioning & Images

- Recommended container image (personal GHCR):
`ghcr.io/georgegeorgakakos/optimusdb:latest`
- Prefer semantic tags (e.g., `v0.1.0`) alongside `latest` for reproducibility.

---

*Last updated: generated from current source tree. If you add or change handlers/routes, update this file accordingly.*

**© 2025 OptimusDB Research Team**
**License**: MPL 2.0
**Repository**: https://github.com/georgegeorgakakos/optimusdb