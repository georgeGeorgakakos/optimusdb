# 🧠 OptimusDB – Decentralized Knowledge Base & Data Catalog

> **OptimusDB** is a decentralized, hybrid database and data catalog engine combining **OrbitDB**, **IPFS**, and **SQLite** for peer-to-peer metadata management, trust synchronization, and TOSCA-based knowledge ingestion.

---

## 🚀 Overview

OptimusDB unifies decentralized storage and query execution across multiple agents.
Each agent acts as a self-contained node hosting:
- Data Store (Berty) for decentralized replication
- SQLite for local metadata persistence
- IPFS for dataset and YAML content storage
- LibP2P PubSub for leader election, peer discovery, and reputation synchronization

All interactions are exposed via a **unified REST endpoint** (`/swarmkb/command`) that executes SQL-like, CRUD, and graph operations both locally and across peers.

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────┐
│                  OptimusDB                 │
│────────────────────────────────────────────│
│ Data Store (Decentralized Docstore)        │
│ SQLite (Local metadata, Data Catalog)      │
│ IPFS (Object/YAML storage)                 │
│ LibP2P PubSub (Discovery, Election)        │
│ EMS/ActiveMQ (External message integration)│
└────────────────────────────────────────────┘
▲                 ▲
┌───────────┘                 └──────────────┐
│                                            │
┌─────────────┐                             ┌─────────────┐
│ Agent A     │                             │ Agent B     │
│ localhost:18001                          │ localhost:18002
│ OrbitDB ⟷ SQLite ⟷ IPFS                  │ OrbitDB ⟷ SQLite ⟷ IPFS
└─────────────┘                             └─────────────┘
↖────── decentralized sync ─────↗
```

---

## 🗃️ Data Catalog Schema

Each node maintains a synchronized SQLite table `datacatalog`:

| Column | Type | Description |
|--------|------|-------------|
| `_id` | `VARCHAR(36)` | Primary unique identifier |
| `author` | `VARCHAR(255)` | Originating node or user |
| `metadata_type` | `VARCHAR(255)` | Type of metadata (dataset, service, etc.) |
| `component` | `VARCHAR(255)` | Logical component name |
| `behaviour` | `VARCHAR(255)` | Function or action associated |
| `relationships` | `TEXT` | JSON defining relationships between components |
| `associated_id` | `VARCHAR(36)` | Link to related object |
| `name` | `VARCHAR(255)` | Human-readable name |
| `description` | `TEXT` | Description of the asset |
| `tags` | `VARCHAR(255)` | Comma-separated labels |
| `status` | `VARCHAR(50)` | Current record state |
| `created_by` | `VARCHAR(255)` | Creator identity |
| `created_at` | `TIMESTAMP` | Creation time |
| `updated_at` | `TIMESTAMP` | Last update |
| `related_ids` | `VARCHAR(255)` | Related record IDs |
| `priority` | `VARCHAR(50)` | Priority level |
| `scheduling_info` | `VARCHAR(255)` | Scheduling hints |
| `sla_constraints` | `VARCHAR(255)` | SLA or QoS metadata |
| `ownership_details` | `VARCHAR(255)` | Owner or custodian |
| `audit_trail` | `VARCHAR(255)` | Reference to audit history |

---

## 🌐 REST API Reference

### 1. `POST /swarmkb/command`
Unified command dispatcher for all local and remote operations.

#### Request
```json
{
"method": {
"argcnt": 2,
"cmd": "sqldml"
},
"args": ["param1", "param2"],
"dstype": "dsswres",
"criteria": [ { "field": "key", "value": "A1" } ],
"UpdateData": [ { "field": "status", "value": "active" } ],
"graph_Traversal": [ { "depth": 2 } ],
"sqldml": "SELECT * FROM datacatalog WHERE status='active'"
}
```

#### Response
```json
{
"status": "success",
"records": [
{
"_id": "9e12d43f-...",
"name": "SolarAssetCatalog",
"component": "RenewableMonitor",
"status": "active"
}
]
}
```

---

### 2. `POST /swarmkb/upload-tosca`
Upload a Base64-encoded TOSCA YAML file for parsing, IPFS persistence, and OrbitDB replication.

#### Request
```json
{
"file": "Base64-encoded YAML",
    "filename": "renewable_topology.yaml"
    }
    ```

    #### Response
    ```json
    {
    "message": "TOSCA uploaded successfully",
    "template_id": "sha256-hash",
    "node_count": 12,
    "filename": "renewable_topology.yaml",
    "filesize": 2456,
    "sha256": "ab12cd..."
    }
    ```

    ---

    ### 3. `GET /swarmkb/peers`
    Lists discovered LibP2P peers.

    #### Response
    ```json
    [
    { "ID": "Qm123...", "Addrs": ["/ip4/127.0.0.1/tcp/4001"] }
    ]
    ```

    ---

    ### 4. `GET /swarmkb/logs?date=YYYY-MM-DD&hour=HH`
    Fetches logs for a given time slot from the local `LoggerSQLite`.

    ---

    ### 5. `GET /swarmkb/benchmarks`
    Aggregates benchmark results from all known peers for decentralized performance evaluation.

    ---

    ## 🧩 Example Test Flow (PowerShell)

    ```powershell
    # Insert record into Agent 1
    $insertPayload = @{
    method = @{ argcnt = 2; cmd = "sqldml" }
    args = @("dummy1", "dummy2")
    dstype = "dsswres"
    sqldml = "INSERT INTO datacatalog (_id,name,status) VALUES ('1','SolarNode','active')"
    }
    Invoke-RestMethod -Uri "http://localhost:18001/swarmkb/command" -Method Post -Body ($insertPayload | ConvertTo-Json -Depth 10) -ContentType "application/json"

    # Query from Agent 3
    $queryPayload = @{
    method = @{ argcnt = 2; cmd = "sqldml" }
    dstype = "dsswres"
    sqldml = "SELECT * FROM datacatalog WHERE status='active'"
    }
    Invoke-RestMethod -Uri "http://localhost:18003/swarmkb/command" -Method Post -Body ($queryPayload | ConvertTo-Json -Depth 10) -ContentType "application/json"
    ```

    ---

    ## ⚙️ Build & Run

    ### 🐳 Docker
    ```bash
    docker build -t optimusdb:latest .
    docker run -p 18001:18001 -e AGENT_NAME=agent1 optimusdb:latest
    ```

    ### 🧰 Local (Go)
    ```bash
    go mod tidy
    go run main.go --port 18001 --context swarmkb
    ```

    ---

    ## 🔄 Integration & Extensibility

    - **OrbitDB / IPFS**: Decentralized data and TOSCA YAML storage
    - **SQLite**: Local catalog persistence (`datacatalog`, `tosca_metadata`, `logger`)
    - **PubSub Election**: Leader election, heartbeat, and reputation sync
    - **ActiveMQ**: Optional EMS integration for external message ingestion
    - **Node-RED & OptimusVis UI**: Visual flow and decentralized telemetry dashboard

    ---

    ## 🧪 Health Check

    ```bash
    curl http://localhost:18001/swarmkb/peers
    curl http://localhost:18001/swarmkb/benchmarks
    curl -X POST http://localhost:18001/swarmkb/command -d '{"method":{"cmd":"sqldml"},"sqldml":"SELECT 1"}' -H "Content-Type: application/json"
    ```

    ---

    ## 📜 License

    This project is licensed under the **Apache 2.0 License**.
    (c) 2025 — George Georgakakos and collaborators (OptimusDB / Swarmchestrate).

    ---

    ## ✨ Authors & References

    - **George Georgakakos** – [@georgeGeorgakakos](https://github.com/georgeGeorgakakos)
    - **OptimusDB / Swarmchestrate** – decentralized metadata and knowledge federation project

    **© 2025 OptimusDB Research Team**
    **License**: MPL 2.0
    **Repository**: https://github.com/georgegeorgakakos/optimusdb