# optimusCapacity

External Python client for the **OptimusDB Capacity Registry** — the agent-issued identifier service used by Capacity Providers and Resource Agents.

```
1. CP  → OptimusDB : reserve a capID for a new capacity
2. CP  → OptimusDB : submit the CDT carrying that capID
3. CP  → RA        : capID goes into the RA configuration
4. RA  → OptimusDB : retrieve the CDT by capID
```

The capID is issued by the OptimusDB agent and embeds that agent's libp2p peer ID, so it is unique across every deployed agent **by construction**. This client never generates, negotiates or deduplicates identifiers — it asks and receives one.

---

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [CLI reference](#cli-reference)
- [Library use](#library-use)
- [The full flow in one command](#the-full-flow-in-one-command)
- [Replication and the 404 you should not panic about](#replication-and-the-404-you-should-not-panic-about)
- [Error handling](#error-handling)
- [Files](#files)

---

## Install

**Prerequisites:** Python 3.8+, and network access to an OptimusDB agent running a build that includes `/api/v1/capacity`.

```bash
./setup.sh                    # Linux / macOS
.\setup.ps1                   # Windows PowerShell
```

Or manually:

```bash
pip3 install -r requirements.txt
```

`requests` is required. `PyYAML` is only needed if you pass CDTs as `.yaml` rather than `.json`.

---

## Quick start

```bash
# point at your agent (or set OPTIMUSDB_URL)
export OPTIMUSDB_URL=http://localhost:18001

python3 capacity_client.py health
```

```json
{
  "url": "http://localhost:18001",
  "peer_id": "QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy",
  "role": "Coordinator",
  "addresses": ["/ip4/10.42.0.17/tcp/4001"],
  "capacity_api": true,
  "kbcapacity_store": false
}
```

`kbcapacity_store: false` on a fresh agent is normal — the store is created on the first reservation.

---

## CLI reference

```
python3 capacity_client.py [--url URL] [--timeout N] [--log-level LEVEL] COMMAND
```

| Option | Default | Notes |
|---|---|---|
| `--url` | `$OPTIMUSDB_URL` or `http://193.225.250.240/optimusdb1` | agent base URL |
| `--timeout` | `30` | seconds per request |
| `--log-level` | `INFO` | `DEBUG` shows every request |

### Step 1 — reserve

```bash
python3 capacity_client.py reserve \
  --provider-id   did:swarm:cp-iccs-01 \
  --capacity-type compute \
  --provider-name "ICCS Edge Cluster" \
  --capacity-name athens-edge-gpu-pool-3 \
  --region        eu-gr-athens \
  --expected-ra   did:swarm:ra-04 \
  --attr num_cpus=64 --attr mem_size_mb=262144 --attr gpu=A100
```

`--provider-id` and `--capacity-type` are required. `--attr` is repeatable and typed automatically: `64` → int, `0.995` → float, `true` → bool, everything else stays a string.

Capture just the identifier for shell use:

```bash
CAP=$(python3 capacity_client.py reserve \
        --provider-id did:swarm:cp-01 --capacity-type compute --quiet)
```

### Step 2 — submit the CDT

```bash
python3 capacity_client.py submit-cdt "$CAP" --file sample_cdt.yaml
```

Accepts `.yaml`, `.yml` or `.json`. Status flips from `reserved` to `active`.

### Step 4 — retrieve

```bash
python3 capacity_client.py get "$CAP"                  # full record
python3 capacity_client.py get "$CAP" --cdt-only       # just the CDT
python3 capacity_client.py get "$CAP" --wait 60        # poll through replication lag
```

### List and filter

```bash
python3 capacity_client.py list --capacity-type compute --status active
python3 capacity_client.py list --provider-id did:swarm:cp-iccs-01
python3 capacity_client.py list --orphans      # reserved but never given a CDT
```

Filters: `--provider-id`, `--capacity-type`, `--region`, `--status`, `--expected-ra`, `--issued-by`. Exact match, case-insensitive.

### Release

```bash
python3 capacity_client.py release "$CAP"
```

The record is kept rather than deleted, so the capID is never reissued and an RA still holding it gets `"status": "released"` instead of a bare 404.

---

## Library use

```python
from capacity_client import CapacityClient, CapacityNotFound

cp = CapacityClient("http://localhost:18001")

# step 1
res = cp.reserve(
    "did:swarm:cp-iccs-01", "compute",
    region="eu-gr-athens",
    expected_ra="did:swarm:ra-04",
    attributes={"num_cpus": 64, "gpu": "A100"},
)
cap_id = res["cap_id"]

# step 2
cp.submit_cdt(cap_id, {"tosca_definitions_version": "tosca_simple_yaml_1_3", ...})

# step 4 — from a different agent
ra = CapacityClient("http://localhost:18002")
try:
    ra.wait_for(cap_id, timeout=60, require_cdt=True)
    cdt = ra.get_cdt(cap_id)
except CapacityNotFound:
    ...  # not replicated within the window
```

| Method | Purpose |
|---|---|
| `reserve(provider_id, capacity_type, **kw)` | step 1 |
| `submit_cdt(cap_id, cdt)` | step 2 |
| `get(cap_id)` | full record + `expired` flag |
| `get_cdt(cap_id)` | CDT body only |
| `wait_for(cap_id, timeout, require_cdt)` | poll through replication lag |
| `list(**filters)` / `orphans()` | query |
| `release(cap_id)` | withdraw |
| `agent()` / `stores()` / `health()` | diagnostics |

---

## The full flow in one command

`flow` runs all four steps, and with `--ra-url` it reads back from a **different agent** — which is the part that demonstrates the capID resolves cluster-wide rather than only where it was issued.

```bash
python3 capacity_client.py flow \
  --url           http://localhost:18001 \
  --provider-id   did:swarm:cp-iccs-01 \
  --capacity-type compute \
  --region        eu-gr-athens \
  --attr num_cpus=64 --attr gpu=A100 \
  --file          sample_cdt.yaml \
  --ra-url        http://localhost:18002
```

```
[1] CP -> OptimusDB: reserve a capID
    capID     cap-QmaqAyTi…bEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90
    issued by QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy
    ✓ capID carries the issuing peer ID — unique by construction

[2] CP -> OptimusDB: submit the CDT
    status active  cdt_submitted True

[3] CP -> RA: capID goes into the RA configuration
    capID = cap-QmaqAyTi…bEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90

[4] RA -> OptimusDB: retrieve the CDT by capID
    reading from http://localhost:18002 (different agent)
    ✓ replicated across agents
    CDT retrieved, 4 top-level key(s)
```

Add `--cleanup` to release the capID when it finishes — useful when running this repeatedly as a smoke test.

---

## Replication and the 404 you should not panic about

Reservations are CRDT-replicated between agents, which is not instantaneous. A `404` from `get()` means **not on this agent**, not "does not exist". The client models that distinction explicitly:

- `CapacityNotFound` is a subclass of `CapacityError`, raised only on 404
- `wait_for()` polls until the record appears, with `require_cdt=True` if you also need the CDT bound
- the CLI prints a hint suggesting `--wait` or querying the issuing agent

In practice: the CP writes to whichever agent it talks to, and the RA may read from another. Use `wait_for()` on the RA side rather than a bare `get()`.

---

## Error handling

| Exit code | Meaning |
|---|---|
| `0` | success |
| `2` | agent error — validation, unreachable, store failure |
| `4` | capID not found on the agent queried |
| `130` | interrupted |

```python
from capacity_client import CapacityError, CapacityNotFound

try:
    cp.reserve("", "compute")
except CapacityError as exc:
    print(exc.status)   # 400
    print(exc.payload)  # {"error": "provider_id is required"}
```

---

## Files

```
optimusCapacity/
├── capacity_client.py   # client library + CLI
├── sample_cdt.yaml      # example Capacity Description Template
├── requirements.txt
├── setup.sh             # Linux / macOS bootstrap
├── setup.ps1            # Windows bootstrap
└── README.md
```

---

## Notes

**Ingress.** Like `/api/v1/stores` and `/api/v1/exchange`, `/api/v1/capacity` has no dedicated Traefik route in the K3s manifest. It works through the per-node path — `http://193.225.250.240/optimusdb1` — but not through the load-balanced root. Point `--url` at a specific node.

**Relationship to optimusPy.** This is a standalone client for one API, in the spirit of [`optimusTrust`](https://github.com/georgeGeorgakakos/optimusTrust). If you would rather fold it into [`optimusPy`](https://github.com/georgeGeorgakakos/optimusPy), the four `CapacityClient` methods drop into `optimusdb_client.py` unchanged — they only need that class's `_get`/`_post` helpers.

**Backup.** Reservations live in the `kbcapacity` document store, which the agent creates on first use and which is included in `POST /api/v1/exchange/export` automatically.

## License

MIT
