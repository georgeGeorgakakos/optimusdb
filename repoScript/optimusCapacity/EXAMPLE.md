# OptimusDB — Capacity Registry: Worked Example

A complete walk-through of the Capacity Provider → OptimusDB → RA flow, using one concrete capacity: a GPU-backed edge compute pool operated by ICCS in Athens.

```
1. CP  → OptimusDB : reserve a capID for a new capacity
2. CP  → OptimusDB : submit the CDT carrying that capID
3. CP  → RA        : capID goes into the RA configuration
4. RA  → OptimusDB : retrieve the CDT by capID
```

Each step below shows the raw HTTP call, the equivalent client command, and exactly what OptimusDB stores and returns.

**See also:** [`CAPACITY_API.md`](CAPACITY_API.md) for the full API reference, and the the `optimusCapacity` client (README.md) client.

---

## Contents

- [Scenario](#scenario)
- [Step 1 — CP requests a capID](#step-1--cp-requests-a-capid)
- [Step 2 — CP submits the CDT](#step-2--cp-submits-the-cdt)
- [Step 3 — capID into the RA configuration](#step-3--capid-into-the-ra-configuration)
- [Step 4 — RA retrieves the CDT](#step-4--ra-retrieves-the-cdt)
- [The whole thing as one command](#the-whole-thing-as-one-command)
- [Lifecycle recap](#lifecycle-recap)
- [Where everything lives](#where-everything-lives)
- [Things that trip people up](#things-that-trip-people-up)

---

## Scenario

| | |
|---|---|
| **Capacity Provider** | ICCS Edge Cluster — `did:swarm:cp-iccs-01` |
| **Capacity** | 64 vCPU / 256 GB / 4× NVIDIA A100, Athens edge site |
| **Resource Agent** | `did:swarm:ra-04` |
| **CP writes to** | `http://193.225.250.240/optimusdb1` |
| **RA reads from** | `http://193.225.250.240/optimusdb2` |

The CP and the RA deliberately talk to **different agents**. That is the point of the design: the RA never needs to know which agent issued the capID.

---

## Step 1 — CP requests a capID

### Over HTTP

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/api/v1/capacity/reserve \
  -H 'Content-Type: application/json' \
  -d '{
    "provider_id":   "did:swarm:cp-iccs-01",
    "provider_name": "ICCS Edge Cluster",
    "capacity_type": "compute",
    "capacity_name": "athens-edge-gpu-pool-3",
    "region":        "eu-gr-athens",
    "cdt_version":   "tosca_simple_yaml_1_3",
    "expected_ra":   "did:swarm:ra-04",
    "attributes": {
      "num_cpus":     64,
      "mem_size_mb":  262144,
      "gpu":          "A100",
      "gpu_count":    4
    }
  }'
```

| Field | Required | Notes |
|---|---|---|
| `provider_id` | **yes** | DID or any stable provider identifier |
| `capacity_type` | **yes** | `compute`, `storage`, `network`, … |
| `provider_name` | no | human-readable |
| `capacity_name` | no | the provider's own label |
| `region` | no | filterable later |
| `cdt_version` | no | TOSCA spec the CDT will use |
| `expected_ra` | no | which RA will consume this capacity |
| `attributes` | no | free-form; does not constrain the CDT schema |

### Same thing via the client

```bash
CAP=$(python3 capacity_client.py --url http://193.225.250.240/optimusdb1 \
  reserve --provider-id   did:swarm:cp-iccs-01 \
          --capacity-type compute \
          --capacity-name athens-edge-gpu-pool-3 \
          --region        eu-gr-athens \
          --expected-ra   did:swarm:ra-04 \
          --attr num_cpus=64 --attr gpu=A100 --attr gpu_count=4 \
          --quiet)

echo "$CAP"
```

`--quiet` prints only the identifier, so it captures cleanly into a shell variable. `--attr` values are typed automatically: `64` → int, `0.995` → float, `true` → bool, anything else stays a string.

### What OptimusDB returns — `201 Created`

```json
{
  "_id":        "cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90",
  "cap_id":     "cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90",
  "status":     "reserved",

  "provider_id":   "did:swarm:cp-iccs-01",
  "provider_name": "ICCS Edge Cluster",
  "capacity_type": "compute",
  "capacity_name": "athens-edge-gpu-pool-3",
  "region":        "eu-gr-athens",
  "cdt_version":   "tosca_simple_yaml_1_3",
  "expected_ra":   "did:swarm:ra-04",
  "attributes": {
    "num_cpus": 64, "mem_size_mb": 262144, "gpu": "A100", "gpu_count": 4
  },

  "issued_by":  "QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy",
  "issued_at":  "2026-09-11T10:22:31Z",
  "expires_at": "2026-09-12T10:22:31Z",

  "cdt_submitted": false,
  "store": "kbcapacity"
}
```

### Reading the capID

```
cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90
│    │                                              │
│    │                                              └─ UUIDv4: unique within this agent
│    └─ the issuing agent's libp2p peer ID
└─ fixed prefix
```

`issued_by` appears verbatim inside `cap_id`. A peer ID is a multihash of the node's public key and is therefore globally unique, so every agent issues only inside its own segment of the namespace — two agents cannot produce the same capID even in principle. No leader, no consensus round, no allocation table. An agent that is fully partitioned from the swarm still issues collision-free identifiers.

### State after step 1

A record now exists in the `kbcapacity` store with `status: reserved` and no CDT, and it starts replicating to the other agents immediately. The reservation is written at step 1 rather than deferred, so the identifier is verifiably taken the moment it is handed out.

---

## Step 2 — CP submits the CDT

### The YAML the CP writes

`athens_gpu_pool.yaml`:

```yaml
tosca_definitions_version: tosca_simple_yaml_1_3

metadata:
  template_name:    athens-edge-gpu-pool-3
  template_author:  ICCS Edge Cluster
  template_version: 1.0.0
  capability_id:    cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90
  provider_id:      did:swarm:cp-iccs-01

description: >
  GPU-backed edge compute pool operated by ICCS in Athens.
  Submitted against an OptimusDB-issued capID.

topology_template:
  node_templates:
    gpu_pool:
      type: tosca.nodes.Compute
      capabilities:
        host:
          properties:
            num_cpus:  64
            mem_size:  256 GB
            disk_size: 4 TB
        os:
          properties:
            architecture: x86_64
            type:         Linux
            distribution: Ubuntu
            version:      "22.04"
      properties:
        accelerator:       NVIDIA A100
        accelerator_count: 4
        region:            eu-gr-athens
        availability:      0.995

  policies:
    - placement:
        type: tosca.policies.Placement
        properties:
          region:        eu-gr-athens
          latency_class: edge
```

OptimusDB does **not** require `capability_id` inside the YAML — the binding is the capID in the URL path. Including it anyway makes the file self-describing: anyone reading it later knows which reservation it belongs to, and the file and the record cannot drift apart.

### Submitting it

```bash
python3 capacity_client.py submit-cdt "$CAP" --file athens_gpu_pool.yaml
```

The client reads `.yaml`, `.yml` or `.json` and posts the parsed document.

### Submitting it with curl

**The endpoint takes JSON, not raw YAML.** Convert first:

```bash
python3 -c "import yaml,json,sys; print(json.dumps(yaml.safe_load(open(sys.argv[1]))))" \
  athens_gpu_pool.yaml \
| curl -s -X POST "http://193.225.250.240/optimusdb1/api/v1/capacity/$CAP/cdt" \
    -H 'Content-Type: application/json' --data-binary @-
```

A `{"cdt": {...}}` wrapper is also accepted if that suits your tooling better.

### Where the YAML ends up

| | |
|---|---|
| **Store** | `kbcapacity` |
| **Document `_id`** | the capID |
| **Field** | `cdt` |

Not a separate document and not a separate store — the CDT is folded into the reservation record created at step 1. One `_id`, one lookup, no join.

### The record after step 2

```json
{
  "_id":    "cap-QmaqAyTi...-8f14e45f-...",
  "cap_id": "cap-QmaqAyTi...-8f14e45f-...",
  "status": "active",

  "cdt_submitted":    true,
  "cdt_submitted_at": "2026-09-11T10:41:07Z",
  "updated_at":       "2026-09-11T10:41:07Z",

  "cdt": {
    "tosca_definitions_version": "tosca_simple_yaml_1_3",
    "metadata": {
      "template_name": "athens-edge-gpu-pool-3",
      "capability_id": "cap-QmaqAyTi...-8f14e45f-..."
    },
    "description": "GPU-backed edge compute pool operated by ICCS in Athens...",
    "topology_template": {
      "node_templates": { "gpu_pool": { "type": "tosca.nodes.Compute", "...": "..." } },
      "policies": [ { "placement": { "...": "..." } } ]
    }
  },

  "provider_id": "did:swarm:cp-iccs-01",
  "issued_by":   "QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy"
}
```

### About the kbcapacity store

`kbcapacity` is a **dynamic store** — the agent created it on the first reservation, so it has no entry in `initPeer.go`. Two useful consequences: it appears in `GET /api/v1/stores`, and it is included in `POST /api/v1/exchange/export` automatically.

```bash
curl -s http://193.225.250.240/optimusdb1/api/v1/stores \
  | jq '.stores[] | select(.name=="kbcapacity")'
```

```json
{ "name": "kbcapacity", "kind": "dynamic", "address": "/orbitdb/bafyrei.../kbcapacity" }
```

Because it is an ordinary document store, the normal CRUD API reaches it too:

```bash
curl -s -X POST http://193.225.250.240/optimusdb1/swarmkb/command \
  -H 'Content-Type: application/json' \
  -d "{\"method\":{\"cmd\":\"crudget\"},\"dstype\":\"kbcapacity\",\"criteria\":[{\"_id\":\"$CAP\"}]}"
```

---

## Step 3 — capID into the RA configuration

No API call. The CP hands the string to the RA however it normally does.

**As a config file:**

```yaml
# ra-04.config.yaml
resource_agent:
  id: did:swarm:ra-04
  optimusdb_endpoint: http://193.225.250.240/optimusdb2

  managed_capacities:
    - capability_id: cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90
      provider_id:   did:swarm:cp-iccs-01
      region:        eu-gr-athens
```

**As environment variables:**

```bash
export CAPABILITY_ID=cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-ea2d-4c1b-9f3a-7b2e1d5c8a90
export OPTIMUSDB_URL=http://193.225.250.240/optimusdb2
```

Note `optimusdb2`. The RA points at a **different agent** than the CP used, and needs no knowledge of which agent issued the identifier.

### If the identifiers are too long for your config

Full peer IDs make for long strings. `app.CapIDPeerLen` in `app/app.go` defaults to `0` (embed the full peer ID); set it to `12` and identifiers become far shorter. The trade-off is that uniqueness reverts from a structural guarantee to a probabilistic one, since two agents could share a suffix and would then depend on the UUID alone. The constant is documented with that trade-off.

---

## Step 4 — RA retrieves the CDT

### Just the CDT

```bash
curl -s "http://193.225.250.240/optimusdb2/api/v1/capacity/$CAPABILITY_ID?cdt_only=true"
```

Returns the TOSCA document exactly as submitted — ready to hand to the orchestrator without unwrapping.

### Full record, with provenance

```bash
curl -s "http://193.225.250.240/optimusdb2/api/v1/capacity/$CAPABILITY_ID" | jq
```

```json
{
  "reservation": {
    "cap_id":        "cap-QmaqAyTi...",
    "status":        "active",
    "provider_id":   "did:swarm:cp-iccs-01",
    "provider_name": "ICCS Edge Cluster",
    "capacity_type": "compute",
    "region":        "eu-gr-athens",
    "expected_ra":   "did:swarm:ra-04",
    "issued_by":     "QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy",
    "issued_at":     "2026-09-11T10:22:31Z",
    "cdt_submitted": true,
    "cdt": { "tosca_definitions_version": "tosca_simple_yaml_1_3", "...": "..." }
  },
  "expired": false
}
```

The RA gets more than the template: `provider_id` and `issued_by` give it provenance, `expected_ra` lets it confirm the capacity was meant for it.

### With the client

```bash
python3 capacity_client.py --url http://193.225.250.240/optimusdb2 \
  get "$CAPABILITY_ID" --cdt-only --wait 60
```

### From the RA's own code

```python
import os, logging
from capacity_client import CapacityClient, CapacityNotFound

log = logging.getLogger("ra")
ra = CapacityClient(os.environ["OPTIMUSDB_URL"])
cap_id = os.environ["CAPABILITY_ID"]

try:
    # Poll rather than assume: the CP wrote to another agent and CRDT
    # replication is not instantaneous.
    ra.wait_for(cap_id, timeout=60, require_cdt=True)
    cdt = ra.get_cdt(cap_id)
    log.info("CDT for %s has %d top-level keys", cap_id, len(cdt))
except CapacityNotFound:
    log.error("capID %s not available on this agent", cap_id)
```

**Use `wait_for`, not a bare `get`.** A 404 means *not on this agent yet*, not "does not exist". The CP wrote to agent 1, the RA reads from agent 2, and replication takes a few seconds. Treating that as a hard failure produces intermittent, confusing errors in the RA.

### Other queries the RA can run

```bash
# everything this RA is expected to manage
curl -s "http://193.225.250.240/optimusdb2/api/v1/capacity?expected_ra=did:swarm:ra-04" | jq

# all active compute capacity in Athens
curl -s "http://193.225.250.240/optimusdb2/api/v1/capacity?capacity_type=compute&region=eu-gr-athens&status=active" | jq '.count'
```

---

## The whole thing as one command

```bash
python3 capacity_client.py flow \
  --url           http://193.225.250.240/optimusdb1 \
  --provider-id   did:swarm:cp-iccs-01 \
  --capacity-type compute \
  --capacity-name athens-edge-gpu-pool-3 \
  --region        eu-gr-athens \
  --expected-ra   did:swarm:ra-04 \
  --attr num_cpus=64 --attr gpu=A100 \
  --file          athens_gpu_pool.yaml \
  --ra-url        http://193.225.250.240/optimusdb2
```

```
[1] CP -> OptimusDB: reserve a capID
    capID     cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-...
    issued by QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy
    ✓ capID carries the issuing peer ID — unique by construction

[2] CP -> OptimusDB: submit the CDT
    status active  cdt_submitted True

[3] CP -> RA: capID goes into the RA configuration
    capID = cap-QmaqAyTizLPzFSsDxNnteTGHZf3o5CVt9NfpSVDMSYbEZy-8f14e45f-...

[4] RA -> OptimusDB: retrieve the CDT by capID
    reading from http://193.225.250.240/optimusdb2 (different agent)
    ✓ replicated across agents
    CDT retrieved, 4 top-level key(s)
```

Steps 1 and 2 go to agent 1, step 4 reads from agent 2. Add `--cleanup` to release the capID afterwards when using this as a smoke test.

---

## Lifecycle recap

| Step | Store state | `status` | `cdt_submitted` |
|---|---|---|---|
| after 1 | record created in `kbcapacity`, `_id` = capID | `reserved` | `false` |
| after 2 | same record, `cdt` field populated | `active` | `true` |
| 3 | unchanged — no API call | `active` | `true` |
| 4 | read only | `active` | `true` |
| release | record kept, never reissued | `released` | unchanged |

`expires_at` is `issued_at + 24h`. Expiry is **advisory** — nothing is deleted, so a late CDT still binds. Reservations that never received one show up as orphans:

```bash
python3 capacity_client.py list --orphans
```

```bash
curl -s "http://193.225.250.240/optimusdb1/api/v1/capacity?status=reserved" \
  | jq '[.reservations[] | select(.cdt_submitted == false)] | length'
```

Release keeps the record rather than deleting it, so the capID is never reissued and an RA still holding it gets `"status": "released"` instead of a bare 404.

---

## Where everything lives

| What | Where |
|---|---|
| Reservation + CDT | `kbcapacity` document store, `_id` = capID |
| Store type | dynamic — created on first reservation, no `initPeer.go` entry |
| Replication | CRDT, to every agent that opens the store |
| Backup | included in `POST /api/v1/exchange/export` as `orbitdb/kbcapacity.jsonl` |
| Go implementation | `app/app.go` (types), `app/service.go` (logic), `api/http.go` (HTTP) |
| Python client | `capacity_client.py` |

---

## Things that trip people up

**The CDT endpoint takes JSON, not YAML.** The client converts for you; curl users must convert first. If you would rather POST YAML directly, that is a short addition to `capacityCDTHandler` — sniff `Content-Type: application/x-yaml` and unmarshal accordingly.

**A 404 from the RA side is usually replication lag.** Use `--wait` or `wait_for()`. Only treat it as fatal after the window expires.

**`/api/v1/capacity` has no dedicated ingress route.** Like `/api/v1/stores` and `/api/v1/exchange`, it works through the per-node path (`/optimusdb1`, `/optimusdb2`) but not the load-balanced root. Point clients at a specific node, or add a Traefik `IngressRoute` mirroring the `/api/v1/chat` one.

**`kbcapacity` will not appear in the store listing until the first reservation.** That is expected — the agent creates it on demand.

**The capID in the YAML is documentation, not the binding.** The binding is the URL path. If they disagree, the URL wins.
