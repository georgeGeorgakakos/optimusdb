# optimusCapacity — Installation

Client for the OptimusDB Capacity Registry. Pure Python, one required dependency, no compiled extensions.

---

## Requirements

| | |
|---|---|
| Python | 3.8 or newer |
| Required | `requests>=2.28.0` |
| Optional | `PyYAML>=6.0` — only for `.yaml` CDTs; skip it if you pass `.json` |
| Network | HTTP access to an OptimusDB agent running a build that includes `/api/v1/capacity` |

No database, no message broker, no libp2p. The client speaks HTTP and nothing else.

---

## Quickest path

### Linux / macOS

```bash
./setup.sh
```

Creates `.venv`, installs the package in editable mode, copies `.env.example` to `.env`.

```bash
./setup.sh --dev        # also installs pytest
./setup.sh --no-venv    # install into the current environment
```

### Windows

```powershell
Set-ExecutionPolicy -Scope Process -Bypass   # only if scripts are blocked
.\setup.ps1
.\setup.ps1 -Dev
```

### Verify

```bash
optimus-capacity --version
optimus-capacity --url http://localhost:18001 health
```

Both entry points work — `optimus-capacity` after install, or `python3 capacity_client.py` directly from the folder.

---

## Manual install

### With pip

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows: .\.venv\Scripts\Activate.ps1
pip install -e ".[yaml]"
```

### Dependencies only, no package install

```bash
pip3 install -r requirements.txt
python3 capacity_client.py health
```

Use this on a locked-down host where you can't install packages — the script runs standalone from the folder.

### Offline / air-gapped

On a machine with network access:

```bash
pip download -r requirements.txt -d wheels/
```

Copy `wheels/` and the project folder across, then:

```bash
pip install --no-index --find-links=wheels/ -r requirements.txt
```

---

## Docker

```bash
docker build -t optimus-capacity .

docker run --rm -e OPTIMUSDB_URL=http://193.225.250.240/optimusdb1 \
  optimus-capacity health

docker run --rm -e OPTIMUSDB_URL=http://193.225.250.240/optimusdb1 \
  optimus-capacity reserve --provider-id did:swarm:cp-01 --capacity-type compute
```

Submitting a CDT means mounting it:

```bash
docker run --rm -v "$(pwd)/samples:/cdt" \
  -e OPTIMUSDB_URL=http://193.225.250.240/optimusdb1 \
  optimus-capacity submit-cdt <capID> --file /cdt/athens_gpu_pool.yaml
```

If the agent runs in the same Docker network, use the service name and skip the ingress entirely:

```bash
docker run --rm --network=swarmnet \
  -e OPTIMUSDB_URL=http://optimusdb1:8089 optimus-capacity health
```

The image runs as UID 10001, not root — the client only makes outbound HTTP calls.

---

## Configuration

`OPTIMUSDB_URL` is the only variable the client itself reads. Everything else is for the Makefile targets and tests.

```bash
cp .env.example .env
set -a && . ./.env && set +a
```

| Variable | Used by | Purpose |
|---|---|---|
| `OPTIMUSDB_URL` | client, Makefile | Agent the CP writes to |
| `OPTIMUSDB_RA_URL` | Makefile | Second agent, for the RA side of `make demo` |
| `OPTIMUS_TEST_URL` | tests | Enables `pytest -m integration` |
| `PROVIDER_ID`, `CAPACITY_TYPE`, `REGION`, `EXPECTED_RA` | Makefile | Demo defaults |

`--url` on the command line always wins over the environment.

**Use a per-node URL.** `/api/v1/capacity` has no dedicated Traefik `IngressRoute` in the K3s manifest, so it is reachable through `http://host/optimusdbN` but **not** through the bare host. `http://193.225.250.240/api/v1/capacity` returns 404; `http://193.225.250.240/optimusdb1/api/v1/capacity` works.

---

## Testing the install

```bash
make test
```

Forty tests against an in-process stub agent — no OptimusDB needed. Covers the whole client surface including the error paths and replication-lag handling.

Against a live agent:

```bash
make test-integration URL=http://localhost:18001
```

End-to-end demo:

```bash
make demo URL=http://localhost:18001 RA=http://localhost:18002
```

---

## Make targets

```
make install           create .venv and install
make install-dev       install with pytest
make test              stub-server test suite
make test-cov          tests with coverage
make test-integration  tests against a live agent
make lint              byte-compile everything
make health            check the agent and the capacity API
make stores            list the agent's document stores
make reserve           issue a capID and print it
make demo              full CP -> OptimusDB -> RA flow
make list              list reservations
make orphans           reservations with no CDT attached
make clean             remove build and test artefacts
```

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'requests'`**
The venv isn't active. `source .venv/bin/activate`, or run `./setup.sh --no-venv`.

**`PyYAML is required for YAML CDTs`**
Install the extra — `pip install -e ".[yaml]"` — or convert the CDT to JSON first.

**`cannot reach http://.../api/v1/capacity`**
Check the agent is up: `curl -s $OPTIMUSDB_URL/swarmkb/agent/status | jq .agent.peer_id`. If that works but capacity doesn't, the agent is running a build without the capacity API.

**404 on `/api/v1/capacity` through the ingress**
Missing Traefik route. Use the per-node path `http://host/optimusdbN/...`.

**`capID not found on this agent`**
Usually replication lag, not a missing record. The CP wrote to one agent and you're reading from another. Add `--wait 60`, or use `wait_for()` in code.

**`optimus-capacity: command not found` after install**
The venv isn't on PATH. Activate it, or call the script directly: `python3 capacity_client.py`.

**Tests fail with `PytestUnknownMarkWarning`**
An old pytest reading a different config. Confirm with `pytest --version` that it's ≥7.0 and that `pyproject.toml` is being picked up.

---

## Files

```
optimusCapacity/
├── capacity_client.py        client library + CLI
├── pyproject.toml            packaging, deps, console script, pytest config
├── requirements.txt          runtime dependencies
├── requirements-dev.txt      + pytest
├── setup.sh / setup.ps1      bootstrap scripts
├── Makefile                  common tasks
├── Dockerfile                containerised client
├── .env.example              configuration template
├── .gitignore
├── LICENSE                   MIT
├── README.md                 client guide and CLI reference
├── EXAMPLE.md                four-step CP -> RA walkthrough
├── INSTALL.md                this file
├── CHANGELOG.md
├── samples/
│   ├── athens_gpu_pool.yaml       compute CDT
│   └── budapest_storage.yaml      storage CDT
└── tests/
    └── test_capacity_client.py    40 tests, no agent required
```

---

## Uninstall

```bash
pip uninstall optimus-capacity
rm -rf .venv
```
