# OptimusDB — Dynamic Datastores: Test Guide

**Applies to:** the seven-file change that removed the `default: → dsswres` fallback and made store names open-ended.
**Scope:** OptimusDB agent, plus downstream impact on [`optimusTrust`](https://github.com/georgeGeorgakakos/optimusTrust) and [`optimusPy`](https://github.com/georgeGeorgakakos/optimusPy).

---

## Contents

- [1. What changed](#1-what-changed)
- [2. Prerequisites](#2-prerequisites)
- [3. Test matrix](#3-test-matrix)
- [4. Detailed scenarios](#4-detailed-scenarios)
- [5. Impact on optimusTrust](#5-impact-on-optimustrust)
- [6. Impact on optimusPy](#6-impact-on-optimuspy)
- [7. Migrating data stranded in dsswres](#7-migrating-data-stranded-in-dsswres)
- [8. Regression checklist](#8-regression-checklist)
- [9. Troubleshooting](#9-troubleshooting)

---

## 1. What changed

**Before.** Every CRUD path carried its own `switch strings.ToLower(dstype)` ending in `default:` → `DsSWres`. An unknown `dstype` was accepted on write *and* on read, so the round-trip succeeded and the caller never learned the data had gone somewhere else. A typo produced no error. `optimusTrust` writing `dstype=kbtrust` had been writing into `dsswres` the whole time.

**After.** One resolver, `ResolveDocStore`, serves every path. Store names are not fixed at compile time — naming a store is how you create it. The first request that mentions a new name materialises a real OrbitDB docstore: replicated, queryable, listed by `GET /api/v1/stores`, included in export/import, reopened after restart.

A `dstype` is never silently redirected to another store.

| Path | Function | Unknown store |
|---|---|---|
| `crudput` | `crudPutDocStoreRev` | created, document written |
| `crudget` | `crudGetDocStoreRev` | created, `[]` returned |
| `crudupdate` | `crudUpdateDocStoreRev` | created, 0 matched |
| `cruddelete` | `crudDeleteDocStoreRev` | created, 0 removed |
| query routing | `resolveDocStoreByType` | created |
| unified query | `unifiedQueryDocStore` | created |
| file upload | `resolveTargetStore` | created, file ingested |
| semantic hydration | `FetchDocument` | created, fills by replication |
| chat pipeline | `createKBQueryFunc` | created |

`dstype: ""` still resolves to `dsswres`. That is a documented default for clients that omit the field, not a fallback.

---

## 2. Prerequisites

```bash
go build ./...                    # must be clean
docker ps                          # or: kubectl -n optimusddc get pods
```

Set a base URL for the rest of this guide:

```bash
# Docker Desktop
export A=http://localhost:18001
export B=http://localhost:18002
export D=http://localhost:18004

# K3s
# export A=http://193.225.250.240/optimusdb1
# export B=http://193.225.250.240/optimusdb2
```

Health check:

```bash
curl -s $A/swarmkb/agent/status | jq '{role,peerID}'
```

---

## 3. Test matrix

| # | Scenario | Command shape | Expected |
|---|---|---|---|
| 1 | Baseline store list | `GET /api/v1/stores` | 11 built-in, 0 dynamic |
| 2 | Implicit create on write | `crudput dstype=kbtrust` | 200, store created |
| 3 | Isolation from dsswres | `crudget dstype=dsswres` | 0 matches for the trust doc |
| 4 | Read back from new store | `crudget dstype=kbtrust` | 1 document |
| 5 | Explicit create | `POST /api/v1/stores` | 201 + address |
| 6 | Read from non-existent | `crudget dstype=neverseen` | 200, `[]`, store created |
| 7 | Update non-existent | `crudupdate dstype=neverseen2` | 0 matched, store created |
| 8 | Delete non-existent | `cruddelete dstype=neverseen3` | 0 removed, store created |
| 9 | Invalid name | `dstype="My Store"` | 400 with naming rule |
| 10 | Reserved name | `dstype=contributions` | 400, EventLog message |
| 11 | Path traversal | `dstype=../etc` | 400, rejected |
| 12 | Upload to new store | `POST /swarmkb/upload` + `dstype` | file ingested |
| 13 | Config persistence | inspect `<repo>_config` | `dynamicStoreAddrs` populated |
    | 14 | Restart survival | restart container, list stores | `kbtrust` still present |
    | 15 | Export coverage | `POST /api/v1/exchange/export` | `orbitdb/kbtrust.jsonl` present |
    | 16 | Import onto fresh node | `POST .../import` on node 4 | store created, docs restored |
    | 17 | Cross-peer replication | compare addresses on A and B | identical |
    | 18 | Detach a store | `DELETE /api/v1/stores/{name}` | detached |
    | 19 | Strict mode | `-dynamic-stores=false` | 400 listing known stores |
    | 20 | Empty dstype regression | `crudget dstype=""` | reads `dsswres` |

    ---

    ## 4. Detailed scenarios

    ### 1 — Baseline

    ```bash
    curl -s $A/api/v1/stores | jq
    ```

    Expect eleven entries, all `"kind": "builtin"`:
    `validations, kbdata, kbmetadata, whoiswho, dsswres, dsswresaloc, tosca_imported, tosca_adt, tosca_capacities, tosca_deploymentplan, tosca_eventhistory`

    `contributions` is absent — it's an EventLogStore, not a docstore.

    Record the count:

    ```bash
    curl -s $A/api/v1/stores | jq '.stores | length'   # 11
    ```

    ### 2 — Create a store by writing to it

    No setup, no registration:

    ```bash
    curl -s -X POST $A/swarmkb/command \
    -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudput"},"dstype":"kbtrust",
    "criteria":[{"_id":"peer:QmTEST001","context":"storage",
    "role":"coordinator","trust_level":0.8,"verified":true}]}' | jq
    ```

    Agent log:

    ```
    [DYNSTORE] store "kbtrust" ready at /orbitdb/bafyrei.../kbtrust
    [INFO] CRUDPUT: Inserting 1 documents into kbtrust
    ```

    ### 3 — Confirm isolation from dsswres

    This is the assertion that matters most:

    ```bash
    curl -s -X POST $A/swarmkb/command \
    -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"dsswres",
    "criteria":[{"_id":"peer:QmTEST001"}]}' | jq 'length'
    ```

    Must be **0**. If it returns 1, the fallback is still active and `app/service.go` didn't get replaced.

    On disk, two separate directories:

    ```bash
    docker exec optimusdb1 ls ~/.cache/optimusdb/swarmkbIpfs/orbitdb/
    # ... dsswres  kbtrust ...
    ```

    ### 4 — Read back

    ```bash
    curl -s -X POST $A/swarmkb/command \
    -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"kbtrust","criteria":[{}]}' | jq
    ```

    One document. Now list stores again — `kbtrust` appears with `"kind": "dynamic"`.

    ### 5 — Explicit creation

    ```bash
    curl -s -X POST $A/api/v1/stores \
    -H 'Content-Type: application/json' \
    -d '{"name":"kbreputation"}' | jq
    ```

    ```json
    {"name":"kbreputation","address":"/orbitdb/bafyrei.../kbreputation","created":true}
    ```

    Repeat the same call — `"created": false` and HTTP 200 instead of 201. Idempotent.

    ### 6–8 — Reads, updates and deletes against stores that don't exist

    ```bash
    # read
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"neverseen","criteria":[{}]}' | jq
    # -> []   (not an error; store now exists)

    # update
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudupdate"},"dstype":"neverseen2",
    "criteria":[{"x":1}],"updatedata":[{"y":2}]}' | jq
    # -> 0 matched

    # delete
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"cruddelete"},"dstype":"neverseen3","criteria":[{"x":1}]}' | jq
    # -> 0 removed
    ```

    All three create the store. Clean them up:

    ```bash
    for s in neverseen neverseen2 neverseen3; do
    curl -s -X DELETE $A/api/v1/stores/$s | jq -c
    done
    ```

    ### 9–11 — Names that must be rejected

    ```bash
    # spaces and uppercase
    curl -s -X POST $A/api/v1/stores -H 'Content-Type: application/json' \
    -d '{"name":"My Trust Store"}' | jq
    # -> invalid store name "my trust store": use 2-63 chars, lowercase letters, ...

    # reserved — EventLog, not a docstore
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"contributions","criteria":[{}]}' | jq
    # -> dstype "contributions" is an EventLog store, not a document store

    # path traversal — the name becomes a directory
    curl -s -X POST $A/api/v1/stores -H 'Content-Type: application/json' \
    -d '{"name":"../../etc/passwd"}' | jq
    # -> invalid store name
    ```

    Confirm nothing escaped the cache root:

    ```bash
    docker exec optimusdb1 ls ~/.cache/optimusdb/swarmkbIpfs/orbitdb/
    ```

    ### 12 — Upload a TOSCA file into a new store

    ```bash
    curl -s -X POST $A/swarmkb/upload \
    -F "file=@repoScript/Tosca/scenarioHackathon/Tosca Samples/webapp_adt.yaml" \
    -F "dstype=tosca_experimental" | jq
    ```

    Then:

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"tosca_experimental","criteria":[{}]}' | jq 'length'
    ```

    ### 13 — Config persistence

    ```bash
    docker exec optimusdb1 cat /root/swarmkbIpfs_config | jq '.dynamicStoreAddrs'
    ```

    ```json
    {
    "kbtrust": "/orbitdb/bafyrei.../kbtrust",
    "kbreputation": "/orbitdb/bafyrei.../kbreputation",
    "tosca_experimental": "/orbitdb/bafyrei.../tosca_experimental"
    }
    ```

    Written the moment the store is created, not at shutdown — an unclean exit can't orphan the OpLog.

    ### 14 — Survive a restart

    ```bash
    docker restart optimusdb1 && sleep 45
    curl -s $A/api/v1/stores | jq '.stores[] | select(.kind=="dynamic")'
    ```

    Agent log on boot:

    ```
    [DYNSTORE] restored 3 of 3 dynamic store(s)
    ```

    Documents must still be there:

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"kbtrust","criteria":[{}]}' | jq 'length'
    ```

    ### 15 — Export coverage

    ```bash
    curl -s -X POST $A/api/v1/exchange/export -o backup.tar.gz
    tar -tzf backup.tar.gz
    ```

    ```
    manifest.json
    sqlite/optimusdb.db
    orbitdb/dsswres.jsonl
    orbitdb/kbtrust.jsonl          ← dynamic, no code change needed
    orbitdb/kbreputation.jsonl
    orbitdb/tosca_experimental.jsonl
    ...
    ```

    Manifest lists it too:

    ```bash
    tar -xzOf backup.tar.gz manifest.json | jq '.stores'
    ```

    Per-store record counts:

    ```bash
    for f in $(tar -tzf backup.tar.gz | grep '^orbitdb/'); do
    echo "$f: $(tar -xzOf backup.tar.gz "$f" | wc -l)"
    done
    ```

    ### 16 — Import onto a node that has never heard of the store

    ```bash
    curl -s $D/api/v1/stores | jq '.stores[] | select(.name=="kbtrust")'   # empty

    curl -s -X POST $D/api/v1/exchange/import -F "archive=@backup.tar.gz" | jq
    ```

    ```json
    {"sqlite_restored": true,
    "stores": {"kbtrust": 1, "kbreputation": 0, "dsswres": 412, ...},
    "errors": []}
    ```

    `errors` must be empty. Under the old code this produced `store "kbtrust" in archive but not open on this node`.

    Node 4 log:

    ```
    [EXCHANGE] created store "kbtrust" during import
    ```

    ### 17 — Cross-peer replication

    A dynamic store replicates only if every node derives the **same** OrbitDB address for the name. The address comes from the manifest — a function of name, store type and access controller — and `EnsureDocStore` uses the same profile as the TOSCA stores, so they should match. Verify rather than assume:

    ```bash
    curl -s $B/swarmkb/command -X POST -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"kbtrust","criteria":[{}]}' > /dev/null

    diff <(curl -s $A/api/v1/stores | jq -r '.stores[]|select(.name=="kbtrust")|.address') \
    <(curl -s $B/api/v1/stores | jq -r '.stores[]|select(.name=="kbtrust")|.address') \
    && echo "SAME ADDRESS — will replicate"
    ```

    Then check the document arrives on B (allow ~30s):

    ```bash
    curl -s -X POST $B/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"kbtrust","criteria":[{}]}' | jq 'length'
    ```

    If the addresses differ, the two nodes have separate logs under one name — report it, the fix is to propagate the address over the existing GossipSub channel.

    ### 18 — Detach

    ```bash
    curl -s -X DELETE $A/api/v1/stores/kbreputation | jq
    # {"name":"kbreputation","detached":true}

    curl -s -X DELETE $A/api/v1/stores/dsswres | jq
    # {"error":"\"dsswres\" is a built-in store and cannot be dropped"}
    ```

    Detach removes the handle and the config entry. The OpLog stays on disk and peer replicas are untouched, so re-creating the store by name reattaches to the same data.

    ### 19 — Strict mode

    Restart one agent with `-dynamic-stores=false`:

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudput"},"dstype":"brandnew","criteria":[{"_id":"x"}]}' | jq
    ```

    ```
    unknown dstype "brandnew" and implicit store creation is disabled
    (-dynamic-stores=false); create it explicitly with POST /api/v1/stores
    ```

    `POST /api/v1/stores` still works in this mode — that's the point.

    ### 20 — Empty dstype regression

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"criteria":[{}]}' | jq 'length'
    ```

    Must return the `dsswres` contents, same as before the change. No store named `""` may appear in the listing.

    ---

    ## 5. Impact on optimusTrust

    **This repo is directly affected, and one part needs action.**

    ### What now works as documented

    `store_demo.py persist --store kbtrust` creates a genuine `kbtrust` docstore. The README's note —

    > *The first `persist` run is also the test of whether `kbtrust` is accepted: if writes round-trip into `retrieve`, lazy store creation works; if not, register `kbtrust` in the Go agent config.*

    — was previously unfalsifiable. Writes and reads both fell through to `dsswres`, so the round-trip always succeeded and "lazy store creation works" looked true. It is true now.

    No code change is needed in `store_demo.py`, `trust_store.py` or `tms_demo.py`. The client already sends `dstype=kbtrust`; the server finally honours it.

    ### What breaks

    **Trust documents written before the upgrade are in `dsswres`, not `kbtrust`.** After the upgrade, `retrieve --store kbtrust` reads the new, empty store and returns nothing. The data is not lost — it's in `dsswres`. See [section 7](#7-migrating-data-stranded-in-dsswres).

    ### README updates to make

    | Section | Change |
    |---|---|
    | Notes → "The datastore" | Replace the lazy-creation caveat: `kbtrust` is created on first write, and is a real, separately-addressed store. Drop "if not, register `kbtrust` in the Go agent config." |
    | New note | Data written before the OptimusDB dynamic-store upgrade lives in `dsswres`; link the migration steps. |
    | New note | `kbtrust` is included in `POST /api/v1/exchange/export` automatically. |
    | Optional | Mention `GET /api/v1/stores` as the way to confirm the store exists and is separate. |

    ### Verify after upgrading

    ```bash
    python3 store_demo.py persist --file sample_documents.json --store kbtrust
    python3 store_demo.py retrieve --store kbtrust --where trust_level:0.6:gte

    # the key check — dsswres must NOT contain them
    python3 store_demo.py retrieve --store dsswres --where trust_level:0.6:gte
    # -> 0 documents
    ```

    ---

    ## 6. Impact on optimusPy

    **No breaking API change. Two documentation fixes and one optional feature.**

    `optimusdb_client.py` passes `dstype` straight through and does no client-side validation, so nothing in the request format changes.

    ### Behaviour change worth knowing

    A **typo in `--dstype`** used to return `dsswres` data and look like a successful query. It now returns an empty result and quietly creates a store:

    ```bash
    python optimusdb_client.py get --dstype kbmetdata     # note the typo
    # before: returned dsswres documents
    # after:  returns [], and a "kbmetdata" store now exists
    ```

    This is an improvement — you were reading the wrong store before — but any test that depended on the old behaviour will now report zero rows. Check `GET /api/v1/stores` after a test run; an unexpected name in the list means a `dstype` somewhere is misspelled.

    `--target-store` on upload changes in the opposite direction: it used to return `unknown store type: X` for an unrecognised value, and now creates the store instead.

    ### README updates to make

    **§6 "Available Datastores"** currently reads as a closed list of ten. Add a line beneath the table:

    > The ten stores above exist from boot. The set is not closed — any `dstype` naming a store that doesn't exist creates it (names: 2–63 chars, `^[a-z0-9][a-z0-9_-]*$`). `GET /api/v1/stores` lists everything live on a node. `contributions` is an EventLog and cannot be used as a `dstype`.

    Also add `kbtrust` to the table if `optimusTrust` is part of your standard deployment.

    **§7 "API Endpoints"** — three new rows:

    | Endpoint | Method | Purpose |
    |---|---|---|
    | `/api/v1/stores` | GET | List live document stores (built-in + dynamic) |
    | `/api/v1/stores` | POST | Create a document store explicitly |
    | `/api/v1/stores/{name}` | DELETE | Detach a dynamic store from this node |

    Worth adding the exchange endpoints too if they aren't documented yet — note that `batch_operations.py export` is a *client-side* JSON dump of one store, whereas `/api/v1/exchange/export` is a *server-side* tar.gz of every store plus the SQLite catalog. They are not interchangeable.

    **§ Troubleshooting → "No Documents Found"** — add a first check:

    ```bash
    # confirm the store you queried actually holds data
    curl -s $A/api/v1/stores | jq
    ```

    An unexpected store name here means a misspelled `dstype` created it.

    ### Optional client additions

    ```python
    def list_stores(self):
    return self._get("/api/v1/stores")

    def create_store(self, name):
    return self._post("/api/v1/stores", {"name": name})
    ```

    CLI: `python optimusdb_client.py stores` and `... create-store --name kbtrust`. Worth having — it makes the store list visible from the same tool people already use.

    ### New test scenarios for the optimusPy guide

    The existing 20 scenarios all still pass unchanged. Four worth appending:

    | # | Goal | Command | Expected |
    |---|---|---|---|
    | 21 | List stores | `stores` | 11 built-in + any dynamic |
    | 22 | Create on write | `create --json '[{"_id":"t1"}]' --dstype demo_store` | store created |
    | 23 | Isolation | `get --dstype dsswres --criteria '_id:t1'` | 0 documents |
    | 24 | Typo detection | `get --dstype dsswress` | `[]`, and `dsswress` appears in the store list |

    ---

    ## 7. Migrating data stranded in dsswres

    Trust documents written by `optimusTrust` before the upgrade are in `dsswres`. Move them.

    **Back up first.** This is destructive at step 4.

    ```bash
    curl -s -X POST $A/api/v1/exchange/export -o pre-migration.tar.gz
    ```

    **1. Find them.** They carry `trust_level` and a `peer:Qm...` `_id`:

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"dsswres",
    "criteria":[{"_id":{"$regex":"^peer:Qm"}}]}' > trust-docs.json

    jq 'length' trust-docs.json
    jq -r '.[]._id' trust-docs.json
    ```

    Inspect the list before going further — make sure nothing unrelated matches.

    **2. Write them into kbtrust:**

    ```bash
    jq -c '.' trust-docs.json | while read -r batch; do
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d "{\"method\":{\"cmd\":\"crudput\"},\"dstype\":\"kbtrust\",\"criteria\":$batch}" | jq -c
    done
    ```

    **3. Verify the counts match:**

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"kbtrust","criteria":[{}]}' | jq 'length'
    ```

    **4. Only then remove them from dsswres:**

    ```bash
    jq -r '.[]._id' trust-docs.json | while read -r id; do
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d "{\"method\":{\"cmd\":\"cruddelete\"},\"dstype\":\"dsswres\",\"criteria\":[{\"_id\":\"$id\"}]}" | jq -c
    done
    ```

    **5. Confirm:**

    ```bash
    curl -s -X POST $A/swarmkb/command -H 'Content-Type: application/json' \
    -d '{"method":{"cmd":"crudget"},"dstype":"dsswres",
    "criteria":[{"_id":{"$regex":"^peer:Qm"}}]}' | jq 'length'   # 0
    ```

    If `$regex` isn't supported by your criteria evaluator, filter client-side on the full `dsswres` dump instead.

    ---

    ## 8. Regression checklist

    The change touches the resolution path for every store-addressed operation, so re-run the existing suites:

    - [ ] `repoScript/Tosca/**` — TOSCA upload and query scenarios
    - [ ] `repoScript/Metadata/**` — 48-field metadata generation
    - [ ] `repoScript/semantic/test_semantic_search.sh` — hydration now goes through the resolver
    - [ ] `repoScript/ElectionAnalysis/**` — coordinator election (untouched, but confirm)
    - [ ] `repoScript/EMS/**` — sensor → catalog path
    - [ ] `optimusPy` scenarios 1–20
    - [ ] `optimusTrust` `store_demo.py` persist / retrieve / delete
    - [ ] Chat: `POST /api/v1/chat` with a question naming a known dataset
    - [ ] Chat: a question naming a **nonsense** dataset — confirm whether you want the store created (see the `NOTE:` in `createKBQueryFunc`)

    Specific regressions to assert:

    | Assertion | Why |
    |---|---|
    | `dstype: ""` still reads `dsswres` | documented default must survive |
    | All 11 built-in stores resolve by name | the switches were replaced wholesale |
    | `validations` hydrates in semantic search | it had no case in the old `FetchDocument` |
    | `kbmetadata` query via `unifiedQueryDocStore` | that path only knew two stores before |
    | Export contains all 11 built-in stores | `storeRegistry` was rewritten |

    ---

    ## 9. Troubleshooting

    **`GET /api/v1/stores` returns 404** — routes weren't registered. Confirm `api/http.go` was replaced: `grep -n storesHandler api/http.go`.

    **Unresolved references to `StoreCreate`, `StoreMode`, `dynMu`** — `app/app.go` wasn't replaced; those types live there. In GoLand also do File → Invalidate Caches / Restart.

    **Data appears to vanish after upgrade** — expected if a client was using a `dstype` that used to fall through to `dsswres`. The data is in `dsswres`. See [section 7](#7-migrating-data-stranded-in-dsswres).

    **Store created but empty after restart** — check `dynamicStoreAddrs` in `<repo>_config`. If the entry is missing, the config file wasn't writable; if present but the store is empty, the address changed and it opened a fresh log.

        **Peers don't see a dynamic store** — run scenario 17. Different addresses mean separate logs under one name.

        **Unexpected store names in the listing** — a client is sending a misspelled `dstype`. Find it, fix the client, then `DELETE /api/v1/stores/{name}`.
