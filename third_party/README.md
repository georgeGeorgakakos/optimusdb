# third_party/go-orbit-db

A copy of `berty.tech/go-orbit-db` **v1.21.0** (tests removed) with three files changed:
`stores/documentstore/index.go`, `stores/documentstore/document.go` and `stores/basestore/base_store.go`. `go.mod` points to it with
`replace berty.tech/go-orbit-db => ./third_party/go-orbit-db`.

## Why

OptimusDB wrote every document with its own `Put`: one signed oplog entry, one IPFS block and one
GossipSub head announcement per document, and upstream re-parsed the whole oplog on every write.
Bulk loads were therefore O(N^2) (about 30 minutes for 10,000 documents on the evaluation testbed).
Batching with `PutAll` was not possible because of a bug in the upstream index.

## Changes in `stores/documentstore/index.go`

1. **PUTALL bug fix.** Upstream marked the operation key (always `""`) as handled instead of each
   document's key, so an older write of the same key, processed later, overwrote the newer value.
2. **Incremental index.** Each oplog entry is parsed once (entries are remembered by hash). For every key
   the index keeps the value of the entry with the highest Lamport clock (time, then writer ID), the
   last-writer-wins order the log itself uses; deletions keep a tombstone clock. The result is the same
   as a full rebuild regardless of the order in which entries arrive (checked with a randomised test of
   15,000 shuffled logs against the upstream rebuild rule with the bug fixed).
3. **No log traversal per write.** Upstream passed no entries to the index and the index read
   `oplog.Values()`, which walks the whole log from its heads and sorts it, on every write, so a write
   became slower as the log grew. A local write now passes the entry it appended (the one-line change in
   `base_store.go`), which is applied on its own. Replication and loading read the log's entry map, keyed by
   entry hash, and apply only entries not seen yet; order does not matter because of (2).
4. **Field indexes.** For the top-level fields listed in `OPTIMUSDB_INDEX_FIELDS` (default
   `type,status,region,resource_id`; `off` disables them), the index keeps, per string value, the keys of
   the documents holding it. If a stored value is not JSON, the field indexes switch themselves off.
5. **Decoded-document cache.** Up to `OPTIMUSDB_DOC_CACHE_MB` per store (default 128 MiB, estimated as
   three times the stored size; 0 disables) of decoded documents are kept. Each is tied to the exact stored bytes it came from, so a newer value is never
   served from the cache, and readers receive deep copies.

Changes 2 to 5 are checked by a randomised test (400 shuffled logs of PUT, PUTALL and DEL entries applied
through both update paths, compared with a brute-force last-writer-wins rebuild, including the field
indexes) and by tests of the cache's copy and staleness rules.

`app/batch_put.go` uses `PutAll` with batches of up to 500 documents or about 512 KiB per oplog entry.
`OPTIMUSDB_BATCH_PUT=false` restores the old one-document-per-entry behaviour, for comparison.

## Change in `stores/documentstore/document.go`

**Exact-key `Get`.** Upstream `Get` compared the key with every key in the store. For an exact lookup (no
`CaseInsensitive`, no `PartialMatches`, no multi-term key) it now reads the index entry for that key
directly. Same result, constant cost. `app/query_fastpath.go` uses it for queries and updates whose only
condition is `{"_id": "<id>"}`.

**Faster reads.** `Get` and `Query` use the decoded-document cache. The new `QueryIndexed` reads only the
documents selected by an equality condition on an indexed field and applies the full filter to each, so its
result equals `Query` with that filter. `queryLocalDB` uses it for single-filter queries with such a condition.
