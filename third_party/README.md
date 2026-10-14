# third_party/go-orbit-db

A copy of `berty.tech/go-orbit-db` **v1.21.0** (tests removed) with one file changed:
`stores/documentstore/index.go`. `go.mod` points to it with
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

`app/batch_put.go` uses `PutAll` with batches of up to 500 documents or about 512 KiB per oplog entry.
`OPTIMUSDB_BATCH_PUT=false` restores the old one-document-per-entry behaviour, for comparison.
