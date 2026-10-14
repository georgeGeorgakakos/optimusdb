package app

// Batched document ingestion.
//
// Before: every document was written with its own DocumentStore.Put, i.e. one signed oplog entry, one IPFS
// block, one GossipSub head announcement and (upstream go-orbit-db) one full index rebuild per document.
// A 10,000-document load took about 30 minutes on the evaluation testbed.
//
// Now: documents are grouped into batches and each batch is written with DocumentStore.PutAll, i.e. ONE
// oplog entry (one IPFS block) that carries many documents. Peers fetch one block per batch instead of one
// per document. This depends on the patched go-orbit-db in third_party/go-orbit-db (PUTALL index bug fixed,
// incremental index), wired in through the replace directive in go.mod.
//
// Settings (environment):
//   OPTIMUSDB_BATCH_PUT        "false" restores the old one-document-per-entry path (default: true)
//   OPTIMUSDB_BATCH_MAX_DOCS   documents per oplog entry (default: 500)
//   OPTIMUSDB_BATCH_MAX_BYTES  approximate JSON bytes per oplog entry (default: 524288 = 512 KiB; IPFS
//                              blocks above ~1 MiB are not exchanged by bitswap, so stay well below)

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"berty.tech/go-orbit-db/iface"
	"optimusdb/logger"
)

func batchPutEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("OPTIMUSDB_BATCH_PUT")))
	return v != "false" && v != "0" && v != "no"
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name))); err == nil && v > 0 {
		return v
	}
	return def
}

// splitBatches groups documents so that each group stays within maxDocs documents and roughly maxBytes of
// JSON. A single document larger than maxBytes forms a group of its own.
func splitBatches(docs []interface{}, maxDocs, maxBytes int) [][]interface{} {
	var out [][]interface{}
	var cur []interface{}
	size := 0
	for _, d := range docs {
		n := 1024
		if b, err := json.Marshal(d); err == nil {
			n = len(b)
		}
		if len(cur) > 0 && (len(cur) >= maxDocs || size+n > maxBytes) {
			out = append(out, cur)
			cur, size = nil, 0
		}
		cur = append(cur, d)
		size += n
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// PutDocumentsBatched writes documents into a document store, one PutAll (one oplog entry) per batch.
// If a PutAll fails, that batch falls back to individual Puts so a single bad document cannot drop the rest.
// It returns the documents that were stored and the error of the last failure, if any.
func PutDocumentsBatched(ctx context.Context, store iface.DocumentStore, docs []interface{}, storeName string) ([]interface{}, error) {
	if !batchPutEnabled() {
		return putDocumentsOneByOne(ctx, store, docs, storeName)
	}
	// A few documents (an application insert, not a bulk load) go through the store's group commit, so
	// concurrent inserts share oplog entries (group_commit.go).
	if groupCommitEnabled() && len(docs) <= envInt("OPTIMUSDB_GROUP_COMMIT_MAX_DOCS", 64) {
		var stored []interface{}
		var lastErr error
		for i, err := range PutManyCoalesced(ctx, store, docs, storeName) {
			if err != nil {
				logger.Error("[ERROR] BATCHPUT: failed to insert a document into %s: %v", storeName, err)
				lastErr = err
				continue
			}
			stored = append(stored, docs[i])
		}
		return stored, lastErr
	}
	maxDocs := envInt("OPTIMUSDB_BATCH_MAX_DOCS", 500)
	maxBytes := envInt("OPTIMUSDB_BATCH_MAX_BYTES", 512*1024)

	var stored []interface{}
	var lastErr error
	batches := splitBatches(docs, maxDocs, maxBytes)
	for bi, batch := range batches {
		if _, err := store.PutAll(ctx, batch); err != nil {
			logger.Warn("[WARN] BATCHPUT: PutAll of batch %d/%d (%d docs) into %s failed: %v; retrying one by one",
				bi+1, len(batches), len(batch), storeName, err)
			ok, e := putDocumentsOneByOne(ctx, store, batch, storeName)
			stored = append(stored, ok...)
			if e != nil {
				lastErr = e
			}
			continue
		}
		stored = append(stored, batch...)
	}
	logger.Info("[INFO] BATCHPUT: %d/%d documents stored into %s in %d oplog entries",
		len(stored), len(docs), storeName, len(batches))
	return stored, lastErr
}

func putDocumentsOneByOne(ctx context.Context, store iface.DocumentStore, docs []interface{}, storeName string) ([]interface{}, error) {
	var stored []interface{}
	var lastErr error
	for i, d := range docs {
		if _, err := store.Put(ctx, d); err != nil {
			id := fmt.Sprintf("doc_%d", i)
			if m, ok := d.(map[string]interface{}); ok {
				if v, has := m["_id"]; has {
					id = fmt.Sprintf("%v", v)
				}
			}
			logger.Error("[ERROR] BATCHPUT: failed to insert document %s into %s: %v", id, storeName, err)
			lastErr = err
			continue
		}
		stored = append(stored, d)
	}
	return stored, lastErr
}
