package app

// Group commit for single-document writes.
//
// Every DocumentStore write is a signed oplog entry stored in IPFS and announced to peers, and the store
// appends entries one at a time. Under concurrent load each insert or update therefore waited for all the
// writes queued before it. Writes to the same store that arrive while an entry is being written are now
// combined into the next entry (one PutAll); each caller is answered when the entry that holds its document
// is stored. A write that arrives on an idle store is written at once with Put, exactly as before, so this
// adds no delay at low load. Documents with the same _id in one entry keep arrival order (the later wins),
// the same result as writing them one after the other.
//
// Settings (environment):
//   OPTIMUSDB_GROUP_COMMIT   "false" writes every document with its own Put (default: true)
//   OPTIMUSDB_BATCH_MAX_DOCS / OPTIMUSDB_BATCH_MAX_BYTES  limits per entry, shared with batch_put.go

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"berty.tech/go-orbit-db/iface"
	"optimusdb/logger"
)

func groupCommitEnabled() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("OPTIMUSDB_GROUP_COMMIT")))
	return v != "false" && v != "0" && v != "no" && v != "off" && batchPutEnabled()
}

type putRequest struct {
	doc  interface{}
	size int
	done chan error
}

// storeWriter serialises the writes of one store and combines the ones that queue up.
type storeWriter struct {
	mu      sync.Mutex
	queue   []*putRequest
	running bool
}

var storeWriters sync.Map // store instance -> *storeWriter

// writerFor returns the writer of a store instance (a reopened store gets its own writer).
func writerFor(store iface.DocumentStore) *storeWriter {
	w, _ := storeWriters.LoadOrStore(store, &storeWriter{})
	return w.(*storeWriter)
}

// put and putAll turn a panic inside the store into an error, so every waiting caller gets an answer.
func put(ctx context.Context, store iface.DocumentStore, doc interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store write failed: %v", r)
		}
	}()
	_, err = store.Put(ctx, doc)
	return err
}

func putAll(ctx context.Context, store iface.DocumentStore, docs []interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store write failed: %v", r)
		}
	}()
	_, err = store.PutAll(ctx, docs)
	return err
}

// PutCoalesced stores one document, combined with other writes to the same store that are waiting.
func PutCoalesced(ctx context.Context, store iface.DocumentStore, doc interface{}, storeName string) error {
	errs := PutManyCoalesced(ctx, store, []interface{}{doc}, storeName)
	return errs[0]
}

// PutManyCoalesced stores a few documents through the store's writer and returns one error per document.
func PutManyCoalesced(ctx context.Context, store iface.DocumentStore, docs []interface{}, storeName string) []error {
	errs := make([]error, len(docs))
	if !groupCommitEnabled() {
		for i, d := range docs {
			_, errs[i] = store.Put(ctx, d)
		}
		return errs
	}

	reqs := make([]*putRequest, len(docs))
	for i, d := range docs {
		n := 1024
		if b, err := json.Marshal(d); err == nil {
			n = len(b)
		}
		reqs[i] = &putRequest{doc: d, size: n, done: make(chan error, 1)}
	}

	w := writerFor(store)
	w.mu.Lock()
	w.queue = append(w.queue, reqs...)
	if !w.running {
		w.running = true
		go w.run(store, storeName)
	}
	w.mu.Unlock()

	for i, r := range reqs {
		select {
		case errs[i] = <-r.done:
		case <-ctx.Done():
			errs[i] = ctx.Err()
		}
	}
	return errs
}

func (w *storeWriter) run(store iface.DocumentStore, storeName string) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("[GROUPCOMMIT] writer for %s stopped: %v", storeName, r)
			w.mu.Lock()
			pending := w.queue
			w.queue = nil
			w.running = false
			w.mu.Unlock()
			for _, q := range pending {
				q.done <- fmt.Errorf("store writer failed: %v", r)
			}
		}
	}()
	for {
		w.mu.Lock()
		batch := w.queue
		w.queue = nil
		if len(batch) == 0 {
			w.running = false
			w.mu.Unlock()
			return
		}
		w.mu.Unlock()
		w.write(store, storeName, batch)
	}
}

// write stores the queued requests in as few entries as the size limits allow.
func (w *storeWriter) write(store iface.DocumentStore, storeName string, batch []*putRequest) {
	ctx := context.Background()
	maxDocs := envInt("OPTIMUSDB_BATCH_MAX_DOCS", 500)
	maxBytes := envInt("OPTIMUSDB_BATCH_MAX_BYTES", 512*1024)

	for start := 0; start < len(batch); {
		end, size := start, 0
		for end < len(batch) && (end == start || (end-start < maxDocs && size+batch[end].size <= maxBytes)) {
			size += batch[end].size
			end++
		}
		chunk := batch[start:end]
		start = end

		if len(chunk) == 1 {
			chunk[0].done <- put(ctx, store, chunk[0].doc)
			continue
		}

		docs := make([]interface{}, len(chunk))
		for i, r := range chunk {
			docs[i] = r.doc
		}
		if err := putAll(ctx, store, docs); err != nil {
			logger.Warn("[GROUPCOMMIT] PutAll of %d documents into %s failed: %v; writing them one by one", len(chunk), storeName, err)
			for _, r := range chunk {
				r.done <- put(ctx, store, r.doc)
			}
			continue
		}
		for _, r := range chunk {
			r.done <- nil
		}
	}
}
