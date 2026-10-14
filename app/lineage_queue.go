package app

// Lineage and metadata bookkeeping after updates, off the request path.
//
// updateLineageAndMetadata writes the data catalog and lineage tables in SQLite. Starting a goroutine per
// update let these writes pile up under load and compete with request handling. Updates now go to one
// queue served by one worker. Only the latest state of a document matters for its catalog entry, so a
// document updated again before its turn is processed once, with its latest version; no final state is lost.

import (
	"sync"

	"optimusdb/logger"
)

type lineageJob struct {
	db        *KnowledgeBaseDB
	doc       map[string]interface{}
	storeName string
	docID     string
}

var lineageQ = struct {
	mu      sync.Mutex
	pending map[string]lineageJob
	order   []string
	running bool
}{pending: map[string]lineageJob{}}

func enqueueLineage(db *KnowledgeBaseDB, doc map[string]interface{}, storeName, docID string) {
	key := storeName + "\x00" + docID
	lineageQ.mu.Lock()
	if _, queued := lineageQ.pending[key]; !queued {
		lineageQ.order = append(lineageQ.order, key)
	}
	lineageQ.pending[key] = lineageJob{db: db, doc: doc, storeName: storeName, docID: docID}
	if !lineageQ.running {
		lineageQ.running = true
		go lineageWorker()
	}
	lineageQ.mu.Unlock()
}

func lineageWorker() {
	for {
		lineageQ.mu.Lock()
		if len(lineageQ.order) == 0 {
			lineageQ.running = false
			lineageQ.order = nil
			lineageQ.mu.Unlock()
			return
		}
		key := lineageQ.order[0]
		lineageQ.order = lineageQ.order[1:]
		job := lineageQ.pending[key]
		delete(lineageQ.pending, key)
		lineageQ.mu.Unlock()

		func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("[LINEAGE] bookkeeping for %s/%s failed: %v", job.storeName, job.docID, r)
				}
			}()
			updateLineageAndMetadata(job.db, job.doc, job.storeName, job.docID)
		}()
	}
}
