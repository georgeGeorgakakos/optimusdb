package app

// Lookups by document id and persistence of rows learned from peers.
//
// Every document store is keyed by "_id" (go-orbit-db MapKeyExtractor), so a query whose only condition is
// "_id" == <string> has at most one match: the index entry under that key. Reading that entry directly gives
// the same result as scanning and unmarshalling every document in the store, at a cost that does not grow
// with the size of the store.

import (
	"context"

	"berty.tech/go-orbit-db/iface"
	"optimusdb/logger"
)

// idOnlyFilter returns the id when filter has exactly one condition, "_id" equal to a string.
func idOnlyFilter(filter map[string]interface{}) (string, bool) {
	if len(filter) != 1 {
		return "", false
	}
	id, ok := filter["_id"].(string)
	return id, ok
}

// idOnlyCriteria returns the id when criteria is a single filter of the form {"_id": "<id>"}.
func idOnlyCriteria(criteria []map[string]interface{}) (string, bool) {
	if len(criteria) != 1 {
		return "", false
	}
	return idOnlyFilter(criteria[0])
}

// getDocsByID reads the document stored under id (nil when there is none).
func getDocsByID(ctx context.Context, store iface.DocumentStore, id string) ([]interface{}, error) {
	return store.Get(ctx, id, nil)
}

// annotationKeys are added to query results for the client (annotate, tagRows); they are not document data.
var annotationKeys = []string{"_source", "_trace"}

// persistRemoteRows stores query results that are not yet in the local store (rows learned from peers),
// without the annotation fields. Rows already stored locally are not written again: a read must not create
// new log entries, and a copy carrying _source/_trace must not replace the original document. Differences
// between replicas of a document that exists locally are left to log replication, which converges them.
// They go into the store the query was made on (dstype), which is also the store the local part read.
func persistRemoteRows(optimusdb *KnowledgeBaseDB, dstype string, rows []map[string]interface{}) {
	if len(rows) == 0 {
		return
	}
	ctx := context.Background()
	store, storeName, err := optimusdb.ResolveDocStore(ctx, dstype, StoreCreate)
	if err != nil || store == nil {
		logger.Warn("[QUERY] cannot persist rows learned from peers: store %q not available: %v", dstype, err)
		return
	}

	var missing []interface{}
	for _, row := range rows {
		id, ok := row["_id"].(string)
		if !ok || id == "" {
			continue
		}
		if existing, err := getDocsByID(ctx, store, id); err == nil && len(existing) > 0 {
			continue
		}
		doc := make(map[string]interface{}, len(row))
		for k, v := range row {
			doc[k] = v
		}
		for _, k := range annotationKeys {
			delete(doc, k)
		}
		missing = append(missing, doc)
	}
	if len(missing) == 0 {
		return
	}
	stored, err := PutDocumentsBatched(ctx, store, missing, storeName)
	if err != nil {
		logger.Warn("[QUERY] persisting %d rows learned from peers: %d stored, last error: %v", len(missing), len(stored), err)
		return
	}
	logger.Info("[QUERY] stored %d rows learned from peers into %s", len(stored), storeName)
}

// toDocMaps keeps the documents that are JSON objects.
func toDocMaps(results []interface{}) []map[string]interface{} {
	var documents []map[string]interface{}
	for _, result := range results {
		if doc, ok := result.(map[string]interface{}); ok {
			documents = append(documents, doc)
		}
	}
	return documents
}
