package app

// Lookups by document id and persistence of rows learned from peers.
//
// Every document store is keyed by "_id" (go-orbit-db MapKeyExtractor), so a query whose only condition is
// "_id" == <string> has at most one match: the index entry under that key. Reading that entry directly gives
// the same result as scanning and unmarshalling every document in the store, at a cost that does not grow
// with the size of the store.

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"

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

// indexedQuerier is implemented by the patched go-orbit-db document store (third_party/go-orbit-db).
type indexedQuerier interface {
	QueryIndexed(ctx context.Context, equal map[string]string, filter func(doc interface{}) (bool, error)) ([]interface{}, bool, error)
}

// indexableEqualities returns the conditions "field equals a string" of a query with a single filter (no OR),
// on top-level fields. Every matching document satisfies all of them, so any one can select the candidates.
func indexableEqualities(criteria []map[string]interface{}) map[string]string {
	if len(criteria) != 1 {
		return nil
	}
	var eq map[string]string
	for k, v := range criteria[0] {
		s, ok := v.(string)
		if !ok || k == "" || strings.Contains(k, ".") {
			continue
		}
		if eq == nil {
			eq = map[string]string{}
		}
		eq[k] = s
	}
	return eq
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

	// Existence check without reading the document (patched document store); this runs for every row of a
	// query result, most of which are local, so reading and copying each document doubled the cost of a query.
	hk, cheap := store.(interface{ HasKey(string) bool })
	var missing []interface{}
	for _, row := range rows {
		id, ok := row["_id"].(string)
		if !ok || id == "" {
			continue
		}
		if cheap {
			if hk.HasKey(id) {
				continue
			}
		} else if existing, err := getDocsByID(ctx, store, id); err == nil && len(existing) > 0 {
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

// RawDocs is a query result made of stored JSON documents. The HTTP API writes it as it is; for any other
// consumer it encodes as a JSON array of the documents.
type RawDocs [][]byte

// MarshalJSON writes the documents as a JSON array.
func (r RawDocs) MarshalJSON() ([]byte, error) {
	n := 2
	for _, d := range r {
		n += len(d) + 1
	}
	out := make([]byte, 0, n)
	out = append(out, '[')
	for i, d := range r {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, d...)
	}
	return append(out, ']'), nil
}

// exactRawQuerier is implemented by the patched go-orbit-db document store (third_party/go-orbit-db).
type exactRawQuerier interface {
	QueryRawExact(equal map[string]string) ([][]byte, bool)
}

// exactEqualities returns the criteria as field -> value when they are a single set of string equalities on
// top-level fields (what criteriaFilter then checks with plain equality); ok is false for anything else.
func exactEqualities(criteria []map[string]interface{}) (map[string]string, bool) {
	if len(criteria) != 1 || len(criteria[0]) == 0 {
		return nil, false
	}
	eq := make(map[string]string, len(criteria[0]))
	for k, v := range criteria[0] {
		s, ok := v.(string)
		if !ok || k == "" || strings.Contains(k, ".") {
			return nil, false
		}
		eq[k] = s
	}
	return eq, true
}

// rawQuerier is implemented by the patched go-orbit-db document store (third_party/go-orbit-db).
type rawQuerier interface {
	QueryRaw(ctx context.Context, equal map[string]string, filter func(doc interface{}) (bool, error)) ([][]byte, error)
	GetRaw(key string) []byte
}

// queryLocalRaw answers a query from the local replica with the stored JSON of the matching documents: the
// same documents as queryLocalDB, without decoding them for the response and encoding them again. ok is
// false when the store cannot (the caller then uses queryLocalDB).
func queryLocalRaw(kb *KnowledgeBaseDB, criteria []map[string]interface{}, dstype string) (RawDocs, bool, error) {
	store, err := resolveDocStoreByType(kb, dstype)
	if err != nil || store == nil {
		return nil, false, err
	}
	rq, ok := store.(rawQuerier)
	if !ok {
		return nil, false, nil
	}
	if id, ok := idOnlyCriteria(criteria); ok {
		if strings.Contains(id, " ") {
			return nil, false, nil // multi-term keys are matched differently by Get: use queryLocalDB
		}
		b := rq.GetRaw(id)
		if b == nil {
			return RawDocs{}, true, nil
		}
		// queryLocalDB returns only JSON objects: anything else goes through it (and its error handling)
		if t := bytes.TrimLeft(b, " \t\r\n"); len(t) == 0 || t[0] != '{' || !json.Valid(b) {
			return nil, false, nil
		}
		return RawDocs{b}, true, nil
	}
	// Only string equalities on top-level fields, all of them indexed: the index answers exactly, no decoding.
	if eq, ok := exactEqualities(criteria); ok {
		if ex, ok := store.(exactRawQuerier); ok {
			if docs, ok := ex.QueryRawExact(eq); ok {
				return RawDocs(docs), true, nil
			}
		}
	}
	docs, err := rq.QueryRaw(context.Background(), indexableEqualities(criteria), criteriaFilter(criteria))
	if err != nil {
		return nil, true, err
	}
	return RawDocs(docs), true, nil
}

// LocalDocRaw returns the stored JSON of the document with this id in the local replica of store dstype, as
// the query command with criteria [{"_id": id}] would return it locally. ok is false when the store cannot
// answer this way (the caller then uses the query command); found is false when there is no such document.
func LocalDocRaw(kb *KnowledgeBaseDB, dstype, id string) (doc []byte, found bool, ok bool) {
	raw, ok, err := queryLocalRaw(kb, []map[string]interface{}{{"_id": id}}, dstype)
	if !ok || err != nil {
		return nil, false, false
	}
	if len(raw) == 0 {
		return nil, false, true
	}
	return raw[0], true, true
}
