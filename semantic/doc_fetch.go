// doc_fetch.go — document hydration after ANN search
//
// After sqlite-vec returns a ranked list of doc_ids, the coordinator fetches
// the actual document content from the originating OrbitDB store so the
// search response includes the full document, not just the doc_id.
//
// Design: DocFetcher is a narrow interface implemented by KnowledgeBaseDB
// in app/app.go. Using an interface keeps the import boundary clean:
//   semantic → interface  (this file defines DocFetcher)
//   app      → implements interface (app/app.go: FetchDocument method)
// No import cycle is created.

package semantic

import (
	"context"
	"encoding/json"
)

// DocFetcher is implemented by KnowledgeBaseDB (app/app.go).
// It resolves a store name + doc _id to the raw document map.
// The live implementation lives in app/app.go as FetchDocument().
type DocFetcher interface {
	FetchDocument(ctx context.Context, storeName, docID string) (map[string]interface{}, error)
}

// SearchResult is one ranked document returned by the search.
// Document is populated for local results (SourceNode == this node).
// It is nil for remote peer results — document content is not serialised
// over GossipSub to keep reply payloads small.
type SearchResult struct {
	DocID      string                 `json:"doc_id"`
	Score      float32                `json:"score"`
	SourceNode string                 `json:"source_node"`
	StoreName  string                 `json:"store,omitempty"`
	Document   map[string]interface{} `json:"document,omitempty"`
}

// enrichResults fetches the full document for each local result after ANN ranking.
// Remote peer results (SourceNode != selfID) are left un-hydrated — fetching
// per-result across the network would blow the budget window.
func (idx *Index) enrichResults(
	ctx context.Context,
	results []SearchResult,
	selfID string,
) []SearchResult {
	if idx.fetcher == nil {
		return results
	}
	for i, r := range results {
		if r.SourceNode != selfID || r.StoreName == "" {
			continue
		}
		doc, err := idx.fetcher.FetchDocument(ctx, r.StoreName, r.DocID)
		if err != nil || doc == nil {
			continue
		}
		// Strip internal OrbitDB housekeeping keys from the payload.
		clean := make(map[string]interface{}, len(doc))
		for k, v := range doc {
			if k == "_id" || k == "_created_at" {
				continue
			}
			clean[k] = v
		}
		results[i].Document = clean
	}
	return results
}

// MarshalJSON serialises SearchResult with Document nested under "document".
func (r SearchResult) MarshalJSON() ([]byte, error) {
	type Alias SearchResult
	return json.Marshal((Alias)(r))
}
