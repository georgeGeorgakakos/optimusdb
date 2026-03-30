package semantic

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// SearchHandler — GET /api/v1/semantic/search?q=...&top_k=10&budget_ms=1500
func (idx *Index) SearchHandler(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeErr(w, http.StatusBadRequest, "q is required")
		return
	}

	topK := DefaultTopK
	if s := r.URL.Query().Get("top_k"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			topK = n
		}
	}

	budget := DefaultBudget
	if s := r.URL.Query().Get("budget_ms"); s != "" {
		if ms, err := strconv.Atoi(s); err == nil && ms > 0 {
			budget = time.Duration(ms) * time.Millisecond
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), budget+200*time.Millisecond)
	defer cancel()

	results, err := idx.Search(ctx, q, topK, budget)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"query":   q,
		"count":   len(results),
		"results": results,
	})
}

// IndexHandler — POST /api/v1/semantic/index
//
// Body: { "store": "dsswres", "doc_id": "asset_001", "fields": { "name": "...", ... } }
func (idx *Index) IndexHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Store  string            `json:"store"`
		DocID  string            `json:"doc_id"`
		Fields map[string]string `json:"fields"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DocID == "" {
		writeErr(w, http.StatusBadRequest, "store, doc_id and fields required")
		return
	}
	if err := idx.IndexDocument(body.Store, body.DocID, body.Fields); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "indexed", "doc_id": body.DocID})
}

// BootstrapHandler — POST /api/v1/semantic/bootstrap
//
// Body: { "doc_id": "asset_001", "ipfs_cid": "QmXyz..." }
//
// Fetches the embedding blob from IPFS (pinned by the originating node)
// and inserts it into the local sqlite-vec index without calling llama-server.
func (idx *Index) BootstrapHandler(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DocID   string `json:"doc_id"`
		IPFSCid string `json:"ipfs_cid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DocID == "" || body.IPFSCid == "" {
		writeErr(w, http.StatusBadRequest, "doc_id and ipfs_cid required")
		return
	}
	if err := idx.BootstrapFromIPFS(r.Context(), body.DocID, body.IPFSCid); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "bootstrapped", "doc_id": body.DocID})
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
