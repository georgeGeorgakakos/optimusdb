package semantic

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	iface "berty.tech/go-orbit-db/iface"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/path"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
)

const (
	TopicSearch  = "/optimusdb/semantic/search/1.0.0"
	TopicResults = "/optimusdb/semantic/results/1.0.0"

	EmbedDim      = 2048
	DefaultTopK   = 10
	DefaultBudget = 1500 * time.Millisecond
)

// SearchQuery is broadcast on TopicSearch by the query originator.
type SearchQuery struct {
	CorrelationID string    `json:"cid"`
	RequesterID   string    `json:"requester"`
	Vector        []float32 `json:"vector"`
	TopK          int       `json:"top_k"`
	DeadlineUnix  int64     `json:"deadline"`
}

// SearchReply is published on TopicResults by each responding peer.
// Peers return doc_id + score + store name only — document content is
// hydrated by the coordinator from its own OrbitDB stores.
type SearchReply struct {
	CorrelationID string         `json:"cid"`
	Results       []SearchResult `json:"results"`
}

// Index owns the sqlite-vec virtual table, llama-server embed connection,
// GossipSub topics, and the optional DocFetcher for result hydration.
type Index struct {
	db       *sql.DB
	llamaURL string
	orbit    *iface.OrbitDB
	host     host.Host
	ps       *pubsub.PubSub

	// fetcher hydrates full document content after ANN — set via WithFetcher().
	// nil = doc_id + score only (backward-compatible).
	fetcher DocFetcher

	searchTopic  *pubsub.Topic
	resultsTopic *pubsub.Topic
	searchSub    *pubsub.Subscription
	resultsSub   *pubsub.Subscription

	mu      sync.Mutex
	pending map[string]chan []SearchResult
}

// New creates the Index. Call WithFetcher() immediately after to enable
// document hydration in search results.
func New(db *sql.DB, llamaURL string, orbit *iface.OrbitDB, h host.Host, ps *pubsub.PubSub) (*Index, error) {
	idx := &Index{
		db:       db,
		llamaURL: strings.TrimRight(llamaURL, "/"),
		orbit:    orbit,
		host:     h,
		ps:       ps,
		pending:  make(map[string]chan []SearchResult),
	}
	if err := idx.migrate(); err != nil {
		return nil, fmt.Errorf("semantic migrate: %w", err)
	}
	if err := idx.joinTopics(); err != nil {
		return nil, fmt.Errorf("semantic topics: %w", err)
	}
	go idx.handleIncomingQueries()
	go idx.handleIncomingReplies()
	return idx, nil
}

// WithFetcher sets the DocFetcher used to hydrate document content in search
// results. Call immediately after New() before the index receives queries.
//
//	semanticIdx, _ := semantic.New(...)
//	semanticIdx.WithFetcher(knowledgeBaseDB)  // KnowledgeBaseDB implements DocFetcher
func (idx *Index) WithFetcher(f DocFetcher) *Index {
	idx.fetcher = f
	return idx
}

// ── Schema ────────────────────────────────────────────────────────────────────

func (idx *Index) migrate() error {
	// Load vec0 as a SQLite loadable extension.
	// Requires _allow_load_extension=1 in the DSN (set in app/app.go InitSQLite)
	// and /usr/lib/sqlite-vec/vec0.so present in the image (copied in Dockerfile).
	if _, err := idx.db.Exec(`SELECT load_extension('/usr/lib/sqlite-vec/vec0')`); err != nil {
		return fmt.Errorf("load vec0 extension (/usr/lib/sqlite-vec/vec0.so missing?): %w", err)
	}

	_, err := idx.db.Exec(fmt.Sprintf(`
		CREATE VIRTUAL TABLE IF NOT EXISTS vec_embeddings USING vec0(
			doc_id    TEXT PRIMARY KEY,
			embedding float[%d]
		)`, EmbedDim))
	if err != nil {
		return fmt.Errorf("create vec_embeddings (is sqlite-vec loaded?): %w", err)
	}
	_, err = idx.db.Exec(`
		CREATE TABLE IF NOT EXISTS vec_meta (
			doc_id      TEXT PRIMARY KEY,
			ipfs_cid    TEXT,
			store_name  TEXT,
			indexed_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
			source_text TEXT
		)
	`)
	return err
}

// ── GossipSub ─────────────────────────────────────────────────────────────────

func (idx *Index) joinTopics() error {
	var err error
	if idx.searchTopic, err = idx.ps.Join(TopicSearch); err != nil {
		return fmt.Errorf("join search topic: %w", err)
	}
	if idx.resultsTopic, err = idx.ps.Join(TopicResults); err != nil {
		return fmt.Errorf("join results topic: %w", err)
	}
	if idx.searchSub, err = idx.searchTopic.Subscribe(); err != nil {
		return fmt.Errorf("subscribe search: %w", err)
	}
	if idx.resultsSub, err = idx.resultsTopic.Subscribe(); err != nil {
		return fmt.Errorf("subscribe results: %w", err)
	}
	return nil
}

// handleIncomingQueries answers search queries from other nodes.
// Peers return doc_id + score + store — no document content.
// Document hydration happens only at the coordinator (the node that
// initiated the Search() call), where OrbitDB stores are directly accessible.
func (idx *Index) handleIncomingQueries() {
	selfID := idx.host.ID().String()
	for {
		msg, err := idx.searchSub.Next(context.Background())
		if err != nil {
			return
		}
		if msg.ReceivedFrom.String() == selfID {
			continue
		}
		var q SearchQuery
		if err := json.Unmarshal(msg.Data, &q); err != nil {
			continue
		}
		if time.Now().UnixMilli() > q.DeadlineUnix {
			continue
		}
		go func(q SearchQuery) {
			results, err := idx.localANN(q.Vector, q.TopK)
			if err != nil || len(results) == 0 {
				return
			}
			for i := range results {
				results[i].SourceNode = selfID
				// Intentionally omit Document here — peers don't serialise
				// full documents over GossipSub (bandwidth + latency cost).
				// The coordinator fetches content from its local replica.
			}
			reply := SearchReply{CorrelationID: q.CorrelationID, Results: results}
			data, _ := json.Marshal(reply)
			_ = idx.resultsTopic.Publish(context.Background(), data)
		}(q)
	}
}

func (idx *Index) handleIncomingReplies() {
	for {
		msg, err := idx.resultsSub.Next(context.Background())
		if err != nil {
			return
		}
		var reply SearchReply
		if err := json.Unmarshal(msg.Data, &reply); err != nil {
			continue
		}
		idx.mu.Lock()
		ch, ok := idx.pending[reply.CorrelationID]
		idx.mu.Unlock()
		if ok {
			select {
			case ch <- reply.Results:
			default:
			}
		}
	}
}

// ── Public API ────────────────────────────────────────────────────────────────
// IndexDocument embeds fields, writes to sqlite-vec, and pins to IPFS.
func (idx *Index) IndexDocument(storeName, docID string, fields map[string]string) error {
	text := buildIndexText(fields)
	vector, err := idx.embed(text)
	if err != nil {
		return fmt.Errorf("embed %s/%s: %w", storeName, docID, err)
	}
	blob := encodeVec(vector)
	if _, err := idx.db.Exec(`
		INSERT OR REPLACE INTO vec_embeddings(doc_id, embedding)
		VALUES (?, ?)`, docID, blob); err != nil {
		return fmt.Errorf("vec upsert %s: %w", docID, err)
	}
	cid := idx.pinToIPFS(blob)
	_, err = idx.db.Exec(`
		INSERT OR REPLACE INTO vec_meta(doc_id, ipfs_cid, store_name, source_text)
		VALUES (?, ?, ?, ?)`, docID, cid, storeName, text)
	return err
}

// BootstrapFromIPFS fetches a peer's embedding blob and inserts it locally.
func (idx *Index) BootstrapFromIPFS(ctx context.Context, docID, ipfsCID string) error {
	coreAPI := (*idx.orbit).IPFS()
	pth := path.New("/ipfs/" + ipfsCID)
	node, err := coreAPI.Unixfs().Get(ctx, pth)
	if err != nil {
		return fmt.Errorf("ipfs get %s: %w", ipfsCID, err)
	}
	f, ok := node.(files.File)
	if !ok {
		return fmt.Errorf("ipfs CID %s is not a file", ipfsCID)
	}
	blob, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read ipfs blob: %w", err)
	}
	_, err = idx.db.Exec(`
		INSERT OR IGNORE INTO vec_embeddings(doc_id, embedding)
		VALUES (?, ?)`, docID, blob)
	return err
}

// Search runs a hybrid local-ANN + GossipSub distributed semantic search.
// When a DocFetcher has been registered via WithFetcher(), each result whose
// SourceNode matches this node will have its Document field populated with
// the full OrbitDB document content.
func (idx *Index) Search(ctx context.Context, query string, topK int, budget time.Duration) ([]SearchResult, error) {
	if topK <= 0 {
		topK = DefaultTopK
	}
	if budget <= 0 {
		budget = DefaultBudget
	}

	vector, err := idx.embed(query)
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	selfID := idx.host.ID().String()

	// 1. Local ANN.
	local, err := idx.localANN(vector, topK)
	if err != nil {
		return nil, fmt.Errorf("local ANN: %w", err)
	}
	for i := range local {
		local[i].SourceNode = selfID
	}

	// 2. Fan-out via GossipSub.
	corrID := fmt.Sprintf("%d", time.Now().UnixNano())
	deadline := time.Now().Add(budget)

	replyCh := make(chan []SearchResult, 64)
	idx.mu.Lock()
	idx.pending[corrID] = replyCh
	idx.mu.Unlock()
	defer func() {
		idx.mu.Lock()
		delete(idx.pending, corrID)
		idx.mu.Unlock()
	}()

	sqMsg := SearchQuery{
		CorrelationID: corrID,
		RequesterID:   selfID,
		Vector:        vector,
		TopK:          topK,
		DeadlineUnix:  deadline.UnixMilli(),
	}
	data, _ := json.Marshal(sqMsg)
	if err := idx.searchTopic.Publish(ctx, data); err != nil {
		// Non-fatal — return local results only, still hydrated.
		return idx.enrichResults(ctx, rankAndTrim(local, topK), selfID), nil
	}

	// 3. Collect peer replies within budget.
	all := append([]SearchResult(nil), local...)
	timer := time.NewTimer(budget)
	defer timer.Stop()
	for {
		select {
		case peerResults := <-replyCh:
			all = append(all, peerResults...)
		case <-timer.C:
			// 4. Merge, deduplicate, rank, then hydrate local docs.
			ranked := rankAndTrim(all, topK)
			return idx.enrichResults(ctx, ranked, selfID), nil
		case <-ctx.Done():
			ranked := rankAndTrim(all, topK)
			return idx.enrichResults(ctx, ranked, selfID), nil
		}
	}
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (idx *Index) embed(text string) ([]float32, error) {
	body, _ := json.Marshal(map[string]string{"content": text})
	resp, err := http.Post(idx.llamaURL+"/embedding", "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("llama /embedding: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out struct {
		Embedding []float32 `json:"embedding"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("parse embedding response: %w", err)
	}
	if len(out.Embedding) == 0 {
		return nil, fmt.Errorf("empty embedding — add --embedding flag to llama-server")
	}
	return out.Embedding, nil
}

func (idx *Index) pinToIPFS(blob []byte) string {
	if idx.orbit == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coreAPI := (*idx.orbit).IPFS()
	pth, err := coreAPI.Unixfs().Add(ctx, files.NewBytesFile(blob))
	if err != nil {
		return ""
	}
	_ = coreAPI.Pin().Add(ctx, pth)
	return pth.Cid().String()
}

func (idx *Index) localANN(vector []float32, topK int) ([]SearchResult, error) {
	blob := encodeVec(vector)
	rows, err := idx.db.Query(`
		SELECT e.doc_id, e.distance, m.store_name
		FROM   vec_embeddings e
		LEFT JOIN vec_meta m ON m.doc_id = e.doc_id
		WHERE  e.embedding MATCH ?
		  AND  k = ?
		ORDER  BY e.distance
	`, blob, topK)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []SearchResult
	for rows.Next() {
		var r SearchResult
		var dist float32
		var storeName *string
		if err := rows.Scan(&r.DocID, &dist, &storeName); err != nil {
			continue
		}
		r.Score = 1.0 - dist
		if storeName != nil {
			r.StoreName = *storeName
		}
		results = append(results, r)
	}
	return results, rows.Err()
}

func buildIndexText(fields map[string]string) string {
	priority := []string{
		"name", "description", "generated_description",
		"tags", "generated_tags",
		"type", "asset_type", "status",
		"location", "country", "region",
	}
	var parts []string
	seen := map[string]bool{}
	for _, k := range priority {
		if v, ok := fields[k]; ok && strings.TrimSpace(v) != "" {
			parts = append(parts, v)
			seen[k] = true
		}
	}
	for k, v := range fields {
		if !seen[k] && strings.TrimSpace(v) != "" {
			parts = append(parts, v)
		}
	}
	return strings.Join(parts, " ")
}

func rankAndTrim(results []SearchResult, topK int) []SearchResult {
	best := make(map[string]SearchResult, len(results))
	for _, r := range results {
		if ex, ok := best[r.DocID]; !ok || r.Score > ex.Score {
			best[r.DocID] = r
		}
	}
	ranked := make([]SearchResult, 0, len(best))
	for _, r := range best {
		ranked = append(ranked, r)
	}
	for i := 1; i < len(ranked); i++ {
		for j := i; j > 0 && ranked[j].Score > ranked[j-1].Score; j-- {
			ranked[j], ranked[j-1] = ranked[j-1], ranked[j]
		}
	}
	if len(ranked) > topK {
		return ranked[:topK]
	}
	return ranked
}

func encodeVec(v []float32) []byte {
	buf := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	return buf
}
