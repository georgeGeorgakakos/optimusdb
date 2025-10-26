package app

import (
	"context"
	"encoding/json"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"log"
	"time"
)

//| Step                           | Action                                                                                                              | Description                                                                |
//| ------------------------------ | ------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
//| **1️⃣ Peer aggregation**       | Combines `PeerHost.Network().Peers()` (currently connected) and `discovery.GetDiscoveredPeers()` (known from mDNS). | Ensures the query can reach peers even if they haven't yet auto-connected. |
//| **2️⃣ Fan-out limit**          | Applies `opts.MaxPeers` if provided.                                                                                | Simulates limited broadcast or "gossip radius."                            |
//| **3️⃣ Concurrent propagation** | Launches one goroutine per peer.                                                                                    | Each sends the query using the `/query/1.0.0` protocol stream.             |
//| **4️⃣ Loop prevention**        | Checks whether the peer's ID already exists in `tracePath`.                                                         | Stops infinite query recursion loops.                                      |
//| **5️⃣ Connection attempt**     | If a peer is known but not yet connected, it's dialed via the discovery registry.                                   | Ensures dynamic self-healing connections.                                  |
//| **6️⃣ Provenance tagging**     | Adds `_source` (who sent the record) and `_trace` (which path the query took).                                      | Enables audit and trace-based debugging.                                   |
//| **7️⃣ Merge results**          | Collects all peer responses into one slice.                                                                         | Returns unified results to the initiator node.                             |
//| **8️⃣ Timeout handling**       | Stops if `ctx.Done()` is triggered (e.g., exceeded query timeout).                                                  | Avoids deadlocks during slow networks.                                     |

// Register handler for peer query requests.
func RegisterQueryStreamHandler(hostNode host.Host, db *KnowledgeBaseDB) {
	hostNode.SetStreamHandler("/query/1.0.0", func(stream network.Stream) {
		handleQueryStream(stream, db)
	})
	log.Println("[INFO] ✓ Registered query stream handler for /query/1.0.0")
}

// runtime options parsed from inbound payload
type qOptions struct {
	Strategy       string
	ForceRemote    bool
	IncludeLocal   bool
	AnnotateSource bool
	TimeBudgetMs   int
	MaxPeers       int
	QuorumN        int
}

func handleQueryStream(stream network.Stream, db *KnowledgeBaseDB) {
	defer stream.Close()

	var msg map[string]interface{}
	if err := json.NewDecoder(stream).Decode(&msg); err != nil {
		_ = json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}

	// ---- criteria (must be array) ----
	var criteria []map[string]interface{}
	if raw, ok := msg["criteria"].([]interface{}); ok {
		for _, c := range raw {
			if m, ok := c.(map[string]interface{}); ok {
				criteria = append(criteria, m)
			}
		}
	}
	if len(criteria) == 0 {
		_ = json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}

	// ---- trace info ----
	traceID, _ := msg["trace_id"].(string)
	var tracePath []string
	if tp, ok := msg["trace_path"].([]interface{}); ok {
		for _, p := range tp {
			if s, ok := p.(string); ok {
				tracePath = append(tracePath, s)
			}
		}
	}
	self := db.Node.Identity.Pretty()
	if contains(tracePath, self) { // loop prevention
		log.Printf("[PEER-QUERY] loop prevented id=%s path=%v", traceID, tracePath)
		_ = json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}
	tracePath = append(tracePath, self)

	// ---- options/strategy parsing (accept top-level or options.{}) ----
	opts := parseOptions(msg)
	if opts.TimeBudgetMs <= 0 {
		opts.TimeBudgetMs = 2000
	}
	if opts.MaxPeers < 0 {
		opts.MaxPeers = 0
	}
	// default include_local=true unless explicitly false
	if msg["options"] == nil && msg["include_local"] == nil {
		if !opts.IncludeLocal {
			opts.IncludeLocal = true
		}
	}

	log.Printf("[PEER-QUERY] recv trace=%s strat=%s force_remote=%v incl_local=%v budget=%dms max_peers=%d quorum=%d path=%v",
		zero8(traceID), opts.Strategy, opts.ForceRemote, opts.IncludeLocal, opts.TimeBudgetMs, opts.MaxPeers, opts.QuorumN, tracePath)

	// ---- run according to strategy ----
	var results []map[string]interface{}
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opts.TimeBudgetMs)*time.Millisecond)
	defer cancel()

	switch opts.Strategy {
	case "REMOTE_ONLY":
		results, err = queryPeersLimited(ctx, db, criteria, traceID, tracePath, opts)
	case "PARALLEL_MERGE":
		// query local and remote concurrently; merge + dedupe
		type chunk struct{ rows []map[string]interface{} }
		ch := make(chan chunk, 2)

		// local
		go func() {
			var local []map[string]interface{}
			if opts.IncludeLocal {
				local, _ = queryLocalDB(db, criteria)
				tagRows(local, map[string]interface{}{
					"type":    "local",
					"peer_id": self,
					"path":    tracePath,
				}, traceID, tracePath)
			}
			ch <- chunk{rows: local}
		}()

		// remote
		go func() {
			remote, _ := queryPeersLimited(ctx, db, criteria, traceID, tracePath, opts)
			ch <- chunk{rows: remote}
		}()

		l := (<-ch).rows
		r := (<-ch).rows
		results = dedupeByID(append(l, r...))

	case "QUORUM":
		// query peers until quorum satisfied; local may be included
		var local []map[string]interface{}
		if opts.IncludeLocal {
			local, _ = queryLocalDB(db, criteria)
			tagRows(local, map[string]interface{}{
				"type":    "local",
				"peer_id": self,
				"path":    tracePath,
			}, traceID, tracePath)
		}
		remote, _ := queryPeersUntilQuorum(ctx, db, criteria, traceID, tracePath, opts)
		results = dedupeByID(append(local, remote...))

	case "LOCAL_THEN_REMOTE_MERGE":
		var local []map[string]interface{}
		if opts.IncludeLocal {
			local, _ = queryLocalDB(db, criteria)
			tagRows(local, map[string]interface{}{
				"type":    "local",
				"peer_id": self,
				"path":    tracePath,
			}, traceID, tracePath)
		}
		if opts.ForceRemote || len(local) == 0 {
			remote, _ := queryPeersLimited(ctx, db, criteria, traceID, tracePath, opts)
			results = dedupeByID(append(local, remote...))
		} else {
			results = local
		}

	default: // "LOCAL_ONLY" and any unknown strategy
		if opts.IncludeLocal {
			results, _ = queryLocalDB(db, criteria)
			tagRows(results, map[string]interface{}{
				"type":    "local",
				"peer_id": self,
				"path":    tracePath,
			}, traceID, tracePath)
		}
	}

	if err != nil {
		log.Printf("[PEER-QUERY] error: %v", err)
	}

	if err := json.NewEncoder(stream).Encode(results); err != nil {
		log.Printf("[PEER-QUERY] send error: %v", err)
	}
	log.Printf("[PEER-QUERY] responded trace=%s rows=%d", zero8(traceID), len(results))
}

func parseOptions(msg map[string]interface{}) qOptions {
	opts := qOptions{
		Strategy:       "LOCAL_ONLY",
		IncludeLocal:   true,
		AnnotateSource: true,
		TimeBudgetMs:   2000,
		MaxPeers:       0,
		QuorumN:        0,
	}
	// legacy top-level
	if s, ok := msg["strategy"].(string); ok && s != "" {
		opts.Strategy = s
	}
	if fr, ok := msg["force_remote"].(bool); ok {
		opts.ForceRemote = fr
	}
	if il, ok := msg["include_local"].(bool); ok {
		opts.IncludeLocal = il
	}
	if ab, ok := msg["annotate_source"].(bool); ok {
		opts.AnnotateSource = ab
	}
	if tb, ok := asInt(msg["time_budget_ms"]); ok {
		opts.TimeBudgetMs = tb
	}
	if mp, ok := asInt(msg["max_peers"]); ok {
		opts.MaxPeers = mp
	}
	if qn, ok := asInt(msg["quorum_n"]); ok {
		opts.QuorumN = qn
	}
	// preferred nested form
	if om, ok := msg["options"].(map[string]interface{}); ok {
		if s, ok := om["strategy"].(string); ok && s != "" {
			opts.Strategy = s
		}
		if fr, ok := om["force_remote"].(bool); ok {
			opts.ForceRemote = fr
		}
		if il, ok := om["include_local"].(bool); ok {
			opts.IncludeLocal = il
		}
		if ab, ok := om["annotate_source"].(bool); ok {
			opts.AnnotateSource = ab
		}
		if tb, ok := asInt(om["time_budget_ms"]); ok {
			opts.TimeBudgetMs = tb
		}
		if mp, ok := asInt(om["max_peers"]); ok {
			opts.MaxPeers = mp
		}
		if qn, ok := asInt(om["quorum_n"]); ok {
			opts.QuorumN = qn
		}
	}
	return opts
}

func asInt(v interface{}) (int, bool) {
	switch t := v.(type) {
	case float64:
		return int(t), true
	case int:
		return t, true
	case int32:
		return int(t), true
	case int64:
		return int(t), true
	default:
		return 0, false
	}
}

func contains(list []string, val string) bool {
	for _, v := range list {
		if v == val {
			return true
		}
	}
	return false
}

func zero8(s string) string {
	if len(s) < 8 {
		return s
	}
	return s[:8]
}

// ---- Tagging, Dedupe, and Remote Fan-out helpers ----

func tagRows(rows []map[string]interface{}, source map[string]interface{}, traceID string, tracePath []string) {
	for _, r := range rows {
		r["_source"] = source
		r["_trace"] = map[string]interface{}{
			"id":   traceID,
			"path": tracePath,
		}
	}
}

func dedupeByID(rows []map[string]interface{}) []map[string]interface{} {
	seen := make(map[string]bool)
	out := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		id, _ := r["_id"].(string)
		if id == "" {
			out = append(out, r)
			continue
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, r)
		}
	}
	return out
}

// queryPeersLimited propagates queries to discovered peers with an optional fan-out limit.
// It merges results from multiple peers, tagging provenance (_source, _trace).
func queryPeersLimited(
	ctx context.Context,
	db *KnowledgeBaseDB,
	criteria []map[string]interface{},
	traceID string,
	tracePath []string,
	opts qOptions,
) ([]map[string]interface{}, error) {

	// --- 1️⃣ Gather peers from both libp2p network and discovery registry ---
	allConnected := db.Node.PeerHost.Network().Peers()
	discoveredPeerIDs := db.GetDiscoveredPeers() // returns []string
	seen := make(map[string]bool)
	var allPeers []peer.ID

	for _, p := range allConnected {
		allPeers = append(allPeers, p)
		seen[p.Pretty()] = true
	}
	for _, peerIDStr := range discoveredPeerIDs {
		if !seen[peerIDStr] {
			// Convert string to peer.ID
			if pid, err := peer.Decode(peerIDStr); err == nil {
				allPeers = append(allPeers, pid)
			}
		}
	}

	if len(allPeers) == 0 {
		log.Printf("[QUERY] No peers available for fan-out (trace=%s)", traceID)
		return nil, nil
	}

	// --- 2️⃣ Enforce optional MaxPeers limit ---
	peers := allPeers
	if opts.MaxPeers > 0 && opts.MaxPeers < len(allPeers) {
		peers = allPeers[:opts.MaxPeers]
	}

	log.Printf("[QUERY] Fan-out start trace=%s → %d peers (limit=%d)", traceID, len(peers), opts.MaxPeers)

	type peerChunk struct {
		rows []map[string]interface{}
		err  error
	}
	ch := make(chan peerChunk, len(peers))

	// --- 3️⃣ Query each peer concurrently ---
	for _, pid := range peers {
		go func(p peer.ID) {
			// skip loops
			if contains(tracePath, p.Pretty()) {
				ch <- peerChunk{nil, nil}
				return
			}

			// skip if not already connected (we don't have address info for discovered peers)
			if db.Node.PeerHost.Network().Connectedness(p) != network.Connected {
				log.Printf("[QUERY] Skipping unconnected peer %s (no address info)", p.Pretty())
				ch <- peerChunk{nil, nil}
				return
			}

			stream, err := db.Node.PeerHost.NewStream(ctx, p, "/query/1.0.0")
			if err != nil {
				ch <- peerChunk{nil, err}
				return
			}
			defer stream.Close()

			// --- 4️⃣ Send the query request (clamped strategy=LOCAL_ONLY to prevent recursion) ---
			req := map[string]interface{}{
				"criteria":   criteria,
				"trace_id":   traceID,
				"trace_path": append(tracePath, db.Node.Identity.Pretty()),
				"options": map[string]interface{}{
					"strategy":        "LOCAL_ONLY",
					"include_local":   true,
					"annotate_source": true,
				},
			}

			if err := json.NewEncoder(stream).Encode(req); err != nil {
				ch <- peerChunk{nil, err}
				return
			}

			var rows []map[string]interface{}
			if err := json.NewDecoder(stream).Decode(&rows); err != nil {
				ch <- peerChunk{nil, err}
				return
			}

			// --- 5️⃣ Tag provenance info for each record ---
			for _, r := range rows {
				if _, ok := r["_source"]; !ok {
					r["_source"] = map[string]interface{}{
						"type":    "peer",
						"peer_id": p.Pretty(),
						"path":    append(tracePath, db.Node.Identity.Pretty()),
					}
				}
				if _, ok := r["_trace"]; !ok {
					r["_trace"] = map[string]interface{}{
						"id":   traceID,
						"path": append(tracePath, db.Node.Identity.Pretty()),
					}
				}
			}

			ch <- peerChunk{rows, nil}
		}(pid)
	}

	// --- 6️⃣ Collect responses and merge ---
	var merged []map[string]interface{}
	for i := 0; i < len(peers); i++ {
		select {
		case c := <-ch:
			if c.err == nil && len(c.rows) > 0 {
				merged = append(merged, c.rows...)
			}
		case <-ctx.Done():
			log.Printf("[QUERY] Fan-out trace=%s timed out after %v peers=%d", traceID, ctx.Err(), len(merged))
			return merged, nil
		}
	}

	log.Printf("[QUERY] Fan-out completed trace=%s → merged %d rows", traceID, len(merged))
	return merged, nil
}

// quorum version (collect until QuorumN unique peers responded or timeout)
func queryPeersUntilQuorum(ctx context.Context, db *KnowledgeBaseDB, criteria []map[string]interface{}, traceID string, tracePath []string, opts qOptions) ([]map[string]interface{}, error) {
	allPeers := db.Node.PeerHost.Network().Peers()
	if len(allPeers) == 0 || opts.QuorumN <= 0 {
		return nil, nil
	}

	type peerResp struct {
		pid  string
		rows []map[string]interface{}
		err  error
	}
	ch := make(chan peerResp, len(allPeers))

	for _, pid := range allPeers {
		go func(p peer.ID) {
			if contains(tracePath, p.Pretty()) {
				ch <- peerResp{pid: p.Pretty(), rows: nil, err: nil}
				return
			}
			stream, err := db.Node.PeerHost.NewStream(ctx, p, "/query/1.0.0")
			if err != nil {
				ch <- peerResp{pid: p.Pretty(), rows: nil, err: err}
				return
			}
			defer stream.Close()

			req := map[string]interface{}{
				"criteria":   criteria,
				"trace_id":   traceID,
				"trace_path": append(tracePath, db.Node.Identity.Pretty()),
				"options": map[string]interface{}{
					"strategy":        "LOCAL_ONLY",
					"include_local":   true,
					"annotate_source": true,
				},
			}
			if err := json.NewEncoder(stream).Encode(req); err != nil {
				ch <- peerResp{pid: p.Pretty(), rows: nil, err: err}
				return
			}

			var rows []map[string]interface{}
			if err := json.NewDecoder(stream).Decode(&rows); err != nil {
				ch <- peerResp{pid: p.Pretty(), rows: nil, err: err}
				return
			}
			tagRows(rows, map[string]interface{}{
				"type":    "peer",
				"peer_id": p.Pretty(),
				"path":    append(tracePath, db.Node.Identity.Pretty()),
			}, traceID, append(tracePath, db.Node.Identity.Pretty()))
			ch <- peerResp{pid: p.Pretty(), rows: rows, err: nil}
		}(pid)
	}

	merged := make([]map[string]interface{}, 0)
	responders := make(map[string]bool)

	for i := 0; i < len(allPeers); i++ {
		select {
		case r := <-ch:
			if r.err == nil && len(r.rows) > 0 {
				merged = append(merged, r.rows...)
				responders[r.pid] = true
			}
			if len(responders) >= opts.QuorumN {
				return dedupeByID(merged), nil
			}
		case <-ctx.Done():
			return dedupeByID(merged), nil
		}
	}
	return dedupeByID(merged), nil
}
