package query

import (
	"context"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// OptimizedEngine combines worker pool and cache
type OptimizedEngine struct {
	workerPool *WorkerPoolEngine
	cache      *SimpleCache
}

// NewOptimizedEngine creates an optimized query engine
func NewOptimizedEngine(maxWorkers int, timeout time.Duration, cacheTTL time.Duration) *OptimizedEngine {
	log.Printf("[ENGINE] Initializing optimized query engine: workers=%d, timeout=%v, cacheTTL=%v",
		maxWorkers, timeout, cacheTTL)

	return &OptimizedEngine{
		workerPool: NewWorkerPoolEngine(maxWorkers, timeout),
		cache:      NewSimpleCache(cacheTTL),
	}
}

// Query executes an optimized query
func (oe *OptimizedEngine) Query(
	ctx context.Context,
	hostNode host.Host,
	selfID peer.ID,
	criteria []map[string]interface{},
) ([]map[string]interface{}, error) {

	start := time.Now()

	// Check cache first
	if cached, found := oe.cache.Get(criteria); found {
		log.Printf("[ENGINE] Query completed from cache in %v", time.Since(start))
		return cached, nil
	}

	// Get connected peers (excluding self)
	allPeers := hostNode.Peerstore().Peers()
	var peers []peer.ID
	for _, p := range allPeers {
		if p != selfID {
			peers = append(peers, p)
		}
	}

	if len(peers) == 0 {
		log.Println("[ENGINE] No peers available for query")
		return []map[string]interface{}{}, nil
	}

	log.Printf("[ENGINE] Querying %d peers with worker pool...", len(peers))

	// Query peers with worker pool
	results, err := oe.workerPool.QueryWithWorkerPool(ctx, hostNode, criteria, peers)
	if err != nil {
		return nil, err
	}

	// Cache results
	if len(results) > 0 {
		oe.cache.Set(criteria, results)
	}

	log.Printf("[ENGINE] Query completed in %v, found %d results", time.Since(start), len(results))

	return results, nil
}

// CacheStats returns cache statistics
func (oe *OptimizedEngine) CacheStats() map[string]interface{} {
	return oe.cache.Stats()
}

// ClearCache clears all cached results
func (oe *OptimizedEngine) ClearCache() {
	oe.cache.Clear()
}
