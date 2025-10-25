package query

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

// QueryResult represents a peer's query response
type QueryResult struct {
	PeerID  string
	Data    []map[string]interface{}
	Error   error
	Latency time.Duration
}

// WorkerPoolEngine manages parallel query execution
type WorkerPoolEngine struct {
	maxWorkers   int
	queryTimeout time.Duration
}

// NewWorkerPoolEngine creates a new worker pool
func NewWorkerPoolEngine(maxWorkers int, timeout time.Duration) *WorkerPoolEngine {
	return &WorkerPoolEngine{
		maxWorkers:   maxWorkers,
		queryTimeout: timeout,
	}
}

// QueryWithWorkerPool executes queries in parallel using a worker pool
func (wpe *WorkerPoolEngine) QueryWithWorkerPool(
	ctx context.Context,
	hostNode host.Host,
	criteria []map[string]interface{},
	peers []peer.ID,
) ([]map[string]interface{}, error) {

	if len(peers) == 0 {
		return []map[string]interface{}{}, nil
	}

	// Create context with timeout
	queryCtx, cancel := context.WithTimeout(ctx, wpe.queryTimeout)
	defer cancel()

	// Create job queue and result channel
	jobs := make(chan peer.ID, len(peers))
	results := make(chan QueryResult, len(peers))

	var wg sync.WaitGroup

	// Start workers
	numWorkers := min(wpe.maxWorkers, len(peers))
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go wpe.worker(queryCtx, &wg, hostNode, criteria, jobs, results)
	}

	// Send jobs
	go func() {
		for _, peerID := range peers {
			select {
			case jobs <- peerID:
			case <-queryCtx.Done():
				break
			}
		}
		close(jobs)
	}()

	// Wait for workers
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect and deduplicate results
	return wpe.collectResults(results)
}

// worker processes queries from the job queue
func (wpe *WorkerPoolEngine) worker(
	ctx context.Context,
	wg *sync.WaitGroup,
	hostNode host.Host,
	criteria []map[string]interface{},
	jobs <-chan peer.ID,
	results chan<- QueryResult,
) {
	defer wg.Done()

	for peerID := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
			start := time.Now()
			data, err := wpe.queryPeer(ctx, hostNode, peerID, criteria)
			latency := time.Since(start)

			results <- QueryResult{
				PeerID:  peerID.String(),
				Data:    data,
				Error:   err,
				Latency: latency,
			}
		}
	}
}

// queryPeer sends a query to a single peer
func (wpe *WorkerPoolEngine) queryPeer(
	ctx context.Context,
	hostNode host.Host,
	peerID peer.ID,
	criteria []map[string]interface{},
) ([]map[string]interface{}, error) {

	// Open stream to peer
	stream, err := hostNode.NewStream(ctx, peerID, "/query/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}
	defer stream.Close()

	// Create query message
	queryMsg := map[string]interface{}{
		"criteria": criteria,
	}

	// Send query
	if err := json.NewEncoder(stream).Encode(queryMsg); err != nil {
		return nil, fmt.Errorf("failed to send query: %w", err)
	}

	// Read response
	var results []map[string]interface{}
	if err := json.NewDecoder(stream).Decode(&results); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return results, nil
}

// collectResults aggregates and deduplicates results
func (wpe *WorkerPoolEngine) collectResults(results <-chan QueryResult) ([]map[string]interface{}, error) {
	var allData []map[string]interface{}
	seen := make(map[string]bool)
	errorCount := 0
	successCount := 0

	for result := range results {
		if result.Error != nil {
			log.Printf("[WARN] Query to peer %s failed: %v", result.PeerID, result.Error)
			errorCount++
			continue
		}

		successCount++
		log.Printf("[INFO] Received %d results from peer %s in %v", len(result.Data), result.PeerID, result.Latency)

		// Deduplicate
		for _, item := range result.Data {
			key := generateItemKey(item)
			if !seen[key] {
				seen[key] = true
				allData = append(allData, item)
			}
		}
	}

	log.Printf("[INFO] Query complete: %d successful, %d failed, %d unique results", successCount, errorCount, len(allData))

	return allData, nil
}

// generateItemKey creates a unique key for deduplication
func generateItemKey(item map[string]interface{}) string {
	// Try _id first
	if id, ok := item["_id"].(string); ok {
		return id
	}

	// Fallback to content hash
	data, _ := json.Marshal(item)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
