package queryengine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
)

// CacheEntry represents a cached query result
type CacheEntry struct {
	Data      []map[string]interface{}
	ExpiresAt time.Time
	CreatedAt time.Time
}

// SimpleCache implements a basic TTL cache
type SimpleCache struct {
	entries map[string]*CacheEntry
	ttl     time.Duration
	mu      sync.RWMutex
	hits    int64
	misses  int64
	hitsMu  sync.Mutex
}

// NewSimpleCache creates a new cache with TTL
func NewSimpleCache(ttl time.Duration) *SimpleCache {
	cache := &SimpleCache{
		entries: make(map[string]*CacheEntry),
		ttl:     ttl,
	}

	// Start cleanup goroutine
	go cache.cleanup()

	return cache
}

// Get retrieves a value from cache
func (sc *SimpleCache) Get(criteria []map[string]interface{}) ([]map[string]interface{}, bool) {
	key := sc.generateKey(criteria)

	sc.mu.RLock()
	entry, exists := sc.entries[key]
	sc.mu.RUnlock()

	if !exists {
		sc.recordMiss()
		return nil, false
	}

	// Check if expired
	if time.Now().After(entry.ExpiresAt) {
		sc.recordMiss()
		return nil, false
	}

	sc.recordHit()
	log.Printf("[CACHE] Hit! Returning %d cached results", len(entry.Data))
	return entry.Data, true
}

// Set stores a value in cache
func (sc *SimpleCache) Set(criteria []map[string]interface{}, data []map[string]interface{}) {
	key := sc.generateKey(criteria)

	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.entries[key] = &CacheEntry{
		Data:      data,
		ExpiresAt: time.Now().Add(sc.ttl),
		CreatedAt: time.Now(),
	}

	log.Printf("[CACHE] Stored %d results (expires in %v)", len(data), sc.ttl)
}

// generateKey creates a cache key from criteria
func (sc *SimpleCache) generateKey(criteria []map[string]interface{}) string {
	data, _ := json.Marshal(criteria)
	hash := sha256.Sum256(data)
	return fmt.Sprintf("%x", hash)
}

// recordHit increments hit counter
func (sc *SimpleCache) recordHit() {
	sc.hitsMu.Lock()
	sc.hits++
	sc.hitsMu.Unlock()
}

// recordMiss increments miss counter
func (sc *SimpleCache) recordMiss() {
	sc.hitsMu.Lock()
	sc.misses++
	sc.hitsMu.Unlock()
}

// cleanup removes expired entries
func (sc *SimpleCache) cleanup() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		sc.mu.Lock()
		now := time.Now()
		removed := 0
		for key, entry := range sc.entries {
			if now.After(entry.ExpiresAt) {
				delete(sc.entries, key)
				removed++
			}
		}
		if removed > 0 {
			log.Printf("[CACHE] Cleanup: removed %d expired entries", removed)
		}
		sc.mu.Unlock()
	}
}

// Stats returns cache statistics
func (sc *SimpleCache) Stats() map[string]interface{} {
	sc.mu.RLock()
	entries := len(sc.entries)
	sc.mu.RUnlock()

	sc.hitsMu.Lock()
	hits := sc.hits
	misses := sc.misses
	sc.hitsMu.Unlock()

	total := hits + misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(hits) / float64(total) * 100
	}

	return map[string]interface{}{
		"entries":  entries,
		"ttl":      sc.ttl.String(),
		"hits":     hits,
		"misses":   misses,
		"hit_rate": fmt.Sprintf("%.2f%%", hitRate),
	}
}

// Clear removes all entries from cache
func (sc *SimpleCache) Clear() {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	sc.entries = make(map[string]*CacheEntry)
	log.Println("[CACHE] Cleared all entries")
}
