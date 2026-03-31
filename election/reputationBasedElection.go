package election

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"optimusdb/logger"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/utilities"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

/*
===================================================================================
OPTIMUSDB LEADER ELECTION - PRODUCTION VERSION v2.4.1 (BLOCKING FIX)
===================================================================================

CHANGELOG v2.4.1 (2026-04-01):
✅ FIX #23: RunFullNode no longer blocks on sigChan — returns immediately after
           launching election goroutines so main.go can proceed to launch the
           semantic search index goroutine. Shutdown is handled by termCtx
           from main.go which is already wired to SIGINT/SIGTERM. All background
           goroutines check ctx.Done() and exit cleanly on shutdown.
           Removed: os/signal, syscall imports (no longer needed).

CHANGELOG v2.4.0 (2026-02-13):
✅ FIX #18: Default coordinator from container name (optimusdb1 auto-promotes)
✅ FIX #19: Election retry loop (replaces broken recursive call that was
           blocked by isElecting atomic flag - 0 retries ever succeeded)
✅ FIX #20: Fallback election now reachable after 3 failed attempts
✅ FIX #21: consecutiveElectionFailures counter triggers self-promotion
✅ FIX #22: finalizeElectionInline returns winner to retry loop

PREVIOUS CHANGELOG v2.3.1 (2025-01-05):
✅ FIX #12: Complete topic recreation (not just subscription cancel)
✅ FIX #13: Faster healing trigger (2 checks = 20s instead of 30s)
✅ FIX #14: Lower term threshold (>15 instead of >20)
✅ FIX #15: Robust leader empty detection (multiple formats)
✅ FIX #16: Startup term validation (checks on init)
✅ FIX #17: Aggressive mesh recovery (5 test messages, longer wait)

PREVIOUS FIXES (v2.3):
✅ FIX #7: Block election participation when mesh is empty
✅ FIX #8: Aggressive mesh healing with forced re-subscription
✅ FIX #9: Continuous mesh monitoring during elections
✅ FIX #10: Term reconciliation to detect and fix split-brain
✅ FIX #11: Emergency re-sync when high term + no mesh + no leader

CRITICAL CHANGES FROM v2.3:
1. emergencyMeshHealing() now CLOSES topic completely and rejoins
2. checkTermDivergence() triggers at term >15 (not >20)
3. Leader empty check: handles "", "<peer.ID  >", "<peer.ID >", or len<10
4. Mesh healing triggers after 2 consecutive checks (not 3)
5. Sends 5 test messages (not 3) with longer delays
6. Validates term on startup and resets if suspiciously high

===================================================================================
*/

// Global variables
var GlobalReputationDB *ReputationSQLite
var GlobalElectionNode *Node
var electionNodeMutex sync.RWMutex

type ReputationSQLite struct {
	ReputationDB *sql.DB
	mu           sync.Mutex
}

type TopicManager struct {
	pubsub *pubsub.PubSub
	topics map[string]*pubsub.Topic
	subs   map[string]*pubsub.Subscription
	mu     sync.Mutex
}

func NewTopicManager(ps *pubsub.PubSub) *TopicManager {
	return &TopicManager{
		pubsub: ps,
		topics: make(map[string]*pubsub.Topic),
		subs:   make(map[string]*pubsub.Subscription),
	}
}

func (tm *TopicManager) GetTopicAndSubscribe(name string) (*pubsub.Topic, *pubsub.Subscription, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	topic, ok := tm.topics[name]
	if !ok {
		logger.Election("Creating new topic: %s", name)
		var err error
		topic, err = tm.pubsub.Join(name)
		if err != nil {
			logger.Error("Failed to join topic '%s': %v", name, err)
			return nil, nil, fmt.Errorf("failed to join topic '%s': %w", name, err)
		}
		tm.topics[name] = topic
	}

	sub, ok := tm.subs[name]
	if !ok {
		logger.Election("Creating new subscription for: %s", name)
		var err error
		sub, err = topic.Subscribe()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to subscribe to topic '%s': %w", name, err)
		}
		tm.subs[name] = sub
	}

	return topic, sub, nil
}

// Constants
const (
	electionTopic = "optimusdb"

	TypeVote           = "vote"
	TypeHeartbeat      = "heartbeat"
	TypeRole           = "role"
	TypeAnnouncement   = "announcement"
	TypeReputation     = "reputation"
	TypeElectionResult = "election_result"

	heartbeatInterval      = 5 * time.Second
	heartbeatTimeout       = 15 * time.Second
	electionTimeout        = 10 * time.Second
	peerDiscoveryThreshold = 1
	reElectionBackoff      = 15 * time.Second
	heartbeatRetryLimit    = 3

	PhaseIdle      = "idle"
	PhaseVoting    = "voting"
	PhaseCompleted = "completed"
)

// Message structures
type CoreMessage struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type ElectionResultMessage struct {
	LeaderID string         `json:"leader"`
	Votes    map[string]int `json:"votes"`
	Term     int            `json:"term"`
}

type NodeReputation struct {
	NodeID                string  `json:"nodeId"`
	Uptime                float64 `json:"uptime"`
	LeadershipCount       int     `json:"leadership_count"`
	Latency               float64 `json:"latency"`
	UserCPU               float64 `json:"user_cpu"`
	SystemCPU             float64 `json:"system_cpu"`
	IdleCPU               float64 `json:"idle_cpu"`
	MemoryAvailable       float64 `json:"memory_available"`
	MemoryAllocationTotal float64 `json:"memory_total_alloc"`
	MemorySystem          float64 `json:"memory_sys"`
	AvgReadMBs            float64 `json:"avg_read_mbs"`
	AvgWriteMBs           float64 `json:"avg_write_mbs"`
	GeographyScore        float64 `json:"geography_score"`
	// ContainerName is broadcast via JSON but NOT stored in the DB.
	// Used to identify which peer is the preferred coordinator.
	ContainerName string `json:"container_name,omitempty"`
}

type VoteMessage struct {
	NodeID     string `json:"nodeId"`
	Vote       string `json:"vote"`
	ElectionID string `json:"electionId"`
	Term       int    `json:"term"`
}

type HeartbeatMessage struct {
	LeaderID string `json:"leaderId"`
	Time     int64  `json:"time"`
	Term     int    `json:"term"`
}

type RoleMessage struct {
	NodeID string `json:"nodeId"`
	Role   string `json:"role"`
	Term   int    `json:"term"`
}

type MessageRateLimiter struct {
	mu          sync.Mutex
	lastMessage map[peer.ID]map[string]time.Time
	violators   map[peer.ID]int
	bannedPeers map[peer.ID]time.Time
}

func NewMessageRateLimiter() *MessageRateLimiter {
	return &MessageRateLimiter{
		lastMessage: make(map[peer.ID]map[string]time.Time),
		violators:   make(map[peer.ID]int),
		bannedPeers: make(map[peer.ID]time.Time),
	}
}

func (rl *MessageRateLimiter) AllowMessage(from peer.ID, msgType string) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Check if peer is banned
	if banUntil, banned := rl.bannedPeers[from]; banned {
		if time.Now().Before(banUntil) {
			return false
		}
		// Ban expired, clear it
		delete(rl.bannedPeers, from)
		delete(rl.violators, from)
		logger.Election("Rate limit ban expired for %s", from.String()[:12])
	}

	if rl.lastMessage[from] == nil {
		rl.lastMessage[from] = make(map[string]time.Time)
	}

	// ✅ MORE LENIENT INTERVALS (5x longer)
	var minInterval time.Duration
	switch msgType {
	case TypeVote:
		minInterval = 5 * time.Second // Was 1s
	case TypeHeartbeat:
		minInterval = 4 * time.Second // Was 3s (heartbeat is 5s, so 4s is safe)
	case TypeReputation:
		minInterval = 25 * time.Second // Was 10s
	default:
		minInterval = 2 * time.Second // Was 500ms
	}

	last, exists := rl.lastMessage[from][msgType]
	if exists && time.Since(last) < minInterval {
		rl.violators[from]++

		// ✅ INCREASED THRESHOLD (10 violations before ban, was 5)
		if rl.violators[from] >= 10 {
			rl.bannedPeers[from] = time.Now().Add(2 * time.Minute) // ✅ Reduced ban time (2min, was 5min)
			logger.Warn("[SECURITY] Peer %s BANNED for 2min (violations: %d)",
				from.String()[:12], rl.violators[from])
		} else {
			logger.Warn("Rate limit warning for %s: %s message (violation #%d/10)",
				from.String()[:12], msgType, rl.violators[from])
		}

		return false
	}

	// ✅ RESET VIOLATION COUNT on successful message (forgiveness)
	if rl.violators[from] > 0 {
		rl.violators[from]--
	}

	rl.lastMessage[from][msgType] = time.Now()
	return true
}

// Node structure
type Node struct {
	ctx          context.Context
	host         host.Host
	pubsub       *pubsub.PubSub
	topicManager *TopicManager

	leader          peer.ID
	role            string
	leadershipCount int
	lastHeartbeat   time.Time
	heartbeatMissed int
	mutex           sync.Mutex

	electionTopic *pubsub.Topic
	electionSub   *pubsub.Subscription

	discovery *app.KnowledgeBaseDB

	votes             map[string]int
	votedNodes        map[string]string
	currentElectionID string
	currentTerm       int
	electionPhase     string
	electionDeadline  time.Time
	electionCancel    context.CancelFunc
	peerCount         int
	lastElection      time.Time
	electionMutex     sync.Mutex

	isElecting      int32
	listenerStarted int32

	votedForInTerm             map[int]string
	announcedLeaderForElection map[string]string
	announcementMutex          sync.Mutex

	rateLimiter *MessageRateLimiter

	consecutiveHeartbeatFailures int
	requiredConsecutiveFailures  int

	meshHealthy                bool
	lastMeshHealingAttempt     time.Time
	consecutiveEmptyMeshChecks int

	containerName               string
	consecutiveElectionFailures int

	// FIX: Track actual message reception instead of trusting ListPeers()
	// ListPeers() returns 0 with Kubo's internal PubSub even when messages flow
	lastMessageFromPeer map[string]time.Time
	messagePeerMu       sync.RWMutex

	// Maps peer ID → container base name (e.g. "optimusdb1")
	// Populated from incoming reputation broadcasts that include ContainerName
	peerContainerNames   map[string]string
	peerContainerNamesMu sync.RWMutex
}

// Utility functions
func hashPeerList(peerIDs []string) string {
	sorted := make([]string, len(peerIDs))
	copy(sorted, peerIDs)
	sort.Strings(sorted)

	h := sha256.New()
	for _, id := range sorted {
		h.Write([]byte(id))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// getContainerName returns the Docker container name (hostname).
// getContainerName returns the logical container/service name.
// Priority: AGENT_NAME → POD_NAME → HOSTNAME → os.Hostname()
// In K8s, pod names like "optimusdb1-54c9f77c9f-c9c94" are parsed
// to extract the base name "optimusdb1".
func getContainerName() string {
	// First check explicit AGENT_NAME env var (highest priority)
	if name := os.Getenv("AGENT_NAME"); name != "" {
		return extractBaseName(name)
	}
	// K8s: POD_NAME is set via fieldRef in the manifest
	if name := os.Getenv("POD_NAME"); name != "" {
		return extractBaseName(name)
	}
	// Docker: HOSTNAME is set to the container name
	if name := os.Getenv("HOSTNAME"); name != "" {
		return extractBaseName(name)
	}
	// Fallback to os.Hostname()
	if name, err := os.Hostname(); err == nil {
		return extractBaseName(name)
	}
	return ""
}

// extractBaseName extracts the logical service name from various naming formats:
//
//	Docker Compose:  "optimusdb1"                   → "optimusdb1"
//	K8s Deployment:  "optimusdb1-54c9f77c9f-c9c94"  → "optimusdb1"
//	K8s StatefulSet: "optimusdb-0"                   → "optimusdb-0"
//	Plain hostname:  "epm-server"                    → "epm-server"
func extractBaseName(name string) string {
	// K8s Deployment pattern: {name}-{replicaset-hash}-{pod-hash}
	parts := strings.Split(name, "-")
	if len(parts) >= 3 {
		last := parts[len(parts)-1]
		secondLast := parts[len(parts)-2]
		if len(last) >= 5 && len(secondLast) >= 5 && looksLikeHash(last) && looksLikeHash(secondLast) {
			return strings.Join(parts[:len(parts)-2], "-")
		}
	}
	return name
}

// looksLikeHash returns true if s looks like a K8s-generated hash segment
// (alphanumeric, 5-10 chars, mix of letters and digits)
func looksLikeHash(s string) bool {
	if len(s) < 5 || len(s) > 10 {
		return false
	}
	hasLetter := false
	hasDigit := false
	for _, c := range s {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			hasLetter = true
		} else if c >= '0' && c <= '9' {
			hasDigit = true
		} else {
			return false
		}
	}
	return hasLetter && hasDigit
}

// isDefaultCoordinatorNode returns true if this container should be the
// preferred coordinator. Handles all naming formats:
//
//	Docker:          "optimusdb1"       → true
//	K8s Deployment:  "optimusdb1"       → true  (after extractBaseName)
//	K8s StatefulSet: "optimusdb-0"      → true  (index 0 = first pod)
func isDefaultCoordinatorNode(containerName string) bool {
	if containerName == "" {
		return false
	}
	// Docker / K8s Deployment (after extractBaseName): "optimusdb1"
	if strings.Contains(containerName, "optimusdb1") {
		return true
	}
	// K8s StatefulSet: "optimusdb-0" (first pod)
	if strings.HasSuffix(containerName, "-0") && strings.Contains(containerName, "optimusdb") {
		return true
	}
	// Generic fallback: name ends with "1"
	return strings.HasSuffix(containerName, "1")
}

func selectInitiatorDeterministic(peerIDs []string) string {
	if len(peerIDs) == 0 {
		return ""
	}

	sorted := make([]string, len(peerIDs))
	copy(sorted, peerIDs)
	sort.Strings(sorted)

	h := sha256.New()
	for _, id := range sorted {
		h.Write([]byte(id))
	}
	hashBytes := h.Sum(nil)

	hashInt := binary.BigEndian.Uint64(hashBytes[:8])
	index := int(hashInt % uint64(len(sorted)))

	return sorted[index]
}

func getReputationWeights() map[string]float64 {
	return map[string]float64{
		"uptime":          0.20,
		"leadership":      0.10,
		"cpu":             0.20,
		"memory":          0.20,
		"disk":            0.10,
		"latency":         0.10,
		"geography_score": 0.10,
	}
}

func calculateReputation(nr NodeReputation) float64 {
	w := getReputationWeights()

	cpuUsage := nr.UserCPU + nr.SystemCPU
	if cpuUsage > 100 {
		cpuUsage = 100
	}
	cpuScore := 100 - cpuUsage

	memoryScore := 100.0
	if nr.MemorySystem > 0 {
		memoryUsedPct := (nr.MemoryAllocationTotal / nr.MemorySystem) * 100
		if memoryUsedPct > 100 {
			memoryUsedPct = 100
		}
		memoryScore = 100 - memoryUsedPct
	}

	diskIO := nr.AvgReadMBs + nr.AvgWriteMBs
	diskScore := 100.0
	if diskIO > 0 {
		logDisk := math.Log10(diskIO)
		diskScore = 100 - (logDisk * 25)
		if diskScore < 0 {
			diskScore = 0
		}
		if diskScore > 100 {
			diskScore = 100
		}
	}

	latency := nr.Latency
	if latency > 100 {
		latency = 100
	}
	latencyScore := 100 - latency

	uptimeScore := nr.Uptime * 100
	if uptimeScore > 100 {
		uptimeScore = 100
	}

	leadershipScore := float64(nr.LeadershipCount) * 10
	if leadershipScore > 100 {
		leadershipScore = 100
	}

	geographyScore := nr.GeographyScore * 100
	if geographyScore > 100 {
		geographyScore = 100
	}

	score := (w["uptime"] * uptimeScore) +
		(w["leadership"] * leadershipScore) +
		(w["cpu"] * cpuScore) +
		(w["memory"] * memoryScore) +
		(w["disk"] * diskScore) +
		(w["latency"] * latencyScore) +
		(w["geography_score"] * geographyScore)

	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}

	return score
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Database initialization
func InitReputationDB() (*ReputationSQLite, error) {
	rdbmsCache := filepath.Join(
		filepath.Join(
			filepath.Join(os.Getenv("HOME"), ".cache"),
			"optimusdb",
			*config.FlagRepo,
			"optimusdb",
		),
		"optimusreputation.db",
	)

	dir := filepath.Dir(rdbmsCache)
	if err := os.MkdirAll(dir, 0755); err != nil {
		logger.Error("Failed to create directory for Reputation DB: %v", err)
		return nil, fmt.Errorf("failed to create directory for Reputation DB: %w", err)
	}

	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		logger.Error("Cannot open SQLite DB for Reputation: %v", err)
		return nil, err
	}

	GlobalReputationDB = &ReputationSQLite{ReputationDB: db}

	if err := GlobalReputationDB.createReputationDB(); err != nil {
		logger.Error("Table creation failed for Reputation DB: %v", err)
		return nil, err
	}

	logger.Election("SQLite Reputation Database Ready at: %s", rdbmsCache)
	return GlobalReputationDB, nil
}

func (rep *ReputationSQLite) createReputationDB() error {
	tableQuery := `CREATE TABLE IF NOT EXISTS reputation (
		node_id TEXT PRIMARY KEY,
		uptime REAL,
		leadership_count INTEGER,
		latency REAL,
		user_cpu REAL,
		system_cpu REAL,
		idle_cpu REAL,
		memory_available REAL,
		memory_total_alloc REAL,
		memory_sys REAL,
		avg_read_mbs REAL,
		avg_write_mbs REAL,
		geography_score REAL
	);`
	if _, err := rep.ReputationDB.Exec(tableQuery); err != nil {
		return err
	}

	electionLogQuery := `CREATE TABLE IF NOT EXISTS election_log (
		id TEXT PRIMARY KEY,
		timestamp TEXT,
		leader_id TEXT,
		term INTEGER,
		votes_json TEXT
	);`
	if _, err := rep.ReputationDB.Exec(electionLogQuery); err != nil {
		logger.Error("Failed to create election_log table: %v", err)
		return fmt.Errorf("failed to create election_log table: %w", err)
	}

	return nil
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPER: Check if leader is empty (multiple formats)
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) isLeaderEmpty() bool {
	leaderStr := n.leader.String()
	return leaderStr == "" ||
		leaderStr == "<peer.ID  >" ||
		leaderStr == "<peer.ID >" ||
		leaderStr == "<peer.ID>" ||
		len(leaderStr) < 10
}

// ═══════════════════════════════════════════════════════════════════════════
// MESSAGE PUBLISHING
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) publishMessage(msgType string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		logger.Error("Failed to marshal payload for %s: %v", msgType, err)
		return fmt.Errorf("marshal payload failed: %w", err)
	}

	core := CoreMessage{Type: msgType, Payload: data}
	coreData, err := json.Marshal(core)
	if err != nil {
		logger.Error("Failed to marshal CoreMessage for %s: %v", msgType, err)
		return fmt.Errorf("marshal core failed: %w", err)
	}

	meshPeers := n.electionTopic.ListPeers()
	logger.Election("Publishing %s: %d bytes, %d peers in mesh",
		msgType, len(coreData), len(meshPeers))

	if len(meshPeers) == 0 {
		logger.Warn("No mesh peers! Message may not propagate")
		allPeers := n.host.Network().Peers()
		logger.Election("Connected peers: %d", len(allPeers))
	}

	for attempt := 0; attempt < 3; attempt++ {
		err = n.electionTopic.Publish(n.ctx, coreData)
		if err == nil {
			logger.Election("✅ %s published successfully (attempt %d)", msgType, attempt+1)
			return nil
		}

		logger.Error("Publish attempt %d failed: %v", attempt+1, err)
		if attempt < 2 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return fmt.Errorf("failed to publish after 3 attempts: %w", err)
}

// ═══════════════════════════════════════════════════════════════════════════
// START ELECTION (WITH MESH BLOCKING)
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
	// Check mesh health — but use ACTUAL message reception, not just ListPeers()
	meshPeers := n.electionTopic.ListPeers()
	discoveredPeers := n.discovery.GetDiscoveredPeers()
	recentMsgPeers := n.hasRecentMessagePeers(60 * time.Second)

	// FIX: Only block if BOTH ListPeers AND actual message reception confirm isolation
	// ListPeers() returns 0 with Kubo's internal PubSub even when messages flow
	trulyIsolated := len(meshPeers) == 0 && recentMsgPeers == 0

	if trulyIsolated && len(discoveredPeers) > 0 {
		logger.Error("══════════════════════════════════════════")
		logger.Error("ELECTION BLOCKED: NODE IS TRULY ISOLATED!")
		logger.Error("   ListPeers: %d, Recent msg peers: %d", len(meshPeers), recentMsgPeers)
		logger.Error("   Triggering emergency mesh healing...")

		go n.emergencyMeshHealing()

		// FIX: Still increment failure counter so default coordinator fallback works
		n.consecutiveElectionFailures++
		logger.Error("   Consecutive election failures: %d", n.consecutiveElectionFailures)

		// Default coordinator fallback even when mesh-blocked
		if isDefaultCoordinatorNode(n.containerName) && n.consecutiveElectionFailures >= 2 {
			logger.Election("🏗️  DEFAULT COORDINATOR FALLBACK (mesh-blocked): self-promoting after %d failures",
				n.consecutiveElectionFailures)
			n.promoteAsDefaultCoordinator()
			return
		}

		return
	}

	if len(meshPeers) == 0 && recentMsgPeers > 0 {
		logger.Election("⚠️  ListPeers()=0 but %d peers seen via messages — proceeding with election", recentMsgPeers)
	}

	if !atomic.CompareAndSwapInt32(&n.isElecting, 0, 1) {
		logger.Election("Election already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&n.isElecting, 0)

	// ═══════════════════════════════════════════════════════════════
	// FIX: Internal retry loop (replaces broken recursive call)
	// Previously, finalizeElection called StartElection recursively,
	// but the isElecting atomic flag was still held, so retries ALWAYS
	// failed with "Election already in progress". Now retries happen
	// inside this same goroutine with the flag held once.
	// ═══════════════════════════════════════════════════════════════
	maxAttempts := 3
	for currentAttempt := attempt; currentAttempt < maxAttempts; currentAttempt++ {
		discoveredPeers = n.discovery.GetDiscoveredPeers()
		totalPeers := len(discoveredPeers) + 1

		n.electionMutex.Lock()
		n.currentTerm++
		term := n.currentTerm
		n.peerCount = totalPeers
		n.electionMutex.Unlock()

		logger.Election("════════════════════════════════════════")
		logger.Election("Starting Election - Term %d, Attempt %d/%d", term, currentAttempt+1, maxAttempts)
		logger.Election("Cluster size: %d peers", totalPeers)
		logger.Election("Mesh peers: %d", len(n.electionTopic.ListPeers()))
		logger.Election("════════════════════════════════════════")

		allPeerIDs := append([]string{n.host.ID().String()}, discoveredPeers...)
		sort.Strings(allPeerIDs)
		peerListHash := hashPeerList(allPeerIDs)

		electionID := fmt.Sprintf("cluster-term%d-attempt%d-peers%s",
			term, currentAttempt, peerListHash[:8])
		logger.Election("Election ID: %s (clock-independent)", electionID)

		n.electionMutex.Lock()
		n.currentElectionID = electionID
		n.electionPhase = PhaseVoting
		n.electionDeadline = time.Now().Add(electionTimeout)
		n.votes = make(map[string]int)
		n.votedNodes = make(map[string]string)
		n.electionMutex.Unlock()

		if len(peers) == 0 {
			peers = []NodeReputation{{NodeID: n.host.ID().String()}}
		}

		selected := n.selectCandidate(peers)
		vote := VoteMessage{
			NodeID:     n.host.ID().String(),
			Vote:       selected,
			ElectionID: electionID,
			Term:       term,
		}

		n.electionMutex.Lock()
		n.votedNodes[vote.NodeID] = vote.Vote
		n.votes[vote.Vote]++
		logger.Election("🗳️  I vote for: %s", vote.Vote)
		n.electionMutex.Unlock()

		if err := n.publishMessage(TypeVote, vote); err != nil {
			logger.Error("Failed to publish vote: %v", err)
		}

		electionCtx, cancel := context.WithTimeout(n.ctx, electionTimeout)

		n.electionMutex.Lock()
		n.electionCancel = cancel
		n.electionMutex.Unlock()

		<-electionCtx.Done()
		cancel()

		// ── Finalize this attempt inline ──
		winner := n.finalizeElectionInline(term, electionID)

		if winner != "" {
			// Election succeeded - reset failure counter
			n.consecutiveElectionFailures = 0
			return
		}

		// No winner - log and continue retry loop
		logger.Warn("No winner in term %d (attempt %d/%d)", term, currentAttempt+1, maxAttempts)

		if currentAttempt < maxAttempts-1 {
			backoff := time.Duration(math.Pow(2, float64(currentAttempt))) * time.Second
			logger.Election("Retrying in %v...", backoff)
			time.Sleep(backoff)
		}
	}

	// All attempts exhausted - use fallback
	n.consecutiveElectionFailures++
	logger.Error("Election failed after %d attempts (consecutive failures: %d)", maxAttempts, n.consecutiveElectionFailures)

	// If this is the default coordinator node and elections keep failing,
	// self-promote to break the deadlock
	if isDefaultCoordinatorNode(n.containerName) && n.consecutiveElectionFailures >= 2 {
		logger.Election("🏗️  DEFAULT COORDINATOR FALLBACK: %s self-promoting after %d consecutive failures",
			n.containerName, n.consecutiveElectionFailures)
		n.promoteAsDefaultCoordinator()
		return
	}

	n.fallbackElection()
}

func (n *Node) selectCandidate(peers []NodeReputation) string {
	if len(peers) == 0 {
		return n.host.ID().String()
	}

	// ═══════════════════════════════════════════════════════════
	// COORDINATOR PREFERENCE: Always vote for the preferred
	// coordinator (optimusdb1) if it is alive and healthy.
	// This guarantees deterministic leadership unless node1 fails.
	// ═══════════════════════════════════════════════════════════
	preferredPeerID := n.getPreferredCoordinatorPeerID(peers)
	if preferredPeerID != "" {
		for _, p := range peers {
			if p.NodeID == preferredPeerID {
				score := calculateReputation(p)
				if score > 10 { // Minimum health threshold (out of 100)
					logger.Election("👑 Voting for preferred coordinator %s [%s] (score: %.2f)",
						preferredPeerID, p.ContainerName, score)
					return preferredPeerID
				}
				logger.Warn("⚠️  Preferred coordinator %s has low score (%.2f), falling back to election",
					preferredPeerID, score)
				break
			}
		}
	}

	// Fallback: weighted random selection (original logic)
	// Only used when preferred coordinator is down or unhealthy
	logger.Election("📊 No preferred coordinator available, using reputation-weighted selection")
	total := 0.0
	for _, p := range peers {
		total += calculateReputation(p)
	}

	if total <= 0 {
		return peers[rand.Intn(len(peers))].NodeID
	}

	randVal := rand.Float64() * total
	cumulative := 0.0
	for _, p := range peers {
		cumulative += calculateReputation(p)
		if cumulative >= randVal {
			return p.NodeID
		}
	}

	return peers[len(peers)-1].NodeID
}

// getPreferredCoordinatorPeerID returns the peer ID of the preferred
// coordinator if it is present in the candidate list.
func (n *Node) getPreferredCoordinatorPeerID(peers []NodeReputation) string {
	// Check if WE are the preferred coordinator
	if isDefaultCoordinatorNode(n.containerName) {
		myID := n.host.ID().String()
		for _, p := range peers {
			if p.NodeID == myID {
				return myID
			}
		}
	}

	// Check ContainerName in reputation data (direct from broadcast)
	for _, p := range peers {
		if p.ContainerName != "" && isDefaultCoordinatorNode(p.ContainerName) {
			return p.NodeID
		}
	}

	// Check accumulated peer container name mapping
	n.peerContainerNamesMu.RLock()
	defer n.peerContainerNamesMu.RUnlock()
	for _, p := range peers {
		if cname, ok := n.peerContainerNames[p.NodeID]; ok {
			if isDefaultCoordinatorNode(cname) {
				return p.NodeID
			}
		}
	}

	return ""
}

// finalizeElectionInline evaluates the election result and returns the winner
// (empty string if no winner). Called from the retry loop in StartElection.
func (n *Node) finalizeElectionInline(term int, electionID string) string {
	n.electionMutex.Lock()

	if n.currentElectionID != electionID || n.currentTerm != term {
		n.electionMutex.Unlock()
		logger.Warn("Election state changed, aborting finalization")
		return ""
	}

	n.electionPhase = PhaseCompleted

	logger.Election("Final Results - Term %d:", term)
	for candidate, count := range n.votes {
		logger.Election("  %s: %d votes", candidate, count)
	}
	logger.Election("Participation: %d/%d nodes voted", len(n.votedNodes), n.peerCount)

	winner := n.determineWinner()

	votesCopy := make(map[string]int)
	for k, v := range n.votes {
		votesCopy[k] = v
	}

	n.electionMutex.Unlock()

	if winner == "" {
		return ""
	}

	logger.Election("🎉 WINNER: %s with %d votes", winner, votesCopy[winner])
	n.announceLeader(winner, term)

	if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
		err := InsertElectionLog(
			GlobalReputationDB.ReputationDB,
			electionID,
			time.Now(),
			winner,
			term,
			votesCopy,
		)
		if err != nil {
			logger.Error("Failed to log election: %v", err)
		}
	}

	return winner
}

func (n *Node) finalizeElection(term int, electionID string, attempt int, peers []NodeReputation) {
	n.electionMutex.Lock()

	if n.currentElectionID != electionID || n.currentTerm != term {
		n.electionMutex.Unlock()
		logger.Warn("Election state changed, aborting finalization")
		return
	}

	n.electionPhase = PhaseCompleted

	logger.Election("Final Results - Term %d:", term)
	for candidate, count := range n.votes {
		logger.Election("  %s: %d votes", candidate, count)
	}
	logger.Election("Participation: %d/%d nodes voted", len(n.votedNodes), n.peerCount)

	winner := n.determineWinner()

	votesCopy := make(map[string]int)
	for k, v := range n.votes {
		votesCopy[k] = v
	}

	n.electionMutex.Unlock()

	if winner == "" {
		logger.Warn("No winner in term %d (attempt %d/%d)", term, attempt+1, 3)

		if attempt < 2 {
			backoff := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			logger.Election("Retrying in %v...", backoff)
			time.Sleep(backoff)
			n.StartElection(peers, attempt+1)
		} else {
			logger.Error("Election failed after 3 attempts, using fallback")
			n.fallbackElection()
		}
		return
	}

	logger.Election("🎉 WINNER: %s with %d votes", winner, votesCopy[winner])
	n.announceLeader(winner, term)

	if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
		err := InsertElectionLog(
			GlobalReputationDB.ReputationDB,
			electionID,
			time.Now(),
			winner,
			term,
			votesCopy,
		)
		if err != nil {
			logger.Error("Failed to log election: %v", err)
		}
	}
}

func (n *Node) determineWinner() string {
	if len(n.votes) == 0 {
		return ""
	}

	var winner string
	maxVotes := 0
	for node, count := range n.votes {
		if count > maxVotes || (count == maxVotes && node < winner) {
			maxVotes = count
			winner = node
		}
	}

	participation := len(n.votedNodes)
	var required int

	if n.peerCount == 1 {
		required = 1
	} else if n.peerCount <= 3 {
		required = (n.peerCount + 1) / 2
		if required < 2 {
			required = 2
		}
	} else if n.peerCount <= 8 {
		required = (n.peerCount + 1) / 2
	} else {
		required = (n.peerCount * 3) / 10
		if required < 3 {
			required = 3
		}
	}

	logger.Election("Quorum Analysis:")
	logger.Election("  Cluster size: %d", n.peerCount)
	logger.Election("  Participation: %d nodes voted", participation)
	logger.Election("  Required: %d votes", required)
	logger.Election("  Winner votes: %d", maxVotes)

	if participation >= required && maxVotes >= required {
		logger.Election("✅ Quorum ACHIEVED")
		return winner
	}

	logger.Warn("Quorum NOT met (need %d, got %d)", required, maxVotes)
	return ""
}

// ═══════════════════════════════════════════════════════════════════════════
// MESSAGE LISTENER
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) ListenForElectionEvents() {
	if !atomic.CompareAndSwapInt32(&n.listenerStarted, 0, 1) {
		logger.Warn("Listener already started")
		return
	}

	logger.Election("Starting Election Message Listener, Node: %s", n.host.ID().String())

	if n.electionSub == nil {
		log.Fatal("[FATAL] No GossipSub subscription available!")
	}

	go func() {
		msgCount := 0
		for {
			msg, err := n.electionSub.Next(n.ctx)
			if err != nil {
				// Check if context was cancelled (normal shutdown)
				if n.ctx.Err() != nil {
					logger.Election("Listener shutting down")
					return
				}

				// All other subscription errors - log and retry
				logger.Warn("Subscription error (continuing): %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			msgCount++

			// FIX: Track actual message reception from each peer
			// This is the TRUE mesh health signal — ListPeers() lies with Kubo PubSub
			if msg.ReceivedFrom != n.host.ID() {
				n.messagePeerMu.Lock()
				n.lastMessageFromPeer[msg.ReceivedFrom.String()] = time.Now()
				n.messagePeerMu.Unlock()
			}

			sender := msg.ReceivedFrom.String()
			if len(sender) > 12 {
				sender = sender[:12] + "..."
			}

			logger.Election("📨 MSG #%d from %s (%d bytes)",
				msgCount, sender, len(msg.Data))

			var core CoreMessage
			if err := json.Unmarshal(msg.Data, &core); err != nil {
				logger.Error("Failed to unmarshal message: %v", err)
				continue
			}

			logger.Election("📨 MSG #%d type: %s", msgCount, core.Type)

			n.handleMessage(core, msg.ReceivedFrom)
		}
	}()
}

// hasRecentMessagePeers returns the count of peers from whom we received
// a message within the given duration. This is the REAL mesh health indicator
// because ListPeers() returns 0 with Kubo's internal PubSub even when
// messages are actually flowing between nodes.
func (n *Node) hasRecentMessagePeers(within time.Duration) int {
	n.messagePeerMu.RLock()
	defer n.messagePeerMu.RUnlock()
	count := 0
	cutoff := time.Now().Add(-within)
	for _, lastSeen := range n.lastMessageFromPeer {
		if lastSeen.After(cutoff) {
			count++
		}
	}
	return count
}

func validateReputationData(rep NodeReputation) error {
	if rep.UserCPU < 0 || rep.UserCPU > 100 {
		return fmt.Errorf("invalid UserCPU: %.2f (must be 0-100)", rep.UserCPU)
	}
	if rep.SystemCPU < 0 || rep.SystemCPU > 100 {
		return fmt.Errorf("invalid SystemCPU: %.2f (must be 0-100)", rep.SystemCPU)
	}
	if rep.IdleCPU < 0 || rep.IdleCPU > 100 {
		return fmt.Errorf("invalid IdleCPU: %.2f (must be 0-100)", rep.IdleCPU)
	}
	if rep.MemorySystem < 0 {
		return fmt.Errorf("invalid MemorySystem: %.2f (must be >= 0)", rep.MemorySystem)
	}
	if rep.MemoryAllocationTotal < 0 {
		return fmt.Errorf("invalid MemoryAllocationTotal: %.2f (must be >= 0)", rep.MemoryAllocationTotal)
	}
	if rep.MemorySystem > 0 && rep.MemoryAllocationTotal > rep.MemorySystem*2 {
		return fmt.Errorf("invalid memory: allocated (%.2f) > 2× system (%.2f)",
			rep.MemoryAllocationTotal, rep.MemorySystem)
	}
	if rep.Uptime < 0 || rep.Uptime > 1 {
		return fmt.Errorf("invalid Uptime: %.2f (must be 0.0-1.0)", rep.Uptime)
	}
	if rep.GeographyScore < 0 || rep.GeographyScore > 1 {
		return fmt.Errorf("invalid GeographyScore: %.2f (must be 0.0-1.0)", rep.GeographyScore)
	}
	if rep.AvgReadMBs < 0 || rep.AvgReadMBs > 10000 {
		return fmt.Errorf("invalid AvgReadMBs: %.2f (must be 0-10000)", rep.AvgReadMBs)
	}
	if rep.AvgWriteMBs < 0 || rep.AvgWriteMBs > 10000 {
		return fmt.Errorf("invalid AvgWriteMBs: %.2f (must be 0-10000)", rep.AvgWriteMBs)
	}
	if rep.Latency < 0 || rep.Latency > 1000 {
		return fmt.Errorf("invalid Latency: %.2f (must be 0-1000ms)", rep.Latency)
	}
	return nil
}

func (n *Node) handleMessage(core CoreMessage, from peer.ID) {
	if !n.rateLimiter.AllowMessage(from, core.Type) {
		return
	}

	switch core.Type {
	case TypeVote:
		var vote VoteMessage
		if err := json.Unmarshal(core.Payload, &vote); err != nil {
			logger.Error("Failed to unmarshal vote: %v", err)
			return
		}
		logger.Election("🗳️  Vote: %s → %s (election: %s, term: %d)",
			vote.NodeID, vote.Vote, vote.ElectionID, vote.Term)
		n.handleVote(vote)

	case TypeHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(core.Payload, &hb); err != nil {
			logger.Error("Failed to unmarshal heartbeat: %v", err)
			return
		}
		logger.Election("💓 Heartbeat from %s (term %d)", hb.LeaderID, hb.Term)
		n.handleHeartbeat(hb)

	case TypeReputation:
		var rep NodeReputation
		if err := json.Unmarshal(core.Payload, &rep); err != nil {
			logger.Error("Failed to unmarshal reputation: %v", err)
			return
		}

		// Track peer ID → container name mapping for coordinator preference
		if rep.ContainerName != "" && rep.NodeID != "" {
			n.peerContainerNamesMu.Lock()
			n.peerContainerNames[rep.NodeID] = rep.ContainerName
			n.peerContainerNamesMu.Unlock()
		}

		if rep.NodeID != n.host.ID().String() {
			if err := validateReputationData(rep); err != nil {
				logger.Warn("Invalid reputation from %s: %v (IGNORING)",
					rep.NodeID, err)
				return
			}

			score := calculateReputation(rep)
			logger.Election("📊 Reputation from %s [%s]: %.2f", rep.NodeID, rep.ContainerName, score)

			if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
				UpsertReputation(GlobalReputationDB.ReputationDB, rep)
			}
		}

	case TypeAnnouncement:
		var ann map[string]interface{}
		if err := json.Unmarshal(core.Payload, &ann); err != nil {
			logger.Error("Failed to unmarshal announcement: %v", err)
			return
		}
		leaderID, _ := ann["leader"].(string)
		term := int(ann["term"].(float64))
		logger.Election("📢 Announcement: %s is leader (term %d)", leaderID, term)
		n.handleAnnouncement(leaderID, term)

	case TypeElectionResult:
		var result ElectionResultMessage
		if err := json.Unmarshal(core.Payload, &result); err != nil {
			logger.Error("Failed to unmarshal election result: %v", err)
			return
		}
		logger.Election("📋 Result: Leader=%s, Term=%d, Votes=%v",
			result.LeaderID, result.Term, result.Votes)
	}
}

func (n *Node) handleVote(vote VoteMessage) {
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()

	shouldJoin := n.electionPhase == PhaseIdle ||
		vote.Term > n.currentTerm ||
		(vote.Term == n.currentTerm && vote.ElectionID != n.currentElectionID)

	if shouldJoin {
		logger.Election("📥 JOINING election started by %s", vote.NodeID)
		logger.Election("   Election ID: %s", vote.ElectionID)
		logger.Election("   Term: %d", vote.Term)

		n.electionPhase = PhaseVoting
		n.currentElectionID = vote.ElectionID
		n.currentTerm = vote.Term
		n.electionDeadline = time.Now().Add(electionTimeout)
		n.votes = make(map[string]int)
		n.votedNodes = make(map[string]string)

		if _, hasVoted := n.votedNodes[n.host.ID().String()]; !hasVoted {
			peers, err := QueryAllReputations(GlobalReputationDB.ReputationDB)
			if err != nil || len(peers) == 0 {
				selfRep := NodeReputation{
					NodeID:         n.host.ID().String(),
					Uptime:         1.0,
					GeographyScore: 0.5,
				}
				peers = []NodeReputation{selfRep}
			}

			selected := n.selectCandidate(peers)
			ownVote := VoteMessage{
				NodeID:     n.host.ID().String(),
				Vote:       selected,
				ElectionID: vote.ElectionID,
				Term:       vote.Term,
			}

			logger.Election("🗳️  My vote in this election: %s → %s",
				ownVote.NodeID, ownVote.Vote)

			n.votedNodes[ownVote.NodeID] = ownVote.Vote
			n.votes[ownVote.Vote]++

			n.electionMutex.Unlock()
			n.publishMessage(TypeVote, ownVote)
			n.electionMutex.Lock()
		}
	}

	if n.electionPhase != PhaseVoting ||
		vote.ElectionID != n.currentElectionID ||
		vote.Term != n.currentTerm {
		return
	}

	if _, hasVoted := n.votedNodes[vote.NodeID]; !hasVoted {
		n.votedNodes[vote.NodeID] = vote.Vote
		n.votes[vote.Vote]++

		logger.Election("✅ Recorded vote: %s → %s (total for %s: %d)",
			vote.NodeID, vote.Vote, vote.Vote, n.votes[vote.Vote])
	} else {
		logger.Warn("Duplicate vote from %s ignored", vote.NodeID)
	}
}

func (n *Node) handleHeartbeat(hb HeartbeatMessage) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.role == "Coordinator" {
		if hb.LeaderID != n.host.ID().String() {
			logger.Warn("⚠️  Detected competing coordinator: %s", hb.LeaderID)

			if hb.Term > n.currentTerm {
				logger.Warn("Their term (%d) > our term (%d), stepping down",
					hb.Term, n.currentTerm)
				n.stepDownLocked(hb.LeaderID, hb.Term)
			} else if hb.Term == n.currentTerm && hb.LeaderID < n.host.ID().String() {
				logger.Warn("Same term, their ID < our ID, stepping down")
				n.stepDownLocked(hb.LeaderID, hb.Term)
			} else {
				logger.Election("We have precedence, ignoring their heartbeat")
			}
		}
		return
	}

	if n.role == "Follower" {
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		n.consecutiveHeartbeatFailures = 0

		leaderPeerID, err := peer.Decode(hb.LeaderID)
		if err != nil {
			logger.Error("Failed to decode leader ID: %v", err)
			return
		}

		n.leader = leaderPeerID

		if hb.Term > n.currentTerm {
			logger.Election("Updating term: %d → %d", n.currentTerm, hb.Term)
			n.currentTerm = hb.Term
		}
	}
}

func (n *Node) stepDownLocked(newLeaderID string, term int) {
	logger.Election("⬇️  STEPPING DOWN: Coordinator → Follower")
	logger.Election("   New leader: %s (term %d)", newLeaderID, term)

	n.role = "Follower"
	n.currentTerm = term
	n.lastHeartbeat = time.Now()
	n.heartbeatMissed = 0
	n.consecutiveHeartbeatFailures = 0

	leaderPeerID, err := peer.Decode(newLeaderID)
	if err != nil {
		logger.Error("Failed to decode new leader ID: %v", err)
		return
	}
	n.leader = leaderPeerID
}

func (n *Node) handleAnnouncement(leaderID string, term int) {
	n.mutex.Lock()

	leaderPeerID, err := peer.Decode(leaderID)
	if err != nil {
		logger.Error("Failed to decode leader ID '%s': %v", leaderID, err)
		n.mutex.Unlock()
		return
	}

	if leaderID == n.host.ID().String() {
		n.role = "Coordinator"
		n.leader = leaderPeerID
		n.leadershipCount++
		logger.Election("👑 I AM THE COORDINATOR (term %d)", term)
		logger.Election("   Leadership count: %d", n.leadershipCount)
	} else {
		n.role = "Follower"
		n.leader = leaderPeerID
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		n.consecutiveHeartbeatFailures = 0
		logger.Election("📋 FOLLOWER: Following %s (term %d)", leaderID, term)
	}

	n.mutex.Unlock()

	n.electionMutex.Lock()
	n.currentTerm = term
	n.electionMutex.Unlock()
}

func (n *Node) announceLeader(leaderID string, term int) {
	announcement := map[string]interface{}{
		"leader": leaderID,
		"term":   term,
	}

	if err := n.publishMessage(TypeAnnouncement, announcement); err != nil {
		logger.Error("Failed to announce leader: %v", err)
		return
	}

	logger.Election("📢 Announced coordinator: %s (term %d)", leaderID, term)

	n.handleAnnouncement(leaderID, term)

	if leaderID == n.host.ID().String() {
		go func() {
			time.Sleep(2 * time.Second)
			n.sendHeartbeats(term)
		}()
	}
}

func (n *Node) sendHeartbeats(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	logger.Election("Starting heartbeat broadcast (every %v)", heartbeatInterval)

	for {
		select {
		case <-ticker.C:
			n.mutex.Lock()
			if n.role != "Coordinator" {
				n.mutex.Unlock()
				logger.Election("No longer coordinator, stopping heartbeats")
				return
			}
			n.mutex.Unlock()

			// FIX: ALWAYS send heartbeats — never gate on ListPeers()
			// ListPeers() returns 0 with Kubo's internal PubSub, but messages
			// DO flow (proven by reception logs). Blocking heartbeats here was
			// the #1 cause of the election deadlock.
			meshPeers := n.electionTopic.ListPeers()
			recentMsgPeers := n.hasRecentMessagePeers(60 * time.Second)

			hb := HeartbeatMessage{
				LeaderID: n.host.ID().String(),
				Time:     time.Now().Unix(),
				Term:     term,
			}

			// ✅ RETRY LOGIC WITH EXPONENTIAL BACKOFF
			var publishErr error
			for attempt := 0; attempt < 3; attempt++ {
				publishErr = n.publishMessage(TypeHeartbeat, hb)
				if publishErr == nil {
					logger.Election("💓 Heartbeat sent (term %d, mesh=%d, msgPeers=%d)",
						term, len(meshPeers), recentMsgPeers)
					break
				}

				backoff := time.Duration(math.Pow(2, float64(attempt))) * 500 * time.Millisecond
				logger.Warn("💔 Heartbeat attempt %d failed: %v (retry in %v)",
					attempt+1, publishErr, backoff)
				time.Sleep(backoff)
			}

			if publishErr != nil {
				logger.Error("💔 Heartbeat failed after 3 attempts: %v", publishErr)
				go n.emergencyMeshHealing()
			}

		case <-n.ctx.Done():
			logger.Election("Context cancelled, stopping heartbeats")
			return
		}
	}
}

// promoteAsDefaultCoordinator forces this node to become coordinator.
// Used when this node is the designated default (container name ends with "1")
// and GossipSub elections consistently fail due to mesh issues.
func (n *Node) promoteAsDefaultCoordinator() {
	logger.Election("═══════════════════════════════════════")
	logger.Election("🏗️  DEFAULT COORDINATOR SELF-PROMOTION")
	logger.Election("   Container: %s", n.containerName)
	logger.Election("   Reason: GossipSub elections failing")
	logger.Election("═══════════════════════════════════════")

	n.electionMutex.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.electionPhase = PhaseCompleted
	n.electionMutex.Unlock()

	// Announce ourselves as leader
	n.announceLeader(n.host.ID().String(), term)

	// Log the promotion
	if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
		votes := map[string]int{n.host.ID().String(): 1}
		InsertElectionLog(
			GlobalReputationDB.ReputationDB,
			fmt.Sprintf("default-coordinator-%s-term%d", n.containerName, term),
			time.Now(),
			n.host.ID().String(),
			term,
			votes,
		)
	}

	logger.Election("✅ Default coordinator promotion complete")
}

func (n *Node) fallbackElection() {
	logger.Warn("Executing FALLBACK election")

	peers, err := QueryAllReputations(GlobalReputationDB.ReputationDB)
	if err != nil || len(peers) == 0 {
		logger.Warn("No peers found, making self coordinator")
		n.announceLeader(n.host.ID().String(), n.currentTerm+1)
		return
	}

	var best NodeReputation
	maxScore := -1.0
	for _, p := range peers {
		score := calculateReputation(p)
		if score > maxScore {
			maxScore = score
			best = p
		}
	}

	logger.Election("Fallback winner: %s (reputation: %.2f)", best.NodeID, maxScore)
	n.announceLeader(best.NodeID, n.currentTerm+1)
}

func (n *Node) CheckLeaderFailure() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	logger.Election("Starting leader failure detection with mesh-aware logic")

	for range ticker.C {
		n.mutex.Lock()

		if n.role == "Coordinator" {
			n.consecutiveHeartbeatFailures = 0
			n.mutex.Unlock()
			continue
		}

		if n.lastHeartbeat.IsZero() {
			n.lastHeartbeat = time.Now()
			n.mutex.Unlock()
			continue
		}

		timeSince := time.Since(n.lastHeartbeat)
		if timeSince > heartbeatTimeout {
			n.heartbeatMissed++
			n.consecutiveHeartbeatFailures++

			logger.Warn("Heartbeat timeout: %v since last (miss #%d, consecutive #%d)",
				timeSince, n.heartbeatMissed, n.consecutiveHeartbeatFailures)

			if n.heartbeatMissed >= heartbeatRetryLimit &&
				n.consecutiveHeartbeatFailures >= n.requiredConsecutiveFailures {

				logger.Error("LEADER FAILURE CONFIRMED: %d consecutive timeouts", n.consecutiveHeartbeatFailures)

				// Check mesh health using BOTH ListPeers AND actual message reception
				meshPeers := n.electionTopic.ListPeers()
				discoveredPeers := n.discovery.GetDiscoveredPeers()
				recentMsgPeers := n.hasRecentMessagePeers(60 * time.Second)

				// FIX: Only treat as mesh failure if BOTH indicators confirm isolation
				trulyIsolated := len(meshPeers) == 0 && recentMsgPeers == 0

				if trulyIsolated && len(discoveredPeers) > 0 {
					logger.Error("ROOT CAUSE: TRUE MESH ISOLATION")
					logger.Error("  Discovered peers: %d ✅", len(discoveredPeers))
					logger.Error("  Mesh peers: %d ❌, Recent msg peers: %d ❌", len(meshPeers), recentMsgPeers)
					logger.Error("  Triggering emergency mesh healing...")

					// FIX: Increment election failure counter even when mesh-blocked
					n.consecutiveElectionFailures++

					// FIX: Default coordinator fallback when truly isolated
					if isDefaultCoordinatorNode(n.containerName) && n.consecutiveElectionFailures >= 2 {
						n.heartbeatMissed = 0
						n.consecutiveHeartbeatFailures = 0
						n.mutex.Unlock()
						logger.Election("🏗️  DEFAULT COORDINATOR FALLBACK from CheckLeaderFailure")
						n.promoteAsDefaultCoordinator()
						continue
					}

					n.heartbeatMissed = 0
					n.consecutiveHeartbeatFailures = 0
					n.mutex.Unlock()

					go n.emergencyMeshHealing()
					continue
				}

				if len(meshPeers) == 0 && recentMsgPeers > 0 {
					logger.Election("ListPeers()=0 but %d peers via messages — treating as leader failure, not mesh failure", recentMsgPeers)
				} else {
					logger.Election("Mesh healthy (mesh=%d, msgPeers=%d), leader actually failed", len(meshPeers), recentMsgPeers)
				}
				logger.Election("Starting re-election...")

				n.heartbeatMissed = 0
				n.consecutiveHeartbeatFailures = 0
				n.mutex.Unlock()

				backoffMs := rand.Intn(5000)
				backoff := time.Duration(backoffMs) * time.Millisecond

				logger.Election("Applying random backoff: %v", backoff)
				time.Sleep(backoff)

				if atomic.LoadInt32(&n.isElecting) == 0 {
					logger.Election("Starting re-election after confirmed leader failure")
					go func() {
						peers, _ := QueryAllReputations(GlobalReputationDB.ReputationDB)
						n.StartElection(peers, 0)
					}()
				} else {
					logger.Election("Another node already started election, joining")
				}
				continue
			}
		} else {
			n.heartbeatMissed = 0
			n.consecutiveHeartbeatFailures = 0
		}

		n.mutex.Unlock()
	}
}

func (n *Node) PeriodicReputationPublisher() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	logger.Election("Starting reputation publisher (every 30s)")

	for {
		select {
		case <-ticker.C:
			userCPU, systemCPU, idleCPU, _ := utilities.GetCPUUsage()
			allocMB, totalAllocMB, sysMB := utilities.GetMemoryUsage()
			avgReadMBs, avgWriteMBs, _ := utilities.GetDiskUsage(5)
			actualLatency := utilities.GetActualLatency(n.host)
			actualGeoScore := utilities.GetGeographyScore(n.host)
			actualUptime := utilities.GetActualUptime()

			reputation := NodeReputation{
				NodeID:                n.host.ID().String(),
				Uptime:                actualUptime,
				LeadershipCount:       n.leadershipCount,
				Latency:               actualLatency,
				UserCPU:               userCPU,
				SystemCPU:             systemCPU,
				IdleCPU:               idleCPU,
				MemoryAvailable:       allocMB,
				MemoryAllocationTotal: totalAllocMB,
				MemorySystem:          sysMB,
				AvgReadMBs:            avgReadMBs,
				AvgWriteMBs:           avgWriteMBs,
				GeographyScore:        actualGeoScore,
				ContainerName:         n.containerName,
			}

			if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
				UpsertReputation(GlobalReputationDB.ReputationDB, reputation)
			}

			if err := n.publishMessage(TypeReputation, reputation); err != nil {
				logger.Error("Failed to publish reputation: %v", err)
			} else {
				score := calculateReputation(reputation)
				logger.Election("📊 Reputation published (score: %.2f)", score)
			}

		case <-n.ctx.Done():
			logger.Election("Reputation publisher shutting down")
			return
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// FIX #12: COMPLETE TOPIC RECREATION (NOT JUST SUBSCRIPTION)
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) emergencyMeshHealing() {
	logger.Election("[MESH-HEAL] ═══════════════════════════════════════════")
	logger.Election("[MESH-HEAL] INITIATING SMART MESH HEALING")
	logger.Election("[MESH-HEAL] ═══════════════════════════════════════════")

	// Rate limiting
	if time.Since(n.lastMeshHealingAttempt) < 30*time.Second {
		logger.Warn("[MESH-HEAL] Cooldown active, skipping heal (%.0fs remaining)",
			(30*time.Second - time.Since(n.lastMeshHealingAttempt)).Seconds())
		return
	}
	n.lastMeshHealingAttempt = time.Now()

	// Step 1: Get current state
	meshPeers := n.electionTopic.ListPeers()
	discoveredPeers := n.discovery.GetDiscoveredPeers()
	connectedPeers := n.host.Network().Peers()

	logger.Election("[MESH-HEAL] Current state:")
	logger.Election("[MESH-HEAL]   Mesh peers: %d", len(meshPeers))
	logger.Election("[MESH-HEAL]   Discovered peers: %d", len(discoveredPeers))
	logger.Election("[MESH-HEAL]   Connected LibP2P peers: %d", len(connectedPeers))

	// Step 2: Ensure LibP2P connections to all discovered peers
	logger.Election("[MESH-HEAL] Step 1: Ensuring LibP2P connections...")
	connectedCount := 0
	validPeerCount := 0

	for i, peerIDStr := range discoveredPeers {
		// ✅ SIMPLE VALIDATION: Only check for obviously broken IDs
		if peerIDStr == "" || len(peerIDStr) < 5 {
			logger.Warn("[MESH-HEAL]   [%d/%d] Empty or too short peer ID (skipping)",
				i+1, len(discoveredPeers))
			continue
		}

		validPeerCount++

		peerID, err := peer.Decode(peerIDStr)
		if err != nil {
			// This catches actual invalid peer IDs
			logger.Error("[MESH-HEAL]   [%d/%d] Failed to decode peer: %v",
				i+1, len(discoveredPeers), err)
			continue
		}

		// Skip self
		if peerID == n.host.ID() {
			continue
		}

		// Check if already connected
		connectedness := n.host.Network().Connectedness(peerID)
		if connectedness == 1 { // network.Connected
			logger.Election("[MESH-HEAL]   ✅ Already connected to %s", peerID.String()[:12])
			connectedCount++
			continue
		}

		// Get peer info from peerstore
		addrs := n.host.Peerstore().Addrs(peerID)
		if len(addrs) == 0 {
			logger.Warn("[MESH-HEAL]   ⚠️  No addresses for peer %s", peerID.String()[:12])
			continue
		}

		// Try to connect with timeout
		peerInfo := peer.AddrInfo{
			ID:    peerID,
			Addrs: addrs,
		}

		ctx, cancel := context.WithTimeout(n.ctx, 10*time.Second)
		err = n.host.Connect(ctx, peerInfo)
		cancel()

		if err != nil {
			logger.Warn("[MESH-HEAL]   ❌ Failed to connect to %s: %v",
				peerID.String()[:12], err)
		} else {
			logger.Election("[MESH-HEAL]   ✅ Connected to %s", peerID.String()[:12])
			connectedCount++
		}
	}

	logger.Election("[MESH-HEAL] LibP2P connections: %d/%d successful",
		connectedCount, validPeerCount)

	// Step 3: Wait for connections to stabilize
	logger.Election("[MESH-HEAL] Step 2: Waiting for connections to stabilize (3s)...")
	time.Sleep(3 * time.Second)

	// Step 4: Trigger GossipSub mesh refresh by publishing
	logger.Election("[MESH-HEAL] Step 3: Triggering GossipSub mesh refresh...")

	// Publishing forces GossipSub to evaluate mesh and send GRAFT messages
	for i := 0; i < 3; i++ {
		refreshMsg := map[string]interface{}{
			"type":      "mesh_refresh",
			"from":      n.host.ID().String(),
			"timestamp": time.Now().Unix(),
			"attempt":   i + 1,
		}

		data, err := json.Marshal(refreshMsg)
		if err != nil {
			logger.Error("[MESH-HEAL] Failed to marshal refresh message: %v", err)
			continue
		}

		ctx, cancel := context.WithTimeout(n.ctx, 5*time.Second)
		err = n.electionTopic.Publish(ctx, data)
		cancel()

		if err != nil {
			logger.Error("[MESH-HEAL] Publish attempt %d failed: %v", i+1, err)
		} else {
			logger.Election("[MESH-HEAL] Refresh message %d/3 published", i+1)
		}

		time.Sleep(2 * time.Second)
	}

	// Step 5: Wait for mesh to form
	logger.Election("[MESH-HEAL] Step 4: Waiting for mesh to form (5s)...")
	time.Sleep(5 * time.Second)

	// Step 6: Check results
	newMeshPeers := n.electionTopic.ListPeers()

	logger.Election("[MESH-HEAL] HEALING COMPLETE")
	logger.Election("[MESH-HEAL]   Before: %d mesh peers", len(meshPeers))
	logger.Election("[MESH-HEAL]   After:  %d mesh peers", len(newMeshPeers))

	if len(newMeshPeers) > len(meshPeers) {
		logger.Election("[MESH-HEAL] ✅ SUCCESS: Mesh improved")
		for i, p := range newMeshPeers {
			logger.Election("[MESH-HEAL]   [%d] %s", i+1, p.String()[:12])
		}
		n.meshHealthy = true
		n.consecutiveEmptyMeshChecks = 0
	} else if len(newMeshPeers) == 0 {
		logger.Error("[MESH-HEAL] ❌ FAILED: Mesh still empty")
		logger.Error("[MESH-HEAL] Valid peers found: %d", validPeerCount)
		logger.Error("[MESH-HEAL] Connected peers: %d", connectedCount)
		n.meshHealthy = false
		n.consecutiveEmptyMeshChecks++
	} else {
		logger.Election("[MESH-HEAL] ⚠️  PARTIAL: Mesh unchanged at %d peers", len(newMeshPeers))
		n.meshHealthy = len(newMeshPeers) > 0
	}

	logger.Election("[MESH-HEAL] ═══════════════════════════════════════════")

	// Step 7: Check if we need term reconciliation
	if len(newMeshPeers) > 0 {
		n.mutex.Lock()
		hasNoLeader := n.isLeaderEmpty()
		n.mutex.Unlock()

		n.electionMutex.Lock()
		highTerm := n.currentTerm > 15
		term := n.currentTerm
		n.electionMutex.Unlock()

		if highTerm && hasNoLeader {
			logger.Error("[MESH-HEAL] High term (%d) with no leader, forcing reconciliation", term)
			go func() {
				time.Sleep(2 * time.Second)
				n.checkTermDivergence()
			}()
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// FIX #13 & #14: FASTER TRIGGER + LOWER THRESHOLD
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) MonitorAndHealMesh() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	logger.Election("[MESH-MONITOR] Starting continuous mesh monitoring")

	for {
		select {
		case <-ticker.C:
			n.checkMeshHealth()
			n.checkTermDivergence()

		case <-n.ctx.Done():
			logger.Election("[MESH-MONITOR] Shutting down")
			return
		}
	}
}

func (n *Node) checkMeshHealth() {
	meshPeers := n.electionTopic.ListPeers()
	discoveredPeers := n.discovery.GetDiscoveredPeers()
	connectedPeers := n.host.Network().Peers()
	recentMsgPeers := n.hasRecentMessagePeers(60 * time.Second)

	meshSize := len(meshPeers)
	discoveredSize := len(discoveredPeers)
	connectedSize := len(connectedPeers)

	logger.Election("[MESH-MONITOR] mesh=%d, discovered=%d, connected=%d, msgPeers(60s)=%d",
		meshSize, discoveredSize, connectedSize, recentMsgPeers)

	// FIX: Use actual message reception as the real health indicator
	// ListPeers() returns 0 with Kubo PubSub even when messages flow
	if recentMsgPeers > 0 {
		// Messages flowing — mesh is actually working regardless of ListPeers()
		n.consecutiveEmptyMeshChecks = 0
		n.meshHealthy = true
	} else if meshSize == 0 && discoveredSize > 0 {
		n.consecutiveEmptyMeshChecks++
		logger.Warn("[MESH-MONITOR] ⚠️  UNHEALTHY: No messages received, no mesh peers (check #%d)",
			n.consecutiveEmptyMeshChecks)

		// FIX #13: Trigger after 2 checks (20s instead of 30s)
		if n.consecutiveEmptyMeshChecks >= 2 {
			logger.Error("[MESH-MONITOR] 🚨 2 consecutive empty checks, triggering IMMEDIATE healing")
			go n.emergencyMeshHealing()
			n.consecutiveEmptyMeshChecks = 0
		}
	} else if meshSize > 0 {
		n.consecutiveEmptyMeshChecks = 0
		n.meshHealthy = true

		// ✅ ONLY LOG EVERY 6TH CHECK (once per minute instead of every 10s)
		if time.Now().Unix()%60 < 10 {
			var meshCoverage float64
			if discoveredSize > 0 {
				meshCoverage = float64(meshSize) / float64(discoveredSize) * 100
			} else {
				meshCoverage = 100.0
			}
			logger.Election("[MESH-MONITOR] ✅ Healthy: mesh=%d, discovered=%d, coverage=%.0f%%",
				meshSize, discoveredSize, meshCoverage)
		}
	}

	// Extra check: High term + empty mesh
	n.electionMutex.Lock()
	highTerm := n.currentTerm > 15
	term := n.currentTerm
	n.electionMutex.Unlock()

	if highTerm && meshSize == 0 && discoveredSize > 0 {
		logger.Error("[MESH-MONITOR] High term (%d) + empty mesh detected, forcing reconciliation", term)
		go n.checkTermDivergence()
	}
}

func (n *Node) checkTermDivergence() {
	n.electionMutex.Lock()
	ourTerm := n.currentTerm
	n.electionMutex.Unlock()

	n.mutex.Lock()
	ourRole := n.role
	hasNoLeader := n.isLeaderEmpty()
	n.mutex.Unlock()

	meshPeers := n.electionTopic.ListPeers()
	discoveredPeers := n.discovery.GetDiscoveredPeers()

	// FIX #14: Lower threshold (term > 15 instead of > 20)
	// FIX #15: Robust leader empty detection
	if ourTerm > 15 && len(meshPeers) == 0 && len(discoveredPeers) > 0 && hasNoLeader {
		logger.Error("[TERM-RECONCILE] 🚨 SPLIT-BRAIN DETECTED:")
		logger.Error("[TERM-RECONCILE]    Our term: %d (suspiciously high)", ourTerm)
		logger.Error("[TERM-RECONCILE]    Our role: %s", ourRole)
		logger.Error("[TERM-RECONCILE]    Our leader: EMPTY/INVALID")
		logger.Error("[TERM-RECONCILE]    Mesh peers: %d", len(meshPeers))
		logger.Error("[TERM-RECONCILE]    Discovered: %d", len(discoveredPeers))

		// Get cluster term from reputation (proxy)
		clusterTerm := 1
		if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
			peers, err := QueryAllReputations(GlobalReputationDB.ReputationDB)
			if err == nil {
				maxLeadership := 0
				for _, peer := range peers {
					if peer.NodeID != n.host.ID().String() {
						if peer.LeadershipCount > maxLeadership {
							maxLeadership = peer.LeadershipCount
							// Estimate term from leadership count
							clusterTerm = maxLeadership * 3
							if clusterTerm > 30 {
								clusterTerm = 30 // Cap it
							}
						}
					}
				}
			}
		}

		// FORCE TERM RESET
		logger.Election("[TERM-RECONCILE] 🔄 FORCING TERM RESET")

		n.electionMutex.Lock()
		oldTerm := n.currentTerm
		n.currentTerm = clusterTerm
		if n.currentTerm < 1 {
			n.currentTerm = 1
		}
		n.electionPhase = PhaseIdle
		n.votes = make(map[string]int)
		n.votedNodes = make(map[string]string)
		n.electionMutex.Unlock()

		logger.Election("[TERM-RECONCILE]    Term: %d → %d", oldTerm, clusterTerm)

		// Reset to follower
		n.mutex.Lock()
		n.role = "Follower"
		n.leader = peer.ID("")
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		n.consecutiveHeartbeatFailures = 0
		n.mutex.Unlock()

		logger.Election("[TERM-RECONCILE]    Role: reset to Follower")
		logger.Election("[TERM-RECONCILE] Waiting for announcements...")

		// Trigger healing if mesh still empty
		if len(meshPeers) == 0 {
			logger.Election("[TERM-RECONCILE] Triggering emergency mesh healing...")
			go n.emergencyMeshHealing()
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// FIX #16: STARTUP TERM VALIDATION
// ═══════════════════════════════════════════════════════════════════════════
func (n *Node) validateStartupTerm() {
	n.electionMutex.Lock()
	currentTerm := n.currentTerm
	n.electionMutex.Unlock()

	// If starting with high term (from previous run), validate
	if currentTerm > 15 {
		logger.Warn("[STARTUP] Starting with high term: %d", currentTerm)
		logger.Warn("[STARTUP] This may indicate previous split-brain")

		// Wait for discovery
		time.Sleep(10 * time.Second)

		discoveredPeers := n.discovery.GetDiscoveredPeers()
		meshPeers := n.electionTopic.ListPeers()

		if len(discoveredPeers) > 0 && len(meshPeers) == 0 {
			logger.Error("[STARTUP] High term + empty mesh detected on startup")
			logger.Error("[STARTUP] Forcing immediate term reconciliation...")
			n.checkTermDivergence()
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// MAIN NODE INITIALIZATION
// ═══════════════════════════════════════════════════════════════════════════
func RunFullNode(ctx context.Context, host host.Host, pubsubObj *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	logger.Election("════════════════════════════════════════")
	logger.Election("OptimusDB Election v2.4.1 - BLOCKING FIX")
	logger.Election("Default coordinator + Fixed election retries + RunFullNode returns immediately")
	logger.Election("════════════════════════════════════════")

	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	if discovery.ElectionTopic != nil && discovery.ElectionSub != nil {
		electionTopic = discovery.ElectionTopic
		electionSub = discovery.ElectionSub
		logger.Election("Using pre-created GossipSub topic")
	} else {
		logger.Election("Creating new GossipSub topic: optimusdb")
		var err error
		electionTopic, err = pubsubObj.Join("optimusdb")
		if err != nil {
			logger.Error("Cannot join election topic: %v", err)
			log.Fatalf("[FATAL] Cannot join election topic: %v", err)
		}

		electionSub, err = electionTopic.Subscribe()
		if err != nil {
			logger.Error("Cannot subscribe to election topic: %v", err)
			log.Fatalf("[FATAL] Cannot subscribe to election topic: %v", err)
		}
	}

	node := NewNode(ctx, host, pubsubObj, discovery)
	node.electionTopic = electionTopic
	node.electionSub = electionSub
	node.requiredConsecutiveFailures = 3

	electionNodeMutex.Lock()
	GlobalElectionNode = node
	electionNodeMutex.Unlock()

	logger.Election("Node initialized as FOLLOWER")
	logger.Election("Peer ID: %s", node.host.ID().String())

	// Start background services
	go node.ListenForElectionEvents()
	go node.PeriodicReputationPublisher()
	go node.CheckLeaderFailure()
	go node.LogRoleStatus()
	go node.MonitorAndHealMesh()

	logger.Election("✅ Background services started")

	// FIX #16: Validate term on startup
	go node.validateStartupTerm()

	// PHASE 1: Mesh Formation
	logger.Election("═══════════════════════════════════════")
	logger.Election("PHASE 1: Mesh Formation (BLOCKING)")
	logger.Election("═══════════════════════════════════════")

	time.Sleep(5 * time.Second)

	maxMeshAttempts := 10
	meshCheckInterval := 5 * time.Second

	for attempt := 1; attempt <= maxMeshAttempts; attempt++ {
		discovered := discovery.GetDiscoveredPeers()
		meshPeers := electionTopic.ListPeers()

		logger.Election("Mesh formation attempt %d/%d:", attempt, maxMeshAttempts)
		logger.Election("   Discovered peers: %d", len(discovered))
		logger.Election("   Mesh peers: %d", len(meshPeers))

		if len(meshPeers) > 0 {
			logger.Election("✅ MESH FORMED with %d peers", len(meshPeers))
			node.meshHealthy = true
			break
		}

		if len(discovered) > 0 && len(meshPeers) == 0 {
			logger.Warn("⚠️  Peers discovered but mesh not formed")

			if attempt >= 3 {
				logger.Error("🚨 Mesh still empty after %d attempts, forcing healing...", attempt)
				node.emergencyMeshHealing()
			} else {
				testMsg := map[string]string{
					"type":    "mesh_formation_test",
					"from":    node.host.ID().String(),
					"attempt": fmt.Sprintf("%d", attempt),
				}
				if data, _ := json.Marshal(testMsg); data != nil {
					electionTopic.Publish(ctx, data)
				}
			}
		}

		if attempt < maxMeshAttempts {
			time.Sleep(meshCheckInterval)
		}
	}

	finalMeshPeers := electionTopic.ListPeers()
	discoveredPeers := discovery.GetDiscoveredPeers()

	if len(discoveredPeers) > 0 && len(finalMeshPeers) == 0 {
		logger.Error("❌ CRITICAL: Mesh formation FAILED after %d attempts", maxMeshAttempts)
		logger.Error("   Node is ISOLATED - elections BLOCKED")
		logger.Error("   Continuous healing will retry in background")
		node.meshHealthy = false
	} else if len(finalMeshPeers) < len(discoveredPeers) {
		logger.Warn("⚠️  PARTIAL MESH: %d/%d peers", len(finalMeshPeers), len(discoveredPeers))
		node.meshHealthy = true
	} else {
		logger.Election("✅ MESH FORMATION COMPLETE: %d peers", len(finalMeshPeers))
		node.meshHealthy = true
	}

	logger.Election("═══════════════════════════════════════")

	// PHASE 2: Discovery Stabilization (only if mesh healthy)
	// FIX #23 (v2.4.1): Return immediately when mesh is unhealthy instead of
	// blocking on sigChan. Background goroutines (MonitorAndHealMesh, CheckLeaderFailure)
	// will heal the mesh and trigger elections dynamically via ctx. This allows
	// main.go to proceed and launch the semantic search goroutine.
	if !node.meshHealthy {
		logger.Error("Skipping election initiation due to unhealthy mesh")
		logger.Error("Node will join elections dynamically when mesh heals")
		logger.Election("[ELECTION] RunFullNode returning to main (unhealthy mesh path)")
		return node
	}

	logger.Election("════════════════════════════════════════")
	logger.Election("PHASE 2: Discovery Stabilization")
	logger.Election("════════════════════════════════════════")

	time.Sleep(10 * time.Second)

	var stablePeerIDs []string
	stableCount := 0
	requiredStableChecks := 5
	maxAttempts := 15

	for attempt := 0; attempt < maxAttempts; attempt++ {
		discoveredPeersNow := discovery.GetDiscoveredPeers()
		currentPeerIDs := []string{node.host.ID().String()}

		for _, p := range discoveredPeersNow {
			currentPeerIDs = append(currentPeerIDs, p)
		}
		sort.Strings(currentPeerIDs)

		if attempt > 0 {
			sameAsBefore := len(currentPeerIDs) == len(stablePeerIDs)
			if sameAsBefore {
				for i := range currentPeerIDs {
					if currentPeerIDs[i] != stablePeerIDs[i] {
						sameAsBefore = false
						break
					}
				}
			}

			if sameAsBefore {
				stableCount++
				logger.Election("Discovery stable (%d/%d checks)",
					stableCount, requiredStableChecks)

				if stableCount >= requiredStableChecks {
					logger.Election("✅ Discovery stabilized with %d peers",
						len(currentPeerIDs))

					meshPeers := electionTopic.ListPeers()
					var meshCoverage float64
					if len(discoveredPeersNow) > 0 {
						meshCoverage = float64(len(meshPeers)) / float64(len(discoveredPeersNow))
					} else {
						meshCoverage = 1.0
					}

					logger.Election("Mesh coverage: %.1f%% (%d/%d peers)",
						meshCoverage*100, len(meshPeers), len(discoveredPeersNow))

					if meshCoverage >= 0.8 || len(meshPeers) >= len(discoveredPeersNow) {
						logger.Election("✅ Mesh coverage sufficient")
						break
					} else {
						logger.Warn("⚠️  Mesh coverage low (%.1f%%), waiting...", meshCoverage*100)
						stableCount = max(0, stableCount-1)
					}
				}
			} else {
				stableCount = 0
				logger.Election("Discovery changed, restarting stability count")
			}
		}

		stablePeerIDs = currentPeerIDs
		time.Sleep(3 * time.Second)
	}

	allPeerIDs := stablePeerIDs
	if len(allPeerIDs) == 0 {
		logger.Warn("No peers discovered, using self only")
		allPeerIDs = []string{node.host.ID().String()}
	}

	logger.Election("Cluster composition:")
	for i, peerID := range allPeerIDs {
		logger.Election("  [%d] %s", i+1, peerID)
	}

	// Store self reputation
	selfRep := NodeReputation{
		NodeID:         node.host.ID().String(),
		Uptime:         1.0,
		GeographyScore: 0.5,
	}
	if GlobalReputationDB != nil && GlobalReputationDB.ReputationDB != nil {
		UpsertReputation(GlobalReputationDB.ReputationDB, selfRep)
	}

	peers, err := QueryAllReputations(GlobalReputationDB.ReputationDB)
	if err != nil || len(peers) == 0 {
		peers = []NodeReputation{selfRep}
	}

	containerName := node.containerName

	// Register our own peer ID → container name mapping
	node.peerContainerNamesMu.Lock()
	node.peerContainerNames[node.host.ID().String()] = containerName
	node.peerContainerNamesMu.Unlock()
	logger.Election("════════════════════════════════════════")
	logger.Election("Container Name: %s", containerName)
	logger.Election("════════════════════════════════════════")

	// ═══════════════════════════════════════════════════════════════
	// FIX: DEFAULT COORDINATOR FROM CONTAINER NAME
	// If this is optimusdb1 (container name ends with "1"), immediately
	// self-promote as coordinator. This breaks the chicken-and-egg problem
	// where GossipSub mesh issues prevent vote propagation, which prevents
	// any coordinator from being elected, which prevents heartbeats.
	// Other nodes will accept this via the announcement mechanism.
	// If a node with higher reputation later wins a proper election,
	// it will take over via the normal term-based precedence.
	// ═══════════════════════════════════════════════════════════════
	if isDefaultCoordinatorNode(containerName) {
		logger.Election("👑 DEFAULT COORDINATOR: %s", containerName)
		logger.Election("   Self-promoting as startup coordinator...")

		// Brief delay to let other nodes finish their own stabilization
		time.Sleep(3 * time.Second)

		node.promoteAsDefaultCoordinator()

		logger.Election("✅ Default coordinator active, heartbeats will start")
		logger.Election("   Other nodes can trigger re-election if they have")
		logger.Election("   a candidate with higher reputation")
	} else {
		// Non-default nodes: try normal election, but with working retries
		initiatorID := selectInitiatorDeterministic(allPeerIDs)

		logger.Election("════════════════════════════════════════")
		logger.Election("Consensus Initiator Selection")
		logger.Election("   Algorithm: Deterministic consistent hashing")
		logger.Election("   Input: %d peer IDs (sorted)", len(allPeerIDs))
		logger.Election("   Selected: %s", initiatorID)
		logger.Election("════════════════════════════════════════")

		if node.host.ID().String() == initiatorID {
			logger.Election("👑 I AM THE CONSENSUS INITIATOR")

			// FIX: Don't block on ListPeers() — it lies with Kubo PubSub
			// Always attempt election; StartElection has its own checks
			finalCheck := electionTopic.ListPeers()
			recentMsgPeers := node.hasRecentMessagePeers(60 * time.Second)
			logger.Election("   Pre-election check: ListPeers=%d, recentMsgPeers=%d",
				len(finalCheck), recentMsgPeers)

			if len(finalCheck) == 0 && recentMsgPeers == 0 && len(discoveredPeers) > 0 {
				logger.Warn("🚫 Node appears truly isolated — healing + proceeding anyway")
				go node.emergencyMeshHealing()
			}

			// Always attempt election regardless of ListPeers()
			logger.Election("   Starting election in 2s...")
			jitter := time.Duration(rand.Intn(500)) * time.Millisecond
			time.Sleep(2*time.Second + jitter)
			go node.StartElection(peers, 0)
		} else {
			logger.Election("📋 FOLLOWER: Initiator is %s", initiatorID)
			logger.Election("   Waiting for election votes...")
		}
	}

	// FIX #23 (v2.4.1): Return immediately — do NOT block on sigChan.
	// main.go holds termCtx which cancels all background goroutines on shutdown
	// (ListenForElectionEvents, PeriodicReputationPublisher, CheckLeaderFailure,
	// LogRoleStatus, MonitorAndHealMesh all check ctx.Done()).
	// The old sigChan block here was the root cause of the semantic search goroutine
	// never launching — main.go never reached the line after election.RunFullNode().
	logger.Election("[ELECTION] RunFullNode returning to main — all background services running")
	return node
}

func NewNode(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	return &Node{
		ctx:                          ctx,
		host:                         host,
		pubsub:                       pubsub,
		discovery:                    discovery,
		topicManager:                 NewTopicManager(pubsub),
		role:                         "Follower",
		votes:                        make(map[string]int),
		votedNodes:                   make(map[string]string),
		votedForInTerm:               make(map[int]string),
		announcedLeaderForElection:   make(map[string]string),
		electionPhase:                PhaseIdle,
		currentTerm:                  0,
		rateLimiter:                  NewMessageRateLimiter(),
		consecutiveHeartbeatFailures: 0,
		requiredConsecutiveFailures:  3,
		meshHealthy:                  false,
		consecutiveEmptyMeshChecks:   0,
		containerName:                getContainerName(),
		consecutiveElectionFailures:  0,
		lastMessageFromPeer:          make(map[string]time.Time),
		peerContainerNames:           make(map[string]string),
	}
}

// Database access functions
func UpsertReputation(db *sql.DB, rep NodeReputation) error {
	query := `INSERT INTO reputation (
		node_id, uptime, leadership_count, latency, user_cpu, system_cpu,
		idle_cpu, memory_available, memory_total_alloc, memory_sys,
		avg_read_mbs, avg_write_mbs, geography_score
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(node_id) DO UPDATE SET
		uptime = excluded.uptime,
		leadership_count = excluded.leadership_count,
		latency = excluded.latency,
		user_cpu = excluded.user_cpu,
		system_cpu = excluded.system_cpu,
		idle_cpu = excluded.idle_cpu,
		memory_available = excluded.memory_available,
		memory_total_alloc = excluded.memory_total_alloc,
		memory_sys = excluded.memory_sys,
		avg_read_mbs = excluded.avg_read_mbs,
		avg_write_mbs = excluded.avg_write_mbs,
		geography_score = excluded.geography_score;`

	_, err := db.Exec(query,
		rep.NodeID, rep.Uptime, rep.LeadershipCount, rep.Latency,
		rep.UserCPU, rep.SystemCPU, rep.IdleCPU,
		rep.MemoryAvailable, rep.MemoryAllocationTotal, rep.MemorySystem,
		rep.AvgReadMBs, rep.AvgWriteMBs, rep.GeographyScore)
	return err
}

func QueryAllReputations(db *sql.DB) ([]NodeReputation, error) {
	rows, err := db.Query(`SELECT * FROM reputation`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reps []NodeReputation
	for rows.Next() {
		var rep NodeReputation
		if err := rows.Scan(
			&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
			&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
			&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
			&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
		); err != nil {
			return nil, err
		}
		reps = append(reps, rep)
	}
	return reps, nil
}

func GetReputationByID(db *sql.DB, nodeID string) (NodeReputation, error) {
	row := db.QueryRow(`SELECT * FROM reputation WHERE node_id = ?`, nodeID)
	var rep NodeReputation
	err := row.Scan(
		&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
		&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
		&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
		&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
	)
	return rep, err
}

func InsertElectionLog(db *sql.DB, id string, timestamp time.Time, leaderID string, term int, votes map[string]int) error {
	votesJSON, _ := json.Marshal(votes)
	_, err := db.Exec(
		`INSERT INTO election_log (id, timestamp, leader_id, term, votes_json) VALUES (?, ?, ?, ?, ?);`,
		id, timestamp.Format(time.RFC3339), leaderID, term, string(votesJSON))
	return err
}

func (r *ReputationSQLite) SafeExec(query string, args ...interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.ReputationDB.Exec(query, args...)
	return err
}

func (n *Node) LogRoleStatus() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.mutex.Lock()
			role := n.role
			leader := n.leader
			term := n.currentTerm
			n.mutex.Unlock()

			meshPeers := n.electionTopic.ListPeers()

			if role == "Coordinator" {
				logger.Election("[STATUS] 👑 COORDINATOR (term %d, mesh: %d peers)", term, len(meshPeers))
			} else {
				logger.Election("[STATUS] 📋 FOLLOWER following %s (term %d, mesh: %d peers)",
					leader.String(), term, len(meshPeers))
			}

		case <-n.ctx.Done():
			return
		}
	}
}

func GetNodeStatus() (role string, leader string, term int, leadershipCount int) {
	electionNodeMutex.RLock()
	defer electionNodeMutex.RUnlock()

	if GlobalElectionNode == nil {
		return "Unknown", "", 0, 0
	}

	GlobalElectionNode.mutex.Lock()
	role = GlobalElectionNode.role
	leader = GlobalElectionNode.leader.String()
	term = GlobalElectionNode.currentTerm
	leadershipCount = GlobalElectionNode.leadershipCount
	GlobalElectionNode.mutex.Unlock()

	return
}

func GetAllPeersReputation() ([]NodeReputation, error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return nil, fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT node_id, uptime, leadership_count, latency, user_cpu, system_cpu, 
              idle_cpu, memory_available, memory_total_alloc, memory_sys, 
              avg_read_mbs, avg_write_mbs, geography_score 
              FROM reputation ORDER BY node_id`

	rows, err := GlobalReputationDB.ReputationDB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var reputations []NodeReputation
	for rows.Next() {
		var rep NodeReputation
		err := rows.Scan(
			&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
			&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
			&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
			&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
		)
		if err != nil {
			logger.Error("Failed to scan reputation row: %v", err)
			continue
		}
		reputations = append(reputations, rep)
	}

	return reputations, nil
}

func GetPeerReputation(peerID string) (*NodeReputation, error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return nil, fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT node_id, uptime, leadership_count, latency, user_cpu, system_cpu, 
              idle_cpu, memory_available, memory_total_alloc, memory_sys, 
              avg_read_mbs, avg_write_mbs, geography_score 
              FROM reputation WHERE node_id = ?`

	row := GlobalReputationDB.ReputationDB.QueryRow(query, peerID)

	var rep NodeReputation
	err := row.Scan(
		&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
		&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
		&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
		&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func CalculateHealthScore(nr NodeReputation) float64 {
	return calculateReputation(nr)
}

func GetLatestElectionInfo() (leaderID string, term int, timestamp string, err error) {
	if GlobalReputationDB == nil || GlobalReputationDB.ReputationDB == nil {
		return "", 0, "", fmt.Errorf("reputation database not initialized")
	}

	query := `SELECT leader_id, term, timestamp FROM election_log 
              ORDER BY timestamp DESC LIMIT 1`

	row := GlobalReputationDB.ReputationDB.QueryRow(query)
	err = row.Scan(&leaderID, &term, &timestamp)

	if err == sql.ErrNoRows {
		return "", 0, "", nil
	}

	return leaderID, term, timestamp, err
}

// createGossipSubWithBetterScoring creates GossipSub with lenient peer scoring
// to prevent legitimate peers from being dropped from the mesh
// CreateBetterGossipSubParams creates GossipSub with lenient peer scoring
// to prevent legitimate peers from being dropped from the mesh
func CreateBetterGossipSubParams() pubsub.Option {
	// Lenient peer scoring parameters
	peerScoreParams := &pubsub.PeerScoreParams{
		// Give all peers positive score by default
		AppSpecificScore: func(p peer.ID) float64 {
			return 100.0 // Everyone starts with high score
		},
		AppSpecificWeight: 0.5, // Reduced weight

		Topics: map[string]*pubsub.TopicScoreParams{
			"optimusdb": {
				TopicWeight:                     0.5,   // Reduced from 1.0
				TimeInMeshWeight:                0.001, // Very low (was 0.01)
				TimeInMeshQuantum:               time.Second,
				TimeInMeshCap:                   100.0,
				FirstMessageDeliveriesWeight:    0.5, // Reduced from 1.0
				FirstMessageDeliveriesDecay:     0.99,
				FirstMessageDeliveriesCap:       100,
				MeshMessageDeliveriesWeight:     -0.5, // Less penalty
				MeshMessageDeliveriesDecay:      0.99,
				MeshMessageDeliveriesCap:        100,
				MeshMessageDeliveriesThreshold:  1, // Lower threshold
				MeshMessageDeliveriesWindow:     2 * time.Second,
				MeshMessageDeliveriesActivation: 10 * time.Second,
				MeshFailurePenaltyWeight:        -0.5, // Less penalty
				MeshFailurePenaltyDecay:         0.99,
				InvalidMessageDeliveriesWeight:  -10.0, // Reduced from -100
				InvalidMessageDeliveriesDecay:   0.99,
			},
		},

		DecayInterval: time.Second,
		DecayToZero:   0.01,
		RetainScore:   10 * time.Minute,
	}

	// ✅ SIMPLIFIED: Only use the core threshold fields that exist in all versions
	peerScoreThresholds := &pubsub.PeerScoreThresholds{
		GossipThreshold:   -100,  // Very lenient (peers can gossip even with low score)
		PublishThreshold:  -500,  // Very lenient (peers can publish even with low score)
		GraylistThreshold: -1000, // Very lenient (only ban truly malicious peers)
	}

	return pubsub.WithPeerScore(peerScoreParams, peerScoreThresholds)
}

// ═══════════════════════════════════════════════════════════════════════════
// HELPER: Validate peer ID format before decoding
// ═══════════════════════════════════════════════════════════════════════════
func isValidPeerID(peerIDStr string) bool {
	// Length check
	if len(peerIDStr) < 10 || len(peerIDStr) > 100 {
		return false
	}

	// Check for binary garbage (non-printable characters)
	// Valid peer IDs are base58 encoded, so only alphanumeric chars
	for _, ch := range peerIDStr {
		// Allow: 0-9, A-Z, a-z (base58 charset)
		if !((ch >= '0' && ch <= '9') ||
			(ch >= 'A' && ch <= 'Z') ||
			(ch >= 'a' && ch <= 'z')) {
			return false
		}
	}

	// Check for common invalid patterns
	if strings.Contains(peerIDStr, "peer.ID") ||
		strings.Contains(peerIDStr, "<") ||
		strings.Contains(peerIDStr, ">") ||
		strings.Contains(peerIDStr, " ") {
		return false
	}

	return true
}
