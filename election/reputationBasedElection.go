package election

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/utilities"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

var GlobalReputationDB *ReputationSQLite

type ReputationSQLite struct {
	reputationDB *sql.DB
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

	// Get or create topic
	topic, ok := tm.topics[name]
	if !ok {
		log.Printf("[TOPIC] Creating new topic: %s", name)
		var err error
		topic, err = tm.pubsub.Join(name)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to join topic '%s': %w", name, err)
		}
		tm.topics[name] = topic
	} else {
		log.Printf("[TOPIC] Reusing existing topic: %s", name)
	}

	// Get or create subscription
	sub, ok := tm.subs[name]
	if !ok {
		log.Printf("[TOPIC] Creating new subscription for: %s", name)
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

// Message types
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

// Node state
type Node struct {
	ctx             context.Context
	host            host.Host
	pubsub          *pubsub.PubSub
	topicManager    *TopicManager
	leader          peer.ID
	mutex           sync.Mutex
	lastHeartbeat   time.Time
	heartbeatMissed int
	role            string
	discovery       *app.KnowledgeBaseDB
	electionTopic   *pubsub.Topic
	electionSub     *pubsub.Subscription
	leadershipCount int

	votes                      map[string]int
	votedNodes                 map[string]string
	currentElectionID          string
	electionMutex              sync.Mutex
	isElecting                 int32
	lastElection               time.Time
	announcedLeaderForElection map[string]string
	announcementMutex          sync.Mutex

	currentTerm      int
	votedForInTerm   map[int]string
	electionPhase    string
	electionDeadline time.Time
	listenerStarted  int32
	electionCancel   context.CancelFunc
	peerCount        int
}

// Reputation weights
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

// DB initialization
func InitReputationDB() (*ReputationSQLite, error) {
	rdbmsCache := filepath.Join(filepath.Join(filepath.Join(os.Getenv("HOME"), ".cache"), "optimusdb", *config.FlagRepo, "optimusdb"), "optimusreputation.db")
	dir := filepath.Dir(rdbmsCache)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for Reputation DB: %w", err)
	}
	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		log.Fatalf("[FATAL] Cannot open SQLite DB: %v", err)
	}
	GlobalReputationDB = &ReputationSQLite{reputationDB: db}
	if err := GlobalReputationDB.createReputationDB(); err != nil {
		log.Fatalf("[ERROR] Table creation failed for Reputation DB: %v", err)
		return nil, err
	}
	log.Println("[INFO] SQLite Reputation Database Ready at:", rdbmsCache)
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
	if _, err := rep.reputationDB.Exec(tableQuery); err != nil {
		return err
	}

	electionLogQuery := `CREATE TABLE IF NOT EXISTS election_log (
		id TEXT PRIMARY KEY,
		timestamp TEXT,
		leader_id TEXT,
		term INTEGER,
		votes_json TEXT
	);`
	if _, err := rep.reputationDB.Exec(electionLogQuery); err != nil {
		return fmt.Errorf("failed to create election_log table: %w", err)
	}

	return nil
}

func calculateReputation(nr NodeReputation) float64 {
	w := getReputationWeights()
	cpuScore := 100 - (nr.UserCPU + nr.SystemCPU)
	memoryScore := nr.MemoryAvailable
	diskScore := 100 - (nr.AvgReadMBs + nr.AvgWriteMBs)
	latencyScore := 100 - nr.Latency

	return (w["uptime"] * nr.Uptime) +
		(w["leadership"] * float64(nr.LeadershipCount)) +
		(w["cpu"] * cpuScore) +
		(w["memory"] * memoryScore) +
		(w["disk"] * diskScore) +
		(w["latency"] * latencyScore) +
		(w["geography_score"] * nr.GeographyScore)
}

// IMPROVED publish with better debugging
func (n *Node) publishMessage(msgType string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload failed: %w", err)
	}

	core := CoreMessage{Type: msgType, Payload: data}
	coreData, err := json.Marshal(core)
	if err != nil {
		return fmt.Errorf("marshal core failed: %w", err)
	}

	// Check mesh status before publishing
	meshPeers := n.electionTopic.ListPeers()
	log.Printf("[PUBLISH-DEBUG] Type: %s, Size: %d bytes, Mesh peers: %d",
		msgType, len(coreData), len(meshPeers))

	if len(meshPeers) == 0 {
		log.Printf("[PUBLISH-WARN] No mesh peers! Message may not propagate")

		// List all connected peers for comparison
		allPeers := n.host.Network().Peers()
		log.Printf("[PUBLISH-DEBUG] Connected peers: %d", len(allPeers))

		// Check subscription status
		topics := n.pubsub.GetTopics()
		log.Printf("[PUBLISH-DEBUG] Our subscribed topics: %v", topics)
	}

	// Publish with retries
	for attempt := 0; attempt < 3; attempt++ {
		err = n.electionTopic.Publish(n.ctx, coreData)
		if err == nil {
			log.Printf("[PUBLISH] ✅ %s published (attempt %d)", msgType, attempt+1)
			return nil
		}

		log.Printf("[PUBLISH] ⚠️ Attempt %d failed: %v", attempt+1, err)
		if attempt < 2 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	return fmt.Errorf("failed after 3 attempts: %w", err)
}

// StartElection with better coordination
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
	if !atomic.CompareAndSwapInt32(&n.isElecting, 0, 1) {
		log.Printf("[ELECTION] Already in progress, skipping")
		return
	}
	defer atomic.StoreInt32(&n.isElecting, 0)

	discoveredPeers := n.discovery.GetDiscoveredPeers()
	totalPeers := len(discoveredPeers) + 1

	n.electionMutex.Lock()
	n.currentTerm++
	term := n.currentTerm
	n.peerCount = totalPeers
	n.electionMutex.Unlock()

	log.Printf("[ELECTION] ════════════════════════════════════════")
	log.Printf("[ELECTION] Starting Term %d, Attempt %d", term, attempt+1)
	log.Printf("[ELECTION] Discovered: %d, Total cluster: %d", len(discoveredPeers), totalPeers)
	log.Printf("[ELECTION] Topic peers: %d", len(n.electionTopic.ListPeers()))
	log.Printf("[ELECTION] ════════════════════════════════════════")

	// Generate election ID
	electionID := fmt.Sprintf("%s-term%d-%d-attempt%d",
		n.host.ID().String(), term, time.Now().UnixNano(), attempt)

	n.electionMutex.Lock()
	n.currentElectionID = electionID
	n.electionPhase = PhaseVoting
	n.electionDeadline = time.Now().Add(electionTimeout)
	n.votes = make(map[string]int)
	n.votedNodes = make(map[string]string)
	n.electionMutex.Unlock()

	// Ensure we have candidates
	if len(peers) == 0 {
		peers = []NodeReputation{{NodeID: n.host.ID().String()}}
	}

	// Select and vote
	selected := n.selectCandidate(peers)
	vote := VoteMessage{
		NodeID:     n.host.ID().String(),
		Vote:       selected,
		ElectionID: electionID,
		Term:       term,
	}

	// Record own vote immediately
	n.electionMutex.Lock()
	n.votedNodes[vote.NodeID] = vote.Vote
	n.votes[vote.Vote]++
	n.electionMutex.Unlock()

	// Publish vote
	if err := n.publishMessage(TypeVote, vote); err != nil {
		log.Printf("[ERROR] Failed to publish vote: %v", err)
	}

	log.Printf("[ELECTION] Node %s voted for %s",
		vote.NodeID[:min(8, len(vote.NodeID))]+"...", vote.Vote[:min(8, len(vote.Vote))]+"...")

	// Wait for votes
	electionCtx, cancel := context.WithTimeout(n.ctx, electionTimeout)
	defer cancel()

	n.electionMutex.Lock()
	n.electionCancel = cancel
	n.electionMutex.Unlock()

	<-electionCtx.Done()
	n.finalizeElection(term, electionID, attempt, peers)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (n *Node) selectCandidate(peers []NodeReputation) string {
	if len(peers) == 0 {
		return n.host.ID().String()
	}

	// Weight-based selection
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

// RELAXED quorum for small networks
func (n *Node) finalizeElection(term int, electionID string, attempt int, peers []NodeReputation) {
	n.electionMutex.Lock()
	if n.currentElectionID != electionID || n.currentTerm != term {
		n.electionMutex.Unlock()
		return
	}
	n.electionPhase = PhaseCompleted

	log.Printf("[ELECTION] ════════════════════════════════════════")
	log.Printf("[ELECTION] Results for Term %d:", term)
	for candidate, count := range n.votes {
		shortCandidate := candidate[:min(8, len(candidate))] + "..."
		log.Printf("[ELECTION]   %s: %d votes", shortCandidate, count)
	}
	log.Printf("[ELECTION] Participation: %d/%d nodes voted", len(n.votedNodes), n.peerCount)
	log.Printf("[ELECTION] ════════════════════════════════════════")

	winner := n.determineWinner()
	votesCopy := make(map[string]int)
	for k, v := range n.votes {
		votesCopy[k] = v
	}
	n.electionMutex.Unlock()

	if winner == "" {
		log.Printf("[ELECTION] No winner, attempt %d/%d", attempt+1, 3)
		if attempt < 2 {
			time.Sleep(time.Duration(math.Pow(2, float64(attempt))) * time.Second)
			n.StartElection(peers, attempt+1)
		} else {
			n.fallbackElection()
		}
		return
	}

	shortWinner := winner[:min(8, len(winner))] + "..."
	log.Printf("[ELECTION] ✅ WINNER: %s with %d votes", shortWinner, votesCopy[winner])
	n.announceLeader(winner, term)
}

// RELAXED winner determination for Docker
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

	// Very relaxed: accept any winner with votes
	participation := len(n.votedNodes)
	required := 1

	if n.peerCount <= 3 {
		required = 1 // Small cluster: any vote wins
	} else if n.peerCount <= 8 {
		required = 2 // Medium: need 2 votes
	} else {
		required = (n.peerCount * 3) / 10 // Large: 30%
	}

	log.Printf("[ELECTION] Participation: %d, Required: %d", participation, required)

	if participation >= required && maxVotes >= 1 {
		return winner
	}

	return ""
}

// ENHANCED listener with detailed logging
func (n *Node) ListenForElectionEvents() {
	if !atomic.CompareAndSwapInt32(&n.listenerStarted, 0, 1) {
		return
	}

	log.Println("[LISTENER] ════════════════════════════════════")
	log.Println("[LISTENER] Starting election listener")
	log.Printf("[LISTENER] Node: %s", n.host.ID().String())
	log.Println("[LISTENER] ════════════════════════════════════")

	if n.electionSub == nil {
		log.Fatal("[LISTENER] No subscription!")
	}

	go func() {
		msgCount := 0
		for {
			msg, err := n.electionSub.Next(n.ctx)
			if err != nil {
				if n.ctx.Err() != nil {
					return
				}
				log.Printf("[ERROR] Receive failed: %v", err)
				continue
			}

			msgCount++
			from := msg.ReceivedFrom.String()
			if len(from) > 8 {
				from = from[:8] + "..."
			}

			log.Printf("[MSG-RX-%d] From: %s, Size: %d bytes", msgCount, from, len(msg.Data))

			var core CoreMessage
			if err := json.Unmarshal(msg.Data, &core); err != nil {
				log.Printf("[ERROR] Unmarshal failed: %v", err)
				continue
			}

			log.Printf("[MSG-RX-%d] Type: %s", msgCount, core.Type)
			n.handleMessage(core, msg.ReceivedFrom)
		}
	}()
}

// Message handler
func (n *Node) handleMessage(core CoreMessage, from peer.ID) {
	switch core.Type {
	case TypeVote:
		var vote VoteMessage
		if err := json.Unmarshal(core.Payload, &vote); err != nil {
			return
		}

		shortNode := vote.NodeID[:min(8, len(vote.NodeID))] + "..."
		shortVote := vote.Vote[:min(8, len(vote.Vote))] + "..."
		shortElection := vote.ElectionID
		if len(shortElection) > 20 {
			shortElection = shortElection[:20] + "..."
		}

		log.Printf("[VOTE-RX] %s voted for %s (election: %s, term: %d)",
			shortNode, shortVote, shortElection, vote.Term)

		n.handleVote(vote)

	case TypeHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(core.Payload, &hb); err != nil {
			return
		}
		shortLeader := hb.LeaderID[:min(8, len(hb.LeaderID))] + "..."
		log.Printf("[HB-RX] From %s (term %d)", shortLeader, hb.Term)
		n.handleHeartbeat(hb)

	case TypeReputation:
		var rep NodeReputation
		if err := json.Unmarshal(core.Payload, &rep); err != nil {
			return
		}
		if rep.NodeID != n.host.ID().String() {
			shortNode := rep.NodeID[:min(8, len(rep.NodeID))] + "..."
			log.Printf("[REP-RX] From %s, Score: %.2f",
				shortNode, calculateReputation(rep))
			UpsertReputation(GlobalReputationDB.reputationDB, rep)
		}

	case TypeAnnouncement:
		var ann map[string]interface{}
		if err := json.Unmarshal(core.Payload, &ann); err != nil {
			return
		}
		leaderID, _ := ann["leader"].(string)
		term := int(ann["term"].(float64))
		shortLeader := leaderID[:min(8, len(leaderID))] + "..."
		log.Printf("[ANNOUNCE-RX] Leader: %s (term %d)", shortLeader, term)
		n.handleAnnouncement(leaderID, term)

	case TypeElectionResult:
		var result ElectionResultMessage
		if err := json.Unmarshal(core.Payload, &result); err != nil {
			return
		}
		shortLeader := result.LeaderID[:min(8, len(result.LeaderID))] + "..."
		log.Printf("[RESULT-RX] Leader: %s, Term: %d, Votes: %v",
			shortLeader, result.Term, result.Votes)
	}
}

func (n *Node) handleVote(vote VoteMessage) {
	n.electionMutex.Lock()
	defer n.electionMutex.Unlock()

	// Join election if idle
	if n.electionPhase == PhaseIdle {
		n.electionPhase = PhaseVoting
		n.currentElectionID = vote.ElectionID
		n.currentTerm = vote.Term
		n.electionDeadline = time.Now().Add(electionTimeout)
		n.votes = make(map[string]int)
		n.votedNodes = make(map[string]string)
	}

	// Validate
	if n.electionPhase != PhaseVoting ||
		vote.ElectionID != n.currentElectionID ||
		vote.Term != n.currentTerm {
		return
	}

	// Record vote
	if _, hasVoted := n.votedNodes[vote.NodeID]; !hasVoted {
		n.votedNodes[vote.NodeID] = vote.Vote
		n.votes[vote.Vote]++
		shortNode := vote.NodeID[:min(8, len(vote.NodeID))] + "..."
		shortVote := vote.Vote[:min(8, len(vote.Vote))] + "..."
		log.Printf("[ELECTION] Recorded: %s → %s (total: %d)",
			shortNode, shortVote, n.votes[vote.Vote])
	}
}

func (n *Node) handleHeartbeat(hb HeartbeatMessage) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.role == "Follower" {
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		n.leader = peer.ID(hb.LeaderID)
	}
}

func (n *Node) handleAnnouncement(leaderID string, term int) {
	n.mutex.Lock()
	if leaderID == n.host.ID().String() {
		n.role = "Coordinator"
		n.leader = peer.ID(leaderID)
		n.leadershipCount++
		log.Printf("[ROLE] ✅ I AM COORDINATOR (term %d)", term)
	} else {
		n.role = "Follower"
		n.leader = peer.ID(leaderID)
		n.lastHeartbeat = time.Now()
		n.heartbeatMissed = 0
		shortLeader := leaderID[:min(8, len(leaderID))] + "..."
		log.Printf("[ROLE] Following %s (term %d)", shortLeader, term)
	}
	n.mutex.Unlock()

	n.electionMutex.Lock()
	n.currentTerm = term
	n.electionMutex.Unlock()
}

// Leader announcement
func (n *Node) announceLeader(leaderID string, term int) {
	announcement := map[string]interface{}{"leader": leaderID, "term": term}
	if err := n.publishMessage(TypeAnnouncement, announcement); err != nil {
		log.Printf("[ERROR] Failed to announce leader: %v", err)
		return
	}

	shortLeader := leaderID[:min(8, len(leaderID))] + "..."
	log.Printf("[LEADER] Announced: %s (term %d)", shortLeader, term)

	// Update role
	n.handleAnnouncement(leaderID, term)

	// Start heartbeat if coordinator
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

	for {
		select {
		case <-ticker.C:
			n.mutex.Lock()
			if n.role != "Coordinator" {
				n.mutex.Unlock()
				return
			}
			n.mutex.Unlock()

			hb := HeartbeatMessage{
				LeaderID: n.host.ID().String(),
				Time:     time.Now().Unix(),
				Term:     term,
			}

			if err := n.publishMessage(TypeHeartbeat, hb); err != nil {
				log.Printf("[ERROR] Heartbeat failed: %v", err)
			} else {
				log.Printf("[HEARTBEAT] Sent (term %d)", term)
			}

		case <-n.ctx.Done():
			return
		}
	}
}

func (n *Node) fallbackElection() {
	peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
	if len(peers) == 0 {
		// Use self as fallback
		n.announceLeader(n.host.ID().String(), n.currentTerm+1)
		return
	}

	// Pick highest reputation
	var best NodeReputation
	maxScore := -1.0
	for _, p := range peers {
		if score := calculateReputation(p); score > maxScore {
			maxScore = score
			best = p
		}
	}

	n.announceLeader(best.NodeID, n.currentTerm+1)
}

// Check for leader failure
func (n *Node) CheckLeaderFailure() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		n.mutex.Lock()

		if n.role == "Coordinator" {
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
			log.Printf("[WARN] Missed %d heartbeats (last: %v ago)",
				n.heartbeatMissed, timeSince)

			if n.heartbeatMissed >= heartbeatRetryLimit {
				log.Println("[FAILURE] Leader dead, starting election")
				n.heartbeatMissed = 0
				n.mutex.Unlock()

				if atomic.LoadInt32(&n.isElecting) == 0 {
					go func() {
						peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
						n.StartElection(peers, 0)
					}()
				}
				continue
			}
		} else {
			n.heartbeatMissed = 0
		}
		n.mutex.Unlock()
	}
}

// Reputation publisher
func (n *Node) PeriodicReputationPublisher() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			userCPU, systemCPU, idleCPU, _ := utilities.GetCPUUsage()
			allocMB, totalAllocMB, sysMB := utilities.GetMemoryUsage()
			avgReadMBs, avgWriteMBs, _ := utilities.GetDiskUsage(5)

			reputation := NodeReputation{
				NodeID:                n.host.ID().String(),
				Uptime:                float64(time.Now().Unix()%1000) / 1000,
				LeadershipCount:       n.leadershipCount,
				Latency:               10.0,
				UserCPU:               userCPU,
				SystemCPU:             systemCPU,
				IdleCPU:               idleCPU,
				MemoryAvailable:       allocMB,
				MemoryAllocationTotal: totalAllocMB,
				MemorySystem:          sysMB,
				AvgReadMBs:            avgReadMBs,
				AvgWriteMBs:           avgWriteMBs,
				GeographyScore:        0.5,
			}

			UpsertReputation(GlobalReputationDB.reputationDB, reputation)
			n.publishMessage(TypeReputation, reputation)
			log.Printf("[REPUTATION] Published (score: %.2f)", calculateReputation(reputation))

		case <-n.ctx.Done():
			return
		}
	}
}

// IMPROVED RunFullNode with better mesh waiting
func RunFullNode(ctx context.Context, host host.Host, pubsubObj *pubsub.PubSub, discovery *app.KnowledgeBaseDB) {
	// Get pre-created topic and subscription from discovery
	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	// Check if already created in main.go
	if discovery.ElectionTopic != nil && discovery.ElectionSub != nil {
		electionTopic = discovery.ElectionTopic
		electionSub = discovery.ElectionSub
		log.Println("[ELECTION] Using pre-created topic and subscription")
	} else {
		// Fallback: create new ones
		log.Println("[ELECTION] Creating new topic and subscription")
		var err error
		electionTopic, err = pubsubObj.Join("optimusdb")
		if err != nil {
			log.Fatalf("[FATAL] Cannot join election topic: %v", err)
		}

		electionSub, err = electionTopic.Subscribe()
		if err != nil {
			log.Fatalf("[FATAL] Cannot subscribe to election topic: %v", err)
		}
	}

	// Create node with topics already set
	node := NewNode(ctx, host, pubsubObj, discovery)
	node.electionTopic = electionTopic
	node.electionSub = electionSub

	defer GlobalReputationDB.reputationDB.Close()

	log.Println("[INIT] Starting OptimusDB Election Node as FOLLOWER")
	node.role = "Follower" // Ensure all start as followers

	// Start listener IMMEDIATELY
	go node.ListenForElectionEvents()
	log.Println("[ELECTION] ✅ Message listener started")

	// Start background services
	go node.PeriodicReputationPublisher()
	go node.CheckLeaderFailure()
	go node.LogRoleStatus() // For printing the Coordinator / Follower

	// Wait for mesh to form with better logging
	log.Println("[ELECTION] Waiting for mesh formation...")
	meshCheckInterval := 2 * time.Second
	maxMeshWait := 30 * time.Second
	meshStart := time.Now()

	for time.Since(meshStart) < maxMeshWait {
		discovered := discovery.GetDiscoveredPeers()
		meshPeers := electionTopic.ListPeers()
		allTopics := pubsubObj.GetTopics()

		log.Printf("[ELECTION-MESH] Status check:")
		log.Printf("  - Discovered peers: %d", len(discovered))
		log.Printf("  - Mesh peers on 'optimusdb': %d", len(meshPeers))
		log.Printf("  - Subscribed topics: %v", allTopics)

		// Debug: List mesh peers
		if len(meshPeers) > 0 {
			log.Printf("  - Mesh peer IDs:")
			for i, p := range meshPeers {
				shortID := p.String()
				if len(shortID) > 8 {
					shortID = shortID[:8] + "..."
				}
				log.Printf("    [%d] %s", i+1, shortID)
			}
		}

		// Wait for at least 1 mesh peer (not just discovered)
		if len(meshPeers) >= 1 {
			log.Printf("[ELECTION] ✅ Mesh formed with %d peers!", len(meshPeers))
			break
		}

		if len(discovered) > 0 && len(meshPeers) == 0 {
			log.Println("[ELECTION] ⚠️ Peers discovered but mesh not formed, waiting...")

			// Try to force mesh formation by publishing a test message
			testMsg := map[string]string{
				"type": "mesh_test",
				"from": host.ID().String(),
				"time": time.Now().Format(time.RFC3339),
			}
			testData, _ := json.Marshal(testMsg)
			if err := electionTopic.Publish(ctx, testData); err != nil {
				log.Printf("[ELECTION] Test publish failed: %v", err)
			} else {
				log.Println("[ELECTION] Sent test message to stimulate mesh")
			}
		}

		time.Sleep(meshCheckInterval)
	}

	// Give mesh time to stabilize
	log.Println("[ELECTION] Allowing 5s for mesh stabilization...")
	time.Sleep(5 * time.Second)

	// Final mesh check
	finalMeshPeers := electionTopic.ListPeers()
	log.Printf("[ELECTION] Final mesh status: %d peers in mesh", len(finalMeshPeers))

	// Initialize reputation for self
	selfRep := NodeReputation{
		NodeID:         node.host.ID().String(),
		Uptime:         1.0,
		GeographyScore: 0.5,
	}
	UpsertReputation(GlobalReputationDB.reputationDB, selfRep)

	// Query all reputations
	peers, err := QueryAllReputations(GlobalReputationDB.reputationDB)
	if err != nil || len(peers) == 0 {
		peers = []NodeReputation{selfRep}
	}

	// Wait a bit before starting election
	log.Println("[ELECTION] Waiting 10s before first election...")
	time.Sleep(10 * time.Second)

	log.Printf("[ELECTION] Starting first election with %d candidates", len(peers))
	go node.StartElection(peers, 0)

	// Keep running
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Println("[SHUTDOWN] Election controller exiting")
}

func NewNode(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	return &Node{
		ctx:                        ctx,
		host:                       host,
		pubsub:                     pubsub,
		discovery:                  discovery,
		topicManager:               NewTopicManager(pubsub),
		role:                       "Follower",
		votes:                      make(map[string]int),
		votedNodes:                 make(map[string]string),
		votedForInTerm:             make(map[int]string),
		announcedLeaderForElection: make(map[string]string),
		electionPhase:              PhaseIdle,
		currentTerm:                0,
	}
}

// Database functions
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
	_, err := r.reputationDB.Exec(query, args...)
	return err
}

// Add this function to periodically log role status
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

			if role == "Coordinator" {
				log.Printf("[STATUS] 👑 I AM THE COORDINATOR (term %d)", term)
			} else {
				leaderShort := leader.String()
				if len(leaderShort) > 8 {
					leaderShort = leaderShort[:8] + "..."
				}
				log.Printf("[STATUS] 📋 FOLLOWER following %s (term %d)", leaderShort, term)
			}

		case <-n.ctx.Done():
			return
		}
	}
}
