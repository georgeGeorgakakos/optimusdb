package election

import (
	//orbitdb "berty.tech/go-orbit-db"
	//orbitdb "berty.tech/go-orbit-db"
	//orbitdbIface "berty.tech/go-orbit-db/iface"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"log"
	"math"
	"math/rand"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/utilities"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

var GlobalReputationDB *ReputationSQLite

type ReputationSQLite struct {
	reputationDB *sql.DB
	mu           sync.Mutex
}

var (
	electionTopicInstance *pubsub.Topic
	electionJoinOnce      sync.Once
	electionJoinErr       error
)

type TopicManager struct {
	pubsub *pubsub.PubSub
	topics map[string]*pubsub.Topic
	mu     sync.Mutex
}

func NewTopicManager(ps *pubsub.PubSub) *TopicManager {
	return &TopicManager{
		pubsub: ps,
		topics: make(map[string]*pubsub.Topic),
	}
}

func (tm *TopicManager) GetTopic(name string) (*pubsub.Topic, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if topic, ok := tm.topics[name]; ok {
		log.Printf("[DEBUG] Reusing topic: %s", name)
		return topic, nil
	}

	log.Printf("[DEBUG] Joining topic for first time: %s", name)
	topic, err := tm.pubsub.Join(name)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic '%s': %w", name, err)
	}
	tm.topics[name] = topic
	return topic, nil
}

// leader_election: used for exchanging votes, not for reputation data.
// leader_announcement: used after election for leader identity.
// heartbeatTopic and roleTopic: used for coordination role & health check.
// reputationSyncTopic   allows your agents to: Publish their updated reputation info or Receive reputation updates from other peer
const (
	//electionTopic          = "leader_election"
	//leaderAnnounceTopic    = "leader_announcement"
	//heartbeatTopic         = "leader_heartbeat"
	//roleTopic              = "node_role"
	electionTopic = "optimusdb"

	// Message types
	TypeVote               = "vote"
	TypeHeartbeat          = "heartbeat"
	TypeRole               = "role"
	TypeAnnouncement       = "announcement"
	TypeReputation         = "reputation"
	TypeElectionResult     = "election_result"
	heartbeatInterval      = 5 * time.Second  // How often leader sends heartbeat
	heartbeatTimeout       = 10 * time.Second // Timeout before a leader is considered dead
	electionTimeout        = 5 * time.Second  // Timeout window to collect votes
	peerDiscoveryThreshold = 1                // Minimum peer count required to trigger election
	reElectionBackoff      = 15 * time.Second // Backoff to reduce re-election pressure
	heartbeatRetryLimit    = 3                // Retry count before assuming failure // Wait for at least 2 discovered peers before starting election, 1 depicts the additional apart the running
)

// Helper:s
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
func (n *Node) getElectionTopic() (*pubsub.Topic, error) {
	return n.topicManager.GetTopic(electionTopic)
}

type CoreMessage struct {
	Type    string          `json:"type"`    // e.g. "vote", "heartbeat", "role", etc.
	Payload json.RawMessage `json:"payload"` // contains VoteMessage, RoleMessage, etc.
}

type ElectionResultMessage struct {
	LeaderID string         `json:"leader"`
	Votes    map[string]int `json:"votes"` // candidateID -> number of votes
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

// PubSub message types
type VoteMessage struct {
	NodeID string `json:"nodeId"`
	Vote   string `json:"vote"`
}

type HeartbeatMessage struct {
	LeaderID string `json:"leaderId"`
	Time     int64  `json:"time"`
}

type RoleMessage struct {
	NodeID string `json:"nodeId"`
	Role   string `json:"role"`
}

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
	reputationTopic *pubsub.Topic
	electionTopic   *pubsub.Topic
	leadershipCount int
	votes           map[string]int
	electionMutex   sync.Mutex
	isElecting      bool
	lastElection    time.Time
}

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
	// Create the KnowledgeBaseSQLite instance
	GlobalReputationDB = &ReputationSQLite{reputationDB: db}

	// Create tables
	err = GlobalReputationDB.createReputationDB()
	if err != nil {
		log.Fatalf("[ERROR] Table creation failed for Reputation DB: %v", err)
		app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Table creation failed for Reputation DB: %v", err), runtime.GOOS)
		return nil, err
	}

	log.Println("[INFO] SQLite Reputation Database Ready at:", rdbmsCache)
	app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite Reputation Database Ready at: %v", rdbmsCache), runtime.GOOS)
	return GlobalReputationDB, nil
}

// createTables ensures the `datacatalog` table exists
func (rep *ReputationSQLite) createReputationDB() error {
	tableQuery :=
		`CREATE TABLE IF NOT EXISTS reputation (
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
	_, err := rep.reputationDB.Exec(tableQuery)
	if err != nil {
		return err
	}
	electionLogQuery := `CREATE TABLE IF NOT EXISTS election_log (
				id TEXT PRIMARY KEY,
				timestamp TEXT,
				leader_id TEXT,
				votes_json TEXT
	);`

	_, err2 := rep.reputationDB.Exec(electionLogQuery)
	if err2 != nil {
		return fmt.Errorf("failed to create election_log table: %w", err2)
	}

	app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `datacatalog` created or already exists."), runtime.GOOS)
	return nil
}

func InsertElectionLog(db *sql.DB, id string, timestamp time.Time, leaderID string, votes map[string]int) error {
	votesJSON, err := json.Marshal(votes)
	if err != nil {
		return fmt.Errorf("failed to marshal votes: %w", err)
	}

	query := `
	INSERT INTO election_log (id, timestamp, leader_id, votes_json)
	VALUES (?, ?, ?, ?);`

	_, err = db.Exec(query, id, timestamp.Format(time.RFC3339), leaderID, string(votesJSON))
	return err
}

func calculateReputation(nr NodeReputation) float64 {
	w := getReputationWeights()
	cpuLoad := nr.UserCPU + nr.SystemCPU
	cpuScore := 100 - cpuLoad
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
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
	if err := n.ensureElectionTopic(); err != nil {
		log.Printf("[ERROR] [StartElection] Could not get election topic: %v", err)
		return
	}
	log.Printf("[INFO] Joined or reused topic '%s'", electionTopic)

	selected := weightedRandomSelection(peers)
	vote := VoteMessage{
		NodeID: n.host.ID().String(),
		Vote:   selected,
	}

	if err := n.publishMessage(TypeVote, vote); err != nil {
		log.Printf("[ERROR] Failed to publish vote message: %v", err)
		return
	}
	log.Printf("[ELECTION] [Attempt %d] Node %s voted for %s", attempt+1, vote.NodeID, vote.Vote)

	go func() {
		time.Sleep(electionTimeout)

		n.electionMutex.Lock()
		defer n.electionMutex.Unlock()

		winner := evaluateVotes(n.votes)
		if winner == "" {
			log.Printf("[ELECTION] [Attempt %d] No winner. Retrying election...", attempt+1)

			if attempt < *config.ElectionMaxRetries {
				backoff := time.Duration(math.Pow(2, float64(attempt))) * (*config.ElectionRetryDelay)
				time.Sleep(backoff)

				peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
				n.StartElection(peers, attempt+1)
			} else {
				log.Printf("[ELECTION] [Attempt %d] Reached max retries. Fallback to highest reputation.", attempt+1)
				n.fallbackElection()
			}
			return
		}

		log.Printf("[ELECTION] [Attempt %d] Winner: %s", attempt+1, winner)

		electionID := fmt.Sprintf("election-%d", time.Now().UnixNano())
		timestamp := time.Now()

		result := ElectionResultMessage{
			LeaderID: winner,
			Votes:    n.votes,
		}

		if err := n.publishMessage(TypeElectionResult, result); err != nil {
			log.Printf("[ERROR] Failed to broadcast election result: %v", err)
		}

		if err := InsertElectionLog(GlobalReputationDB.reputationDB, electionID, timestamp, winner, n.votes); err != nil {
			log.Printf("[ERROR] Failed to persist election log: %v", err)
			app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to store election log: %v", err), runtime.GOOS)
		} else {
			app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Election result stored: ID=%s, Leader=%s", electionID, winner), runtime.GOOS)
		}

		n.announceLeader(winner)
		n.votes = make(map[string]int)
	}()
}

func mustMarshal(v interface{}) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("[FATAL] Failed to marshal payload: %v", err)
	}
	return data
}

func weightedRandomSelection(peers []NodeReputation) string {
	total := 0.0
	for _, p := range peers {
		total += calculateReputation(p)
	}
	randVal := rand.Float64() * total
	cumulative := 0.0
	for _, p := range peers {
		score := calculateReputation(p)
		cumulative += score
		if cumulative >= randVal {
			return p.NodeID
		}
	}
	return peers[len(peers)-1].NodeID
}

// Re-election logic with backoff and retry after missed heartbeats
func (n *Node) CheckLeaderFailure() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		n.mutex.Lock()
		if n.lastHeartbeat.IsZero() {
			n.mutex.Unlock()
			continue
		}

		if time.Since(n.lastHeartbeat) > heartbeatTimeout {
			n.heartbeatMissed++
			log.Printf("[WARN] Missed heartbeat %d time(s)", n.heartbeatMissed)

			if n.heartbeatMissed >= heartbeatRetryLimit {
				log.Println("[FAILURE] Leader confirmed dead. Starting re-election.")

				n.electionMutex.Lock()
				if n.isElecting {
					n.electionMutex.Unlock()
					n.mutex.Unlock()
					continue // already electing
				}
				if time.Since(n.lastElection) < reElectionBackoff {
					log.Println("[BACKOFF] Re-election skipped (backoff active)")
					n.electionMutex.Unlock()
					n.mutex.Unlock()
					continue
				}
				n.isElecting = true
				n.lastElection = time.Now()
				n.electionMutex.Unlock()

				// Launch election async
				go func() {
					peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
					if len(peers) > 0 {
						n.StartElection(peers, 0)

						// Persist election info
						log.Printf("[SQLITE] Election triggered by %s at %s", n.host.ID().String(), time.Now().Format(time.RFC3339))
						msg := fmt.Sprintf("Triggered re-election at %s by %s", time.Now().Format(time.RFC3339), n.host.ID().String())
						app.GlobalLoggerDB.AddToOptimusLog("ELECTION", msg, runtime.GOOS)

						// Notify peers via pubsub
						notify := map[string]string{"event": "re_election", "initiator": n.host.ID().String(), "timestamp": time.Now().Format(time.RFC3339)}
						bytes, _ := json.Marshal(notify)
						n.reputationTopic.Publish(n.ctx, bytes)
					}
					n.electionMutex.Lock()
					n.isElecting = false
					n.electionMutex.Unlock()
				}()

				n.heartbeatMissed = 0
			}
		} else {
			n.heartbeatMissed = 0 // Reset on successful heartbeat
		}
		n.mutex.Unlock()
	}
}

func evaluateVotes(votes map[string]int) string {
	var leader string
	maxVotes := 0
	for node, count := range votes {
		if count > maxVotes {
			maxVotes = count
			leader = node
		}
	}
	return leader
}

// In announceLeader, add an explicit COORDINATOR log
func (n *Node) announceLeader(leaderID string) {
	// Ensure election topic is initialized
	if err := n.ensureElectionTopic(); err != nil {
		log.Printf("[ERROR] Could not get election topic: %v", err)
		return
	}

	//announcement := fmt.Sprintf(`{"leader": "%s"}`, leaderID)
	//err := n.electionTopic.Publish(n.ctx, []byte(announcement))
	coreAnn := CoreMessage{
		Type:    TypeAnnouncement,
		Payload: mustMarshal(map[string]string{"leader": leaderID}),
	}
	msgData, _ := json.Marshal(coreAnn)
	err := n.electionTopic.Publish(n.ctx, msgData)

	if err != nil {
		log.Println("[ERROR] Failed to announce leader:", err)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to announce leader: %v", err), runtime.GOOS)
		return
	}
	log.Println("[LEADER] Announcing new leader:", leaderID)
	app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Announcing new leader: %v", leaderID), runtime.GOOS)

	leaderReputation, _ := GetReputationByID(GlobalReputationDB.reputationDB, leaderID)
	leaderReputation.LeadershipCount++
	err = UpsertReputation(GlobalReputationDB.reputationDB, leaderReputation)
	if err != nil {
		log.Printf("[ERROR] Failed to update leader reputation: %v", err)
	}

	if leaderID == n.host.ID().String() {
		n.role = "Coordinator"
		n.leadershipCount++
		n.HandleLeaderAnnouncement(leaderID)
		log.Printf("[COORDINATOR] Node %s is now acting as leader", leaderID)
	} else {
		n.role = "Follower"
	}

	log.Printf("[ROLE] Node %s is now %s", n.host.ID().String(), n.role)
	app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Node %s is now %s", n.host.ID().String(), n.role), runtime.GOOS)

	n.publishRole()
	go n.startHeartbeat(leaderID)
}

func (n *Node) publishRole() {
	roleMsg := RoleMessage{
		NodeID: n.host.ID().String(),
		Role:   n.role,
	}

	if err := n.publishMessage(TypeRole, roleMsg); err != nil {
		log.Printf("[ERROR] Failed to publish role message: %v", err)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to publish role message: %v", err), runtime.GOOS)
		return
	}

	log.Printf("[ROLE] Published role: %s", roleMsg.Role)
	app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Published role: Node=%s, Role=%s", roleMsg.NodeID, roleMsg.Role), runtime.GOOS)
}

func (n *Node) fallbackElection() {
	peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
	if len(peers) == 0 {
		log.Println("[FALLBACK] No peers found in DB.")
		return
	}

	highestScore := float64(-1)
	var selected NodeReputation
	for _, peer := range peers {
		score := calculateReputation(peer)
		if score > highestScore {
			highestScore = score
			selected = peer
		}
	}

	log.Printf("[FALLBACK] Selected %s with highest reputation score %.2f\n", selected.NodeID, highestScore)
	n.leader = peer.ID(selected.NodeID)
	n.announceLeader(selected.NodeID)
}

func (n *Node) startHeartbeat(leaderID string) {
	// Ensure the election topic is properly initialized
	if err := n.ensureElectionTopic(); err != nil {
		log.Printf("[ERROR] Cannot start heartbeat, failed to join topic %s: %v", electionTopic, err)
		return
	}

	topic := n.electionTopic
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	log.Printf("[HEARTBEAT] Starting heartbeat loop as leader: %s", leaderID)

	for {
		select {
		case <-ticker.C:
			if n.host.ID().String() != leaderID {
				log.Printf("[HEARTBEAT] Node %s is no longer leader. Stopping heartbeat.", n.host.ID())
				return
			}

			heartbeat := HeartbeatMessage{
				LeaderID: leaderID,
				Time:     time.Now().Unix(),
			}

			data, err := json.Marshal(heartbeat)
			if err != nil {
				log.Printf("[ERROR] Failed to marshal heartbeat message: %v", err)
				continue
			}

			if err := topic.Publish(n.ctx, data); err != nil {
				log.Printf("[ERROR] Failed to publish heartbeat: %v", err)
				continue
			}

			log.Printf("[HEARTBEAT] Sent heartbeat from leader: %s", leaderID)
		case <-n.ctx.Done():
			log.Printf("[HEARTBEAT] Context canceled, stopping heartbeat.")
			return
		}
	}
}

func (n *Node) ListenForHeartbeats() {
	topic, err := n.getElectionTopic()
	if err != nil {
		log.Printf("...error...")
		return
	}
	sub, err := topic.Subscribe()

	for {
		msg, _ := sub.Next(n.ctx)
		var hb HeartbeatMessage
		err := json.Unmarshal(msg.Data, &hb)
		if err == nil {
			n.mutex.Lock()
			n.lastHeartbeat = time.Now()
			n.mutex.Unlock()
			log.Println("[HEARTBEAT] Received heartbeat from:", hb.LeaderID)
		}
	}
}

func RunFullNode(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, discovery *app.KnowledgeBaseDB) {

	node := NewNode(ctx, host, pubsub, discovery)
	defer GlobalReputationDB.reputationDB.Close()

	// Start background services
	go node.PeriodicReputationPublisher()
	go node.ListenForReputationUpdates()
	go node.ListenForRoleUpdates()
	go node.CheckLeaderFailure()
	go node.ListenForElectionEvents()

	for {
		discovered := discovery.GetDiscoveredPeers()
		log.Printf("Discovered %d peers", len(discovered))
		if len(discovered) >= peerDiscoveryThreshold {
			break
		}
		time.Sleep(5 * time.Second)
	}

	for {
		peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
		if len(peers) >= peerDiscoveryThreshold {
			node.StartElection(peers, 0)
			break
		}
		log.Printf("[ELECTION] Waiting for reputation data... currently: %d", len(peers))
		time.Sleep(5 * time.Second)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	log.Println("[SHUTDOWN] Node exiting")
}

func (n *Node) PeriodicReputationPublisher() {
	ticker := time.NewTicker(30 * time.Second) // Adjust interval as you wish
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Collect metrics
			userCPU, systemCPU, idleCPU, _ := utilities.GetCPUUsage()
			allocMB, totalAllocMB, sysMB := utilities.GetMemoryUsage()
			avgReadMBs, avgWriteMBs, _ := utilities.GetDiskUsage(5)
			avgRxKBs, avgTxKBs, _ := utilities.GetNetworkUsage()
			uptime := float64(time.Now().Unix()%1000) / 1000 // Stub uptime; replace with real uptime if you have
			//leadershipCount := 0                             // This will be updated during elections
			latency := (avgRxKBs + avgTxKBs) / 1024 // Simplified "latency" based on network activity
			memory := allocMB                       // Use actual memory used/free if you want
			geography := 0.5                        // Stub, replace with real geographic score later

			reputation := NodeReputation{
				NodeID: n.host.ID().String(),
				Uptime: uptime,
				//LeadershipCount:       leadershipCount,
				LeadershipCount:       n.leadershipCount,
				Latency:               latency,
				UserCPU:               userCPU,
				SystemCPU:             systemCPU,
				IdleCPU:               idleCPU,
				MemoryAvailable:       memory,
				AvgReadMBs:            avgReadMBs,
				AvgWriteMBs:           avgWriteMBs,
				MemoryAllocationTotal: totalAllocMB,
				MemorySystem:          sysMB,
				GeographyScore:        geography,
			}

			//data, _ := json.Marshal(reputation)
			//n.dbDocStore.Put(n.ctx, map[string]interface{}{"_id": reputation.NodeID, "data": string(data)})
			err := UpsertReputation(GlobalReputationDB.reputationDB, reputation)
			if err != nil {
				log.Printf("[ERROR] Failed to upsert reputation: %v", err)
			}

			log.Printf("[REPUTATION] Stored updated reputation: %+v", reputation)

			// Use unified publish method so that other nodes can parse the message properly
			err = n.publishMessage(TypeReputation, reputation)
			if err != nil {
				log.Printf("[ERROR] Failed to publish reputation via CoreMessage: %v", err)
			} else {
				log.Printf("[REPUTATION] Published reputation update to peers")
				log.Printf("[REPUTATION] Published updated reputation: %+v", reputation)
			}

			app.GlobalLoggerDB.AddToOptimusLog("REPUTATION", fmt.Sprintf("Published: Node=%s, Score=%.2f", reputation.NodeID, calculateReputation(reputation)), runtime.GOOS)

		case <-n.ctx.Done():
			return
		}
	}
}

func NewNode(ctx context.Context, host host.Host, pubsub *pubsub.PubSub, discovery *app.KnowledgeBaseDB) *Node {
	topicManager := NewTopicManager(pubsub)

	reputationTopic, err := topicManager.GetTopic("reputation_sync")
	if err != nil {
		log.Fatalf("[FATAL] Failed to get topic for reputation_sync: %v", err)
	}

	electionTopic, err := topicManager.GetTopic(electionTopic) // "optimusdb"
	if err != nil {
		log.Fatalf("[FATAL] Failed to get topic for election: %v", err)
	}

	return &Node{
		ctx:             ctx,
		host:            host,
		pubsub:          pubsub,
		discovery:       discovery,
		reputationTopic: reputationTopic,
		topicManager:    topicManager,
		electionTopic:   electionTopic,
		role:            "Follower",
		votes:           make(map[string]int),
	}
}

func (n *Node) ListenForReputationUpdates() {
	topic, err := n.topicManager.GetTopic("reputation_sync")
	if err != nil {
		log.Fatalf("[ERROR] Failed to get topic for reputation_sync: %v", err)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to get topic for reputation_sync: %v", err), runtime.GOOS)
	}

	sub, err := topic.Subscribe()
	if err != nil {
		log.Fatalf("[ERROR] Failed to subscribe to reputation_sync: %v", err)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to subscribe to reputation_sync: %v", err), runtime.GOOS)
	}

	log.Println("[REPUTATION] Subscribed to reputation updates topic")

	for {
		msg, err := sub.Next(n.ctx)
		if err != nil {
			log.Printf("[ERROR] Failed to receive pubsub message: %v", err)
			app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to receive pubsub message: %v", err), runtime.GOOS)
			continue
		}

		var reputation NodeReputation
		err = json.Unmarshal(msg.Data, &reputation)
		if err != nil {
			log.Printf("[ERROR] Failed to unmarshal reputation data: %v", err)
			app.GlobalLoggerDB.AddToOptimusLog("ELECTION-ERROR", fmt.Sprintf("Failed to unmarshal reputation data: %v", err), runtime.GOOS)
			continue
		}

		if reputation.NodeID == n.host.ID().String() {
			continue
		}

		score := calculateReputation(reputation)
		log.Printf("[REPUTATION] Received update from %s | Score: %.2f", reputation.NodeID, score)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Received update from %s | Score: %.2f", reputation.NodeID, score), runtime.GOOS)

		err = UpsertReputation(GlobalReputationDB.reputationDB, reputation)
		if err != nil {
			log.Printf("[ERROR] Failed to upsert reputation from pubsub: %v", err)
		} else {
			log.Printf("[REPUTATION] Reputation from %s stored in SQLite", reputation.NodeID)
		}
		app.GlobalLoggerDB.AddToOptimusLog("REPUTATION", fmt.Sprintf("Received from %s | Score=%.2f | %+v", reputation.NodeID, score, reputation), runtime.GOOS)
	}
}

func (n *Node) ListenForRoleUpdates() {
	topic, err := n.getElectionTopic()
	if err != nil {
		log.Printf("...error...")
		return
	}
	sub, err := topic.Subscribe()

	if err != nil {
		log.Printf("[ERROR] Failed to subscribe to role topic: %v", err)
		return
	}
	log.Println("[ROLE] Subscribed to role updates topic")

	for {
		msg, err := sub.Next(n.ctx)
		if err != nil {
			log.Printf("[ERROR] Failed to receive role update: %v", err)
			continue
		}

		var roleMsg RoleMessage
		if err := json.Unmarshal(msg.Data, &roleMsg); err != nil {
			log.Printf("[ERROR] Failed to parse role message: %v", err)
			continue
		}

		//log.Printf("[ROLE] Received update: Node %s is now %s", roleMsg.NodeID, roleMsg.Role)

		// Optional: Log to global logger
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Node %s is now %s", roleMsg.NodeID, roleMsg.Role), runtime.GOOS)
	}
}
func (n *Node) HandleLeaderAnnouncement(leaderID string) {
	nodeID := n.host.ID().String()
	var role string

	if leaderID == nodeID {
		log.Printf("[LEADER] I (%s) am elected leader. Starting heartbeat...", nodeID)
		role = "Coordinator"
		go n.startHeartbeat(leaderID)
	} else {
		role = "Follower"
		log.Printf("[LEADER] I (%s) acknowledge %s as the new leader. Becoming follower.", nodeID, leaderID)
	}

	n.role = role
	n.leader = peer.ID(leaderID)
	n.publishRole()

	app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Node %s is now %s", nodeID, role), runtime.GOOS)
}

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

	return GlobalReputationDB.SafeExec(query,
		rep.NodeID, rep.Uptime, rep.LeadershipCount, rep.Latency,
		rep.UserCPU, rep.SystemCPU, rep.IdleCPU,
		rep.MemoryAvailable, rep.MemoryAllocationTotal, rep.MemorySystem,
		rep.AvgReadMBs, rep.AvgWriteMBs, rep.GeographyScore)
}

func GetReputationByID(db *sql.DB, nodeID string) (NodeReputation, error) {
	query := `SELECT * FROM reputation WHERE node_id = ?`
	row := db.QueryRow(query, nodeID)

	var rep NodeReputation
	err := row.Scan(
		&rep.NodeID, &rep.Uptime, &rep.LeadershipCount, &rep.Latency,
		&rep.UserCPU, &rep.SystemCPU, &rep.IdleCPU,
		&rep.MemoryAvailable, &rep.MemoryAllocationTotal, &rep.MemorySystem,
		&rep.AvgReadMBs, &rep.AvgWriteMBs, &rep.GeographyScore,
	)
	if err != nil {
		return NodeReputation{}, err
	}
	return rep, nil
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

// publishMessage sends a typed CoreMessage to the electionTopic.
// It ensures the topic is initialized only once.
func (n *Node) publishMessage(msgType string, payload interface{}) error {
	if strings.TrimSpace(msgType) == "" {
		errMsg := "Cannot publish CoreMessage: empty type"
		app.GlobalLoggerDB.AddToOptimusLog("PUBSUB-ERROR", errMsg, runtime.GOOS)
		return fmt.Errorf(errMsg)
	}

	if payload == nil {
		errMsg := fmt.Sprintf("Cannot publish CoreMessage of type '%s': payload is nil", msgType)
		app.GlobalLoggerDB.AddToOptimusLog("PUBSUB-ERROR", errMsg, runtime.GOOS)
		return fmt.Errorf(errMsg)
	}

	// Ensure topic is initialized
	if err := n.ensureElectionTopic(); err != nil {
		errMsg := fmt.Sprintf("Failed to get election topic '%s': %v", electionTopic, err)
		app.GlobalLoggerDB.AddToOptimusLog("PUBSUB-ERROR", errMsg, runtime.GOOS)
		return fmt.Errorf(errMsg)
	}

	payloadData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("Failed to marshal payload for type '%s': %v", msgType, err)
	}

	coreMsg := CoreMessage{
		Type:    msgType,
		Payload: payloadData,
	}

	msgData, err := json.Marshal(coreMsg)
	if err != nil {
		return fmt.Errorf("Failed to marshal CoreMessage: %v", err)
	}

	if err := n.electionTopic.Publish(n.ctx, msgData); err != nil {
		return fmt.Errorf("Failed to publish message of type '%s': %v", msgType, err)
	}

	log.Printf("[PUBSUB] Published message type '%s'", msgType)
	return nil
}

// CoreMessage handler type for routing
func (n *Node) unifiedListener(handler func(CoreMessage)) {
	sub, err := n.pubsub.Subscribe(electionTopic, pubsub.WithBufferSize(64))
	if err != nil {
		log.Fatalf("[ERROR] Failed to subscribe to electionTopic: %v", err)
	}
	for {
		msg, err := sub.Next(n.ctx)
		if err != nil {
			log.Printf("[ERROR] Failed to receive pubsub message: %v", err)
			continue
		}
		var core CoreMessage
		if err := json.Unmarshal(msg.Data, &core); err != nil {
			log.Printf("[ERROR] Failed to unmarshal CoreMessage: %v", err)
			continue
		}
		handler(core)
	}
}

// Unified dispatcher for handling all CoreMessage types
func (n *Node) handleElectionMessage(core CoreMessage) {

	if strings.TrimSpace(core.Type) == "" {
		//log.Printf("[WARN] Ignored CoreMessage with empty type | Raw payload: %s", string(core.Payload))
		return
	}
	if len(core.Payload) == 0 {
		//log.Printf("[WARN] Ignored CoreMessage with empty payload | Type: %s", core.Type)
		return
	}

	switch core.Type {
	case TypeVote:
		var vote VoteMessage
		if err := json.Unmarshal(core.Payload, &vote); err != nil {
			log.Printf("[ERROR] Failed to unmarshal VoteMessage: %v", err)
			return
		}
		// Prevent duplicate votes
		if _, voted := n.votes[vote.NodeID]; !voted {
			n.votes[vote.NodeID] = 0
		}
		n.votes[vote.Vote]++

	case TypeHeartbeat:
		var hb HeartbeatMessage
		if err := json.Unmarshal(core.Payload, &hb); err == nil {
			n.mutex.Lock()
			n.lastHeartbeat = time.Now()
			n.mutex.Unlock()
			//log.Println("[HEARTBEAT] Received heartbeat from:", hb.LeaderID)
		}

	case TypeReputation:
		var rep NodeReputation
		if err := json.Unmarshal(core.Payload, &rep); err == nil {
			if rep.NodeID == n.host.ID().String() {
				return
			}
			score := calculateReputation(rep)
			log.Printf("[REPUTATION] Received update from %s | Score: %.2f", rep.NodeID, score)
			err := UpsertReputation(GlobalReputationDB.reputationDB, rep)
			if err != nil {
				log.Printf("[ERROR] Failed to upsert reputation from pubsub: %v", err)
			}
		}

	case TypeRole:
		var roleMsg RoleMessage
		if err := json.Unmarshal(core.Payload, &roleMsg); err != nil {
			log.Printf("[ERROR] Failed to parse role message: %v", err)
			return
		}
		if roleMsg.NodeID == "" || roleMsg.Role == "" {
			log.Printf("[WARN] Ignored invalid role message: %+v", roleMsg)
			return
		}
		//log.Printf("[ROLE] Received update: Node %s is now %s", roleMsg.NodeID, roleMsg.Role)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Node %s is now %s", roleMsg.NodeID, roleMsg.Role), runtime.GOOS)

	case TypeAnnouncement:
		var ann map[string]string
		if err := json.Unmarshal(core.Payload, &ann); err == nil {
			leaderID := ann["leader"]
			if leaderID != "" {
				n.HandleLeaderAnnouncement(leaderID)
			}
		}
	case TypeElectionResult:
		var result ElectionResultMessage
		if err := json.Unmarshal(core.Payload, &result); err != nil {
			log.Printf("[ERROR] Failed to unmarshal ElectionResultMessage: %v", err)
			return
		}
		log.Printf("[ELECTION] Received election result: Leader: %s, Votes: %+v", result.LeaderID, result.Votes)
		app.GlobalLoggerDB.AddToOptimusLog("ELECTION", fmt.Sprintf("Election result: Leader: %s, Votes: %+v", result.LeaderID, result.Votes), runtime.GOOS)

	default:
		//log.Printf("[WARN] Unknown CoreMessage type: %s", core.Type)
		log.Printf("[WARN] Unknown CoreMessage type: %s | Raw payload: %s", core.Type, string(core.Payload))
	}
}

// Refactored listener using unified dispatcher
func (n *Node) ListenForElectionEvents() {
	go n.unifiedListener(n.handleElectionMessage)
}

func QueryElectionLog(db *sql.DB) ([]ElectionResultMessage, error) {
	rows, err := db.Query(`SELECT id, timestamp, leader_id, votes_json FROM election_log ORDER BY timestamp DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ElectionResultMessage
	for rows.Next() {
		var id, ts, leaderID, votesJSON string
		if err := rows.Scan(&id, &ts, &leaderID, &votesJSON); err != nil {
			return nil, err
		}

		var votes map[string]int
		if err := json.Unmarshal([]byte(votesJSON), &votes); err != nil {
			return nil, err
		}

		results = append(results, ElectionResultMessage{
			LeaderID: leaderID,
			Votes:    votes,
		})
	}
	return results, nil
}

func (n *Node) ensureElectionTopic() error {
	if n.electionTopic == nil {
		return fmt.Errorf("electionTopic was not initialized")
	}
	return nil
}

func (r *ReputationSQLite) SafeExec(query string, args ...interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, err := r.reputationDB.Exec(query, args...)
	return err
}
