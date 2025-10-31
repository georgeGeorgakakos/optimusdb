# OptimusDB Election Fix Proposal

## Overview

This document proposes **safe, incremental fixes** to prevent multiple coordinators. Solutions are ranked by **safety** (risk of breaking existing functionality) and **effectiveness** (how well they prevent split-brain).

---

## Recommended Approach: Layered Defense Strategy

Instead of one big change, implement **multiple layers of protection**:

1. **Layer 1**: Detection (catch problems when they happen)
2. **Layer 2**: Validation (prevent bad state from being accepted)
3. **Layer 3**: Coordination (prevent divergence in the first place)

**Philosophy**: "Defense in depth" - multiple overlapping protections.

---

## Layer 1: Split-Brain Detection and Recovery

### 🟢 Priority 1A: Heartbeat Conflict Detection (IMMEDIATE)

**Risk**: 🟢 Low (only adds detection, doesn't change behavior)
**Effectiveness**: 🟡 Medium (detects and recovers from split-brain)
**Implementation Time**: 30 minutes

**What It Does**:
- Detects when two coordinators exist
- Resolves conflict using deterministic rule (lower peer ID wins)
- Provides immediate visibility into the problem

**Changes**:

```go
// In handleElectionMessage(), case TypeHeartbeat
case TypeHeartbeat:
    var hb HeartbeatMessage
    if err := json.Unmarshal(core.Payload, &hb); err == nil {
        n.mutex.Lock()

        // SPLIT-BRAIN DETECTION
        if n.role == "Coordinator" && hb.LeaderID != n.host.ID().String() {
            log.Printf("[SPLIT-BRAIN] Multiple coordinators detected! Me=%s, Other=%s",
                       n.host.ID().String(), hb.LeaderID)
            app.GlobalLoggerDB.AddToOptimusLog("CRITICAL",
                fmt.Sprintf("SPLIT-BRAIN: Me=%s, Other=%s",
                            n.host.ID().String(), hb.LeaderID), runtime.GOOS)

            // DETERMINISTIC CONFLICT RESOLUTION: Lower ID wins
            myID := n.host.ID().String()
            theirID := hb.LeaderID

            if theirID < myID {
                log.Printf("[SPLIT-BRAIN] Stepping down (other has lower ID)")
                n.role = "Follower"
                n.leader = peer.ID(theirID)
                n.publishRole() // Inform cluster of role change
            } else {
                log.Printf("[SPLIT-BRAIN] Maintaining coordinator role (I have lower ID)")
                // Continue as coordinator
            }
        }

        // FOLLOWER: Update heartbeat
        if n.role == "Follower" {
            // Check if heartbeat is from our known leader
            if n.leader.String() != "" && n.leader.String() != hb.LeaderID {
                log.Printf("[WARN] Heartbeat from %s, but my leader is %s",
                           hb.LeaderID, n.leader.String())
            }
            n.lastHeartbeat = time.Now()
            n.heartbeatMissed = 0
        }

        n.mutex.Unlock()
        log.Printf("[HEARTBEAT] Received from: %s", hb.LeaderID)
    }
```

**Why This Is Safe**:
- Only adds detection logic
- Uses existing mutex properly
- Deterministic resolution (always same outcome)
- Doesn't change election flow
- Can be rolled back easily

**Testing**:
```bash
# Simulate split-brain by manually creating two coordinators
# Watch logs for "[SPLIT-BRAIN]" messages
# Verify one coordinator steps down within one heartbeat cycle (5s)
```

---

### 🟢 Priority 1B: Election ID Tracking (IMMEDIATE)

**Risk**: 🟢 Low (adds tracking, doesn't change core logic)
**Effectiveness**: 🟡 Medium (prevents vote mixing between elections)
**Implementation Time**: 45 minutes

**What It Does**:
- Assigns unique ID to each election
- Tracks which election we're participating in
- Ignores votes/announcements from wrong election

**Changes**:

```go
// Add to Node struct
type Node struct {
    // ... existing fields ...
    currentElectionID string        // Which election are we in?
    electionIDMutex   sync.Mutex
}

// Modify StartElection
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
    // Generate unique election ID (includes node ID + timestamp + attempt)
    electionID := fmt.Sprintf("%s-%d-%d",
                              n.host.ID().String(),
                              time.Now().UnixNano(),
                              attempt)

    n.electionIDMutex.Lock()
    n.currentElectionID = electionID
    n.electionIDMutex.Unlock()

    log.Printf("[ELECTION] Starting election %s (attempt %d)", electionID, attempt+1)

    // ... existing topic join code ...

    selected := weightedRandomSelection(peers)
    vote := VoteMessage{
        NodeID:     n.host.ID().String(),
        Vote:       selected,
        ElectionID: electionID,  // ADD THIS FIELD
    }

    // ... rest of function ...
}

// Update VoteMessage struct
type VoteMessage struct {
    NodeID     string `json:"nodeId"`
    Vote       string `json:"vote"`
    ElectionID string `json:"electionId"`  // NEW
}

// Update vote handling
case TypeVote:
    var vote VoteMessage
    if err := json.Unmarshal(core.Payload, &vote); err != nil {
        log.Printf("[ERROR] Failed to unmarshal VoteMessage: %v", err)
        return
    }

    n.electionIDMutex.Lock()
    currentElectionID := n.currentElectionID
    n.electionIDMutex.Unlock()

    // VALIDATE: Only accept votes from current election
    if vote.ElectionID != currentElectionID {
        log.Printf("[ELECTION] Ignoring vote from wrong election (got %s, current %s)",
                   vote.ElectionID, currentElectionID)
        return
    }

    n.electionMutex.Lock()
    defer n.electionMutex.Unlock()

    // ... existing vote counting logic ...
```

**Why This Is Safe**:
- Adds validation before accepting votes
- Prevents vote mixing from concurrent elections
- Backwards compatible (old messages without ID are ignored safely)
- No changes to core election logic

---

### 🟢 Priority 1C: Announcement Validation (IMMEDIATE)

**Risk**: 🟢 Low (only adds validation)
**Effectiveness**: 🟡 Medium (prevents conflicting announcements from being accepted)
**Implementation Time**: 30 minutes

**What It Does**:
- Only accepts first announcement for an election
- Logs conflicts without accepting them
- Provides visibility into divergence

**Changes**:

```go
// Add to Node struct
type Node struct {
    // ... existing fields ...
    announcedLeaderForElection map[string]string  // electionID -> leaderID
    announcementMutex          sync.Mutex
}

// Initialize in NewNode
return &Node{
    // ... existing fields ...
    announcedLeaderForElection: make(map[string]string),
}

// Update announcement handling
case TypeAnnouncement:
    var ann map[string]interface{}
    if err := json.Unmarshal(core.Payload, &ann); err == nil {
        leaderID, _ := ann["leader"].(string)
        electionID, hasElectionID := ann["electionId"].(string)

        if leaderID == "" {
            return
        }

        if !hasElectionID {
            log.Printf("[WARN] Announcement without election ID, accepting anyway")
            n.HandleLeaderAnnouncement(leaderID)
            return
        }

        n.announcementMutex.Lock()

        // Check if we already accepted a leader for this election
        if existingLeader, exists := n.announcedLeaderForElection[electionID]; exists {
            if existingLeader != leaderID {
                log.Printf("[CONFLICT] Multiple leaders announced for election %s: %s vs %s",
                           electionID, existingLeader, leaderID)
                app.GlobalLoggerDB.AddToOptimusLog("ELECTION-CONFLICT",
                    fmt.Sprintf("Election %s: conflicting leaders %s vs %s",
                                electionID, existingLeader, leaderID), runtime.GOOS)

                // DETERMINISTIC RESOLUTION: Lower ID wins
                if leaderID < existingLeader {
                    log.Printf("[CONFLICT] Accepting %s (lower ID)", leaderID)
                    n.announcedLeaderForElection[electionID] = leaderID
                    n.announcementMutex.Unlock()
                    n.HandleLeaderAnnouncement(leaderID)
                } else {
                    log.Printf("[CONFLICT] Keeping %s (lower ID)", existingLeader)
                    n.announcementMutex.Unlock()
                }
                return
            }
            // Same leader announced again, ignore
            n.announcementMutex.Unlock()
            return
        }

        // First announcement for this election
        n.announcedLeaderForElection[electionID] = leaderID
        n.announcementMutex.Unlock()

        log.Printf("[ELECTION] Accepting leader %s for election %s", leaderID, electionID)
        n.HandleLeaderAnnouncement(leaderID)
    }

// Update announceLeader to include election ID
func (n *Node) announceLeader(leaderID string) {
    n.electionIDMutex.Lock()
    electionID := n.currentElectionID
    n.electionIDMutex.Unlock()

    coreAnn := CoreMessage{
        Type:    TypeAnnouncement,
        Payload: mustMarshal(map[string]string{
            "leader":     leaderID,
            "electionId": electionID,  // ADD THIS
        }),
    }
    // ... rest of function ...
}
```

**Why This Is Safe**:
- Validates before accepting state changes
- Uses deterministic conflict resolution
- Provides logging for debugging
- Gracefully handles missing election IDs (backwards compatible)

---

## Layer 2: Designated Vote Counter

### 🟡 Priority 2A: Single Vote Counter (MODERATE RISK)

**Risk**: 🟡 Medium (changes election flow significantly)
**Effectiveness**: 🟢 High (prevents divergent vote counting)
**Implementation Time**: 2 hours

**What It Does**:
- Only ONE node counts votes and announces winner
- Vote counter is deterministically selected (lowest peer ID)
- Other nodes wait for the authoritative announcement

**Changes**:

```go
// In StartElection, after voting period
go func() {
    time.Sleep(electionTimeout)

    n.electionMutex.Lock()
    defer n.electionMutex.Unlock()

    // NEW: Determine designated vote counter
    voteCounterID := n.selectVoteCounter(peers)

    if voteCounterID == n.host.ID().String() {
        // I AM the vote counter
        log.Printf("[ELECTION] I am the designated vote counter")

        winner := evaluateVotes(n.votes)
        if winner == "" {
            log.Printf("[ELECTION] [Attempt %d] No winner. Retrying...", attempt+1)
            // ... retry logic ...
            return
        }

        log.Printf("[ELECTION] [Attempt %d] Winner: %s with %d votes",
                   attempt+1, winner, n.votes[winner])

        // Persist election log
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
        }

        // ONLY vote counter announces
        n.announceLeader(winner)

        n.votes = make(map[string]int)
        n.votedNodes = make(map[string]string)

    } else {
        // I am NOT the vote counter
        log.Printf("[ELECTION] Waiting for vote counter %s to announce winner", voteCounterID)

        // Wait for announcement (with timeout)
        // If timeout expires, retry election
        go n.waitForAnnouncementWithTimeout(10*time.Second, peers, attempt)
    }
}()

// NEW: Select vote counter deterministically
func (n *Node) selectVoteCounter(peers []NodeReputation) string {
    // Use LOWEST peer ID (deterministic across all nodes)
    lowestID := n.host.ID().String()

    for _, peer := range peers {
        if peer.NodeID < lowestID {
            lowestID = peer.NodeID
        }
    }

    log.Printf("[ELECTION] Designated vote counter: %s", lowestID)
    return lowestID
}

// NEW: Wait for announcement with timeout
func (n *Node) waitForAnnouncementWithTimeout(timeout time.Duration, peers []NodeReputation, attempt int) {
    timer := time.NewTimer(timeout)
    defer timer.Stop()

    <-timer.C

    // Check if announcement was received
    n.mutex.Lock()
    hasLeader := n.role != ""
    n.mutex.Unlock()

    if !hasLeader {
        log.Printf("[ELECTION] Timeout waiting for announcement, retrying election")
        n.StartElection(peers, attempt+1)
    }
}
```

**Why This Is Moderately Safe**:
- Deterministic selection (all nodes pick same counter)
- Timeout handles counter node failure
- Single source of truth for election result

**Risks**:
- If vote counter crashes during counting, election fails (needs retry)
- Requires timeout tuning
- More complex code paths

---

## Layer 3: Improved Tie Breaking

### 🟢 Priority 3A: Deterministic Tie Breaking (LOW RISK)

**Risk**: 🟢 Low (only changes tie-breaking logic)
**Effectiveness**: 🟡 Medium (prevents non-deterministic outcomes)
**Implementation Time**: 15 minutes

**What It Does**:
- Uses deterministic rule when votes are tied
- Ensures all nodes break ties the same way

**Changes**:

```go
func evaluateVotes(votes map[string]int) string {
    var leader string
    maxVotes := 0

    // Collect all candidates with max votes
    var tied []string

    for node, count := range votes {
        if count > maxVotes {
            maxVotes = count
            tied = []string{node}
        } else if count == maxVotes {
            tied = append(tied, node)
        }
    }

    if len(tied) == 0 {
        return ""
    }

    if len(tied) == 1 {
        return tied[0]
    }

    // TIE BREAKING: Sort and pick lowest ID
    sort.Strings(tied)
    leader = tied[0]

    log.Printf("[ELECTION] Tie between %v, breaking tie by lowest ID: %s", tied, leader)

    return leader
}
```

**Why This Is Safe**:
- Only changes behavior when votes are tied
- Deterministic (all nodes get same result)
- Simple logic, hard to break

---

## Layer 4: Quorum Requirements

### 🟡 Priority 4A: Minimum Vote Threshold (MODERATE RISK)

**Risk**: 🟡 Medium (election may fail more often)
**Effectiveness**: 🟢 High (prevents elections with partial data)
**Implementation Time**: 30 minutes

**What It Does**:
- Requires minimum number of votes before declaring winner
- Prevents elections from succeeding with insufficient participation

**Changes**:

```go
func (n *Node) StartElection(peers []NodeReputation, attempt int) {
    // ... existing code ...

    go func() {
        time.Sleep(electionTimeout)

        n.electionMutex.Lock()
        defer n.electionMutex.Unlock()

        // Count total votes received
        totalVotes := 0
        for _, count := range n.votes {
            totalVotes += count
        }

        // Calculate quorum (majority of known peers)
        quorum := (len(peers) / 2) + 1

        if totalVotes < quorum {
            log.Printf("[ELECTION] Insufficient votes: got %d, need %d (quorum)",
                       totalVotes, quorum)

            // Retry election
            n.votes = make(map[string]int)
            n.votedNodes = make(map[string]string)

            if attempt < *config.ElectionMaxRetries {
                backoff := time.Duration(math.Pow(2, float64(attempt))) * (*config.ElectionRetryDelay)
                time.Sleep(backoff)
                peers, _ := QueryAllReputations(GlobalReputationDB.reputationDB)
                n.StartElection(peers, attempt+1)
            } else {
                n.fallbackElection()
            }
            return
        }

        // Proceed with winner evaluation
        winner := evaluateVotes(n.votes)
        // ... rest of logic ...
    }()
}
```

**Why This Is Moderately Safe**:
- Prevents false elections
- Uses standard quorum definition (majority)

**Risks**:
- Elections may fail more frequently
- Requires tuning for different cluster sizes

---

## Recommended Implementation Order

### Phase 1: Immediate Protection (This Week)
1. ✅ **Priority 1A**: Heartbeat conflict detection (30 min)
2. ✅ **Priority 1B**: Election ID tracking (45 min)
3. ✅ **Priority 1C**: Announcement validation (30 min)
4. ✅ **Priority 3A**: Deterministic tie breaking (15 min)

**Total Time**: ~2 hours
**Risk**: Low
**Benefit**: Detects and recovers from split-brain, prevents vote mixing

### Phase 2: Structural Improvements (Next Week)
1. **Priority 2A**: Designated vote counter (2 hours)
2. **Priority 4A**: Quorum requirements (30 min)

**Total Time**: ~2.5 hours
**Risk**: Medium
**Benefit**: Prevents split-brain from occurring

### Phase 3: Testing and Monitoring (Ongoing)
1. Add comprehensive logging
2. Add metrics for election conflicts
3. Chaos testing (simulate network partitions)
4. Load testing with delayed messages

---

## Testing Strategy

### Unit Tests
```go
// Test split-brain detection
func TestHeartbeatConflictDetection(t *testing.T) {
    // Create two nodes that think they're coordinator
    // Send heartbeat from one to other
    // Verify lower ID wins
}

// Test election ID validation
func TestElectionIDValidation(t *testing.T) {
    // Send votes from different elections
    // Verify only current election votes are counted
}

// Test deterministic tie breaking
func TestDeterministicTieBreaking(t *testing.T) {
    // Create tied vote scenario
    // Verify lowest ID always wins
}
```

### Integration Tests
```bash
# Test 1: Normal election
docker-compose up -d node1 node2 node3
# Verify single coordinator elected

# Test 2: Network partition
docker-compose up -d node1 node2 node3
# Partition network using iptables
# Verify split-brain detected and resolved

# Test 3: Concurrent elections
# Kill coordinator
# Verify consistent election outcome
```

---

## Long-Term Recommendation

**Consider migrating to proven consensus algorithm**:

### Option A: HashiCorp Raft
```go
import "github.com/hashicorp/raft"
```
- Production-tested
- Strong consistency guarantees
- Built-in leader election

### Option B: etcd/raft
```go
import "go.etcd.io/raft/v3"
```
- Used by Kubernetes
- Excellent documentation
- Battle-tested

**Why Consensus Algorithm?**
- Proven correctness
- Handles network partitions
- Built-in leader leases
- Extensive testing

**Migration Path**:
1. Implement Phase 1 + Phase 2 fixes (immediate protection)
2. Design Raft integration (parallel work)
3. Gradual migration with feature flag
4. Deprecate custom election after validation

---

## Summary

**Immediate Actions** (Phase 1):
- ✅ Add split-brain detection
- ✅ Add election ID tracking
- ✅ Add announcement validation
- ✅ Fix tie breaking

**Benefits**:
- Detects multi-coordinator scenarios
- Recovers automatically
- Low risk of breaking existing functionality
- ~2 hours implementation time

**Next Steps** (Phase 2):
- Designated vote counter
- Quorum requirements

**Long-Term** (Phase 3):
- Consider Raft migration for production-grade consensus
