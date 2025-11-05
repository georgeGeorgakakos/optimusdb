# OptimusDB Election Architecture - Visual Guide

## 🎨 System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        OptimusDB Cluster                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌───────────┐         ┌───────────┐         ┌───────────┐     │
│  │  Node 1   │         │  Node 2   │         │  Node 3   │     │
│  │           │         │           │         │           │     │
│  │ Follower  │◄────────┤Coordinator│────────►│ Follower  │     │
│  │           │Heartbeat│           │Heartbeat│           │     │
│  └─────┬─────┘         └─────┬─────┘         └─────┬─────┘     │
│        │                     │                     │             │
│        │         ┌───────────▼───────────┐         │             │
│        └────────►│   Election Topic      │◄────────┘             │
│                  │   "optimusdb"         │                       │
│                  │   (PubSub)            │                       │
│                  └───────────────────────┘                       │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Message Flow

### 1. Election Process
```
Node 1                    Election Topic                    Node 2
│                             │                              │
│──[Vote: Node1→Node2]───────►│                              │
│                             │──[Vote: Node1→Node2]────────►│
│                             │                              │
│                             │◄─[Vote: Node2→Node2]─────────│
│◄─[Vote: Node2→Node2]────────│                              │
│                             │                              │
│     [Wait 5 seconds]        │                              │
│                             │                              │
│◄─[Result: Node2 wins]───────│                              │
│                             │──[Result: Node2 wins]───────►│
│                             │                              │
│◄─[Announcement: Node2]──────│                              │
│                             │──[Announcement: Node2]──────►│
│                             │                              │
▼                             ▼                              ▼
Follower                                                  Coordinator
```

### 2. Heartbeat Flow
```
Coordinator                Election Topic               Follower 1              Follower 2
│                          │                           │                       │
│──[Heartbeat]────────────►│                           │                       │
│   every 5s               │──[Heartbeat]─────────────►│                       │
│                          │──[Heartbeat]──────────────┼──────────────────────►│
│                          │                           │                       │
│                          │                         ✓ Received            ✓ Received
│                          │                           │                       │
│                          │                           │                       │
[Next heartbeat...]        │                           │                       │
│                          │                           │                       │
```

### 3. Leader Failure Detection
```
Time    Coordinator    Election Topic    Follower
─────────────────────────────────────────────────
0s         ◄─[Heartbeat]──►            ✓
5s         ◄─[Heartbeat]──►            ✓
10s         ◄─[Heartbeat]──►            ✓
15s    [CRASHED] ✗
20s                                     ⚠️ Timeout!
25s                                     ⚠️ Missed 2
30s                                     ⚠️ Missed 3
30s                                     🔴 FAILURE!
30s                     ◄──[StartElection]
31s         [Re-election process begins]
```

---

## 📊 State Machine

### Node State Transitions
```
┌──────────────┐
│   STARTUP    │
└──────┬───────┘
│
│ Discover Peers
▼
┌──────────────┐
│  DISCOVERING │
└──────┬───────┘
│
│ Peers >= Threshold
▼
┌──────────────┐
│  COLLECTING  │◄────────┐
│  REPUTATION  │         │
└──────┬───────┘         │
│                 │
│ Have Enough Data│ Not Enough
▼                 │
┌──────────────┐         │
┌───►│   ELECTING   │─────────┘
│    └──────┬───────┘
│           │
│           │ Won Election
│           ▼
│    ┌──────────────┐
│    │ COORDINATOR  │──────┐
│    │              │      │ Send Heartbeats
│    └──────┬───────┘      │
│           │              │
│           │ Lost in      │
│           │ Re-election  │
│           ▼              │
│    ┌──────────────┐      │
└────┤   FOLLOWER   │◄─────┘
│              │
└──────┬───────┘
│
│ Detect Leader Failure
│
└──────────────────────┐
│
Re-election Triggered  │
▼
[Back to ELECTING]
```

---

## 🎭 Role-Based View

### Coordinator (Leader) Responsibilities
```
┌─────────────────────────────────────┐
│         Coordinator Node             │
├─────────────────────────────────────┤
│                                      │
│  📤 Send Heartbeat (every 5s)       │
│     └─► All Followers                │
│                                      │
│  📊 Update Own Reputation (30s)     │
│     └─► Publish to cluster           │
│                                      │
│  🗳️  Participate in Elections       │
│     └─► Vote if leadership lost      │
│                                      │
│  📝 Maintain Leadership Count       │
│     └─► Increment on election win    │
│                                      │
│  🔄 Monitor Own Health              │
│     └─► Step down if failing         │
│                                      │
└─────────────────────────────────────┘
```

### Follower Responsibilities
```
┌─────────────────────────────────────┐
│           Follower Node              │
├─────────────────────────────────────┤
│                                      │
│  📥 Listen for Heartbeats           │
│     └─► Check every 2s               │
│     └─► Timeout after 3 misses       │
│                                      │
│  📊 Update Own Reputation (30s)     │
│     └─► Publish to cluster           │
│                                      │
│  🗳️  Participate in Elections       │
│     └─► Vote for best candidate      │
│                                      │
│  🔍 Monitor Leader Health           │
│     └─► Trigger re-election on fail  │
│                                      │
│  🎯 Acknowledge Leadership          │
│     └─► Accept election results      │
│                                      │
└─────────────────────────────────────┘
```

---

## 🔐 Message Types

### CoreMessage Structure
```
┌────────────────────────────────────────┐
│           CoreMessage                   │
├────────────────────────────────────────┤
│ Type: string                            │
│   - "vote"                              │
│   - "heartbeat"                         │
│   - "role"                              │
│   - "announcement"                      │
│   - "reputation"                        │
│   - "election_result"                   │
│                                         │
│ Payload: json.RawMessage                │
│   └─► Specific message struct           │
│       (VoteMessage, HeartbeatMessage,   │
│        RoleMessage, etc.)               │
└────────────────────────────────────────┘
```

### Message Examples

#### Vote Message
```json
{
"type": "vote",
"payload": {
"nodeId": "12D3KooWABC...",
"vote": "12D3KooWXYZ..."
}
}
```

#### Heartbeat Message
```json
{
"type": "heartbeat",
"payload": {
"leaderId": "12D3KooWXYZ...",
"time": 1698504000
}
}
```

#### Reputation Message
```json
{
"type": "reputation",
"payload": {
"nodeId": "12D3KooWABC...",
"uptime": 0.95,
"leadership_count": 2,
"latency": 15.2,
"user_cpu": 12.5,
"system_cpu": 3.2,
...
}
}
```

---

## 🔄 Election Timeline

### Normal Election (2 Nodes)
```
Time    Event                           Node 1          Node 2
─────────────────────────────────────────────────────────────────
0s     Nodes start                     STARTUP         STARTUP
3s     Peer discovery                  DISCOVERING     DISCOVERING
5s     Peers found                     ✓ Found 1       ✓ Found 1
15s     Reputation exchanged            ✓ Have data     ✓ Have data
15s     Election triggered              ELECTING        ELECTING
15s     Votes cast                      Vote→Node2      Vote→Node2
20s     Votes counted                   Count=2         Count=2
20s     Winner announced                FOLLOWER        COORDINATOR
20s     Roles assigned                  ✓               ✓
21s     Heartbeats start                                ●─●─●─●─►
22s     Cluster operational             ✓               ✓
```

### Re-election After Leader Failure (3 Nodes)
```
Time    Event                   Node 1          Node 2          Node 3
────────────────────────────────────────────────────────────────────────
0s     Normal operation        FOLLOWER        COORDINATOR     FOLLOWER
5s     Heartbeat normal        ✓               ●─●─●──►        ✓
10s     Leader crashes          ✓               ✗ CRASH         ✓
15s     Heartbeat timeout       ⚠️ Miss 1                       ⚠️ Miss 1
20s     Heartbeat timeout       ⚠️ Miss 2                       ⚠️ Miss 2
25s     Heartbeat timeout       ⚠️ Miss 3                       ⚠️ Miss 3
25s     Failure detected        🔴 FAILURE!                     🔴 FAILURE!
25s     Re-election trigger     ELECTING                        ELECTING
26s     Votes cast              Vote→Node3                      Vote→Node3
31s     New winner              FOLLOWER                        COORDINATOR
32s     Heartbeats resume       ✓                               ●─●─●──►
```

---

## 🗺️ Data Flow Diagram

### Reputation System
```
┌────────────────┐
│  Node Metrics  │
│  - CPU Usage   │
│  - Memory      │
│  - Disk I/O    │
│  - Network     │
│  - Uptime      │
└────────┬───────┘
│
│ Collect every 30s
▼
┌────────────────┐
│  Calculate     │
│  Reputation    │
│  Score         │
└────────┬───────┘
│
├──────────────┬────────────────┐
│              │                │
▼              ▼                ▼
┌────────────┐  ┌──────────────┐  ┌──────────┐
│ Store in   │  │ Publish to   │  │ Use in   │
│ Local DB   │  │ PubSub Topic │  │ Election │
└────────────┘  └──────────────┘  └──────────┘
```

### Vote Tracking
```
Node receives vote:
│
▼
┌─────────────────────┐
│ Check votedNodes    │◄──── Map: voterID → candidateID
└────────┬────────────┘
│
├─── Already voted? ───► Log & Ignore
│
▼ Not voted yet
┌─────────────────────┐
│ Record voter        │
│ votedNodes[voterID] │
│    = candidateID    │
└────────┬────────────┘
│
▼
┌─────────────────────┐
│ Increment vote      │
│ votes[candidateID]++│◄──── Map: candidateID → count
└─────────────────────┘
```

---

## 🎯 Critical Paths

### Path 1: Startup to First Election
```
[Node Start]
→ [Initialize Topics]
→ [Start Listeners]
→ [Publish Reputation]
→ [Discover Peers]
→ [Wait for Peer Reputation] (10s)
→ [Start Election]
→ [Cast Vote]
→ [Wait for Results] (5s)
→ [Announce Winner]
→ [Assign Roles]
→ [Start Heartbeats]
```

### Path 2: Heartbeat Monitoring
```
[Follower Node]
→ [Receive Heartbeat]
→ [Update lastHeartbeat]
→ [Reset missedCount]
→ [Wait 2s]
→ [Check Timeout]
→ [If timeout > 10s]
→ [Increment missedCount]
→ [If missedCount >= 3]
→ [Trigger Re-election]
```

### Path 3: Vote Processing
```
[Receive Vote Message]
→ [Unmarshal CoreMessage]
→ [Extract VoteMessage]
→ [Lock electionMutex]
→ [Check votedNodes[voterID]]
→ [If already voted]
→ [Log & Return]
→ [Record vote]
→ [votedNodes[voterID] = candidateID]
→ [votes[candidateID]++]
→ [Unlock electionMutex]
→ [Log vote received]
```

---

## 📈 Performance Characteristics

### Timing Diagram
```
Event                    Time     Cumulative
─────────────────────────────────────────────
Node startup              0s         0s
Peer discovery           0-5s       5s
Reputation exchange      5-15s      15s
Election trigger         15s        15s
Vote casting             15-16s     16s
Vote collection          16-20s     20s
Result announcement      20s        20s
Heartbeat starts         21s        21s
─────────────────────────────────────────────
Total to operational:              21s
```

### Resource Usage
```
Component              Goroutines    Memory      Network
─────────────────────────────────────────────────────────
Reputation Publisher        1         ~1KB        ~500B/30s
Election Listener           1         ~2KB        Variable
Heartbeat Monitor           1         ~512B       ~100B/2s
State Logger                1         ~512B       0
Heartbeat Sender            1         ~512B       ~200B/5s
───        ────        ─────────
Total:                      5         ~5KB        ~300B/s avg
```

---

## 🔍 Debug Flow

### State Logging (Every 15s)
```
[DEBUG STATE] Output:
├─ Role: Coordinator | Follower
├─ Leader: peer-id
    ├─ IsElecting: true | false
    ├─ Votes: map[node1:2 node2:1]
    ├─ VotedNodes: map[voter1:node1 voter2:node1]
    ├─ LastHeartbeat: 2025-10-28T18:00:00Z
    └─ HeartbeatMissed: 0
    ```

    ### Log Grep Patterns
    ```bash
    # See full election process
    grep "\[ELECTION\]\|\[VOTE\]\|\[COORDINATOR\]\|\[FOLLOWER\]" log.txt

    # Monitor heartbeats
    grep "\[HEARTBEAT\]" log.txt

    # Check for errors
    grep "\[ERROR\]\|\[WARN\]" log.txt

    # View state dumps
    grep "\[DEBUG STATE\]" log.txt

    # Track specific node
    grep "12D3KooW..." log.txt
    ```

    ---

    ## 🎨 Color-Coded Status

    ### Visual Status Indicators
    ```
    🟢 OPERATIONAL  - Cluster running normally
    🟡 ELECTING     - Election in progress
    🟠 DEGRADED     - Leader missing, functioning
    🔴 FAILED       - Critical failure, re-electing
    ⚪ STARTING     - Node initializing
    🔵 DISCOVERING  - Finding peers
    ```

    ### Health Check Flow
    ```
    ┌──────────────┐
    │ Check Status │
    └──────┬───────┘
    │
    ├─► 🟢 Leader present, heartbeats OK
    │
    ├─► 🟡 Election in progress
    │
    ├─► 🟠 Leader missing <15s
    │
    ├─► 🔴 Leader missing >15s
    │
    ├─► ⚪ Node starting up
    │
    └─► 🔵 Discovering peers
    ```

    ---

    ## 📊 Metrics to Monitor

    ### Key Performance Indicators
    ```
    Metric                      Target    Alert If
    ─────────────────────────────────────────────────
    Election Success Rate       >95%      <90%
    Time to Elect Leader        <30s      >60s
    Heartbeat Latency          <100ms     >500ms
    Re-election Time           <20s       >40s
    Duplicate Votes            0          >0
    Failed Elections           0          >1
    Leader Tenure              >1min      <30s
    Node Sync Time             <15s       >30s
    ```

    ---

    ## 🎯 Summary Diagram

    ### Complete System Overview
    ```
    OptimusDB Cluster
    ┌──────────────────────────────────────────┐
    │                                           │
    │  ┌─────────────┐    ┌─────────────┐     │
    │  │ Coordinator │───►│  Followers  │     │
    │  │   Node 2    │    │  Node 1,3   │     │
    │  └──────┬──────┘    └──────┬──────┘     │
    │         │                   │             │
    │         └─────────┬─────────┘             │
    │                   │                       │
    │         ┌─────────▼─────────┐             │
    │         │   Election Topic  │             │
    │         │                   │             │
    │         │ Messages:         │             │
    │         │ • Vote            │             │
    │         │ • Heartbeat       │             │
    │         │ • Reputation      │             │
    │         │ • Role            │             │
    │         │ • Announcement    │             │
    │         └───────────────────┘             │
    │                                           │
    │         ┌───────────────────┐             │
    │         │  SQLite Database  │             │
    │         │                   │             │
    │         │ • Reputation Data │             │
    │         │ • Election Logs   │             │
    │         └───────────────────┘             │
    │                                           │
    └──────────────────────────────────────────┘
    ```

    ---

    This visual guide should help you understand how all the components work together! 🎨