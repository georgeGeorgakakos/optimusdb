# OptimusDB Core Application Components

## Overview

This document describes the core application components in the `/app` directory, which form the heart of OptimusDB's functionality, managing data storage, peer initialization, service coordination, query handling, and benchmark operations.

## Directory Structure

```
app/
├── app.go              # Core data structures and types
├── service.go          # Main service loop and request handling
├── initPeer.go         # Peer initialization and network setup
├── query_handler.go    # Query processing and routing
├── benchmarks.go       # Performance monitoring and benchmarking
└── ems_subscriber.go   # Event Management Service integration
```

## Key Files and Components

### 1. app.go - Core Data Structures

**Purpose**: Defines the fundamental data structures that represent the OptimusDB application state.

#### Main Structures

##### KnowledgeBaseDB
The central structure representing the entire application state:
```go
type KnowledgeBaseDB struct {
// IPFS & Network
Node          *core.IpfsNode
Orbit         *iface.OrbitDB

// Data Stores
Contributions *orbitdb.EventLogStore    // All data contributions
Validations   *orbitdb.DocumentStore    // Validation records
KBdata        *orbitdb.DocumentStore    // Primary data
KBMetadata    *orbitdb.DocumentStore    // Metadata
whoiswhoStore *orbitdb.DocumentStore    // Peer identity
DsSWres       *orbitdb.DocumentStore    // Software resources
DsSWresaloc   *orbitdb.DocumentStore    // Resource allocation

// TOSCA Stores
DsTOSCA_ADT            *orbitdb.DocumentStore
DsTOSCA_Imported       *orbitdb.DocumentStore
DsTOSCA_Capacities     *orbitdb.DocumentStore
DsTOSCA_DeploymentPlan *orbitdb.DocumentStore
DsTOSCA_EventHistory   *orbitdb.DocumentStore

// Synchronization
ContributionsMtx sync.RWMutex
ValidationsMtx   sync.RWMutex
peersMutex       sync.Mutex

// Configuration
Config    *config.Config
Benchmark *Benchmark

// Network
discoveredPeers map[string]bool
HostID          string

// Message Queue
MQEMS      *mq.Client
EMSClient  *mq.ReconnectingClient
EMSService *mq.EMSService

// Query Engine
QueryEngine *queryengine.OptimizedEngine
}
```

##### KnowledgeBaseSQLite
Manages SQLite database operations:
```go
type KnowledgeBaseSQLite struct {
db *sql.DB
mu sync.Mutex
}
```

##### LoggerSQLite
Handles system logging to SQLite:
```go
type LoggerSQLite struct {
db *sql.DB
mu sync.Mutex
}
```

#### Key Functions

- `InitAgentName()`: Initializes unique agent identifier
- `InitLog()`: Creates and initializes the logging database
- `AddToOptimusLog()`: Adds log entries with timestamp and context

### 2. service.go - Service Coordination

**Purpose**: Main service loop that handles all requests and coordinates between different components.

#### Request Handling

The service uses a channel-based architecture:
```go
type Request struct {
Method Method                 // Operation type
Args   interface{}           // Arguments
Data   interface{}           // Additional data

// Query options
QueryOpts QueryOptions       // Distributed query settings
}
```

#### Supported Methods

- **GET**: Retrieve files from IPFS
- **POST**: Add files to IPFS
- **CONNECT**: Connect to peers
- **QUERY**: Query OrbitDB stores
- **QUERYKBDATA**: Query knowledge base data
- **SQLSELECT**: Execute SQL SELECT
- **SQLDML**: Execute SQL INSERT/UPDATE/DELETE
- **CRUDGET**: Get documents by query
- **CRUDPUT**: Put/update documents
- **CRUDUPDATE**: Update existing documents
- **CRUDDELETE**: Delete documents
- **CONTRI**: Add contributions
- **BENCHMARK**: Get performance metrics
- **HELP**: Display help information

#### Service Loop

The main `Service()` function:
1. Listens on request channel
2. Routes requests to appropriate handlers
3. Executes operations
4. Returns results via response channel
5. Logs all operations
6. Updates benchmarks

#### Key Features

- **Validation Workflow**: Automatic validation of contributions after insertion
- **Peer Communication**: Opens streams to peers for distributed operations
- **SQL Stream Handling**: Dedicated handlers for SQL operations over P2P
- **Error Recovery**: Graceful handling of peer disconnections
- **Metric Tracking**: Performance monitoring for all operations

### 3. initPeer.go - Peer Initialization

**Purpose**: Initializes the peer node, network connections, and all data stores.

#### Initialization Sequence

1. **Load Configuration**
- Attempts to load existing config
- Creates default config if none exists

2. **Setup IPFS Node**
- Creates or loads IPFS repository
- Configures network parameters
- Establishes libp2p host

3. **Initialize OrbitDB**
- Creates OrbitDB instance
- Opens or creates all data stores
- Sets up access control

4. **Setup Network**
- Configure bootstrap peers
- Initialize discovery mechanisms
- Setup stream handlers

5. **Initialize Query Engine**
- Create worker pool
- Setup caching
- Configure timeouts

6. **Setup Benchmarking**
- Initialize performance tracking
- Setup metric collection

#### Bootstrap Peers

Supports connecting to initial peers:
```bash
-bootstrap "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
```

#### Store Initialization

Each OrbitDB store is initialized with:
- Unique address
- Access control settings
- Replication configuration
- Event handlers

### 4. query_handler.go - Query Processing

**Purpose**: Advanced query processing with distributed execution capabilities.

#### Query Strategies

##### LOCAL_ONLY
- Queries only local OrbitDB stores
- Fastest response time
- May have incomplete data

##### REMOTE_ONLY
- Queries only remote peers
- Gets freshest data
- Higher latency

##### LOCAL_THEN_REMOTE_MERGE
- Tries local first
- Falls back to remote if insufficient results
- Balanced approach

##### PARALLEL_MERGE
- Queries local and remote simultaneously
- Merges results
- Best completeness

##### QUORUM
- Requires responses from N peers
- Strongest consistency
- Higher latency

#### Query Options

```go
type QueryOptions struct {
Strategy       QueryStrategy      // Execution strategy
Consistency    ConsistencyLevel   // BEST_EFFORT, QUORUM, ALL
TimeBudgetMs   int               // Max query time
QuorumN        int               // Required responses
MinRows        int               // Stop early if reached
StaleOkTTLms   int               // Use cached results
MaxPeers       int               // Peer selection limit
IncludeLocal   bool              // Include local data
AnnotateSource bool              // Tag results with source
}
```

#### Query Execution Flow

1. Parse query criteria
2. Select strategy based on options
3. Execute local query if included
4. Select target peers (by reputation)
5. Execute remote queries in parallel
6. Merge and deduplicate results
7. Apply filters and sorting
8. Return annotated results

#### Result Format

Results include source annotation:
```json
{
"_id": "doc123",
"field1": "value1",
"_source": "local|peer_id"
}
```

### 5. benchmarks.go - Performance Monitoring

**Purpose**: Track and report system performance metrics.

#### Benchmark Structure

```go
type Benchmark struct {
Get        Operation        // GET operation stats
Post       Operation        // POST operation stats
Connect    Operation        // CONNECT operation stats
Query      Operation        // QUERY operation stats
Validation Operation        // VALIDATION operation stats
CRUDGET    Operation        // CRUD GET stats
CRUDPUT    Operation        // CRUD PUT stats
CRUDUPDATE Operation        // CRUD UPDATE stats
CRUDDELETE Operation        // CRUD DELETE stats

// System metrics
CPUUsage    float64
MemoryUsage uint64
Timestamp   time.Time
}

type Operation struct {
AvgDuration time.Duration
MinDuration time.Duration
MaxDuration time.Duration
Count       int
Errors      int
}
```

#### Monitoring Functions

- `MonitorMemoryAndCPU()`: Continuous system resource monitoring
- `updateBenchmark()`: Updates operation statistics
- `GetBenchmarkReport()`: Returns comprehensive performance report

#### Tracked Metrics

1. **Operation Performance**
- Average, min, max duration
- Success/error count
- Throughput

2. **System Resources**
- CPU usage percentage
- Memory consumption
- Disk I/O
- Network bandwidth

3. **Network Metrics**
- Peer connection count
- Data transfer volume
- Query response times

### 6. ems_subscriber.go - Message Queue Integration

**Purpose**: Integration with Enterprise Message Service (ActiveMQ/TIBCO EMS).

#### EMS Configuration

```go
type EMSConfig struct {
Enabled      bool
BrokerURL    string
Topic        string
DurableID    string    // For durable subscriptions
Username     string
Password     string
RetryDelay   time.Duration
}
```

#### Subscription Management

- **Durable Subscriptions**: Survive broker restarts
- **Auto-reconnection**: Exponential backoff on failure
- **Message Processing**: JSON-based command handling

#### Message Format

```json
{
"action": "query|insert|update|delete",
"resource": "kbdata|metadata|...",
"params": {
"criteria": {...},
"data": {...}
}
}
```

#### Supported Actions

1. **Query**: Execute distributed queries
2. **Insert**: Add new documents
3. **Update**: Modify existing documents
4. **Delete**: Remove documents
5. **Sync**: Trigger peer synchronization

## Global Variables

### GlobalKBSQLite
Singleton instance of the SQLite knowledge base, accessible across the application.

### GlobalLoggerDB
Singleton instance of the logging database for centralized log management.

## Logging System

### Log Types

```go
const (
RecoverableErr    LogType = 0    // Recoverable errors
NonRecoverableErr LogType = 1    // Fatal errors
Info              LogType = 2    // Informational
Print             LogType = 3    // Debug output
)
```

### Log Structure

```go
type Log struct {
Type LogType
Data interface{}
}
```

### Log Database Schema

```sql
CREATE TABLE optimus_log (
id INTEGER PRIMARY KEY AUTOINCREMENT,
log_level TEXT,
message TEXT,
timestamp DATETIME,
osplatform TEXT
)
```

### Log Querying

Logs can be queried by:
- Date (YYYY-MM-DD)
- Hour (HH)
- Log level
- OS platform

## Inter-Component Communication

### Channel Architecture

OptimusDB uses Go channels for inter-component communication:

```go
reqChan := make(chan Request, 100)   // Request channel
resChan := make(chan interface{}, 100) // Response channel
logChan := make(chan Log, 100)       // Logging channel
```

### Request Flow

1. API receives request (HTTP/Shell)
2. API creates Request struct
3. Request sent to reqChan
4. Service loop processes request
5. Result sent to resChan
6. API returns result to client

### Logging Flow

1. Component creates Log struct
2. Log sent to logChan
3. Main loop processes log
4. Log written to GlobalLoggerDB
5. Console output (if configured)

## SQL Integration

### SQLite Databases

1. **Knowledge Base DB** (`optimusdb.db`)
- Relational data storage
- Tables created dynamically
- Full SQL support

2. **Logger DB** (`optimus_logger.db`)
- System logs
- Query history
- Error tracking

### SQL Stream Protocol

For distributed SQL queries:
1. Open stream to peer
2. Send SQL command (JSON)
3. Receive results (JSON)
4. Close stream

```go
type SQLCommand struct {
Type     string   // "select", "insert", "update", "delete"
SQL      string   // SQL statement
Database string   // Target database
}

type SQLResult struct {
Success  bool
Rows     []map[string]interface{}
Error    string
RowCount int
}
```

## Error Handling

### Error Types

1. **Recoverable Errors**
- Network timeouts
- Peer unavailable
- Temporary storage issues
- Retry logic applied

2. **Non-Recoverable Errors**
- Configuration errors
- Critical storage failures
- Invalid peer credentials
- Application termination

### Error Logging

All errors are:
- Logged to GlobalLoggerDB
- Sent to logChan
- Categorized by severity
- Include stack traces
- Timestamped

## Performance Optimizations

### 1. Concurrency

- Read-Write mutexes for data stores
- Channel-based communication
- Worker pools for parallel operations
- Goroutine per peer query

### 2. Caching

- Query result caching
- Peer address caching
- OrbitDB entry caching
- TTL-based invalidation

### 3. Batching

- Batch inserts to SQLite
- Batch validation processing
- Grouped peer queries

### 4. Connection Pooling

- Reuse peer connections
- Stream multiplexing
- Connection keep-alive

## Testing

### Unit Tests

Test coverage for:
- Request parsing
- Query execution
- Benchmark calculations
- Peer communication

### Integration Tests

Scripts available:
- `testQueryOptimization.ps1`
- `testQueryPerformance_Optimization.ps1`
- `Test-OptimusDB-DecentralizedStrategies.ps1`

### Benchmarking

Run with `-benchmark` flag:
```bash
./optimusdb -benchmark -repo testnode
```

Results written to:
- `repo_benchmark` (JSON)
    - Console output
    - BenchmarkResults.json

    ## Configuration Best Practices

    ### Recommended Settings

    **Development**:
    ```bash
    ./optimusdb -repo dev -http -port 18001 -shell -autodiscovery-mdns
    ```

    **Production**:
    ```bash
    ./optimusdb -repo prod -http -port 18001 -autodiscovery-dht \
    -bootstrap "peer1,peer2,peer3" -metrics
                ```

                **Testing**:
                ```bash
                ./optimusdb -repo test -benchmark -autodiscovery-mdns
                ```

                ## Troubleshooting

                ### Common Issues

                1. **Peer Connection Failures**
                - Check firewall settings
                - Verify bootstrap peer addresses
                - Ensure network connectivity

                2. **OrbitDB Store Issues**
                - Verify IPFS daemon is running
                - Check repo directory permissions
                - Review access control settings

                3. **High Memory Usage**
                - Reduce worker pool size
                - Lower cache TTL
                - Adjust query time budget

                4. **Slow Queries**
                - Check peer reputation scores
                - Verify network latency
                - Review query strategy
                - Consider LOCAL_ONLY for known data

                ## Best Practices

                ### 1. Resource Management
                - Always close connections after use
                - Implement proper cleanup on shutdown
                - Monitor memory usage with benchmarks

                ### 2. Error Handling
                - Log all errors with context
                - Implement retry logic for transient failures
                - Use appropriate log levels

                ### 3. Performance
                - Use appropriate query strategies
                - Leverage caching when possible
                - Monitor benchmark metrics
                - Tune worker pool sizes

                ### 4. Security
                - Validate all user inputs
                - Use secure peer connections
                - Implement access control on stores
                - Monitor for malicious peers

                ## Future Enhancements

                Potential improvements:
                - Advanced query optimization
                - Automatic peer selection tuning
                - Machine learning for reputation scoring
                - Enhanced caching strategies
                - Better resource allocation
                - Improved fault tolerance

                ## Related Documentation

                - See `/docs/OptimusDB_Technical_DeepDive.md` for architecture details
                - See `02_README_API.md` for API documentation
                - See `03_README_SQL_ENGINE.md` for SQL engine details
                - See `05_README_ELECTION.md` for election system details