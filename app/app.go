package app

import (
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ipfs/kubo/core"
	"log"
	"optimusdb/config"
	"optimusdb/mq"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var GlobalKBSQLite *KnowledgeBaseSQLite
var GlobalLoggerDB *LoggerSQLite

// KnowledgeBaseDB we will try to connect to on startup
// represents the application across go routines
type KnowledgeBaseDB struct {
	// data storage
	Node          *core.IpfsNode         // TODO : only because of node.PeerHost.EventBus
	Contributions *orbitdb.EventLogStore // the log which holds all contributions
	Validations   *orbitdb.DocumentStore // the store which holds all validations
	KBdata        *orbitdb.DocumentStore // the store which holds data
	//CRDTs: For conflict-free data synchronization.
	KBMetadata    *orbitdb.DocumentStore // the store which holds metadata
	whoiswhoStore *orbitdb.DocumentStore // the store which holds data
	DsSWres       *orbitdb.DocumentStore // the store which holds data
	DsSWresaloc   *orbitdb.DocumentStore // the store which holds metadata
	// TOSCA specific datastores
	DsTOSCA_ADT            *orbitdb.DocumentStore
	DsTOSCA_Imported       *orbitdb.DocumentStore
	DsTOSCA_Capacities     *orbitdb.DocumentStore
	DsTOSCA_DeploymentPlan *orbitdb.DocumentStore
	DsTOSCA_EventHistory   *orbitdb.DocumentStore

	Orbit *iface.OrbitDB
	// mutex to control access to the eventlog db across go routines
	ContributionsMtx sync.RWMutex
	ValidationsMtx   sync.RWMutex
	// persisted config
	Config *config.Config
	// benchmarks
	Benchmark *Benchmark

	// Add below:
	discoveredPeers map[string]bool
	peersMutex      sync.Mutex

	//For EMS
	MQEMS  *mq.Client
	HostID string
	// for the watchdog service
	EMSClient  *mq.ReconnectingClient
	EMSService *mq.EMSService
}

// /** this is the struct for the SQL
//type KnowledgeBaseRDBMS struct {
//	Session *engine.Session
//}

type EMSMessage struct {
	Action   string                 `json:"action"`
	Resource string                 `json:"resource"`
	Params   map[string]interface{} `json:"params"`
}

type LogType uint8

const (
	RecoverableErr    LogType = 0
	NonRecoverableErr LogType = 1
	Info              LogType = 2
	Print             LogType = 3
)

type Log struct {
	Type LogType
	Data interface{}
}

// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// KnowledgeBaseSQLite manages SQLite connection
type KnowledgeBaseSQLite struct {
	DB *sql.DB
}

type LoggerSQLite struct {
	theLog *sql.DB
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//	SQLite instantiation
//
// InitSQLite initializes the SQLite database and ensures tables exist
func InitSQLite(dbPath string) (*KnowledgeBaseSQLite, error) {

	log.Printf("[INFO] Initializing RDBMS KnowledgeBase : %v\n", dbPath)
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Initializing RDBMS KnowledgeBase : %v"), runtime.GOOS)
	// Open SQLite Database
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Fatalf("[ERROR] Failed to connect to SQLite database: %v", err)
		GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Failed to connect to SQLite database: %v", err), runtime.GOOS)
		return nil, err
	}

	// Create the KnowledgeBaseSQLite instance
	GlobalKBSQLite = &KnowledgeBaseSQLite{DB: db}

	// Create tables
	err = GlobalKBSQLite.createDataCatalog()
	if err != nil {
		log.Fatalf("[ERROR] Table creation failed for DataCatalog: %v", err)
		GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Table creation failed for DataCatalog: %v", err), runtime.GOOS)
		return nil, err
	}
	err = GlobalKBSQLite.createTOSCAMetadataTable()
	if err != nil {
		log.Fatalf("[ERROR] Table creation failed for TOSCA Metadata: %v", err)
		GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Table creation failed for TOSCA Metadata: %v", err), runtime.GOOS)
		return nil, err
	}

	log.Println("[INFO] SQLite Database Ready at:", dbPath)
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite Database Ready at: %v", dbPath), runtime.GOOS)
	return GlobalKBSQLite, nil
}

// InitSQLite initializes the SQLite database and ensures tables exist
func InitLog() (*LoggerSQLite, error) {

	rdbmsCache := filepath.Join(filepath.Join(filepath.Join(os.Getenv("HOME"), ".cache"), "optimusdb", *config.FlagRepo, "optimusdb"), "optimuslog.db")
	dir := filepath.Dir(rdbmsCache)

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for DB: %w", err)
	}

	//log.Printf("[INFO] Initializing RDBMS Logger : %v\n", rdbmsCache)
	//AddToOptimusLog("INFO", fmt.Sprintf("Initializing RDBMS Logger: %v", rdbmsCache), runtime.GOOS)
	// Open SQLite Database
	db, err := sql.Open("sqlite3", rdbmsCache)
	if err != nil {
		log.Fatalf("[ERROR] Failed to connect to SQLite database: %v", err)
		return nil, err
	}

	// Create the KnowledgeBaseSQLite instance
	GlobalLoggerDB = &LoggerSQLite{theLog: db}

	err = GlobalLoggerDB.createLogTable()
	if err != nil {
		log.Fatalf("[ERROR] Table creation failed for Optimus Logger: %v", err)
		return nil, err
	}

	//Create tje EMS table events
	err = GlobalLoggerDB.createEMSEventsTable() // EMS
	if err != nil {
		log.Fatalf("[ERROR] Table creation failed for EMS events under the Optimus Logger: %v", err)
		return nil, err
	}

	//
	log.Println("[INFO] SQLite Database Ready at:", rdbmsCache)
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite Database Ready at: %v", rdbmsCache), runtime.GOOS)
	return GlobalLoggerDB, nil
}

// createTables ensures the `datacatalog` table exists
func (kb *KnowledgeBaseSQLite) createDataCatalog() error {
	tableQuery :=
		`CREATE TABLE IF NOT EXISTS datacatalog (
		_id VARCHAR(36) PRIMARY KEY,
		author VARCHAR(255),
		metadata_type VARCHAR(255),
		component VARCHAR(255),
		behaviour VARCHAR(255),
		relationships TEXT,
		associated_id VARCHAR(36),
		name VARCHAR(255),
		description TEXT,
		tags VARCHAR(255),
		status VARCHAR(50),
		created_by VARCHAR(255),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		related_ids VARCHAR(255),
		priority VARCHAR(50),
		scheduling_info VARCHAR(255),
		sla_constraints VARCHAR(255),
		ownership_details VARCHAR(255),
		audit_trail VARCHAR(255)
	);`
	_, err := kb.DB.Exec(tableQuery)
	if err != nil {
		return err
	}
	//log.Println("[INFO] Table `datacatalog` created or already exists.")
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `datacatalog` created or already exists."), runtime.GOOS)
	return nil
}

// createTables ensures the `datacatalog` table exists
func (kb *KnowledgeBaseSQLite) createTOSCAMetadataTable() error {
	tableQuery :=
		`CREATE TABLE IF NOT EXISTS toscametadata (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			template_id TEXT NOT NULL UNIQUE,
			description TEXT,
			node_templates_count INTEGER,
			created_at TEXT,
			-- New metadata
			filename             TEXT,           -- original filename uploaded
			filesize_bytes       INTEGER,        -- file size if known
			content_sha256       TEXT,           -- hash of the uploaded content
			ipfs_path            TEXT,           -- /ipfs/<cid> if stored in IPFS
			uploader             TEXT,           -- free-form (user/agent)
			source_pod           TEXT,           -- k8s pod that processed the upload
			source_ip            TEXT            -- client or node IP
		);`
	_, err := kb.DB.Exec(tableQuery)
	if err != nil {
		return err
	}
	//log.Println("[INFO] Table `toscametadata` created or already exists.")
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `toscametadata` created or already exists"), runtime.GOOS)

	return nil
}

// createTables ensures the `datacatalog` table exists
func (kb *LoggerSQLite) createLogTable() error {
	tableQuery :=
		`
				CREATE TABLE IF NOT EXISTS optimusLogger (
					id INTEGER PRIMARY KEY,
					timestamp TEXT,
					date TEXT,
					hour TEXT,
					level TEXT,
					message TEXT,
					source TEXT
				);
				CREATE INDEX IF NOT EXISTS idx_logs_date_hour ON optimusLogger(date, hour);
			`

	_, err := kb.theLog.Exec(tableQuery)
	if err != nil {
		return err
	}
	//log.Println("[INFO] Table `optimusLogger` created or already exists.")
	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Table `optimusLogger` created or already exists."), runtime.GOOS)
	return nil
}

// Create Insert Function for This Table
func (kb *KnowledgeBaseSQLite) InsertTOSCAMetadata(
	templateID string,
	description string,
	nodeCount int,
	filename string,
	filesizeBytes int64,
	contentSHA256 string,
	ipfsPath string,
	uploader string,
	sourcePod string,
	sourceIP string,
) error {
	query := `
	INSERT INTO toscametadata (
		template_id, description, node_templates_count, created_at,
		filename, filesize_bytes, content_sha256, ipfs_path, uploader, source_pod, source_ip
	) VALUES (?, ?, ?, datetime('now'), ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(template_id) DO UPDATE SET
		description=excluded.description,
		node_templates_count=excluded.node_templates_count,
		created_at=excluded.created_at,
		filename=excluded.filename,
		filesize_bytes=excluded.filesize_bytes,
		content_sha256=excluded.content_sha256,
		ipfs_path=excluded.ipfs_path,
		uploader=excluded.uploader,
		source_pod=excluded.source_pod,
		source_ip=excluded.source_ip;
	`

	_, err := kb.DB.Exec(query,
		templateID, description, nodeCount,
		filename, filesizeBytes, contentSHA256, ipfsPath,
		uploader, sourcePod, sourceIP,
	)
	if err == nil {
		GlobalLoggerDB.AddToOptimusLog("INFO",
			fmt.Sprintf("Inserted/updated record for TOSCAMetadata table: template_id=%s, filename=%s", templateID, filename),
			runtime.GOOS)
	} else {
		GlobalLoggerDB.AddToOptimusLog("ERROR",
			fmt.Sprintf("Failed to insert/update TOSCAMetadata: %v", err),
			runtime.GOOS)
	}
	return err
}

// AddToOptimusLog inserts a log entry into the optimusLogger table
func (kb *LoggerSQLite) AddToOptimusLog(level, message, source string) error {
	now := time.Now()
	timestamp := now.Format(time.RFC3339)
	date := now.Format("2006-01-02")
	hour := now.Format("15")

	// Automatically capture source if not provided
	if source == "" {
		if _, file, line, ok := runtime.Caller(1); ok {
			source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
		} else {
			source = "unknown"
		}
	}

	stmt, err := kb.theLog.Prepare(`INSERT INTO optimusLogger(timestamp, date, hour, level, message, source)
                                VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(timestamp, date, hour, level, message, source)
	return err
}

// Get Logs per Hr
func (kb *LoggerSQLite) GetLogsForHour(date, hour string) ([]map[string]string, error) {
	rows, err := kb.theLog.Query(`SELECT timestamp, level, message, source FROM optimusLogger
                           WHERE date = ? AND hour = ? ORDER BY timestamp DESC`, date, hour)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []map[string]string
	for rows.Next() {
		var timestamp, level, message, source string
		if err := rows.Scan(&timestamp, &level, &message, &source); err != nil {
			continue
		}
		logs = append(logs, map[string]string{
			"timestamp": timestamp,
			"level":     level,
			"message":   message,
			"source":    source,
		})
	}
	return logs, nil
}

// sqlDML executes SQL statements and returns results if it's a SELECT query.
func (kb *KnowledgeBaseSQLite) SqlDML(stmt string, logChan chan Log) (interface{}, error) {
	if kb == nil {
		return nil, errors.New("ERROR: KB obj in SQL DML is nil")
	}
	if kb.DB == nil {
		return nil, errors.New("ERROR: kb.DB obj in SQL DML is nil")
	}

	// Check if the query is a SELECT statement
	if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(stmt)), "SELECT") {
		// Execute SELECT query and fetch results
		rows, err := kb.DB.Query(stmt)
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("ERROR: Problem executing SELECT statement: %v", err)}
			return nil, err
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, err
		}

		// Prepare a slice to store results
		var results []map[string]interface{}

		for rows.Next() {
			// Create a slice of interface{} to store each row's column values
			values := make([]interface{}, len(columns))
			valuePtrs := make([]interface{}, len(columns))
			for i := range columns {
				valuePtrs[i] = &values[i]
			}

			// Scan row into the slice
			if err := rows.Scan(valuePtrs...); err != nil {
				return nil, err
			}

			// Create a map to store column name -> value
			rowMap := make(map[string]interface{})
			for i, col := range columns {
				rowMap[col] = values[i]
			}
			results = append(results, rowMap)
		}

		// Return the fetched data
		return results, nil
	}

	// If it's an INSERT, UPDATE, DELETE statement
	result, err := kb.DB.Exec(stmt)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("ERROR: Problem executing DML statement: %v", err)}
		return nil, err
	}

	// Get affected rows count
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}

	GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQL statement executed successfully, affected rows: %d", rowsAffected), runtime.GOOS)
	// Return success response
	return fmt.Sprintf("SQL statement executed successfully, affected rows: %d", rowsAffected), nil
}

// Close closes the database connection
func (kb *KnowledgeBaseSQLite) Close() {
	if kb.DB != nil {
		err := kb.DB.Close()
		if err != nil {
			log.Println("[ERROR] Problem closing SQLite database:", err)
			GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Problem closing SQLite database: %v", err), runtime.GOOS)
		} else {
			log.Println("[INFO] SQLite database connection closed successfully.")
			GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("SQLite database connection closed successfully."), runtime.GOOS)
		}
	}
}

func (db *KnowledgeBaseDB) AddDiscoveredPeer(peerID string) {
	db.peersMutex.Lock()
	defer db.peersMutex.Unlock()
	if db.discoveredPeers == nil {
		db.discoveredPeers = make(map[string]bool)
	}
	db.discoveredPeers[peerID] = true
}

func (db *KnowledgeBaseDB) GetDiscoveredPeers() []string {
	db.peersMutex.Lock()
	defer db.peersMutex.Unlock()
	var peers []string
	for p := range db.discoveredPeers {
		peers = append(peers, p)
	}
	return peers
}

type Event struct {
	Version   string      `json:"version"`   // e.g., "v1"
	Type      string      `json:"type"`      // "upload","vote","election","heartbeat","log","startup"
	Timestamp time.Time   `json:"timestamp"` // UTC
	NodeID    string      `json:"node_id"`   // libp2p host id
	Payload   interface{} `json:"payload"`   // arbitrary
}

func (db *KnowledgeBaseDB) publishEvent(ev Event) {
	if db == nil || db.MQEMS == nil {
		return
	}
	if ev.Version == "" {
		ev.Version = "v1"
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}
	if ev.NodeID == "" {
		ev.NodeID = db.HostID
	}

	b, err := json.Marshal(ev)
	if err != nil {
		GlobalLoggerDB.AddToOptimusLog("ERROR", "MQ publish marshal failed: "+err.Error(), "mq")
		return
	}

	// Use the default topic set when you created the client (cfg.Topic)
	_ = db.MQEMS.PublishJSON("", b)
}

// createEMSEventsTable ensures the `ems_events` table exists.
func (kb *LoggerSQLite) createEMSEventsTable() error {
	table := `
	CREATE TABLE IF NOT EXISTS ems_events (
		id            INTEGER PRIMARY KEY AUTOINCREMENT,
		received_at   TEXT,          -- UTC RFC3339
		node_id       TEXT,          -- libp2p host id
		client_id     TEXT,          -- MQ_CLIENT_ID (or fallback)
		topic         TEXT,          -- destination topic
		action        TEXT,          -- parsed from payload
		resource      TEXT,          -- parsed from payload
		params_json   TEXT,          -- marshaled params (if parsed)
		raw_json      TEXT           -- original message body
	);
	CREATE INDEX IF NOT EXISTS idx_ems_events_time ON ems_events(received_at);
	CREATE INDEX IF NOT EXISTS idx_ems_events_act_res ON ems_events(action, resource);
	`
	_, err := kb.theLog.Exec(table)
	return err
}

// Insert Helper
func (kb *LoggerSQLite) InsertEMSEvent(
	receivedAt time.Time,
	nodeID, clientID, topic, action, resource, paramsJSON, rawJSON string,
) error {
	const q = `
	INSERT INTO ems_events (received_at, node_id, client_id, topic, action, resource, params_json, raw_json)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?);`
	_, err := kb.theLog.Exec(q,
		receivedAt.UTC().Format(time.RFC3339),
		nodeID, clientID, topic, action, resource, paramsJSON, rawJSON,
	)
	return err
}

// Run a SELECT against the logger DB (optimuslog.db) and return []map[string]interface{}.
func (kb *LoggerSQLite) SelectAll(stmt string) ([]map[string]interface{}, error) {
	if kb == nil || kb.theLog == nil {
		return nil, errors.New("logger DB not initialized")
	}
	rows, err := kb.theLog.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var out []map[string]interface{}
	vals := make([]interface{}, len(cols))
	ptrs := make([]interface{}, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}

	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		row := make(map[string]interface{}, len(cols))
		for i, c := range cols {
			switch v := vals[i].(type) {
			case []byte:
				row[c] = string(v)
			default:
				row[c] = v
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
