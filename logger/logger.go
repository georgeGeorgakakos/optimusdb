package logger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"optimusdb/config"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// LogLevel represents different logging severity levels
type LogLevel int

const (
	DEBUG       LogLevel = iota // Most verbose - development debugging
	INFO                        // General information
	QUERY                       // SQL query operations
	LINEAGE                     // Data lineage tracking
	MESH                        // LibP2P mesh network operations
	REPLICATION                 // OrbitDB replication events
	ELECTION                    // Leader election events
	CACHE                       // SQLite cache operations
	gAI                         // TinyLlama AI operations
	METRICS                     // Performance metrics
	PROC                        // General processing
	WARN                        // Warning conditions
	ERROR                       // Error conditions
	DISCOVERY                   //Discovery related logs
)

var (
	logMutex sync.RWMutex // Protects all logging operations
	lokiMux  sync.Mutex   // Separate mutex for Loki operations
)

// GlobalLogger instance accessible throughout the app
var GlobalLogger *Logger

func init() {
	lokiURL := os.Getenv("LOKI_URL")
	logFilename := "./logs/optimusdb.log"
	if config.FlagLogFilename != nil && *config.FlagLogFilename != "" {
		logFilename = *config.FlagLogFilename
	}
	GlobalLogger = NewLogger(levelFromEnv(INFO), logFilename, lokiURL)
}

// levelFromEnv reads OPTIMUSDB_LOG_LEVEL (DEBUG, INFO, WARN, ERROR); anything else keeps the default.
// Every log line is also written to the SQLite log table, so WARN reduces per-request overhead.
func levelFromEnv(def LogLevel) LogLevel {
	switch strings.ToUpper(strings.TrimSpace(os.Getenv("OPTIMUSDB_LOG_LEVEL"))) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	}
	return def
}

// Logger is a custom logger with different log levels
type Logger struct {
	level      LogLevel
	logFile    *os.File
	lokiURL    string
	db         LoggerDBInterface
	lokiBuffer chan *lokiLogEntry
	wg         sync.WaitGroup
	shutdown   chan struct{}

	// Database persistence runs in the background: Log() only queues the row, a single writer goroutine stores
	// the queued rows in batches (one transaction per batch when the database supports it). Previously every
	// log line was inserted synchronously, in its own transaction, while holding the global log lock, so all
	// request handlers waited on SQLite. When the queue is full, rows are dropped and counted, never blocking.
	dbQueue   chan LogRow
	dbOnce    sync.Once
	dbDropped uint64
}

// LogRow is one log line queued for database persistence.
type LogRow struct {
	Level, Message, Source string
	At                     time.Time
}

// LoggerBatchDB is implemented by databases that can store many log rows in one transaction.
type LoggerBatchDB interface {
	AddToOptimusLogBatch(rows []LogRow) error
}

// lokiLogEntry represents a queued log for async Loki sending
type lokiLogEntry struct {
	level   string
	message string
}

// LoggerDBInterface allows injecting the database logger
type LoggerDBInterface interface {
	AddToOptimusLog(level, message, source string) error
}

// NewLogger initializes a new logger instance with file, database & Loki support
func NewLogger(level LogLevel, logFilePath, lokiURL string) *Logger {
	// Create logs directory if it doesn't exist
	logDir := filepath.Dir(logFilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		log.Fatalf("Failed to create logs directory: %v", err)
	}

	// Open log file
	logFile, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}

	// Configure standard logger
	log.SetOutput(logFile)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	logger := &Logger{
		level:      level,
		logFile:    logFile,
		lokiURL:    lokiURL,
		lokiBuffer: make(chan *lokiLogEntry, 10000), // Buffer up to 10000 logs
		shutdown:   make(chan struct{}),
	}

	// Start async Loki sender if URL is configured
	if lokiURL != "" {
		logger.wg.Add(1)
		go logger.lokiWorker()
		log.Printf("[INFO] Loki integration enabled: %s\n", lokiURL)
	} else {
		log.Printf("[INFO] Loki integration disabled (no URL configured)\n")
	}

	return logger
}

// SetDatabase sets the database logger for persistence
func (l *Logger) SetDatabase(db LoggerDBInterface) {
	logMutex.Lock()
	l.db = db
	logMutex.Unlock()
	l.dbOnce.Do(func() {
		l.dbQueue = make(chan LogRow, 20000)
		l.wg.Add(1)
		go l.dbWriter()
	})
}

// dbWriter stores queued log rows in batches of up to 500 rows or every 200 ms.
func (l *Logger) dbWriter() {
	defer l.wg.Done()
	batch := make([]LogRow, 0, 500)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	flush := func() {
		if len(batch) == 0 {
			return
		}
		logMutex.RLock()
		db := l.db
		logMutex.RUnlock()
		if db != nil {
			if bdb, ok := db.(LoggerBatchDB); ok {
				if err := bdb.AddToOptimusLogBatch(batch); err != nil {
					log.Printf("[ERROR] Failed to persist %d log rows: %v\n", len(batch), err)
				}
			} else {
				for _, r := range batch {
					if err := db.AddToOptimusLog(r.Level, r.Message, r.Source); err != nil {
						log.Printf("[ERROR] Failed to persist log to database: %v\n", err)
						break
					}
				}
			}
		}
		batch = batch[:0]
	}
	for {
		select {
		case r := <-l.dbQueue:
			batch = append(batch, r)
			if len(batch) >= 500 {
				flush()
			}
		case <-tick.C:
			flush()
		case <-l.shutdown:
			for {
				select {
				case r := <-l.dbQueue:
					batch = append(batch, r)
				default:
					flush()
					return
				}
			}
		}
	}
}

// SetLevel changes the current log level
func (l *Logger) SetLevel(level LogLevel) {
	logMutex.Lock()
	defer logMutex.Unlock()
	l.level = level
}

// levelToString converts LogLevel to string
func levelToString(level LogLevel) string {
	switch level {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case QUERY:
		return "QUERY"
	case LINEAGE:
		return "LINEAGE"
	case MESH:
		return "MESH"
	case REPLICATION:
		return "REPLICATION"
	case ELECTION:
		return "ELECTION"
	case CACHE:
		return "CACHE"
	case gAI:
		return "gAI"
	case METRICS:
		return "METRICS"
	case PROC:
		return "PROC"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case DISCOVERY:
		return "DISCOVERY"
	default:
		return "LOG"
	}
}

// Log writes a log message based on the log level (thread-safe)
func (l *Logger) Log(level LogLevel, message string, args ...interface{}) {
	// Quick check without lock
	if level < l.level {
		return
	}

	// Get caller information before acquiring lock (reduces lock contention)
	_, file, line, ok := runtime.Caller(2)
	var source string
	if ok {
		source = fmt.Sprintf("%s:%d", filepath.Base(file), line)
	} else {
		source = "unknown"
	}

	// Format message
	formattedMessage := fmt.Sprintf(message, args...)
	prefix := levelToString(level)
	fullMessage := fmt.Sprintf("[%s] %s", prefix, formattedMessage)

	// Write to file (the standard logger serialises its own writes; no global lock is held here)
	log.Print(fullMessage)

	// Queue for Loki (non-blocking) — skip entirely if Loki is not configured
	// to prevent buffer overflow from high-frequency election/mesh log lines
	if l.lokiURL != "" {
		select {
		case l.lokiBuffer <- &lokiLogEntry{level: prefix, message: fullMessage}:
			// Successfully queued
		default:
			// Buffer full, drop the log (avoid blocking)
			log.Printf("[WARN] Loki buffer full, dropping log entry\n")
		}
	}

	// Queue for database persistence (non-blocking; see dbWriter)
	if l.dbQueue != nil {
		select {
		case l.dbQueue <- LogRow{Level: prefix, Message: formattedMessage, Source: source, At: time.Now()}:
		default:
			if n := atomic.AddUint64(&l.dbDropped, 1); n == 1 || n%10000 == 0 {
				log.Printf("[WARN] log database queue full: %d rows not persisted so far\n", n)
			}
		}
	}
}

// lokiWorker processes the Loki queue asynchronously
func (l *Logger) lokiWorker() {
	defer l.wg.Done()

	for {
		select {
		case entry := <-l.lokiBuffer:
			l.sendToLoki(entry.level, entry.message)
		case <-l.shutdown:
			// Drain remaining logs
			for {
				select {
				case entry := <-l.lokiBuffer:
					l.sendToLoki(entry.level, entry.message)
				default:
					return
				}
			}
		}
	}
}

// escapeLogMessage sanitizes log messages for JSON
func escapeLogMessage(message string) string {
	message = strings.ReplaceAll(message, "\n", " ")
	message = strings.ReplaceAll(message, "\t", " ")
	message = strings.ReplaceAll(message, "\r", " ")
	return message
}

// sendToLoki sends a log entry to Loki (with retries)
func (l *Logger) sendToLoki(level, message string) {
	if l.lokiURL == "" {
		return
	}

	if config.FlagLokiIsDisabled != nil && *config.FlagLokiIsDisabled {
		return
	}

	escapedMessage := escapeLogMessage(message)

	logEntry := map[string]interface{}{
		"streams": []map[string]interface{}{
			{
				"stream": map[string]string{
					"job":    "optimusdb",
					"level":  level,
					"source": "optimusdb-cluster",
				},
				"values": [][]string{
					{
						fmt.Sprintf("%d", time.Now().UnixNano()),
						escapedMessage,
					},
				},
			},
		},
	}

	jsonData, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal Loki log entry: %v\n", err)
		return
	}

	// Retry logic with exponential backoff
	for attempt := 1; attempt <= 3; attempt++ {
		req, err := http.NewRequest("POST", l.lokiURL, bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[ERROR] Failed to create Loki request: %v\n", err)
			return
		}

		req.Header.Set("Content-Type", "application/json")

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)

		if err != nil {
			log.Printf("[WARN] Failed to send log to Loki (attempt %d/3): %v\n", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}

		// Always close response body
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return // Success
		}

		log.Printf("[WARN] Loki returned status %d (attempt %d/3): %s\n", resp.StatusCode, attempt, string(body))
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	log.Printf("[ERROR] Failed to send log to Loki after 3 attempts\n")
}

// CloseLogger gracefully shuts down the logger
func (l *Logger) CloseLogger() {
	// Signal shutdown to Loki worker
	close(l.shutdown)

	// Wait for Loki worker to finish
	l.wg.Wait()

	// Close log file
	logMutex.Lock()
	defer logMutex.Unlock()

	if l.logFile != nil {
		_ = l.logFile.Close()
	}

	log.Printf("[INFO] Logger closed gracefully\n")
}

// SetGlobalDatabase sets the database for the global logger
func SetGlobalDatabase(db LoggerDBInterface) {
	if GlobalLogger != nil {
		GlobalLogger.SetDatabase(db)
	}
}

// SetGlobalLevel changes the global logger level
func SetGlobalLevel(level LogLevel) {
	if GlobalLogger != nil {
		GlobalLogger.SetLevel(level)
	}
}

// Convenience logging functions

func Debug(format string, args ...interface{}) {
	GlobalLogger.Log(DEBUG, format, args...)
}

func Info(format string, args ...interface{}) {
	GlobalLogger.Log(INFO, format, args...)
}

func Query(format string, args ...interface{}) {
	GlobalLogger.Log(QUERY, format, args...)
}

func Lineage(format string, args ...interface{}) {
	GlobalLogger.Log(LINEAGE, format, args...)
}

func Mesh(format string, args ...interface{}) {
	GlobalLogger.Log(MESH, format, args...)
}

func Replication(format string, args ...interface{}) {
	GlobalLogger.Log(REPLICATION, format, args...)
}

func Election(format string, args ...interface{}) {
	GlobalLogger.Log(ELECTION, format, args...)
}

func Cache(format string, args ...interface{}) {
	GlobalLogger.Log(CACHE, format, args...)
}

func AI(format string, args ...interface{}) {
	GlobalLogger.Log(gAI, format, args...)
}

func Metrics(format string, args ...interface{}) {
	GlobalLogger.Log(METRICS, format, args...)
}

func Proc(format string, args ...interface{}) {
	GlobalLogger.Log(PROC, format, args...)
}

func DISc(format string, args ...interface{}) {
	GlobalLogger.Log(DISCOVERY, format, args...)
}

func Warn(format string, args ...interface{}) {
	GlobalLogger.Log(WARN, format, args...)
}

func Error(format string, args ...interface{}) {
	GlobalLogger.Log(ERROR, format, args...)
}

// CheckAndLogError logs the error if it is not nil
func CheckAndLogError(err error, message string, args ...interface{}) {
	if err != nil {
		Error("%s: %v", fmt.Sprintf(message, args...), err)
	}
}

// LogWithContext logs with additional context fields (useful for lineage tracking)
func LogWithContext(level LogLevel, message string, context map[string]interface{}) {
	contextStr := ""
	if len(context) > 0 {
		parts := make([]string, 0, len(context))
		for k, v := range context {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
		contextStr = fmt.Sprintf(" | %s", strings.Join(parts, ", "))
	}
	GlobalLogger.Log(level, "%s%s", message, contextStr)
}

// LogLineage is a specialized function for lineage events
func LogLineage(sourceURI, targetURI, operation string, metadata map[string]interface{}) {
	context := map[string]interface{}{
		"source":    sourceURI,
		"target":    targetURI,
		"operation": operation,
	}
	for k, v := range metadata {
		context[k] = v
	}
	LogWithContext(LINEAGE, fmt.Sprintf("Lineage: %s -> %s", sourceURI, targetURI), context)
}
