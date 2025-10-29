# OptimusDB SQL Engine

## Overview

OptimusDB includes a complete SQL database engine implementation with parsing, query execution, storage management, and transaction support. This document covers the SQL engine architecture, components, and usage.

## Directory Structure

```
sql/
├── cmd/
│   ├── console/          # Interactive SQL console
│   └── csvimport/        # CSV import utility
├── debuger/              # SQL debugging and optimization tools
├── engine/               # Query execution engine
├── sql/                  # SQL parser and lexer
├── storage/              # Storage layer (B-Tree, WAL, paging)
└── usage/                # SQL engine integration
```

## Architecture

### Layered Architecture

```
┌─────────────────────────────────┐
│    Application Layer            │
│  (HTTP API, Shell, EMS)         │
└─────────────────────────────────┘
↓
┌─────────────────────────────────┐
│    SQL Engine Layer             │
│  (Parser, Optimizer, Executor)  │
└─────────────────────────────────┘
↓
┌─────────────────────────────────┐
│    Storage Layer                │
│  (B-Tree, WAL, Page Manager)    │
└─────────────────────────────────┘
↓
┌─────────────────────────────────┐
│    File System                  │
└─────────────────────────────────┘
```

## 1. SQL Parser (sql/)

### Parser (parser.go)

Converts SQL statements into abstract syntax trees (AST).

#### Supported SQL Statements

**DDL (Data Definition Language)**:
- `CREATE DATABASE`
- `CREATE TABLE`
- `DROP TABLE` (partial)
- `SHOW DATABASES`

**DML (Data Manipulation Language)**:
- `SELECT`
- `INSERT`
- `UPDATE`
- `DELETE`

**DCL (Data Control Language)**:
- `USE DATABASE`

### Parser Components

#### Lexer/Scanner (scanner.go, go_scanner.go)

Tokenizes SQL input into lexical tokens.

**Token Types**:
```go
const (
KEYWORD
IDENTIFIER
NUMBER
STRING
OPERATOR
DELIMITER
WHITESPACE
COMMENT
EOF
)
```

**Example Tokenization**:
```sql
SELECT name, age FROM users WHERE age > 25
```
Tokens:
```
[KEYWORD:SELECT] [IDENTIFIER:name] [DELIMITER:,] [IDENTIFIER:age]
[KEYWORD:FROM] [IDENTIFIER:users] [KEYWORD:WHERE] [IDENTIFIER:age]
[OPERATOR:>] [NUMBER:25]
```

#### AST Structures

```go
type Select struct {
SelectList      []SelectItem
TableExpression TableExpression
GroupByClause   []Identifier
SortSpecificationList []SortSpecification
LimitOffsetClause LimitOffsetClause
}

type CreateTable struct {
Name    string
Columns []ColumnDefinition
}

type InsertStatement struct {
TableName string
Columns   []string
Values    [][]interface{}
}

type UpdateStatementSearched struct {
TableName string
SetClause []SetClauseItem
WhereClause WhereClause
}

type DeleteStatementSearched struct {
TableName string
WhereClause WhereClause
}
```

### Example Parsing

```go
import "github.com/mk6i/mkdb/sql"

// Parse SQL
stmt, err := sql.Parse("SELECT * FROM users WHERE age > 25")

// Access parsed structure
selectStmt := stmt.(sql.Select)
fmt.Println(selectStmt.SelectList)
fmt.Println(selectStmt.TableExpression.FromClause)
```

## 2. Query Execution Engine (engine/)

### Session Management (session.go)

```go
type Session struct {
CurDB           string
RelationService *storage.RelationService
}
```

**Lifecycle**:
1. Create session
2. Select database with `USE`
3. Execute queries
4. Close session

**Example**:
```go
session := &Session{}
session.ExecQuery("USE mydb")
session.ExecQuery("SELECT * FROM users")
session.Close()
```

### Query Executors

#### SELECT Executor (select.go)

Implements the SELECT statement execution pipeline.

**Execution Pipeline**:
1. **FROM Clause**: Load tables via nested loop join
2. **WHERE Clause**: Filter rows
3. **SELECT List**: Project columns
4. **GROUP BY**: Aggregate rows
5. **ORDER BY**: Sort results
6. **LIMIT/OFFSET**: Pagination

**Join Strategies**:
- Nested Loop Join
- Hash Join (planned)
- Merge Join (planned)

**Join Types**:
- INNER JOIN
- LEFT OUTER JOIN
- RIGHT OUTER JOIN
- FULL OUTER JOIN
- CROSS JOIN

**Example**:
```go
func EvaluateSelect(q sql.Select, rm RelationManager)
([]*storage.Row, []*storage.Field, error) {

// Start transaction
rm.StartTxn()
defer rm.EndTxn()

// Execute FROM clause
rows, fields := nestedLoopJoin(rm, q.TableExpression.FromClause)

// Execute WHERE clause
rows = filterRows(q.TableExpression.WhereClause, fields, rows)

// Project columns
fields = projectColumns(q.SelectList, fields, rows)

// Aggregate
rows = aggregateRows(q.SelectList, q.GroupByClause, rows)

// Sort
sortColumns(q.SortSpecificationList, fields, rows)

// Pagination
rows = offset(q.LimitOffsetClause.Offset, rows)
rows = limit(q.LimitOffsetClause.Limit, rows)

return rows, fields, nil
}
```

#### INSERT Executor (insert.go)

```go
func EvaluateInsert(stmt sql.InsertStatement, rm RelationManager) (int, error) {
rm.StartTxn()
defer rm.EndTxn()

count := 0
batch := storage.WALBatch{}

for _, valRow := range stmt.Values {
walEntry, err := rm.Insert(stmt.TableName, stmt.Columns, valRow)
if err != nil {
return count, err
}
batch = append(batch, walEntry...)
count++
}

// Flush WAL batch
return count, rm.FlushWALBatch(batch)
}
```

#### UPDATE Executor (update.go)

```go
func EvaluateUpdate(stmt sql.UpdateStatementSearched, rm RelationManager) error {
rm.StartTxn()
defer rm.EndTxn()

// Fetch all rows
rows, fields, err := rm.Fetch(stmt.TableName)

// Filter rows matching WHERE clause
rowsToUpdate := filterRows(stmt.WhereClause, fields, rows)

// Update each row
batch := storage.WALBatch{}
for _, row := range rowsToUpdate {
walEntry, err := rm.Update(
stmt.TableName,
row.ID,
stmt.SetClause.Columns,
stmt.SetClause.Values,
)
batch = append(batch, walEntry...)
}

return rm.FlushWALBatch(batch)
}
```

#### DELETE Executor (delete.go)

```go
func EvaluateDelete(stmt sql.DeleteStatementSearched, rm RelationManager) (int, error) {
rm.StartTxn()
defer rm.EndTxn()

// Fetch all rows
rows, fields, err := rm.Fetch(stmt.TableName)

// Filter rows to delete
rowsToDelete := filterRows(stmt.WhereClause, fields, rows)

// Mark each row as deleted
batch := storage.WALBatch{}
count := 0
for _, row := range rowsToDelete {
walEntry, err := rm.MarkDeleted(stmt.TableName, row.ID)
batch = append(batch, walEntry...)
count++
}

return count, rm.FlushWALBatch(batch)
}
```

#### CREATE Executor (create.go)

```go
func EvaluateCreateTable(stmt sql.CreateTable, rm RelationManager) error {
// Convert column definitions to storage fields
fields := make([]*storage.Field, len(stmt.Columns))
for i, col := range stmt.Columns {
fields[i] = &storage.Field{
Name: col.Name,
Type: convertType(col.Type),
}
}

// Create relation
relation := storage.NewRelation(stmt.Name, fields)
return rm.CreateTable(relation, stmt.Name)
}
```

#### SHOW Executor (show.go)

```go
func EvaluateShowDatabase(stmt sql.ShowDatabase)
([]*storage.Row, []*storage.Field, error) {

// List database directories
databases := listDatabases()

// Format as result set
fields := []*storage.Field{
{Name: "Database", Type: storage.TypeText},
}

rows := make([]*storage.Row, len(databases))
for i, db := range databases {
rows[i] = &storage.Row{
Data: []interface{}{db},
}
}

return rows, fields, nil
}
```

## 3. Storage Layer (storage/)

### B-Tree Index (btree.go)

Implements a B-Tree for fast key-based lookups.

**Structure**:
```go
type BTree struct {
store
rootOffset uint64
}

type btreeNode struct {
isLeaf       bool
cells        []btreeCell
rightOffset  uint64  // for internal nodes
fileOffset   uint64
}

type btreeCell struct {
key        uint32
fileOffset uint64  // for internal nodes
lsn        uint64  // for leaf nodes
value      []byte  // for leaf nodes
}
```

**Operations**:
- `Insert(key, value)`: O(log n)
- `Search(key)`: O(log n)
- `Delete(key)`: O(log n)
- `Scan(startKey, endKey)`: O(k + log n)

**Node Splitting**:
When a node exceeds capacity:
1. Create new node
2. Move half the entries to new node
3. Update parent with separator key
4. If parent full, split recursively

**Example**:
```go
btree := &BTree{store: myStore}

// Insert
key, lsn, err := btree.insert([]byte("value"))

// Search
value, err := btree.search(key)

// Scan range
btree.scan(minKey, maxKey, func(key uint32, value []byte) ScanAction {
fmt.Printf("%d: %s\n", key, value)
return KeepScanning
})
```

### Page Management (page.go)

Manages fixed-size pages for efficient disk I/O.

**Page Structure**:
```go
const (
PageSize = 4096  // 4KB pages
)

type Page struct {
Number   uint32
Data     [PageSize]byte
Dirty    bool
PinCount int
}
```

**Page Cache**:
- LRU eviction policy
- Write-through or write-back caching
- Pin/unpin mechanism
- Dirty page tracking

**Example**:
```go
// Read page
page, err := pageManager.ReadPage(pageNum)

// Modify page
copy(page.Data[:], newData)
page.Dirty = true

// Write page
err = pageManager.WritePage(page)
```

### Write-Ahead Logging (wal.go)

Ensures durability and atomicity through write-ahead logging.

**WAL Structure**:
```go
type WALEntry struct {
LSN       uint64      // Log Sequence Number
TxnID     uint64      // Transaction ID
Operation string      // "INSERT", "UPDATE", "DELETE"
TableName string
RowID     uint32
OldValue  []byte
NewValue  []byte
}

type WALBatch []WALEntry
```

**WAL Protocol**:
1. Write operation to WAL
2. Flush WAL to disk
3. Apply operation to data pages
4. Mark pages dirty
5. Checkpoint periodically

**Recovery**:
1. Read WAL from last checkpoint
2. Replay logged operations
3. Restore database to consistent state

**Example**:
```go
// Start transaction
wal.BeginTxn(txnID)

// Log insert
entry := WALEntry{
LSN: nextLSN(),
TxnID: txnID,
Operation: "INSERT",
TableName: "users",
NewValue: rowData,
}
wal.LogEntry(entry)

// Commit
wal.CommitTxn(txnID)
wal.Flush()
```

### Relation Management (relation.go)

Manages table (relation) metadata and operations.

**Relation Structure**:
```go
type Relation struct {
Name   string
Fields []*Field
Index  *BTree
}

type Field struct {
Name     string
Type     FieldType
TableID  string
}

type FieldType int

const (
TypeInt     FieldType = iota
TypeFloat
TypeText
TypeBoolean
TypeBlob
)
```

**Operations**:
```go
// Create relation
relation := storage.NewRelation("users", fields)
err := relationService.CreateTable(relation, "users")

// Insert row
walEntry, err := relationService.Insert("users", cols, vals)

// Fetch rows
rows, fields, err := relationService.Fetch("users")

// Update row
walEntry, err := relationService.Update("users", rowID, cols, vals)

// Delete row
walEntry, err := relationService.MarkDeleted("users", rowID)
```

### LRU Cache (lru.go)

Implements Least Recently Used caching for pages.

**Structure**:
```go
type LRUCache struct {
capacity int
cache    map[uint32]*list.Element
list     *list.List
mu       sync.Mutex
}

type entry struct {
key   uint32
value *Page
}
```

**Operations**:
- `Get(key)`: O(1) lookup
- `Put(key, value)`: O(1) insertion
- `Evict()`: O(1) eviction of LRU item

**Example**:
```go
cache := NewLRUCache(100)  // Cache 100 pages

// Get page (moves to front)
page, found := cache.Get(pageNum)

// Put page
cache.Put(pageNum, page)

// Eviction happens automatically when full
```

### File Management (file.go)

Handles file I/O for database files.

**File Structure**:
```
database/
├── <table>.dat         # Data file
    ├── <table>.idx         # Index file
        ├── <table>.wal         # WAL file
            └── metadata            # Database metadata
            ```

            **Operations**:
            ```go
            // Open database
            db, err := storage.OpenRelation("mydb", createIfNotExists)

            // Read/Write pages
            page, err := db.ReadPage(pageNum)
            err = db.WritePage(page)

            // Close database
            err = db.Close()
            ```

            ## 4. SQL Debugger (debuger/)

            ### SQL Optimizer (sqlOptimusParser.go)

            Provides SQL parsing and optimization specifically for OrbitDB integration.

            **Features**:
            - SQL-to-OrbitDB translation
            - Query optimization hints
            - Explain plan generation

            **Example**:
            ```go
            translator := NewSQLToOrbitDB(orbitClient)

            // Execute SQL
            result, err := translator.ExecuteSQLOptimus(
            "SELECT * FROM kbdata WHERE type = 'sensor'",
            )
            ```

            ### Batching (batching.go)

            Optimizes multiple operations into batches.

            **Batch Types**:
            - Insert batching
            - Update batching
            - Delete batching

            **Example**:
            ```go
            batcher := NewBatcher(100)  // Batch size 100

            for _, row := range rows {
            batcher.Add(row)

            if batcher.IsFull() {
            batcher.Flush()
            }
            }

            batcher.Flush()  // Flush remaining
            ```

            ### Datastore Utilities (datastore.go)

            Helper functions for storage operations.

            **Functions**:
            - Schema validation
            - Data type conversion
            - Index management
            - Statistics collection

            ## 5. Command-Line Tools (cmd/)

            ### SQL Console (console/)

            Interactive SQL shell.

            **Usage**:
            ```bash
            cd sql/cmd/console
            go run main.go
            ```

            **Features**:
            - SQL statement execution
            - Result formatting
            - Command history
            - Tab completion
            - Multi-line statements

            **Commands**:
            ```sql
            > CREATE DATABASE mydb;
            > USE mydb;
            > CREATE TABLE users (id INT, name TEXT, age INT);
            > INSERT INTO users VALUES (1, 'Alice', 30);
            > SELECT * FROM users WHERE age > 25;
            > .exit
            ```

            ### CSV Import (csvimport/)

            Import CSV files into database.

            **Usage**:
            ```bash
            go run csvimport/main.go -db mydb -table users -file users.csv
            ```

            **Options**:
            - `-db`: Database name
            - `-table`: Table name
            - `-file`: CSV file path
            - `-create`: Create table if not exists
            - `-header`: CSV has header row

            **Example**:
            ```bash
            # Import with auto table creation
            go run csvimport/main.go -db mydb -table users -file users.csv -create -header

            # Import to existing table
            go run csvimport/main.go -db mydb -table users -file moreusers.csv
            ```

            ## 6. SQL Engine Integration (usage/)

            ### Integration with OptimusDB (sqlengine.go)

            Integrates SQL engine with OptimusDB application.

            **KnowledgeBaseSQLite**:
            ```go
            type KnowledgeBaseSQLite struct {
            db *sql.DB
            mu sync.Mutex
            }
            ```

            **Operations**:
            ```go
            // Initialize
            kbSQL := app.InitSQLiteKB("mydb")

            // Execute SQL
            rows, err := kbSQL.ExecuteSQL("SELECT * FROM users")

            // Execute DML
            affected, err := kbSQL.ExecuteDML("INSERT INTO users ...")

            // Close
            kbSQL.Close()
            ```

            ## SQL Examples

            ### Basic Queries

            ```sql
            -- Create database
            CREATE DATABASE mydb;
            USE mydb;

            -- Create table
            CREATE TABLE users (
            id INT,
            name TEXT,
            age INT,
            email TEXT
            );

            -- Insert data
            INSERT INTO users VALUES (1, 'Alice', 30, 'alice@example.com');
            INSERT INTO users VALUES (2, 'Bob', 25, 'bob@example.com');
            INSERT INTO users VALUES (3, 'Charlie', 35, 'charlie@example.com');

            -- Select all
            SELECT * FROM users;

            -- Select with WHERE
            SELECT name, age FROM users WHERE age > 25;

            -- Update
            UPDATE users SET age = 26 WHERE name = 'Bob';

            -- Delete
            DELETE FROM users WHERE age < 25;

            -- Order by
            SELECT * FROM users ORDER BY age DESC;

            -- Limit
            SELECT * FROM users ORDER BY age DESC LIMIT 2;

            -- Count
            SELECT COUNT(*) FROM users;

            -- Group by
            SELECT age, COUNT(*) FROM users GROUP BY age;
            ```

            ### Advanced Queries

            ```sql
            -- Create second table
            CREATE TABLE orders (
            id INT,
            user_id INT,
            product TEXT,
            amount INT
            );

            INSERT INTO orders VALUES (1, 1, 'Widget', 100);
            INSERT INTO orders VALUES (2, 1, 'Gadget', 150);
            INSERT INTO orders VALUES (3, 2, 'Tool', 200);

            -- Inner join
            SELECT u.name, o.product, o.amount
            FROM users u
            INNER JOIN orders o ON u.id = o.user_id;

            -- Left join
            SELECT u.name, o.product, o.amount
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id;

            -- Aggregation with join
            SELECT u.name, SUM(o.amount) as total
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            GROUP BY u.name;

            -- Having clause
            SELECT age, COUNT(*) as count
            FROM users
            GROUP BY age
            HAVING count > 1;
            ```

            ## Performance Tuning

            ### Indexing

            Currently uses automatic B-Tree indexing on primary keys. Future enhancements:
            - Secondary indexes
            - Composite indexes
            - Full-text indexes

            ### Query Optimization

            **Current Optimizations**:
            - Predicate pushdown
            - Early termination
            - Index usage when possible

            **Planned Optimizations**:
            - Join reordering
            - Subquery flattening
            - Common subexpression elimination
            - Statistics-based optimization

            ### Caching

            **Page Cache Tuning**:
            ```go
            // Adjust cache size
            cache := NewLRUCache(1000)  // Cache 1000 pages = 4MB

            // Monitor hit rate
            stats := cache.GetStats()
            fmt.Printf("Hit rate: %.2f%%\n", stats.HitRate * 100)
            ```

            **Query Result Caching**:
            - Cache frequently accessed queries
            - TTL-based invalidation
            - Automatic invalidation on writes

            ## Distributed SQL

            ### SQL over P2P

            OptimusDB supports distributed SQL execution:

            **Stream Protocol**:
            ```
            /optimusdb/sql/1.0.0
            ```

            **Request Format**:
            ```json
            {
            "type": "select",
            "database": "mydb",
            "sql": "SELECT * FROM users WHERE age > 25"
            }
            ```

            **Response Format**:
            ```json
            {
            "success": true,
            "rows": [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 3, "name": "Charlie", "age": 35}
            ],
            "fields": ["id", "name", "age"],
            "row_count": 2,
            "execution_time_ms": 15
            }
            ```

            ### Multi-Peer Queries

            Execute SQL across multiple peers:

            ```go
            // Query all peers
            results := []SQLResult{}
            for _, peer := range peers {
            result, err := executeSQLOnPeer(peer, sqlStmt)
            results = append(results, result)
            }

            // Merge results
            mergedRows := mergeResults(results)
            ```

            ## Testing

            ### Unit Tests

            Run all SQL engine tests:
            ```bash
            cd sql
            go test ./...
            ```

            **Test Coverage**:
            - Parser tests
            - Executor tests
            - Storage tests
            - Integration tests

            ### Test Data

            Create test data:
            ```sql
            CREATE DATABASE testdb;
            USE testdb;

            CREATE TABLE test (
            id INT,
            value TEXT
            );

            INSERT INTO test VALUES (1, 'test1');
            INSERT INTO test VALUES (2, 'test2');
            INSERT INTO test VALUES (3, 'test3');

            SELECT * FROM test;
            ```

            ## Limitations

            **Current Limitations**:
            1. No transactions across multiple tables
            2. Limited index types (B-Tree only)
            3. No views or stored procedures
            4. Basic query optimizer
            5. No foreign key constraints
            6. Limited aggregate functions
            7. No subqueries in WHERE clause

            **Planned Features**:
            1. Multi-table transactions
            2. Secondary indexes
            3. View support
            4. Advanced query optimizer
            5. Foreign keys and constraints
            6. More aggregate functions
            7. Subquery support

            ## Troubleshooting

            ### Common Issues

            **1. Table Not Found**
            ```
            Error: table 'users' not found
            ```
            Solution: Check database selection with `USE database`

            **2. Parse Error**
            ```
            Error: unable to parse SQL: unexpected token at position 15
            ```
            Solution: Check SQL syntax, ensure proper quotes

            **3. Type Mismatch**
            ```
            Error: type mismatch in comparison
            ```
            Solution: Ensure types match in WHERE conditions

            **4. Lock Timeout**
            ```
            Error: lock timeout while acquiring table lock
            ```
            Solution: Close previous transactions, check for deadlocks

            ## Best Practices

            ### 1. Schema Design
            - Use appropriate data types
            - Normalize when possible
            - Consider query patterns
            - Plan for growth

            ### 2. Query Writing
            - Use explicit column names
            - Add indexes for frequent lookups
            - Limit result sets when possible
            - Avoid SELECT *

            ### 3. Performance
            - Batch inserts when possible
            - Use transactions for multiple operations
            - Monitor query execution times
            - Optimize slow queries

            ### 4. Maintenance
            - Regular checkpoints
            - Periodic WAL truncation
            - Monitor disk space
            - Backup databases regularly

            ## Future Development

            **Roadmap**:
            1. ✅ Basic SQL parser
            2. ✅ B-Tree indexing
            3. ✅ WAL implementation
            4. ✅ Transaction support
            5. ⏳ Secondary indexes
            6. ⏳ Query optimizer improvements
            7. ⏳ View support
            8. ⏳ Stored procedures
            9. ⏳ Advanced joins (hash, merge)
            10. ⏳ Full-text search

            ## Related Documentation

            - See `01_README_CORE_APP.md` for application integration
            - See `02_README_API.md` for SQL API endpoints
            - See `04_README_QUERY_ENGINE.md` for distributed queries
            - See `/docs/OptimusDB_Technical_DeepDive.md` for architecture

            ## References

            - SQLite Documentation: https://www.sqlite.org/docs.html
            - PostgreSQL Documentation: https://www.postgresql.org/docs/
            - B-Tree Implementation: Knuth, TAOCP Vol. 3
            - Database Systems: The Complete Book (Garcia-Molina et al.)