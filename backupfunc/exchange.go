// Package backupfunc implements export and import of the full OptimusDB
// node state via REST.
//
// Scope: SQLite (consistent VACUUM INTO snapshot, including sqlite-vec
// shadow tables) and every non-nil *orbitdb.DocumentStore on
// app.KnowledgeBaseDB. The Contributions event log is deliberately not
// exported — go-orbit-db's EventLogStore is append-only with no _id
// addressing, so replaying it doc-by-doc breaks its hash chain.
//
// Route wiring lives in api/http.go. That file constructs two thin
// handlers that look up kb.ExchangeService at request time and call
// HandleExport / HandleImport on it via interface assertion. This keeps
// the api package free of any dependency on backupfunc, and keeps
// backupfunc's import of optimusdb/app one-way (no cycle).
//
// Archive layout (inside the returned .tar.gz):
//
//	manifest.json                  archive version, node ID, store list
//	sqlite/optimusdb.db            output of VACUUM INTO
//	orbitdb/<store>.jsonl          one file per exported docstore
package backupfunc

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"

	"optimusdb/app"
)

// ──────────────────────────────────────────────────────────────────────
// Service
// ──────────────────────────────────────────────────────────────────────

// Service is constructed once in main() and stashed on
// KnowledgeBaseDB.ExchangeService. Do NOT construct per request — the
// internal mutex is what keeps concurrent export/import safe.
type Service struct {
	KB     *app.KnowledgeBaseDB
	SQLite *app.KnowledgeBaseSQLite

	// mu serialises export and import. VACUUM INTO into a temp file is
	// safe while readers are active, but two concurrent imports would
	// both try to replace the same DB file, and a concurrent export +
	// import would race on the sqlite handle.
	mu sync.Mutex
}

// New builds a ready-to-use service. The DB path is read from
// sqlite.DBPath (which you populate inside app.InitSQLite), so the
// caller doesn't have to reconstruct the $HOME/.cache/... path here.
func New(kb *app.KnowledgeBaseDB, sqlite *app.KnowledgeBaseSQLite) *Service {
	return &Service{KB: kb, SQLite: sqlite}
}

// ──────────────────────────────────────────────────────────────────────
// Manifest
// ──────────────────────────────────────────────────────────────────────

// Manifest lives at the top of every archive. The import path reads it
// first to decide which stores to apply.
type Manifest struct {
	Version    string    `json:"version"` // "1"
	CreatedAt  time.Time `json:"created_at"`
	SourceNode string    `json:"source_node"`
	Stores     []string  `json:"stores"`
	HasSQLite  bool      `json:"has_sqlite"`
}

// ──────────────────────────────────────────────────────────────────────
// Store registry
// ──────────────────────────────────────────────────────────────────────

// storeRegistry returns every non-nil *orbitdb.DocumentStore on the KB,
// keyed by the same lowercase names your /command API already uses.
// Stores absent from the KB (because that subsystem isn't initialised on
// this node) are silently skipped. Export is best-effort over whatever
// is actually live.
func (s *Service) storeRegistry() map[string]*orbitdb.DocumentStore {
	kb := s.KB
	m := map[string]*orbitdb.DocumentStore{}
	add := func(name string, ptr *orbitdb.DocumentStore) {
		if ptr != nil {
			m[name] = ptr
		}
	}
	add("validations", kb.Validations)
	add("kbdata", kb.KBdata)
	add("kbmetadata", kb.KBMetadata)
	add("whoiswho", kb.WhoiswhoStore)
	add("dsswres", kb.DsSWres)
	add("dsswresaloc", kb.DsSWresaloc)
	add("tosca_adt", kb.DsTOSCA_ADT)
	add("tosca_imported", kb.DsTOSCA_Imported)
	add("tosca_capacities", kb.DsTOSCA_Capacities)
	add("tosca_deployment_plan", kb.DsTOSCA_DeploymentPlan)
	add("tosca_event_history", kb.DsTOSCA_EventHistory)
	return m
}

// ──────────────────────────────────────────────────────────────────────
// Export
// ──────────────────────────────────────────────────────────────────────

// Export streams a tar.gz of full node state into w. Manifest first so
// consumers can decide how to proceed without buffering the archive.
func (s *Service) Export(ctx context.Context, w io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	registry := s.storeRegistry()
	storeNames := make([]string, 0, len(registry))
	for name := range registry {
		storeNames = append(storeNames, name)
	}

	// SQLite snapshot into a temp file. We need its size up front for
	// the tar header, so this happens before we open the stream.
	sqliteSnapshot, cleanup, err := s.vacuumIntoTemp(ctx)
	if err != nil {
		return fmt.Errorf("sqlite snapshot: %w", err)
	}
	defer cleanup()

	gz := gzip.NewWriter(w)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	// 1) manifest.json
	manifest := Manifest{
		Version:    "1",
		CreatedAt:  time.Now().UTC(),
		SourceNode: s.KB.HostID,
		Stores:     storeNames,
		HasSQLite:  true,
	}
	manifestBytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if err := writeTarFile(tw, "manifest.json", manifestBytes); err != nil {
		return err
	}

	// 2) sqlite/optimusdb.db — streamed from the snapshot file
	if err := writeTarFromFile(tw, "sqlite/optimusdb.db", sqliteSnapshot); err != nil {
		return fmt.Errorf("tar sqlite: %w", err)
	}

	// 3) orbitdb/<store>.jsonl
	for _, name := range storeNames {
		docs, err := queryAllDocs(ctx, *registry[name])
		if err != nil {
			return fmt.Errorf("query %s: %w", name, err)
		}
		buf, err := docsToJSONL(docs)
		if err != nil {
			return fmt.Errorf("jsonl %s: %w", name, err)
		}
		if err := writeTarFile(tw, "orbitdb/"+name+".jsonl", buf); err != nil {
			return err
		}
	}

	return nil
}

// vacuumIntoTemp runs VACUUM INTO against a fresh file in os.TempDir.
// VACUUM INTO is safe with active readers/writers — SQLite takes a read
// lock for the copy, the destination is a fresh file, and the WAL is
// checkpointed into it. sqlite-vec shadow tables come along for free.
func (s *Service) vacuumIntoTemp(ctx context.Context) (string, func(), error) {
	if s.SQLite == nil || s.SQLite.DB == nil {
		return "", func() {}, errors.New("sqlite handle is nil")
	}
	tmp, err := os.CreateTemp("", "optimusdb-export-*.db")
	if err != nil {
		return "", func() {}, err
	}
	path := tmp.Name()
	_ = tmp.Close()
	// VACUUM INTO requires the file NOT to exist; remove it first.
	_ = os.Remove(path)

	safe := strings.ReplaceAll(path, "'", "''")
	if _, err := s.SQLite.DB.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", safe)); err != nil {
		return "", func() {}, fmt.Errorf("VACUUM INTO: %w", err)
	}
	return path, func() { _ = os.Remove(path) }, nil
}

// queryAllDocs uses the DocumentStore.Query(ctx, filter) pattern your
// service.go already uses — a filter returning true keeps every doc.
func queryAllDocs(ctx context.Context, ds iface.DocumentStore) ([]interface{}, error) {
	return ds.Query(ctx, func(doc interface{}) (bool, error) {
		return true, nil
	})
}

// docsToJSONL writes one JSON-encoded doc per line.
func docsToJSONL(docs []interface{}) ([]byte, error) {
	var out []byte
	for _, d := range docs {
		b, err := json.Marshal(d)
		if err != nil {
			return nil, err
		}
		out = append(out, b...)
		out = append(out, '\n')
	}
	return out, nil
}

// ──────────────────────────────────────────────────────────────────────
// Import
// ──────────────────────────────────────────────────────────────────────

// ImportReport is returned to the HTTP caller as JSON.
type ImportReport struct {
	SQLiteRestored bool             `json:"sqlite_restored"`
	Stores         map[string]int64 `json:"stores"`
	Errors         []string         `json:"errors,omitempty"`
}

// Import consumes a tar.gz produced by Export and applies it. Collision
// policy is always "overwrite":
//   - OrbitDB docs: Put by _id. CRDT merge resolves to last-writer-wins.
//   - SQLite: close the live *sql.DB, swap the file, reopen via
//     app.InitSQLite so the sqlite-vec driver hook re-attaches cleanly.
//
// SQLite is restored LAST so a failed OrbitDB import doesn't leave us
// with a restarted SQLite handle and nothing else.
func (s *Service) Import(ctx context.Context, r io.Reader) (*ImportReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	report := &ImportReport{Stores: map[string]int64{}}

	staging, err := os.MkdirTemp("", "optimusdb-import-*")
	if err != nil {
		return nil, fmt.Errorf("mkdtemp: %w", err)
	}
	defer os.RemoveAll(staging)

	if err := extractTarGz(r, staging); err != nil {
		return nil, fmt.Errorf("extract: %w", err)
	}

	manifest, err := readManifest(filepath.Join(staging, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if manifest.Version != "1" {
		return nil, fmt.Errorf("unsupported archive version: %q", manifest.Version)
	}

	// OrbitDB first
	registry := s.storeRegistry()
	for _, name := range manifest.Stores {
		ptr, ok := registry[name]
		if !ok || ptr == nil {
			report.Errors = append(report.Errors,
				fmt.Sprintf("store %q in archive but not open on this node", name))
			continue
		}
		jsonlPath := filepath.Join(staging, "orbitdb", name+".jsonl")
		n, err := importStoreJSONL(ctx, *ptr, jsonlPath)
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("import %s: %v", name, err))
			continue
		}
		report.Stores[name] = n
	}

	// SQLite last
	if manifest.HasSQLite {
		src := filepath.Join(staging, "sqlite", "optimusdb.db")
		if err := s.restoreSQLite(src); err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("sqlite restore: %v", err))
		} else {
			report.SQLiteRestored = true
		}
	}

	return report, nil
}

// importStoreJSONL walks a JSONL file and Puts every document into ds.
// go-orbit-db's docstore Put with an existing _id writes a new OpLog
// entry — CRDT merge resolves to last-writer-wins, so the imported doc
// wins locally and other peers converge the same way once heads sync.
func importStoreJSONL(ctx context.Context, ds iface.DocumentStore, path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil // store listed in manifest but file missing — treat as empty
		}
		return 0, err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Some TOSCA docs are large. 32 MB max per line is generous headroom.
	scanner.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)

	var count int64
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var doc map[string]interface{}
		if err := json.Unmarshal(line, &doc); err != nil {
			return count, fmt.Errorf("decode line %d: %w", count+1, err)
		}
		// DocumentStore.Put requires _id to be a non-empty string.
		id, ok := doc["_id"].(string)
		if !ok || id == "" {
			continue
		}
		if _, err := ds.Put(ctx, doc); err != nil {
			return count, fmt.Errorf("put %q: %w", id, err)
		}
		count++
	}
	return count, scanner.Err()
}

// restoreSQLite replaces the on-disk SQLite file with src, then reopens
// through app.InitSQLite so the sqlite3_vec_kb driver hook re-attaches
// and the CreateX/MigrateX paths re-run against the new file.
//
// Callers holding a pointer to the OLD *sql.DB directly (rather than
// going through app.GlobalKBSQLite.DB) will see "database is closed".
// In practice everything in OptimusDB reaches the DB through the global,
// which we update below.
func (s *Service) restoreSQLite(src string) error {
	if s.SQLite == nil || s.SQLite.DB == nil {
		return errors.New("sqlite handle is nil")
	}
	if s.SQLite.DBPath == "" {
		return errors.New("SQLite.DBPath is empty; cannot restore in-place")
	}

	if err := s.SQLite.DB.Close(); err != nil {
		return fmt.Errorf("close live DB: %w", err)
	}

	if err := copyFile(src, s.SQLite.DBPath); err != nil {
		return fmt.Errorf("copy restored file: %w", err)
	}

	// Reopen. app.InitSQLite writes to app.GlobalKBSQLite, so after this
	// returns, both s.SQLite and app.GlobalKBSQLite point at the new DB.
	newKB, err := app.InitSQLite(s.SQLite.DBPath)
	if err != nil {
		return fmt.Errorf("reopen SQLite: %w", err)
	}
	s.SQLite.DB = newKB.DB
	s.SQLite.DBPath = newKB.DBPath
	return nil
}

// ──────────────────────────────────────────────────────────────────────
// HTTP handlers (called from api/http.go via interface assertion)
// ──────────────────────────────────────────────────────────────────────

// HandleExport serves GET/POST /api/v1/exchange/export.
func (s *Service) HandleExport(w http.ResponseWriter, r *http.Request) {
	fname := fmt.Sprintf("optimusdb-export-%s.tar.gz",
		time.Now().UTC().Format("20060102T150405Z"))
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", `attachment; filename="`+fname+`"`)

	if err := s.Export(r.Context(), w); err != nil {
		// Headers may already be flushed — best-effort error reporting.
		http.Error(w, "export failed: "+err.Error(), http.StatusInternalServerError)
	}
}

// HandleImport serves POST /api/v1/exchange/import.
// Expects multipart/form-data with field "archive" holding the tar.gz.
func (s *Service) HandleImport(w http.ResponseWriter, r *http.Request) {
	// 1 GB cap. Raise if your archives routinely exceed this.
	if err := r.ParseMultipartForm(1 << 30); err != nil {
		httpJSONError(w, http.StatusBadRequest, "parse multipart: "+err.Error())
		return
	}
	file, _, err := r.FormFile("archive")
	if err != nil {
		httpJSONError(w, http.StatusBadRequest,
			`missing form field "archive" (POST a tar.gz as multipart/form-data)`)
		return
	}
	defer file.Close()

	report, err := s.Import(r.Context(), file)
	if err != nil {
		httpJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(report)
}

// ──────────────────────────────────────────────────────────────────────
// tar + file helpers
// ──────────────────────────────────────────────────────────────────────

func writeTarFile(tw *tar.Writer, name string, body []byte) error {
	hdr := &tar.Header{
		Name:    name,
		Mode:    0o640,
		Size:    int64(len(body)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err := tw.Write(body)
	return err
}

func writeTarFromFile(tw *tar.Writer, name, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	hdr := &tar.Header{
		Name:    name,
		Mode:    0o640,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, f)
	return err
}

func extractTarGz(r io.Reader, destDir string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// Defensive path-traversal check.
		clean := filepath.Clean(hdr.Name)
		if strings.HasPrefix(clean, "..") || strings.Contains(clean, "/../") {
			return fmt.Errorf("illegal path in archive: %q", hdr.Name)
		}
		target := filepath.Join(destDir, clean)
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			_ = out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
}

func readManifest(path string) (*Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var m Manifest
	if err := json.NewDecoder(f).Decode(&m); err != nil {
		return nil, err
	}
	return &m, nil
}

// copyFile copies src to dst via a .tmp + rename so dst is never left
// half-written. Rename is atomic on the same filesystem.
func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".restore.tmp"
	out, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Sync(); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}

func httpJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
