//go:build !linux || !cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Fallback for non-Linux platforms (e.g. Windows development).
	// Registers sqlite3_vec_kb as a standard sqlite3 driver without
	// loading vec0 extension — semantic search will not initialise but
	// the app starts normally and retries gracefully on Linux deployment.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{})
}
