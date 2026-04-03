//go:build !linux || !cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Fallback for non-Linux platforms (e.g. Windows development).
	// Registers sqlite3_vec_kb as a standard sqlite3 driver without
	// EnableLoadExtension — vec0 will not load but the app starts normally.
	// Semantic search retries gracefully; it only fully works in the Linux container.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{})
}
