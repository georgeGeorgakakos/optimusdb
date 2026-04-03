//go:build linux && cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Register a custom SQLite driver that pre-loads the vec0 extension
	// on every new connection via the Extensions field of SQLiteDriver.
	// This avoids the need for EnableLoadExtension or SELECT load_extension().
	// vec0.so is installed at /usr/lib/sqlite-vec/vec0.so in the container image.
	// The SQLiteDriver.Extensions field calls sqlite3_load_extension() directly
	// at the C level, bypassing the EnableLoadExtension requirement entirely.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{
		Extensions: []string{"/usr/lib/sqlite-vec/vec0"},
	})
}
