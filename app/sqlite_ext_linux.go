//go:build linux && cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Register sqlite3_vec_kb driver with vec0 pre-loaded on every connection.
	// SQLiteDriver.Extensions calls sqlite3_enable_load_extension() + loads the
	// .so at the C level on each new connection — no manual load_extension() needed.
	// This file only compiles on Linux (inside Docker), not on Windows.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{
		Extensions: []string{"/usr/lib/sqlite-vec/vec0"},
	})
}
