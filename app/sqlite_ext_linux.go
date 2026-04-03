//go:build linux && cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Register a custom SQLite driver that enables load_extension per-connection.
	// This is required for SELECT load_extension('/usr/lib/sqlite-vec/vec0') to work
	// in semantic_search.go. The -tags allow_load_extension build tag (set in Dockerfile)
	// compiles EnableLoadExtension into the mattn/go-sqlite3 CGO layer.
	// This file is excluded on Windows (no build tag match) so the IDE won't complain.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{
		ConnectHook: func(conn *sqlite3.SQLiteConn) error {
			return conn.EnableLoadExtension(true)
		},
	})
}
