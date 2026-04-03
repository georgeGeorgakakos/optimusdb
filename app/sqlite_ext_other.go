//go:build !linux || !cgo

package app

import (
	"database/sql"
	"github.com/mattn/go-sqlite3"
)

func init() {
	// Fallback for Windows — no extension loading, app starts normally.
	sql.Register("sqlite3_vec_kb", &sqlite3.SQLiteDriver{})
}
