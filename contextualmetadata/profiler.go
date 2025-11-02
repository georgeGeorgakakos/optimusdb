package contextualmetadata

import (
	"fmt"
)

// ColumnProfile holds inferred semantics & stats.
type ColumnProfile struct {
	Name          string
	SampleValues  []string
	InferredType  string // int, float, bool, date, datetime, string, json, categorical
	NullRatio     float64
	Cardinality   int
	ExampleValues []string
	Min, Max      *string // if numeric/date summarized as strings
	AvgLength     float64
	Entropy       float64
	IsIdentifier  bool
	IsTimestamp   bool
	IsGeo         bool
	IsCodeLike    bool // SKU, ID-like patterns
}

type DatasetProfile struct {
	DB       string
	Table    string
	RowCount int
	Profiles []ColumnProfile
}

// TODO: Wire to your SQL engine's reader to fetch a sample and compute stats.
// For now leave this as the public entry to integrate.
func ProfileTable(dbName, table string, maxRows int) (*DatasetProfile, error) {
	// Pseudocode:
	// 1) Open relation (dbName/table) through your RelationManager
	// 2) Scan up to maxRows rows
	// 3) For each column, compute:
	//    - type inference (regex + parsers)
	//    - null ratio, cardinality, avg length, entropy
	//    - examples (first K distinct)
	//    - min/max for numeric/date
	//    - heuristics: timestamp, id-like, geo coords, ISO codes, etc.
	return nil, fmt.Errorf("ProfileTable not wired yet")
}
