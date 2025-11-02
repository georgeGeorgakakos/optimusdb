package contextualmetadata

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"optimusdb/app"
)

type EnrichmentOutput struct {
	Description string   `json:"description"`
	Tags        []string `json:"tags"`
	Columns     []struct {
		Name  string `json:"name"`
		Role  string `json:"role"`
		Notes string `json:"notes"`
	} `json:"columns"`
}

type Saver interface {
	SaveMetadata(ctx context.Context, kb *app.KnowledgeBaseDB, entry map[string]any) error
}

// Default saver → OrbitDB KBMetadata store
type OrbitDBSaver struct{}

func (s OrbitDBSaver) SaveMetadata(ctx context.Context, kb *app.KnowledgeBaseDB, entry map[string]any) error {
	dbMetaDocStore := *kb.KBMetadata
	metadataRecordsAsInterface := make([]interface{}, len(entry))
	_, err := dbMetaDocStore.PutAll(ctx, metadataRecordsAsInterface)

	return err
}

type Service struct {
	UseGreek bool
	Client   interface {
		Generate(string, int) (string, error)
	}
	Saver Saver
}

func (s *Service) EnrichDataset(ctx context.Context, kb *app.KnowledgeBaseDB, dbName, table string, maxRows int) (map[string]any, error) {
	profile, err := ProfileTable(dbName, table, maxRows)
	if err != nil {
		return nil, fmt.Errorf("profiling failed: %w", err)
	}

	prompt := BuildPrompt(EnrichmentRequest{DB: dbName, Table: table, Profile: profile, UseGreek: s.UseGreek})
	raw, err := s.Client.Generate(prompt, 512)
	if err != nil {
		return nil, fmt.Errorf("llm generate failed: %w", err)
	}

	var out EnrichmentOutput
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		// Be forgiving: small models can spill text — try to locate JSON braces quickly.
		start, end := findJSON(raw)
		if start >= 0 && end > start {
			if err2 := json.Unmarshal([]byte(raw[start:end]), &out); err2 != nil {
				return nil, fmt.Errorf("failed to parse llm JSON: %v; raw: %.200s", err2, raw)
			}
		} else {
			return nil, fmt.Errorf("no JSON in llm output: %.200s", raw)
		}
	}

	// Build KBMetadata entry
	h := sha256.Sum256([]byte(dbName + "/" + table + time.Now().UTC().String()))
	id := "meta-" + hex.EncodeToString(h[:])
	entry := map[string]any{
		"_id":           id,
		"metadata_type": "dataset_context",
		"associated_id": fmt.Sprintf("%s/%s", dbName, table),
		"name":          table,
		"description":   out.Description,
		"tags":          out.Tags,
		"status":        "generated",
		"created_by":    "contextual-enricher",
		"created_at":    time.Now().UTC(),
		"updated_at":    time.Now().UTC(),
	}

	if s.Saver == nil {
		s.Saver = OrbitDBSaver{}
	}
	if err := s.Saver.SaveMetadata(ctx, kb, entry); err != nil {
		return nil, fmt.Errorf("save to KBMetadata failed: %w", err)
	}
	return entry, nil
}

func findJSON(s string) (int, int) {
	start := -1
	depth := 0
	for i, r := range s {
		if r == '{' {
			if start < 0 {
				start = i
			}
			depth++
		}
		if r == '}' && start >= 0 {
			depth--
			if depth == 0 {
				return start, i + 1
			}
		}
	}
	return -1, -1
}
