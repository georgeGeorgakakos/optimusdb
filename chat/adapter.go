// Copyright Contributors to the OptimusDB project.
// SPDX-License-Identifier: Apache-2.0

package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"optimusdb/logger"
)

// ============================================================================
// ADAPTER INTERFACE
// ============================================================================

// NLQueryAdapter interface for the NL Query engine
type NLQueryAdapter interface {
	TranslateQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error)
	ExecuteQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error)
	GetSchema(ctx context.Context, dstype string) (*SchemaInfo, error)
	GetAvailableDatasets() []DatasetInfo
}

// ============================================================================
// FUNCTION TYPES FOR DEPENDENCY INJECTION
// ============================================================================

// QueryFunc is the function signature for executing queries against KB
type QueryFunc func(ctx context.Context, dstype string, criteria []map[string]interface{}) ([]map[string]interface{}, error)

// SchemaFunc is the function signature for getting schema
type SchemaFunc func(dstype string) (*SchemaInfo, error)

// ============================================================================
// ADAPTER CONFIGURATION
// ============================================================================

// AdapterConfig configuration for the KnowledgeBase adapter
type AdapterConfig struct {
	TinyllamaURL string
	QueryFunc    QueryFunc
	SchemaFunc   SchemaFunc
	Datasets     []DatasetInfo
	Timeout      time.Duration
	SchemaTTL    time.Duration
}

// DefaultAdapterConfig returns default configuration.
//
// The Datasets list enumerates every OrbitDB docstore the chat adapter
// is allowed to route queries to. The Description strings are consumed
// by the LLM when it decides which dataset a natural-language question
// targets — keep them factual and mention key field names explicitly
// (e.g. num_cpus, mem_size) so the model can route compute-capacity
// questions to tosca_capacities rather than falling back to the default.
func DefaultAdapterConfig() AdapterConfig {
	return AdapterConfig{
		TinyllamaURL: "http://localhost:11434/api/chat",
		Datasets: []DatasetInfo{
			// Knowledge-base core
			{Type: "kbdata", Name: "Knowledge base data",
				Description: "General knowledge-base documents and records"},
			{Type: "kbmetadata", Name: "Knowledge base metadata",
				Description: "Catalog metadata including tables, columns, schemas"},
			{Type: "validations", Name: "Validations",
				Description: "Validation records and audit data"},
			{Type: "whoiswho", Name: "Who-is-who directory",
				Description: "Identity and role metadata for peers and services"},

			// Energy / resources
			{Type: "dsswres", Name: "Solar and wind resources",
				Description: "Renewable energy asset metadata — solar panels, wind turbines, resource specs, locations"},
			{Type: "dsswresaloc", Name: "Resource allocations",
				Description: "Resource allocation and scheduling data for energy assets"},

			// TOSCA orchestration (Swarmchestrate)
			{Type: "tosca_adt", Name: "TOSCA application descriptor templates",
				Description: "TOSCA ADT templates defining application topology — nodes, relationships, policies"},
			{Type: "tosca_imported", Name: "TOSCA imported templates",
				Description: "Third-party TOSCA templates imported into the catalog"},
			{Type: "tosca_capacities", Name: "TOSCA capacity profiles",
				Description: "Compute, memory, storage capacity requirements for TOSCA deployments — fields include num_cpus, mem_size, mem_size_mb, storage_gb, network_bandwidth_mbps, gpu_count"},
			{Type: "tosca_deploymentplan", Name: "TOSCA deployment plans",
				Description: "Scheduled deployment plans derived from TOSCA ADTs"},
			{Type: "tosca_eventhistory", Name: "TOSCA event history",
				Description: "Runtime events and lifecycle transitions for TOSCA deployments"},
		},
		Timeout:   30 * time.Second,
		SchemaTTL: 5 * time.Minute,
	}
}

// ============================================================================
// KNOWLEDGE BASE ADAPTER
// ============================================================================

// KnowledgeBaseAdapter connects directly to OptimusDB's knowledge base
type KnowledgeBaseAdapter struct {
	tinyllamaURL  string
	queryFunc     QueryFunc
	schemaFunc    SchemaFunc
	datasets      []DatasetInfo
	client        *http.Client
	schemaCache   map[string]*SchemaInfo
	schemaCacheMu sync.RWMutex
	schemaTTL     time.Duration
}

// NewKnowledgeBaseAdapter creates a new KB adapter
func NewKnowledgeBaseAdapter(config AdapterConfig) *KnowledgeBaseAdapter {
	return &KnowledgeBaseAdapter{
		tinyllamaURL: config.TinyllamaURL,
		queryFunc:    config.QueryFunc,
		schemaFunc:   config.SchemaFunc,
		datasets:     config.Datasets,
		client: &http.Client{
			Timeout: config.Timeout,
		},
		schemaCache: make(map[string]*SchemaInfo),
		schemaTTL:   config.SchemaTTL,
	}
}

// TranslateQuery translates natural language to query without executing
func (a *KnowledgeBaseAdapter) TranslateQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error) {
	logger.Info("[CHAT-ADAPTER] Translating query: %s (dstype: %s)", truncateString(prompt, 100), dstype)
	logger.Debug("[CHAT-ADAPTER] LLM endpoint: %s", a.tinyllamaURL)

	translationStart := time.Now()
	cmd, cmdType, criteria, err := a.translateWithTinyLlama(ctx, prompt, dstype)
	translationTime := time.Since(translationStart)

	if err != nil {
		logger.Warn("[CHAT-ADAPTER] TinyLlama translation failed after %v: %v — using fallback", translationTime, err)
		cmd, cmdType, criteria = a.fallbackTranslation(prompt, dstype)
		logger.Info("[CHAT-ADAPTER] Fallback produced: cmd=%s cmdType=%s", cmd, cmdType)
	} else {
		logger.Info("[CHAT-ADAPTER] LLM translation completed in %v", translationTime)
	}

	// Log the translated criteria so operators can see what the LLM produced.
	// This is the single most useful diagnostic line when debugging wrong results:
	// it shows exactly what field names, operators, and values the LLM chose.
	criteriaJSON, _ := json.Marshal(criteria)
	logger.Info("[CHAT-ADAPTER] Translated: cmd=%s cmdType=%s criteria=%s", cmd, cmdType, string(criteriaJSON))

	return &NLQueryResult{
		OriginalPrompt: prompt,
		TranslatedCmd:  cmd,
		CommandType:    cmdType,
		Parameters:     map[string]interface{}{"criteria": criteria},
		ResultCount:    0,
	}, nil
}

// ExecuteQuery translates and executes the query
func (a *KnowledgeBaseAdapter) ExecuteQuery(ctx context.Context, prompt string, dstype string) (*NLQueryResult, error) {
	logger.Info("[CHAT-ADAPTER] ExecuteQuery called: %s (dstype: %s)", truncateString(prompt, 100), dstype)

	result, err := a.TranslateQuery(ctx, prompt, dstype)
	if err != nil {
		return nil, err
	}

	if a.queryFunc != nil {
		start := time.Now()

		var criteria []map[string]interface{}
		if c, ok := result.Parameters["criteria"].([]map[string]interface{}); ok {
			criteria = c
		}

		logger.Debug("[CHAT-ADAPTER] Executing against store=%s with %d criteria", dstype, len(criteria))

		results, err := a.queryFunc(ctx, dstype, criteria)
		if err != nil {
			result.Error = err.Error()
			logger.Warn("[CHAT-ADAPTER] Query execution failed: %v", err)
			return result, nil
		}

		result.Results = results
		result.ResultCount = len(results)
		result.ExecutionTime = time.Since(start)

		logger.Info("[CHAT-ADAPTER] Query returned %d results in %v", result.ResultCount, result.ExecutionTime)

		// Log first result ID for traceability (helps confirm the right store was queried)
		if len(results) > 0 {
			if id, ok := results[0]["_id"]; ok {
				logger.Debug("[CHAT-ADAPTER] First result _id: %v", id)
			}
		}
	} else {
		logger.Warn("[CHAT-ADAPTER] queryFunc is nil — cannot execute, returning translation only")
	}

	return result, nil
}

// GetSchema returns schema information for a dataset type
func (a *KnowledgeBaseAdapter) GetSchema(ctx context.Context, dstype string) (*SchemaInfo, error) {
	a.schemaCacheMu.RLock()
	if cached, ok := a.schemaCache[dstype]; ok {
		if time.Since(cached.LastUpdated) < a.schemaTTL {
			a.schemaCacheMu.RUnlock()
			logger.Debug("[CHAT-ADAPTER] Schema cache hit for %s", dstype)
			return cached, nil
		}
	}
	a.schemaCacheMu.RUnlock()

	if a.schemaFunc != nil {
		schema, err := a.schemaFunc(dstype)
		if err != nil {
			return nil, err
		}

		a.schemaCacheMu.Lock()
		a.schemaCache[dstype] = schema
		a.schemaCacheMu.Unlock()

		return schema, nil
	}

	return a.getDefaultSchema(dstype), nil
}

// GetAvailableDatasets returns list of available dataset types
func (a *KnowledgeBaseAdapter) GetAvailableDatasets() []DatasetInfo {
	return a.datasets
}

// ============================================================================
// TINYLLAMA INTEGRATION
// ============================================================================

func (a *KnowledgeBaseAdapter) translateWithTinyLlama(ctx context.Context, prompt string, dstype string) (string, string, []map[string]interface{}, error) {
	systemPrompt := buildTranslationPrompt(dstype)

	// Request body uses the OpenAI-compatible format that llama-server
	// serves at /v1/chat/completions. The "model" field is ignored by
	// llama-server (it always uses the loaded model) but is required by
	// the schema, so we include it for spec compliance.
	reqBody := map[string]interface{}{
		"model": "tinyllama",
		"messages": []map[string]string{
			{"role": "system", "content": systemPrompt},
			{"role": "user", "content": prompt},
		},
		"temperature": 0.1,
		"max_tokens":  256,
		"stream":      false,
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", nil, err
	}

	logger.Debug("[CHAT-LLM] Sending request to %s (prompt length: %d chars, system prompt: %d chars)",
		a.tinyllamaURL, len(prompt), len(systemPrompt))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.tinyllamaURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return "", "", nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	llmStart := time.Now()
	resp, err := a.client.Do(req)
	llmLatency := time.Since(llmStart)

	if err != nil {
		logger.Error("[CHAT-LLM] HTTP request failed after %v: %v", llmLatency, err)
		return "", "", nil, fmt.Errorf("TinyLlama request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug("[CHAT-LLM] HTTP response: status=%d latency=%v", resp.StatusCode, llmLatency)

	if resp.StatusCode != http.StatusOK {
		logger.Error("[CHAT-LLM] Non-200 status: %d", resp.StatusCode)
		return "", "", nil, fmt.Errorf("TinyLlama returned status %d", resp.StatusCode)
	}

	// Parse the response — supports both formats so this adapter works
	// with Ollama (/api/chat) and llama-server (/v1/chat/completions):
	//
	//   Ollama:       {"message": {"content": "..."}}
	//   llama-server: {"choices": [{"message": {"content": "..."}}]}
	//
	// We try OpenAI format first (llama-server), then fall back to Ollama.
	var raw map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		logger.Error("[CHAT-LLM] Failed to decode JSON response: %v", err)
		return "", "", nil, fmt.Errorf("decode LLM response: %w", err)
	}

	var content string
	var responseFormat string

	// Try OpenAI format: choices[0].message.content
	if choices, ok := raw["choices"].([]interface{}); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]interface{}); ok {
			if msg, ok := choice["message"].(map[string]interface{}); ok {
				if c, ok := msg["content"].(string); ok {
					content = c
					responseFormat = "openai"
				}
			}
		}
	}

	// Fall back to Ollama format: message.content
	if content == "" {
		if msg, ok := raw["message"].(map[string]interface{}); ok {
			if c, ok := msg["content"].(string); ok {
				content = c
				responseFormat = "ollama"
			}
		}
	}

	if content == "" {
		logger.Error("[CHAT-LLM] No content found in LLM response (tried OpenAI + Ollama formats)")
		logger.Debug("[CHAT-LLM] Raw response keys: %v", func() []string {
			keys := make([]string, 0, len(raw))
			for k := range raw {
				keys = append(keys, k)
			}
			return keys
		}())
		return "", "", nil, fmt.Errorf("no content in LLM response")
	}

	logger.Info("[CHAT-LLM] Raw LLM output (%s format, %d chars): %s",
		responseFormat, len(content), truncateString(content, 500))

	cmd, cmdType, criteria := parseTranslationResponse(content)

	logger.Debug("[CHAT-LLM] Parsed: cmd=%s cmdType=%s criteriaCount=%d", cmd, cmdType, len(criteria))

	return cmd, cmdType, criteria, nil
}

// buildTranslationPrompt constructs the system prompt that teaches the LLM
// how to translate a natural-language question into OptimusDB query criteria.
//
// Design notes:
//   - Each dataset has explicit field hints so the model can pick valid field
//     names rather than inventing them.
//   - Numeric-operator examples (>=, >, <=) are worked in directly, with
//     types shown correctly (numbers unquoted, strings quoted) — TinyLlama
//     is small enough that example coverage is how we get reliability.
//   - The example for "find applications with at least 2 vCPUs and more
//     than 2GB memory" is included verbatim because that is the specific
//     query shape the OptimusDB chat endpoint is expected to handle.
func buildTranslationPrompt(dstype string) string {
	return fmt.Sprintf(`You translate natural-language questions into OptimusDB query criteria.

Dataset type: %s

=== DATASET FIELD HINTS ===
dsswres / dsswresaloc: resource_provider, resource_name, resource_type,
    resource_def, resource_nature, status, cpu_capacity, memory_capacity,
    storage_capacity, country, region, city, energy_type
tosca_adt: _id, node_templates, topology_template, policies
tosca_imported: _id, source, template_name, imported_at
tosca_capacities: _id, num_cpus, mem_size, mem_size_mb, storage_gb,
    network_bandwidth_mbps, gpu_count
tosca_deploymentplan: _id, adt_ref, planned_at, target_agents, status
tosca_eventhistory: _id, deployment_id, event_type, occurred_at
validations: _id, path, is_valid, vote_cnt
kbmetadata: _id, table_name, column_name, data_type, description
kbdata: _id, table_name, row_data

=== RESPONSE FORMAT (JSON ONLY — no explanation, no markdown fences) ===
{"command":"get|query", "criteria":[{"field":"...", "operator":"...", "value":...}]}

Operators: "==", "!=", ">", ">=", "<", "<=", "contains"

=== EXAMPLES ===

Q: "list all TOSCA templates"
A: {"command":"get", "criteria":[]}

Q: "find applications with at least 2 vCPUs and more than 2GB memory"
A: {"command":"query", "criteria":[
     {"field":"num_cpus", "operator":">=", "value":2},
     {"field":"mem_size_mb", "operator":">", "value":2048}
   ]}

Q: "show TOSCA deployments with status deployed"
A: {"command":"query", "criteria":[
     {"field":"status", "operator":"==", "value":"deployed"}
   ]}

Q: "find capacity profiles needing more than 4 CPUs"
A: {"command":"query", "criteria":[
     {"field":"num_cpus", "operator":">", "value":4}
   ]}

Q: "show solar assets in Greece"
A: {"command":"query", "criteria":[
     {"field":"resource_type", "operator":"==", "value":"solar"},
     {"field":"country", "operator":"==", "value":"Greece"}
   ]}

Q: "wind resources with capacity between 1000 and 5000"
A: {"command":"query", "criteria":[
     {"field":"resource_type", "operator":"==", "value":"wind"},
     {"field":"cpu_capacity", "operator":">=", "value":1000},
     {"field":"cpu_capacity", "operator":"<=", "value":5000}
   ]}

Q: "anything in kbmetadata with 'price' in the column name"
A: {"command":"query", "criteria":[
     {"field":"column_name", "operator":"contains", "value":"price"}
   ]}

=== RULES ===
- Output JSON only. No prose, no markdown code fences.
- Use numbers without quotes for numeric values: 2 not "2".
- Use strings with quotes for text values: "Greece" not Greece.
- If the question is just "list" or "show all", return empty criteria [].
- If a field is not in the DATASET FIELD HINTS above, choose the closest
  existing field name — do not invent one.
- Multiple conditions on the same field mean an AND (range queries).

Respond with JSON only.`, dstype)
}

func parseTranslationResponse(response string) (string, string, []map[string]interface{}) {
	response = strings.TrimSpace(response)

	if strings.Contains(response, "```") {
		logger.Debug("[CHAT-LLM] Stripping markdown code fences from LLM output")
		start := strings.Index(response, "{")
		end := strings.LastIndex(response, "}")
		if start >= 0 && end > start {
			response = response[start : end+1]
		}
	}

	var parsed struct {
		Command  string                   `json:"command"`
		Criteria []map[string]interface{} `json:"criteria"`
	}

	if err := json.Unmarshal([]byte(response), &parsed); err != nil {
		logger.Warn("[CHAT-LLM] Failed to parse LLM output as JSON: %v", err)
		logger.Debug("[CHAT-LLM] Unparseable output was: %s", truncateString(response, 300))
		return "get", "crudget", nil
	}

	cmdType := "crudget"
	if parsed.Command == "query" {
		cmdType = "crudquery"
	}

	return parsed.Command, cmdType, parsed.Criteria
}

// fallbackTranslation is used when TinyLlama is unreachable or returns
// unparseable output. It handles the same domain keywords as inferDatasetType
// (in chat/handler.go) so that at minimum the right dataset is addressed
// even without a live LLM.
//
// For TOSCA numeric questions, fallback can only express the comparison
// keyword ("greater", "less", ">", "<") by returning cmdType=crudquery.
// The LLM-based path produces much better criteria — fallback is a safety
// net, not a replacement.
func (a *KnowledgeBaseAdapter) fallbackTranslation(prompt string, dstype string) (string, string, []map[string]interface{}) {
	promptLower := strings.ToLower(prompt)
	criteria := []map[string]interface{}{}

	// Energy-domain field hints
	if strings.Contains(promptLower, "solar") {
		criteria = append(criteria, map[string]interface{}{
			"field":    "resource_type",
			"operator": "==",
			"value":    "solar",
		})
		logger.Debug("[CHAT-FALLBACK] Matched keyword 'solar' → resource_type==solar")
	}
	if strings.Contains(promptLower, "wind") {
		criteria = append(criteria, map[string]interface{}{
			"field":    "resource_type",
			"operator": "==",
			"value":    "wind",
		})
		logger.Debug("[CHAT-FALLBACK] Matched keyword 'wind' → resource_type==wind")
	}

	locations := []string{"greece", "thessaloniki", "athens", "crete", "patras",
		"germany", "frankfurt", "berlin", "italy", "spain"}
	for _, loc := range locations {
		if strings.Contains(promptLower, loc) {
			criteria = append(criteria, map[string]interface{}{
				"field":    "country",
				"operator": "contains",
				"value":    loc,
			})
			logger.Debug("[CHAT-FALLBACK] Matched location '%s' → country contains %s", loc, loc)
			break
		}
	}

	// Status match — common across energy + TOSCA domains
	statuses := []string{"running", "stopped", "pending", "deployed", "failed"}
	for _, st := range statuses {
		if strings.Contains(promptLower, st) {
			criteria = append(criteria, map[string]interface{}{
				"field":    "status",
				"operator": "==",
				"value":    st,
			})
			logger.Debug("[CHAT-FALLBACK] Matched status '%s' → status==%s", st, st)
			break
		}
	}

	// Any comparison keyword flips the command to "query" so the caller
	// knows a range/filter semantic was intended, even if we couldn't
	// extract the numeric parameters reliably.
	cmdType := "crudget"
	if strings.Contains(promptLower, ">") || strings.Contains(promptLower, "<") ||
		strings.Contains(promptLower, "greater") || strings.Contains(promptLower, "less") ||
		strings.Contains(promptLower, "more than") || strings.Contains(promptLower, "at least") ||
		strings.Contains(promptLower, "at most") {
		cmdType = "crudquery"
		logger.Debug("[CHAT-FALLBACK] Detected comparison keyword → cmdType=crudquery")
	}

	cmd := "get"
	if cmdType == "crudquery" {
		cmd = "query"
	}

	logger.Info("[CHAT-FALLBACK] Fallback result: cmd=%s criteriaCount=%d", cmd, len(criteria))
	return cmd, cmdType, criteria
}

// getDefaultSchema returns a hardcoded SchemaInfo for a dataset when the
// schemaFunc injection point is not wired up. These schemas are hints for
// UIs and debugging — they are NOT authoritative. The authoritative source
// is the shape of the documents actually stored in each OrbitDB docstore.
func (a *KnowledgeBaseAdapter) getDefaultSchema(dstype string) *SchemaInfo {
	schemas := map[string]*SchemaInfo{
		"dsswres": {
			DatasetType: "dsswres",
			Tables: []TableInfo{
				{
					Name:        "assets",
					Description: "Renewable energy assets",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "resource_name", Type: "string", Required: true},
						{Name: "resource_type", Type: "string", Required: true},
						{Name: "resource_provider", Type: "string"},
						{Name: "country", Type: "string"},
						{Name: "region", Type: "string"},
						{Name: "city", Type: "string"},
						{Name: "cpu_capacity", Type: "number"},
						{Name: "memory_capacity", Type: "number"},
						{Name: "storage_capacity", Type: "number"},
						{Name: "status", Type: "string"},
						{Name: "energy_type", Type: "string"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"dsswresaloc": {
			DatasetType: "dsswresaloc",
			Tables: []TableInfo{
				{
					Name:        "allocations",
					Description: "Resource allocations",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "resource_id", Type: "string", Required: true},
						{Name: "allocated_to", Type: "string"},
						{Name: "start_time", Type: "datetime"},
						{Name: "end_time", Type: "datetime"},
						{Name: "priority", Type: "number"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"kbmetadata": {
			DatasetType: "kbmetadata",
			Tables: []TableInfo{
				{
					Name:        "metadata",
					Description: "Catalog metadata entries",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "table_name", Type: "string"},
						{Name: "column_name", Type: "string"},
						{Name: "data_type", Type: "string"},
						{Name: "description", Type: "string"},
						{Name: "owner", Type: "string"},
						{Name: "tags", Type: "array"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"tosca_adt": {
			DatasetType: "tosca_adt",
			Tables: []TableInfo{
				{
					Name:        "adt_templates",
					Description: "TOSCA application descriptor templates",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "template_name", Type: "string"},
						{Name: "node_templates", Type: "object"},
						{Name: "topology_template", Type: "object"},
						{Name: "policies", Type: "array"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"tosca_imported": {
			DatasetType: "tosca_imported",
			Tables: []TableInfo{
				{
					Name:        "imported_templates",
					Description: "Third-party TOSCA templates",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "source", Type: "string"},
						{Name: "template_name", Type: "string"},
						{Name: "imported_at", Type: "datetime"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"tosca_capacities": {
			DatasetType: "tosca_capacities",
			Tables: []TableInfo{
				{
					Name:        "capacity_profiles",
					Description: "TOSCA compute / memory / storage requirements",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "num_cpus", Type: "number"},
						{Name: "mem_size", Type: "number"},
						{Name: "mem_size_mb", Type: "number"},
						{Name: "storage_gb", Type: "number"},
						{Name: "network_bandwidth_mbps", Type: "number"},
						{Name: "gpu_count", Type: "number"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"tosca_deploymentplan": {
			DatasetType: "tosca_deploymentplan",
			Tables: []TableInfo{
				{
					Name:        "deployment_plans",
					Description: "Scheduled TOSCA deployments",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "adt_ref", Type: "string"},
						{Name: "planned_at", Type: "datetime"},
						{Name: "target_agents", Type: "array"},
						{Name: "status", Type: "string"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"tosca_eventhistory": {
			DatasetType: "tosca_eventhistory",
			Tables: []TableInfo{
				{
					Name:        "events",
					Description: "TOSCA runtime events",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "deployment_id", Type: "string"},
						{Name: "event_type", Type: "string"},
						{Name: "occurred_at", Type: "datetime"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"validations": {
			DatasetType: "validations",
			Tables: []TableInfo{
				{
					Name:        "validation_records",
					Description: "Validation records and audit trail",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "path", Type: "string"},
						{Name: "is_valid", Type: "boolean"},
						{Name: "vote_cnt", Type: "number"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
		"whoiswho": {
			DatasetType: "whoiswho",
			Tables: []TableInfo{
				{
					Name:        "directory",
					Description: "Identity and role directory",
					Fields: []FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "peer_id", Type: "string"},
						{Name: "role", Type: "string"},
					},
				},
			},
			LastUpdated: time.Now(),
		},
	}

	if schema, ok := schemas[dstype]; ok {
		return schema
	}

	return &SchemaInfo{
		DatasetType: dstype,
		Tables: []TableInfo{
			{
				Name:        "documents",
				Description: "Document store",
				Fields: []FieldInfo{
					{Name: "_id", Type: "string", Required: true},
					{Name: "data", Type: "object"},
				},
			},
		},
		LastUpdated: time.Now(),
	}
}
