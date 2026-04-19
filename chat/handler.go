// Copyright Contributors to the OptimusDB project.
// SPDX-License-Identifier: Apache-2.0

package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"optimusdb/logger"
)

// ============================================================================
// EXPORTED TYPES
// ============================================================================

// ChatRequest matches the DataCatalogAssistant frontend format
type ChatRequest struct {
	Message             string        `json:"message"`
	ConversationHistory []ChatMessage `json:"conversation_history"`
}

// ChatMessage represents a single message in conversation history
type ChatMessage struct {
	Role      string `json:"role"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp,omitempty"`
}

// ChatResponse is what the frontend expects
type ChatResponse struct {
	Response  string            `json:"response"`
	Metadata  *ResponseMetadata `json:"metadata,omitempty"`
	Timestamp string            `json:"timestamp"`
}

// ResponseMetadata provides optional context about the response
type ResponseMetadata struct {
	QueryType   string   `json:"query_type,omitempty"`
	DatasetType string   `json:"dataset_type,omitempty"`
	ExecutedCmd string   `json:"executed_cmd,omitempty"`
	ResultCount int      `json:"result_count,omitempty"`
	Sources     []string `json:"sources,omitempty"`
	Confidence  float64  `json:"confidence,omitempty"`
}

// NLQueryResult from the NL Query engine
type NLQueryResult struct {
	OriginalPrompt string                   `json:"original_prompt"`
	TranslatedCmd  string                   `json:"translated_cmd"`
	CommandType    string                   `json:"command_type"`
	Parameters     map[string]interface{}   `json:"parameters"`
	Results        []map[string]interface{} `json:"results,omitempty"`
	ResultCount    int                      `json:"result_count"`
	ExecutionTime  time.Duration            `json:"execution_time"`
	Error          string                   `json:"error,omitempty"`
}

// SchemaInfo contains dataset schema information
type SchemaInfo struct {
	DatasetType string      `json:"dataset_type"`
	Tables      []TableInfo `json:"tables"`
	LastUpdated time.Time   `json:"last_updated"`
}

// TableInfo represents a table/collection in the schema
type TableInfo struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Fields      []FieldInfo `json:"fields"`
	RecordCount int         `json:"record_count,omitempty"`
}

// FieldInfo represents a field in a table
type FieldInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
}

// DatasetInfo describes an available dataset
type DatasetInfo struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

// ============================================================================
// HANDLER CONFIGURATION
// ============================================================================

// HandlerConfig holds handler configuration
type HandlerConfig struct {
	DefaultDataset   string
	MaxHistoryLength int
	EnableExecution  bool
	GreetingEnabled  bool
	AssistantName    string
}

// DefaultHandlerConfig returns sensible defaults
func DefaultHandlerConfig() HandlerConfig {
	return HandlerConfig{
		DefaultDataset:   "dsswres",
		MaxHistoryLength: 10,
		EnableExecution:  true,
		GreetingEnabled:  true,
		AssistantName:    "OptimusDB Assistant",
	}
}

// ============================================================================
// HANDLER
// ============================================================================

// Handler wraps the NL Query engine for conversational access
type Handler struct {
	adapter NLQueryAdapter
	mu      sync.RWMutex
	config  HandlerConfig
}

// NewHandler creates a new chat handler
func NewHandler(adapter NLQueryAdapter, config HandlerConfig) *Handler {
	return &Handler{
		adapter: adapter,
		config:  config,
	}
}

// ServeHTTP implements http.Handler
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodPost {
		h.sendError(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ChatRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Warn("[CHAT] Failed to decode request body: %v", err)
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.Message) == "" {
		logger.Debug("[CHAT] Received empty message — rejecting")
		h.sendError(w, "Message cannot be empty", http.StatusBadRequest)
		return
	}

	logger.Info("[CHAT] ════════════════ NEW QUERY ════════════════")
	logger.Info("[CHAT] Message: %s", truncateString(req.Message, 200))
	if len(req.ConversationHistory) > 0 {
		logger.Debug("[CHAT] Conversation history: %d previous messages", len(req.ConversationHistory))
	}

	ctx := r.Context()
	start := time.Now()
	response := h.processMessage(ctx, req)
	elapsed := time.Since(start)

	logger.Info("[CHAT] ──── Response in %v ────", elapsed)
	logger.Info("[CHAT] Result: type=%s dataset=%s cmd=%s count=%d confidence=%.1f",
		response.Metadata.QueryType,
		response.Metadata.DatasetType,
		response.Metadata.ExecutedCmd,
		response.Metadata.ResultCount,
		response.Metadata.Confidence)
	logger.Info("[CHAT] ════════════════ END QUERY ════════════════")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleChat is an alternative handler function for mux registration
func (h *Handler) HandleChat(w http.ResponseWriter, r *http.Request) {
	h.ServeHTTP(w, r)
}

// processMessage handles the core logic
func (h *Handler) processMessage(ctx context.Context, req ChatRequest) ChatResponse {
	message := strings.TrimSpace(req.Message)
	messageLower := strings.ToLower(message)

	intent := h.classifyIntent(messageLower)
	logger.Debug("[CHAT] Intent classified as: %s", intent)

	var response ChatResponse
	response.Timestamp = time.Now().Format(time.RFC3339)

	switch intent {
	case "greeting":
		response.Response = h.handleGreeting(message)
		response.Metadata = &ResponseMetadata{QueryType: "greeting"}

	case "help":
		response.Response = h.handleHelp()
		response.Metadata = &ResponseMetadata{QueryType: "help"}

	case "schema":
		response = h.handleSchemaQuery(ctx, message)

	case "list_datasets":
		response = h.handleListDatasets(ctx)

	case "query":
		response = h.handleDataQuery(ctx, message, req.ConversationHistory)

	default:
		response = h.handleDataQuery(ctx, message, req.ConversationHistory)
	}

	return response
}

// ============================================================================
// INTENT CLASSIFICATION
// ============================================================================

func (h *Handler) classifyIntent(message string) string {
	greetingPatterns := []string{
		`^(hi|hello|hey|good\s+(morning|afternoon|evening))`,
		`^(what'?s up|howdy|greetings)`,
	}
	for _, pattern := range greetingPatterns {
		if matched, _ := regexp.MatchString(pattern, message); matched {
			return "greeting"
		}
	}

	helpPatterns := []string{
		`^(help|what can you do|how do i|how to)`,
		`capabilities|features|commands`,
	}
	for _, pattern := range helpPatterns {
		if matched, _ := regexp.MatchString(pattern, message); matched {
			return "help"
		}
	}

	schemaPatterns := []string{
		`(what|which|list|show).*(tables?|columns?|fields?|schema)`,
		`describe\s+(table|schema|structure)`,
		`table.*(available|exist)`,
	}
	for _, pattern := range schemaPatterns {
		if matched, _ := regexp.MatchString(pattern, message); matched {
			return "schema"
		}
	}

	if matched, _ := regexp.MatchString(`(what|which|list|show).*(datasets?|data\s*sources?)`, message); matched {
		return "list_datasets"
	}

	return "query"
}

// ============================================================================
// INTENT HANDLERS
// ============================================================================

func (h *Handler) handleGreeting(message string) string {
	if !h.config.GreetingEnabled {
		return fmt.Sprintf("Hello! I'm %s. How can I help you explore your data catalog today?", h.config.AssistantName)
	}

	hour := time.Now().Hour()
	var timeGreeting string
	switch {
	case hour < 12:
		timeGreeting = "Good morning"
	case hour < 17:
		timeGreeting = "Good afternoon"
	default:
		timeGreeting = "Good evening"
	}

	return fmt.Sprintf("%s! I'm %s, your data catalog assistant. I can help you:\n\n"+
		"• **Search** for tables, datasets, and metadata\n"+
		"• **Query** renewable energy data using natural language\n"+
		"• **Explore** schema and table structures\n"+
		"• **Find** data owners and popular tags\n\n"+
		"What would you like to explore?", timeGreeting, h.config.AssistantName)
}

func (h *Handler) handleHelp() string {
	return fmt.Sprintf("# %s - Help\n\n"+
		"I understand natural language queries about your data catalog. Here are some examples:\n\n"+
		"**Data Discovery:**\n"+
		"• \"Show me solar energy data\"\n"+
		"• \"Find renewable energy datasets\"\n"+
		"• \"What tables contain wind data?\"\n\n"+
		"**Schema Exploration:**\n"+
		"• \"What tables are available?\"\n"+
		"• \"Show table schema for power_output\"\n"+
		"• \"What columns does the turbine table have?\"\n\n"+
		"**Ownership & Tags:**\n"+
		"• \"Who owns the wind_turbine table?\"\n"+
		"• \"What are the most popular tags?\"\n"+
		"• \"Show tables tagged with 'production'\"\n\n"+
		"**Filtering:**\n"+
		"• \"Show assets with capacity > 1000\"\n"+
		"• \"Find solar panels in Thessaloniki\"\n"+
		"• \"List wind farms installed after 2020\"\n\n"+
		"Just ask in plain English and I'll translate it to the appropriate query!", h.config.AssistantName)
}

func (h *Handler) handleSchemaQuery(ctx context.Context, message string) ChatResponse {
	response := ChatResponse{
		Timestamp: time.Now().Format(time.RFC3339),
		Metadata:  &ResponseMetadata{QueryType: "schema"},
	}

	dstype := h.inferDatasetType(message)
	if dstype == "" {
		dstype = h.config.DefaultDataset
		logger.Debug("[CHAT] Schema query: no dataset inferred, using default: %s", dstype)
	}
	response.Metadata.DatasetType = dstype

	schema, err := h.adapter.GetSchema(ctx, dstype)
	if err != nil {
		logger.Warn("[CHAT] Schema retrieval failed for %s: %v", dstype, err)
		response.Response = fmt.Sprintf("I couldn't retrieve the schema information: %v\n\n"+
			"Try asking about a specific table or dataset.", err)
		return response
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Here's the schema for **%s**:\n\n", dstype))

	for i, table := range schema.Tables {
		if i >= 5 {
			sb.WriteString(fmt.Sprintf("\n...and %d more tables. Ask about a specific table for details.", len(schema.Tables)-5))
			break
		}

		sb.WriteString(fmt.Sprintf("**%s**", table.Name))
		if table.Description != "" {
			sb.WriteString(fmt.Sprintf(" - %s", table.Description))
		}
		sb.WriteString("\n")

		if len(table.Fields) > 0 {
			fieldNames := make([]string, 0, len(table.Fields))
			for _, f := range table.Fields {
				fieldNames = append(fieldNames, f.Name)
			}
			sb.WriteString(fmt.Sprintf("  Fields: %s\n", strings.Join(fieldNames, ", ")))
		}
		sb.WriteString("\n")
	}

	response.Response = sb.String()
	return response
}

func (h *Handler) handleListDatasets(ctx context.Context) ChatResponse {
	response := ChatResponse{
		Timestamp: time.Now().Format(time.RFC3339),
		Metadata:  &ResponseMetadata{QueryType: "list_datasets"},
	}

	datasets := h.adapter.GetAvailableDatasets()
	if len(datasets) == 0 {
		response.Response = "No datasets are currently available. Please check your OptimusDB configuration."
		return response
	}

	var sb strings.Builder
	sb.WriteString("Here are the available datasets:\n\n")

	for _, ds := range datasets {
		sb.WriteString(fmt.Sprintf("**%s** (`%s`)\n", ds.Name, ds.Type))
		if ds.Description != "" {
			sb.WriteString(fmt.Sprintf("  %s\n", ds.Description))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("You can query any of these by mentioning them in your question!")
	response.Response = sb.String()
	return response
}

func (h *Handler) handleDataQuery(ctx context.Context, message string, history []ChatMessage) ChatResponse {
	response := ChatResponse{
		Timestamp: time.Now().Format(time.RFC3339),
		Metadata:  &ResponseMetadata{QueryType: "nlquery"},
	}

	// Step 1: Infer the target dataset
	dstype := h.inferDatasetType(message)
	if dstype == "" {
		dstype = h.inferFromHistory(history)
		if dstype != "" {
			logger.Debug("[CHAT-QUERY] Dataset inferred from conversation history: %s", dstype)
		}
	}
	if dstype == "" {
		dstype = h.config.DefaultDataset
		logger.Debug("[CHAT-QUERY] No dataset inferred — using default: %s", dstype)
	}
	response.Metadata.DatasetType = dstype

	// Step 2: Translate and/or execute
	var result *NLQueryResult
	var err error

	if h.config.EnableExecution {
		logger.Debug("[CHAT-QUERY] Execution enabled — calling ExecuteQuery (translate + run)")
		result, err = h.adapter.ExecuteQuery(ctx, message, dstype)
	} else {
		logger.Debug("[CHAT-QUERY] Execution disabled — calling TranslateQuery only")
		result, err = h.adapter.TranslateQuery(ctx, message, dstype)
	}

	if err != nil {
		logger.Error("[CHAT-QUERY] Query processing failed: %v", err)
		response.Response = fmt.Sprintf("I had trouble processing that query: %v\n\n"+
			"Could you try rephrasing? For example:\n"+
			"• \"Show me all solar assets\"\n"+
			"• \"Find wind turbines with capacity > 500\"", err)
		return response
	}

	// Step 3: Format and return
	response.Response = h.formatQueryResult(result)
	response.Metadata.ExecutedCmd = result.TranslatedCmd
	response.Metadata.ResultCount = result.ResultCount
	response.Metadata.Confidence = h.estimateConfidence(result)

	return response
}

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================
/*
	DESIGN NOTES

	TOSCA routing is FIRST because these keywords (cpu, memory, application,
	deployment) are the ones most likely to collide with the energy domain
	patterns that follow. A query like "find applications with 2 vCPUs and
	2GB memory" should NEVER reach the energy-domain regex.
	The word "capacity" was REMOVED from the dsswres regex because it's
	ambiguous — TOSCA has capacity profiles too, and the ambiguity was
	causing false routing. If someone wants renewable-energy capacity
	queries, they'll use more specific words (kwh, mwh, generation).
	Word boundaries (\b) are used throughout so "scheduled" doesn't match
	"scheduler" and "vm" doesn't match inside "vmware". The original code
	didn't use these, which caused subtle false-positives.
	"dstelemetry" and "dsmeta" were removed — those aren't real stores in
	this codebase. They were leftover from an earlier design.
	Ordering matters. Within the TOSCA block, more specific patterns
	(deployment, event) come before general ones. The capacity pattern is
	first because it's the most common TOSCA query shape.

	LIMITATIONS
	Regex routing is inherently brittle. A prompt like "how many deployments
	have more than 2 CPUs" will route to tosca_deploymentplan (matches
	"deployment" first) rather than tosca_capacities (which has the num_cpus
	field). The user will get zero results because deployment_plan docs don't
	have num_cpus.
	The fix for this is a second-stage LLM router that picks the dataset
	AFTER seeing the prompt's actual structure. That's a bigger change and
	not in this patch. For now, the simple regex approach handles the 80%
	case and is much better than what exists today.
*/

func (h *Handler) inferDatasetType(message string) string {
	message = strings.ToLower(message)

	// TOSCA routing — check these FIRST because TOSCA questions often contain
	// words like "application" or "deployment" that are too generic to match
	// energy-domain patterns but specific to TOSCA in this system's context.
	if matched, _ := regexp.MatchString(
		`\b(tosca|vcpu|vcpus|cpu|cpus|mem|memory|ram|gb|mb|vm|vms|gpu)\b`, message); matched {
		// Numeric compute queries overwhelmingly target capacity profiles.
		logger.Debug("[CHAT-ROUTER] Matched TOSCA capacities keywords → tosca_capacities")
		return "tosca_capacities"
	}
	if matched, _ := regexp.MatchString(
		`\b(deployment|deploy|deployed|rollout)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched deployment keywords → tosca_deploymentplan")
		return "tosca_deploymentplan"
	}
	if matched, _ := regexp.MatchString(
		`\b(event|history|timeline|lifecycle)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched event/history keywords → tosca_eventhistory")
		return "tosca_eventhistory"
	}
	if matched, _ := regexp.MatchString(
		`\b(adt|node[_ ]?template|topology|policy|application[_ ]?descriptor)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched ADT keywords → tosca_adt")
		return "tosca_adt"
	}
	if matched, _ := regexp.MatchString(
		`\b(imported|third[- ]?party)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched imported keywords → tosca_imported")
		return "tosca_imported"
	}

	// Energy/resource domain — solar, wind, generation
	if matched, _ := regexp.MatchString(
		`\b(solar|pv|photovoltaic|panel)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched solar keywords → dsswres")
		return "dsswres"
	}
	if matched, _ := regexp.MatchString(
		`\b(wind|turbine|windmill)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched wind keywords → dsswres")
		return "dsswres"
	}
	if matched, _ := regexp.MatchString(
		`\b(renewable|generation|kwh|mwh)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched energy keywords → dsswres")
		return "dsswres"
	}
	if matched, _ := regexp.MatchString(
		`\b(allocation|allocated|schedule|scheduled|reservation)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched allocation keywords → dsswresaloc")
		return "dsswresaloc"
	}

	// Knowledge-base housekeeping
	if matched, _ := regexp.MatchString(
		`\b(metadata|catalog|table|column|schema|field)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched metadata keywords → kbmetadata")
		return "kbmetadata"
	}
	if matched, _ := regexp.MatchString(
		`\b(validation|validate|verified|valid|invalid)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched validation keywords → validations")
		return "validations"
	}
	if matched, _ := regexp.MatchString(
		`\b(who|role|identity|directory|whoiswho)\b`, message); matched {
		logger.Debug("[CHAT-ROUTER] Matched identity keywords → whoiswho")
		return "whoiswho"
	}

	logger.Debug("[CHAT-ROUTER] No keyword matched — returning empty (will use default)")
	return ""
}

func (h *Handler) inferFromHistory(history []ChatMessage) string {
	for i := len(history) - 1; i >= 0 && i >= len(history)-5; i-- {
		msg := history[i]
		if msg.Role == "user" {
			if ds := h.inferDatasetType(msg.Content); ds != "" {
				return ds
			}
		}
	}
	return ""
}

func (h *Handler) formatQueryResult(result *NLQueryResult) string {
	var sb strings.Builder

	if result.Error != "" {
		sb.WriteString(fmt.Sprintf("⚠️ Query issue: %s\n\n", result.Error))
		sb.WriteString("Try rephrasing your question or being more specific.")
		return sb.String()
	}

	if result.ResultCount == 0 {
		sb.WriteString("I didn't find any matching results for that query.\n\n")
		sb.WriteString("**Suggestions:**\n")
		sb.WriteString("• Try broader search terms\n")
		sb.WriteString("• Check if the table/field names are correct\n")
		sb.WriteString("• Ask \"What tables are available?\" to see options")
		return sb.String()
	}

	sb.WriteString(fmt.Sprintf("Found **%d** result(s):\n\n", result.ResultCount))

	maxShow := 5
	if result.ResultCount < maxShow {
		maxShow = result.ResultCount
	}

	for i := 0; i < maxShow && i < len(result.Results); i++ {
		row := result.Results[i]
		sb.WriteString(fmt.Sprintf("**%d.** ", i+1))

		if name, ok := row["name"].(string); ok {
			sb.WriteString(name)
		} else if title, ok := row["title"].(string); ok {
			sb.WriteString(title)
		} else if id, ok := row["_id"].(string); ok {
			sb.WriteString(fmt.Sprintf("ID: %s", id))
		} else {
			parts := make([]string, 0)
			count := 0
			for k, v := range row {
				if count >= 3 {
					break
				}
				if !strings.HasPrefix(k, "_") {
					parts = append(parts, fmt.Sprintf("%s: %v", k, v))
					count++
				}
			}
			sb.WriteString(strings.Join(parts, ", "))
		}
		sb.WriteString("\n")

		details := make([]string, 0)
		if location, ok := row["location"].(string); ok {
			details = append(details, fmt.Sprintf("📍 %s", location))
		}
		if capacity, ok := row["capacity"]; ok {
			details = append(details, fmt.Sprintf("⚡ Capacity: %v", capacity))
		}
		if owner, ok := row["owner"].(string); ok {
			details = append(details, fmt.Sprintf("👤 %s", owner))
		}
		if status, ok := row["status"].(string); ok {
			details = append(details, fmt.Sprintf("Status: %s", status))
		}

		if len(details) > 0 {
			sb.WriteString(fmt.Sprintf("   %s\n", strings.Join(details, " | ")))
		}
		sb.WriteString("\n")
	}

	if result.ResultCount > maxShow {
		sb.WriteString(fmt.Sprintf("...and %d more results. ", result.ResultCount-maxShow))
		sb.WriteString("Ask me to narrow down the results or show specific fields.")
	}

	return sb.String()
}

func (h *Handler) estimateConfidence(result *NLQueryResult) float64 {
	if result.Error != "" {
		return 0.3
	}
	if result.ResultCount == 0 {
		return 0.5
	}
	if result.ResultCount > 0 && result.TranslatedCmd != "" {
		return 0.9
	}
	return 0.7
}

func (h *Handler) sendError(w http.ResponseWriter, message string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(ChatResponse{
		Response:  message,
		Timestamp: time.Now().Format(time.RFC3339),
	})
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
