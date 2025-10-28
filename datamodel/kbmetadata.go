package datamodel

import (
	orbitdb "berty.tech/go-orbit-db"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"sync"
	"time"
)

//Replication Across Peers:
//Use replication features to share the metadata store across distributed nodes.

/*
Insert Metadata Entries
Use the Put method to insert metadata documents into the store.
*/

// MetadataEntry represents a metadata object
type MetadataEntry struct {
	ID               string                   `json:"_id"`
	Author           string                   `json:"author"`
	MetadataType     string                   `json:"metadata_type"`
	Component        string                   `json:"component"`
	Behaviour        string                   `json:"behaviour"`
	Relationships    string                   `json:"relationships"`
	AssociatedID     string                   `json:"associated_id"`
	Name             string                   `json:"name"`
	Description      string                   `json:"description"`
	Tags             []string                 `json:"tags"`
	Status           string                   `json:"status"`
	CreatedBy        string                   `json:"created_by"`
	CreatedAt        time.Time                `json:"created_at"`
	UpdatedAt        time.Time                `json:"updated_at"`
	RelatedIDs       []string                 `json:"related_ids"`
	Priority         string                   `json:"priority"`
	SchedulingInfo   map[string]interface{}   `json:"scheduling_info"`
	SLAConstraints   map[string]interface{}   `json:"sla_constraints"`
	OwnershipDetails map[string]interface{}   `json:"ownership_details"`
	AuditTrail       []map[string]interface{} `json:"audit_trail"`
}

// MetadataStore keeps track of metadata entries
type MetadataStore struct {
	sync.Mutex
	Entries map[string]MetadataEntry
}

// Global Metadata Storage
var Metadata = MetadataStore{
	Entries: make(map[string]MetadataEntry),
}

// AddMetadata inserts or updates a metadata entry
func (ms *MetadataStore) AddMetadata(entry MetadataEntry) {
	ms.Lock()
	defer ms.Unlock()

	entry.UpdatedAt = time.Now()
	if _, exists := ms.Entries[entry.ID]; !exists {
		entry.CreatedAt = time.Now()
	}

	ms.Entries[entry.ID] = entry
}

// GetMetadata retrieves metadata entries
func (ms *MetadataStore) GetMetadata() []MetadataEntry {
	ms.Lock()
	defer ms.Unlock()

	metadataList := make([]MetadataEntry, 0, len(ms.Entries))
	for _, entry := range ms.Entries {
		metadataList = append(metadataList, entry)
	}
	return metadataList
}

// DeleteMetadata removes an entry
func (ms *MetadataStore) DeleteMetadata(id string) {
	ms.Lock()
	defer ms.Unlock()
	delete(ms.Entries, id)
}

/*
func AddMetadata(store orbitdb.DocumentStore, ctx context.Context) {
	metadata := map[string]interface{}{
		"_id":           "workflow-1",
		"metadata_type": "Workflow",
		"associated_id": "workflow-1",
		"name":          "Backup Task",
		"description":   "A scheduled workflow to back up database resources daily.",
		"tags":          []string{"backup", "critical"},
		"status":        "Active",
		"created_by":    "admin-user",
		"created_at":    time.Now().Format(time.RFC3339),
		"updated_at":    time.Now().Format(time.RFC3339),
		"related_ids":   []string{"resource-1", "resource-2"},
		"priority":      "High",
		"scheduling_info": map[string]interface{}{
			"cron_expression": "0 0 * * *",
			"time_zone":       "UTC",
		},
		"sla_constraints": map[string]interface{}{
			"latency_ms":      500,
			"throughput_mbps": 100,
		},
		"ownership_details": map[string]interface{}{
			"owner":        "Team A",
			"organization": "Company XYZ",
		},
		"audit_trail": []map[string]string{
			{
				"timestamp": time.Now().Format(time.RFC3339),
				"user":      "admin-user",
				"action":    "Created entry",
			},
		},
	}

	_, err := store.Put(ctx, metadata)
	if err != nil {
		log.Fatalf("Failed to insert metadata: %v", err)
	}

	fmt.Println("Metadata added successfully")
}
*/
/*
Delete Metadata
Use the Delete method to remove a metadata entry.
*/
func deleteMetadata(store orbitdb.DocumentStore, ctx context.Context, id string) {
	//err := store.Delete(ctx, id)
	_, err := store.Delete(ctx, id)
	if err != nil {
		log.Fatalf("Failed to delete metadata: %v", err)
	}

	fmt.Println("Metadata deleted successfully")
}

// Generate a random UUID
func generateUUID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("Error generating UUID:", err)
		return ""
	}
	return hex.EncodeToString(b)
}
func GenerateMetadataFromResource(entry map[string]interface{}) MetadataEntry {
	metadataID := entry["_id"].(string) // Ensure `_id` is properly assigned

	// Convert resource_tags safely
	tags := convertToStringSlice(entry["resource_tags"])

	metadata := MetadataEntry{
		ID:               metadataID,
		Author:           "System", // Make sure this is set
		MetadataType:     "Resource",
		Component:        fmt.Sprintf("%s-%s", entry["resource_type"], entry["resource_def"]),
		Behaviour:        "Auto-Generated",
		Relationships:    fmt.Sprintf("Related to %s", entry["resource_grpname"]),
		AssociatedID:     metadataID, // Ensure this matches the original record
		Name:             fmt.Sprintf("Metadata for %s", entry["resource_name"]),
		Description:      fmt.Sprintf("Metadata auto-generated for resource: %s", entry["resource_name"]),
		Tags:             tags, // Ensure safe conversion
		Status:           "Active",
		CreatedBy:        "System",
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		RelatedIDs:       []string{metadataID},
		Priority:         "Medium",
		SchedulingInfo:   map[string]interface{}{"schedule": "On-Demand"},
		SLAConstraints:   map[string]interface{}{"latency": "10ms"},
		OwnershipDetails: map[string]interface{}{"owner": "DefaultOwner"},
		AuditTrail: []map[string]interface{}{
			{"action": "created", "timestamp": time.Now()},
		},
	}

	fmt.Printf("DEBUG: Generated Metadata: %+v\n", metadata) // Debug print
	return metadata
}

// Helper Function to Parse String Arrays
func parseStringArray(input interface{}) []string {
	if arr, ok := input.([]string); ok {
		return arr
	}
	if arr, ok := input.([]interface{}); ok {
		result := make([]string, len(arr))
		for i, v := range arr {
			if str, ok := v.(string); ok {
				result[i] = str
			}
		}
		return result
	}
	return []string{} // Return empty if conversion fails
}

func convertToStringSlice(input interface{}) []string {
	if input == nil {
		return []string{}
	}

	switch v := input.(type) {
	case []string:
		return v
	case []interface{}:
		var result []string
		for _, val := range v {
			if str, ok := val.(string); ok {
				result = append(result, str)
			}
		}
		return result
	default:
		return []string{}
	}
}

// GenerateMetadataFromSQL creates metadata from a SQL DML statement.
func GenerateMetadataFromSQL(sql string) MetadataEntry {
	id := fmt.Sprintf("meta-%x", sha256.Sum256([]byte(sql+time.Now().String())))
	return MetadataEntry{
		ID:           id,
		MetadataType: "sql_insert",
		Component:    "SQLite",
		Behaviour:    "DataInsertion",
		Description:  "Auto-generated metadata for SQL insert operation",
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		CreatedBy:    "system",
		Name:         "SQL Metadata Entry",
		Tags:         []string{"SQLite", "AutoGenerated"},
	}
}
