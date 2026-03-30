package api

import (
	orbitdb "berty.tech/go-orbit-db"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"io/ioutil"
	"net"
	"net/http"
	"optimusdb/app"
	"optimusdb/chat"
	"optimusdb/config"
	"optimusdb/contextualmetadata"
	"optimusdb/credentials"
	"optimusdb/datamodel"
	"optimusdb/election"
	"optimusdb/logger"
	"optimusdb/tosca"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// PeerTracker stores discovered peers
type PeerTracker struct {
	sync.Mutex
	Peers map[peer.ID]peer.AddrInfo
}

// Global peer tracker
var peerTracker = &PeerTracker{Peers: make(map[peer.ID]peer.AddrInfo)}

type enrichReq struct {
	DB      string `json:"db"`
	Table   string `json:"table"`
	MaxRows int    `json:"maxRows"`
	Greek   bool   `json:"greek"`
}

// TrackPeer adds a new peer to the list
func TrackPeer(pi peer.AddrInfo) {
	peerTracker.Lock()
	defer peerTracker.Unlock()
	peerTracker.Peers[pi.ID] = pi
}

// GetPeers returns all discovered peers
func GetPeers() []peer.AddrInfo {
	peerTracker.Lock()
	defer peerTracker.Unlock()
	peers := make([]peer.AddrInfo, 0, len(peerTracker.Peers))
	for _, info := range peerTracker.Peers {
		peers = append(peers, info)
	}
	return peers
}

// peersHandler returns a JSON list of known peers
func peersHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		peers := GetPeers()

		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(peers)
		if err != nil {
			http.Error(w, "Failed to encode peers", http.StatusInternalServerError)
			return
		}
	}
}

// LogsHandler handles GET /<context>/log?date=YYYY-MM-DD&hour=HH
func LogsHandler(kb *app.LoggerSQLite) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		date := r.URL.Query().Get("date")
		hour := r.URL.Query().Get("hour")

		if date == "" || hour == "" {
			http.Error(w, "Missing 'date' or 'hour' query parameter", http.StatusBadRequest)
			return
		}

		logs, err := kb.GetLogsForHour(date, hour)
		if err != nil {
			http.Error(w, "Failed to fetch logs: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(logs)
	}
}

// resolveTargetStore maps a dstype string to the correct OrbitDB DocumentStore pointer.
// Returns (store pointer, store name for logging, error if not found/initialized).
func resolveTargetStore(optimusdb *app.KnowledgeBaseDB, dstype string) (*orbitdb.DocumentStore, string, error) {
	switch strings.ToLower(dstype) {
	case "dsswres", "":
		if optimusdb.DsSWres == nil {
			return nil, "", fmt.Errorf("DsSWres store not initialized")
		}
		return optimusdb.DsSWres, "dsswres", nil
	case "dsswresaloc":
		if optimusdb.DsSWresaloc == nil {
			return nil, "", fmt.Errorf("DsSWresaloc store not initialized")
		}
		return optimusdb.DsSWresaloc, "dsswresaloc", nil
	case "kbmetadata":
		if optimusdb.KBMetadata == nil {
			return nil, "", fmt.Errorf("KBMetadata store not initialized")
		}
		return optimusdb.KBMetadata, "kbmetadata", nil
	case "kbdata":
		if optimusdb.KBdata == nil {
			return nil, "", fmt.Errorf("KBdata store not initialized")
		}
		return optimusdb.KBdata, "kbdata", nil
	case "tosca_imported":
		if optimusdb.DsTOSCA_Imported == nil {
			return nil, "", fmt.Errorf("DsTOSCA_Imported store not initialized")
		}
		return optimusdb.DsTOSCA_Imported, "tosca_imported", nil
	case "tosca_adt":
		if optimusdb.DsTOSCA_ADT == nil {
			return nil, "", fmt.Errorf("DsTOSCA_ADT store not initialized")
		}
		return optimusdb.DsTOSCA_ADT, "tosca_adt", nil
	case "tosca_capacities":
		if optimusdb.DsTOSCA_Capacities == nil {
			return nil, "", fmt.Errorf("DsTOSCA_Capacities store not initialized")
		}
		return optimusdb.DsTOSCA_Capacities, "tosca_capacities", nil
	case "tosca_deploymentplan":
		if optimusdb.DsTOSCA_DeploymentPlan == nil {
			return nil, "", fmt.Errorf("DsTOSCA_DeploymentPlan store not initialized")
		}
		return optimusdb.DsTOSCA_DeploymentPlan, "tosca_deploymentplan", nil
	case "tosca_eventhistory":
		if optimusdb.DsTOSCA_EventHistory == nil {
			return nil, "", fmt.Errorf("DsTOSCA_EventHistory store not initialized")
		}
		return optimusdb.DsTOSCA_EventHistory, "tosca_eventhistory", nil
	case "whoiswho":
		if optimusdb.WhoiswhoStore == nil {
			return nil, "", fmt.Errorf("WhoiswhoStore store not initialized")
		}
		return optimusdb.WhoiswhoStore, "whoiswho", nil
	default:
		return nil, "", fmt.Errorf("unknown store type: %s", dstype)
	}
}

// uploadTOSCAHandler handles TOSCA template uploads with optional full structure storage
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File               string `json:"file"`
		Filename           string `json:"filename,omitempty"`
		StoreFullStructure bool   `json:"store_full_structure,omitempty"`
		TargetStore        string `json:"target_store,omitempty"` // NEW: dstype key
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST is allowed")
			return
		}

		var req UploadRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.File == "" {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		// 1) Base64 decode
		decoded, err := base64.StdEncoding.DecodeString(req.File)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Base64 decoding failed")
			return
		}

		ctx := r.Context()
		//templateID := tosca.ComputeTemplateID(decoded)
		filename := req.Filename
		if filename == "" {
			filename = "unknown"
		}
		// FIX: When a filename (swarmID) is provided, incorporate it into the template ID
		// hash so that files with identical content but different filenames produce distinct
		// _id values in OrbitDB. Without this, same-content files share the same _id and
		// the second Put() silently overwrites the first (OrbitDB DocumentStore keying).
		// The pure content hash is still stored separately in the content_hash metadata
		// field for deduplication auditing.
		var templateID string
		if filename != "" && filename != "unknown" {
			templateID = tosca.ComputeTemplateIDWithSeed(filename, decoded)
		} else {
			templateID = tosca.ComputeTemplateID(decoded)
		}

		// 2) Determine storage strategy based on request parameter
		if req.StoreFullStructure {
			// ============================================================
			// NEW APPROACH: Store full parsed structure for queryability
			// ============================================================

			// Parse TOSCA YAML to complete JSON structure
			toscaDoc, err := tosca.ParseTOSCAToFullJSON(decoded)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
				return
			}

			// Add system fields for tracking and lineage
			toscaDoc["_id"] = templateID
			toscaDoc["_original_yaml"] = string(decoded)
			toscaDoc["_imported_at"] = time.Now().UTC().Format(time.RFC3339)
			toscaDoc["_filename"] = filename
			toscaDoc["_storage_type"] = "full_structure"

			// Add lineage metadata
			uploader := r.Header.Get("X-User")
			if uploader == "" {
				uploader = app.GetAgentName()
			}
			sourcePod := os.Getenv("POD_NAME")
			sourceIP, _ := getLocalIPAddress()

			toscaDoc["_lineage"] = map[string]interface{}{
				"uploader":   uploader,
				"source_pod": sourcePod,
				"source_ip":  sourceIP,
			}

			// Resolve target store (defaults to dsswres if not specified)
			targetStore, storeName, err := resolveTargetStore(optimusdb, req.TargetStore)
			if err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Target store error: %v", err))
				return
			}
			if _, err := (*targetStore).Put(ctx, toscaDoc); err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist to %s: %v", storeName, err))
				return
			}

			// Trigger automatic metadata extraction and lineage tracking
			if optimusdb.Interceptor != nil {
				if err := optimusdb.Interceptor.OnDocumentPut(toscaDoc, storeName); err != nil {
					logger.Warn("Metadata extraction failed for TOSCA upload %s: %v", templateID, err)
				} else {
					logger.Lineage("TOSCA document %s indexed with automatic lineage tracking", templateID)
				}
			}

			// ── Add to IPFS (hoisted out — needed by both SQLite index AND metadata goroutine) ──
			var ipfsPath string
			if optimusdb.Orbit != nil {
				coreAPI := (*optimusdb.Orbit).IPFS()
				nd := files.NewBytesFile(decoded)
				p, err := coreAPI.Unixfs().Add(ctx, nd)
				if err == nil {
					ipfsPath = p.String()
				}
			}

			// Also index in SQLite for fast lookups
			if app.GlobalKBSQLite != nil {
				nodeCount := tosca.CountNodeTemplatesFromJSON(toscaDoc)
				description := extractDescription(toscaDoc)

				filesize := int64(len(decoded))
				sum := sha256.Sum256(decoded)
				sha := fmt.Sprintf("%x", sum[:])

				app.GlobalKBSQLite.InsertTOSCAMetadata(
					templateID, description, nodeCount, filename,
					filesize, sha, ipfsPath, uploader, sourcePod, sourceIP,
				)
			}

			// ── AUTO-GENERATE EXTENDED METADATA → KBMetadata + SQLite ────
			go func() {
				defer func() {
					if r := recover(); r != nil {
						logger.Warn("Panic in TOSCA metadata auto-generation: %v", r)
					}
				}()

				metaEntry := datamodel.GenerateMetadataFromTOSCA(
					templateID,
					filename,
					decoded,
					toscaDoc,
					ipfsPath,
					uploader,
					sourcePod,
					sourceIP,
					storeName,
					app.GetAgentName(),
				)

				// 1) OrbitDB KBMetadata → CRDT-replicates to all peers
				if optimusdb.KBMetadata != nil && *optimusdb.KBMetadata != nil {
					metadataMap := datamodel.ConvertMetadataToMap(metaEntry)
					metaCtx, metaCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer metaCancel()

					_, metaErr := (*optimusdb.KBMetadata).Put(metaCtx, metadataMap)
					if metaErr != nil {
						logger.Warn("Failed to store TOSCA metadata in KBMetadata: %v", metaErr)
					} else {
						logger.Info("TOSCA metadata auto-generated in KBMetadata: %s → associated_id=%s",
							metaEntry.ID, templateID)
					}
				}

				// 2) SQLite metadata_catalog → fast local SQL queries (all 48 columns)
				if app.GlobalKBSQLite != nil && app.GlobalKBSQLite.DB != nil {
					insertSQL := `
					INSERT OR REPLACE INTO metadata_catalog (
						id, author, metadata_type, component, behaviour,
						relationships, associated_id, name, description, tags,
						status, created_by, created_at, updated_at, related_ids,
						priority, scheduling_info, sla_constraints, ownership_details, audit_trail,
						data_domain, data_classification, geo_location, temporal_coverage, data_quality_score,
						schema_version, content_hash, file_format, file_size_bytes, record_count,
						update_frequency, retention_policy, access_control, compliance_tags, provenance_chain,
						processing_status, api_endpoint, version, parent_id, expiry_date,
						language, license_type, contact_info, node_count, ipfs_cid,
						source_agent, source_pod, source_ip
					) VALUES (
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?
					)`

					metaMap := datamodel.ConvertMetadataToMap(metaEntry)
					_, sqlErr := app.GlobalKBSQLite.DB.Exec(insertSQL,
						metaMap["_id"], metaMap["author"], metaMap["metadata_type"],
						metaMap["component"], metaMap["behaviour"],
						metaMap["relationships"], metaMap["associated_id"],
						metaMap["name"], metaMap["description"],
						strings.Join(metaEntry.Tags, ","),
						metaMap["status"], metaMap["created_by"],
						metaMap["created_at"], metaMap["updated_at"],
						strings.Join(metaEntry.RelatedIDs, ","),
						metaMap["priority"],
						toJSON(metaEntry.SchedulingInfo), toJSON(metaEntry.SLAConstraints),
						toJSON(metaEntry.OwnershipDetails), toJSON(metaEntry.AuditTrail),
						metaMap["data_domain"], metaMap["data_classification"],
						metaMap["geo_location"], metaMap["temporal_coverage"],
						metaEntry.DataQualityScore,
						metaMap["schema_version"], metaMap["content_hash"],
						metaMap["file_format"], metaEntry.FileSizeBytes,
						metaEntry.RecordCount,
						metaMap["update_frequency"], metaMap["retention_policy"],
						metaMap["access_control"], metaMap["compliance_tags"],
						metaMap["provenance_chain"],
						metaMap["processing_status"], metaMap["api_endpoint"],
						metaMap["version"], metaMap["parent_id"],
						metaMap["expiry_date"],
						metaMap["language"], metaMap["license_type"],
						metaMap["contact_info"], metaEntry.NodeCount,
						metaMap["ipfs_cid"],
						metaMap["source_agent"], metaMap["source_pod"],
						metaMap["source_ip"],
					)
					if sqlErr != nil {
						logger.Warn("Failed to store TOSCA metadata in SQLite: %v", sqlErr)
					}
				}

				// 3) In-memory store for fast lookups
				datamodel.Metadata.AddMetadata(metaEntry)
			}()

			// Extract sample queryable fields for response
			queryableFields := extractQueryableFieldPaths(toscaDoc, "", 50)

			logger.Info("TOSCA uploaded with full structure: %s (filename: %s, size: %d bytes)",
				templateID, filename, len(decoded))

			sendSuccessResponse(w, map[string]interface{}{
				"message":      "TOSCA uploaded with full queryable structure",
				"template_id":  templateID,
				"storage_type": "full_structure",
				//"storage_location": "dsswres",
				"storage_location": storeName,
				"filename":         filename,
				"filesize":         len(decoded),
				"queryable":        true,
				"sample_fields":    queryableFields,
				"query_info": map[string]interface{}{
					"datastore": "dsswres",
					"query_example": fmt.Sprintf(
						`{"method":{"cmd":"query"},"dstype":"dsswres","criteria":[{"field":"_id","operator":"==","value":"%s"}]}`,
						templateID,
					),
				},
			})

		} else {
			// ============================================================
			// LEGACY APPROACH: Store minimal metadata + YAML blob
			// ============================================================

			tmpl, err := tosca.ParseTOSCA(decoded)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
				return
			}

			nodeCount := tosca.CountNodeTemplates(tmpl)
			description := tmpl.Description

			// Store in DsTOSCA_Imported (legacy store)
			if optimusdb.DsTOSCA_Imported == nil {
				sendErrorResponse(w, http.StatusInternalServerError, "TOSCA store not initialized")
				return
			}

			doc := map[string]interface{}{
				"_id":         templateID,
				"type":        "tosca_template",
				"description": description,
				"nodeCount":   nodeCount,
				"yaml":        string(decoded),
				"createdAt":   time.Now().UTC().Format(time.RFC3339),
			}

			if _, err := (*optimusdb.DsTOSCA_Imported).Put(ctx, doc); err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to persist to OrbitDB: %v", err))
				return
			}

			// Trigger automatic metadata extraction for legacy TOSCA uploads
			if optimusdb.Interceptor != nil {
				if err := optimusdb.Interceptor.OnDocumentPut(doc, "tosca_imported"); err != nil {
					logger.Warn("Metadata extraction failed for legacy TOSCA upload %s: %v", templateID, err)
				} else {
					logger.Lineage("Legacy TOSCA document %s indexed", templateID)
				}
			}

			// Index in SQLite
			if app.GlobalKBSQLite != nil {
				var ipfsPath string
				if optimusdb.Orbit != nil {
					coreAPI := (*optimusdb.Orbit).IPFS()
					nd := files.NewBytesFile(decoded)
					p, err := coreAPI.Unixfs().Add(ctx, nd)
					if err == nil {
						ipfsPath = p.String()
					}
				}

				filesize := int64(len(decoded))
				sum := sha256.Sum256(decoded)
				sha := fmt.Sprintf("%x", sum[:])

				uploader := r.Header.Get("X-User")
				if uploader == "" {
					uploader = app.GetAgentName()
				}
				sourcePod := os.Getenv("POD_NAME")
				sourceIP, _ := getLocalIPAddress()

				app.GlobalKBSQLite.InsertTOSCAMetadata(
					templateID, description, nodeCount, filename,
					filesize, sha, ipfsPath, uploader, sourcePod, sourceIP,
				)
			}

			// ── FIX: AUTO-GENERATE EXTENDED METADATA for legacy uploads too ──
			// Same as full_structure path: write to KBMetadata + metadata_catalog
			go func() {
				defer func() {
					if rec := recover(); rec != nil {
						logger.Warn("Panic in legacy TOSCA metadata auto-generation: %v", rec)
					}
				}()

				// Build a toscaDoc from the raw YAML for GenerateMetadataFromTOSCA
				legacyDoc := map[string]interface{}{
					"description": description,
				}
				// Re-parse to get full structure for better metadata (best-effort)
				if fullDoc, parseErr := tosca.ParseTOSCAToFullJSON(decoded); parseErr == nil {
					legacyDoc = fullDoc
				}

				lgUploader := r.Header.Get("X-User")
				if lgUploader == "" {
					lgUploader = app.GetAgentName()
				}
				lgSourcePod := os.Getenv("POD_NAME")
				lgSourceIP, _ := getLocalIPAddress()

				var lgIpfsPath string
				if optimusdb.Orbit != nil {
					coreAPI := (*optimusdb.Orbit).IPFS()
					nd := files.NewBytesFile(decoded)
					p, ipfsErr := coreAPI.Unixfs().Add(context.Background(), nd)
					if ipfsErr == nil {
						lgIpfsPath = p.String()
					}
				}

				metaEntry := datamodel.GenerateMetadataFromTOSCA(
					templateID,
					filename,
					decoded,
					legacyDoc,
					lgIpfsPath,
					lgUploader,
					lgSourcePod,
					lgSourceIP,
					"tosca_imported",
					app.GetAgentName(),
				)

				// 1) OrbitDB KBMetadata → CRDT-replicates to all peers
				if optimusdb.KBMetadata != nil && *optimusdb.KBMetadata != nil {
					metadataMap := datamodel.ConvertMetadataToMap(metaEntry)
					metaCtx, metaCancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer metaCancel()

					_, metaErr := (*optimusdb.KBMetadata).Put(metaCtx, metadataMap)
					if metaErr != nil {
						logger.Warn("Failed to store legacy TOSCA metadata in KBMetadata: %v", metaErr)
					} else {
						logger.Info("Legacy TOSCA metadata auto-generated in KBMetadata: %s → associated_id=%s",
							metaEntry.ID, templateID)
					}
				}

				// 2) SQLite metadata_catalog → fast local SQL queries
				if app.GlobalKBSQLite != nil && app.GlobalKBSQLite.DB != nil {
					insertSQL := `
					INSERT OR REPLACE INTO metadata_catalog (
						id, author, metadata_type, component, behaviour,
						relationships, associated_id, name, description, tags,
						status, created_by, created_at, updated_at, related_ids,
						priority, scheduling_info, sla_constraints, ownership_details, audit_trail,
						data_domain, data_classification, geo_location, temporal_coverage, data_quality_score,
						schema_version, content_hash, file_format, file_size_bytes, record_count,
						update_frequency, retention_policy, access_control, compliance_tags, provenance_chain,
						processing_status, api_endpoint, version, parent_id, expiry_date,
						language, license_type, contact_info, node_count, ipfs_cid,
						source_agent, source_pod, source_ip
					) VALUES (
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?, ?, ?,
						?, ?, ?, ?, ?,  ?, ?, ?
					)`

					metaMap := datamodel.ConvertMetadataToMap(metaEntry)
					_, sqlErr := app.GlobalKBSQLite.DB.Exec(insertSQL,
						metaMap["_id"], metaMap["author"], metaMap["metadata_type"],
						metaMap["component"], metaMap["behaviour"],
						metaMap["relationships"], metaMap["associated_id"],
						metaMap["name"], metaMap["description"],
						strings.Join(metaEntry.Tags, ","),
						metaMap["status"], metaMap["created_by"],
						metaMap["created_at"], metaMap["updated_at"],
						strings.Join(metaEntry.RelatedIDs, ","),
						metaMap["priority"],
						toJSON(metaEntry.SchedulingInfo), toJSON(metaEntry.SLAConstraints),
						toJSON(metaEntry.OwnershipDetails), toJSON(metaEntry.AuditTrail),
						metaMap["data_domain"], metaMap["data_classification"],
						metaMap["geo_location"], metaMap["temporal_coverage"],
						metaEntry.DataQualityScore,
						metaMap["schema_version"], metaMap["content_hash"],
						metaMap["file_format"], metaEntry.FileSizeBytes,
						metaEntry.RecordCount,
						metaMap["update_frequency"], metaMap["retention_policy"],
						metaMap["access_control"], metaMap["compliance_tags"],
						metaMap["provenance_chain"],
						metaMap["processing_status"], metaMap["api_endpoint"],
						metaMap["version"], metaMap["parent_id"],
						metaMap["expiry_date"],
						metaMap["language"], metaMap["license_type"],
						metaMap["contact_info"], metaEntry.NodeCount,
						metaMap["ipfs_cid"],
						metaMap["source_agent"], metaMap["source_pod"],
						metaMap["source_ip"],
					)
					if sqlErr != nil {
						logger.Warn("Failed to store legacy TOSCA metadata in SQLite: %v", sqlErr)
					}
				}

				// 3) In-memory store for fast lookups
				datamodel.Metadata.AddMetadata(metaEntry)
			}()

			logger.Info("TOSCA uploaded (legacy mode): %s (filename: %s, nodes: %d)",
				templateID, filename, nodeCount)

			sendSuccessResponse(w, map[string]interface{}{
				"message":          "TOSCA uploaded (legacy mode)",
				"template_id":      templateID,
				"storage_type":     "yaml_blob",
				"storage_location": "tosca_imported",
				"node_count":       nodeCount,
				"filename":         filename,
				"filesize":         len(decoded),
				"queryable":        false,
				"note":             "Set store_full_structure:true for queryable fields",
			})
		}
	}
}

// extractDescription extracts description from parsed TOSCA JSON
func extractDescription(toscaDoc map[string]interface{}) string {
	if desc, ok := toscaDoc["description"].(string); ok {
		return desc
	}
	if metadata, ok := toscaDoc["metadata"].(map[string]interface{}); ok {
		if desc, ok := metadata["template_name"].(string); ok {
			return desc
		}
	}
	return "No description"
}

// extractQueryableFieldPaths extracts sample queryable field paths from TOSCA structure
func extractQueryableFieldPaths(doc map[string]interface{}, prefix string, limit int) []string {
	fields := []string{}
	count := 0

	var extract func(string, interface{})
	extract = func(path string, obj interface{}) {
		if count >= limit {
			return
		}

		switch v := obj.(type) {
		case map[string]interface{}:
			for key, val := range v {
				newPath := key
				if path != "" {
					newPath = path + "." + key
				}
				// Skip internal fields except _id
				if !strings.HasPrefix(key, "_") || key == "_id" {
					fields = append(fields, newPath)
					count++
					if count < limit {
						extract(newPath, val)
					}
				}
			}
		case []interface{}:
			// Show array notation
			fields = append(fields, path+"[]")
			count++
		}
	}

	extract(prefix, doc)
	return fields
}

// benchmarksHandler gathers all benchmark data from known peers
func benchmarksHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		o := *optimusdb.Orbit
		ctx := context.Background()
		cinfo, err := o.IPFS().Swarm().Peers(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		client := &http.Client{}
		var benchmarks []app.Benchmark
		for _, c := range cinfo {
			ma := c.Address()
			ip, err := extractIPFromMultiaddr(ma)
			if err != nil {
				logger.Warn("Failed to extract IP from multiaddr %s: %v", ma, err)
				continue
			}

			bm, err := getBenchmark(client, ip)
			if err != nil {
				logger.Warn("Failed to get benchmark from peer %s: %v", ip, err)
				continue
			}

			benchmarks = append(benchmarks, bm)
		}
		benchmarks = append(benchmarks, *optimusdb.Benchmark)

		// convert data to json
		jsonData, err := json.Marshal(benchmarks)
		if err != nil {
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		// send response
		w.WriteHeader(http.StatusOK)
		w.Write(jsonData)
	}
}

// getBenchmark retrieves benchmark data from a peer
func getBenchmark(client *http.Client, peerIP string) (app.Benchmark, error) {
	var bm app.Benchmark

	bmReq := app.Request{Method: app.BENCHMARK, Args: []string{}}
	jsonData, err := json.Marshal(bmReq)
	if err != nil {
		return bm, err
	}

	// send get benchmark request
	cmdPath := "http://" + peerIP + ":" + *config.FlagHTTPPort + "/" + *config.FlagContext + "/command"
	req, err := http.NewRequest("POST", cmdPath, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Printf("There is an error in the request: %v\n", err)
		logger.Error("There is an error in the request: %v with error: %v", req, err)
		return bm, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return bm, err
	}
	defer resp.Body.Close()

	// read response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("There is an error in the read response body: %v\n", err)
		logger.Error("There is an error in the read response body: %v with error: %v", body, err)
		return bm, err
	}

	// unmarshal response
	err = json.Unmarshal(body, &bm)
	if err != nil {
		fmt.Printf("There is an error in the unmarshal response response body: %v\n", err)
		logger.Error("There is an error in the unmarshal response response body: %v with error: %v", body, err)
		return bm, err
	}

	return bm, nil
}

func extractIPFromMultiaddr(maddr multiaddr.Multiaddr) (string, error) {
	re := regexp.MustCompile(`/ip4/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})`)
	match := re.FindStringSubmatch(maddr.String())

	if len(match) >= 2 {
		return match[1], nil
	}

	return "", fmt.Errorf("No ip found in ma " + maddr.String())
}

// commandHandler handles HTTP requests and routes them to the service layer
func commandHandler(reqChan chan<- app.Request, resChan <-chan interface{}) http.HandlerFunc {

	type HTTPRequest struct {
		Method          app.Method               `json:"method"`
		Args            []string                 `json:"args"`
		File            string                   `json:"file"`
		DSType          string                   `json:"dstype"`
		Criteria        []map[string]interface{} `json:"criteria"`
		UpdateData      []map[string]interface{} `json:"UpdateData"`
		Graph_traversal []map[string]interface{} `json:"graph_Traversal"`
		SQLDML          string                   `json:"sqldml"`
	}

	logger.Debug("Command handler initialized")

	return func(w http.ResponseWriter, r *http.Request) {
		logger.Debug("Processing command request from %s", r.RemoteAddr)

		if r.Method != "POST" {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Method not allowed")
			return
		}

		var req HTTPRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		serviceReq := app.Request{
			Method:          req.Method,
			Args:            req.Args,
			DSType:          req.DSType,
			Criteria:        req.Criteria,
			UpdateData:      req.UpdateData,
			SQLDML:          req.SQLDML,
			Graph_traversal: req.Graph_traversal,
		}

		if serviceReq.Method == app.POST {
			decoded, err := base64.StdEncoding.DecodeString(req.File)
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "Error decoding Base64")
				return
			}
			serviceReq.Args = append(serviceReq.Args, string(decoded))
		}

		reqChan <- serviceReq // send request to processing
		res := <-resChan      // wait for response

		_, err = json.Marshal(res)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Internal Server Error, parsing the service Request json Marshal")
			return
		}

		if result, ok := res.(map[string]interface{}); ok && result["error"] != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Error processing request")
		} else {
			sendSuccessResponse(w, res)
		}
	}
}

// sendErrorResponse sends an error response
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  http.Error,
		"message": message,
	})
}

// sendSuccessResponse sends a success response
func sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": http.StatusOK,
		"data":   data,
	})
}

// orbitDBMeshHandler returns comprehensive OrbitDB mesh connectivity status
func optimusdbMeshHandler(kb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET is allowed")
			return
		}

		status := make(map[string]interface{})

		// ═══════════════════════════════════════════════════════════════
		// 1. SELF IDENTITY
		// ═══════════════════════════════════════════════════════════════
		selfID := kb.Node.Identity
		status["self_id"] = selfID.String()
		status["self_id_short"] = selfID.String()[:12]
		status["agent_name"] = app.GetAgentName()
		status["timestamp"] = time.Now().UTC().Format(time.RFC3339)

		// ═══════════════════════════════════════════════════════════════
		// 2. LIBP2P TRANSPORT LAYER
		// ═══════════════════════════════════════════════════════════════
		libp2pPeers := kb.Node.PeerHost.Network().Peers()
		libp2pInfo := make([]map[string]interface{}, 0)

		for _, peerID := range libp2pPeers {
			connectedness := kb.Node.PeerHost.Network().Connectedness(peerID)
			conns := kb.Node.PeerHost.Network().ConnsToPeer(peerID)

			peerInfo := map[string]interface{}{
				"peer_id":       peerID.String(),
				"peer_id_short": peerID.String()[:12],
				"connectedness": connectedness.String(),
				"connections":   len(conns),
				"addresses":     []string{},
			}

			// Get peer addresses
			addrs := kb.Node.PeerHost.Peerstore().Addrs(peerID)
			addrStrs := make([]string, len(addrs))
			for i, addr := range addrs {
				addrStrs[i] = addr.String()
			}
			peerInfo["addresses"] = addrStrs

			libp2pInfo = append(libp2pInfo, peerInfo)
		}

		status["libp2p"] = map[string]interface{}{
			"connected_peers": len(libp2pPeers),
			"peers":           libp2pInfo,
		}

		// ═══════════════════════════════════════════════════════════════
		// 3. GOSSIPSUB MESH (ELECTION LAYER)
		// ═══════════════════════════════════════════════════════════════
		gossipsubInfo := map[string]interface{}{}

		if kb.PubSub != nil && kb.ElectionTopic != nil {
			topicPeers := kb.ElectionTopic.ListPeers()
			meshPeers := kb.PubSub.ListPeers("optimusdb")

			topicPeerStrs := make([]string, len(topicPeers))
			for i, p := range topicPeers {
				topicPeerStrs[i] = p.String()[:12]
			}

			meshPeerStrs := make([]string, len(meshPeers))
			for i, p := range meshPeers {
				meshPeerStrs[i] = p.String()[:12]
			}

			gossipsubInfo["topic_subscribers"] = len(topicPeers)
			gossipsubInfo["mesh_peers"] = len(meshPeers)
			gossipsubInfo["topic_peer_ids"] = topicPeerStrs
			gossipsubInfo["mesh_peer_ids"] = meshPeerStrs
		} else {
			gossipsubInfo["error"] = "GossipSub not initialized"
		}

		status["gossipsub"] = gossipsubInfo

		// ═══════════════════════════════════════════════════════════════
		// 4. DISCOVERY LAYER
		// ═══════════════════════════════════════════════════════════════
		discoveredPeers := kb.GetDiscoveredPeers()
		discoveredShort := make([]string, len(discoveredPeers))
		for i, p := range discoveredPeers {
			if len(p) >= 12 {
				discoveredShort[i] = p[:12]
			} else {
				discoveredShort[i] = p
			}
		}

		status["discovery"] = map[string]interface{}{
			"discovered_count": len(discoveredPeers),
			"discovered_peers": discoveredShort,
		}

		// ═══════════════════════════════════════════════════════════════
		// 5. ORBITDB STORES STATUS
		// ═══════════════════════════════════════════════════════════════
		stores := make(map[string]interface{})

		// Helper to safely get store address
		getStoreAddress := func(storeName string, storePtr interface{}) string {
			if storePtr == nil {
				return ""
			}

			// Use type assertion to get Address() method
			type addressable interface {
				Address() interface{ String() string }
			}

			if addr, ok := storePtr.(addressable); ok {
				return addr.Address().String()
			}
			return "unknown"
		}

		// Check Contributions (EventLogStore)
		if kb.Contributions != nil {
			stores["contributions"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("contributions", *kb.Contributions),
				"type":        "EventLogStore",
			}
		} else {
			stores["contributions"] = map[string]interface{}{"initialized": false}
		}

		// Check DsSWres (DocumentStore)
		if kb.DsSWres != nil {
			stores["dsswres"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("dsswres", *kb.DsSWres),
				"type":        "DocumentStore",
			}
		} else {
			stores["dsswres"] = map[string]interface{}{"initialized": false}
		}

		// Check DsSWresaloc (DocumentStore)
		if kb.DsSWresaloc != nil {
			stores["dsswresaloc"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("dsswresaloc", *kb.DsSWresaloc),
				"type":        "DocumentStore",
			}
		} else {
			stores["dsswresaloc"] = map[string]interface{}{"initialized": false}
		}

		// Check KBMetadata (DocumentStore)
		if kb.KBMetadata != nil {
			stores["kbmetadata"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("kbmetadata", *kb.KBMetadata),
				"type":        "DocumentStore",
			}
		} else {
			stores["kbmetadata"] = map[string]interface{}{"initialized": false}
		}

		// Check KBdata (DocumentStore)
		if kb.KBdata != nil {
			stores["kbdata"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("kbdata", *kb.KBdata),
				"type":        "DocumentStore",
			}
		} else {
			stores["kbdata"] = map[string]interface{}{"initialized": false}
		}

		// Check Validations (DocumentStore)
		if kb.Validations != nil {
			stores["validations"] = map[string]interface{}{
				"initialized": true,
				"address":     getStoreAddress("validations", *kb.Validations),
				"type":        "DocumentStore",
			}
		} else {
			stores["validations"] = map[string]interface{}{"initialized": false}
		}

		status["orbitdb_stores"] = stores

		// ═══════════════════════════════════════════════════════════════
		// 6. MESH HEALTH SUMMARY
		// ═══════════════════════════════════════════════════════════════
		discoveredCount := len(discoveredPeers)
		libp2pCount := len(libp2pPeers)

		meshHealth := "UNKNOWN"
		meshCoverage := 0.0

		if discoveredCount > 0 {
			meshCoverage = float64(libp2pCount) / float64(discoveredCount) * 100

			if meshCoverage >= 90 {
				meshHealth = "EXCELLENT"
			} else if meshCoverage >= 70 {
				meshHealth = "GOOD"
			} else if meshCoverage >= 50 {
				meshHealth = "FAIR"
			} else {
				meshHealth = "POOR"
			}
		} else if libp2pCount > 0 {
			meshHealth = "GOOD"
			meshCoverage = 100.0
		}

		status["mesh_health"] = map[string]interface{}{
			"status":                meshHealth,
			"coverage_percent":      fmt.Sprintf("%.1f", meshCoverage),
			"discovered_peers":      discoveredCount,
			"connected_peers":       libp2pCount,
			"missing_connections":   discoveredCount - libp2pCount,
			"can_replicate_orbitdb": libp2pCount > 0,
		}

		// ═══════════════════════════════════════════════════════════════
		// 7. REPLICATION DIAGNOSTICS
		// ═══════════════════════════════════════════════════════════════
		diagnostics := []string{}

		if libp2pCount == 0 {
			diagnostics = append(diagnostics, "❌ No LibP2P connections - OrbitDB cannot replicate")
		} else if discoveredCount > libp2pCount {
			diagnostics = append(diagnostics, fmt.Sprintf("⚠️  Missing %d connections out of %d discovered peers", discoveredCount-libp2pCount, discoveredCount))
		}

		if kb.PubSub == nil {
			diagnostics = append(diagnostics, "❌ GossipSub not initialized - election system unavailable")
		} else if kb.ElectionTopic == nil {
			diagnostics = append(diagnostics, "❌ Election topic not initialized")
		}

		if kb.DsSWres == nil {
			diagnostics = append(diagnostics, "⚠️  Primary data store (dsswres) not initialized")
		}

		if len(diagnostics) == 0 {
			diagnostics = append(diagnostics, "✅ All systems operational")
		}

		status["diagnostics"] = diagnostics

		sendJSONResponse(w, status)
	}
}

// agentStatusHandler returns comprehensive agent status including role and peer health
func agentStatusHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET is allowed")
			return
		}

		ctx := context.Background()

		// Get current node info
		h := optimusdb.Node.PeerHost
		selfPeerID := h.ID().String()

		// Get self addresses
		selfAddrs := make([]string, 0)
		for _, addr := range h.Addrs() {
			selfAddrs = append(selfAddrs, addr.String())
		}

		// Get election status
		role, currentLeader, currentTerm, leadershipCount := election.GetNodeStatus()

		// Determine if this node is the coordinator
		isCoordinator := (role == "Coordinator")
		isCurrentLeader := (role == "Coordinator" && selfPeerID == currentLeader)

		// Get self reputation/health metrics
		selfReputation, _ := election.GetPeerReputation(selfPeerID)
		var selfHealthScore float64 = 0
		if selfReputation != nil {
			selfHealthScore = election.CalculateHealthScore(*selfReputation)
		}

		// Get all peers reputation
		allReputations, err := election.GetAllPeersReputation()
		if err != nil {
			logger.Error("Failed to get peer reputations: %v", err)
			allReputations = []election.NodeReputation{}
		}

		// Create a map for quick reputation lookup
		reputationMap := make(map[string]*election.NodeReputation)
		for i := range allReputations {
			reputationMap[allReputations[i].NodeID] = &allReputations[i]
		}

		// Get connected peers from IPFS
		coreAPI := (*optimusdb.Orbit).IPFS()
		connInfo, _ := coreAPI.Swarm().Peers(ctx)
		connectedPeerIDs := make(map[string]bool)
		for _, ci := range connInfo {
			connectedPeerIDs[ci.ID().String()] = true
		}

		// Get discovered peers
		discoveredPeers := optimusdb.GetDiscoveredPeers()

		// Build peer list with roles and health
		peersList := make([]map[string]interface{}, 0)

		// Use connected peers as source
		for peerIDStr := range connectedPeerIDs {
			// Skip self
			if peerIDStr == selfPeerID {
				continue
			}

			// Try to get reputation data
			rep, hasReputation := reputationMap[peerIDStr]

			// Determine peer role
			peerRole := "Follower"
			isLeader := false
			if peerIDStr == currentLeader {
				peerRole = "Coordinator"
				isLeader = true
			}

			var peerInfo map[string]interface{}

			if hasReputation {
				healthScore := election.CalculateHealthScore(*rep)

				var healthStatus string
				if healthScore >= 80 {
					healthStatus = "Excellent"
				} else if healthScore >= 60 {
					healthStatus = "Good"
				} else if healthScore >= 40 {
					healthStatus = "Fair"
				} else if healthScore >= 20 {
					healthStatus = "Poor"
				} else {
					healthStatus = "Critical"
				}

				peerInfo = map[string]interface{}{
					"peer_id":   peerIDStr,
					"role":      peerRole,
					"is_leader": isLeader,
					"connected": true,
					"health": map[string]interface{}{
						"score":        fmt.Sprintf("%.2f", healthScore),
						"status":       healthStatus,
						"cpu_usage":    fmt.Sprintf("%.2f%%", rep.UserCPU+rep.SystemCPU),
						"cpu_idle":     fmt.Sprintf("%.2f%%", rep.IdleCPU),
						"memory_used":  fmt.Sprintf("%.2f MB", rep.MemoryAvailable),
						"memory_total": fmt.Sprintf("%.2f MB", rep.MemoryAllocationTotal),
						"memory_sys":   fmt.Sprintf("%.2f MB", rep.MemorySystem),
						"disk_read":    fmt.Sprintf("%.2f MB/s", rep.AvgReadMBs),
						"disk_write":   fmt.Sprintf("%.2f MB/s", rep.AvgWriteMBs),
						"latency":      fmt.Sprintf("%.2f ms", rep.Latency),
						"uptime":       fmt.Sprintf("%.2f", rep.Uptime),
					},
					"metrics": map[string]interface{}{
						"leadership_count": rep.LeadershipCount,
						"geography_score":  rep.GeographyScore,
					},
				}
			} else {
				peerInfo = map[string]interface{}{
					"peer_id":   peerIDStr,
					"role":      peerRole,
					"is_leader": isLeader,
					"connected": true,
					"health": map[string]interface{}{
						"score":        "50.00",
						"status":       "Connected",
						"cpu_usage":    "N/A",
						"cpu_idle":     "N/A",
						"memory_used":  "N/A",
						"memory_total": "N/A",
						"memory_sys":   "N/A",
						"disk_read":    "N/A",
						"disk_write":   "N/A",
						"latency":      "10.00 ms",
						"uptime":       "N/A",
					},
					"metrics": map[string]interface{}{
						"leadership_count": 0,
						"geography_score":  0,
					},
				}
			}

			peersList = append(peersList, peerInfo)
		}

		// Get latest election info
		_, lastElectionTerm, lastElectionTime, _ := election.GetLatestElectionInfo()

		// Build self health info
		var selfHealth map[string]interface{}
		if selfReputation != nil {
			healthStatus := "Unknown"
			if selfHealthScore >= 80 {
				healthStatus = "Excellent"
			} else if selfHealthScore >= 60 {
				healthStatus = "Good"
			} else if selfHealthScore >= 40 {
				healthStatus = "Fair"
			} else if selfHealthScore >= 20 {
				healthStatus = "Poor"
			} else {
				healthStatus = "Critical"
			}

			selfHealth = map[string]interface{}{
				"score":        fmt.Sprintf("%.2f", selfHealthScore),
				"status":       healthStatus,
				"cpu_usage":    fmt.Sprintf("%.2f%%", selfReputation.UserCPU+selfReputation.SystemCPU),
				"cpu_idle":     fmt.Sprintf("%.2f%%", selfReputation.IdleCPU),
				"memory_used":  fmt.Sprintf("%.2f MB", selfReputation.MemoryAvailable),
				"memory_total": fmt.Sprintf("%.2f MB", selfReputation.MemoryAllocationTotal),
				"memory_sys":   fmt.Sprintf("%.2f MB", selfReputation.MemorySystem),
				"disk_read":    fmt.Sprintf("%.2f MB/s", selfReputation.AvgReadMBs),
				"disk_write":   fmt.Sprintf("%.2f MB/s", selfReputation.AvgWriteMBs),
				"latency":      fmt.Sprintf("%.2f ms", selfReputation.Latency),
				"uptime":       fmt.Sprintf("%.2f", selfReputation.Uptime),
			}
		} else {
			selfHealth = map[string]interface{}{
				"score":  "N/A",
				"status": "Initializing",
			}
		}

		// Count coordinators and followers
		coordCount := 0
		followerCount := 0
		for _, peer := range peersList {
			if peer["role"] == "Coordinator" {
				coordCount++
			} else {
				followerCount++
			}
		}
		// Add self to counts
		if role == "Coordinator" {
			coordCount++
		} else {
			followerCount++
		}

		// Build complete response
		response := map[string]interface{}{
			"status": "success",
			"agent": map[string]interface{}{
				"peer_id":           selfPeerID,
				"addresses":         selfAddrs,
				"role":              role,
				"is_coordinator":    isCoordinator,
				"is_current_leader": isCurrentLeader,
				"health":            selfHealth,
				"metrics": map[string]interface{}{
					"leadership_count": leadershipCount,
				},
			},
			"election": map[string]interface{}{
				"current_leader":     currentLeader,
				"current_term":       currentTerm,
				"last_election_time": lastElectionTime,
				"last_election_term": lastElectionTerm,
			},
			"cluster": map[string]interface{}{
				"total_peers":      len(peersList) + 1,
				"connected_peers":  len(connectedPeerIDs) - 1,
				"discovered_peers": len(discoveredPeers),
				"coordinators":     coordCount,
				"followers":        followerCount,
			},
			"peers": peersList,
			"configuration": map[string]interface{}{
				"context":   *config.FlagContext,
				"http_port": *config.FlagHTTPPort,
			},
			"timestamp": time.Now().UTC().Format(time.RFC3339),
		}

		sendJSONResponse(w, response)
	}
}

// ServeHTTP initializes and starts the HTTP server
func ServeHTTP(optimusdb *app.KnowledgeBaseDB, theLog *app.LoggerSQLite, reqChan chan app.Request,
	resChan chan interface{}, logChan chan app.Log) {

	server := http.NewServeMux()

	// middleware to handle CORS headers and preflight requests
	mw := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			logChan <- app.Log{app.Info, "Received HTTP request from " + ip}
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// Register command handler
	server.Handle("/"+*config.FlagContext+"/command", mw(commandHandler(reqChan, resChan)))

	// Agent status endpoint
	server.Handle("/"+*config.FlagContext+"/agent/status", mw(agentStatusHandler(optimusdb)))

	// TOSCA upload endpoint
	server.Handle("/"+*config.FlagContext+"/upload", mw(uploadTOSCAHandler(optimusdb)))

	// Peers endpoint
	server.Handle("/"+*config.FlagContext+"/peers", mw(peersHandler()))

	// EMS endpoints
	server.Handle("/"+*config.FlagContext+"/ems",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sendSuccessResponse(w, map[string]string{
				"hint": "Try /" + *config.FlagContext + "/ems/logs and /" + *config.FlagContext + "/ems/events",
			})
		})))

	// Logging endpoint
	server.Handle("/"+*config.FlagContext+"/log", mw(LogsHandler(theLog)))

	// ✅ NEW: OrbitDB mesh status endpoint
	server.Handle("/"+*config.FlagContext+"/debug/optimusdb/mesh", mw(optimusdbMeshHandler(optimusdb)))
	logger.Info("Registered optimusdb mesh debug endpoint: /%s/debug/optimusdb/mesh", *config.FlagContext)

	// EMS logs endpoint with filters
	server.Handle("/"+*config.FlagContext+"/ems/logs",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			q := r.URL.Query()

			// limit (safe clamp)
			limit := 50
			if s := q.Get("limit"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 1000 {
					limit = n
				}
			}
			// level (whitelist)
			level := strings.ToUpper(strings.TrimSpace(q.Get("level")))
			if level != "INFO" && level != "WARN" && level != "ERROR" && level != "DEBUG" {
				level = ""
			}
			// since_min (relative time window)
			sinceMin := 0
			if s := q.Get("since_min"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 24*60 {
					sinceMin = n
				}
			}

			// Build WHERE
			where := `source = 'ems'`
			if level != "" {
				where += fmt.Sprintf(` AND level = '%s'`, level)
			}
			if sinceMin > 0 {
				where += fmt.Sprintf(` AND timestamp >= datetime('now','-%d minutes')`, sinceMin)
			}

			sql := fmt.Sprintf(`
			SELECT id, timestamp, level, source, message
			FROM optimusLogger
			WHERE %s
			ORDER BY id DESC
			LIMIT %d;`, where, limit)

			rows, err := app.GlobalLoggerDB.SelectAll(sql)
			if err != nil {
				sendErrorResponse(w, http.StatusInternalServerError, "query failed: "+err.Error())
				return
			}
			sendJSONResponse(w, map[string]interface{}{"records": rows})
		})))

	// EMS events endpoint
	server.Handle("/"+*config.FlagContext+"/ems/events",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			q := r.URL.Query()
			limit := 50
			if s := q.Get("limit"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 1000 {
					limit = n
				}
			}
			sinceMin := 0
			if s := q.Get("since_min"); s != "" {
				if n, err := strconv.Atoi(s); err == nil && n > 0 && n <= 24*60 {
					sinceMin = n
				}
			}

			where := `1=1`
			if sinceMin > 0 {
				where += fmt.Sprintf(` AND received_at >= datetime('now','-%d minutes')`, sinceMin)
			}

			sql := fmt.Sprintf(`
			SELECT id, received_at, client_id, topic, action, resource,
			       substr(params_json,1,240) AS params,
			       substr(raw_json,1,240)    AS raw
			FROM ems_events
			WHERE %s
			ORDER BY id DESC
			LIMIT %d;`, where, limit)

			rows, err := app.GlobalLoggerDB.SelectAll(sql)
			if err != nil {
				if strings.Contains(strings.ToLower(err.Error()), "no such table") {
					sendErrorResponse(w, http.StatusNotFound, "ems_events table not found (enable EMS persistence or redeploy with events table)")
					return
				}
				sendErrorResponse(w, http.StatusInternalServerError, "query failed: "+err.Error())
				return
			}
			sendJSONResponse(w, map[string]interface{}{"records": rows})
		})))

	// EMS SQL endpoint
	server.Handle("/"+*config.FlagContext+"/ems/sql",
		mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if app.GlobalLoggerDB == nil {
				sendErrorResponse(w, http.StatusServiceUnavailable, "logger DB not ready")
				return
			}
			var sql string
			if r.Method == http.MethodGet {
				sql = r.URL.Query().Get("q")
			} else if r.Method == http.MethodPost {
				var body struct {
					SQL string `json:"sql"`
				}
				_ = json.NewDecoder(r.Body).Decode(&body)
				sql = body.SQL
			} else {
				sendErrorResponse(w, http.StatusMethodNotAllowed, "use GET or POST")
				return
			}
			sql = strings.TrimSpace(sql)
			if sql == "" {
				sendErrorResponse(w, http.StatusBadRequest, "missing SQL")
				return
			}

			// FIX: Route SQL to the correct SQLite database.
			// Tables in KnowledgeBaseSQLite: metadata_catalog, datacatalog, toscametadata
			// Tables in LoggerSQLite: optimusLogger, ems_events
			// Auto-detect by checking if the query references a KnowledgeBase table.
			sqlUpper := strings.ToUpper(sql)
			kbTables := []string{"METADATA_CATALOG", "DATACATALOG", "TOSCAMETADATA"}
			useKBSQLite := false
			for _, t := range kbTables {
				if strings.Contains(sqlUpper, t) {
					useKBSQLite = true
					break
				}
			}

			// Also support explicit db selection via query parameter
			dbParam := r.URL.Query().Get("db")
			if dbParam == "kb" || dbParam == "knowledgebase" {
				useKBSQLite = true
			} else if dbParam == "log" || dbParam == "logger" {
				useKBSQLite = false
			}

			var rows []map[string]interface{}
			var err error
			if useKBSQLite && app.GlobalKBSQLite != nil {
				rows, err = app.GlobalKBSQLite.SelectAll(sql)
			} else {
				rows, err = app.GlobalLoggerDB.SelectAll(sql)
			}
			if err != nil {
				sendErrorResponse(w, http.StatusBadRequest, "query failed: "+err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{"records": rows})
		})))

	// Register inventory endpoint
	server.Handle("/"+*config.FlagContext+"/agent/inventory",
		mw(AgentInventoryHandler(optimusdb, app.GlobalKBSQLite, theLog)))

	// DID Endpoints
	credentials.SetupCredentialsEndpoints(server, mw, *config.FlagContext, optimusdb, theLog)

	// Add metadata routes (includes new chat handler)
	metadataRouter := mux.NewRouter()
	RegisterMetadataRoutes(metadataRouter, optimusdb)
	server.Handle("/api/", mw(metadataRouter))

	// Register benchmarks handler
	if *config.FlagBenchmark {
		server.Handle("/"+*config.FlagContext+"/benchmarks", mw(benchmarksHandler(optimusdb)))
	}

	// Get the local IP address
	ip, err := getLocalIPAddress()
	if err != nil {
		logger.Warn("Failed to determine local IP address: %v", err)
		ip = "unknown"
	}

	logger.Info("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort)
	logChan <- app.Log{
		Type: app.Info,
		Data: fmt.Sprintf("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort),
	}

	http.ListenAndServe(":"+*config.FlagHTTPPort, server)
}

// GetLocalIPAddress retrieves the first non-loopback IPv4 address
func getLocalIPAddress() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		// Skip down or loopback interfaces
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			// Check if the address is IPv4
			if ipNet, ok := addr.(*net.IPNet); ok && ipNet.IP.To4() != nil {
				return ipNet.IP.String(), nil
			}
		}
	}

	return "", nil
}

// sendJSONResponse writes a 200 JSON body
func sendJSONResponse(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(payload)
}

func EnrichHandler(kb *app.KnowledgeBaseDB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req enrichReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if req.MaxRows <= 0 {
			req.MaxRows = 200
		}

		var svc contextualmetadata.Service
		svc.UseGreek = req.Greek

		entry, err := svc.EnrichDataset(r.Context(), kb, req.DB, req.Table, req.MaxRows)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(entry)
	}
}

// RegisterMetadataRoutes registers metadata enrichment endpoints including the chat handler
func RegisterMetadataRoutes(router *mux.Router, kb *app.KnowledgeBaseDB) {
	// Create API v1 subrouter
	apiV1 := router.PathPrefix("/api/v1").Subrouter()

	// ═══════════════════════════════════════════════════════════════
	// METADATA ENRICHMENT ENDPOINTS (existing)
	// ═══════════════════════════════════════════════════════════════
	if kb.MetadataService != nil && kb.MetadataCache != nil {
		metadataHandler := &contextualmetadata.MetadataHandler{
			Service: kb.MetadataService.(*contextualmetadata.Service),
			KB:      kb,
			Cache:   kb.MetadataCache.(*contextualmetadata.MetadataCache),
		}

		apiV1.HandleFunc("/metadata/enrich", metadataHandler.EnrichDataset).Methods("POST")
		apiV1.HandleFunc("/metadata/enrich-batch", metadataHandler.EnrichBatch).Methods("POST")
		apiV1.HandleFunc("/metadata/profile", metadataHandler.ProfileDataset).Methods("GET")
		apiV1.HandleFunc("/metadata/metrics", metadataHandler.GetMetrics).Methods("GET")
		apiV1.HandleFunc("/metadata/health", metadataHandler.HealthCheck).Methods("GET")
		apiV1.HandleFunc("/metadata/cache", metadataHandler.ClearCache).Methods("DELETE")

		logger.Info("Metadata enrichment endpoints registered at /api/v1/metadata")
	} else {
		logger.Info("Metadata service not initialized, skipping metadata routes")
	}

	// ═══════════════════════════════════════════════════════════════
	// CHAT HANDLER (NEW - DataCatalogAssistant integration)
	// ═══════════════════════════════════════════════════════════════
	logger.Info("[CHAT] Initializing chat handler for DataCatalogAssistant...")

	// Create query function that connects to existing KB query system
	queryFunc := createKBQueryFunc(kb)

	// Create schema function
	schemaFunc := createSchemaFunc(kb)

	// Get TinyLlama URL from environment or use default
	tinyllamaURL := os.Getenv("TINYLLAMA_URL")
	if tinyllamaURL == "" {
		tinyllamaURL = "http://localhost:11434/api/chat"
	}

	// Create the KB adapter
	adapterConfig := chat.AdapterConfig{
		TinyllamaURL: tinyllamaURL,
		QueryFunc:    queryFunc,
		SchemaFunc:   schemaFunc,
		Datasets: []chat.DatasetInfo{
			{Type: "dsswres", Name: "Solar & Wind Resources", Description: "Renewable energy asset metadata including solar panels and wind turbines"},
			{Type: "dsswresaloc", Name: "Resource Allocations", Description: "Resource allocation and scheduling data"},
			{Type: "kbmetadata", Name: "Knowledge Base Metadata", Description: "Catalog metadata including tables and columns"},
			{Type: "kbdata", Name: "Knowledge Base Data", Description: "General knowledge base documents"},
		},
		Timeout:   30 * time.Second,
		SchemaTTL: 5 * time.Minute,
	}
	chatAdapter := chat.NewKnowledgeBaseAdapter(adapterConfig)

	// Create the chat handler
	chatConfig := chat.HandlerConfig{
		DefaultDataset:   "dsswres",
		MaxHistoryLength: 10,
		EnableExecution:  true,
		GreetingEnabled:  true,
		AssistantName:    "OptimusDB Assistant",
	}
	chatHandler := chat.NewHandler(chatAdapter, chatConfig)

	// Register the chat endpoint
	apiV1.Handle("/chat", chatHandler).Methods("POST", "OPTIONS")
	apiV1.HandleFunc("/chat/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":    "healthy",
			"service":   "chat",
			"assistant": chatConfig.AssistantName,
			"tinyllama": tinyllamaURL,
			"timestamp": time.Now().Format(time.RFC3339),
		})
	}).Methods("GET")

	logger.Info("[CHAT] Chat endpoint registered at /api/v1/chat")
	logger.Info("[CHAT] Chat health endpoint registered at /api/v1/chat/health")
	logger.Info("[CHAT] TinyLlama URL: %s", tinyllamaURL)

	// ═══════════════════════════════════════════════════════════════
	// SEMANTIC SEARCH ENDPOINTS (NEW)
	// ═══════════════════════════════════════════════════════════════
	if kb.SemanticIdx != nil {
		type semanticRouter interface {
			SearchHandler(http.ResponseWriter, *http.Request)
			IndexHandler(http.ResponseWriter, *http.Request)
			BootstrapHandler(http.ResponseWriter, *http.Request)
		}
		if sidx, ok := kb.SemanticIdx.(semanticRouter); ok {
			apiV1.HandleFunc("/semantic/search", sidx.SearchHandler).Methods("GET")
			apiV1.HandleFunc("/semantic/index", sidx.IndexHandler).Methods("POST")
			apiV1.HandleFunc("/semantic/bootstrap", sidx.BootstrapHandler).Methods("POST")
			logger.Info("[SEMANTIC] Routes registered at /api/v1/semantic/{search,index,bootstrap}")
		}
	} else {
		logger.Info("[SEMANTIC] Index not initialized, skipping semantic routes")
	}
}

// createKBQueryFunc creates a query function that connects to OptimusDB's document stores
func createKBQueryFunc(kb *app.KnowledgeBaseDB) chat.QueryFunc {
	return func(ctx context.Context, dstype string, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
		logger.Debug("[CHAT-QUERY] Executing query on dstype=%s with %d criteria", dstype, len(criteria))

		// Get the appropriate store
		var store interface{}
		switch dstype {
		case "dsswres":
			if kb.DsSWres != nil {
				store = *kb.DsSWres
			}
		case "dsswresaloc":
			if kb.DsSWresaloc != nil {
				store = *kb.DsSWresaloc
			}
		case "kbmetadata":
			if kb.KBMetadata != nil {
				store = *kb.KBMetadata
			}
		case "kbdata":
			if kb.KBdata != nil {
				store = *kb.KBdata
			}
		default:
			// Default to dsswres
			if kb.DsSWres != nil {
				store = *kb.DsSWres
			}
		}

		if store == nil {
			return nil, fmt.Errorf("store %s not initialized", dstype)
		}

		// Type assert to get Query method
		type queryable interface {
			Query(ctx context.Context, filter func(doc interface{}) (bool, error)) ([]interface{}, error)
		}

		type allGettable interface {
			All(ctx context.Context) ([]interface{}, error)
		}

		// Try to get all documents if no criteria
		if len(criteria) == 0 {
			if getter, ok := store.(allGettable); ok {
				docs, err := getter.All(ctx)
				if err != nil {
					return nil, err
				}

				results := make([]map[string]interface{}, 0, len(docs))
				for _, doc := range docs {
					if m, ok := doc.(map[string]interface{}); ok {
						results = append(results, m)
					}
				}

				// Limit to 100 results
				if len(results) > 100 {
					results = results[:100]
				}

				logger.Debug("[CHAT-QUERY] Retrieved %d documents from %s", len(results), dstype)
				return results, nil
			}
		}

		// Build filter function from criteria
		filterFunc := func(doc interface{}) (bool, error) {
			docMap, ok := doc.(map[string]interface{})
			if !ok {
				return false, nil
			}

			for _, crit := range criteria {
				field, _ := crit["field"].(string)
				operator, _ := crit["operator"].(string)
				value := crit["value"]

				if field == "" {
					continue
				}

				docValue, exists := docMap[field]
				if !exists {
					return false, nil
				}

				match := false
				switch operator {
				case "==", "=":
					match = fmt.Sprintf("%v", docValue) == fmt.Sprintf("%v", value)
				case "!=":
					match = fmt.Sprintf("%v", docValue) != fmt.Sprintf("%v", value)
				case "contains":
					match = strings.Contains(
						strings.ToLower(fmt.Sprintf("%v", docValue)),
						strings.ToLower(fmt.Sprintf("%v", value)),
					)
				case ">":
					if dv, ok := toFloat(docValue); ok {
						if vv, ok := toFloat(value); ok {
							match = dv > vv
						}
					}
				case "<":
					if dv, ok := toFloat(docValue); ok {
						if vv, ok := toFloat(value); ok {
							match = dv < vv
						}
					}
				case ">=":
					if dv, ok := toFloat(docValue); ok {
						if vv, ok := toFloat(value); ok {
							match = dv >= vv
						}
					}
				case "<=":
					if dv, ok := toFloat(docValue); ok {
						if vv, ok := toFloat(value); ok {
							match = dv <= vv
						}
					}
				default:
					// Default to equality
					match = fmt.Sprintf("%v", docValue) == fmt.Sprintf("%v", value)
				}

				if !match {
					return false, nil
				}
			}

			return true, nil
		}

		// Execute query with filter
		if querier, ok := store.(queryable); ok {
			docs, err := querier.Query(ctx, filterFunc)
			if err != nil {
				return nil, err
			}

			results := make([]map[string]interface{}, 0, len(docs))
			for _, doc := range docs {
				if m, ok := doc.(map[string]interface{}); ok {
					results = append(results, m)
				}
			}

			// Limit to 100 results
			if len(results) > 100 {
				results = results[:100]
			}

			logger.Debug("[CHAT-QUERY] Query returned %d results from %s", len(results), dstype)
			return results, nil
		}

		// Fallback: try to get all and filter manually
		if getter, ok := store.(allGettable); ok {
			docs, err := getter.All(ctx)
			if err != nil {
				return nil, err
			}

			results := make([]map[string]interface{}, 0)
			for _, doc := range docs {
				match, _ := filterFunc(doc)
				if match {
					if m, ok := doc.(map[string]interface{}); ok {
						results = append(results, m)
					}
				}
			}

			// Limit to 100 results
			if len(results) > 100 {
				results = results[:100]
			}

			logger.Debug("[CHAT-QUERY] Filtered query returned %d results from %s", len(results), dstype)
			return results, nil
		}

		return nil, fmt.Errorf("store %s does not support querying", dstype)
	}
}

// createSchemaFunc creates a schema function for the chat adapter
func createSchemaFunc(kb *app.KnowledgeBaseDB) chat.SchemaFunc {
	return func(dstype string) (*chat.SchemaInfo, error) {
		// Return predefined schemas based on dataset type
		// In a real implementation, this could introspect the actual data
		schemas := map[string]*chat.SchemaInfo{
			"dsswres": {
				DatasetType: "dsswres",
				Tables: []chat.TableInfo{
					{
						Name:        "assets",
						Description: "Renewable energy assets",
						Fields: []chat.FieldInfo{
							{Name: "_id", Type: "string", Required: true},
							{Name: "name", Type: "string", Required: true},
							{Name: "type", Type: "string", Required: true},
							{Name: "location", Type: "string"},
							{Name: "capacity", Type: "number"},
							{Name: "status", Type: "string"},
							{Name: "owner", Type: "string"},
							{Name: "installed_date", Type: "date"},
							{Name: "latitude", Type: "number"},
							{Name: "longitude", Type: "number"},
						},
					},
				},
				LastUpdated: time.Now(),
			},
			"dsswresaloc": {
				DatasetType: "dsswresaloc",
				Tables: []chat.TableInfo{
					{
						Name:        "allocations",
						Description: "Resource allocations",
						Fields: []chat.FieldInfo{
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
				Tables: []chat.TableInfo{
					{
						Name:        "metadata",
						Description: "Catalog metadata entries",
						Fields: []chat.FieldInfo{
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
		}

		if schema, ok := schemas[dstype]; ok {
			return schema, nil
		}

		// Generic fallback schema
		return &chat.SchemaInfo{
			DatasetType: dstype,
			Tables: []chat.TableInfo{
				{
					Name:        "documents",
					Description: "Document store",
					Fields: []chat.FieldInfo{
						{Name: "_id", Type: "string", Required: true},
						{Name: "data", Type: "object"},
					},
				},
			},
			LastUpdated: time.Now(),
		}, nil
	}
}

// toFloat converts a value to float64 for comparison
func toFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case string:
		var f float64
		_, err := fmt.Sscanf(val, "%f", &f)
		return f, err == nil
	default:
		return 0, false
	}
}

// toJSON safely marshals a value to JSON string for SQLite storage.
func toJSON(v interface{}) string {
	if v == nil {
		return "{}"
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}
