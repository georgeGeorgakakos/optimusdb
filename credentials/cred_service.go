package credentials

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/iface"
	files "github.com/ipfs/go-ipfs-files"
	ipfscore "github.com/ipfs/kubo/core"
	"github.com/ipfs/kubo/core/coreapi"
	"optimusdb/app"
)

// Service handles all credential operations
type Service struct {
	node             *ipfscore.IpfsNode
	orbit            iface.OrbitDB
	sqliteDB         *sql.DB
	repoPath         string
	credentialsStore orbitdb.DocumentStore
	auditLog         orbitdb.EventLogStore
	loggerDB         *app.LoggerSQLite
}

// NewService creates a new credentials service
func NewService(kb *app.KnowledgeBaseDB, logger *app.LoggerSQLite) (*Service, error) {
	service := &Service{
		node:     kb.Node,
		orbit:    *kb.Orbit,
		loggerDB: logger,
	}

	// Initialize SQLite table for credentials metadata
	if err := service.initSQLiteTable(kb); err != nil {
		return nil, fmt.Errorf("failed to initialize SQLite table: %v", err)
	}

	log.Println("[CREDENTIALS] Service initialized successfully")
	if logger != nil {
		_ = logger.AddToOptimusLog("INFO", "Credentials Service initialized successfully", runtime.GOOS)
	}

	return service, nil
}

// initSQLiteTable creates the credentials_metadata table if it doesn't exist
func (s *Service) initSQLiteTable(kb *app.KnowledgeBaseDB) error {
	// Get the SQLite database from the KnowledgeBaseSQLite instance
	if app.GlobalKBSQLite == nil || app.GlobalKBSQLite.DB == nil {
		return fmt.Errorf("SQLite database not initialized")
	}

	s.sqliteDB = app.GlobalKBSQLite.DB

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS credentials_metadata (
		credential_id TEXT PRIMARY KEY,
		issuer_id TEXT,
		subject_id TEXT,
		credential_type TEXT,
		issuance_date DATETIME,
		expiration_date DATETIME,
		status TEXT,
		stored_at DATETIME,
		ipfs_hash TEXT,
		orbitdb_hash TEXT
	);`

	if _, err := s.sqliteDB.Exec(createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %v", err)
	}

	// Create indexes for better query performance
	indexes := []string{
		"CREATE INDEX IF NOT EXISTS idx_issuer ON credentials_metadata(issuer_id);",
		"CREATE INDEX IF NOT EXISTS idx_subject ON credentials_metadata(subject_id);",
		"CREATE INDEX IF NOT EXISTS idx_status ON credentials_metadata(status);",
		"CREATE INDEX IF NOT EXISTS idx_issuance_date ON credentials_metadata(issuance_date);",
	}

	for _, idxSQL := range indexes {
		if _, err := s.sqliteDB.Exec(idxSQL); err != nil {
			log.Printf("[WARN] Failed to create index: %v", err)
		}
	}

	log.Println("[CREDENTIALS] SQLite table and indexes created successfully")
	if s.loggerDB != nil {
		_ = s.loggerDB.AddToOptimusLog("INFO", "Credentials SQLite table initialized", runtime.GOOS)
	}

	return nil
}

// ValidateCredential validates the W3C VC structure
func (s *Service) ValidateCredential(vc *VerifiableCredential) error {
	// Check required @context
	if len(vc.Context) == 0 {
		return fmt.Errorf("@context is required")
	}

	// First context must be https://www.w3.org/2018/credentials/v1
	if vc.Context[0] != "https://www.w3.org/2018/credentials/v1" {
		return fmt.Errorf("first @context must be https://www.w3.org/2018/credentials/v1")
	}

	// Check required type
	if len(vc.Type) == 0 {
		return fmt.Errorf("type is required")
	}

	// Type must include "VerifiableCredential"
	hasVerifiableCredential := false
	for _, t := range vc.Type {
		if t == "VerifiableCredential" {
			hasVerifiableCredential = true
			break
		}
	}
	if !hasVerifiableCredential {
		return fmt.Errorf("type must include 'VerifiableCredential'")
	}

	// Check issuer
	if vc.Issuer == nil {
		return fmt.Errorf("issuer is required")
	}

	// Check issuance date
	if vc.IssuanceDate == "" {
		return fmt.Errorf("issuanceDate is required")
	}

	// Validate issuance date format (RFC3339)
	if _, err := time.Parse(time.RFC3339, vc.IssuanceDate); err != nil {
		return fmt.Errorf("issuanceDate must be in RFC3339 format: %v", err)
	}

	// Validate expiration date if present
	if vc.ExpirationDate != "" {
		if _, err := time.Parse(time.RFC3339, vc.ExpirationDate); err != nil {
			return fmt.Errorf("expirationDate must be in RFC3339 format: %v", err)
		}
	}

	// Check credential subject
	if vc.CredentialSubject == nil || len(vc.CredentialSubject) == 0 {
		return fmt.Errorf("credentialSubject is required and cannot be empty")
	}

	return nil
}

// VerifyProof verifies the cryptographic proof of the credential
func (s *Service) VerifyProof(vc *VerifiableCredential) error {
	// This is a placeholder for proof verification
	// In a production system, you would:
	// 1. Extract the verification method (public key/DID document)
	// 2. Verify the signature using the appropriate algorithm
	// 3. Check the proof purpose matches the intended use
	// 4. Validate challenge and domain if present

	if vc.Proof == nil {
		return nil // Proof is optional
	}

	if vc.Proof.Type == "" {
		return fmt.Errorf("proof type is required")
	}

	if vc.Proof.VerificationMethod == "" {
		return fmt.Errorf("verificationMethod is required")
	}

	if vc.Proof.ProofValue == "" && vc.Proof.Jws == "" {
		return fmt.Errorf("either proofValue or jws is required")
	}

	// TODO: Implement actual cryptographic verification
	// For now, we accept the proof as valid if it has the required fields

	return nil
}

// GenerateCredentialID generates a unique ID for the credential
func (s *Service) GenerateCredentialID(vc *VerifiableCredential) string {
	// Create a hash of the credential content
	data, _ := json.Marshal(vc)
	hash := sha256.Sum256(data)
	return "urn:uuid:vc-" + hex.EncodeToString(hash[:16])
}

// StoreCredential stores the credential in all OptimusDB datastores
func (s *Service) StoreCredential(vc *VerifiableCredential) (*CredentialResponse, error) {
	ctx := context.Background()

	// Validate credential
	if err := s.ValidateCredential(vc); err != nil {
		return nil, fmt.Errorf("validation failed: %v", err)
	}

	// Verify proof if present
	if err := s.VerifyProof(vc); err != nil {
		return nil, fmt.Errorf("proof verification failed: %v", err)
	}

	// Generate credential ID if not provided
	if vc.ID == "" {
		vc.ID = s.GenerateCredentialID(vc)
	}

	// Extract metadata
	metadata := s.extractMetadata(vc)

	// 1. Store in IPFS (immutable storage)
	ipfsHash, err := s.storeInIPFS(ctx, vc)
	if err != nil {
		return nil, fmt.Errorf("failed to store in IPFS: %v", err)
	}
	metadata.IPFSHash = ipfsHash

	// 2. Store in OrbitDB DocumentStore (distributed, mutable storage)
	orbitHash, err := s.storeInDocumentStore(ctx, vc, metadata)
	if err != nil {
		return nil, fmt.Errorf("failed to store in OrbitDB DocumentStore: %v", err)
	}
	metadata.OrbitDBHash = orbitHash

	// 3. Store audit log in EventLog (append-only log)
	if err := s.storeInEventLog(ctx, vc, metadata); err != nil {
		return nil, fmt.Errorf("failed to store in EventLog: %v", err)
	}

	// 4. Store metadata in SQLite for fast querying
	if err := s.storeInSQLite(metadata); err != nil {
		return nil, fmt.Errorf("failed to store in SQLite: %v", err)
	}

	log.Printf("[CREDENTIALS] Stored credential: %s", vc.ID)
	if s.loggerDB != nil {
		_ = s.loggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Stored credential: %s", vc.ID), runtime.GOOS)
	}

	// Create response
	response := &CredentialResponse{
		Success:      true,
		CredentialID: vc.ID,
		Message:      "Verifiable Credential stored successfully",
		IPFSHash:     ipfsHash,
		OrbitDBHash:  orbitHash,
		StorageDetails: StorageDetails{
			DocumentStore: "credentials-store",
			EventLog:      "credentials-audit-log",
			SQLiteIndex:   "credentials_metadata",
		},
	}

	return response, nil
}

// extractMetadata extracts metadata from the credential for indexing
func (s *Service) extractMetadata(vc *VerifiableCredential) *CredentialMetadata {
	metadata := &CredentialMetadata{
		CredentialID:   vc.ID,
		CredentialType: vc.Type,
		Status:         "active",
		StoredAt:       time.Now(),
	}

	// Extract issuer ID
	switch issuer := vc.Issuer.(type) {
	case string:
		metadata.IssuerID = issuer
	case map[string]interface{}:
		if id, ok := issuer["id"].(string); ok {
			metadata.IssuerID = id
		}
	}

	// Extract subject ID
	if id, ok := vc.CredentialSubject["id"].(string); ok {
		metadata.SubjectID = id
	}

	// Parse issuance date
	if issuanceDate, err := time.Parse(time.RFC3339, vc.IssuanceDate); err == nil {
		metadata.IssuanceDate = issuanceDate
	}

	// Parse expiration date
	if vc.ExpirationDate != "" {
		if expirationDate, err := time.Parse(time.RFC3339, vc.ExpirationDate); err == nil {
			metadata.ExpirationDate = &expirationDate
		}
	}

	return metadata
}

// storeInIPFS stores the credential in IPFS
func (s *Service) storeInIPFS(ctx context.Context, vc *VerifiableCredential) (string, error) {
	data, err := json.Marshal(vc)
	if err != nil {
		return "", err
	}

	// Use CoreAPI to add data to IPFS
	api, err := coreapi.NewCoreAPI(s.node)
	if err != nil {
		return "", fmt.Errorf("failed to create CoreAPI: %v", err)
	}

	// Create a file from the data
	fileNode := files.NewBytesFile(data)

	// Add the file to IPFS
	path, err := api.Unixfs().Add(ctx, fileNode)
	if err != nil {
		return "", fmt.Errorf("failed to add to IPFS: %v", err)
	}

	return path.Cid().String(), nil
}

// storeInDocumentStore stores the credential in OrbitDB DocumentStore
func (s *Service) storeInDocumentStore(ctx context.Context, vc *VerifiableCredential, metadata *CredentialMetadata) (string, error) {
	// Get or create the credentials document store
	if s.credentialsStore == nil {
		store, err := s.getOrCreateDocumentStore(ctx, "credentials-store")
		if err != nil {
			return "", err
		}
		s.credentialsStore = store
	}

	// Prepare document for storage
	doc := map[string]interface{}{
		"_id":        vc.ID,
		"credential": vc,
		"metadata":   metadata,
		"timestamp":  time.Now().Unix(),
	}

	// Put document in store
	_, err := s.credentialsStore.Put(ctx, doc)
	if err != nil {
		return "", fmt.Errorf("failed to store in OrbitDB: %v", err)
	}

	// Return the document ID as the hash reference
	// OrbitDB stores this internally and we can retrieve it using the _id
	return vc.ID, nil
}

// storeInEventLog stores an audit log entry in OrbitDB EventLog
func (s *Service) storeInEventLog(ctx context.Context, vc *VerifiableCredential, metadata *CredentialMetadata) error {
	// Get or create audit log
	if s.auditLog == nil {
		logStore, err := s.getOrCreateEventLog(ctx, "credentials-audit-log")
		if err != nil {
			return err
		}
		s.auditLog = logStore
	}

	// Create audit log entry
	logEntry := map[string]interface{}{
		"action":       "CREDENTIAL_STORED",
		"credentialId": vc.ID,
		"issuerId":     metadata.IssuerID,
		"subjectId":    metadata.SubjectID,
		"timestamp":    time.Now().Unix(),
		"ipfsHash":     metadata.IPFSHash,
		"orbitDbHash":  metadata.OrbitDBHash,
	}

	// Marshal the log entry to bytes before adding
	logEntryBytes, err := json.Marshal(logEntry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %v", err)
	}

	// Add entry to log (as bytes)
	_, err = s.auditLog.Add(ctx, logEntryBytes)
	return err
}

// storeInSQLite stores metadata in SQLite for fast querying
func (s *Service) storeInSQLite(metadata *CredentialMetadata) error {
	insertSQL := `
	INSERT INTO credentials_metadata (
		credential_id, issuer_id, subject_id, credential_type,
		issuance_date, expiration_date, status, stored_at,
		ipfs_hash, orbitdb_hash
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	credentialTypeJSON, _ := json.Marshal(metadata.CredentialType)

	_, err := s.sqliteDB.Exec(
		insertSQL,
		metadata.CredentialID,
		metadata.IssuerID,
		metadata.SubjectID,
		string(credentialTypeJSON),
		metadata.IssuanceDate,
		metadata.ExpirationDate,
		metadata.Status,
		metadata.StoredAt,
		metadata.IPFSHash,
		metadata.OrbitDBHash,
	)

	return err
}

// Helper functions for OrbitDB store management
func (s *Service) getOrCreateDocumentStore(ctx context.Context, name string) (orbitdb.DocumentStore, error) {
	// Try to open existing store, or create if it doesn't exist
	// Docs() automatically handles creation with correct store type
	store, err := s.orbit.Docs(ctx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get/create document store: %v", err)
	}

	// Load the store
	if err := store.Load(ctx, -1); err != nil {
		return nil, fmt.Errorf("failed to load document store: %v", err)
	}

	return store, nil
}

func (s *Service) getOrCreateEventLog(ctx context.Context, name string) (orbitdb.EventLogStore, error) {
	// Try to open existing log, or create if it doesn't exist
	// Log() automatically handles creation with correct store type
	logStore, err := s.orbit.Log(ctx, name, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get/create event log: %v", err)
	}

	// Load the log
	if err := logStore.Load(ctx, -1); err != nil {
		return nil, fmt.Errorf("failed to load event log: %v", err)
	}

	return logStore, nil
}
