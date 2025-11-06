package credentials

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"
)

// ListCredentials lists all credentials with pagination
func (s *Service) ListCredentials(limit, offset int) ([]CredentialMetadata, error) {
	if limit <= 0 {
		limit = 50
	}

	query := `
	SELECT credential_id, issuer_id, subject_id, credential_type, 
	       issuance_date, expiration_date, status, stored_at,
	       ipfs_hash, orbitdb_hash
	FROM credentials_metadata
	ORDER BY stored_at DESC
	LIMIT ? OFFSET ?`

	rows, err := s.sqliteDB.Query(query, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("database query failed: %v", err)
	}
	defer rows.Close()

	credentials := []CredentialMetadata{}
	for rows.Next() {
		var meta CredentialMetadata
		var credentialTypeJSON string

		err := rows.Scan(
			&meta.CredentialID,
			&meta.IssuerID,
			&meta.SubjectID,
			&credentialTypeJSON,
			&meta.IssuanceDate,
			&meta.ExpirationDate,
			&meta.Status,
			&meta.StoredAt,
			&meta.IPFSHash,
			&meta.OrbitDBHash,
		)
		if err != nil {
			log.Printf("[WARN] Failed to scan row: %v", err)
			continue
		}

		json.Unmarshal([]byte(credentialTypeJSON), &meta.CredentialType)
		credentials = append(credentials, meta)
	}

	return credentials, nil
}

// GetCredential retrieves a specific credential by ID
func (s *Service) GetCredential(credentialID string) (*VerifiableCredential, *CredentialMetadata, error) {
	// Query metadata from SQLite
	query := `
	SELECT credential_id, issuer_id, subject_id, credential_type,
	       issuance_date, expiration_date, status, stored_at,
	       ipfs_hash, orbitdb_hash
	FROM credentials_metadata
	WHERE credential_id = ?`

	var meta CredentialMetadata
	var credentialTypeJSON string

	err := s.sqliteDB.QueryRow(query, credentialID).Scan(
		&meta.CredentialID,
		&meta.IssuerID,
		&meta.SubjectID,
		&credentialTypeJSON,
		&meta.IssuanceDate,
		&meta.ExpirationDate,
		&meta.Status,
		&meta.StoredAt,
		&meta.IPFSHash,
		&meta.OrbitDBHash,
	)

	if err != nil {
		return nil, nil, fmt.Errorf("credential not found: %v", err)
	}

	json.Unmarshal([]byte(credentialTypeJSON), &meta.CredentialType)

	// Retrieve full credential from OrbitDB
	credential, err := s.retrieveCredentialFromStorage(credentialID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve credential: %v", err)
	}

	return credential, &meta, nil
}

// QueryCredentials performs advanced queries
func (s *Service) QueryCredentials(params QueryParams) ([]CredentialMetadata, error) {
	// Build dynamic SQL query
	query := `SELECT credential_id, issuer_id, subject_id, credential_type,
	                 issuance_date, expiration_date, status, stored_at,
	                 ipfs_hash, orbitdb_hash
	          FROM credentials_metadata WHERE 1=1`

	args := []interface{}{}

	if params.IssuerID != "" {
		query += " AND issuer_id = ?"
		args = append(args, params.IssuerID)
	}

	if params.SubjectID != "" {
		query += " AND subject_id = ?"
		args = append(args, params.SubjectID)
	}

	if params.CredentialType != "" {
		query += " AND credential_type LIKE ?"
		args = append(args, "%"+params.CredentialType+"%")
	}

	if params.Status != "" {
		query += " AND status = ?"
		args = append(args, params.Status)
	}

	if params.IssuedAfter != nil {
		query += " AND issuance_date >= ?"
		args = append(args, params.IssuedAfter)
	}

	if params.IssuedBefore != nil {
		query += " AND issuance_date <= ?"
		args = append(args, params.IssuedBefore)
	}

	query += " ORDER BY stored_at DESC"

	// Apply pagination
	limit := params.Limit
	if limit <= 0 {
		limit = 50
	}
	query += " LIMIT ?"
	args = append(args, limit)

	if params.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, params.Offset)
	}

	// Execute query
	rows, err := s.sqliteDB.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	credentials := []CredentialMetadata{}
	for rows.Next() {
		var meta CredentialMetadata
		var credentialTypeJSON string

		err := rows.Scan(
			&meta.CredentialID,
			&meta.IssuerID,
			&meta.SubjectID,
			&credentialTypeJSON,
			&meta.IssuanceDate,
			&meta.ExpirationDate,
			&meta.Status,
			&meta.StoredAt,
			&meta.IPFSHash,
			&meta.OrbitDBHash,
		)
		if err != nil {
			continue
		}

		json.Unmarshal([]byte(credentialTypeJSON), &meta.CredentialType)
		credentials = append(credentials, meta)
	}

	return credentials, nil
}

// GetCredentialsByIssuer retrieves all credentials by issuer
func (s *Service) GetCredentialsByIssuer(issuerID string) ([]CredentialMetadata, error) {
	query := `SELECT credential_id, issuer_id, subject_id, credential_type,
	                 issuance_date, expiration_date, status, stored_at,
	                 ipfs_hash, orbitdb_hash
	          FROM credentials_metadata
	          WHERE issuer_id = ?
	          ORDER BY issuance_date DESC`

	rows, err := s.sqliteDB.Query(query, issuerID)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	credentials := []CredentialMetadata{}
	for rows.Next() {
		var meta CredentialMetadata
		var credentialTypeJSON string

		err := rows.Scan(
			&meta.CredentialID,
			&meta.IssuerID,
			&meta.SubjectID,
			&credentialTypeJSON,
			&meta.IssuanceDate,
			&meta.ExpirationDate,
			&meta.Status,
			&meta.StoredAt,
			&meta.IPFSHash,
			&meta.OrbitDBHash,
		)
		if err != nil {
			continue
		}

		json.Unmarshal([]byte(credentialTypeJSON), &meta.CredentialType)
		credentials = append(credentials, meta)
	}

	return credentials, nil
}

// GetCredentialsBySubject retrieves all credentials for a subject
func (s *Service) GetCredentialsBySubject(subjectID string) ([]CredentialMetadata, error) {
	query := `SELECT credential_id, issuer_id, subject_id, credential_type,
	                 issuance_date, expiration_date, status, stored_at,
	                 ipfs_hash, orbitdb_hash
	          FROM credentials_metadata
	          WHERE subject_id = ?
	          ORDER BY issuance_date DESC`

	rows, err := s.sqliteDB.Query(query, subjectID)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}
	defer rows.Close()

	credentials := []CredentialMetadata{}
	for rows.Next() {
		var meta CredentialMetadata
		var credentialTypeJSON string

		err := rows.Scan(
			&meta.CredentialID,
			&meta.IssuerID,
			&meta.SubjectID,
			&credentialTypeJSON,
			&meta.IssuanceDate,
			&meta.ExpirationDate,
			&meta.Status,
			&meta.StoredAt,
			&meta.IPFSHash,
			&meta.OrbitDBHash,
		)
		if err != nil {
			continue
		}

		json.Unmarshal([]byte(credentialTypeJSON), &meta.CredentialType)
		credentials = append(credentials, meta)
	}

	return credentials, nil
}

// RevokeCredential revokes a credential
func (s *Service) RevokeCredential(credentialID, reason string) error {
	// Update status in SQLite
	updateSQL := `UPDATE credentials_metadata SET status = 'revoked' WHERE credential_id = ?`
	result, err := s.sqliteDB.Exec(updateSQL, credentialID)
	if err != nil {
		return fmt.Errorf("failed to revoke credential: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected == 0 {
		return fmt.Errorf("credential not found")
	}

	// Log revocation in EventLog
	ctx := context.Background()
	if s.auditLog == nil {
		logStore, err := s.getOrCreateEventLog(ctx, "credentials-audit-log")
		if err != nil {
			return err
		}
		s.auditLog = logStore
	}

	logEntry := map[string]interface{}{
		"action":       "CREDENTIAL_REVOKED",
		"credentialId": credentialID,
		"reason":       reason,
		"timestamp":    time.Now().Unix(),
	}

	// Fixed: Marshal to bytes before adding to log
	logEntryBytes, err := json.Marshal(logEntry)
	if err != nil {
		log.Printf("[WARN] Failed to marshal log entry: %v", err)
	} else {
		_, err = s.auditLog.Add(ctx, logEntryBytes)
		if err != nil {
			log.Printf("[WARN] Failed to log revocation: %v", err)
		}
	}

	log.Printf("[CREDENTIALS] Revoked credential: %s", credentialID)
	if s.loggerDB != nil {
		_ = s.loggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Revoked credential: %s", credentialID), runtime.GOOS)
	}

	return nil
}

// VerifyCredential verifies a credential's authenticity
func (s *Service) VerifyCredential(vc *VerifiableCredential) (bool, []string) {
	errors := []string{}

	// Verify structure
	if err := s.ValidateCredential(vc); err != nil {
		errors = append(errors, err.Error())
	}

	// Verify proof
	if vc.Proof != nil {
		if err := s.VerifyProof(vc); err != nil {
			errors = append(errors, "Proof verification failed: "+err.Error())
		}
	} else {
		errors = append(errors, "No proof provided")
	}

	// Check if credential is in database and not revoked
	query := `SELECT status FROM credentials_metadata WHERE credential_id = ?`
	var status string
	err := s.sqliteDB.QueryRow(query, vc.ID).Scan(&status)

	if err != nil {
		errors = append(errors, "Credential not found in database")
	} else if status == "revoked" {
		errors = append(errors, "Credential has been revoked")
	} else if status == "expired" {
		errors = append(errors, "Credential has expired")
	}

	// Check expiration
	if vc.ExpirationDate != "" {
		expirationDate, _ := time.Parse(time.RFC3339, vc.ExpirationDate)
		if time.Now().After(expirationDate) {
			errors = append(errors, "Credential has expired")
		}
	}

	verified := len(errors) == 0
	return verified, errors
}

// retrieveCredentialFromStorage retrieves credential from OrbitDB
func (s *Service) retrieveCredentialFromStorage(credentialID string) (*VerifiableCredential, error) {
	ctx := context.Background()

	// Get the credentials store
	if s.credentialsStore == nil {
		store, err := s.getOrCreateDocumentStore(ctx, "credentials-store")
		if err != nil {
			return nil, err
		}
		s.credentialsStore = store
	}

	docs, err := s.credentialsStore.Get(ctx, credentialID, nil)
	if err != nil || len(docs) == 0 {
		return nil, fmt.Errorf("credential not found in storage")
	}

	// Extract credential from document
	// FIXED: Type assert interface{} to map[string]interface{} first
	docInterface := docs[0]
	doc, ok := docInterface.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid document format: expected map[string]interface{}, got %T", docInterface)
	}

	// Now we can safely index the map
	credentialData, ok := doc["credential"]
	if !ok {
		return nil, fmt.Errorf("invalid document format: missing 'credential' field")
	}

	// Convert to VerifiableCredential
	credentialJSON, err := json.Marshal(credentialData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal credential data: %v", err)
	}

	var credential VerifiableCredential
	if err := json.Unmarshal(credentialJSON, &credential); err != nil {
		return nil, fmt.Errorf("failed to unmarshal credential: %v", err)
	}

	return &credential, nil
}
