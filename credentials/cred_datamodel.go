package credentials

import (
	"time"
)

// W3C Verifiable Credential structure following the W3C VC Data Model
type VerifiableCredential struct {
	Context           []string               `json:"@context"`
	ID                string                 `json:"id"`
	Type              []string               `json:"type"`
	Issuer            interface{}            `json:"issuer"` // Can be string or object
	IssuanceDate      string                 `json:"issuanceDate"`
	ExpirationDate    string                 `json:"expirationDate,omitempty"`
	CredentialSubject map[string]interface{} `json:"credentialSubject"`
	Proof             *Proof                 `json:"proof,omitempty"`
}

// Proof structure for cryptographic verification
type Proof struct {
	Type               string `json:"type"`
	Created            string `json:"created"`
	ProofPurpose       string `json:"proofPurpose"`
	VerificationMethod string `json:"verificationMethod"`
	ProofValue         string `json:"proofValue,omitempty"`
	Jws                string `json:"jws,omitempty"`
	Challenge          string `json:"challenge,omitempty"`
	Domain             string `json:"domain,omitempty"`
}

// CredentialMetadata for additional indexing and querying
type CredentialMetadata struct {
	CredentialID   string     `json:"credentialId"`
	IssuerID       string     `json:"issuerId"`
	SubjectID      string     `json:"subjectId"`
	CredentialType []string   `json:"credentialType"`
	IssuanceDate   time.Time  `json:"issuanceDate"`
	ExpirationDate *time.Time `json:"expirationDate,omitempty"`
	Status         string     `json:"status"` // active, revoked, expired
	StoredAt       time.Time  `json:"storedAt"`
	IPFSHash       string     `json:"ipfsHash"`
	OrbitDBHash    string     `json:"orbitDbHash"`
}

// CredentialResponse returned to the client
type CredentialResponse struct {
	Success        bool           `json:"success"`
	CredentialID   string         `json:"credentialId"`
	Message        string         `json:"message"`
	IPFSHash       string         `json:"ipfsHash,omitempty"`
	OrbitDBHash    string         `json:"orbitDbHash,omitempty"`
	StorageDetails StorageDetails `json:"storageDetails"`
}

type StorageDetails struct {
	DocumentStore string `json:"documentStore"`
	EventLog      string `json:"eventLog"`
	SQLiteIndex   string `json:"sqliteIndex"`
}

// ErrorResponse for error handling
type ErrorResponse struct {
	Success bool   `json:"success"`
	Error   string `json:"error"`
	Details string `json:"details,omitempty"`
}

// QueryParams for advanced credential queries
type QueryParams struct {
	IssuerID       string     `json:"issuerId,omitempty"`
	SubjectID      string     `json:"subjectId,omitempty"`
	CredentialType string     `json:"credentialType,omitempty"`
	Status         string     `json:"status,omitempty"`
	IssuedAfter    *time.Time `json:"issuedAfter,omitempty"`
	IssuedBefore   *time.Time `json:"issuedBefore,omitempty"`
	Limit          int        `json:"limit,omitempty"`
	Offset         int        `json:"offset,omitempty"`
}

// RevokeRequest for credential revocation
type RevokeRequest struct {
	CredentialID string `json:"credentialId"`
	Reason       string `json:"reason,omitempty"`
}
