package credentials

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
)

// Handler wraps the credentials service for HTTP handlers
type Handler struct {
	service *Service
}

// NewHandler creates a new HTTP handler for credentials
func NewHandler(service *Service) *Handler {
	return &Handler{service: service}
}

// StoreCredentialHandler handles POST requests to store credentials
func (h *Handler) StoreCredentialHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Only accept POST requests
		if r.Method != http.MethodPost {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is allowed", "")
			return
		}

		// Read request body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			h.sendErrorResponse(w, http.StatusBadRequest, "Failed to read request body", err.Error())
			return
		}
		defer r.Body.Close()

		// Parse the Verifiable Credential
		var credential VerifiableCredential
		if err := json.Unmarshal(body, &credential); err != nil {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON format", err.Error())
			return
		}

		// Store the credential
		response, err := h.service.StoreCredential(&credential)
		if err != nil {
			h.sendErrorResponse(w, http.StatusInternalServerError, "Failed to store credential", err.Error())
			return
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(response)

		log.Printf("[CREDENTIALS] HTTP: Stored credential %s", credential.ID)
		if h.service.loggerDB != nil {
			_ = h.service.loggerDB.AddToOptimusLog("INFO", fmt.Sprintf("HTTP: Stored credential %s", credential.ID), runtime.GOOS)
		}
	}
}

// ListCredentialsHandler lists all credentials with pagination
func (h *Handler) ListCredentialsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is allowed", "")
			return
		}

		// Parse pagination parameters
		limit := 50 // default
		offset := 0

		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
				limit = l
			}
		}

		if offsetStr := r.URL.Query().Get("offset"); offsetStr != "" {
			if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
				offset = o
			}
		}

		// Get credentials
		credentials, err := h.service.ListCredentials(limit, offset)
		if err != nil {
			h.sendErrorResponse(w, http.StatusInternalServerError, "Failed to list credentials", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"count":       len(credentials),
			"credentials": credentials,
		})
	}
}

// GetCredentialHandler retrieves a specific credential by ID
func (h *Handler) GetCredentialHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is allowed", "")
			return
		}

		// Extract credential ID from URL path
		// Expected format: /<context>/credentials/<credentialID>
		path := r.URL.Path
		parts := strings.Split(path, "/")
		if len(parts) < 4 {
			h.sendErrorResponse(w, http.StatusBadRequest, "Credential ID is required", "")
			return
		}

		credentialID, err := url.QueryUnescape(parts[len(parts)-1])
		if err != nil || credentialID == "" {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid credential ID", "")
			return
		}

		// Get credential
		credential, metadata, err := h.service.GetCredential(credentialID)
		if err != nil {
			h.sendErrorResponse(w, http.StatusNotFound, "Credential not found", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":    true,
			"credential": credential,
			"metadata":   metadata,
		})
	}
}

// QueryCredentialsHandler provides advanced query capabilities
func (h *Handler) QueryCredentialsHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is allowed", "")
			return
		}

		// Parse query parameters
		var params QueryParams
		if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid query parameters", err.Error())
			return
		}

		// Execute query
		results, err := h.service.QueryCredentials(params)
		if err != nil {
			h.sendErrorResponse(w, http.StatusInternalServerError, "Query failed", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success": true,
			"count":   len(results),
			"query":   params,
			"results": results,
		})
	}
}

// GetCredentialsByIssuerHandler retrieves all credentials by issuer
func (h *Handler) GetCredentialsByIssuerHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is allowed", "")
			return
		}

		// Extract issuer ID from URL
		path := r.URL.Path
		parts := strings.Split(path, "/")
		if len(parts) < 5 {
			h.sendErrorResponse(w, http.StatusBadRequest, "Issuer ID is required", "")
			return
		}

		issuerID, err := url.QueryUnescape(parts[len(parts)-1])
		if err != nil || issuerID == "" {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid issuer ID", "")
			return
		}

		// Get credentials
		credentials, err := h.service.GetCredentialsByIssuer(issuerID)
		if err != nil {
			h.sendErrorResponse(w, http.StatusInternalServerError, "Query failed", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"issuerId":    issuerID,
			"count":       len(credentials),
			"credentials": credentials,
		})
	}
}

// GetCredentialsBySubjectHandler retrieves all credentials for a subject
func (h *Handler) GetCredentialsBySubjectHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only GET method is allowed", "")
			return
		}

		// Extract subject ID from URL
		path := r.URL.Path
		parts := strings.Split(path, "/")
		if len(parts) < 5 {
			h.sendErrorResponse(w, http.StatusBadRequest, "Subject ID is required", "")
			return
		}

		subjectID, err := url.QueryUnescape(parts[len(parts)-1])
		if err != nil || subjectID == "" {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid subject ID", "")
			return
		}

		// Get credentials
		credentials, err := h.service.GetCredentialsBySubject(subjectID)
		if err != nil {
			h.sendErrorResponse(w, http.StatusInternalServerError, "Query failed", err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":     true,
			"subjectId":   subjectID,
			"count":       len(credentials),
			"credentials": credentials,
		})
	}
}

// RevokeCredentialHandler revokes a credential
func (h *Handler) RevokeCredentialHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is allowed", "")
			return
		}

		var request RevokeRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid request body", err.Error())
			return
		}

		// Revoke credential
		if err := h.service.RevokeCredential(request.CredentialID, request.Reason); err != nil {
			if err.Error() == "credential not found" {
				h.sendErrorResponse(w, http.StatusNotFound, "Credential not found", "")
			} else {
				h.sendErrorResponse(w, http.StatusInternalServerError, "Failed to revoke credential", err.Error())
			}
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":      true,
			"message":      "Credential revoked successfully",
			"credentialId": request.CredentialID,
		})

		log.Printf("[CREDENTIALS] HTTP: Revoked credential %s", request.CredentialID)
		if h.service.loggerDB != nil {
			_ = h.service.loggerDB.AddToOptimusLog("INFO", fmt.Sprintf("HTTP: Revoked credential %s", request.CredentialID), runtime.GOOS)
		}
	}
}

// VerifyCredentialHandler verifies a credential's authenticity
func (h *Handler) VerifyCredentialHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			h.sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST method is allowed", "")
			return
		}

		var credential VerifiableCredential
		if err := json.NewDecoder(r.Body).Decode(&credential); err != nil {
			h.sendErrorResponse(w, http.StatusBadRequest, "Invalid credential format", err.Error())
			return
		}

		// Verify credential
		verified, errors := h.service.VerifyCredential(&credential)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"success":  true,
			"verified": verified,
			"errors":   errors,
			"credential": map[string]interface{}{
				"id":     credential.ID,
				"type":   credential.Type,
				"issuer": credential.Issuer,
			},
		})
	}
}

// sendErrorResponse sends a JSON error response
func (h *Handler) sendErrorResponse(w http.ResponseWriter, statusCode int, message, details string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Success: false,
		Error:   message,
		Details: details,
	})
}
