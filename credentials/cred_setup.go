package credentials

import (
	"log"
	"net/http"
	"runtime"

	"optimusdb/app"
)

// SetupCredentialsEndpoints registers all credential-related HTTP endpoints
// This should be called from api.ServeHTTP() in api/http.go
func SetupCredentialsEndpoints(mux *http.ServeMux, middleware func(http.Handler) http.Handler, context string, kb *app.KnowledgeBaseDB, logger *app.LoggerSQLite) error {

	// Initialize credentials service
	service, err := NewService(kb, logger)
	if err != nil {
		log.Printf("[ERROR] Failed to initialize credentials service: %v", err)
		if logger != nil {
			_ = logger.AddToOptimusLog("ERROR", "Failed to initialize credentials service", runtime.GOOS)
		}
		return err
	}

	// Create handler
	handler := NewHandler(service)

	// Register endpoints with the provided middleware
	// Main credentials endpoint (POST to store, GET to list)
	mux.Handle("/"+context+"/credentials", middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handler.StoreCredentialHandler()(w, r)
		case http.MethodGet:
			handler.ListCredentialsHandler()(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})))

	// Get specific credential by ID
	// Example: /optimusdb/credentials/get/<credentialID>
	mux.Handle("/"+context+"/credentials/get/", middleware(handler.GetCredentialHandler()))

	// Advanced query endpoint
	mux.Handle("/"+context+"/credentials/query", middleware(handler.QueryCredentialsHandler()))

	// Get credentials by issuer
	// Example: /optimusdb/credentials/issuer/<issuerID>
	mux.Handle("/"+context+"/credentials/issuer/", middleware(handler.GetCredentialsByIssuerHandler()))

	// Get credentials by subject
	// Example: /optimusdb/credentials/subject/<subjectID>
	mux.Handle("/"+context+"/credentials/subject/", middleware(handler.GetCredentialsBySubjectHandler()))

	// Revoke credential
	mux.Handle("/"+context+"/credentials/revoke", middleware(handler.RevokeCredentialHandler()))

	// Verify credential
	mux.Handle("/"+context+"/credentials/verify", middleware(handler.VerifyCredentialHandler()))

	log.Println("[CREDENTIALS] All endpoints registered successfully")
	if logger != nil {
		_ = logger.AddToOptimusLog("INFO", "Credentials endpoints registered", runtime.GOOS)
	}

	return nil
}
