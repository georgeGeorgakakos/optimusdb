package api

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"io/ioutil"
	"net"
	"net/http"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/tosca"
	"regexp"
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

/*
// logsHandler handles GET /<context>/log?date=YYYY-MM-DD&hour=HH
*/
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

/*
*  dedicated HTTP handler for TOSCA uploads
 */
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File string `json:"file"` // Base64-encoded TOSCA YAML
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

		// 2) Parse TOSCA
		tmpl, err := tosca.ParseTOSCA(decoded)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA parse error: %v", err))
			return
		}

		// 3) Compute IDs / counts
		templateID := tosca.ComputeTemplateID(decoded)
		nodeCount := tosca.CountNodeTemplates(tmpl)
		description := tmpl.Description

		// 4) Persist ORIGINAL YAML to OrbitDB (e.g., DsTOSCA_Imported)
		if optimusdb.DsTOSCA_Imported == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "TOSCA store not initialized")
			return
		}
		ctx := r.Context()
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

		// 5) Index lightweight metadata into SQLite
		if app.GlobalKBSQLite == nil {
			sendErrorResponse(w, http.StatusInternalServerError, "SQLite not initialized")
			return
		}
		if err := app.GlobalKBSQLite.InsertTOSCAMetadata(templateID, description, nodeCount); err != nil {
			sendErrorResponse(w, http.StatusInternalServerError, fmt.Sprintf("Failed to index metadata: %v", err))
			return
		}

		// 6) Respond
		sendSuccessResponse(w, map[string]interface{}{
			"message":     "TOSCA uploaded successfully",
			"template_id": templateID,
			"node_count":  nodeCount,
		})
	}
}

/*
func uploadTOSCAHandler(optimusdb *app.KnowledgeBaseDB) http.HandlerFunc {
	type UploadRequest struct {
		File string `json:"file"` // Base64-encoded TOSCA YAML
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			sendErrorResponse(w, http.StatusMethodNotAllowed, "Only POST is allowed")
			return
		}

		var req UploadRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		if err != nil || req.File == "" {
			sendErrorResponse(w, http.StatusBadRequest, "Invalid JSON payload")
			return
		}

		// Decode base64
		//decoded, err := base64.StdEncoding.DecodeString(req.File)
		_, err = base64.StdEncoding.DecodeString(req.File)
		if err != nil {
			sendErrorResponse(w, http.StatusBadRequest, "Base64 decoding failed")
			return
		}

		// Process TOSCA (parser + OrbitDB + SQLite)
		//err = HandleTOSCAUpload(decoded, optimusdb)
		//err = tosca.HandleTOSCAUpload(decoded, optimusdb)
		//if err != nil {
		//	sendErrorResponse(w, http.StatusBadRequest, fmt.Sprintf("TOSCA upload failed: %v", err))
		//	return
		//}

		sendSuccessResponse(w, map[string]string{"message": "TOSCA uploaded successfully"})
	}
}
*/

// gathers all benchmark data from known peers
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
				// TODO : log the error using logChan
				fmt.Print(err)
				continue
			}

			bm, err := getBenchmark(client, ip)
			if err != nil {
				// TODO : log the error ?
				fmt.Print(err)
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

/*
*
 */
func getBenchmark(client *http.Client, peerIP string) (app.Benchmark, error) {
	var bm app.Benchmark

	bmReq := app.Request{Method: app.BENCHMARK, Args: []string{}}
	jsonData, err := json.Marshal(bmReq)
	if err != nil {
		return bm, err
	}

	// send get benchmark request
	//cmdPath := "http://" + peerIP + ":8089/optimusdb/command"
	cmdPath := "http://" + peerIP + ":" + *config.FlagHTTPPort + "/" + *config.FlagContext + "/command"
	req, err := http.NewRequest("POST", cmdPath, bytes.NewBuffer(jsonData))
	if err != nil {
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
		return bm, err
	}

	// unmarshal response
	err = json.Unmarshal(body, &bm)
	if err != nil {
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

/*
*
This is the context handler for the REQ/RSP of the Http payload
*/
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

	fmt.Println("[DEBUG] Inside command Handler formation")

	return func(w http.ResponseWriter, r *http.Request) {

		fmt.Println("[DEBUG] Inside command Handler ~ Response Writer")
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

/*
*	sendErrorResponse
 */
func sendErrorResponse(w http.ResponseWriter, statusCode int, message string) {
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		//"status":  "error",
		"status":  http.Error,
		"message": message,
	})
}

/*
*	sendSuccessResponse
 */
func sendSuccessResponse(w http.ResponseWriter, data interface{}) {
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		//"status": "success",
		"status": http.StatusOK,
		"data":   data,
	})
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// ServeHTTP
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func ServeHTTP(optimusdb *app.KnowledgeBaseDB, theLog *app.LoggerSQLite, reqChan chan app.Request,
	resChan chan interface{}, logChan chan app.Log) {

	server := http.NewServeMux()

	// middleware to handle CORS headers and preflight requests
	mw := func(next http.Handler) http.Handler {
		/////
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip := r.RemoteAddr
			logChan <- app.Log{app.Info, "Received HTTP request from " + ip}
			w.Header().Set("Access-Control-Allow-Origin", "*")
			//w.Header().Set("Access-Control-Allow-Methods", "POST")
			w.Header().Set("Access-Control-Allow-Methods", "*")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

			if r.Method == "OPTIONS" {
				w.WriteHeader(http.StatusOK)
				return
			}
			next.ServeHTTP(w, r)
		})
	}

	// register command handler which allows to run commands similar to the shell
	//fmt.Println("config.FlagContext is:", *config.FlagContext)

	/**
	simple HTTP endpoint all command related to data stores
	*/
	server.Handle("/"+*config.FlagContext+"/command", mw(commandHandler(reqChan, resChan)))

	/**
		/upload-tosca endpoint
		curl -X POST http://localhost:8089/optimusdb/upload-tosca \
	  -H "Content-Type: application/json" \
	  -d '{"file": "'$(base64 -w 0 mytosca.yaml)'"}'

	*/
	server.Handle("/"+*config.FlagContext+"/upload", mw(uploadTOSCAHandler(optimusdb)))

	/**
	simple HTTP endpoint to fetch the list of connected peers
	*/
	server.Handle("/"+*config.FlagContext+"/peers", mw(peersHandler()))

	/**
	simple HTTP endpoint to fetch the communication with EMS
	*/
	server.Handle("/"+*config.FlagContext+"/ems", mw(peersHandler()))

	/**
	simple HTTP endpoint to fetch the communication with Logging of OptimusDB
	*/
	server.Handle("/"+*config.FlagContext+"/log", mw(LogsHandler(theLog)))

	//fmt.Println("config.FlagBenchmark is:", *config.FlagBenchmark)

	// register benchmarks handler which is specific for this API because it's
	// used to gather all peers data
	if *config.FlagBenchmark {
		server.Handle("/"+*config.FlagContext+"/benchmarks", mw(benchmarksHandler(optimusdb)))
	}

	// start the HTTP server
	// Get the local IP address of the server
	ip, err := getLocalIPAddress()
	if err != nil {
		logChan <- app.Log{app.Info, "\nFailed to determine local IP address: " + err.Error()}
		ip = "unknown"
	}
	//logChan <- app.Log{app.Info, "Starting HTTP Server in port: " + " " + *config.FlagHTTPPort}
	//logChan <- app.Log{app.Info, fmt.Sprintf("Starting HTTP Server on IP %s and port %s", ip, *config.FlagHTTPPort)}

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
