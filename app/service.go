package app

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"log"
	"optimusdb/config"
	"optimusdb/datamodel"
	"optimusdb/ipfs"
	"optimusdb/queryengine"
	"os"
	"os/user"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores"
	files "github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/interface-go-ipfs-core/options"
	"github.com/ipfs/interface-go-ipfs-core/path"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"golang.org/x/net/context"
)

var agentName string
var handlerOnce sync.Once

// Method defines simple schemas for actions to perform on the db
type Method struct {
	Cmd    string `json:"cmd"`
	ArgCnt int    `json:"argcnt"`
}

type Validation struct {
	Path    string `json:"path"` // ipfs path for a file, looks like this : /ipfs/<file cid>
	IsValid bool   `json:"isValid"`
	VoteCnt uint32 `json:"voteCnt"` // how many peers have contributed a vote, 0 if it was self determined
}

var (
	GET         Method = Method{"get", 1}     // needs the ipfs filepath
	POST        Method = Method{"post", 1}    // needs a string of bytes representing the file
	CONNECT     Method = Method{"connect", 1} // needs the peer address
	QUERY       Method = Method{"query", 0}
	QUERYKBDATA Method = Method{"querykbdata", 2}
	SQLSELECT   Method = Method{"sqlselect", 1} // needs the sql statement
	SQLDML      Method = Method{"sqldml", 1}    // needs the sql statement
	CRUDGET     Method = Method{"crudget", 1}
	CRUDPUT     Method = Method{"crudput", 1}
	CONTRI      Method = Method{"contri", 1}
	CRUDUPDATE  Method = Method{"crudupdate", 1} // reflects the update
	CRUDDELETE  Method = Method{"cruddelete", 1} // reflects the delete
	BENCHMARK   Method = Method{"benchmark", 0}
	HELP        Method = Method{"help", 0}
)

// Request Requests are an abstraction for the communication between this applications
// various apis (shell, http, grpc etc.) and the actual db service
// (n to 1 relation at the moment)
// Add near your existing Request struct
type QueryStrategy string
type ConsistencyLevel string

const (
	StrategyLocalOnly            QueryStrategy = "LOCAL_ONLY"
	StrategyRemoteOnly           QueryStrategy = "REMOTE_ONLY"
	StrategyLocalThenRemoteMerge QueryStrategy = "LOCAL_THEN_REMOTE_MERGE"
	StrategyParallelMerge        QueryStrategy = "PARALLEL_MERGE"
	StrategyQuorum               QueryStrategy = "QUORUM"
)

const (
	ConsistencyBestEffort ConsistencyLevel = "BEST_EFFORT" // return as much as we have by budget
	ConsistencyQuorum     ConsistencyLevel = "QUORUM"      // honor quorum_n
	ConsistencyAll        ConsistencyLevel = "ALL"         // wait all (bounded by budget)
)

type QueryOptions struct {
	Strategy       QueryStrategy    `json:"strategy"`        // default: LOCAL_THEN_REMOTE_MERGE
	Consistency    ConsistencyLevel `json:"consistency"`     // default: BEST_EFFORT
	TimeBudgetMs   int              `json:"time_budget_ms"`  // default: 1200
	QuorumN        int              `json:"quorum_n"`        // number of peers required (for QUORUM)
	MinRows        int              `json:"min_rows"`        // threshold to stop early if enough rows
	StaleOkTTLms   int              `json:"stale_ok_ttl_ms"` // use cached remote results within TTL
	MaxPeers       int              `json:"max_peers"`       // top-K peers by reputation to query
	IncludeLocal   bool             `json:"include_local"`   // default: true
	AnnotateSource bool             `json:"annotate_source"` // default: true
}

type Request struct {
	Method          Method                   `json:"method"`
	Args            []string                 `json:"args"`
	DSType          string                   `json:"dstype"` /// has http.go
	SQLDML          string                   `json:"sqldml"` /// has http.go
	UpdateData      []map[string]interface{} `json:"UpdateData"`
	Criteria        []map[string]interface{} `json:"criteria"`        // Updated type
	Graph_traversal []map[string]interface{} `json:"graph_Traversal"` // Updated type

	// NEW: //25102025
	Options *QueryOptions `json:"options,omitempty"`
}

// SQLQuery represents a parsed SQL query
type SQLQuery struct {
	SelectFields []string
	Table        string
	Conditions   []FilterCriterion
}

type opDoc struct {
	Key   string `json:"key,omitempty"`
	Value []byte `json:"value,omitempty"`
}

type Contribution struct {
	AgentName   string    `json:"agentname"`
	Path        string    `json:"path"`        // ipfs file path which includes the cid
	Contributor string    `json:"contributor"` // ipfs node id
	CreationTS  time.Time `json:"creationTS"`  // timestamp of creation
	LocalIP     string    `json:"localip"`
	NodeIP      string    `json:"nodeip"`
	RemoteIPs   []string  `json:"remoteIPs"`
}

// FilterCriterion represents a single filter condition
type FilterCriterion struct {
	Field    string      // The field to filter on
	Operator string      // The operator (e.g., "=", ">", "<", ">=", "<=", "!=")
	Value    interface{} // The value to compare against
}

// InitAgentName initializes the agentName variable
func InitAgentName() {
	agentName = os.Getenv("AGENT_NAME")
	if agentName == "" {
		agentName = os.Args[0] // Fallback to the executable name
	}
}

// GetAgentName returns the value of agentName
func GetAgentName() string {
	return agentName
}

// Service starts all reoccuring tasks on optimusdb level
func Service(knowledgeBaseDB *KnowledgeBaseDB,
	reqChan chan Request,
	resChan chan interface{},
	hostCID host.Host,
	logChan chan Log,
	rdbms *KnowledgeBaseSQLite) {

	// wait for and handle connectedness changed event
	go awaitConnected(knowledgeBaseDB, logChan)

	// wait for pubsub messages to self which will be received when another peer
	// connects (see "awaitConnected" above)
	go awaitStoreExchange(knowledgeBaseDB, logChan)

	// wait for write events to handle validation
	go awaitWriteEvent(knowledgeBaseDB, logChan)

	// wait for and handle "validation" requests
	go awaitValidationReq(knowledgeBaseDB, logChan)

	// wait for and handle replication event
	go awaitReplicateEvent(knowledgeBaseDB, logChan)

	handlerOnce.Do(func() {
		RegisterQueryStreamHandler(knowledgeBaseDB.Node.PeerHost, knowledgeBaseDB)
	})

	//--------------------------------------------------------------------------
	// handle API requests
	for {
		req := <-reqChan
		logChan <- Log{Info, "Received service request"}

		var res interface{}
		switch strings.ToLower(req.Method.Cmd) {
		case strings.ToLower(GET.Cmd):
			ipfsPath := req.Args[0]
			logChan <- Log{Info, "Received service request: GET"}
			res = get(knowledgeBaseDB, ipfsPath, logChan)

		case strings.ToLower(POST.Cmd):
			file := req.Args[0]
			node := files.NewBytesFile([]byte(file))
			logChan <- Log{Info, "Received service request: POST"}
			fmt.Printf("\nReceived service request: %s : ", POST.Cmd)
			res = post(knowledgeBaseDB, node, logChan)

		case strings.ToLower(CONNECT.Cmd):
			// type checking
			peerId := req.Args[0]
			logChan <- Log{Info, "Connecting to " + peerId}
			res = connect(knowledgeBaseDB, peerId, logChan)

		////////////////////////////////
		case strings.ToLower(QUERY.Cmd):
			logChan <- Log{Info, "Received service request: QUERY"}

			// Defaults
			opt := QueryOptions{
				Strategy:       StrategyLocalThenRemoteMerge,
				Consistency:    ConsistencyBestEffort,
				TimeBudgetMs:   1200,
				MinRows:        0,
				QuorumN:        0,
				IncludeLocal:   true,
				AnnotateSource: true,
			}
			if req.Options != nil {
				// shallow override
				if req.Options.Strategy != "" {
					opt.Strategy = req.Options.Strategy
				}
				if req.Options.Consistency != "" {
					opt.Consistency = req.Options.Consistency
				}
				if req.Options.TimeBudgetMs > 0 {
					opt.TimeBudgetMs = req.Options.TimeBudgetMs
				}
				if req.Options.MinRows > 0 {
					opt.MinRows = req.Options.MinRows
				}
				if req.Options.QuorumN > 0 {
					opt.QuorumN = req.Options.QuorumN
				}
				if req.Options.MaxPeers > 0 {
					opt.MaxPeers = req.Options.MaxPeers
				}
				opt.IncludeLocal = req.Options.IncludeLocal || req.Options.IncludeLocal == true
				opt.AnnotateSource = !(req.Options.AnnotateSource == false)
			}

			var out []map[string]interface{}
			var err error

			switch opt.Strategy {
			case StrategyLocalOnly:
				out, err = queryLocalDB(knowledgeBaseDB, req.Criteria)
				if err == nil && opt.AnnotateSource {
					annotate(out, "local", "", StrategyLocalOnly)
				}

			case StrategyRemoteOnly:
				out, err = queryPeersOptimized(knowledgeBaseDB, req.Criteria)
				if err == nil && opt.AnnotateSource {
					annotate(out, "peer", "", StrategyRemoteOnly)
				}

			case StrategyParallelMerge:
				out, err = parallelMerge(knowledgeBaseDB, req.Criteria, opt)

			case StrategyQuorum:
				out, err = quorumMerge(knowledgeBaseDB, req.Criteria, opt)

			case StrategyLocalThenRemoteMerge:
				fallthrough
			default:
				out, err = localThenRemoteMerge(knowledgeBaseDB, req.Criteria, opt)
			}

			if err != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("QUERY: error: %v", err)}
				res = map[string]interface{}{"error": err.Error()}
				break
			}

			// Optional: persist remote rows you just learned about
			if len(out) > 0 {
				storeResults(knowledgeBaseDB, logChan, req.DSType, out)
			}

			res = out

		////////////////////////////////
		case strings.ToLower("CACHESTATS"):
			if knowledgeBaseDB.QueryEngine != nil {
				stats := knowledgeBaseDB.QueryEngine.CacheStats()
				res = stats
			} else {
				res = map[string]interface{}{
					"error": "Query engine not initialized",
				}
			}
		case strings.ToLower("CLEARCACHE"):
			if knowledgeBaseDB.QueryEngine != nil {
				knowledgeBaseDB.QueryEngine.ClearCache()
				res = "Cache cleared successfully"
			} else {
				res = "Query engine not initialized"
			}
		////////////////////////////////
		case strings.ToLower(SQLDML.Cmd):
			logChan <- Log{Type: Info, Data: "Received service request: SQL.Cmd"}
			fmt.Printf("\n[INFO] SQL DML received: %v : %v\n", SQLDML.Cmd, req.SQLDML)

			// Execute SQL DML command
			rspResults, err := SQLDMLWithPeerFallback(req, logChan, knowledgeBaseDB)
			if err != nil {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("[ERROR] SQL DML Execution Failed: %v", err)}
				res = fmt.Sprintf("ERROR! #120 Failed to execute SQL statement: %v", err)
			} else {
				// Handle SELECT queries differently
				/*
					if records, ok := result.([]map[string]interface{}); ok {
						deduplicated := DedupSQLResults(records)
						logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Deduplicated → %v records after deduplication", deduplicated)}
						logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Retrieved %d → %d records after deduplication", len(records), len(deduplicated))}

						logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Successfully executed with Deduplication %v", req.SQLDML)}
						res = map[string]interface{}{
							"message": "OK: Successfully retrieved records",
							"records": deduplicated,
						}

					} else {
						logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Successfully executed %v", req.SQLDML)}
						res = "OK: SQL statement executed successfully"
					}

				*/
				logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Successfully executed %v", req.SQLDML)}
				res = rspResults //"OK: Successfully got records"
			}
		/**
		Use for contribution records - Data Store is read only
		*/
		case strings.ToLower(CONTRI.Cmd):
			logChan <- Log{Type: Info, Data: "Received service request: CONTRI.Cmd"}
			var test2 error
			//rspResults, test2 := crudGetDocStoreRev(knowledgeBaseDB, logChan, req.DSType, hostCID, req.Criteria)
			rspResults, test2 := getContri(knowledgeBaseDB, logChan, req.DSType, req.Criteria)
			if test2 != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CONTRI: ERROR! %v", test2)}
				res = "ERROR! #121 Failed to get Contribution Records"
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CONTRI: Successfully finished %d ", len(rspResults))}
				res = rspResults //"OK: Successfully got records"
			}

		/**
		Use for Get method
		*/
		case strings.ToLower(CRUDGET.Cmd):
			logChan <- Log{Info, "Received service request: CRUDGET"}
			fmt.Printf("\nReceived service request: %s : \n", CRUDGET.Cmd)
			var test2 error
			rspResults, test2 := crudGetDocStoreRev(knowledgeBaseDB, logChan, req.DSType, hostCID, req.Criteria)
			if test2 != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CRUDGET: ERROR! %v", test2)}
				res = "ERROR! #114 Failed to get records"
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CRUDGET: Successfully finished %d ", len(rspResults))}
				res = rspResults //"OK: Successfully got records"
			}

		case strings.ToLower(CRUDPUT.Cmd):
			logChan <- Log{Info, "Received service request: CRUDPUT"}
			//res, _ = crudPutDocStore(knowledgeBaseDB, logChan, req.DSType, req.Criteria)
			var errorCase error
			resultPut, errorCase := crudPutDocStoreRev(knowledgeBaseDB, logChan, req.DSType, req.Criteria)
			if errorCase != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CRUDPUT: ERROR! %v", errorCase)}
				res = "ERROR! #112 Failed to insert records"
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("CRUDPUT: Successfully finished %d ", len(resultPut))}
				res = "OK: Successfully inserted records"
			}
			// Generate metadata for each inserted resource
			//for _, entry := range req.Criteria {
			//	metadataEntry := datamodel.GenerateMetadataFromResource(entry)
			//	datamodel.AddMetadata(metadataEntry) // Store metadata
			//	logChan <- Log{Type: Info, Data: fmt.Sprintf("Metadata generated for resource: %s", metadataEntry.ID)}
			//}

		case strings.ToLower(CRUDUPDATE.Cmd):
			logChan <- Log{Info, "Received service request: CRUDUPDATE"}
			updatedCount, err := crudUpdateDocStoreRev(knowledgeBaseDB, req.Criteria, req.UpdateData)
			if err != nil {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("UPDATE: Error updating document: %v", err)}
				res = fmt.Sprintf("ERROR! Update failed: %v", err)
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("UPDATE: %d document(s) updated", updatedCount)}
				res = fmt.Sprintf("SUCCESS! %d document(s) updated", updatedCount)
			}

		case strings.ToLower(CRUDDELETE.Cmd):
			logChan <- Log{Info, "Received service request: CRUDDELETE"}
			// Perform delete operation in OrbitDB
			deletedCount, err := crudDeleteDocStoreRev(knowledgeBaseDB, req.Criteria)
			if err != nil {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("DELETE: Error deleting document: %v", err)}
				res = fmt.Sprintf("ERROR! Delete failed: %v", err)
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("DELETE: %d document(s) deleted", deletedCount)}
				res = fmt.Sprintf("SUCCESS! %d document(s) deleted", deletedCount)
			}

		case strings.ToLower(HELP.Cmd):
			logChan <- Log{Info, "Received service request: HELP"}
			//fmt.Printf("\nReceived service request: %s : \n", HELP.Cmd)
			//res = query(knowledgeBaseDB, logChan)

		case strings.ToLower(BENCHMARK.Cmd):
			if !*config.FlagBenchmark {
				res = "Benchmark is not enabled, use -benchmark to do so"
				break
			}

			res = *knowledgeBaseDB.Benchmark

		default:
			// Default case for unhandled commands
			logChan <- Log{RecoverableErr, "Received unknown service request: " + req.Method.Cmd}
			fmt.Printf("\nReceived unknown service request: %s\n", req.Method.Cmd)
			res = "Unknown command" + req.Method.Cmd + ". Use HELP for the list of available commands."

		}

		// send response
		resChan <- res
	}
}

//#####################################################################################################################
//#####################################################################################################################
//#####################################################################################################################
//#####################################################################################################################
//#####################################################################################################################

// waits for connectedness changed events and on success sends the stores id
func awaitConnected(optimusdb *KnowledgeBaseDB, logChan chan Log) {
	// subscribe to ipfs level connectedness changed event
	subipfs, err := (*optimusdb.Node).PeerHost.EventBus().Subscribe(
		new(event.EvtPeerConnectednessChanged))
	if err != nil {
		logChan <- Log{Type: NonRecoverableErr, Data: err}
		return
	}

	db := optimusdb.Contributions
	coreAPI := (*optimusdb.Orbit).IPFS()

	for e := range subipfs.Out() {
		e, ok := e.(event.EvtPeerConnectednessChanged)
		fmt.Print("\nConnectedness : ", e)

		// on established connection
		go func() {
			if ok && e.Connectedness == network.Connected && db != nil {
				time.Sleep(time.Second * 5)

				// send this stores id to peer by publishing it to the topic
				// identified by their id
				cidDbId := (*db).Address().String()
				logChan <- Log{Info, "" +
					"\nSend contributions db " + cidDbId + " to peer for replication"}
				peerId := e.Peer.String()
				ctx := context.Background()
				err := coreAPI.PubSub().Publish(ctx, peerId, []byte(cidDbId))
				if err != nil {
					logChan <- Log{Type: RecoverableErr, Data: err}
				}
			}
		}()
	}
}

// on connectedness changed events, peers exchange their event logs
func awaitStoreExchange(optimusdb *KnowledgeBaseDB, logChan chan Log) {
	// subscribe to own topic
	nodeId := optimusdb.Config.PeerID
	coreAPI := (*optimusdb.Orbit).IPFS()
	ctx := context.Background()
	sub, err := coreAPI.PubSub().Subscribe(ctx, nodeId)
	if err != nil {
		logChan <- Log{Type: NonRecoverableErr, Data: err}
		return
	}

	for {
		// TODO : refactor to return if contributions is present

		// received data should contain the id of the peers db
		msg, err := sub.Next(context.Background())
		if err != nil {
			logChan <- Log{Type: NonRecoverableErr, Data: err}
			return
		}

		// in case we started without any db, replicate this one
		if optimusdb.Contributions == nil {
			addr := string(msg.Data())
			logChan <- Log{Info, "Replicate db " + addr}
			create := false
			storeType := "eventlog"

			// give anyone write access
			ac := &accesscontroller.CreateAccessControllerOptions{
				Access: map[string][]string{
					"write": {
						"*",
					},
				},
			}

			dbopts := orbitdb.CreateDBOptions{
				AccessController: ac,
				Create:           &create,
				StoreType:        &storeType,
			}

			store, err := (*optimusdb.Orbit).Open(ctx, addr, &dbopts)
			if err != nil {
				logChan <- Log{Type: RecoverableErr, Data: err}
			}

			db := store.(iface.EventLogStore)
			db.Load(ctx, -1)
			optimusdb.Contributions = &db

			// persist store address
			optimusdb.Config.ContributionsStoreAddr = addr
		}
	}
}

func extractCIDFromIPFSPath(ipfsPath string) (string, error) {
	// Remove the "/ipfs/" prefix from the IPFS path
	cidStartIndex := strings.Index(ipfsPath, "/ipfs/") + 6
	if cidStartIndex < 6 || cidStartIndex >= len(ipfsPath) {
		return "", fmt.Errorf("extractCIDFromIPFSPath:invalid IPFS path")
	}

	cid := ipfsPath[cidStartIndex:]
	return cid, nil
}

// Listens for write events to the contributions database and validates the added data.
// What this does:
// Subscribes to write events on contributions
// Extracts new entries when they are written
// Logs the contribution's IPFS path
func awaitWriteEvent(optimusdb *KnowledgeBaseDB, logChan chan Log) {
	// since contributions datastore may be nil, wait till it isn't
	for optimusdb.Contributions == nil {
		time.Sleep(time.Second)
	}

	// subscribe to write event
	contributions := *optimusdb.Contributions
	subdb, err := contributions.EventBus().Subscribe([]interface{}{
		new(stores.EventWrite),
	})
	if err != nil {
		logChan <- Log{RecoverableErr, err}
		return
	}
	defer subdb.Close()

	coreAPI := (*optimusdb.Orbit).IPFS()
	ctx := context.Background()

	validations := *optimusdb.Validations

	subChan := subdb.Out()
	for {
		// get the new entry
		e := <-subChan
		we := e.(stores.EventWrite)

		// check if the write was executed on the contributions db
		if we.Address.GetPath() != contributions.Address().GetPath() {
			continue
		}

		entry := we.Entry

		// get the ipfs-log operation from the entry
		opStr := entry.GetPayload()
		var op opDoc
		err := json.Unmarshal(opStr, &op)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		// get the ipfs file path the from the contribution block
		var contribution Contribution
		err = json.Unmarshal(op.Value, &contribution)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}
		pth := contribution.Path

		// get the file from ipfs
		parsedPth := path.New(pth)
		file, err := coreAPI.Unixfs().Get(ctx, parsedPth)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		// try to validate the file
		valid, err := validateStub(file)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		// store validation info
		valdoc := map[string]interface{}{
			"path":    pth,
			"isValid": valid,
			"voteCnt": 0,
		}

		logChan <- Log{Info, fmt.Sprintf("validated %s with result %t",
			valdoc["path"], valdoc["isValid"])}

		optimusdb.ValidationsMtx.Lock()
		_, err = validations.Put(ctx, valdoc)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}
		optimusdb.ValidationsMtx.Unlock()
	}
}

func validateStub(file files.Node) (bool, error) {
	return true, nil
}

/*
*
 */
func crudQueryDocStoreRev(
	optimusdb *KnowledgeBaseDB,
	logChan chan Log,
	dbtype string,
	h host.Host,
	criteria []map[string]interface{},
) ([]map[string]interface{}, error) {
	ctx := context.Background()
	dbDocStore := *optimusdb.DsSWres

	var results []map[string]interface{}
	var errorList []error
	counter := 0

	// Iterate over the criteria and apply filtering
	for _, criterion := range criteria {
		docs, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
			record, ok := doc.(map[string]interface{})
			if !ok {
				return false, nil
			}

			// Apply all filters in the criterion
			for key, expectedValue := range criterion {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("Key/ExpectedValue: %v %v", key, expectedValue)}
				actualValue, exists := record[key]

				// Skip if the key is not present
				if !exists {
					return false, nil
				}

				// Handle slice comparisons properly
				if reflect.TypeOf(actualValue).Kind() == reflect.Slice {
					// Convert to []interface{} explicitly
					actualSlice, ok1 := actualValue.([]interface{})
					expectedSlice, ok2 := expectedValue.([]interface{})

					// If both are slices, compare using DeepEqual
					if ok1 && ok2 {
						if !reflect.DeepEqual(actualSlice, expectedSlice) {
							return false, nil
						}
					} else {
						// If types are mismatched, ignore
						return false, nil
					}
				} else {
					// Direct comparison for non-slice values
					if actualValue != expectedValue {
						return false, nil
					}
				}
			}
			return true, nil
		})

		// Handle query errors
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Error querying documents: %v", err)}
			errorList = append(errorList, err)
			continue
		}

		// Convert results to maps and append
		for _, doc := range docs {
			docMap, ok := doc.(map[string]interface{})
			if !ok {
				logChan <- Log{Type: RecoverableErr, Data: "Skipping invalid document format"}
				continue
			}

			results = append(results, docMap)
			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Document added to results: %v", docMap)}
			counter++
		}

		logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d documents matching criteria: %v", len(docs), criterion)}
	}

	// Final status logging
	if len(errorList) > 0 {
		return results, fmt.Errorf("partial retrieval error: %v", errorList)
	}

	logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d total documents", counter)}
	return results, nil
}

/*
*
unifiedQueryDocStore
*/
func unifiedQueryDocStore(optimusdb *KnowledgeBaseDB, logChan chan Log, dbtype string, criteria []string) ([]map[string]interface{}, error) {
	// Convert criteria []string to []FilterCriterion
	parsedFilters, err := ConvertCriteria(criteria)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to parse criteria: %v", err)}
		return nil, err
	}
	logChan <- Log{Type: Info, Data: fmt.Sprintf("Parsed filters: %v", parsedFilters)}

	// Select the appropriate docstore based on dbtype
	dbDocStore := *optimusdb.DsSWres
	switch dbtype {
	case "dsswres":
		dbDocStore = *optimusdb.DsSWres
	case "dsswresaloc":
		dbDocStore = *optimusdb.DsSWresaloc
	default:
		logChan <- Log{Type: Info, Data: "Defaulting to DsSWres as dbtype did not match available stores\n"}
		dbDocStore = *optimusdb.DsSWres
	}

	if dbDocStore == nil {
		logChan <- Log{Type: RecoverableErr, Data: "Selected docstore is nil"}
		return nil, fmt.Errorf("selected docstore is nil")
	}

	logChan <- Log{Type: Info, Data: "Querying the selected document store"}

	// Define the filter function
	filterFunc := func(doc interface{}) (bool, error) {
		// Assert that the doc is of type map[string]interface{}
		document, ok := doc.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("document is not of type map[string]interface{}")
		}

		// Apply each filter criterion to the document
		for _, criterion := range parsedFilters {
			fieldValue, exists := document[criterion.Field]
			if !exists {
				return false, nil // Skip documents without the required field
			}

			// Apply the operator
			switch criterion.Operator {
			case "=":
				if fieldValue != criterion.Value {
					return false, nil
				}
			case "!=":
				if fieldValue == criterion.Value {
					return false, nil
				}
			case ">":
				if fieldValue.(float64) <= criterion.Value.(float64) {
					return false, nil
				}
			case "<":
				if fieldValue.(float64) >= criterion.Value.(float64) {
					return false, nil
				}
			case ">=":
				if fieldValue.(float64) < criterion.Value.(float64) {
					return false, nil
				}
			case "<=":
				if fieldValue.(float64) > criterion.Value.(float64) {
					return false, nil
				}
			default:
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Unsupported operator: %s", criterion.Operator)}
				return false, fmt.Errorf("unsupported operator: %s", criterion.Operator)
			}
		}
		return true, nil
	}

	// Query the document store
	ctx := context.Background()
	rawResults, err := dbDocStore.Query(ctx, filterFunc)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to query documents: %v", err)}
		return nil, err
	}

	// Convert raw results to []map[string]interface{}
	results := make([]map[string]interface{}, 0, len(rawResults))
	for _, raw := range rawResults {
		document, ok := raw.(map[string]interface{})
		if !ok {
			logChan <- Log{Type: RecoverableErr, Data: "Failed to cast document to map[string]interface{}"}
			continue
		}
		results = append(results, document)
	}

	if len(results) > 0 {
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d matching records", len(results))}
	} else {
		logChan <- Log{Type: Info, Data: "No matching records found"}
	}

	return results, nil
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func ConvertCriteriaForCRUDPUT_rev(criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	// Check if criteria is empty
	if len(criteria) == 0 {
		return nil, fmt.Errorf("criteria is empty")
	}

	// Initialize the list of documents
	var documents []map[string]interface{}

	for _, criterion := range criteria {
		// Ensure the `_id` field exists in each document
		if _, exists := criterion["_id"]; !exists {
			return nil, fmt.Errorf("one of the documents is missing the '_id' field: %v", criterion)
		}

		// Clone the criterion map to avoid modifying the original input
		document := make(map[string]interface{})
		for key, value := range criterion {
			document[key] = value
		}

		// Append the cloned document to the list
		documents = append(documents, document)
	}

	return documents, nil
}

// ////////////////////
func crudGetDocStoreRev(
	optimusdb *KnowledgeBaseDB,
	logChan chan Log,
	dbtype string,
	h host.Host,
	criteria []map[string]interface{},
) ([]map[string]interface{}, error) {
	var StatusError error

	// Initialize the result slice
	var results []map[string]interface{}
	dbDocStore := *optimusdb.DsSWres
	var selectedDBType string

	switch dbtype {
	case "validations":
		selectedDBType = "validations"
		if optimusdb.Validations == nil {
			return nil, fmt.Errorf("validations store not initialized")
		}
		dbDocStore = *optimusdb.Validations

	case "kbdata":
		selectedDBType = "kbdata"
		if optimusdb.KBdata == nil {
			return nil, fmt.Errorf("kbdata store not initialized")
		}
		dbDocStore = *optimusdb.KBdata

	case "kbmetadata":
		selectedDBType = "kbmetadata"
		if optimusdb.KBMetadata == nil {
			return nil, fmt.Errorf("kbmetadata store not initialized")
		}
		dbDocStore = *optimusdb.KBMetadata

	case "tosca_imported":
		selectedDBType = "tosca_imported"
		if optimusdb.DsTOSCA_Imported == nil {
			return nil, fmt.Errorf("tosca_imported store not initialized")
		}
		dbDocStore = *optimusdb.DsTOSCA_Imported

	default:
		selectedDBType = "dsswres"
		if optimusdb.DsSWres == nil {
			return nil, fmt.Errorf("dsswres store not initialized")
		}
		dbDocStore = *optimusdb.DsSWres
	}

	fmt.Println("Selected DB Type:", selectedDBType)

	ctx := context.Background()
	counter := 0

	// Iterate over the criteria
	for _, criterion := range criteria {

		for _, value := range criterion {
			// Convert the value to a string
			strValue := fmt.Sprintf("%v", value)
			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Value for Criteria: %v", strValue)}
			if len(strValue) <= 0 {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("DEBUG: Invalid strValue: %v", strValue)}
				return nil, fmt.Errorf("failed to get documents: invalid strValue")
			}

			// Retrieve documents from the document store
			//docs, err1 := dbDocStore.Get(ctx, strValue, &iface.DocumentStoreGetOptions{PartialMatches: true})
			docs, err1 := dbDocStore.Get(ctx, strValue, &iface.DocumentStoreGetOptions{PartialMatches: false})

			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Attempt to retrieve documents: %d with value: %v", len(docs), strValue)}

			for _, doc := range docs {
				// Marshal the document into JSON
				docJSON, err2 := json.MarshalIndent(doc, "", "  ")
				if err2 != nil {
					logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error marshalling document: %v", err2)}
					continue
				}

				// Convert JSON to map[string]interface{}
				var docMap map[string]interface{}
				err3 := json.Unmarshal(docJSON, &docMap)
				if err3 != nil {
					logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error unmarshalling document JSON: %v", err3)}
					continue
				}

				// Append the converted document to results
				results = append(results, docMap)
				logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Document added to results: %v", docMap)}
				counter++
			}

			// Log successful retrieval
			if err1 == nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d documents", len(docs))}
			} else {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error retrieving documents: %v", err1)}
				StatusError = err1
			}
		}
	}

	// Final log and return
	if StatusError == nil {
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d documents in total", counter)}
		return results, nil
	}

	return nil, fmt.Errorf("failed to retrieve documents: %w", StatusError)
}

// ////////////////////
/**
Specialized Function for hte contribution records
*/
// Function to get contributions from OrbitDB EventLogStore
func getContri(
	optimusdb *KnowledgeBaseDB,
	logChan chan Log,
	dbtype string,
	criteria []map[string]interface{},
) ([]map[string]interface{}, error) {

	var results []map[string]interface{}
	var statusError error
	var counter int

	// Ensure Contributions database is initialized
	if optimusdb.Contributions == nil {
		logChan <- Log{Type: RecoverableErr, Data: "ERROR: Contributions DB is nil"}
		return nil, errors.New("contributions database is not initialized")
	}

	dbContri := *optimusdb.Contributions // No need to dereference
	ctx := context.Background()

	// Get all entries
	entries, err := dbContri.List(ctx, &orbitdb.StreamOptions{Amount: intPtr(-1)})
	//entries, err := dbContri.List(ctx, &orbitdb.StreamOptions{Amount: intPtr(-1))
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("ERROR: Failed to retrieve records for Contribution: %v", err)}
		return nil, err
	}

	for _, doc := range entries {
		// Marshal the document into JSON
		docJSON, err2 := json.MarshalIndent(doc, "", "  ")
		if err2 != nil {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error marshalling document: %v", err2)}
			continue
		}

		// Convert JSON to map[string]interface{}
		var docMap map[string]interface{}
		err3 := json.Unmarshal(docJSON, &docMap)
		if err3 != nil {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error unmarshalling document JSON: %v", err3)}
			continue
		}

		// Check if "value" key exists and decode it from Base64
		if encodedValue, ok := docMap["value"].(string); ok {
			decodedBytes, err := base64.StdEncoding.DecodeString(encodedValue)
			if err != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error decoding Base64 value: %v", err)}
				continue
			}

			// Convert the decoded bytes into a JSON object (if applicable)
			var decodedData map[string]interface{}
			err = json.Unmarshal(decodedBytes, &decodedData)
			if err != nil {
				logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Decoded Base64 is not valid JSON, keeping as string")}
				docMap["value"] = string(decodedBytes) // Store as a raw string
			} else {
				docMap["value"] = decodedData // Store as a JSON object
			}
		}

		// Append the converted document to results
		results = append(results, docMap)
		logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Document added to results: %v", docMap)}
		counter++

	}

	// Return error if no records found
	if statusError != nil {
		return nil, fmt.Errorf("failed to retrieve Contribution records: %w", statusError)
	}

	logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully retrieved %d Contribution records", counter)}
	//return nil, errors.New("no Contribution records found")
	return results, nil
}

func intPtr(i int) *int {
	return &i
}

/*
*
 */
func ConvertMetadataToMap(entry datamodel.MetadataEntry) map[string]interface{} {
	metadataMap := make(map[string]interface{})

	metadataMap["_id"] = entry.ID
	metadataMap["author"] = entry.Author
	metadataMap["metadata_type"] = entry.MetadataType
	metadataMap["component"] = entry.Component
	metadataMap["behaviour"] = entry.Behaviour
	metadataMap["relationships"] = entry.Relationships
	metadataMap["associated_id"] = entry.AssociatedID
	metadataMap["name"] = entry.Name
	metadataMap["description"] = entry.Description
	metadataMap["tags"] = entry.Tags
	metadataMap["status"] = entry.Status
	metadataMap["created_by"] = entry.CreatedBy
	metadataMap["created_at"] = entry.CreatedAt.Format(time.RFC3339)
	metadataMap["updated_at"] = entry.UpdatedAt.Format(time.RFC3339)
	metadataMap["related_ids"] = entry.RelatedIDs
	metadataMap["priority"] = entry.Priority
	metadataMap["scheduling_info"] = entry.SchedulingInfo
	metadataMap["sla_constraints"] = entry.SLAConstraints
	metadataMap["ownership_details"] = entry.OwnershipDetails
	metadataMap["audit_trail"] = entry.AuditTrail

	return metadataMap
}

/*
 */
func crudPutDocStoreRev(optimusdb *KnowledgeBaseDB, logChan chan Log, dbtype string, criteria []map[string]interface{}) ([]map[string]interface{}, error) {

	parsedRecords, err := ConvertCriteriaForCRUDPUT_rev(criteria)

	// Doc Specific
	dbDocStore := *optimusdb.DsSWres
	dbMetaDocStore := *optimusdb.KBMetadata

	dataRecordsAsInterface := make([]interface{}, len(parsedRecords))
	metadataRecordsAsInterface := make([]interface{}, len(parsedRecords))
	for i, record := range parsedRecords {
		dataRecordsAsInterface[i] = record
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Processing Record: %v ", dataRecordsAsInterface[i])}

		metadataEntry := datamodel.GenerateMetadataFromResource(record)
		metadataMap := ConvertMetadataToMap(metadataEntry)
		metadataRecordsAsInterface[i] = metadataMap
		///logChan <- Log{Type: Info, Data: fmt.Sprintf("Generated Metadata Record: %v", metadataRecordsAsInterface[i])}
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Generated Metadata Record (as struct): %+v", metadataEntry)}
	}

	ctx := context.Background()
	//logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG:...: %v ", dataRecordsAsInterface)}
	_, err = dbDocStore.PutAll(ctx, dataRecordsAsInterface)

	if err != nil {
		logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Error putting records: %v", err.Error())}
		return nil, err
	} else {

		logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Metadata To Be inserted: %v", metadataRecordsAsInterface)}

		// Debug Print Before Metadata Insertion
		for _, meta := range metadataRecordsAsInterface {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("DEBUG: Metadata Before Insertion: %v", meta)}
			if meta.(map[string]interface{})["_id"] == nil {
				logChan <- Log{Type: RecoverableErr, Data: "ERROR: Metadata is missing _id before insertion!"}
			}
		}

		_, err = dbMetaDocStore.PutAll(ctx, metadataRecordsAsInterface)
		if err != nil {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Problem in inserting %d MetaData Records", len(parsedRecords))}
			return nil, err
		} else {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully inserted %d MetaData Records", len(parsedRecords))}
		}

	}

	if err == nil {
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Successfully inserted %d records", len(parsedRecords))}
		return nil, nil
	}

	return nil, fmt.Errorf("failed to insert documents: %w", err)
}

/*
*
optimusdb *KnowledgeBaseDB  Knowledge Base DB instance.
logChan chan Log: Channel to log messages.
dbtype string: Specifies which docstore to use (dsswres or dsswresaloc).
records []map[string]interface{}: A slice of records to be inserted into the Data Store.

Return 1: []map[string]interface{} – This is the list of verified records.
Return 2: error – Any errors encountered during execution.
*/
func crudPutDocStore(optimusdb *KnowledgeBaseDB, logChan chan Log, dbtype string, criteria []string) ([]map[string]interface{}, error) {
	//func crudPutDocStore(optimusdb *KnowledgeBaseDB, logChan chan Log, dbtype string, request map[string]interface{}) (error, string) {

	//logChan <- Log{Type: Info, Data: fmt.Sprintf("In Function crudPutDocStore \n")}
	// Convert criteria []string to []FilterCriterion
	parsedRecords, err := ConvertCriteriaForCRUDPUT(criteria)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to parse criteria: %v\n", err)}
		return nil, fmt.Errorf("failed to parse criteria: %w", err)
	}

	// Select the appropriate docstore based on dstype
	dbDocStore := *optimusdb.DsSWres
	switch dbtype {
	case "dsswres":
		dbDocStore = *optimusdb.DsSWres
		//logChan <- Log{Type: Info, Data: fmt.Sprintf("Selected store %s\n ", dbtype)}
	case "dsswresaloc":
		dbDocStore = *optimusdb.DsSWresaloc
		//logChan <- Log{Type: Info, Data: fmt.Sprintf("Selected store %s\n ", dbtype)}
	default:
		logChan <- Log{Type: Info, Data: "Defaulting to DsSWres as dstype did not match available stores\n"}
		dbDocStore = *optimusdb.DsSWres
	}

	//if dbDocStore == nil {
	//	logChan <- Log{Type: RecoverableErr, Data: "Selected docstore is nil"}
	//	return nil, fmt.Errorf("selected docstore is nil")
	//}

	logChan <- Log{Type: Info, Data: fmt.Sprintf("parsedRecords %v ", parsedRecords)}
	// Insert each record into the Data Store
	ctx := context.Background()
	//
	insertedRecords := make([]map[string]interface{}, 0)
	//
	for _, rec := range parsedRecords {

		//logChan <- Log{Type: Info, Data: fmt.Sprintf("Insider Loop for  record %v \n ", rec)}
		// Construct a record to insert
		recordToInsert := map[string]interface{}{
			"Field": rec.Field,
			"Value": rec.Value,
		}

		// Insert the record into the datastore
		result, err := dbDocStore.Put(ctx, recordToInsert)
		//internal function
		status := func() string {
			if result == nil {
				return "ok"
			} else {
				return "error"
			}
		}()
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Put Status of Record: %v, result: %v , error: %v", recordToInsert, status, err)}

		// Add the inserted record to the tracking list for verification
		insertedRecords = append(insertedRecords, recordToInsert)
	}

	//return nil, nil
	// Verify the inserted records
	logChan <- Log{Type: Info, Data: "Verifying inserted records"}
	verifiedRecords := make([]map[string]interface{}, 0)
	for _, record := range insertedRecords {

		//internal function
		filterFunc := func(doc interface{}) (bool, error) {
			// Assert document type
			document, ok := doc.(map[string]interface{})
			if !ok {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to cast document: %v", doc)}
				return false, fmt.Errorf("document is not of type map[string]interface{}")
			}
			// Check if the document matches the inserted record
			// Log the document being checked
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Checking document: %v", document)}
			return document["Field"] == record["Field"] && document["Value"] == record["Value"], nil
		}
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Attempting to query after insert for verification: %v, error: %v", record, filterFunc)}
		// Query the docstore for the inserted record
		rawResults, err := dbDocStore.Query(ctx, filterFunc)
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to verify record: %v, error: %v", record, err)}
			continue
		}
		// Log raw results
		logChan <- Log{Type: Info, Data: fmt.Sprintf("Raw results for record %v: %v", record, rawResults)}

		for _, raw := range rawResults {
			doc, ok := raw.(map[string]interface{})
			if !ok {
				logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("Failed to cast raw result: %v", raw)}
				continue
			}
			verifiedRecords = append(verifiedRecords, doc)
		}
	}

	// Log verification results
	logChan <- Log{Type: Info, Data: fmt.Sprintf("Verified %d/%d records ", len(verifiedRecords), len(insertedRecords))}
	if len(verifiedRecords) != len(insertedRecords) {
		logChan <- Log{Type: RecoverableErr, Data: "Some records failed verification"}
		return verifiedRecords, fmt.Errorf("Some records failed verification:", err)
	}
	logChan <- Log{Type: Info, Data: "Return from function ..."}

	return verifiedRecords, nil
}

func filterCriterionToMap(fc FilterCriterion) map[string]interface{} {
	return map[string]interface{}{
		"Field":    fc.Field,
		"Operator": fc.Operator,
		"Value":    fc.Value,
	}
}

// ConvertCriteria /*
func ConvertCriteria(criteria []string) ([]FilterCriterion, error) {
	var filterCriteria []FilterCriterion

	//fmt.Println("In function ConvertCriteria")

	for _, c := range criteria {
		// Split the string into parts (assumes "field operator value" format)
		parts, err := SplitCriterion(strings.Fields(c)[0])
		if err != nil {
			fmt.Printf("Error parsing criterion: %s\n", err)
			continue
		}
		fmt.Println("Inside Loop: ", parts)
		//fmt.Println("size of parts: ", len(parts))
		field := parts[0]
		operator := parts[1]
		valueStr := parts[2]

		//fmt.Println("field: ", field)
		//fmt.Println("operator: ", operator)
		//fmt.Println("valueStr: ", valueStr)

		// Parse value to the correct type (int, float, or string)
		var value interface{}
		if i, err := strconv.Atoi(valueStr); err == nil {
			value = i // Integer value
		} else if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = f // Float value
		} else {
			value = valueStr // String value
		}

		// Create FilterCriterion
		filterCriteria = append(filterCriteria, FilterCriterion{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}

	return filterCriteria, nil
}

func SplitCriterion(criterion string) ([]string, error) {
	// List of supported operators
	operators := []string{"<=", ">=", "=", ">", "<", "!="}

	/// Find the operator in the criterion
	for _, operator := range operators {
		if idx := strings.Index(criterion, operator); idx != -1 {
			// Split into field, operator, and value
			field := strings.TrimSpace(criterion[:idx])
			value := strings.TrimSpace(criterion[idx+len(operator):])
			return []string{field, operator, value}, nil
		}
	}

	// Return an error if no operator is found
	return nil, fmt.Errorf("invalid criterion: %s", criterion)

}

func ConvertCriteriaForCRUDPUT(criteria []string) ([]FilterCriterion, error) {
	var filterCriteria []FilterCriterion

	//fmt.Println("Infunction ConvertCriteriaForCRUDPUT")
	for _, c := range criteria {
		// Split the string into parts (assumes "field operator value" format)
		//parts := strings.Fields(c)
		parts := strings.SplitN(strings.Fields(c)[0], "=", 2)
		//fmt.Println("Inside Loop: ", parts)

		field := parts[0]
		operator := "="
		valueStr := parts[1] // Handles multi-word values
		//fmt.Println("field: ", field)
		//fmt.Println("operator: ", operator)
		//fmt.Println("valueStr: ", valueStr)

		// Parse value to the correct type (int, float, or string)
		var value interface{}
		if i, err := strconv.Atoi(valueStr); err == nil {
			value = i // Integer value
		} else if f, err := strconv.ParseFloat(valueStr, 64); err == nil {
			value = f // Float value
		} else {
			value = valueStr // String value
		}

		// Create FilterCriterion
		filterCriteria = append(filterCriteria, FilterCriterion{
			Field:    field,
			Operator: operator,
			Value:    value,
		})
	}
	//fmt.Println("Left function ConvertCriteriaForCRUDPUT")
	return filterCriteria, nil
}

/*
*
Get the File
*/
func get(optimusdb *KnowledgeBaseDB, ipfsPath string, logChan chan Log) interface{} {
	db := *optimusdb.Contributions
	coreAPI := db.IPFS()
	ctx := context.Background()

	pth := path.New(ipfsPath)
	n, err := coreAPI.Unixfs().Get(ctx, pth)
	if err != nil {
		logChan <- Log{RecoverableErr, err}
		return nil
	}

	// determine destination location
	// TODO : can we get the file info/name from the node ?
	// otherwise add it to contribution block metadata
	fileName := strings.TrimPrefix(ipfsPath, "/ipfs/")
	dest := *config.FlagDownloadDir + fileName
	if dest[:2] == "~/" {
		// expand the tilde (~) notation to the user's home directory
		usr, err := user.Current()
		if err != nil {
			return err
		}
		dir := usr.HomeDir
		dest = filepath.Join(dir, dest[2:])
	}

	if err := files.WriteTo(n, dest); err != nil {
		logChan <- Log{RecoverableErr, err}
		return nil
	}

	return "stored " + ipfsPath + " successfully under " + dest
}

/*
executes post command, placing the file
*/
func post(optimusdb *KnowledgeBaseDB, node files.Node, logChan chan Log) interface{} {
	ctx := context.Background()
	coreAPI := (*optimusdb.Orbit).IPFS()

	// contributions store may be nil for non-root nodes
	db := optimusdb.Contributions
	if db == nil {
		err := errors.New("you need a datastore first, try connecting to a peer")
		logChan <- Log{Type: RecoverableErr, Data: err}
		return err
	}

	// store node in ipfs' blockstore as merkleDag and get it's key (= path)
	filePath, err := coreAPI.Unixfs().Add(ctx, node)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
		return err
	}

	// create the contribution block
	ipfsPath := filePath.String()
	//ts := time.Now()
	data := Contribution{GetAgentName(), optimusdb.Config.PeerID, ipfsPath, time.Now(), GetOwnIP(), GetPublicIPAddress(), []string{}}
	//data := Contribution{ipfsPath, optimusdb.Config.PeerID, ts, "", ""}
	dataJSON, err := json.Marshal(data)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
		return err
	}

	// add the contribution block
	optimusdb.ContributionsMtx.Lock()
	_, err = (*db).Add(ctx, dataJSON)
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
		return err
	}
	optimusdb.ContributionsMtx.Unlock()

	return "File uploaded"
}

// executes connect command
func connect(optimusdb *KnowledgeBaseDB, peerId string, logChan chan Log) string {
	ctx := context.Background()
	err := ipfs.ConnectToPeers(ctx, optimusdb.Orbit, []string{peerId})
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
	}
	return "Peer id processed"
}

// executes query command in a KeyValue store
func queryKeyValue(optimusdb *KnowledgeBaseDB, logChan chan Log) []Contribution {
	db := optimusdb.Contributions
	if db == nil {
		err := errors.New("you need a datastore first, try connecting to a peer")
		logChan <- Log{Type: RecoverableErr, Data: err}
		return nil
	}

	// fetch data from network
	infinity := -1
	ctx := context.Background()
	(*db).Load(ctx, infinity)

	// TODO : await ready event
	//time.Sleep(time.Second * 5)

	// get all entries and parse them
	res, err := (*db).List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
		return []Contribution{}
	}

	jsonRes := make([]Contribution, len(res))
	for i, op := range res {
		err := json.Unmarshal(op.GetValue(), &jsonRes[i])
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: err}
			continue
		}

		// TODO : optionally filter by validity
		valid, err := isValid(optimusdb, jsonRes[i].Path)
		if err == nil && valid {
			//fmt.Print("valid file found")
			logChan <- Log{Info, "Received HTTP request and found valid file " + jsonRes[i].Path}
		}

		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: err}
		}
	}

	return jsonRes
}

// executes query command
func query(optimusdb *KnowledgeBaseDB, logChan chan Log) []Contribution {
	db := optimusdb.Contributions
	if db == nil {
		err := errors.New("you need a datastore first, try connecting to a peer")
		logChan <- Log{Type: RecoverableErr, Data: err}
		return nil
	}

	// fetch data from network
	infinity := -1
	ctx := context.Background()
	(*db).Load(ctx, infinity)

	// TODO : await ready event
	//time.Sleep(time.Second * 5)

	// get all entries and parse them
	res, err := (*db).List(ctx, &orbitdb.StreamOptions{Amount: &infinity})
	if err != nil {
		logChan <- Log{Type: RecoverableErr, Data: err}
		return []Contribution{}
	}

	jsonRes := make([]Contribution, len(res))
	for i, op := range res {
		err := json.Unmarshal(op.GetValue(), &jsonRes[i])
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: err}
			continue
		}

		// TODO : optionally filter by validity
		valid, err := isValid(optimusdb, jsonRes[i].Path)
		if err == nil && valid {
			//fmt.Print("valid file found")
			logChan <- Log{Info, "Received HTTP request and found valid file " + jsonRes[i].Path}
		}

		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: err}
		}
	}

	return jsonRes
}

// ////
func sqlqdata(optimusdb *KnowledgeBaseDB, logChan chan Log) []Contribution {
	db := optimusdb.KBdata
	if db == nil {
		err := errors.New("you need a datastore first, try connecting to a peer")
		logChan <- Log{Type: RecoverableErr, Data: err}
		return nil
	}

	// fetch data from network
	infinity := -1
	ctx := context.Background()
	(*db).Load(ctx, infinity)

	// TODO : await ready event

	return nil
}

// checks if the file identified by the ipfs path is valid according to local
// entries or peers
func isValid(optimusdb *KnowledgeBaseDB, path string) (bool, error) {
	// check local entry
	validations := *optimusdb.Validations
	getopts := iface.DocumentStoreGetOptions{
		CaseInsensitive: false,
		PartialMatches:  false,
	}
	ctx := context.Background()
	local, err := validations.Get(ctx, path, &getopts)
	if err != nil {
		return false, err
	}

	// found a local entry
	if len(local) >= 1 {
		valdoc := local[0].(map[string]interface{})
		isValid := valdoc["isValid"].(bool)
		return isValid, nil
	}

	// no local entry, so fetch votes via pubsub and accumulate them
	validation, err := accValidations(optimusdb, path)
	if err != nil {
		return false, err
	}

	// persist result
	valdoc := map[string]interface{}{
		"path":    validation.Path,
		"isValid": validation.IsValid,
		"voteCnt": validation.VoteCnt,
	}

	// TODO : not 100% sure we need these locks
	optimusdb.ValidationsMtx.Lock()
	_, err = validations.Put(ctx, valdoc)
	if err != nil {
		isValid := valdoc["isValid"].(bool)
		return isValid, err
	}
	optimusdb.ValidationsMtx.Unlock()

	return validation.IsValid, nil
}

type ValidationReq struct {
	Path   string `json:"path"`
	PeerID string `json:"peerId"`
}
type ValidationRes struct {
	Vote bool `json:"vote"`
}

const validationReqTopic = "validation"

// requests and accumulates votes via pubsub
// returns a probability between 0 and 1 for validity of data
func accValidations(optimusdb *KnowledgeBaseDB, pth string) (Validation, error) {
	// receive votes via topic : this nodes id + the files path
	coreAPI := (*optimusdb.Orbit).IPFS()
	nodeId := (*optimusdb.Config).PeerID
	ctx := context.Background()
	resSub, err := coreAPI.PubSub().Subscribe(ctx, nodeId+pth)
	if err != nil {
		return Validation{}, err
	}

	// announce their wish via topic : "validation" with message data : their id + the files cid
	req := ValidationReq{pth, nodeId}
	reqData, err := json.Marshal(req)
	if err != nil {
		return Validation{}, err
	}

	err = coreAPI.PubSub().Publish(ctx, validationReqTopic, reqData)
	if err != nil {
		return Validation{}, err
	}

	// wait 5 seconds to accumulate votes
	timeout := 20 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	validCnt := 0
	inValidCnt := 0
	ret := false
	for {
		select {
		case <-ctx.Done():
			// the deadline has been reached or the context was canceled
			ret = true
		default:
			msg, err := resSub.Next(ctx)
			if err != nil {
				// TODO : log error
				continue
			}

			// accumulate votes
			var res ValidationRes
			err = json.Unmarshal(msg.Data(), &res)
			if err != nil {
				// TODO : log error
				continue
			}

			if res.Vote {
				validCnt++
				continue
			}

			inValidCnt++
		}

		if ret {
			break
		}
	}

	totalVotes := validCnt + inValidCnt
	validation := Validation{pth, false, uint32(totalVotes)}

	// if more than half have voted for valid, the data is considered valid
	// else self-validate
	// TODO : use KnownAddrs as reference instead of Peers ?
	peers, err := coreAPI.Swarm().Peers(ctx)
	if err != nil {
		return Validation{}, err
	}
	numPeers := float64(len(peers))
	if float64(validCnt) > (.5 * numPeers) {
		validation.IsValid = true
	} else {

		// get the file from ipfs
		parsedPth := path.New(pth)
		file, err := coreAPI.Unixfs().Get(ctx, parsedPth)
		if err != nil {
			return Validation{}, err
		}

		validation.IsValid, err = validateStub(file)
		if err != nil {
			return Validation{}, err
		}
	}

	return validation, nil
}

// waits for validation requests
func awaitValidationReq(optimusdb *KnowledgeBaseDB, logChan chan Log) {
	// receive validation requests via pubsub
	coreAPI := (*optimusdb.Orbit).IPFS()
	ctx := context.Background()
	resSub, err := coreAPI.PubSub().Subscribe(ctx, validationReqTopic)
	if err != nil {
		// TODO : is it correct to flag this as recoverable ?
		logChan <- Log{RecoverableErr, err}
	}

	for {
		msg, err := resSub.Next(ctx)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		var validationReq ValidationReq
		err = json.Unmarshal(msg.Data(), &validationReq)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		// from the validation store get the corresponding entry, if any
		validations := *optimusdb.Validations
		res, err := validations.Get(ctx, validationReq.Path, &iface.DocumentStoreGetOptions{})
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		// no internal vote
		// TODO : should be reason to listen to the voting topic aswell right ?
		if len(res) < 1 {
			continue
		}

		// only respond if the vote comes from self
		valdoc := res[0].(map[string]interface{})
		e := validationMapToStruct(valdoc)
		if e.VoteCnt != 0 {
			continue
		}

		validationRes := ValidationRes{e.IsValid}
		resTopic := validationReq.PeerID + validationReq.Path
		resData, err := json.Marshal(validationRes)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}

		err = coreAPI.PubSub().Publish(ctx, resTopic, resData)
		if err != nil {
			logChan <- Log{RecoverableErr, err}
			continue
		}
	}
}

// creates a validation struct from a map as returned from the validations
// docstore
func validationMapToStruct(m map[string]interface{}) Validation {
	pth := m["path"].(string)
	isValid := m["isValid"].(bool)
	voteCntF := m["voteCnt"].(float64)
	voteCntU := uint32(voteCntF)

	return Validation{
		Path:    pth,
		IsValid: isValid,
		VoteCnt: voteCntU,
	}
}

// wait for the replicated event and pin data if full replication is enabled
// TODO : this is very similar to awaitWriteEvent, try to combine the two and see
// if it makes sense
func awaitReplicateEvent(optimusdb *KnowledgeBaseDB, logChan chan Log) {
	// since contributions datastore may be nil, wait till it isn't
	for optimusdb.Contributions == nil {
		time.Sleep(time.Second)
	}

	// subscribe to replicated event
	contributions := *optimusdb.Contributions
	subdb, err := contributions.EventBus().Subscribe([]interface{}{
		new(stores.EventReplicated),
	})
	if err != nil {
		logChan <- Log{RecoverableErr, err}
		return
	}
	defer subdb.Close()

	coreAPI := (*optimusdb.Orbit).IPFS()

	subChan := subdb.Out()
	for {
		// get the new entry
		e := <-subChan
		re := e.(stores.EventReplicated)

		// check if the replication was executed on the contributions db
		if re.Address.GetPath() != contributions.Address().GetPath() {
			continue
		}
		entries := re.Entries

		logChan <- Log{Info, fmt.Sprintf("Replicated event with %d entries", len(entries))}
		for _, entry := range entries {
			// get the ipfs-log operation from the entry
			opStr := entry.GetPayload()
			var op opDoc
			err := json.Unmarshal(opStr, &op)
			if err != nil {
				logChan <- Log{RecoverableErr, err}
				continue
			}

			// parse to contribution block
			var contribution Contribution
			err = json.Unmarshal(op.Value, &contribution)
			if err != nil {
				logChan <- Log{RecoverableErr, err}
				continue
			}

			// store bootstrap and new contribution benchmark
			if *config.FlagBenchmark {
				optimusdb.Benchmark.UpdateBootstrap(contribution.CreationTS)
				optimusdb.Benchmark.UpdateNewContributions(contribution.CreationTS)
			}

			// replicate by adding pin
			if *config.FlagFullReplica {
				pth := contribution.Path

				ctx := context.Background()
				parsedPth := path.New(pth)
				opts := options.Pin.Recursive(true)
				coreAPI.Pin().Add(ctx, parsedPth, opts)
			}
		}
	}
}

func QueryUsingSQL(optimusdb *KnowledgeBaseDB, sqlQuery *SQLQuery) ([]map[string]interface{}, error) {
	ctx := context.Background()

	// Select the appropriate docstore based on dstype
	dbDocStore := *optimusdb.DsSWres

	// Define the filter function
	filterFunc := func(doc interface{}) (bool, error) {
		document, ok := doc.(map[string]interface{})
		if !ok {
			return false, fmt.Errorf("document is not of type map[string]interface{}")
		}

		// Apply all conditions (AND logic)
		for _, condition := range sqlQuery.Conditions {
			fieldValue, exists := document[condition.Field]
			if !exists {
				return false, nil
			}

			// Evaluate the condition
			switch condition.Operator {
			case "=":
				if fieldValue != condition.Value {
					return false, nil
				}
			case "!=":
				if fieldValue == condition.Value {
					return false, nil
				}
			case ">":
				if fieldValue.(float64) <= condition.Value.(float64) {
					return false, nil
				}
			case "<":
				if fieldValue.(float64) >= condition.Value.(float64) {
					return false, nil
				}
			case ">=":
				if fieldValue.(float64) < condition.Value.(float64) {
					return false, nil
				}
			case "<=":
				if fieldValue.(float64) > condition.Value.(float64) {
					return false, nil
				}
			default:
				return false, fmt.Errorf("unsupported operator: %s", condition.Operator)
			}
		}
		return true, nil
	}

	// Execute the query
	results, err := dbDocStore.Query(ctx, filterFunc)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Cast results to a slice of maps
	documents := []map[string]interface{}{}

	for i, result := range results {
		doc, ok := result.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid document type in query results")
		}
		fmt.Printf("Record %d: %v\n", i+1, doc) // Print the retrieved record
		documents = append(documents, doc)
	}

	return documents, nil
}

// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query local OrbitDB with enhanced filtering logic
func queryLocalDB(knowledgeBaseDB *KnowledgeBaseDB, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	ctx := context.Background()
	dbDocStore := *knowledgeBaseDB.DsSWres

	results, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		match := false

		// Loop through all criteria (supports OR conditions)
		for _, filter := range criteria {
			match = true

			for key, val := range filter {
				switch condition := val.(type) {
				case map[string]interface{}: // Handle special cases ($gte, $regex)
					if gteVal, exists := condition["$gte"]; exists {
						recordVal, ok := record[key].(float64) // Ensure numeric comparison
						if !ok {
							match = false
							continue
						}
						gteFloat, ok := gteVal.(float64)
						if !ok || recordVal < gteFloat {
							match = false
							continue
						}
					}
					if regexPattern, exists := condition["$regex"]; exists {
						recordStr, ok := record[key].(string)
						if !ok {
							match = false
							continue
						}
						regex, err := regexp.Compile(regexPattern.(string))
						if err != nil || !regex.MatchString(recordStr) {
							match = false
							continue
						}
					}
				default:
					if record[key] != val { // Default AND filter
						match = false
						continue
					}
				}
			}

			if match {
				return true, nil // At least one condition matched
			}
		}

		return false, nil // No match found
	})

	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	var documents []map[string]interface{}
	for _, result := range results {
		doc, ok := result.(map[string]interface{})
		if ok {
			documents = append(documents, doc)
		}
	}

	return documents, nil
}

/*  Change of Method 25.10.2025
func queryPeers(optimusdb *KnowledgeBaseDB, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	ctx := context.Background()
	//Gets a list of connected peers
	peers := optimusdb.Node.PeerHost.Peerstore().Peers()

	var allResults []map[string]interface{}

	// Debug: Print the list of peers
	log.Println("[DEBUG] queryPeers: Found", len(peers), "peers.")
	// Convert `criteria` to `[]interface{}` since Libp2p requires this format
	var criteriaAsInterface []interface{}
	for _, c := range criteria {
		criteriaAsInterface = append(criteriaAsInterface, c)
	}

	for _, peerID := range peers {
		if peerID == optimusdb.Node.Identity {
			log.Println("[DEBUG] Processing peer:", peerID)
			continue // Skip self
		}
		// Debug: Print criteria being sent
		log.Println("[DEBUG] Sending query to peer:", peerID, "with criteria:", criteriaAsInterface)

		// Send request to peer with converted criteria
		response, err := sendQueryToPeer(ctx, optimusdb, peerID, criteriaAsInterface)
		if err == nil && len(response) > 0 {
			allResults = append(allResults, response...)
			// Debug: Print received response
			log.Println("[DEBUG] Received response from peer:", peerID, "| Records:", len(response))
		} else if err != nil {
			log.Println("[ERROR] Failed to send query to peer:", peerID, "| Error:", err)
			continue
		}
	}

	// Debug: Print final results count
	log.Println("[DEBUG] queryPeers: Total records received from peers:", len(allResults))
	return allResults, nil
}
*/

// Change of Method 25.10.2025
func queryPeers(optimusdb *KnowledgeBaseDB, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	ctx := context.Background()
	peers := optimusdb.Node.PeerHost.Peerstore().Peers()
	var allResults []map[string]interface{}

	// Debug: Print the list of peers
	log.Println("[DEBUG] queryPeers: Found", len(peers), "peers.")
	// Convert `criteria` to `[]interface{}` since Libp2p requires this format
	var criteriaAsInterface []interface{}
	for _, c := range criteria {
		criteriaAsInterface = append(criteriaAsInterface, c)
	}

	for _, peerID := range peers {
		if peerID == optimusdb.Node.Identity {
			log.Println("[DEBUG] Processing peer:", peerID)
			continue // Skip self
		}
		// Debug: Print criteria being sent
		log.Println("[DEBUG] Sending query to peer:", peerID, "with criteria:", criteriaAsInterface)

		// Send request to peer with converted criteria
		response, err := sendQueryToPeer(ctx, optimusdb, peerID, criteriaAsInterface)
		if err == nil && len(response) > 0 {
			allResults = append(allResults, response...)
			// Debug: Print received response
			log.Println("[DEBUG] Received response from peer:", peerID, "| Records:", len(response))
		} else if err != nil {
			log.Println("[ERROR] Failed to send query to peer:", peerID, "| Error:", err)
			continue
		}
	}

	// Debug: Print final results count
	log.Println("[DEBUG] queryPeers: Total records received from peers:", len(allResults))
	return allResults, nil
}

// // Added optimized version of Query Peers
// /
// /
func queryPeersOptimized(optimusdb *KnowledgeBaseDB, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	ctx := context.Background()

	// Initialize engine if not already done (lazy initialization)
	if optimusdb.QueryEngine == nil {
		log.Println("[INFO] Initializing optimized query engine...")
		optimusdb.QueryEngine = queryengine.NewOptimizedEngine(
			8,              // 8 worker threads for parallel queries
			5*time.Second,  // 5 second timeout per query
			10*time.Minute, // 10 minute cache TTL
		)
	}

	// Execute optimized query with worker pool and caching
	results, err := optimusdb.QueryEngine.Query(
		ctx,
		optimusdb.Node.PeerHost,
		optimusdb.Node.Identity,
		criteria,
	)

	if err != nil {
		return nil, fmt.Errorf("optimized query failed: %w", err)
	}

	return results, nil
}

func sendQueryToPeer(ctx context.Context, optimusdb *KnowledgeBaseDB, peerID peer.ID, criteria []interface{}) ([]map[string]interface{}, error) {
	// Convert criteria back to JSON for transmission
	//jsonData, err := json.Marshal(criteria)
	//if err != nil {
	//	return nil, fmt.Errorf("failed to marshal request: %v", err)
	//}

	var convertedCriteria []map[string]interface{}
	for _, item := range criteria {
		mappedItem, ok := item.(map[string]interface{})
		if ok {
			convertedCriteria = append(convertedCriteria, mappedItem)
		} else {
			log.Println("[ERROR] Invalid query criteria format")
			return nil, fmt.Errorf("invalid query criteria format")
		}
	}

	// Create a properly formatted query message
	queryRequest := QueryMessage{Criteria: convertedCriteria}

	stream, err := optimusdb.Node.PeerHost.NewStream(ctx, peerID, "/query/1.0.0")
	if err != nil {
		log.Println("[ERROR] Failed to open stream to peer:", peerID, "| Error:", err)
		return nil, err
	}
	defer stream.Close()

	// Open a stream using the correct protocol
	// Open a stream to the peer

	/*
		// Send request
		_, err = stream.Write(append(jsonData, '\n'))
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %v", err)
		}

		// Read response
		var response []map[string]interface{}
		decoder := json.NewDecoder(stream)
		err = decoder.Decode(&response)
		if err != nil {
			return nil, fmt.Errorf("failed to parse response: %v", err)
		}
	*/
	// Send query request
	err = json.NewEncoder(stream).Encode(queryRequest)
	if err != nil {
		log.Println("[ERROR] Failed to send query request to peer:", peerID, "| Error:", err)
		return nil, err
	}

	// Read response
	var results []map[string]interface{}
	err = json.NewDecoder(stream).Decode(&results)
	if err != nil {
		log.Println("[ERROR] Failed to read query response from peer:", peerID, "| Error:", err)
		return nil, err
	}

	log.Println("[INFO] Successfully received query response from peer:", peerID, "| Records:", len(results))
	return results, nil

}

func cacheInOrbitDB(optimusdb *KnowledgeBaseDB, results []map[string]interface{}) error {
	ctx := context.Background()
	dbDocStore := *optimusdb.DsSWres

	for _, record := range results {
		_, err := dbDocStore.Put(ctx, record)
		if err != nil {
			return fmt.Errorf("failed to cache data: %w", err)
		}
	}

	return nil
}

func storeResults(optimusdb *KnowledgeBaseDB, logChan chan Log, dbType string, records []map[string]interface{}) {
	ctx := context.Background()

	// Select the correct document store based on the provided `dbType`
	var dbDocStore = *optimusdb.DsSWres

	/*
		switch dbType {
		case "dsswres":
			dbDocStore = optimusdb.DsSWres
		case "dsmetadata":
			dbDocStore = knowledgeBaseDB.KBMetadata
		default:
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Unknown database type: %s", dbType)}
			return
		}

	*/

	// Ensure the document store is available
	if dbDocStore == nil {
		logChan <- Log{Type: Info, Data: "Document store is nil, cannot store records"}
		return
	}

	// Iterate over received records and store them in OrbitDB
	for _, record := range records {
		_, err := dbDocStore.Put(ctx, record)
		if err != nil {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Failed to store record: %v", err)}
		} else {
			logChan <- Log{Type: Info, Data: fmt.Sprintf("Stored received record in OrbitDB: %v", record["_id"])}
		}
	}
}

type QueryMessage struct {
	Criteria []map[string]interface{} `json:"criteria"`
}

func handleQueryRequest(s network.Stream, knowledgeBaseDB *KnowledgeBaseDB) {
	defer s.Close()

	log.Println("[INFO] Received query request from peer:", s.Conn().RemotePeer())

	// Decode the query request
	var queryRequest QueryMessage
	err := json.NewDecoder(s).Decode(&queryRequest)
	if err != nil {
		log.Println("[ERROR] Failed to decode query request:", err)
		return
	}

	// Search the local database
	results, err := queryLocalDB(knowledgeBaseDB, queryRequest.Criteria)
	if err != nil {
		log.Println("[ERROR] Failed to query local DB:", err)
		return
	}

	// Send results back
	responseData, _ := json.Marshal(results)
	_, err = s.Write(responseData)
	if err != nil {
		log.Println("[ERROR] Failed to send query response:", err)
	}
}

func crudDeleteDocStoreRev(optimusdb *KnowledgeBaseDB, criteria []map[string]interface{}) (int, error) {
	ctx := context.Background()
	dbDocStore := *optimusdb.DsSWres
	var deletedCount int

	docs, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		for _, filter := range criteria {
			match := true
			for key, value := range filter {
				if record[key] != value {
					match = false
					break
				}
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	})

	if err != nil {
		return 0, fmt.Errorf("query execution failed: %w", err)
	}

	for _, doc := range docs {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		// Remove the document
		_, err := dbDocStore.Delete(ctx, docMap["_id"].(string))
		if err != nil {
			return deletedCount, fmt.Errorf("failed to delete document: %w", err)
		}
		deletedCount++
	}

	return deletedCount, nil
}

func crudUpdateDocStoreRev(optimusdb *KnowledgeBaseDB, criteria []map[string]interface{}, updateData []map[string]interface{}) (int, error) {
	ctx := context.Background()
	dbDocStore := *optimusdb.DsSWres
	var updatedCount int

	// Query for documents matching criteria
	docs, err := dbDocStore.Query(ctx, func(doc interface{}) (bool, error) {
		record, ok := doc.(map[string]interface{})
		if !ok {
			return false, nil
		}

		for _, filter := range criteria {
			match := true
			for key, value := range filter {
				if record[key] != value {
					match = false
					break
				}
			}
			if match {
				return true, nil
			}
		}
		return false, nil
	})

	if err != nil {
		return 0, fmt.Errorf("query execution failed: %w", err)
	}

	for _, doc := range docs {
		docMap, ok := doc.(map[string]interface{})
		if !ok {
			continue
		}

		// Apply updates from all maps in `updateData`
		for _, updates := range updateData {
			for key, value := range updates {
				docMap[key] = value
			}
		}

		// Save updated document back to OrbitDB
		_, err := dbDocStore.Put(ctx, docMap)
		if err != nil {
			return updatedCount, fmt.Errorf("failed to update document: %w", err)
		}
		updatedCount++
	}

	return updatedCount, nil
}

/*
Both dedupResults and DedupSQLResults now use parallel processing:
Each item is deduplicated in a separate goroutine.
Results are collected safely through a channel.
A sync.Map ensures thread-safe tracking of seen hashes.
*/

// dedupResults removes duplicate documents based on _id or content hash
// dedupResults removes duplicate documents based on _id or content hash using parallel processing
func dedupResults(results []map[string]interface{}) []map[string]interface{} {
	var (
		seen = sync.Map{}
		//mutex   = sync.Mutex{}
		deduped = []map[string]interface{}{}
		wg      sync.WaitGroup
	)

	resultCh := make(chan map[string]interface{}, len(results))

	for _, item := range results {
		wg.Add(1)
		item := item // capture range variable
		go func() {
			defer wg.Done()
			var key string
			if id, ok := item["_id"].(string); ok {
				key = id
			} else {
				b, _ := json.Marshal(item)
				key = fmt.Sprintf("%x", sha256.Sum256(b))
			}

			if _, loaded := seen.LoadOrStore(key, true); !loaded {
				resultCh <- item
			}
		}()
	}

	wg.Wait()
	close(resultCh)

	for item := range resultCh {
		deduped = append(deduped, item)
	}

	return deduped
}

// DedupSQLResults removes duplicate rows returned from SQL queries based on row content hash using parallel processing
func DedupSQLResults(rows []map[string]interface{}) []map[string]interface{} {
	var (
		seen    = sync.Map{}
		deduped = []map[string]interface{}{}
		wg      sync.WaitGroup
	)

	resultCh := make(chan map[string]interface{}, len(rows))

	for _, row := range rows {
		wg.Add(1)
		row := row // capture range variable
		go func() {
			defer wg.Done()
			b, _ := json.Marshal(row)
			hash := fmt.Sprintf("%x", sha256.Sum256(b))

			if _, loaded := seen.LoadOrStore(hash, true); !loaded {
				resultCh <- row
			}
		}()
	}

	wg.Wait()
	close(resultCh)

	for row := range resultCh {
		deduped = append(deduped, row)
	}

	return deduped
}

// SQLDMLWithPeerFallback attempts SQL locally, then queries peers if nothing is found.
func SQLDMLWithPeerFallback(req Request, logChan chan Log, db *KnowledgeBaseDB) (interface{}, error) {
	// Execute local SQL DML
	result, err := GlobalKBSQLite.SqlDML(req.SQLDML, logChan)
	if err != nil {
		return nil, fmt.Errorf("local SQLDML failed: %w", err)
	}

	// Only process metadata for INSERT statements
	upperSQL := strings.ToUpper(strings.TrimSpace(req.SQLDML))
	if strings.HasPrefix(upperSQL, "INSERT") {
		// Generate and store metadata asynchronously to avoid blocking
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("[WARN] Panic in metadata generation: %v", r)}
				}
			}()

			// 1️Base system-level metadata
			metadataEntry := datamodel.GenerateMetadataFromSQL(req.SQLDML)

			// 2️Enrich with contextual metadata (semantic / descriptive layer)
			/*
				contextualMeta := contextualMetadata.GenerateMetadataFromPayload(req.SQLDML)

				if contextualMeta != nil {
					if contextualMeta.Description != "" {
						metadataEntry.Description = contextualMeta.Description
					}
					if contextualMeta.Title != "" {
						metadataEntry.Name = contextualMeta.Title
					}
					if len(contextualMeta.Keywords) > 0 {
						metadataEntry.Tags = append(metadataEntry.Tags, contextualMeta.Keywords...)
					}
				}
			*/
			metadataEntry.Description = "Autogenerated description (TinyLlama disabled)"
			metadataEntry.Name = "SampleDataset_" + time.Now().Format("20060102_150405")

			metadataEntry.Tags = []string{"auto", "metadata", "fallback"}

			// 3️⃣ Convert and store metadata
			metadataMap := ConvertMetadataToMap(metadataEntry)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// --- Store metadata in OrbitDB (decentralized catalog) ---
			if db.KBMetadata != nil && *db.KBMetadata != nil {
				_, metaErr := (*db.KBMetadata).Put(ctx, metadataMap)
				if metaErr != nil {
					logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("[WARN] Failed to insert metadata into OrbitDB: %v", metaErr)}
				} else {
					logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] Metadata inserted into OrbitDB for SQL record ID=%s", metadataEntry.ID)}
				}
			}

			// --- Store metadata in local SQLite (metadata_catalog) ---
			if GlobalKBSQLite != nil && GlobalKBSQLite.DB != nil {
				insertMetaSQL := `
					INSERT INTO metadata_catalog (
						id, metadata_type, component, behaviour, description,
						created_by, created_at, updated_at, name, tags
					) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
				`
				_, sqlMetaErr := GlobalKBSQLite.DB.Exec(insertMetaSQL,
					metadataEntry.ID,
					metadataEntry.MetadataType,
					metadataEntry.Component,
					metadataEntry.Behaviour,
					metadataEntry.Description,
					metadataEntry.CreatedBy,
					metadataEntry.CreatedAt.Format(time.RFC3339),
					metadataEntry.UpdatedAt.Format(time.RFC3339),
					metadataEntry.Name,
					strings.Join(metadataEntry.Tags, ","),
				)

				if sqlMetaErr != nil {
					logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("[WARN] Failed to insert metadata into SQLite: %v", sqlMetaErr)}
				} else {
					logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] Metadata also inserted into local SQLite for ID=%s", metadataEntry.ID)}
				}
			}
		}()
	}

	// --- Handle remote peer queries if no local results ---
	records, ok := result.([]map[string]interface{})
	if ok && len(records) == 0 {
		logChan <- Log{Type: Info, Data: "[INFO] SQL DML: No local results — querying remote SQLite peers"}
		peerRecords, err := queryRemoteSQLitePeers(req, db)
		if err != nil {
			logChan <- Log{Type: RecoverableErr, Data: fmt.Sprintf("[WARN] Failed to query remote peers: %v", err)}
		} else if len(peerRecords) > 0 {
			records = append(records, peerRecords...)
			logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Retrieved %d records from remote peers", len(peerRecords))}
		}
	}

	// Deduplicate results
	deduped := DedupSQLResults(records)
	logChan <- Log{Type: Info, Data: fmt.Sprintf("[INFO] SQL DML: Retrieved %d → %d records after deduplication", len(records), len(deduped))}

	// Return appropriate response based on result type
	if _, ok := result.([]map[string]interface{}); ok {
		return map[string]interface{}{
			"records": deduped,
		}, nil
	}

	// Likely for INSERT, UPDATE, DELETE
	return map[string]interface{}{
		"status":  "success",
		"message": "SQL statement executed successfully",
	}, nil
}

// querySQLPeers sends SQLDML query to peers and collects responses
func querySQLPeers(req Request, db *KnowledgeBaseDB) ([]map[string]interface{}, error) {
	ctx := context.Background()
	peers := db.Node.PeerHost.Peerstore().Peers()
	var allResults []map[string]interface{}

	for _, peerID := range peers {
		if peerID == db.Node.Identity {
			continue
		}
		stream, err := db.Node.PeerHost.NewStream(ctx, peerID, "/sqldml/1.0.0")
		if err != nil {
			log.Printf("[ERROR] Could not create stream to peer %s: %v", peerID, err)
			continue
		}
		err = json.NewEncoder(stream).Encode(req)
		if err != nil {
			log.Printf("[ERROR] Failed to send SQL request to peer %s: %v", peerID, err)
			stream.Close()
			continue
		}
		var results []map[string]interface{}
		err = json.NewDecoder(stream).Decode(&results)
		stream.Close()
		if err == nil {
			allResults = append(allResults, results...)
		} else {
			log.Printf("[ERROR] Failed to decode response from peer %s: %v", peerID, err)
		}
	}

	return allResults, nil
}

// queryRemoteSQLitePeers sends SQL DML query to peers and collects responses from their SQLite instances
func queryRemoteSQLitePeers(req Request, db *KnowledgeBaseDB) ([]map[string]interface{}, error) {
	ctx := context.Background()
	peers := db.Node.PeerHost.Peerstore().Peers()
	var allResults []map[string]interface{}

	for _, peerID := range peers {
		if peerID == db.Node.Identity {
			continue
		}
		stream, err := db.Node.PeerHost.NewStream(ctx, peerID, "/sqldml/1.0.0")
		if err != nil {
			log.Printf("[ERROR] Could not create stream to peer %s: %v", peerID, err)
			continue
		}
		err = json.NewEncoder(stream).Encode(req)
		if err != nil {
			log.Printf("[ERROR] Failed to send SQL request to peer %s: %v", peerID, err)
			stream.Close()
			continue
		}
		var results []map[string]interface{}
		err = json.NewDecoder(stream).Decode(&results)
		stream.Close()
		if err == nil {
			allResults = append(allResults, results...)
		} else {
			log.Printf("[ERROR] Failed to decode response from peer %s: %v", peerID, err)
		}
	}

	return allResults, nil
}

func AwaitRegisterSQLDMLStreamHandler(hostCID host.Host, logChan chan Log) {
	hostCID.SetStreamHandler("/sqldml/1.0.0", func(s network.Stream) {
		defer s.Close()

		var req Request
		if err := json.NewDecoder(s).Decode(&req); err != nil {
			log.Printf("[ERROR] Failed to decode incoming SQLDML request: %v", err)
			return
		}

		result, err := GlobalKBSQLite.SqlDML(req.SQLDML, logChan)
		if err != nil {
			log.Printf("[ERROR] SQL execution error for peer request: %v", err)
			return
		}

		if records, ok := result.([]map[string]interface{}); ok {
			if err := json.NewEncoder(s).Encode(records); err != nil {
				log.Printf("[ERROR] Failed to send SQL results to peer: %v", err)
			}
		} else {
			_ = json.NewEncoder(s).Encode([]map[string]interface{}{}) // empty fallback
		}
	})
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// / EMS
func (db *KnowledgeBaseDB) ProcessEMS(action, resource string, params map[string]interface{}) error {
	// TODO: implement your actual EMS logic here
	fmt.Sprintf("[INFO]  EMS %s on %s params=%v", action, resource, params)
	GlobalLoggerDB.AddToOptimusLog("INFO",
		fmt.Sprintf("EMS %s on %s params=%v", action, resource, params), "ems")
	return nil
}

// annotate source information on each item
func annotate(items []map[string]interface{}, source string, peerID string, strategy QueryStrategy) {
	for _, it := range items {
		if _, ok := it["_source"]; !ok {
			it["_source"] = map[string]interface{}{
				"type":    source, // "local" or "peer"
				"peer_id": peerID,
			}
		}
		if _, ok := it["_trace"]; !ok {
			it["_trace"] = map[string]interface{}{
				"strategy": string(strategy),
			}
		}
	}
}

// localThenRemoteMerge executes local first, then remote, and merges within a time budget.
func localThenRemoteMerge(kb *KnowledgeBaseDB, criteria []map[string]interface{}, opt QueryOptions) ([]map[string]interface{}, error) {
	if opt.TimeBudgetMs <= 0 {
		opt.TimeBudgetMs = 1200
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opt.TimeBudgetMs)*time.Millisecond)
	defer cancel()

	var local []map[string]interface{}
	var remote []map[string]interface{}
	var lerr, rerr error
	var wg sync.WaitGroup

	if opt.IncludeLocal {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local, lerr = queryLocalDB(kb, criteria)
			if lerr == nil && opt.AnnotateSource {
				annotate(local, "local", "", StrategyLocalThenRemoteMerge)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		// respect cache TTL if provided via StaleOkTTLms by configuring your engine TTL
		if kb.QueryEngine == nil {
			kb.QueryEngine = queryengine.NewOptimizedEngine(8, 5*time.Second, 10*time.Minute)
		}
		remote, rerr = kb.QueryEngine.Query(ctx, kb.Node.PeerHost, kb.Node.Identity, criteria)
		if rerr == nil && opt.AnnotateSource {
			annotate(remote, "peer", "", StrategyLocalThenRemoteMerge)
		}
	}()

	wg.Wait()

	if lerr != nil && rerr != nil {
		return nil, fmt.Errorf("local and remote failed: local=%v remote=%v", lerr, rerr)
	}

	merged := append([]map[string]interface{}{}, local...)
	merged = append(merged, remote...)

	merged = dedupResults(merged)

	if opt.MinRows > 0 && len(merged) < opt.MinRows {
		// OPTIONAL: you can perform a second-chance fanout here if needed
	}

	return merged, nil
}

// parallelMerge fires local and remote immediately and returns merged results by budget.
func parallelMerge(kb *KnowledgeBaseDB, criteria []map[string]interface{}, opt QueryOptions) ([]map[string]interface{}, error) {
	// Same as localThenRemoteMerge (kept for clarity—here they are functionally equivalent)
	return localThenRemoteMerge(kb, criteria, opt)
}

// quorumMerge waits for N peer responses (or min rows) then merges with local.
func quorumMerge(kb *KnowledgeBaseDB, criteria []map[string]interface{}, opt QueryOptions) ([]map[string]interface{}, error) {
	if opt.QuorumN <= 0 {
		opt.QuorumN = 2
	}
	if opt.TimeBudgetMs <= 0 {
		opt.TimeBudgetMs = 2000
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(opt.TimeBudgetMs)*time.Millisecond)
	defer cancel()

	// local first (non-blocking)
	local, _ := queryLocalDB(kb, criteria)
	if opt.AnnotateSource {
		annotate(local, "local", "", StrategyQuorum)
	}

	// Ask engine for streaming/partial results from peers
	if kb.QueryEngine == nil {
		kb.QueryEngine = queryengine.NewOptimizedEngine(8, 5*time.Second, 10*time.Minute)
	}

	// Use worker pool to query peers individually and collect until quorum/minrows
	type peerChunk struct {
		rows   []map[string]interface{}
		peerID string
		err    error
	}
	chunks := make(chan peerChunk, 32)

	peers := kb.Node.PeerHost.Network().Peers()
	// Optionally sort peers by reputation here (if you expose a getter)
	if opt.MaxPeers > 0 && opt.MaxPeers < len(peers) {
		peers = peers[:opt.MaxPeers]
	}

	var wg sync.WaitGroup
	var gotPeers int
	var collected []map[string]interface{}

	// fanout
	for _, pid := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			rows, err := queryOnePeer(ctx, kb.Node.PeerHost, p, criteria)
			if err == nil && opt.AnnotateSource {
				annotate(rows, "peer", p.String(), StrategyQuorum)
			}
			chunks <- peerChunk{rows: rows, peerID: p.String(), err: err}
		}(pid)
	}

	go func() { wg.Wait(); close(chunks) }()

	for ch := range chunks {
		if ch.err == nil {
			gotPeers++
			collected = append(collected, ch.rows...)
			if (opt.Consistency == ConsistencyQuorum && gotPeers >= opt.QuorumN) ||
				(opt.MinRows > 0 && len(collected) >= opt.MinRows) {
				break
			}
		}
		select {
		case <-ctx.Done():
			break
		default:
		}
	}

	merged := append(local, collected...)
	merged = dedupResults(merged)
	return merged, nil
}

// queryOnePeer is a thin wrapper over your existing stream pattern.
func queryOnePeer(ctx context.Context, hostNode host.Host, peerID peer.ID, criteria []map[string]interface{}) ([]map[string]interface{}, error) {
	stream, err := hostNode.NewStream(ctx, peerID, "/query/1.0.0")
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	q := map[string]interface{}{"criteria": criteria}
	if err := json.NewEncoder(stream).Encode(q); err != nil {
		return nil, err
	}

	var rows []map[string]interface{}
	if err := json.NewDecoder(stream).Decode(&rows); err != nil {
		return nil, err
	}
	return rows, nil
}
