package config

import (
	"flag"
	"os"
	"time"
)

var FlagShell = flag.Bool("shell", false, "enable shell interface")
var FlagHTTP = flag.Bool("http", true, "enable http interface")

var FlagIPFSPort = flag.String("ipfs-port", "4001", "configure ipfs port")
var FlagHTTPPort = flag.String("http-port", "8089", "configure http port")

var FlagExp = flag.Bool("experimental", true, "enable ipfs experimental features")
var FlagRepo = flag.String("repo", "swarmkbIpfs", "configure the repo/directory name for the ipfs KUBO node")
var FlagDevLogs = flag.Bool("devlogs", false, "enable development level logging for optimusdb")
var FlagCoordinator = flag.Bool("coordinator", true, "creating a Coordinator (LSA) node means it's possible to create a new datastore")
var FlagDownloadDir = flag.String("download-dir", "~/", "the destination path for downloaded data")
var FlagFullReplica = flag.Bool("full-replica", true, "pins all added data")
var FlagBootstrap = flag.String("bootstrap", "", "set a bootstrap peer to connect to on startup")
var FlagBenchmark = flag.Bool("benchmark", false, "enable benchmarking")
var FlagSwarmName = flag.String("Swarmchestrate", "", "the swarm name this agent is operating")

var FlagMetrics = flag.Bool("metrics", true, "enable metrics for CPU,RAM,etc..")

/*
Dynamic document stores.

Store names are not fixed at compile time. When true (the default) any request
naming a store that does not exist — crudput, crudget, query, file upload,
semantic hydration — creates that document store on demand and makes it live:
replicated, queryable, listed by GET /api/v1/stores, included in export and
import, and reopened after a restart.

Set false to freeze the set of stores: an unknown dstype is then rejected with
an error listing the stores that do exist, and new ones must be created
deliberately via POST /api/v1/stores. Useful when you want a typo to fail loudly
rather than produce an empty store.

Either way an unknown dstype is NEVER silently redirected to dsswres, which was
the old behaviour and the reason trust documents ended up mixed into the swarm
resources store.
*/
var FlagDynamicStores = flag.Bool("dynamic-stores", true,
	"allow implicit creation of a document store on first write to an unknown dstype")

/*
This is for the discocvery
*/
var FlagAutodiscovery = flag.Bool("autodis", true, "aim to address autodiscovery under multiswarm")
var FlagAutodiscoveryMDNS = flag.Bool("dismDNS", true, "aim to address autodiscovery under mDNS")
var FlagAutodiscoveryipfsPubSub = flag.Bool("disIpfsPubSub", false, "aim to address autodiscovery under ipfsPubSub")
var FlagAutodiscoveryDHT = flag.Bool("disDHT", false, "aim to address autodiscovery under DHT")

/*
THe P2P context among the agents of the swarm
*/
var FlagContext = flag.String("swarmkb", "swarmkb", "set a context to use for http and other P2P")

var Flagdsswres = flag.String("dsswres", "dsswres", "dsswres Data Store")
var Flagkbdata = flag.String("kbdata", "kbdata", "kbdata Data Store")
var Flagkbmetadata = flag.String("kbmetadata", "kbmetadata", "kbmetadata Data Store")
var Flagcontributions = flag.String("contributions", "contributions", "contributions Data Store")

var Flagdsswresaloc = flag.String("dsswresaloc", "dsswresaloc", "dsswresaloc Data Store")

var FlagRDBMSDB = flag.String("kbrdbms", "kbrdbms", "kbrdbms Database")
var FlagRDBMSTable1 = flag.String("datacatalog", "datacatalog", "datacatalog RDBMS")

var FlagLogFilename = flag.String("logfile", "logs/optimusdb.log", "The log path and filename of OptimusDB")
var FlagLokiIsDisabled = flag.Bool("LokiIsDisabled", false, "enables Loki telemetry")
var ElectionMaxRetries = flag.Int("election-retry-limit", 1, "Max number of election retry attempts")
var ElectionRetryDelay = flag.Duration("election-retry-delay", 3*time.Second, "Initial delay before retrying election")

/** FOr integrating with Monitoring system (EMS)
 */
// Messaging / MQ flags
var (
	MQURL   = flag.String("mq-url", getenvDefault("MQ_URL", ""), "STOMP broker URL (e.g., tcp://localhost:61610)")
	MQUser  = flag.String("mq-user", getenvDefault("MQ_USER", "admin"), "STOMP username")
	MQPass  = flag.String("mq-pass", getenvDefault("MQ_PASS", "admin"), "STOMP password")
	MQTopic = flag.String("mq-topic", getenvDefault("MQ_TOPIC", "/topic/>"), "STOMP topic path")
)

func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
