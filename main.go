package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pubsub_pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/lukesampson/figlet/figletlib"

	_ "github.com/mattn/go-sqlite3"

	"optimusdb/api"
	"optimusdb/app"
	"optimusdb/config"
	"optimusdb/election"
	"optimusdb/utilities"
)

func init() {
	app.InitAgentName()
}

// MeshTracer implements pubsub.EventTracer for debugging mesh formation
type MeshTracer struct{}

func (mt *MeshTracer) Trace(evt *pubsub_pb.TraceEvent) {
	if evt == nil || evt.Type == nil {
		return
	}

	switch *evt.Type {
	case pubsub_pb.TraceEvent_GRAFT:
		if evt.Graft != nil && evt.Graft.Topic != nil {
			peerID := ""
			if evt.PeerID != nil && len(evt.PeerID) > 0 {
				peerID = string(evt.PeerID)
				if len(peerID) > 8 {
					peerID = peerID[:8] + "..."
				}
			}
			log.Printf("[MESH] 🌿 GRAFT: Peer %s joined mesh for topic %s",
				peerID, *evt.Graft.Topic)
		}
	case pubsub_pb.TraceEvent_PRUNE:
		if evt.Prune != nil && evt.Prune.Topic != nil {
			peerID := ""
			if evt.PeerID != nil && len(evt.PeerID) > 0 {
				peerID = string(evt.PeerID)
				if len(peerID) > 8 {
					peerID = peerID[:8] + "..."
				}
			}
			log.Printf("[MESH] ✂️ PRUNE: Peer %s left mesh for topic %s",
				peerID, *evt.Prune.Topic)
		}
	case pubsub_pb.TraceEvent_JOIN:
		if evt.Join != nil && evt.Join.Topic != nil {
			log.Printf("[MESH] ➕ JOIN: Subscribed to topic %s", *evt.Join.Topic)
		}
	case pubsub_pb.TraceEvent_ADD_PEER:
		peerID := ""
		if evt.PeerID != nil && len(evt.PeerID) > 0 {
			peerID = string(evt.PeerID)
			if len(peerID) > 8 {
				peerID = peerID[:8] + "..."
			}
		}
		log.Printf("[MESH] 👥 ADD_PEER: Connected to %s", peerID)
	}
}

// MonitorMeshStatus monitors and logs mesh formation
func MonitorMeshStatus(ctx context.Context, ps *pubsub.PubSub, topic *pubsub.Topic, host host.Host) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Get all topic peers
			topicPeers := topic.ListPeers()

			// Get all connected peers
			allPeers := host.Network().Peers()

			// Get mesh peers for the specific topic
			meshPeers := ps.ListPeers("optimusdb")

			log.Printf("[MESH-STATUS] ════════════════════════════════")
			log.Printf("[MESH-STATUS] Connected peers: %d", len(allPeers))
			log.Printf("[MESH-STATUS] Topic 'optimusdb' subscribers: %d", len(topicPeers))
			log.Printf("[MESH-STATUS] Mesh peers: %d", len(meshPeers))

			// List mesh peer details
			for i, p := range meshPeers {
				shortID := p.String()
				if len(shortID) > 8 {
					shortID = shortID[:8] + "..."
				}
				connectedness := host.Network().Connectedness(p)
				log.Printf("[MESH-STATUS]   [%d] %s - %s", i+1, shortID, connectedness)
			}
			log.Printf("[MESH-STATUS] ════════════════════════════════")
		}
	}
}

func main() {
	flag.Parse()

	printSwarmchestrate()

	// Metrics (optional)
	if *config.FlagMetrics {
		interval := 2 * time.Second
		if runtime.GOOS == "windows" {
			log.Printf("Running on Windows")
			utilities.GetMemoryUsage()
			utilities.GetDiskUsage(interval)
		} else {
			log.Printf("Running on OS: %s", runtime.GOOS)
			utilities.GetMemoryUsage()
			utilities.GetCPUUsage()
			utilities.GetNetworkUsage()
			utilities.GetDiskUsage(interval)
		}
	}

	// Termination context
	termCtx, termCancel := context.WithCancel(context.Background())

	// Init logging DB
	app.GlobalLoggerDB, _ = app.InitLog()

	// Reputation DB
	election.GlobalReputationDB, _ = election.InitReputationDB()

	// OS signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		termCancel()
	}()

	// Optional benchmark monitors
	var bench app.Benchmark
	if *config.FlagBenchmark {
		go app.MonitorMemoryAndCPU(termCtx, &bench)
	}

	// Central logging channel
	logChan := make(chan app.Log, 100)

	// Datastores
	var knowledgeBaseDB app.KnowledgeBaseDB
	var rdbms app.KnowledgeBaseSQLite
	defer rdbms.Close()

	// Init peer + KB components
	if err := app.InitPeer(&knowledgeBaseDB, &rdbms, &bench, logChan); err != nil {
		fmt.Fprintf(os.Stderr, "Error on setup:\n %+v\n", err)
		os.Exit(1)
	}

	// HostID for payloads/fallbacks
	if knowledgeBaseDB.Node != nil && knowledgeBaseDB.Node.PeerHost != nil {
		knowledgeBaseDB.HostID = knowledgeBaseDB.Node.PeerHost.ID().String()
	}

	// EMS subscriber (ActiveMQ/STOMP)
	emsCtx, emsCancel := context.WithCancel(termCtx)
	cleanupEMS, err := knowledgeBaseDB.StartEMSSubscriber(emsCtx)
	if err != nil {
		log.Printf("[ERROR] EMS init failed: %v", err)
		if app.GlobalLoggerDB != nil {
			_ = app.GlobalLoggerDB.AddToOptimusLog("ERROR", "EMS init failed: "+err.Error(), runtime.GOOS)
		}
	} else {
		go func() {
			<-termCtx.Done()
			_ = cleanupEMS()
			emsCancel()
		}()
		log.Println("[INFO] EMS service started (auto-reconnect enabled)")
		if app.GlobalLoggerDB != nil {
			_ = app.GlobalLoggerDB.AddToOptimusLog("INFO", "EMS service started (auto-reconnect enabled)", runtime.GOOS)
		}
	}

	// API channels
	reqChan := make(chan app.Request, 100)
	resChan := make(chan interface{}, 100)

	// Log fan-in
	go func() {
		for l := range logChan {
			switch l.Type {
			case app.RecoverableErr:
				if err, ok := l.Data.(error); ok {
					log.Printf("[ERROR] Recovering from : %v\n", err)
				} else {
					log.Printf("[ERROR] Recovering from non-error type: %v\n", l.Data)
				}
			case app.NonRecoverableErr:
				if err, ok := l.Data.(error); ok {
					log.Printf("[ERROR] Cannot recover from : %+v\n", err)
				} else {
					log.Printf("[ERROR] Non-recoverable issue, but not an error type: %v\n", l.Data)
				}
				termCancel()
			case app.Info:
				if msg, ok := l.Data.(string); ok {
					log.Printf("[INFO] Logging Channel: %s\n", msg)
				} else {
					log.Printf("[INFO] Unexpected info format: %v\n", l.Data)
					_ = app.GlobalLoggerDB.AddToOptimusLog("WARN", fmt.Sprintf("Unexpected info format: %+v", l.Data), runtime.GOOS)
				}
			case app.Print:
				log.Print(l.Data)
				_ = app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Main Data: %+v", l.Data), runtime.GOOS)
			default:
				log.Printf("[WARN] Unknown log type: %+v\n", l)
				_ = app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Unknown log type: %+v", l), runtime.GOOS)
			}
		}
	}()

	// Optional shell
	if *config.FlagShell {
		go api.Shell(reqChan, resChan, logChan)
	}
	// HTTP API
	if *config.FlagHTTP {
		go api.ServeHTTP(&knowledgeBaseDB, app.GlobalLoggerDB, reqChan, resChan, logChan)
	}

	// ===============================
	// USE SINGLE HOST FOR EVERYTHING
	// ===============================
	hostMain := knowledgeBaseDB.Node.PeerHost
	log.Println("[INIT] Using unified libp2p host for discovery and GossipSub")
	log.Println("[INIT] Libp2p Node ID:", hostMain.ID())

	// Register SQL stream handler on main host
	go app.AwaitRegisterSQLDMLStreamHandler(hostMain, logChan)

	// Main service loop on main host
	go app.Service(&knowledgeBaseDB, reqChan, resChan, hostMain, logChan, &rdbms)

	// ===============================
	// PEER DISCOVERY (MUST START FIRST - IT CREATES GOSSIPSUB)
	// ===============================
	var discoveryService *api.Service

	if *config.FlagAutodiscovery {
		log.Println("[DISCOVERY] Auto Discovery for Peers has been enabled")
		_ = app.GlobalLoggerDB.AddToOptimusLog("INFO", "Auto Discovery for Peers has been enabled", runtime.GOOS)

		var prMsg string
		if *config.FlagAutodiscoveryMDNS {
			prMsg = "Using MDNS for Auto-Discovery"
		} else if *config.FlagAutodiscoveryipfsPubSub {
			prMsg = "Using IPFS PubSub for Auto-Discovery"
		} else if *config.FlagAutodiscoveryDHT {
			prMsg = "Using DHT for Auto-Discovery"
		} else {
			prMsg = "No Auto-Discovery method selected"
		}
		log.Println("[DISCOVERY]", prMsg)
		_ = app.GlobalLoggerDB.AddToOptimusLog("INFO", prMsg, runtime.GOOS)

		// Start discovery - this will create GossipSub if PubSub discovery is enabled
		discoveryService = api.StartDiscovery(hostMain, &knowledgeBaseDB)
		if discoveryService == nil {
			log.Println("[ERROR] Discovery service failed to start")
			_ = app.GlobalLoggerDB.AddToOptimusLog("ERROR", "Discovery service failed to start", runtime.GOOS)
		} else {
			log.Println("[DISCOVERY] ✅ Discovery service started on unified host")

			// Wait for initial peer discovery
			log.Println("[DISCOVERY] Waiting for peer discovery...")
			time.Sleep(3 * time.Second)

			go api.PrintDiscoveredPeers(&knowledgeBaseDB)
		}
	}

	// ===============================
	// REUSE GOSSIPSUB FROM DISCOVERY FOR ELECTION
	// ===============================
	var ps *pubsub.PubSub
	var electionTopic *pubsub.Topic
	var electionSub *pubsub.Subscription

	if discoveryService != nil && discoveryService.Pubsub != nil {
		// Reuse the GossipSub instance from discovery
		ps = discoveryService.Pubsub
		electionTopic = discoveryService.Topic
		electionSub = discoveryService.Sub

		log.Println("[INIT] ✅ Reusing GossipSub from discovery service")
		log.Println("[INIT] ✅ Topic: 'optimusdb' already joined")
		log.Println("[INIT] ✅ Subscription already active")
	} else {
		// Fallback: Create new GossipSub if discovery didn't create one
		log.Println("[INIT] Creating new GossipSub instance (discovery not using pubsub)...")

		messageIDFunc := func(pmsg *pubsub_pb.Message) string {
			h := sha256.New()
			h.Write(pmsg.Data)
			h.Write(pmsg.From)
			return hex.EncodeToString(h.Sum(nil))[:20]
		}

		gparams := pubsub.DefaultGossipSubParams()
		gparams.D = 3
		gparams.Dlo = 2
		gparams.Dhi = 6
		gparams.Dscore = 2
		gparams.Dout = 2
		gparams.Dlazy = 3
		gparams.HeartbeatInterval = 700 * time.Millisecond
		gparams.HistoryLength = 10
		gparams.HistoryGossip = 5
		gparams.GossipFactor = 0.25
		gparams.OpportunisticGraftTicks = 30
		gparams.OpportunisticGraftPeers = 2
		gparams.PruneBackoff = 10 * time.Second
		gparams.GraftFloodThreshold = 2 * time.Second
		gparams.FanoutTTL = 30 * time.Second

		psOpts := []pubsub.Option{
			pubsub.WithMessageIdFn(messageIDFunc),
			pubsub.WithSeenMessagesTTL(2 * time.Minute),
			pubsub.WithFloodPublish(true),
			pubsub.WithPeerExchange(true),
			pubsub.WithDirectPeers([]peer.AddrInfo{}),
			pubsub.WithGossipSubParams(gparams),
			pubsub.WithDirectConnectTicks(5),
			pubsub.WithEventTracer(&MeshTracer{}),
		}

		if trace := os.Getenv("GOSSIPSUB_TRACE"); trace != "" {
			if tr, err := pubsub.NewJSONTracer(trace); err == nil {
				psOpts = append(psOpts, pubsub.WithEventTracer(tr))
				log.Printf("[TRACE] GossipSub trace enabled: %s", trace)
			}
		}

		ps, err = pubsub.NewGossipSub(termCtx, hostMain, psOpts...)
		if err != nil {
			log.Fatalf("[FATAL] Failed to initialize GossipSub: %v", err)
		}

		log.Println("[INIT] ✅ GossipSub initialized successfully")
		log.Printf("[INIT] ✅ Configuration: D=%d, Dlo=%d, Dhi=%d, Heartbeat=%v",
			gparams.D, gparams.Dlo, gparams.Dhi, gparams.HeartbeatInterval)
		log.Println("[INIT] ✅ FloodPublish=true, PeerExchange=true")

		electionTopic, err = ps.Join("optimusdb")
		if err != nil {
			log.Fatalf("[FATAL] Failed to join election topic: %v", err)
		}
		log.Println("[INIT] ✅ Joined election topic 'optimusdb'")

		electionSub, err = electionTopic.Subscribe()
		if err != nil {
			log.Fatalf("[FATAL] Failed to subscribe to election topic: %v", err)
		}
		log.Println("[INIT] ✅ Subscribed to election topic")
	}

	// Store in knowledgeBaseDB for election to use
	knowledgeBaseDB.ElectionTopic = electionTopic
	knowledgeBaseDB.ElectionSub = electionSub
	knowledgeBaseDB.PubSub = ps

	log.Println("[INIT] ✅ Election topic and subscription ready")

	// ===============================
	// START MESH MONITORING
	// ===============================
	go MonitorMeshStatus(termCtx, ps, electionTopic, hostMain)

	// ===============================
	// START ELECTION CONTROLLER
	// ===============================
	log.Println("[INIT] Waiting for GossipSub mesh to stabilize...")
	time.Sleep(5 * time.Second)

	log.Println("[INIT] Starting Election Controller...")
	go election.RunFullNode(termCtx, hostMain, ps, &knowledgeBaseDB)

	// Register shutdown handler
	if discoveryService != nil {
		go handleShutdown(discoveryService, &knowledgeBaseDB, hostMain)
	}

	// Await termination
	<-termCtx.Done()
	fmt.Printf("\n[SHUTDOWN] Shutting down OptimusDB node...\n")

	// Persist config & benchmark
	config.SaveStructAsJSON(knowledgeBaseDB.Config, *config.FlagRepo+"_config")
	benchmarkPath := *config.FlagRepo + "_benchmark"
	config.SaveStructAsJSON(knowledgeBaseDB.Benchmark, benchmarkPath)

	// Close OrbitDB
	if knowledgeBaseDB.Orbit != nil {
		(*knowledgeBaseDB.Orbit).Close()
	}

	log.Println("[SHUTDOWN] Complete")
}

func printSwarmchestrate() {
	fontsDir := "/usr/share/figlet/fonts"
	var font *figletlib.Font

	if _, err := os.Stat(fontsDir); os.IsNotExist(err) {
		fmt.Println("Directory does not exist:", fontsDir)
		fontsDir = figletlib.GuessFontsDirectory()
		f, err := figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			fmt.Println("Error loading font:", err)
			return
		}
		font = f
	} else {
		fmt.Println("Directory exists:", fontsDir)
		f, err := figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			fmt.Println("Error loading font:", err)
			return
		}
		font = f
	}

	figletlib.PrintMsg("Swarmchestrate", font, 80, font.Settings(), "")
	figletlib.PrintMsg("ICCS", font, 40, font.Settings(), "")
}

func handleShutdown(service *api.Service, knowledgeBaseDB *app.KnowledgeBaseDB, h host.Host) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	log.Println("[SHUTDOWN] Received shutdown signal...")

	if service != nil {
		service.StopDiscovery()
		log.Println("[SHUTDOWN] Peer Discovery stopped.")
	}

	if knowledgeBaseDB != nil && knowledgeBaseDB.Orbit != nil {
		(*knowledgeBaseDB.Orbit).Close()
		log.Println("[SHUTDOWN] OrbitDB instance closed.")
	}

	if err := h.Close(); err != nil {
		log.Println("[ERROR] Error while closing LibP2P host:", err)
	} else {
		log.Println("[SHUTDOWN] LibP2P host shut down successfully.")
	}

	os.Exit(0)
}

func shutdownHandler(fn func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for {
			switch <-ch {
			case syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT:
				fn()
				os.Exit(0)
			}
		}
	}()
}
