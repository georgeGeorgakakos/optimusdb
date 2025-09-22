package main

//Update 22.09.2025
import (
	"context"
	"flag"
	"fmt"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/lukesampson/figlet/figletlib"
	"optimusdb/election"

	//"github.com/mk6i/mkdb/storage"
	_ "github.com/mattn/go-sqlite3"
	"log"
	"optimusdb/api"
	"optimusdb/app"
	"optimusdb/config"
	_ "optimusdb/election"
	"optimusdb/utilities"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

// main function which terminates when SIGINT or SIGTERM is received
// via termCtx/termCancel, any cancellation can be forwarded to go routines for
// graceful shutdown

// //// This is for the RDBMS storage

func init() {
	//if err := storage.InitStorage(); err != nil {
	//	panic(fmt.Sprintf("storage init error: %s", err.Error()))
	//}
	// Initialize the application name
	app.InitAgentName()
}

// / Main Function
func main() {

	flag.Parse()

	// Write the text as ASCII art
	printSwarmchestrate()

	///////// Metrics start

	///// UDP Fix
	//addr, _ := net.ResolveUDPAddr("udp", ":4242")
	//conn, _ := net.ListenUDP("udp", addr)
	//_ = setUDPBufferSize(conn)
	//quic.Listen(conn, nil, nil)

	//// RDBMS struct
	//rdbms := &app.KnowledgeBaseRDBMS{
	//	Session: &engine.Session{},
	//}
	//shutdownHandler(func() {
	//	rdbms.Session.Close()
	//})
	// Create SQLite RDBMS Instance

	//getCPUUsage(interval)  Computes CPU usage over the given interval.
	//getMemoryUsage()  Fetches total, used, and free memory.
	//getDiskUsage(interval)  Monitors disk read/write speeds per second.
	//getNetworkUsage(interval)  Measures network bandwidth usage per second.
	// Check if metrics are enabled
	if *config.FlagMetrics {
		interval := 2 * time.Second // Measurement interval

		if runtime.GOOS == "windows" {
			log.Printf("Running on Windows")
			utilities.GetMemoryUsage()
			utilities.GetDiskUsage(interval)
		} else {
			//fmt.Println("Not running on Windows (OS:", runtime.GOOS, ")")
			log.Printf("Running on OS: %s", runtime.GOOS)
			utilities.GetMemoryUsage()
			utilities.GetCPUUsage()
			utilities.GetNetworkUsage()
			utilities.GetDiskUsage(interval)
		}
	}

	// prep termination context
	termCtx, termCancel := context.WithCancel(context.Background())
	// Initialize logging DB very early

	app.GlobalLoggerDB, _ = app.InitLog()
	//log.Printf("[ERROR] creating database GlobalLoggerDB: %v", err1)
	//log.Printf("Running on OS: %s", runtime.GOOS)

	// Create here the reputation DB
	election.GlobalReputationDB, _ = election.InitReputationDB()
	//log.Printf("Running on OS: %s", runtime.GOOS)

	// prep channel for os signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// handle os signals
	go func() {
		// wait for receiving an interrupt/termination signal
		<-sigs
		termCancel()
	}()

	// run profiling
	var bench app.Benchmark
	if *config.FlagBenchmark {
		go app.MonitorMemoryAndCPU(termCtx, &bench)
	}

	// channel for centralized loggin
	/////////////////////////////////////////////////
	logChan := make(chan app.Log, 100)
	// init application
	var knowledgeBaseDB app.KnowledgeBaseDB
	var rdbms app.KnowledgeBaseSQLite //rdbms := &app.KnowledgeBaseSQLite{}

	///////////////////// PEER initiation and KB components
	//err := app.InitPeer(&knowledgeBaseDB, &bench, logChan) -- change 28022025
	err := app.InitPeer(&knowledgeBaseDB, &rdbms, &bench, logChan)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error on setup:\n %+v\n", err)
		os.Exit(1)
	}

	// Extract host, pubsub, and orbitdb from your knowledgeBaseDB
	// --- Set HostID (needed for event payloads & durable client id fallback) ---
	if knowledgeBaseDB.Node != nil && knowledgeBaseDB.Node.PeerHost != nil {
		knowledgeBaseDB.HostID = knowledgeBaseDB.Node.PeerHost.ID().String()
	}

	// --- Start EMS subscriber (ActiveMQ/STOMP) ---
	// Derive a context from your existing termination context so it cancels cleanly.
	// --- Start EMS subscriber (ActiveMQ/STOMP) ---
	emsCtx, emsCancel := context.WithCancel(termCtx)
	cleanupEMS, err := knowledgeBaseDB.StartEMSSubscriber(emsCtx)
	if err != nil {
		// This should only happen on config/init errors
		log.Printf("[ERROR] EMS init failed: %v", err)
		if app.GlobalLoggerDB != nil {
			_ = app.GlobalLoggerDB.AddToOptimusLog("ERROR", "EMS init failed: "+err.Error(), runtime.GOOS)
		}
	} else {
		// Arrange cleanup on shutdown
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

	// channels to communicate requests from all apis to the service routine
	// for processing
	// TODO : should be able to hold configurable many requests as buffer
	// TODO : right now only one api can work and it cannot process requests in
	//		  parallel
	reqChan := make(chan app.Request, 100)
	resChan := make(chan interface{}, 100)

	go func() {
		for {
			l := <-logChan
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
					app.GlobalLoggerDB.AddToOptimusLog("WARN", fmt.Sprintf("Unexpected info format: %+v", l.Data), runtime.GOOS)
				}
			case app.Print:
				log.Print(l.Data)
				app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Main Data: %+v", l.Data), runtime.GOOS)
			default:
				log.Printf("[WARN] Unknown log type: %+v\n", l)
				app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Unknown log type: %+v", l), runtime.GOOS)
			}
		}
	}()

	// start the shell interface
	if *config.FlagShell {
		go api.Shell(reqChan, resChan, logChan)
	}

	// start the http interface
	if *config.FlagHTTP {
		go api.ServeHTTP(&knowledgeBaseDB, app.GlobalLoggerDB, reqChan, resChan, logChan)
	}

	// Initialize LibP2P host

	// Close the SQLite Database when exiting
	defer rdbms.Close()

	hostDis, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip4/0.0.0.0/udp/0/quic"),
	)
	//It’s registering a callback, not blocking or polling.
	//LibP2P itself handles incoming streams in the background.
	//When a peer opens a stream to this node, LibP2P automatically calls the handler in a separate goroutine.
	// Register SQL stream handler
	go app.AwaitRegisterSQLDMLStreamHandler(hostDis, logChan)

	// handle the dbs lifecycle after start and internal interface for
	// shell/api requests
	go app.Service(&knowledgeBaseDB, reqChan, resChan, hostDis, logChan, &rdbms)

	////////////////////////////////////////////////////////////
	/// ELECTION
	// Extract initialized components
	host := knowledgeBaseDB.Node.PeerHost
	// Create pubsub
	pubsub, err := pubsub.NewGossipSub(termCtx, host)
	if err != nil {
		log.Fatalf("Failed to initialize PubSub: %v", err)
		app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Failed to initialize PubSub on ELECTION process: %v"), runtime.GOOS)
	}
	//
	//
	// Start Election Controller
	go election.RunFullNode(termCtx, host, pubsub, &knowledgeBaseDB)

	////////////////////////////////////////////////////////////

	// idenitify autodiscovery
	if *config.FlagAutodiscovery {

		// Create a LibP2P Host

		if err != nil {
			log.Println("[ERROR] Failed to create LibP2P host:", err)
			app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Failed to create LibP2P host: %v", err), runtime.GOOS)
			return
		}
		log.Println("Auto Discovery for Peers has been enabled")
		app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Auto Discovery for Peers has been enabled"), runtime.GOOS)
		app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Libp2p Node Started. ID: %v", hostDis.ID()), runtime.GOOS)
		log.Println("Libp2p Node Started. ID:", hostDis.ID())
		prMsg := ""
		if *config.FlagAutodiscoveryMDNS {
			prMsg = "Using MDNS for Auto-Discovery"
		} else if *config.FlagAutodiscoveryipfsPubSub {
			prMsg = "Using IPFS PubSub for Auto-Discovery"
		} else if *config.FlagAutodiscoveryDHT {
			prMsg = "Using DHT for Auto-Discovery"
		} else {
			prMsg = "No Auto-Discovery method selected"
		}

		log.Println(prMsg)
		app.GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf(prMsg), runtime.GOOS)

		/*** Workding......
		service := api.StartMdnsDiscovery(hostDis, "my-mdns-service") // Start mDNS discovery
		api.WaitForExit(service) // Handle graceful exit
		*/
		// Start the new enhanced discovery mechanism
		service := api.StartDiscovery(hostDis, &knowledgeBaseDB)

		// Periodically print discovered peers
		go api.PrintDiscoveredPeers(&knowledgeBaseDB)
		if service == nil {
			fmt.Println("[ERROR] Discovery service failed to start")
			app.GlobalLoggerDB.AddToOptimusLog("ERROR", fmt.Sprintf("Discovery service failed to start"), runtime.GOOS)
			return
		}
		// Handle graceful exit for discovery
		//go api.WaitForExit(service)  -- Working but a global approach
		// Start handling shutdown signals
		handleShutdown(service, &knowledgeBaseDB, hostDis)
	}

	// await termination context
	<-termCtx.Done()
	fmt.Printf("Shutdown")

	// DEVNOTE : general graceful shutdown stuff may go here

	// write config and benchmark to persistent files

	config.SaveStructAsJSON(knowledgeBaseDB.Config, *config.FlagRepo+"_config")

	benchmarkPath := *config.FlagRepo + "_benchmark"
	config.SaveStructAsJSON(knowledgeBaseDB.Benchmark, benchmarkPath)

	// close orbitdb instance
	(*knowledgeBaseDB.Orbit).Close()

}

// /////////////////////////////////////////////////////////
func printSwarmchestrate() {
	fontsDir := "/usr/share/figlet/fonts"
	var font *figletlib.Font // Ensure font is defined before use

	if _, err := os.Stat(fontsDir); os.IsNotExist(err) {
		fmt.Println("Directory does not exist:", fontsDir)
		fontsDir = figletlib.GuessFontsDirectory() // Update the existing fontsDir variable

		font, err = figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			fmt.Println("Error loading font:", err)
			return
		}
	} else {
		fmt.Println("Directory exists:", fontsDir)

		font, err = figletlib.GetFontByName(fontsDir, "standard")
		if err != nil {
			fmt.Println("Error loading font:", err)
			return
		}
	}

	// Print ASCII art to the terminal
	figletlib.PrintMsg("Swarmchestrate", font, 80, font.Settings(), "")
	figletlib.PrintMsg("ICCS", font, 40, font.Settings(), "")

}

/*
*
 */
func handleShutdown(service *api.Service, knowledgeBaseDB *app.KnowledgeBaseDB, h host.Host) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Block execution until a termination signal is received
	log.Println("[INFO] Shutting down OptimusDB Node...")

	// Stop peer discovery services
	if service != nil {
		service.StopDiscovery()
		log.Println("[INFO] Peer Discovery stopped.")
	}

	// Close OrbitDB instance if available
	if knowledgeBaseDB != nil && knowledgeBaseDB.Orbit != nil {
		(*knowledgeBaseDB.Orbit).Close()
		log.Println("[INFO] OrbitDB instance closed.")
	}

	// Close the LibP2P host
	if err := h.Close(); err != nil {
		log.Println("[ERROR] Error while closing LibP2P host:", err)
	} else {
		log.Println("[INFO] LibP2P host shut down successfully.")
	}

	os.Exit(0)
}

func shutdownHandler(fn func()) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	go func() {
		for {
			s := <-ch
			switch s {
			case syscall.SIGHUP:
				fallthrough
			case syscall.SIGINT:
				fallthrough
			case syscall.SIGTERM:
				fallthrough
			case syscall.SIGQUIT:
				fn()
				os.Exit(0)
			}
		}
	}()
}

/**
func handleLogChannel(logChan chan app.Log, termCancel context.CancelFunc) {
	go func() {
		for {
			l := <-logChan
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
				}
			case app.Print:
				log.Print(l.Data)
			default:
				log.Printf("[WARN] Unknown log type: %+v\n", l)
			}
		}
	}()
}

*/
