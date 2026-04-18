package app

import (
	orbitdb "berty.tech/go-orbit-db"
	"berty.tech/go-orbit-db/accesscontroller"
	"berty.tech/go-orbit-db/iface"
	"berty.tech/go-orbit-db/stores/documentstore"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/kubo/core/coreapi"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/p2p/host/eventbus"
	"go.uber.org/zap"
	"log"
	"net"
	"net/http"
	"optimusdb/config"
	"optimusdb/ipfs"
	"optimusdb/logger"
	"os"
	"path/filepath"
	"time"
)

var orbit iface.OrbitDB

// InitPeer starts the ipfs node and creates the orbitdb structures on top of it
// DEVNOTE : optimusdb.EventLogDB may be nil after init ! that is if it's not root
// and has no transaction datastore locally. A datastore will be replicated on
// the first established peer connection
func InitPeer(knowledgeBaseDB *KnowledgeBaseDB, rdbms *KnowledgeBaseSQLite, bench *Benchmark, logChan chan Log) error {

	ctx := context.Background()
	conf, err := config.LoadConfig() // load persistent config

	if err != nil {
		logger.Error("Couldn't load config: %+v", err)
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("Couldn't load config : %+v", err)}
		return err
	}

	node, err := ipfs.SpawnEphemeral(ctx) // start ipfs node
	if err != nil {
		logger.Error("IPFS Node could not start: %+v", err)
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("IPFS Node could not started  : %+v", err)}
		return err
	}
	//conf.PeerID = node.Identity.String()
	conf.PeerID = fmt.Sprintf("%s", node.Identity)
	knowledgeBaseDB.Node = node
	//logger.Info("OptimusDB peer initialized with PeerID: %s", conf.PeerID)
	coreAPI, err := coreapi.NewCoreAPI(node)
	if err != nil {
		logger.Error("Instantiation of Package coreapi which provides direct access to the core commands  in IPFS resulted to a problem : %+v", err)
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("Instantiation of Package coreapi which provides direct access to the core commands  in IPFS resulted to a problem : %+v", err)}
		return err
	}

	var devLog *zap.Logger
	if *config.FlagDevLogs {
		devLog, err = zap.NewDevelopment()
		if err != nil {
			logger.Error("Development Flag resulted to a problem : %+v", err)
			//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("Development Flag resulted to a problem : %+v", err)}
			return err
		}
	}

	//cacheDir := filepath.Join(os.Getenv("HOME"), ".cache")
	//cache := filepath.Join(cacheDir, "optimusdb", *config.FlagRepo, "orbitdb")
	//cache := *config.FlagRepo
	//log.Println("Directory set: ", cache)
	// set cache dir
	//cacheDir := filepath.Join(os.Getenv("HOME"), ".cache")
	//cache := filepath.Join(cacheDir, "optimusdb", *config.FlagRepo, "orbitdb")
	cacheDir := filepath.Join(os.Getenv("HOME"), ".cache")
	cache := filepath.Join(cacheDir, "optimusdb", *config.FlagRepo, "orbitdb")
	//logger.Info("Cache directory set to: %s", cache)

	// set orbitdb create options
	orbitopts := &orbitdb.NewOrbitDBOptions{
		Logger:    devLog,
		Directory: &cache,
	}
	if conf.PeerID != "" {
		orbitopts.ID = &conf.PeerID
		logger.Info("PeerID: store and set Identity as non-string : %+v", orbitopts.ID)
		//logChan <- Log{Type: Info, Data: fmt.Sprintf("PeerID: store and set Identity as non-string : %+v", orbitopts.ID)}
	}

	//log.Println("####################################################################")
	//log.Println("Start OptimusDB instance")
	//logger.Info("Start OptimusDB instance")
	orbit, err = orbitdb.NewOrbitDB(ctx, coreAPI, orbitopts)

	if err != nil {
		logger.Error("New OptimusDB instance creation failed: %+v", err)
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("New OptimusDB instance is not created : %+v", err)}
		return err
	}
	knowledgeBaseDB.Orbit = &orbit

	///////////////////////////////////////////////////////////////////////////////////////
	/**
	Knowledge Base, immutable
	*/
	///////////////////////////////////////////////////////////////////////////////////////
	docstoreOpt := documentstore.DefaultStoreOptsForMap("path")
	cacheStore := filepath.Join(cache, *config.Flagcontributions)
	logger.Debug("[DEBUG] : OptimusDB instance eventlog")
	logger.Debug("[DEBUG] : eventlog cache %s ", cache)
	logger.Debug("[DEBUG] : eventlog contributions Cache %s ", cacheStore)
	// give write access to all
	///////////////////////////////////////////////////////////////////////////////////////
	/////////////////// contributions ////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////////
	onlyWriteAccess := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {
				"*",
			},
		},
	}

	/*
		type CreateDBOptions struct {
		    EventBus                event.Bus
		    Directory               *string
		    Overwrite               *bool
		    LocalOnly               *bool
		    Create                  *bool
		    StoreType               *string
		    AccessControllerAddress string
		    AccessController        accesscontroller.ManifestParams
		    Replicate               *bool
		    Keystore                keystore.Interface
		    Cache                   datastore.Datastore
		    Identity                *identityprovider.Identity
		    SortFn                  ipfslog.SortFn
		    IO                      ipfslog.IO
		    Timeout                 time.Duration
		    MessageMarshaler        MessageMarshaler
		    Logger                  *zap.Logger
		    CloseFunc               func()
		    StoreSpecificOpts       interface{}
		}
	*/

	dbopts := orbitdb.CreateDBOptions{
		Create:    boolPtr(true),
		StoreType: stringPtr("eventlog"),
		//StoreSpecificOpts: docstoreOpt,
		Directory:        &cacheStore,
		Overwrite:        boolPtr(false),
		AccessController: onlyWriteAccess,
		//Directory:        &cacheStore,
		Replicate: boolPtr(true),
		EventBus:  eventbus.NewBus(),
		Timeout:   setTimeOut(5),
	}

	// see if there is a persisted store available
	log.Println("Will check if a persistent store is available for Swarm(Contributions)")
	store, err := orbit.Open(ctx, conf.ContributionsStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR]: TSwarm(Contributions), is not avaialable, Try resolving it by connecting to a peer, ContributionsStoreAddr %v", err)
	} else {
		db := store.(iface.EventLogStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.Contributions = &db
		//fmt.Fprintf(os.Stdout, "Found database for Swarm: %s \n\n", db.Address().String())
		// persist store address
		conf.ContributionsStoreAddr = db.Address().String()
		logger.Info("[INFO] Found database for Swarm(Contributions): %s ", conf.ContributionsStoreAddr)
	}

	/////////////////// validations ////////////////////////////////////////////////////
	//######################################################
	//a creatable docsstore which no other peer can read or write to
	acVal := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {
				(*knowledgeBaseDB.Orbit).Identity().ID,
			},
			"read": {
				(*knowledgeBaseDB.Orbit).Identity().ID,
			},
		},
	}

	//storeType = "docstore"
	create := true
	replicate := false // no one else has write access
	docstoreOpt = documentstore.DefaultStoreOptsForMap("path")
	validationsCache := filepath.Join(cache, "validations")
	logger.Debug("[DEBUG] : OptimusDB instance validations")
	logger.Debug("[DEBUG] : validations cache %s ", cache)
	logger.Debug("[DEBUG] : docstore validationsCache %s ", validationsCache)
	dbopts = orbitdb.CreateDBOptions{
		Create:            &create,
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		AccessController:  acVal,
		Directory:         &validationsCache,
		Replicate:         &replicate,
		EventBus:          eventbus.NewBus(),
	}

	// see if there is a persisted store available
	store, err = orbit.Open(ctx, conf.ValidationsStoreAddr, &dbopts)
	if err != nil {
		logger.Error("Try resolving it by connecting to a peer, ValidationsStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.Validations = &db
		//fmt.Fprintf(os.Stdout, "Validation Store is available at : %s \n\n", db.Address().String())
		logger.Info("Found database for Swarm(Validations) : %s \n", db.Address().String())
		// persist store address
		conf.ValidationsStoreAddr = db.Address().String()
	}

	//log.Println("####################################################################")
	/////////////////// kbdata ////////////////////////////////////////////////////
	// ############## KB Store for Data Metrics
	//storeType = "docstore"
	create = true
	replicate = true // no one else has write access
	docstoreOpt = documentstore.DefaultStoreOptsForMap("path")
	KBdataCache := filepath.Join(cache, "kbdata")
	logger.Debug("[DEBUG] : OptimusDB instance kbdata")
	logger.Debug("[DEBUG] : kbdata cache %s ", cache)
	logger.Debug("[DEBUG] : docstore KBdataCache %s ", KBdataCache)
	dbopts = orbitdb.CreateDBOptions{
		Create:            &create,
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		AccessController:  onlyWriteAccess,
		Directory:         &KBdataCache,
		Replicate:         &replicate,
		EventBus:          eventbus.NewBus(),
	}
	// see if there is a persisted store available
	store, err = orbit.Open(ctx, conf.KMDataStoreAddr, &dbopts)
	if err != nil {
		logger.Error("Try resolving it by connecting to a peer, KMDataStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.KBdata = &db
		//fmt.Fprintf(os.Stdout, "Knowledge Management store for Data  is available at : %s \n\n", db.Address().String())
		//log.Printf("Knowledge Management store for Data (KBdata): %s \n", db.Address().String())
		//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Knowledge Management store for Data (KBdata): %v", db.Address().String()), runtime.GOOS)
		logger.Info("Knowledge Management store for Data (KBdata): %v", db.Address().String())
		// persist store address
		conf.KMDataStoreAddr = db.Address().String()
	}

	//########################################################################
	/////////////////// kbmetadata ////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	/// For Metadata
	fullRWMetadata := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {
				"*",
			},
			"read": {
				"*",
			},
		},
	}
	docstoreOpt = documentstore.DefaultStoreOptsForMap("_id") ////Here the fiasko
	KBMetadataCache := filepath.Join(cache, "kbmetadata")
	logger.Debug("[DEBUG] : OptimusDB instance kbmetadata")
	logger.Debug("[DEBUG] : kbmetadata cache %s ", cache)
	logger.Debug("[DEBUG] : docstore KBMetadataCache %s ", KBMetadataCache)
	/*
		dbopts = orbitdb.CreateDBOptions{
			Create:            &create,
			StoreType:         &storeType,
			StoreSpecificOpts: docstoreOpt,
			AccessController:  onlyWriteAccess,
			Directory:         &KBMetadataCache,
			Replicate:         &replicate,
			EventBus:          eventbus.NewBus(),
		}
	*/
	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRWMetadata,
		Directory:         &KBMetadataCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}
	store, err = orbit.Open(ctx, conf.KMMetaDataStoreAddr, &dbopts)
	if err != nil {
		logger.Error("Try resolving it by connecting to a peer, KMMetaDataStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.KBMetadata = &db
		//log.Printf("Knowledge Management store for Data (KBMetadata): %s \n", db.Address().String())
		//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Knowledge Management store for MetaData (KBMetadata): %v", db.Address().String()), runtime.GOOS)
		logger.Info("Knowledge Management store for MetaData (KBMetadata): %v", db.Address().String())
		conf.KMMetaDataStoreAddr = db.Address().String()
	}
	//########################################################################
	//########################################################################
	//########################################################################
	//########################################################################
	//########################################################################
	//########################################################################
	//########################################################################
	//########################################################################

	/////////////////// DsSWres ////////////////////////////////////////////////////
	//########################################################################
	fullRW := &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {
				"*",
			},
			"read": {
				"*",
			},
		},
	}
	docstoreOpt = documentstore.DefaultStoreOptsForMap("_id")

	//docstoreOpt = &documentstore.StoreOpts{
	//	IndexBy:    "_id",
	//	MaxEntries: 10000, // Explicitly set instead of using defaults
	//	CacheSize:  50000, // Increase cache size
	//
	//}

	cacheStore = filepath.Join(cache, *config.Flagdsswres)

	logger.Debug("[DEBUG] : OptimusDB instance " + *config.Flagdsswres)
	logger.Debug("[DEBUG] : cache %s ", cache)
	logger.Debug("[DEBUG] : docstore  %s ", cacheStore)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &cacheStore,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}
	store, err = orbit.Open(ctx, conf.DsswresStoreAddr, &dbopts)
	if err != nil {
		///fmt.Fprintf(os.Stderr, "%v\nTry resolving it by connecting to a peer, DsswresStoreAddr\n", err)
		logger.Error("[ERROR] Try resolving it by connecting to a peer, DsswresStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsSWres = &db
		//log.Printf("Knowledge Management store for Data (%s): %s \n", *config.Flagdsswres, db.Address().String())
		//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("Knowledge Management store for Data (%s): %s", *config.Flagdsswres, db.Address().String()), runtime.GOOS)
		logger.Info("[INFO] Knowledge Management store for Data (%s): %s", *config.Flagdsswres, db.Address().String())
		conf.DsswresStoreAddr = db.Address().String()
	}
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	///////////////// RDBMS area
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	rdbmsCache := filepath.Join(cache, *config.FlagRDBMSDB+".db")
	rdbms, err = InitSQLite(rdbmsCache) // Initialize SQLite
	if err != nil {
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("SQLite Initialization Failed: %+v", err)}
		logger.Error("[ERROR] SQLite Initialization Failed: %+v", err)
		return err
	}

	// Ensure `datacatalog` table exists

	if err := rdbms.createDataCatalog(); err != nil {
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("Table Creation Error:  createDataCatalog %+v", err)}
		logger.Error("[ERROR] Table Creation Error:  createDataCatalog %+v", err)
		return err
	}
	if err := rdbms.createTOSCAMetadataTable(); err != nil {
		logger.Error("[ERROR] Table Creation Error: createTOSCAMetadataTable %+v", err)
		//logChan <- Log{Type: NonRecoverableErr, Data: fmt.Sprintf("Table Creation Error: createTOSCAMetadataTable  %+v", err)}
		return err
	}
	//logChan <- Log{Type: Info, Data: "RDBMS database initialized successfully"}
	//GlobalLoggerDB.AddToOptimusLog("INFO", fmt.Sprintf("RDBMS database initialized successfully"), runtime.GOOS)
	logger.Info("[INFO] RDBMS database initialized successfully")

	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

	// ####################### TOSCA Imported Store ###########################
	fullRW = &accesscontroller.CreateAccessControllerOptions{
		Access: map[string][]string{
			"write": {"*"},
			"read":  {"*"},
		},
	}
	docstoreOpt = documentstore.DefaultStoreOptsForMap("_id")

	toscaCache := filepath.Join(cache, "tosca_imported")
	logger.Debug("[DEBUG] : OptimusDB instance tosca_imported")
	logger.Debug("[DEBUG] : cache %s ", cache)
	logger.Debug("[DEBUG] : docstore %s ", toscaCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &toscaCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.TOSCAImportedStoreAddr, &dbopts)
	if err != nil {
		//fmt.Fprintf(os.Stderr, "%v\nTry resolving it by connecting to a peer, TOSCAImportedStoreAddr\n", err)
		logger.Error("[ERROR] Try resolving it by connecting to a peer, TOSCAImportedStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsTOSCA_Imported = &db
		//GlobalLoggerDB.AddToOptimusLog("INFO",
		//	fmt.Sprintf("TOSCA Imported store: %s", db.Address().String()), runtime.GOOS)
		logger.Debug("[DEBUG] TOSCA Imported store: %s", db.Address().String())
		conf.TOSCAImportedStoreAddr = db.Address().String()
	}

	// ####################### dsswresaloc Store ###########################
	// Resource allocations (companion to dsswres).
	cacheStore = filepath.Join(cache, "dsswresaloc")
	logger.Debug("[DEBUG] : OptimusDB instance dsswresaloc")
	logger.Debug("[DEBUG] : docstore %s ", cacheStore)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &cacheStore,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.DsswresalocStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, DsswresalocStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsSWresaloc = &db
		logger.Debug("[DEBUG] dsswresaloc store: %s", db.Address().String())
		conf.DsswresalocStoreAddr = db.Address().String()
	}

	// ####################### whoiswho Store ###########################
	// Identity and role metadata for peers and services.
	whoiswhoCache := filepath.Join(cache, "whoiswho")
	logger.Debug("[DEBUG] : OptimusDB instance whoiswho")
	logger.Debug("[DEBUG] : docstore %s ", whoiswhoCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &whoiswhoCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.WhoiswhoStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, WhoiswhoStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.WhoiswhoStore = &db
		logger.Debug("[DEBUG] whoiswho store: %s", db.Address().String())
		conf.WhoiswhoStoreAddr = db.Address().String()
	}

	// ####################### TOSCA ADT Store ###########################
	// TOSCA application descriptor templates — topology, node templates, policies.
	toscaADTCache := filepath.Join(cache, "tosca_adt")
	logger.Debug("[DEBUG] : OptimusDB instance tosca_adt")
	logger.Debug("[DEBUG] : docstore %s ", toscaADTCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &toscaADTCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.TOSCAADTStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, TOSCAADTStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsTOSCA_ADT = &db
		logger.Debug("[DEBUG] TOSCA ADT store: %s", db.Address().String())
		conf.TOSCAADTStoreAddr = db.Address().String()
	}

	// ####################### TOSCA Capacities Store ###########################
	// Compute / memory / storage requirements — fields: num_cpus, mem_size, etc.
	toscaCapsCache := filepath.Join(cache, "tosca_capacities")
	logger.Debug("[DEBUG] : OptimusDB instance tosca_capacities")
	logger.Debug("[DEBUG] : docstore %s ", toscaCapsCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &toscaCapsCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.TOSCACapacitiesStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, TOSCACapacitiesStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsTOSCA_Capacities = &db
		logger.Debug("[DEBUG] TOSCA Capacities store: %s", db.Address().String())
		conf.TOSCACapacitiesStoreAddr = db.Address().String()
	}

	// ####################### TOSCA Deployment Plan Store ###########################
	// Scheduled deployment plans derived from TOSCA ADTs.
	toscaDplanCache := filepath.Join(cache, "tosca_deploymentplan")
	logger.Debug("[DEBUG] : OptimusDB instance tosca_deploymentplan")
	logger.Debug("[DEBUG] : docstore %s ", toscaDplanCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &toscaDplanCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.TOSCADeploymentPlanStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, TOSCADeploymentPlanStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsTOSCA_DeploymentPlan = &db
		logger.Debug("[DEBUG] TOSCA DeploymentPlan store: %s", db.Address().String())
		conf.TOSCADeploymentPlanStoreAddr = db.Address().String()
	}

	// ####################### TOSCA Event History Store ###########################
	// Runtime events and lifecycle transitions for TOSCA deployments.
	toscaEvtCache := filepath.Join(cache, "tosca_eventhistory")
	logger.Debug("[DEBUG] : OptimusDB instance tosca_eventhistory")
	logger.Debug("[DEBUG] : docstore %s ", toscaEvtCache)

	dbopts = orbitdb.CreateDBOptions{
		Create:            boolPtr(true),
		StoreType:         stringPtr("docstore"),
		StoreSpecificOpts: docstoreOpt,
		Overwrite:         boolPtr(false),
		AccessController:  fullRW,
		Directory:         &toscaEvtCache,
		Replicate:         boolPtr(true),
		EventBus:          eventbus.NewBus(),
		Timeout:           setTimeOut(5),
	}

	store, err = orbit.Open(ctx, conf.TOSCAEventHistoryStoreAddr, &dbopts)
	if err != nil {
		logger.Error("[ERROR] Try resolving it by connecting to a peer, TOSCAEventHistoryStoreAddr %v", err)
	} else {
		db := store.(iface.DocumentStore)
		db.Load(ctx, -1)
		knowledgeBaseDB.DsTOSCA_EventHistory = &db
		logger.Debug("[DEBUG] TOSCA EventHistory store: %s", db.Address().String())
		conf.TOSCAEventHistoryStoreAddr = db.Address().String()
	}

	//########################################################################
	knowledgeBaseDB.Config = conf
	if bench != nil {
		knowledgeBaseDB.Benchmark = bench
	} else {
		knowledgeBaseDB.Benchmark = &Benchmark{}
	}

	if *config.FlagSwarmName != "" {
		knowledgeBaseDB.Benchmark.Region = *config.FlagSwarmName
	}
	//########################################################################

	//########################################################################
	// NEW: Initialize the interceptor for automatic lineage tracking
	knowledgeBaseDB.Interceptor = NewPersistenceInterceptor(knowledgeBaseDB)
	logger.Debug("[DEBUG] Persistence interceptor initialized for automatic metadata extraction")
	//########################################################################

	// connect to a bootstrap peer
	if *config.FlagBootstrap != "" {
		logger.Debug("[DEBUG] bootstrap Connect to a peer, TOSCAImportedStoreAddr %v", *config.FlagBootstrap)
		IssueConnectCmd(knowledgeBaseDB, []string{*config.FlagBootstrap})
	}
	//##########################################################################3
	// Register the query protocol handler
	knowledgeBaseDB.Node.PeerHost.SetStreamHandler("/query/1.0.0", func(s network.Stream) {
		handleQueryRequest(s, knowledgeBaseDB)
	})

	return nil
}

// IssueConnectCmd connect to a peer given their IP, by sending an http request for the "CONNECT"
// cmd, with own connection string
func IssueConnectCmd(knowledgeBaseDB *KnowledgeBaseDB, peers []string) {
	client := &http.Client{}

	// for each given ip, send the connect request
	for _, p := range peers {
		// TODO : can we get the address from the coreapi ?
		myIP := GetOwnIP()
		if p == "127.0.0.1" {
			myIP = p
		}
		myAddr := "/ip4/" + myIP + "/tcp/" + *config.FlagIPFSPort + "/p2p/" + knowledgeBaseDB.Config.PeerID
		logger.Debug("[DEBUG] Sending my address : ", myAddr, " to IP ", p)
		cmdPath := "http://" + p + ":" + *config.FlagHTTPPort + "/" + *config.FlagContext + "/command"

		///// incase there is a change in the http payload
		//connectReq := Request{CONNECT, []string{myAddr}, "", "", nil, nil, nil}
		connectReq := Request{
			Method:          CONNECT,
			Args:            []string{myAddr},
			DSType:          "",
			SQLDML:          "",
			UpdateData:      nil,
			Options:         nil,
			Graph_traversal: nil,
			Criteria:        nil,
		}

		jsonData, err := json.Marshal(connectReq)
		if err != nil {
			logger.Error("[ERROR] Error marshaling JSON:", err)
			return
		}

		req, err := http.NewRequest("POST", cmdPath, bytes.NewBuffer(jsonData))
		if err != nil {
			logger.Error("[ERROR] Error creating request:", err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			logger.Error("[ERROR] Error sending request:", err)
			continue
		}
		defer resp.Body.Close()
	}
}

func GetOwnIP() string {
	// get the list of network interfaces
	interfaces, err := net.Interfaces()
	if err != nil {
		logger.Error("[ERROR] Failed to get network interfaces:", err)
		return ""
	}

	// iterate through each network interface
	for _, iface := range interfaces {
		// check if the interface is up and not a loopback
		if iface.Flags&net.FlagUp != 0 && iface.Flags&net.FlagLoopback == 0 {
			// get the addresses for the current interface
			addrs, err := iface.Addrs()
			if err != nil {
				logger.Error("[ERROR] Failed to get addresses for interface", iface.Name, ":", err)
				//fmt.Println("Failed to get addresses for interface", iface.Name, ":", err)
				continue
			}

			// iterate through each address
			for _, addr := range addrs {
				// check if the address is an IP address
				ipNet, ok := addr.(*net.IPNet)
				if ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
					// print the IP address
					return ipNet.IP.String()
				}
			}
		}
	}

	return ""
}

// Helper function to get the local IP address
func GetPublicIPAddress() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "" //, err
	}

	for _, addr := range addrs {
		// Check for a valid IPv4 address that's not loopback
		if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			return ipNet.IP.String() //, nil
		}
	}

	return "" //, fmt.Errorf("no valid IPv4 address found")
}

/*
	type CreateDBOptions struct {
		EventBus                event.Bus
		Directory               *string
		Overwrite               *bool
		LocalOnly               *bool
		Create                  *bool
		StoreType               *string
		AccessControllerAddress string
		AccessController        accesscontroller.ManifestParams
		Replicate               *bool
		Keystore                keystore.Interface
		Cache                   datastore.Datastore
		Identity                *identityprovider.Identity
		SortFn                  ipfslog.SortFn
		IO                      ipfslog.IO
		Timeout                 time.Duration
		MessageMarshaler        MessageMarshaler
		Logger                  *zap.Logger
		CloseFunc               func()
		StoreSpecificOpts       interface{}
	}
*/

func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

// Function to convert seconds into time.Duration
func setTimeOut(seconds int) time.Duration {
	return time.Duration(seconds) * time.Second
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
