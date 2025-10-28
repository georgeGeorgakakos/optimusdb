package ipfs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	optimusdbConf "optimusdb/config"

	config "github.com/ipfs/kubo/config"
	"github.com/ipfs/kubo/core"
	kubolibp2p "github.com/ipfs/kubo/core/node/libp2p"
	"github.com/ipfs/kubo/plugin/loader" // This package is needed so that all the preloaded plugins are loaded automatically
	"github.com/ipfs/kubo/repo/fsrepo"
)

// Setup ipfs plugins
func setupPlugins(externalPluginsPath string) error {
	// Load any external plugins if available on externalPluginsPath
	plugins, err := loader.NewPluginLoader(filepath.Join(externalPluginsPath, "plugins"))
	if err != nil {
		return fmt.Errorf("error loading plugins: %s", err)
	}

	// Load preloaded and external plugins
	if err := plugins.Initialize(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	if err := plugins.Inject(); err != nil {
		return fmt.Errorf("error initializing plugins: %s", err)
	}

	return nil
}

// exists returns whether the given file or directory exists
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Creates a temporary ipfs repo, from the docs :
// "ipfs stores all its settings and internal data in a directory called the repository"
// removal has to be taken care of by caller
func createDatabase(temporary bool) (string, error) {
	repoPath := "./" + *optimusdbConf.FlagRepo
	//repoPath := *optimusdbConf.FlagRepo

	var err error
	if temporary {
		repoPath, err = os.MkdirTemp("", *optimusdbConf.FlagRepo)
	} else if exists, _ := exists(repoPath); !exists {
		var perm = 0777 // full permissions
		err = os.Mkdir(*optimusdbConf.FlagRepo, os.FileMode(perm))
	}

	if err != nil {
		return "", fmt.Errorf("failed to get dir: %s", err)
	}

	// Create a config with default options and a 2048 bit key
	cfg, err := config.Init(io.Discard, 2048)
	if err != nil {
		return "", err
	}

	// When creating the repository, you can define custom settings on the repository, such as enabling experimental
	// features (See experimental-features.md) or customizing the gateway endpoint.
	// To do such things, you should modify the variable `cfg`. For example:
	// TODO : check these

	pubsubCfg := config.PubsubConfig{
		Enabled: config.Flag(1),
		Router:  "gossipsub",
	}

	// See also: https://github.com/ipfs/kubo/blob/master/docs/config.md
	// And: https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md
	// enable pubsub
	cfg.Pubsub = pubsubCfg

	// Start without peers
	// TODO : should private/public network be controlled by config ?
	cfg.Bootstrap = []string{}

	// There seems to be a problem with pubsub under MDNS ,which leads to
	// nodes on the same system not being able to communicate  :
	// https://github.com/ipfs/kubo/issues/7757

	cfg.Discovery.MDNS.Enabled = false

	// identify autodiscovery
	if *optimusdbConf.FlagAutodiscovery {
		cfg.Discovery.MDNS.Enabled = true
	}

	// TODO : configurable host id
	// cfg.Identity =

	// experimental features
	if *optimusdbConf.FlagExp {
		// https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#ipfs-filestore
		cfg.Experimental.FilestoreEnabled = true
		// https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#ipfs-urlstore
		cfg.Experimental.UrlstoreEnabled = true
		// https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#ipfs-p2p
		cfg.Experimental.Libp2pStreamMounting = true
		// https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md#p2p-http-proxy
		cfg.Experimental.P2pHttpProxy = true
		// See also: https://github.com/ipfs/kubo/blob/master/docs/config.md
		// And: https://github.com/ipfs/kubo/blob/master/docs/experimental-features.md
	}

	// Configure swarm addresses/where to listen
	// TODO : make ip configurable
	cfg.Addresses.Swarm = []string{
		"/ip4/0.0.0.0/tcp/" + *optimusdbConf.FlagIPFSPort,
		"/ip6/::1/tcp/" + *optimusdbConf.FlagIPFSPort,
	}

	// Create the repo with the config
	err = fsrepo.Init(repoPath, cfg)
	if err != nil {
		return "", fmt.Errorf("failed to init ephemeral node: %+v", err)
	}
	log.Printf("Path of the IPFS repository on the Swarm Agent (LSA/SA): %s\n", repoPath)

	return repoPath, nil
}

// Creates an IPFS node and returns its coreAPI
func createAgentIpfs(ctx context.Context, repoPath string) (*core.IpfsNode, error) {
	// Open the repo
	repo, err := fsrepo.Open(repoPath)
	if err != nil {
		return nil, err
	}

	// Construct the node
	nodeOptions := &core.BuildCfg{
		Online:  true,
		Routing: kubolibp2p.DHTOption, // This option sets the node to be a full DHT node (both fetching and storing DHT Records)
		// Routing: libp2p.DHTClientOption, // This option sets the node to be a client DHT node (only fetching records)
		Repo: repo,
		ExtraOpts: map[string]bool{
			"pubsub": true, // must for orbitdb
		},
		Permanent: true, // improve performance for long runs TODO : make this configurable for benchmarking
	}

	return core.NewNode(ctx, nodeOptions)
}

// SpawnEphemeral /*
func SpawnEphemeral(ctx context.Context) (*core.IpfsNode, error) {

	// TODO : why does this have to be run as sync once ?
	var err error
	loadPluginsOnce.Do(func() {
		err = setupPlugins("")
	})
	if err != nil {
		return nil, err
	}

	// Create a Temporary IPFS Repo
	repoPath, err := createDatabase(false)
	if err != nil {
		return nil, err
	}

	// Create actual ipfs node based on temporary repo
	node, err := createAgentIpfs(ctx, repoPath)
	if err != nil {
		err := os.RemoveAll(repoPath)
		if err != nil {
			return nil, err
		}
		return nil, err
	}
	log.Printf("node ident string :%s\n", node.Identity.String())
	log.Printf("IPFS swarm communication (peer-to-peer connections)\n")
	for _, addr := range node.DHT.LAN.Host().Addrs() {
		log.Printf("Listening on %s\n", addr)
	}

	return node, nil
}
