package ipfs

import (
	"context"
	"optimusdb/logger"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"berty.tech/go-orbit-db/iface"
	files "github.com/ipfs/go-ipfs-files"
	coreiface "github.com/ipfs/interface-go-ipfs-core"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

// GetIPFSNode Given a path get the corresponding node where node is a common interface for
// files, directories and other special files
func GetIPFSNode(path string) (files.Node, error) {
	fileStat, err := os.Stat(path)
	if err != nil {
		logger.Error("[ERROR] On GetIPFSNode : %+v", err)
		return nil, err
	}

	node, err := files.NewSerialFile(path, true, fileStat)
	if err != nil {
		logger.Error("[ERROR] NewSerialFile : %+v", err)
		return nil, err
	}

	return node, nil
}

var loadPluginsOnce sync.Once

// ConnectToPeers try to connect to the given peers
func ConnectToPeers(ctx context.Context, orbitdb *iface.OrbitDB, peers []string) error {
	var wg sync.WaitGroup

	api := (*orbitdb).IPFS()

	// extract and map ids to addresses
	peerInfos := make(map[peer.ID]*peer.AddrInfo, len(peers))
	for _, addrStr := range peers {
		addr, err := ma.NewMultiaddr(addrStr)
		if err != nil {
			logger.Error("[ERROR] ConnectToPeers/NewMultiaddr : %+v", err)
			return err
		}
		pii, err := peer.AddrInfoFromP2pAddr(addr)
		if err != nil {
			logger.Error("[ERROR] ConnectToPeers/AddrInfoFromP2pAddr : %+v", err)
			return err
		}
		pi, ok := peerInfos[pii.ID]
		if !ok {
			pi = &peer.AddrInfo{ID: pii.ID}
			peerInfos[pi.ID] = pi
		}
		pi.Addrs = append(pi.Addrs, pii.Addrs...)
	}

	// remember them, so a connection lost during a network partition is dialled again after it heals
	rememberPeers(api, peerInfos)

	// try to connect to peers
	wg.Add(len(peerInfos))
	for _, peerInfo := range peerInfos {
		go func(peerInfo *peer.AddrInfo) {
			defer wg.Done()
			err := api.Swarm().Connect(ctx, *peerInfo)
			if err != nil {
				logger.Error("[ERROR] When trying to ConnectToPeers/peerInfos : %+v", err)
				//fmt.Fprintf(os.Stderr, "ERROR: ConnectToPeers/peerInfos : %+v\n", err)
			}
		}(peerInfo)
	}
	wg.Wait()
	return nil
}

// Peers this agent was asked to connect to are kept connected. libp2p does not dial a lost connection again,
// and the node has no bootstrap list, so after a network partition long enough to drop connections the two
// sides could stay apart until someone reconnected them by hand. Every OPTIMUSDB_RECONNECT_INTERVAL seconds
// (default 10; 0 turns it off) each remembered peer that is not connected is dialled again; a reconnection
// makes the stores exchange heads (pubsub join), so replicas converge after the partition heals.
var (
	knownMu    sync.Mutex
	knownPeers = map[peer.ID]peer.AddrInfo{}
	keeperOnce sync.Once
)

func rememberPeers(api coreiface.CoreAPI, infos map[peer.ID]*peer.AddrInfo) {
	knownMu.Lock()
	for id, pi := range infos {
		knownPeers[id] = *pi
	}
	knownMu.Unlock()
	sec := 10
	if v, err := strconv.Atoi(strings.TrimSpace(os.Getenv("OPTIMUSDB_RECONNECT_INTERVAL"))); err == nil && v >= 0 {
		sec = v
	}
	if sec == 0 {
		return
	}
	keeperOnce.Do(func() {
		go func() {
			t := time.NewTicker(time.Duration(sec) * time.Second)
			defer t.Stop()
			for range t.C {
				connected := map[peer.ID]bool{}
				cctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				conns, err := api.Swarm().Peers(cctx)
				cancel()
				if err != nil {
					continue
				}
				for _, c := range conns {
					connected[c.ID()] = true
				}
				knownMu.Lock()
				todo := make([]peer.AddrInfo, 0, len(knownPeers))
				for id, pi := range knownPeers {
					if !connected[id] {
						todo = append(todo, pi)
					}
				}
				knownMu.Unlock()
				for _, pi := range todo {
					go func(pi peer.AddrInfo) {
						dctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := api.Swarm().Connect(dctx, pi); err == nil {
							logger.Info("reconnected to peer %s", pi.ID.String())
						}
					}(pi)
				}
			}
		}()
	})
}
