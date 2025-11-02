package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host" // Import the correct Host interface
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	mdns "github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"log"
	"optimusdb/app"
	"optimusdb/config"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Service represents the discovery service

type Service struct {
	host   host.Host
	mdns   mdns.Service
	dht    *dht.IpfsDHT
	Pubsub *pubsub.PubSub       // ← Capitalized (exported)
	Topic  *pubsub.Topic        // ← Capitalized (exported)
	Sub    *pubsub.Subscription // ← Capitalized (exported)
}

var peerList = struct {
	sync.Mutex
	peers map[peer.ID]peer.AddrInfo
}{peers: make(map[peer.ID]peer.AddrInfo)}

// DiscoveryNotifee implements the peer discovery handler for mDNS
type DiscoveryNotifee struct {
	host host.Host
	db   *app.KnowledgeBaseDB
}

// isOwnAddress checks if an address belongs to the host
func isOwnAddress(h host.Host, addr string) bool {
	for _, myAddr := range h.Addrs() {
		if strings.Contains(myAddr.String(), addr) {
			return true
		}
	}
	return false
}

/* Woeking release as of 08.02.2025
// HandlePeerFound is triggered when a new peer is discovered via mDNS
func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Println("[mDNS] Found peer:", pi.ID)

	// Add discovered peer to peerstore
	n.host.Peerstore().AddAddr(pi.ID, pi.Addrs[0], peerstore.PermanentAddrTTL)

	// Add peer to global list
	peerList.Lock()
	peerList.peers[pi.ID] = pi
	peerList.Unlock()

	// Attempt connection
	err := n.host.Connect(context.Background(), pi)
	if err != nil {
		log.Println("[mDNS] Failed to connect to discovered peer:", err)
	} else {
		log.Println("[mDNS] Successfully connected to peer:", pi.ID)
	}
}
*/

// PrintDiscoveredPeers periodically prints the list of discovered peers
// Writes discovered peer information to contributions
// Periodically writes new peers to the contributions database.
// Each peer discovery event is added as a JSON entry.
// awaitWriteEvent (service.go) - Listens for write events.
// awaitStoreExchange (service.go) - Replicates contributions DB from peers.
// PrintDiscoveredPeers (discovery.go) - Stores discovered peers in contributions.
// getContri (service.go) - Handles CONTRI command to retrieve contributions.
func PrintDiscoveredPeers(optimusdb *app.KnowledgeBaseDB) {
	ctx := context.Background()
	dbContri := optimusdb.Contributions
	//coreAPI := (*optimusdb.Orbit).IPFS()
	//filePath, err := coreAPI.Unixfs().Add(ctx, node)

	ticker := time.NewTicker(100 * time.Second) // Adjust interval as needed // Runs every 5 minutes
	defer ticker.Stop()

	for range ticker.C {
		peerList.Lock() // Prevent concurrent modification
		if len(peerList.peers) == 0 {
			log.Println("[INFO] No peers discovered yet.")
			app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("No peers discovered yet"), runtime.GOOS)
			//logger.Info("No peers discovered yet.")
		} else {
			log.Println("[INFO] Discovered Peers:")
			log.Printf("[INFO] Discovered Peers will be added to Contributions Store: %v\n", (*dbContri).Address().String())
			app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Discovered Peers will be added to Contributions Store: %v", (*dbContri).Address().String()), runtime.GOOS)

			for id, info := range peerList.peers {
				//logger.Info(" - Peer ID: %s | Addresses: %v\n", id, info.Addrs)
				// **Ensure the peer has valid addresses before proceeding**
				if len(info.Addrs) == 0 {
					log.Printf("[WARN] Skipping peer %s due to empty address list\n", id)
					app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Warning: Skipping peer %s due to empty address list %v", id), runtime.GOOS)
					continue
				}

				// Add the event to the contributions store
				optimusdb.ContributionsMtx.Lock()
				err := (*dbContri).Load(ctx, -1)
				if err != nil {
					log.Printf("[ERROR] Failed to load contributions DB: %v\n", err)
					return
				}
				data := app.Contribution{app.GetAgentName(), optimusdb.Config.PeerID, id.String(), time.Now(), app.GetOwnIP(), app.GetPublicIPAddress(), extractIPs(info.Addrs)}
				dataJSON, err := json.Marshal(data)
				_, err = (*dbContri).Add(ctx, dataJSON)
				log.Printf(" - Peer ID: %s | Addresses: %v\n", id, info.Addrs)
				app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Peer ID: %s | Addresses: %v", id, info.Addrs), runtime.GOOS)
				optimusdb.ContributionsMtx.Unlock()

				if err != nil {
					log.Printf("[ERROR] Failed to store peer information: %v\n", err)
					continue
				}

			}
		}
		peerList.Unlock()
	}
}
func extractIPs(addrs []multiaddr.Multiaddr) []string {
	var ips []string
	for _, addr := range addrs {
		ip, err := extractIPFrom(addr)
		if err != nil {
			log.Printf("[WARN] Failed to extract IP from %s: %v\n", addr, err)
			continue
		}
		ips = append(ips, ip)
	}
	return ips
}

func extractIPFrom(addr multiaddr.Multiaddr) (string, error) {
	components := strings.Split(addr.String(), "/")
	for i, component := range components {
		if component == "ip4" || component == "ip6" {
			if i+1 < len(components) {
				return components[i+1], nil
			}
		}
	}
	return "", fmt.Errorf("no IP component found in multiaddr: %s", addr.String())
}

// Converts Multiaddr array to string array
func convertMultiaddrsToString(addrs []multiaddr.Multiaddr) []string {
	var strAddrs []string
	for _, addr := range addrs {
		strAddrs = append(strAddrs, addr.String())
	}
	return strAddrs
}

// HandlePeerFound is triggered when a new peer is discovered via any method
func (n *DiscoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	log.Println("[DISCOVERY] Found peer:", pi.ID)
	app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Found peer: %v", pi.ID), runtime.GOOS)

	// Add to peer tracker for HTTP API
	TrackPeer(pi)

	// Register into discovered peer DB
	n.db.AddDiscoveredPeer(pi.ID.String())

	// Add peer to peerstore - Accesses the peerstore of the libp2p host.
	n.host.Peerstore().AddAddr(pi.ID, pi.Addrs[0], peerstore.PermanentAddrTTL)

	// Add peer to global list
	peerList.Lock()
	peerList.peers[pi.ID] = pi
	peerList.Unlock()

	// Attempt connection
	err := n.host.Connect(context.Background(), pi)
	if err != nil {
		log.Println("[DISCOVERY] Failed to connect to discovered peer:", err)
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Failed to connect to discovered peer: %v", err), runtime.GOOS)
	} else {
		log.Println("[DISCOVERY] Connected to peer:", pi.ID)
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Connected to peer: %v", pi.ID), runtime.GOOS)
	}
}

// extractIPFromMultiaddr extracts the IP part from a multiaddr.Multiaddr
func extractIP(addr multiaddr.Multiaddr) string {
	// Split the multiaddr into its components
	components := strings.Split(addr.String(), "/")

	// Iterate over the components to find the IP part
	for i, component := range components {
		if component == "ip4" || component == "ip6" {
			// The next component is the IP address
			if i+1 < len(components) {
				return components[i+1]
			}
		}
	}

	return "no IP component found in multiaddr: " + addr.String()
}

// StartMdnsDiscovery initializes mDNS-based peer discovery
func StartMdnsDiscovery(h host.Host, mdnsServiceName string) *Service {
	// Define the discovery handler
	peerHandler := &DiscoveryNotifee{host: h}

	// Start mDNS service
	mdnsService := mdns.NewMdnsService(h, mdnsServiceName, peerHandler)
	//mdnsService.Start()
	if mdnsService == nil {
		log.Println("[ERROR] Failed to start mDNS: Service is nil", mdnsService)
		return nil
	}
	log.Println("[INFO] mDNS discovery started successfully:", mdnsService)
	app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("mDNS discovery started successfully: %v", mdnsService), runtime.GOOS)
	err := mdnsService.Start()
	if err != nil {
		fmt.Println("[ERROR] Failed to start mDNS:", err)
		return nil
	}

	log.Println("[INFO] mDNS discovery started successfully")
	app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("mDNS discovery started successfully"), runtime.GOOS)
	return &Service{
		host: h,
		mdns: mdnsService,
	}
}

// stopMdnsDiscovery stops the mDNS service
func (s *Service) stopMdnsDiscovery() {
	if s.mdns != nil {
		s.mdns.Close()
		fmt.Println("[INFO] mDNS discovery stopped")
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("mDNS discovery stopped"), runtime.GOOS)
	}
}

// WaitForExit listens for termination signals and cleans up
func WaitForExit(service *Service) {
	// Listen for OS termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan // Block until a signal is received

	fmt.Println("[INFO] Received termination signal. Cleaning up...")
	service.stopMdnsDiscovery()
	os.Exit(0)
}

// listenPubSub listens for new peer announcements
func (s *Service) listenPubSub(handler *DiscoveryNotifee) {
	for {
		msg, err := s.Sub.Next(context.Background())
		if err == nil {
			peerID, err := peer.Decode(string(msg.Data))
			if err == nil {
				handler.HandlePeerFound(peer.AddrInfo{ID: peerID})
			}
		}
	}
}

// StopDiscovery cleans up discovery services
func (s *Service) StopDiscovery() {

	///mDNS service
	if s.mdns != nil {
		s.mdns.Close()
		log.Println("[INFO] mDNS discovery stopped.")
	}

	///pubsub service
	if s.Pubsub != nil && s.Topic != nil {
		s.Topic.Close()
		log.Println("[INFO] PubSub discovery stopped.")
	}

	///DHT service
	if s.dht != nil {
		log.Println("[INFO] Stopping DHT discovery.")
		err := s.dht.Close()
		if err != nil {
			log.Println("[ERROR] Failed to stop DHT:", err)
		} else {
			log.Println("[INFO] DHT discovery stopped.")
		}
	}
	log.Println("[INFO] All discovery services stopped.")
}

// StartDiscovery initializes all enabled discovery mechanisms
// func StartDiscovery(h host.Host) *Service {
func StartDiscovery(h host.Host, knowledgeBaseDB *app.KnowledgeBaseDB) *Service {
	log.Println("[INFO] Starting enhanced peer discovery")
	app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Starting enhanced peer discovery"), runtime.GOOS)
	peerHandler := &DiscoveryNotifee{
		host: h,
		db:   knowledgeBaseDB,
	}

	service := &Service{host: h}

	// Start mDNS if enabled
	if *config.FlagAutodiscoveryMDNS {
		log.Println("[INFO] Enabling mDNS discovery, Service : optimusdb-mdns")
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Enabling mDNS discovery, Service : optimusdb-mdns"), runtime.GOOS)
		mdnsService := mdns.NewMdnsService(h, "optimusdb-mdns", peerHandler)
		if err := mdnsService.Start(); err != nil {
			log.Println("[ERROR] Failed to start mDNS:", err)
		} else {
			service.mdns = mdnsService
		}
	}

	if *config.FlagAutodiscoveryDHT {
		log.Println("[INFO] Enabling DHT discovery")
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Enabling DHT discovery"), runtime.GOOS)
		// Initialize Kademlia DHT (full routing mode)
		kademliaDHT, err := dht.New(context.Background(), h, dht.Mode(dht.ModeServer))
		if err != nil {
			log.Println("[ERROR] Failed to initialize DHT:", err)
		} else {
			service.dht = kademliaDHT
			log.Println("[INFO] DHT routing discovery initialized")
			app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("DHT routing discovery initialized"), runtime.GOOS)
			// Start advertising our presence
			go func() {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					mh, err := multihash.Sum([]byte(service.host.ID().String()), multihash.SHA2_256, -1)
					key := cid.NewCidV1(cid.Raw, mh)
					if err != nil {
						log.Println("[ERROR] Failed to generate CID for DHT Provide:", err)
					} else {
						err = service.dht.Provide(ctx, key, true)
					}
					cancel()
					if err != nil {
						log.Println("[ERROR] DHT Advertise failed:", err)
					} else {
						log.Println("[INFO] Successfully advertised on DHT")
					}
					time.Sleep(30 * time.Second)
				}
			}()

			// Start finding peers periodically
			go func() {
				for {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					peerInfo, err := service.dht.FindPeer(ctx, peer.ID("optimusdb-dht"))
					app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("DHT routing discovery with topic: optimusdb-dht"), runtime.GOOS)
					cancel()

					if err != nil {
						log.Println("[ERROR] DHT FindPeers failed:", err)
						time.Sleep(30 * time.Second)
						continue
					}

					if peerInfo.ID != "" {
						peerHandler.HandlePeerFound(peerInfo)
					}
					time.Sleep(30 * time.Second)
				}
			}()
		}
	}
	// Start PubSub discovery if enabled
	if *config.FlagAutodiscoveryipfsPubSub {
		log.Println("[INFO] Enabling PubSub-based discovery")
		app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", "Enabling PubSub-based discovery", runtime.GOOS)

		ps, err := pubsub.NewGossipSub(context.Background(), h,
			pubsub.WithPeerExchange(true),
		)

		if err != nil {
			log.Println("[ERROR] Failed to initialize PubSub:", err)
			app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Failed to initialize PubSub: %v", err), runtime.GOOS)
		} else {
			log.Println("[DISCOVERY] GossipSub created with default mesh (crash-safe)")
			app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", "GossipSub created with default mesh", runtime.GOOS)

			topic, err := ps.Join("optimusdb")
			if err != nil {
				log.Println("[ERROR] Failed to join PubSub topic:", err)
				app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Failed to join PubSub topic: %v", err), runtime.GOOS)
			} else {
				sub, err := topic.Subscribe()
				if err == nil {
					service.Pubsub = ps // ← IMPORTANT: Store in service
					service.Topic = topic
					service.Sub = sub
					go service.listenPubSub(peerHandler)
					log.Println("[DISCOVERY] Subscribed to optimusdb topic")
					app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", "Subscribed to optimusdb topic", runtime.GOOS)
				} else {
					log.Println("[ERROR] Failed to subscribe to topic:", err)
					app.GlobalLoggerDB.AddToOptimusLog("DISCOVERY", fmt.Sprintf("Failed to subscribe to topic: %v", err), runtime.GOOS)
				}
			}
		}
	}

	return service
}
