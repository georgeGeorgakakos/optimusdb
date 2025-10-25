package queryengine

import (
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"log"
)

func RegisterQueryStreamHandler(hostNode host.Host, db *KnowledgeBaseDB) {
	hostNode.SetStreamHandler("/query/1.0.0", func(stream network.Stream) {
		handleQueryStream(stream, db)
	})
	log.Println("[INFO] ✓ Registered query stream handler for /query/1.0.0")
}

func handleQueryStream(stream network.Stream, db *KnowledgeBaseDB) {
	defer stream.Close()

	var queryMsg map[string]interface{}
	if err := json.NewDecoder(stream).Decode(&queryMsg); err != nil {
		json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}

	criteriaRaw, ok := queryMsg["criteria"].([]interface{})
	if !ok {
		json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}

	var criteria []map[string]interface{}
	for _, c := range criteriaRaw {
		if criterion, ok := c.(map[string]interface{}); ok {
			criteria = append(criteria, criterion)
		}
	}

	results, err := queryLocalDB(db, criteria)
	if err != nil {
		json.NewEncoder(stream).Encode([]map[string]interface{}{})
		return
	}

	log.Printf("[PEER-QUERY] Sent %d results to peer", len(results))
	json.NewEncoder(stream).Encode(results)
}
