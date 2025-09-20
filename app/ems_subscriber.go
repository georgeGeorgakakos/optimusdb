package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"optimusdb/mq"
	"os"
	"strconv"
	"strings"
	"time"
)

// StartEMSSubscriber connects to the EMS broker (ActiveMQ STOMP) and subscribes.
// Returns a cleanup() you should defer on shutdown.
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error) {
	// ---- Read config from env (works great in K3s) ----
	serviceName := getenvDefault("EMS_SERVICE_NAME", "ems-broker") // k8s Service name
	namespace := getenvDefault("EMS_NAMESPACE", "messaging")       // k8s namespace
	stompPort := getenvIntDefault("EMS_STOMP_PORT", 61613)
	topic := getenvDefault("EMS_TOPIC", "/topic/ems.events")

	user := getenvDefault("MQ_USER", "admin")
	pass := getenvDefault("MQ_PASS", "admin")
	clientID := os.Getenv("MQ_CLIENT_ID") // required for durable
	useIP := getenvBoolDefault("EMS_USE_IP", false)

	durable := getenvBoolDefault("EMS_DURABLE", true) // default true in cluster
	subName := getenvDefault("EMS_SUB_NAME", "optimusdb-ems")

	// Fallback clientID if not provided: use pod hostname or HostID
	if clientID == "" {
		hn, _ := os.Hostname()
		if hn != "" {
			clientID = hn
		} else if db != nil && db.HostID != "" {
			clientID = "optimusdb-" + db.HostID
		}
	}

	// ---- Resolve broker address in-cluster: Service DNS or IP ----
	host := fmt.Sprintf("%s.%s.svc.cluster.local", serviceName, namespace)
	addr := fmt.Sprintf("%s:%d", host, stompPort)

	if useIP {
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil || len(ips) == 0 {
			return nil, fmt.Errorf("DNS resolve failed for %s: %w", host, err)
		}
		addr = fmt.Sprintf("%s:%d", ips[0].IP.String(), stompPort)
	}

	// ---- Create STOMP client (via mq package) ----
	cfg := mq.Config{
		URL:           "", // we'll dial by Host:Port
		Host:          host,
		Port:          stompPort,
		ServiceName:   serviceName,
		Namespace:     namespace,
		UseIP:         useIP,
		User:          user,
		Pass:          pass,
		ClientID:      clientID, // needed for durable
		Topic:         topic,    // default topic
		DialTimeout:   5 * time.Second,
		HeartbeatSend: 10 * time.Second,
		HeartbeatRecv: 10 * time.Second,
	}
	mqc, err := mq.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("EMS MQ connect failed (%s): %w", addr, err)
	}

	db.MQEMS = mqc

	// ---- Subscribe (durable or not) ----
	var stopSub func() error
	if durable && clientID != "" && subName != "" {
		stopSub, err = db.MQEMS.SubscribeJSONDurable(topic, subName, db.handleEMSMessage)
	} else {
		stopSub, err = db.MQEMS.SubscribeJSON(topic, db.handleEMSMessage)
	}
	if err != nil {
		_ = db.MQEMS.Close()
		db.MQEMS = nil
		return nil, fmt.Errorf("EMS subscribe failed: %w", err)
	}

	// Optional: log a “consumer up” event via your logger DB
	if GlobalLoggerDB != nil {
		_ = GlobalLoggerDB.AddToOptimusLog("INFO",
			fmt.Sprintf("EMS subscribed to %s (durable=%v, clientID=%s, svc=%s.%s)", topic, durable, clientID, serviceName, namespace),
			"ems")
	}

	// ---- Return cleanup function ----
	return func() error {
		if stopSub != nil {
			_ = stopSub()
		}
		if db.MQEMS != nil {
			_ = db.MQEMS.Close()
			db.MQEMS = nil
		}
		return nil
	}, nil
}

// handleEMSMessage parses payload and routes to your internal EMS processor
func (db *KnowledgeBaseDB) handleEMSMessage(body []byte) error {
	var m EMSMessage
	if err := json.Unmarshal(body, &m); err != nil {
		if GlobalLoggerDB != nil {
			_ = GlobalLoggerDB.AddToOptimusLog("ERROR", "EMS unmarshal failed: "+err.Error(), "ems")
		}
		return err
	}
	return db.ProcessEMS(m.Action, m.Resource, m.Params)
}

// Simple helpers (stay local to this file)
func getenvDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
func getenvIntDefault(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
func getenvBoolDefault(k string, def bool) bool {
	switch strings.ToLower(os.Getenv(k)) {
	case "1", "true", "yes", "y":
		return true
	case "0", "false", "no", "n":
		return false
	default:
		return def
	}
}
