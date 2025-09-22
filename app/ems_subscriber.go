package app

import (
	"encoding/json"
	"fmt"
	"github.com/go-stomp/stomp"
	"golang.org/x/net/context"
	"optimusdb/mq"
	"os"
	"strconv"
	"strings"
	"time"
)

// StartEMSSubscriber connects to the EMS broker (ActiveMQ STOMP) and subscribes.
// Returns a cleanup() you should defer on shutdown.
/*
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error) {
	// ---- Read config from env (works great in K3s) ----
	serviceName := getenvDefault("EMS_SERVICE_NAME", "ems-broker") // k8s Service name
	namespace := getenvDefault("EMS_NAMESPACE", "messaging")       // k8s namespace
	stompPort := getenvIntDefault("EMS_STOMP_PORT", 61610)
	topic := getenvDefault("EMS_TOPIC", "/topic/>")

	user := getenvDefault("MQ_USER", "aaa")
	pass := getenvDefault("MQ_PASS", "111")
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

	// Log the exact address that will be dialed
	if GlobalLoggerDB != nil {
		_ = GlobalLoggerDB.AddToOptimusLog(
			"INFO",
			fmt.Sprintf("EMS dialing %s topic=%s durable=%v clientID=%s", addr, topic, durable, clientID),
			"ems",
		)
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

*/
// StartEMSSubscriber starts EMS service with auto-reconnect
func (db *KnowledgeBaseDB) StartEMSSubscriber(ctx context.Context) (cleanup func() error, err error) {
	host := os.Getenv("EMS_SERVICE_NAME")
	if host == "" {
		host = "ems-broker.default.svc.cluster.local"
	}
	portStr := os.Getenv("EMS_STOMP_PORT")
	if portStr == "" {
		portStr = "61610"
	}
	stompPort, _ := strconv.Atoi(portStr)

	user := os.Getenv("MQ_USER")
	if user == "" {
		user = "aaa"
	}
	pass := os.Getenv("MQ_PASS")
	if pass == "" {
		pass = "111"
	}
	clientID := os.Getenv("MQ_CLIENT_ID")
	topic := os.Getenv("EMS_TOPIC")
	if topic == "" {
		topic = "/topic/>"
	}

	cfg := mq.Config{
		Host:     host,
		Port:     stompPort,
		User:     user,
		Pass:     pass,
		ClientID: clientID,
		Topic:    topic,
	}

	service := mq.NewEMSService(cfg, 10*time.Second)
	service.OnMessage(func(dest string, msg *stomp.Message) {
		if msg != nil && msg.Body != nil {
			_ = db.handleEMSMessage(msg.Body)
		}
	})
	service.OnConnected(func() {
		if GlobalLoggerDB != nil {
			_ = GlobalLoggerDB.AddToOptimusLog("INFO",
				fmt.Sprintf("EMS connected (host=%s port=%d topic=%s)", cfg.Host, cfg.Port, cfg.Topic),
				"ems")
		}
	})
	service.OnDisconnected(func(err error) {
		if GlobalLoggerDB != nil {
			_ = GlobalLoggerDB.AddToOptimusLog("WARN",
				fmt.Sprintf("EMS disconnected: %v", err),
				"ems")
		}
	})

	db.EMSService = service
	service.Start()

	return func() error {
		service.Stop()
		db.EMSService = nil
		return nil
	}, nil
}

// Persist every message in handleEMSMessage i.e. from EMS topic
func (db *KnowledgeBaseDB) handleEMSMessage(body []byte) error {
	now := time.Now().UTC()
	topic := getenvDefault("EMS_TOPIC", "/topic/>")
	clientID := os.Getenv("MQ_CLIENT_ID")

	// Try to parse; we still store raw if parsing fails.
	var m EMSMessage
	parseErr := json.Unmarshal(body, &m)

	// Persist one row per message (raw + parsed fields)
	if GlobalLoggerDB != nil {
		paramsJSON := ""
		if parseErr == nil && m.Params != nil {
			if b, err := json.Marshal(m.Params); err == nil {
				paramsJSON = string(b)
			}
		}
		_ = GlobalLoggerDB.InsertEMSEvent(
			now, db.HostID, clientID, topic,
			m.Action, m.Resource, paramsJSON, string(body),
		)

		// Optional: short line to optimusLogger for quick grep/tail
		if parseErr != nil {
			_ = GlobalLoggerDB.AddToOptimusLog("ERROR",
				"EMS recv (unmarshal failed): "+truncate(string(body), 180), "ems")
		} else {
			_ = GlobalLoggerDB.AddToOptimusLog("INFO",
				fmt.Sprintf("EMS recv action=%s resource=%s body=%s",
					m.Action, m.Resource, truncate(string(body), 160)), "ems")
		}
	}

	// Hand off to domain logic (already logs in ProcessEMS)
	if parseErr != nil {
		return parseErr
	}
	return db.ProcessEMS(m.Action, m.Resource, m.Params)
}

// tiny helper used above
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 3 {
		return s[:n]
	}
	return s[:n-3] + "..."
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

// EMSSend sends a message to EMS (if connected)
func (db *KnowledgeBaseDB) EMSSend(dest, contentType string, body []byte) error {
	if db.EMSService == nil {
		return fmt.Errorf("EMS service not initialized")
	}
	return db.EMSService.Send(dest, contentType, body)
}
