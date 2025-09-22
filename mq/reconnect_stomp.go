package mq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-stomp/stomp"
)

// Config is your EMS connection config (same fields you already use)
/*
type Config struct {
	URL         string
	Host        string
	Port        int
	ServiceName string
	Namespace   string
	UseIP       bool
	User        string
	Pass        string
	ClientID    string
	Topic       string
	Durable     bool
}

*/

// ReconnectingClient manages a background STOMP connection with auto-resubscribe.
type ReconnectingClient struct {
	cfg        Config
	retryDelay time.Duration

	mu       sync.RWMutex
	conn     *stomp.Conn
	stopped  chan struct{}
	onceStop sync.Once

	// remembered subscriptions
	subs map[string]stomp.AckMode

	// callback invoked per message
	onMessage func(string, *stomp.Message)
}

// NewReconnectingClient starts the background loop
func NewReconnectingClient(cfg Config, retryDelay time.Duration) *ReconnectingClient {
	rc := &ReconnectingClient{
		cfg:        cfg,
		retryDelay: retryDelay,
		stopped:    make(chan struct{}),
		subs:       make(map[string]stomp.AckMode),
	}
	go rc.loop()
	return rc
}

func (rc *ReconnectingClient) loop() {
	for {
		select {
		case <-rc.stopped:
			return
		default:
		}

		addr := fmt.Sprintf("%s:%d", rc.cfg.Host, rc.cfg.Port)
		opts := []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(rc.cfg.User, rc.cfg.Pass),
			stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
		}
		if rc.cfg.ClientID != "" {
			opts = append(opts, stomp.ConnOpt.Header("client-id", rc.cfg.ClientID))
		}

		conn, err := stomp.Dial("tcp", addr, opts...)
		if err != nil {
			log.Printf("[EMS] STOMP connect failed: %v (retrying in %s)", err, rc.retryDelay)
			select {
			case <-time.After(rc.retryDelay):
				continue
			case <-rc.stopped:
				return
			}
		}

		rc.mu.Lock()
		rc.conn = conn
		rc.mu.Unlock()
		log.Printf("[EMS] Connected to STOMP at %s", addr)

		// restore subscriptions
		for dest, ack := range rc.snapshotSubs() {
			if err := rc.subscribeInternal(dest, ack); err != nil {
				log.Printf("[EMS] Failed to subscribe %s: %v", dest, err)
			}
		}

		// monitor connection
		for {
			select {
			case <-rc.stopped:
				return
			default:
				if !rc.isConnected() {
					break
				}
				// Send a lightweight heartbeat message
				err := rc.Send("/queue/optimusdb-health", "text/plain", []byte("ping"))
				if err != nil {
					log.Printf("[EMS] Heartbeat failed, reconnecting: %v", err)
					rc.closeConn()
					time.Sleep(rc.retryDelay)
					break
				}
			}
			time.Sleep(5 * time.Second)
			if !rc.isConnected() {
				break
			}
		}
	}
}

func (rc *ReconnectingClient) isConnected() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.conn != nil
}

func (rc *ReconnectingClient) closeConn() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.conn != nil {
		_ = rc.conn.Disconnect()
		rc.conn = nil
	}
}

func (rc *ReconnectingClient) snapshotSubs() map[string]stomp.AckMode {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	out := make(map[string]stomp.AckMode, len(rc.subs))
	for k, v := range rc.subs {
		out[k] = v
	}
	return out
}

// Subscribe remembers the destination and (re)subscribes on reconnect.
func (rc *ReconnectingClient) Subscribe(destination string, ack stomp.AckMode) error {
	rc.mu.Lock()
	rc.subs[destination] = ack
	rc.mu.Unlock()
	return rc.subscribeInternal(destination, ack)
}

func (rc *ReconnectingClient) subscribeInternal(destination string, ack stomp.AckMode) error {
	rc.mu.RLock()
	conn := rc.conn
	onMsg := rc.onMessage
	rc.mu.RUnlock()

	if conn == nil {
		return nil // not connected yet
	}

	sub, err := conn.Subscribe(destination, ack)
	if err != nil {
		return err
	}
	go func() {
		for msg := range sub.C {
			if onMsg != nil {
				onMsg(destination, msg)
			}
		}
	}()
	return nil
}

// Send message to EMS
func (rc *ReconnectingClient) Send(destination, contentType string, body []byte) error {
	rc.mu.RLock()
	conn := rc.conn
	rc.mu.RUnlock()
	if conn == nil {
		return fmt.Errorf("EMS not connected")
	}
	return conn.Send(destination, contentType, body)
}

// OnMessage sets callback handler for all messages
func (rc *ReconnectingClient) OnMessage(handler func(dest string, msg *stomp.Message)) {
	rc.mu.Lock()
	rc.onMessage = handler
	rc.mu.Unlock()
}

// Close stops the reconnect loop
func (rc *ReconnectingClient) Close() {
	rc.onceStop.Do(func() {
		close(rc.stopped)
	})
	rc.closeConn()
}
