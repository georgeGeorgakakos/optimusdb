package mq

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-stomp/stomp"
)

type Client struct {
	conn         *stomp.Conn
	DefaultTopic string
	topic        string
	url          string
	user         string
	pass         string
}

type Config struct {
	URL string // e.g., tcp://activemq.messaging.svc.cluster.local:61613
	// 2) Host + Port (if URL is empty)
	Host     string
	Port     int // default 61613 if 0
	User     string
	Pass     string
	Topic    string // e.g., "/topic/ems.events" or "/topic/optimusdb.events"
	ClientID string // required for durable subscriptions (ActiveMQ)
	// ServiceName + Namespace -> "<service>.<ns>.svc.cluster.local[:port]"
	ServiceName string
	Namespace   string // default "default" if empty

	// Resolve to IP instead of DNS name (optional; connects to IP:port)
	UseIP           bool
	ResolveWithDNS  bool          // default true
	DialTimeout     time.Duration // default 5s
	HeartbeatSend   time.Duration // default 10s
	HeartbeatRecv   time.Duration // default 10s
	ConnectionRetry time.Duration // optional backoff if you add reconnect loops

	// Optional env var keys (if you want to read host/port from env)
	EnvHostVar string // e.g., "EMS_BROKER_HOST"
	EnvPortVar string // e.g., "EMS_BROKER_PORT"
}

// PublishJSON sends a JSON payload to the client's configured topic.
func (c *Client) PublishJSON(topic string, payload []byte) error {
	if c == nil || c.conn == nil {
		return errors.New("mq not connected")
	}
	if topic == "" {
		topic = c.DefaultTopic
	}
	return c.conn.Send(topic, "application/json", payload, stomp.SendOpt.Receipt)
}

// SubscribeJSON (non-durable) auto-ack subscription.
func (c *Client) SubscribeJSON(topic string, handler func([]byte) error) (func() error, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("mq not connected")
	}
	if topic == "" {
		topic = c.DefaultTopic
	}
	sub, err := c.conn.Subscribe(topic, stomp.AckAuto)
	if err != nil {
		return nil, err
	}
	stop := func() error { return sub.Unsubscribe() }
	go func() {
		for msg := range sub.C {
			if msg == nil || msg.Err != nil || msg.Body == nil {
				continue
			}
			_ = handler(msg.Body) // best-effort
		}
	}()
	return stop, nil
}

// SubscribeJSONDurable uses client-individual ack + subscriptionName (ActiveMQ durable).
// Durable subscription: requires ActiveMQ and a stable ClientID for the connection.
// Uses client-individual ack; ACK only on successful handler.
func (c *Client) SubscribeJSONDurable(topic, subscriptionName string, handler func([]byte) error) (func() error, error) {
	if c == nil || c.conn == nil {
		return nil, errors.New("mq not connected")
	}
	if topic == "" {
		topic = c.DefaultTopic
	}
	sub, err := c.conn.Subscribe(
		topic,
		stomp.AckClientIndividual,
		stomp.SubscribeOpt.Header("activemq.subscriptionName", subscriptionName),
		stomp.SubscribeOpt.Header("ack", "client-individual"),
	)
	if err != nil {
		return nil, err
	}
	stop := func() error { return sub.Unsubscribe() }
	go func() {
		for msg := range sub.C {
			if msg == nil || msg.Err != nil || msg.Body == nil {
				continue
			}
			if handler != nil {
				if err := handler(msg.Body); err == nil {
					_ = c.conn.Ack(msg) // ack on success
				}
			}
		}
	}()
	return stop, nil
}

/*
	func Connect(cfg Config) (*Client, error) {
		addr := strings.TrimPrefix(cfg.URL, "tcp://")
		if addr == "" {
			return nil, errors.New("invalid MQ URL")
		}
		c, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err != nil {
			return nil, err
		}
		opts := []func(*stomp.Conn) error{
			stomp.ConnOpt.Login(cfg.User, cfg.Pass),
			stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
		}
		if cfg.ClientID != "" {
			opts = append(opts, stomp.ConnOpt.Header("client-id", cfg.ClientID))
		}
		conn, err := stomp.Connect(c, opts...)
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		return &Client{conn: conn, topic: cfg.Topic, url: cfg.URL, user: cfg.User, pass: cfg.Pass}, nil
	}
*/
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Disconnect() // graceful; waits for RECEIPT
}

// /*
// For EMS debug//
func (c *Config) setDefaults() {
	if c.Port == 0 {
		c.Port = 61613
	}
	if c.Namespace == "" {
		c.Namespace = "default"
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.HeartbeatSend == 0 {
		c.HeartbeatSend = 10 * time.Second
	}
	if c.HeartbeatRecv == 0 {
		c.HeartbeatRecv = 10 * time.Second
	}
	if c.ResolveWithDNS == false && c.UseIP == false {
		// Prefer DNS inside K8s unless explicitly disabled
		c.ResolveWithDNS = true
	}
}

// Resolve broker network address like "host:port" or "IP:port"
func (c *Config) resolveAddr(ctx context.Context) (string, error) {
	c.setDefaults()

	// 0) If URL provided, parse it
	if c.URL != "" {
		u, err := url.Parse(c.URL)
		if err != nil {
			return "", fmt.Errorf("invalid MQ URL: %w", err)
		}
		if u.Scheme != "tcp" {
			return "", fmt.Errorf("unsupported scheme %q (use tcp://)", u.Scheme)
		}
		host := u.Host
		if !strings.Contains(host, ":") {
			host = net.JoinHostPort(host, strconv.Itoa(c.Port))
		}
		if c.UseIP {
			return hostToIP(ctx, host)
		}
		return host, nil
	}

	// 1) If Host provided, use it
	host := c.Host
	port := c.Port

	// 2) Try env overrides (if set)
	if host == "" && c.EnvHostVar != "" {
		host = os.Getenv(c.EnvHostVar)
	}
	if port == 0 && c.EnvPortVar != "" {
		if p := os.Getenv(c.EnvPortVar); p != "" {
			if v, err := strconv.Atoi(p); err == nil {
				port = v
			}
		}
	}

	// 3) If still no host, build K8s service FQDN
	if host == "" && c.ServiceName != "" {
		host = fmt.Sprintf("%s.%s.svc.cluster.local", c.ServiceName, c.Namespace)
	}

	if host == "" {
		return "", errors.New("no broker host resolved (provide URL, Host, or ServiceName)")
	}
	if port == 0 {
		port = 61613
	}

	addr := net.JoinHostPort(host, strconv.Itoa(port))
	if c.UseIP {
		return hostToIP(ctx, addr)
	}
	return addr, nil
}

func hostToIP(ctx context.Context, hostPort string) (string, error) {
	host, port, err := net.SplitHostPort(hostPort)
	if err != nil {
		return "", err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return "", err
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("no IPs resolved for %s", host)
	}
	return net.JoinHostPort(ips[0].IP.String(), port), nil
}

func NewClient(cfg Config) (*Client, error) {
	cfg.setDefaults()
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	addr, err := cfg.resolveAddr(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(cfg.User, cfg.Pass),
		stomp.ConnOpt.HeartBeat(cfg.HeartbeatSend, cfg.HeartbeatRecv),
	}
	if cfg.ClientID != "" {
		opts = append(opts, stomp.ConnOpt.Header("client-id", cfg.ClientID))
	}

	// Use stomp.Dial for simplicity
	conn, err := stomp.Dial("tcp", addr, opts...)
	if err != nil {
		return nil, fmt.Errorf("stomp dial %s failed: %w", addr, err)
	}

	return &Client{
		conn:         conn,
		DefaultTopic: cfg.Topic,
	}, nil
}
