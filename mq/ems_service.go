package mq

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/go-stomp/stomp"
)

type EMSService struct {
	cfg        Config
	retryDelay time.Duration

	mu       sync.RWMutex
	conn     *stomp.Conn
	stopped  chan struct{}
	onceStop sync.Once

	onMessage      func(dest string, msg *stomp.Message)
	onConnected    func()
	onDisconnected func(error)
}

// NewEMSService creates the service but does not block
func NewEMSService(cfg Config, retryDelay time.Duration) *EMSService {
	return &EMSService{
		cfg:        cfg,
		retryDelay: retryDelay,
		stopped:    make(chan struct{}),
	}
}

// Start runs the background connection loop
func (s *EMSService) Start() {
	go s.loop()
}

/*
	func (s *EMSService) loop() {
		for {
			select {
			case <-s.stopped:
				return
			default:
			}

			if !s.isConnected() {
				if err := s.connect(); err != nil {
					log.Printf("[EMS] Connect failed: %v (retry in %s)", err, s.retryDelay)
					time.Sleep(s.retryDelay)
					continue
				}
			}

			// heartbeat / check loop
			time.Sleep(s.retryDelay)
			if s.conn != nil && s.conn.Err() != nil {
				log.Printf("[EMS] Connection lost: %v", s.conn.Err())
				s.disconnect()
			}
		}
	}
*/
func (s *EMSService) loop() {
	for {
		select {
		case <-s.stopped:
			return
		default:
		}

		// If not connected → try to connect
		if !s.isConnected() {
			if err := s.connect(); err != nil {
				log.Printf("[EMS] Connect failed: %v (retry in %s)", err, s.retryDelay)
				time.Sleep(s.retryDelay)
				continue
			}
		}

		// Sleep between health checks
		time.Sleep(s.retryDelay)

		// Try a lightweight "send" as a heartbeat
		if s.isConnected() {
			err := s.Send("/queue/optimusdb-health", "text/plain", []byte("ping"))
			if err != nil {
				log.Printf("[EMS] Connection check failed, reconnecting: %v", err)
				s.disconnect()
			}
		}
	}
}
func (s *EMSService) connect() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	opts := []func(*stomp.Conn) error{
		stomp.ConnOpt.Login(s.cfg.User, s.cfg.Pass),
		stomp.ConnOpt.HeartBeat(10*time.Second, 10*time.Second),
	}
	if s.cfg.ClientID != "" {
		opts = append(opts, stomp.ConnOpt.Header("client-id", s.cfg.ClientID))
	}

	conn, err := stomp.Dial("tcp", addr, opts...)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	log.Printf("[EMS] Connected to STOMP at %s", addr)
	if s.onConnected != nil {
		s.onConnected()
	}

	if s.cfg.Topic != "" {
		sub, err := conn.Subscribe(s.cfg.Topic, stomp.AckAuto)
		if err != nil {
			return fmt.Errorf("subscribe failed: %w", err)
		}
		go func() {
			for msg := range sub.C {
				if s.onMessage != nil {
					s.onMessage(s.cfg.Topic, msg)
				}
			}
		}()
	}

	return nil
}

func (s *EMSService) disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Disconnect()
		s.conn = nil
		if s.onDisconnected != nil {
			s.onDisconnected(fmt.Errorf("connection closed"))
		}
	}
}

func (s *EMSService) isConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.conn != nil
}

func (s *EMSService) Send(dest, contentType string, body []byte) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.conn == nil {
		return fmt.Errorf("not connected")
	}
	return s.conn.Send(dest, contentType, body)
}

func (s *EMSService) OnMessage(handler func(dest string, msg *stomp.Message)) {
	s.mu.Lock()
	s.onMessage = handler
	s.mu.Unlock()
}

func (s *EMSService) Stop() {
	s.onceStop.Do(func() {
		close(s.stopped)
	})
	s.disconnect()
}

func (s *EMSService) OnConnected(handler func()) {
	s.onConnected = handler
}
func (s *EMSService) OnDisconnected(handler func(err error)) {
	s.onDisconnected = handler
}
