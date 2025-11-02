//go:build tinyllama_http

package contextualmetadata

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"
)

type LLMResponse struct {
	Text string `json:"text"`
}

type TinyLlamaClient interface {
	Generate(prompt string, maxTokens int) (string, error)
}

type HTTPClient struct {
	Endpoint string
	Timeout  time.Duration
}

func NewTinyLlamaHTTP() (*HTTPClient, error) {
	ep := os.Getenv("TINYLLAMA_ENDPOINT")
	if ep == "" {
		return nil, fmt.Errorf("TINYLLAMA_ENDPOINT not set")
	}
	return &HTTPClient{Endpoint: ep, Timeout: 60 * time.Second}, nil
}

func (c *HTTPClient) Generate(prompt string, maxTokens int) (string, error) {
	req := map[string]any{
		"prompt":      prompt,
		"max_tokens":  maxTokens,
		"temperature": 0.2,
	}
	body, _ := json.Marshal(req)
	httpc := &http.Client{Timeout: c.Timeout}
	resp, err := httpc.Post(c.Endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}
	var out LLMResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Text, nil
}
