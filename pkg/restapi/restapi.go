// Package restapi is a client for the Apstra DCD REST API.
// It mirrors the functionality of the dcdrestapi package from the original
// Apstra telegraf plugin (plugins/inputs/dcd/restapi/dcdrestapi.go).
//
// Responsibilities:
//   - Login and token management
//   - Fetching and caching blueprint metadata
//   - Fetching and caching system (device) metadata
//   - Registering and deregistering telemetry streaming sessions
package restapi

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// API response types
// ----------------------------------------------------------------------------

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
}

// Blueprint represents an DCD blueprint (fabric design).
type Blueprint struct {
	ID   string
	Name string
}

// System represents a managed device registered with DCD.
type System struct {
	DeviceKey     string
	AdminState    string
	BlueprintID   string // status.blueprint_id
	BlueprintRole string // blueprint_active.role
	BlueprintName string // blueprint_active.label
}

type streamingConfigRequest struct {
	StreamingType  string `json:"streaming_type"`
	Transport      string `json:"transport"`
	SequencingMode string `json:"sequencing_mode"`
	Hostname       string `json:"hostname"`
	Protocol       string `json:"protocol"`
	Port           int    `json:"port"`
}

type streamingConfigResponse struct {
	ID string `json:"id"`
}

// ----------------------------------------------------------------------------
// Client
// ----------------------------------------------------------------------------

// Client is the DCD REST API client. It is safe for concurrent use.
type Client struct {
	address  string
	port     int
	user     string
	password string
	protocol string

	token string
	http  *http.Client

	mu                sync.RWMutex
	blueprints        map[string]*Blueprint // id → Blueprint
	systems           map[string]*System    // device_key → System
	streamingSessions []string              // registered session IDs
}

// NewClient creates a new DCD REST API client.
func NewClient(address string, port int, user, password, protocol string) *Client {
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // DCD uses self-signed certs
	}
	return &Client{
		address:    address,
		port:       port,
		user:       user,
		password:   password,
		protocol:   protocol,
		blueprints: make(map[string]*Blueprint),
		systems:    make(map[string]*System),
		http: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// Login authenticates with DCD and stores the session token.
func (c *Client) Login() error {
	body := loginRequest{Username: c.user, Password: c.password}
	var resp loginResponse
	if err := c.post("/api/user/login", body, &resp); err != nil {
		return fmt.Errorf("DCD login failed: %w", err)
	}
	if resp.Token == "" {
		return fmt.Errorf("DCD login: empty token in response")
	}
	c.token = resp.Token
	log.Printf("I! [dcd] Authenticated with DCD server %s://%s:%d", c.protocol, c.address, c.port)
	return nil
}

// GetBlueprints fetches all blueprints from DCD and caches them.
// Mirrors GetBlueprints() in dcdrestapi.go.
func (c *Client) GetBlueprints() error {
	var result struct {
		Items []struct {
			ID          string `json:"id"`
			Label       string `json:"label"`
			DisplayName string `json:"display_name"`
		} `json:"items"`
	}
	if err := c.get("/api/blueprints", &result); err != nil {
		return fmt.Errorf("GetBlueprints: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blueprints = make(map[string]*Blueprint, len(result.Items))
	for _, item := range result.Items {
		name := item.Label
		if name == "" {
			name = item.DisplayName
		}
		if name == "" {
			name = item.ID
		}
		c.blueprints[item.ID] = &Blueprint{ID: item.ID, Name: name}
		log.Printf("D! [dcd] GetBlueprints() - Id %s", item.ID)
	}
	return nil
}

// GetSystems fetches all managed systems from DCD and caches them.
// Mirrors GetSystems() in dcdrestapi.go.
func (c *Client) GetSystems() error {
	var result struct {
		Items []struct {
			ID         string `json:"id"`
			DeviceKey  string `json:"device_key"`
			UserConfig struct {
				AdminState string `json:"admin_state"`
			} `json:"user_config"`
			Status struct {
				BlueprintID string `json:"blueprint_id"`
				Role        string `json:"role"`
			} `json:"status"`
			BlueprintActive struct {
				Role  string `json:"role"`
				Label string `json:"label"`
			} `json:"blueprint_active"`
		} `json:"items"`
	}
	if err := c.get("/api/systems", &result); err != nil {
		return fmt.Errorf("GetSystems: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.systems = make(map[string]*System, len(result.Items))
	for _, item := range result.Items {
		key := item.DeviceKey
		if key == "" {
			key = item.ID
		}
		role := item.BlueprintActive.Role
		if role == "" {
			role = item.Status.Role
		}
		sys := &System{
			DeviceKey:     key,
			AdminState:    item.UserConfig.AdminState,
			BlueprintID:   item.Status.BlueprintID,
			BlueprintRole: role,
			BlueprintName: item.BlueprintActive.Label,
		}
		c.systems[key] = sys
		if sys.BlueprintID != "" {
			log.Printf("I! [dcd] System: %s | %s | bp=%s role=%s",
				key, sys.AdminState, sys.BlueprintID, sys.BlueprintRole)
		} else {
			log.Printf("I! [dcd] System: %s | %s", key, sys.AdminState)
		}
	}
	return nil
}

// GetSystemByKey returns the cached System for a device key, or nil.
// Mirrors GetSystemByKey() in dcdrestapi.go.
func (c *Client) GetSystemByKey(deviceKey string) *System {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.systems[deviceKey]
}

// GetBlueprintByID returns the cached Blueprint for a blueprint ID, or nil.
// Mirrors GetBlueprintById() in dcdrestapi.go.
func (c *Client) GetBlueprintByID(id string) *Blueprint {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.blueprints[id]
}

// StartStreaming registers a telemetry streaming session on the DCD server.
// streamingType: "perfmon" | "alerts" | "events"
// Mirrors StartStreaming() in dcdrestapi.go.
func (c *Client) StartStreaming(streamingType, address string, port int) error {
	body := streamingConfigRequest{
		StreamingType:  streamingType,
		Transport:      "protoBufOverTcp",
		SequencingMode: "sequenced",
		Hostname:       address,
		Protocol:       "protoBufOverTcp",
		Port:           port,
	}
	var resp streamingConfigResponse
	if err := c.post("/api/streaming-config", body, &resp); err != nil {
		return fmt.Errorf("StartStreaming(%s): %w", streamingType, err)
	}
	if resp.ID != "" {
		c.mu.Lock()
		c.streamingSessions = append(c.streamingSessions, resp.ID)
		c.mu.Unlock()
		log.Printf("I! [dcd] Streaming session %s registered (type=%s)", resp.ID, streamingType)
	}
	return nil
}

// StopStreaming deletes all registered streaming sessions from DCD.
// Mirrors StopStreaming() in dcdrestapi.go.
func (c *Client) StopStreaming() error {
	c.mu.Lock()
	sessions := make([]string, len(c.streamingSessions))
	copy(sessions, c.streamingSessions)
	c.mu.Unlock()

	var lastErr error
	for _, id := range sessions {
		if err := c.delete(fmt.Sprintf("/api/streaming-config/%s", id)); err != nil {
			log.Printf("W! [dcd] Failed to delete streaming session %s: %v", id, err)
			lastErr = err
		} else {
			log.Printf("I! [dcd] Streaming session %s removed", id)
		}
	}
	c.mu.Lock()
	c.streamingSessions = nil
	c.mu.Unlock()
	return lastErr
}

// ----------------------------------------------------------------------------
// HTTP helpers
// ----------------------------------------------------------------------------

func (c *Client) url(path string) string {
	return fmt.Sprintf("%s://%s:%d%s", c.protocol, c.address, c.port, path)
}

func (c *Client) newRequest(method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, c.url(path), body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("AUTHTOKEN", c.token)
	}
	return req, nil
}

func (c *Client) do(method, path string, reqBody, respBody interface{}) error {
	var bodyReader io.Reader
	if reqBody != nil {
		b, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := c.newRequest(method, path, bodyReader)
	if err != nil {
		return err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview := string(raw)
		if len(preview) > 200 {
			preview = preview[:200]
		}
		return fmt.Errorf("HTTP %d from %s %s: %s", resp.StatusCode, method, path, preview)
	}

	if respBody != nil && len(raw) > 0 {
		if err := json.Unmarshal(raw, respBody); err != nil {
			return fmt.Errorf("unmarshal response from %s %s: %w", method, path, err)
		}
	}
	return nil
}

func (c *Client) get(path string, out interface{}) error {
	return c.do("GET", path, nil, out)
}

func (c *Client) post(path string, body, out interface{}) error {
	return c.do("POST", path, body, out)
}

func (c *Client) delete(path string) error {
	return c.do("DELETE", path, nil, nil)
}
