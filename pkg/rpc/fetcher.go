package rpc

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/maestroi/solana-snapshot-finder-go/pkg/config"
)

var DEFAULT_HEADERS = map[string]string{
	"Content-Type": "application/json",
}

type RPCNode struct {
	Address string
	Version string
}

func GetRPCNodes(rpcAddress string, retries int, denylist []string, privateRPC bool) ([]RPCNode, []string, error) {
	// Log denylist configuration if any IPs are specified
	if len(denylist) > 0 {
		log.Printf("Denylist configured with %d IPs: %v", len(denylist), denylist)
	}

	payload := []byte(`{"jsonrpc":"2.0", "id":1, "method":"getClusterNodes"}`)
	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(payload))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create request: %v", err)
	}

	// Add default headers
	for key, value := range DEFAULT_HEADERS {
		req.Header.Set(key, value)
	}

	client := &http.Client{Timeout: 15 * time.Second} // Adjust timeout as needed

	var resp *http.Response
	for attempt := 1; attempt <= retries; attempt++ {
		resp, err = client.Do(req)
		if err == nil {
			break
		}
		time.Sleep(2 * time.Second) // Add delay between retries
	}
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch RPC nodes after %d retries: %v", retries, err)
	}
	defer resp.Body.Close()

	var result struct {
		Result []struct {
			RPC     string `json:"rpc"`
			Gossip  string `json:"gossip"`
			Version string `json:"version"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, nil, fmt.Errorf("failed to decode RPC nodes response: %v", err)
	}

	nodes := []RPCNode{}
	addresses := []string{}
	for _, node := range result.Result {
		// Handle regular RPC nodes
		if node.RPC != "" {
			rpcIP := strings.Split(node.RPC, ":")[0]

			// Check if the IP is denied
			isDenied := false
			for _, blocked := range denylist {
				if rpcIP == blocked {
					isDenied = true
					log.Printf("Node %s denied by config (IP %s is in denylist)", node.RPC, rpcIP)
					break
				}
			}

			if !isDenied {
				nodes = append(nodes, RPCNode{
					Address: node.RPC,
					Version: node.Version,
				})
				addresses = append(addresses, node.RPC)
			}
		}

		// Handle private RPC nodes
		if privateRPC && node.Gossip != "" {
			gossipIP := strings.Split(node.Gossip, ":")[0] // Extract gossip IP
			privateRPCAddress := fmt.Sprintf("%s:8899", gossipIP)

			// Check if the IP is denied
			isDenied := false
			for _, blocked := range denylist {
				if gossipIP == blocked {
					isDenied = true
					log.Printf("Private node %s denied by config (IP %s is in denylist)", privateRPCAddress, gossipIP)
					break
				}
			}

			if !isDenied {
				nodes = append(nodes, RPCNode{
					Address: privateRPCAddress,
					Version: node.Version,
				})
				addresses = append(addresses, privateRPCAddress)
			}
		}
	}

	// Log summary of denylist filtering
	if len(denylist) > 0 {
		log.Printf("Denylist filtering complete: %d nodes available, %d nodes denied by config", len(nodes), len(result.Result)-len(nodes))
	}

	return nodes, addresses, nil
}

func GetReferenceSlot(rpcAddress string) (int, error) {
	payload := map[string]interface{}{
		"id":      1,
		"jsonrpc": "2.0",
		"method":  "getSlot",
		"params":  []interface{}{},
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequest("POST", rpcAddress, bytes.NewBuffer(body))
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch slot: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body: %v", err)
	}

	var result struct {
		JSONRPC string `json:"jsonrpc"`
		Result  int    `json:"result"`
		ID      int    `json:"id"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return 0, fmt.Errorf("failed to parse response: %v", err)
	}

	return result.Result, nil
}

// FetchRPCNodes fetches RPC nodes
func FetchRPCNodes(cfg config.Config) []RPCNode {
	var nodes []RPCNode
	var err error

	for attempt := 1; attempt <= cfg.NumOfRetries; attempt++ {
		nodes, _, err = GetRPCNodes(cfg.RPCAddress, cfg.NumOfRetries, cfg.Denylist, cfg.PrivateRPC)
		if err == nil && len(nodes) > 0 {
			log.Printf("Fetched %d RPC nodes on attempt %d.", len(nodes), attempt)
			return nodes
		}

		log.Printf("Attempt %d/%d to fetch RPC nodes failed: %v", attempt, cfg.NumOfRetries, err)
		time.Sleep(2 * time.Second) // Add delay between retries
	}

	if err != nil {
		log.Fatalf("Failed to fetch RPC nodes after %d retries: %v", cfg.NumOfRetries, err)
	} else if len(nodes) == 0 {
		log.Fatalf("No RPC nodes found after %d retries.", cfg.NumOfRetries)
	}

	return nil // Should not reach here
}

// Selects the best RPC from the evaluated nodes
func selectBestRPC(results []struct {
	rpc     string
	speed   float64
	latency float64
	slot    int
	diff    int
	version string
	status  string
}) string {
	var bestGoodNode struct {
		rpc   string
		speed float64
	}
	var bestSlowNode struct {
		rpc   string
		speed float64
	}

	for _, result := range results {
		if result.status == "good" && result.speed > bestGoodNode.speed {
			bestGoodNode = struct {
				rpc   string
				speed float64
			}{rpc: result.rpc, speed: result.speed}
		}
		if result.status == "slow" && result.speed > bestSlowNode.speed {
			bestSlowNode = struct {
				rpc   string
				speed float64
			}{rpc: result.rpc, speed: result.speed}
		}
	}

	// Prioritize good nodes; fallback to the fastest slow node if no good nodes are available
	if bestGoodNode.rpc != "" {
		return bestGoodNode.rpc
	}
	if bestSlowNode.rpc != "" {
		log.Printf("No good nodes found. Falling back to the fastest slow node: %s with speed %.2f MB/s", bestSlowNode.rpc, bestSlowNode.speed)
		return bestSlowNode.rpc
	}

	log.Println("No suitable RPC nodes found.")
	return ""
}
