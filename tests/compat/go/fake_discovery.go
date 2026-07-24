package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

type fakeProducer struct {
	RemoteAddress    string `json:"remote_address"`
	Hostname         string `json:"hostname"`
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	NodeID           uint64 `json:"node_id"`
}

func runFakeDiscovery() error {
	brokers, err := parseFakeProducers(os.Getenv("RUSTQUEUE_FAKE_BROKERS"))
	if err != nil {
		return fmt.Errorf("parse fake Brokers: %w", err)
	}
	gateways, err := parseFakeProducers(os.Getenv("RUSTQUEUE_FAKE_GATEWAYS"))
	if err != nil {
		return fmt.Errorf("parse fake Gateways: %w", err)
	}
	if len(brokers) != 3 || len(gateways) != 3 {
		return fmt.Errorf("fake Discovery requires exactly three Brokers and Gateways")
	}
	publishers := brokers
	if os.Getenv("RUSTQUEUE_FAKE_EMPTY_PUBLISHERS") == "1" {
		publishers = []fakeProducer{}
	}
	mux := http.NewServeMux()
	write := func(value any) http.HandlerFunc {
		return func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(response).Encode(value); err != nil {
				panic(err)
			}
		}
	}
	mux.HandleFunc("/v1/publishers/head", write(map[string]any{
		"revision": 1, "broker_count": len(publishers),
	}))
	mux.HandleFunc("/v1/publishers", write(map[string]any{
		"revision": 1, "producers": publishers,
	}))
	mux.HandleFunc("/v1/brokers", write(map[string]any{
		"revision": 1, "producers": brokers,
	}))
	mux.HandleFunc("/nodes", write(map[string]any{"producers": gateways}))
	mux.HandleFunc("/topics", write(map[string]any{"topics": []string{}}))
	mux.HandleFunc("/channels", write(map[string]any{"channels": []string{}}))
	return http.ListenAndServe(":4161", mux)
}

func parseFakeProducers(value string) ([]fakeProducer, error) {
	var producers []fakeProducer
	for _, item := range strings.Split(value, ",") {
		parts := strings.Split(strings.TrimSpace(item), "|")
		if len(parts) != 4 {
			return nil, fmt.Errorf("%q must be host|tcp|http|node-id", item)
		}
		tcpPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, err
		}
		httpPort, err := strconv.Atoi(parts[2])
		if err != nil {
			return nil, err
		}
		nodeID, err := strconv.ParseUint(parts[3], 10, 64)
		if err != nil {
			return nil, err
		}
		producers = append(producers, fakeProducer{
			RemoteAddress:    net.JoinHostPort(parts[0], parts[1]),
			Hostname:         parts[0],
			BroadcastAddress: parts[0],
			TCPPort:          tcpPort,
			HTTPPort:         httpPort,
			Version:          "0.8.0",
			NodeID:           nodeID,
		})
	}
	return producers, nil
}
