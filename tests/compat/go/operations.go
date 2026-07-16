package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

type lookupNode struct {
	BroadcastAddress string `json:"broadcast_address"`
	HTTPPort         int    `json:"http_port"`
}

type operationalReport struct {
	Topic          string `json:"topic"`
	Expected       int    `json:"expected"`
	Received       int    `json:"received_unique"`
	Duplicates     uint64 `json:"duplicates"`
	Missing        int    `json:"missing"`
	PublishErrors  uint64 `json:"publish_errors"`
	MaxConnections int    `json:"max_connections"`
}

func runOperationalLedger(proxyHTTP, lookupHTTP string, duration time.Duration, minimumBrokers int) error {
	topic := uniqueTopic("operations")
	channel := "workers"
	nodes, err := waitForLookupNodes(lookupHTTP, minimumBrokers)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		address := fmt.Sprintf("%s:%d", node.BroadcastAddress, node.HTTPPort)
		if err := createChannel(address, topic, channel); err != nil {
			return fmt.Errorf("create channel on %s: %w", address, err)
		}
	}

	state := &ledgerState{expected: map[string]struct{}{}, received: map[string]uint64{}}
	config := baseConfig()
	config.LookupdPollInterval = 250 * time.Millisecond
	config.MaxInFlight = 2500
	consumer, err := quietConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		body := string(message.Body)
		state.Lock()
		state.received[body]++
		state.Unlock()
		return nil
	}))
	if err := consumer.ConnectToNSQLookupd(lookupHTTP); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	if err := waitForConnections(consumer, minimumBrokers, 15*time.Second); err != nil {
		return err
	}
	fmt.Printf("operational-ledger-ready topic=%s brokers=%d\n", topic, minimumBrokers)

	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(duration)
	var sequence uint64
	var publishErrors uint64
	maxConnections := 0
	for time.Now().Before(deadline) {
		sequence++
		body := fmt.Sprintf("operation-%08d", sequence)
		state.Lock()
		state.expected[body] = struct{}{}
		state.Unlock()
		if err := publishWithRetry(client, proxyHTTP, topic, body, 5*time.Second); err != nil {
			state.Lock()
			delete(state.expected, body)
			state.Unlock()
			publishErrors++
		}
		if connections := consumer.Stats().Connections; connections > maxConnections {
			maxConnections = connections
		}
		time.Sleep(20 * time.Millisecond)
	}

	waitDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(waitDeadline) {
		state.Lock()
		complete := expectedReceived(state)
		state.Unlock()
		if complete {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	state.Lock()
	received := 0
	var duplicates uint64
	for body := range state.expected {
		count := state.received[body]
		if count > 0 {
			received++
		}
		if count > 1 {
			duplicates += count - 1
		}
	}
	report := operationalReport{
		Topic:          topic,
		Expected:       len(state.expected),
		Received:       received,
		Duplicates:     duplicates,
		Missing:        len(state.expected) - received,
		PublishErrors:  publishErrors,
		MaxConnections: maxConnections,
	}
	state.Unlock()
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return err
	}
	if report.Expected < 100 || report.Missing != 0 || report.PublishErrors != 0 || report.MaxConnections < minimumBrokers {
		return fmt.Errorf("multi-broker operational ledger failed: %+v", report)
	}
	return nil
}

func expectedReceived(state *ledgerState) bool {
	for body := range state.expected {
		if state.received[body] == 0 {
			return false
		}
	}
	return true
}

func waitForConnections(consumer *nsq.Consumer, minimum int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if consumer.Stats().Connections >= minimum {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("consumer reached %d of %d broker connections", consumer.Stats().Connections, minimum)
}

func waitForLookupNodes(address string, minimum int) ([]lookupNode, error) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://" + address + "/nodes")
		if err == nil {
			var body struct {
				Producers []lookupNode `json:"producers"`
			}
			err = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			if err == nil && len(body.Producers) >= minimum {
				return body.Producers, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("lookup did not expose %d brokers", minimum)
}

func createChannel(address, topic, channel string) error {
	endpoint := fmt.Sprintf(
		"http://%s/channel/create?topic=%s&channel=%s",
		address, url.QueryEscape(topic), url.QueryEscape(channel),
	)
	request, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	if token := os.Getenv("RUSTQUEUE_ADMIN_TOKEN"); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("channel create returned %s", response.Status)
	}
	return nil
}

func publishWithRetry(client *http.Client, address, topic, body string, timeout time.Duration) error {
	endpoint := fmt.Sprintf("http://%s/pub?topic=%s", address, url.QueryEscape(topic))
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Post(endpoint, "application/octet-stream", bytes.NewBufferString(body))
		if err == nil {
			response.Body.Close()
			if response.StatusCode/100 == 2 {
				return nil
			}
			lastErr = fmt.Errorf("publish returned %s", response.Status)
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	return lastErr
}
