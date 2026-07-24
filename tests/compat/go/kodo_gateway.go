package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
)

// config.C.NSQ.MaxMessageSize in the reviewed Kodo source. RustQueue keeps a
// 100 MiB protocol ceiling, leaving exactly 100 bytes of compatibility headroom.
const kodoMessageBytes = 104857500

type kodoNode struct {
	BroadcastAddress string `json:"broadcast_address"`
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	NodeID           uint64 `json:"node_id"`
}

type kodoTopicStats struct {
	TopicName    string `json:"topic_name"`
	MessageCount uint64 `json:"message_count"`
}

func runKodoGatewayAcceptance(lookupHTTP, gatewayMetricsHTTP string) error {
	nodes, err := waitKodoNodes(lookupHTTP)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		if err := requireHTTPPublishDisabled(node.httpAddress()); err != nil {
			return err
		}
	}
	for _, node := range nodes[:2] {
		if err := waitGatewayBackendCount(net.JoinHostPort(node.BroadcastAddress, "4160"), 3); err != nil {
			return err
		}
	}
	brokers, err := fakeBrokerNodes()
	if err != nil {
		return err
	}
	topic, channel := uniqueTopic("kodo_max_message"), "workers"
	for _, broker := range brokers {
		if err := createChannel(broker.httpAddress(), topic, channel); err != nil {
			return fmt.Errorf("create channel on Broker %d: %w", broker.NodeID, err)
		}
	}

	body := bytes.Repeat([]byte{0x5a}, kodoMessageBytes)
	budgetHold, err := holdGatewayIngressBudget(nodes[0], 40*1024*1024)
	if err != nil {
		return err
	}
	defer budgetHold.Close()
	started := time.Now()
	producer, publishingGateway, failovers, err := publishLikeKodo(nodes, topic, body)
	if err != nil {
		return fmt.Errorf(
			"publish Kodo's 104857500-byte maximum with the reviewed go-nsq timeouts: %w",
			err,
		)
	}
	budgetHold.Close()
	defer producer.Stop()
	if failovers != 1 || publishingGateway.BroadcastAddress != nodes[1].BroadcastAddress {
		return fmt.Errorf(
			"Kodo connection-error failover used %d retries and selected %s",
			failovers, publishingGateway.BroadcastAddress,
		)
	}
	fmt.Printf(
		"Kodo maximum-size publish completed in %s after %d Gateway failover\n",
		time.Since(started), failovers,
	)

	counts, err := waitMessageCount(brokers, topic, 1)
	if err != nil {
		return err
	}
	if _, err := waitMessageCount(nodes, topic, 1); err != nil {
		return fmt.Errorf("aggregate the three Kodo Gateway Stats shards: %w", err)
	}
	owner, err := singleOwner(brokers, counts)
	if err != nil {
		return err
	}
	if err := consumeLargeMessage(owner, topic, channel, body); err != nil {
		return err
	}
	body = nil

	if _, _, _, err := publishLikeKodo(
		nodes[2:], uniqueTopic("kodo_empty_gateway"), []byte("must-fail-before-commit"),
	); !kodoConnectionError(err) {
		return fmt.Errorf("Gateway without a Broker did not request Kodo connection failover: %v", err)
	}
	if err := requireReconnectMetric(gatewayMetricsHTTP); err != nil {
		return err
	}

	if err := setDrain(owner, true); err != nil {
		return err
	}
	defer setDrain(owner, false) //nolint:errcheck
	if err := producer.Publish(topic, []byte("after-drain")); err != nil {
		return fmt.Errorf("publish through a draining current Broker: %w", err)
	}
	counts, err = waitMessageCount(brokers, topic, 2)
	if err != nil {
		return err
	}
	if _, err := waitMessageCount(nodes, topic, 2); err != nil {
		return fmt.Errorf("aggregate the three Kodo Gateway Stats shards after failover: %w", err)
	}
	if counts[owner.NodeID] != 1 {
		return fmt.Errorf("draining Broker %d accepted another publish", owner.NodeID)
	}
	if len(nonzeroOwners(counts)) != 2 {
		return fmt.Errorf("publish did not fail over to a second Broker: %v", counts)
	}
	if err := producer.DeferredPublish(
		topic, 10*time.Millisecond, []byte("deferred-after-drain"),
	); err != nil {
		return fmt.Errorf("deferred publish through a draining current Broker: %w", err)
	}
	if _, err := waitMessageCount(brokers, topic, 3); err != nil {
		return err
	}
	if _, err := waitMessageCount(nodes, topic, 3); err != nil {
		return fmt.Errorf(
			"aggregate the three Kodo Gateway Stats shards after deferred publish: %w",
			err,
		)
	}
	if err := requireRetryMetric(net.JoinHostPort(publishingGateway.BroadcastAddress, "4160")); err != nil {
		return err
	}
	return nil
}

func holdGatewayIngressBudget(node kodoNode, bodyBytes uint32) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", node.tcpAddress(), 3*time.Second)
	if err != nil {
		return nil, fmt.Errorf("connect Gateway budget hold: %w", err)
	}
	closeOnError := func(err error) (net.Conn, error) {
		conn.Close()
		return nil, err
	}
	if err := conn.SetWriteDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return closeOnError(err)
	}
	request := append([]byte("  V2PUB ingress_budget_hold\n"), make([]byte, 4)...)
	binary.BigEndian.PutUint32(request[len(request)-4:], bodyBytes)
	request = append(request, 0x5a)
	if _, err := io.Copy(conn, bytes.NewReader(request)); err != nil {
		return closeOnError(fmt.Errorf("reserve Gateway ingress budget: %w", err))
	}
	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		return closeOnError(err)
	}
	// The Gateway reserves the declared body before waiting for the remainder.
	time.Sleep(250 * time.Millisecond)
	return conn, nil
}

func publishLikeKodo(
	nodes []kodoNode, topic string, body []byte,
) (*nsq.Producer, kodoNode, int, error) {
	var lastErr error
	for index, node := range nodes {
		config := nsq.NewConfig()
		if config.WriteTimeout != time.Second {
			return nil, kodoNode{}, index, fmt.Errorf(
				"go-nsq default WriteTimeout changed to %s", config.WriteTimeout,
			)
		}
		if config.ReadTimeout != 60*time.Second {
			return nil, kodoNode{}, index, fmt.Errorf(
				"go-nsq default ReadTimeout changed to %s", config.ReadTimeout,
			)
		}
		config.DialTimeout = 3 * time.Second
		config.WriteTimeout = 3 * time.Second
		producer, err := quietProducer(node.tcpAddress(), config)
		if err == nil {
			err = producer.Publish(topic, body)
		}
		if err == nil {
			return producer, node, index, nil
		}
		if producer != nil {
			producer.Stop()
		}
		lastErr = err
		if !kodoConnectionError(err) {
			return nil, kodoNode{}, index, err
		}
	}
	return nil, kodoNode{}, len(nodes), lastErr
}

func kodoConnectionError(err error) bool {
	if err == nil {
		return false
	}
	detail := err.Error()
	for _, fragment := range []string{
		"not connected",
		"connection reset by peer",
		"broken pipe",
		"EOF",
		"use of closed network connection",
	} {
		if strings.Contains(detail, fragment) {
			return true
		}
	}
	return false
}

func requireHTTPPublishDisabled(address string) error {
	request, err := http.NewRequest(
		http.MethodPost,
		"http://"+address+"/pub?topic=must_not_publish",
		strings.NewReader("blocked"),
	)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return fmt.Errorf("probe disabled Gateway HTTP publishing: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		return fmt.Errorf(
			"Gateway Stats port exposed HTTP publishing: expected 404, got %s",
			response.Status,
		)
	}
	return nil
}

func waitKodoNodes(address string) ([]kodoNode, error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get("http://" + address + "/nodes")
		if err == nil {
			var document struct {
				Producers []kodoNode `json:"producers"`
			}
			err = json.NewDecoder(response.Body).Decode(&document)
			response.Body.Close()
			if err == nil && len(document.Producers) == 3 {
				return document.Producers, nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("Kodo Discovery did not return exactly three Gateways")
}

func fakeBrokerNodes() ([]kodoNode, error) {
	producers, err := parseFakeProducers(os.Getenv("RUSTQUEUE_FAKE_BROKERS"))
	if err != nil {
		return nil, err
	}
	nodes := make([]kodoNode, 0, len(producers))
	for _, producer := range producers {
		nodes = append(nodes, kodoNode{
			BroadcastAddress: producer.BroadcastAddress,
			TCPPort:          producer.TCPPort,
			HTTPPort:         producer.HTTPPort,
			NodeID:           producer.NodeID,
		})
	}
	return nodes, nil
}

func waitMessageCount(brokers []kodoNode, topic string, expected uint64) (map[uint64]uint64, error) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		counts := make(map[uint64]uint64, len(brokers))
		var total uint64
		valid := true
		for _, broker := range brokers {
			count, err := topicMessageCount(broker.httpAddress(), topic)
			if err != nil {
				valid = false
				break
			}
			counts[broker.NodeID] = count
			total += count
		}
		if valid && total == expected {
			return counts, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, fmt.Errorf("topic %s did not reach cumulative message count %d", topic, expected)
}

func topicMessageCount(address, topic string) (uint64, error) {
	endpoint := fmt.Sprintf(
		"http://%s/stats?format=json&topic=%s",
		address, url.QueryEscape(topic),
	)
	response, err := http.Get(endpoint)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("stats returned %s", response.Status)
	}
	var document struct {
		Topics []kodoTopicStats `json:"topics"`
	}
	if err := json.NewDecoder(response.Body).Decode(&document); err != nil {
		return 0, err
	}
	for _, stats := range document.Topics {
		if stats.TopicName == topic {
			return stats.MessageCount, nil
		}
	}
	return 0, nil
}

func singleOwner(brokers []kodoNode, counts map[uint64]uint64) (kodoNode, error) {
	owners := nonzeroOwners(counts)
	if len(owners) != 1 {
		return kodoNode{}, fmt.Errorf("maximum-size publish has unexpected owners: %v", counts)
	}
	for _, broker := range brokers {
		if broker.NodeID == owners[0] {
			return broker, nil
		}
	}
	return kodoNode{}, fmt.Errorf("owner %d is not in the Broker inventory", owners[0])
}

func nonzeroOwners(counts map[uint64]uint64) []uint64 {
	var owners []uint64
	for nodeID, count := range counts {
		if count > 0 {
			owners = append(owners, nodeID)
		}
	}
	return owners
}

func consumeLargeMessage(broker kodoNode, topic, channel string, expected []byte) error {
	config := nsq.NewConfig()
	config.MaxInFlight = 1
	consumer, err := quietConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	received := make(chan error, 1)
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		if !bytes.Equal(message.Body, expected) {
			received <- fmt.Errorf("maximum-size delivery body mismatch")
		} else {
			received <- nil
		}
		return nil
	}))
	if err := consumer.ConnectToNSQD(broker.tcpAddress()); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	select {
	case err := <-received:
		return err
	case <-time.After(90 * time.Second):
		return fmt.Errorf("maximum-size delivery timed out")
	}
}

func setDrain(broker kodoNode, enabled bool) error {
	body := strings.NewReader(fmt.Sprintf(`{"enabled":%t}`, enabled))
	request, err := http.NewRequest(
		http.MethodPost, "http://"+broker.httpAddress()+"/v1/drain", body,
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("drain Broker %d returned %s: %s", broker.NodeID, response.Status, detail)
	}
	return nil
}

func requireRetryMetric(address string) error {
	value, err := proxyMetricValue(address, "rustqueue_proxy_producer_retries_total")
	if err == nil && value > 0 {
		return nil
	}
	return fmt.Errorf("Gateway did not record an explicit pre-commit retry")
}

func requireReconnectMetric(address string) error {
	value, err := proxyMetricValue(address, "rustqueue_proxy_publish_backends")
	if err == nil && value == 0 {
		return nil
	}
	return fmt.Errorf("isolated Gateway unexpectedly had a publish backend")
}

func waitGatewayBackendCount(address string, expected uint64) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		value, err := proxyMetricValue(address, "rustqueue_proxy_publish_backends")
		if err == nil && value == expected {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("Gateway %s did not discover %d publish backends", address, expected)
}

func proxyMetricValue(address, name string) (uint64, error) {
	response, err := http.Get("http://" + address + "/metrics")
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1024*1024))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == name {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			return value, err
		}
	}
	return 0, fmt.Errorf("Gateway metric %s is absent", name)
}

func (node kodoNode) tcpAddress() string {
	return net.JoinHostPort(node.BroadcastAddress, strconv.Itoa(node.TCPPort))
}

func (node kodoNode) httpAddress() string {
	return net.JoinHostPort(node.BroadcastAddress, strconv.Itoa(node.HTTPPort))
}
