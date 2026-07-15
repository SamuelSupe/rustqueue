package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type ledgerState struct {
	sync.Mutex
	expected   map[string]struct{}
	received   map[string]uint64
	unexpected uint64
}

type ledgerReport struct {
	Topic       string `json:"topic"`
	Expected    int    `json:"expected"`
	Received    int    `json:"received_unique"`
	Duplicates  uint64 `json:"duplicates"`
	Missing     int    `json:"missing"`
	Unexpected  uint64 `json:"unexpected"`
	Connections int    `json:"connections"`
}

func runLedger(lookup, topic, channel, expectedPath string, timeout time.Duration) error {
	expected, err := readExpected(expectedPath)
	if err != nil {
		return err
	}
	if len(expected) == 0 {
		return fmt.Errorf("ledger has no acknowledged messages")
	}
	state := &ledgerState{expected: expected, received: map[string]uint64{}}
	config := baseConfig()
	config.MaxInFlight = 2500
	config.MsgTimeout = 30 * time.Second
	consumer, err := quietConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		body := string(message.Body)
		state.Lock()
		if _, ok := state.expected[body]; ok {
			state.received[body]++
		} else {
			state.unexpected++
		}
		state.Unlock()
		return nil
	}))
	if err := consumer.ConnectToNSQLookupd(lookup); err != nil {
		return fmt.Errorf("connect lookup: %w", err)
	}
	defer stopConsumer(consumer)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		state.Lock()
		complete := len(state.received) == len(state.expected)
		state.Unlock()
		if complete {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	state.Lock()
	report := ledgerReport{
		Topic:       topic,
		Expected:    len(state.expected),
		Received:    len(state.received),
		Missing:     len(state.expected) - len(state.received),
		Unexpected:  state.unexpected,
		Connections: consumer.Stats().Connections,
	}
	for _, count := range state.received {
		if count > 1 {
			report.Duplicates += count - 1
		}
	}
	state.Unlock()
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return err
	}
	if report.Missing != 0 || report.Unexpected != 0 || report.Connections == 0 {
		return fmt.Errorf("ledger did not recover every acknowledged message: %+v", report)
	}
	return nil
}

func readExpected(path string) (map[string]struct{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	expected := map[string]struct{}{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		body := scanner.Text()
		if body == "" {
			return nil, fmt.Errorf("ledger contains an empty message")
		}
		if _, duplicate := expected[body]; duplicate {
			return nil, fmt.Errorf("ledger contains duplicate expected body %q", body)
		}
		expected[body] = struct{}{}
	}
	return expected, scanner.Err()
}
