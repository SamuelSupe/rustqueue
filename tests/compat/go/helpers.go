package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/nsqio/go-nsq"
)

type configFactory func() *nsq.Config

func baseConfig() *nsq.Config {
	config := nsq.NewConfig()
	config.UserAgent = "rustqueue-compat/0.4"
	config.HeartbeatInterval = time.Second
	config.ReadTimeout = 5 * time.Second
	config.WriteTimeout = 5 * time.Second
	config.MsgTimeout = 2 * time.Second
	config.OutputBufferSize = 4096
	config.OutputBufferTimeout = 100 * time.Millisecond
	config.MaxInFlight = 4
	return config
}

func uniqueTopic(prefix string) string {
	return fmt.Sprintf("compat_%s_%d", prefix, time.Now().UnixNano())
}

func quietConsumer(topic, channel string, config *nsq.Config) (*nsq.Consumer, error) {
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		return nil, err
	}
	consumer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	return consumer, nil
}

func quietProducer(address string, config *nsq.Config) (*nsq.Producer, error) {
	producer, err := nsq.NewProducer(address, config)
	if err != nil {
		return nil, err
	}
	producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	return producer, nil
}

func connectConsumer(consumer *nsq.Consumer, address, httpAddress, topic, channel string) error {
	if err := consumer.ConnectToNSQD(address); err != nil {
		return fmt.Errorf("connect consumer: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for consumer.Stats().Connections == 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("consumer did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return waitForChannel(httpAddress, topic, channel, true)
}

func stopConsumer(consumer *nsq.Consumer) {
	consumer.Stop()
	<-consumer.StopChan
}

func waitForChannel(address, topic, channel string, present bool) error {
	endpoint := fmt.Sprintf("http://%s/channels?topic=%s", address, url.QueryEscape(topic))
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint)
		if err == nil {
			var body struct {
				Channels []string `json:"channels"`
			}
			err = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			if err == nil {
				found := false
				for _, current := range body.Channels {
					found = found || current == channel
				}
				if found == present {
					return nil
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("channel %s presence did not become %t", channel, present)
}

func waitForLookupProducer(address, topic string) error {
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", address, url.QueryEscape(topic))
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		request, err := http.NewRequest(http.MethodGet, endpoint, nil)
		if err != nil {
			return err
		}
		request.Header.Set("Accept", "application/vnd.nsq; version=1.0")
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			var body struct {
				Producers []json.RawMessage `json:"producers"`
			}
			err = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			if err == nil && len(body.Producers) > 0 {
				return nil
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("topic %s did not become discoverable within 10s", topic)
}

func receive(channel <-chan []byte, timeout time.Duration) ([]byte, error) {
	select {
	case body := <-channel:
		return body, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("timed out waiting for delivery")
	}
}
