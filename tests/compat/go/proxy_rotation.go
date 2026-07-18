package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

func runProxyRotation(proxyTCP, lookupHTTP string) error {
	topic := uniqueTopic("proxy_rotation")
	producer, err := quietProducer(proxyTCP, baseConfig())
	if err != nil {
		return err
	}
	defer producer.Stop()

	deadline := time.Now().Add(8 * time.Second)
	published := 0
	for time.Now().Before(deadline) {
		body := []byte(fmt.Sprintf("rotation-%08d", published+1))
		if err := publishTCPWithRetry(producer.Publish, topic, body, 3*time.Second); err != nil {
			return err
		}
		published++
		time.Sleep(100 * time.Millisecond)
	}
	if published < 20 {
		return fmt.Errorf("only %d messages were published during rotation", published)
	}
	if err := waitForTopicOwners(lookupHTTP, topic, 2, 15*time.Second); err != nil {
		return err
	}
	return nil
}

func publishTCPWithRetry(
	publish func(string, []byte) error,
	topic string,
	body []byte,
	timeout time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := publish(topic, body); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("publish did not recover after proxy rotation: %w", lastErr)
}

func waitForTopicOwners(address, topic string, minimum int, timeout time.Duration) error {
	endpoint := fmt.Sprintf("http://%s/lookup?topic=%s", address, url.QueryEscape(topic))
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := http.Get(endpoint)
		if err == nil {
			var body struct {
				Producers []json.RawMessage `json:"producers"`
			}
			err = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			if err == nil && len(body.Producers) >= minimum {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("topic %s did not spread to %d owners", topic, minimum)
}
