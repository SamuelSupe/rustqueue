package main

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"
)

func createTopic(address, topic string) error {
	endpoint := fmt.Sprintf("http://%s/topic/create?topic=%s", address, url.QueryEscape(topic))
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		request, err := http.NewRequest(http.MethodPost, endpoint, nil)
		if err != nil {
			return err
		}
		if token := os.Getenv("RUSTQUEUE_ADMIN_TOKEN"); token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := http.DefaultClient.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode/100 == 2 {
				return nil
			}
			lastErr = fmt.Errorf("topic create returned %s", response.Status)
			if response.StatusCode/100 == 4 && response.StatusCode != http.StatusTooManyRequests {
				return lastErr
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			return lastErr
		}
		time.Sleep(100 * time.Millisecond)
	}
}
