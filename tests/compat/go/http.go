package main

import (
	"fmt"
	"net/http"
	"net/url"
)

func createTopic(address, topic string) error {
	endpoint := fmt.Sprintf("http://%s/topic/create?topic=%s", address, url.QueryEscape(topic))
	request, err := http.NewRequest(http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode/100 != 2 {
		return fmt.Errorf("topic create returned %s", response.Status)
	}
	return nil
}
