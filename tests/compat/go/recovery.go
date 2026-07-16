package main

import (
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
)

func consumeOne(address, topic, channel, expected string) error {
	delivery := make(chan []byte, 1)
	consumer, err := quietConsumer(topic, channel, baseConfig())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		delivery <- append([]byte(nil), message.Body...)
		return nil
	}))
	if err := consumer.ConnectToNSQD(address); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	body, err := receive(delivery, 20*time.Second)
	if err != nil {
		return err
	}
	if string(body) != expected {
		return fmt.Errorf("recovered body mismatch: got %q, want %q", body, expected)
	}
	return nil
}
