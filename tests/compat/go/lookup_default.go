package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

func runDefaultLookupBootstrap(lookupHTTP, seedHTTP, newOwnerHTTP string) error {
	topic := uniqueTopic("lookup_default")
	channel := "workers"
	body := fmt.Sprintf("bootstrap-%d", time.Now().UnixNano())
	if err := createTopic(seedHTTP, topic); err != nil {
		return fmt.Errorf("create seed topic: %w", err)
	}
	if err := waitForLookupProducer(lookupHTTP, topic); err != nil {
		return err
	}

	config := baseConfig()
	if config.LookupdPollInterval != 60*time.Second || config.LookupdPollJitter != 0.3 {
		return fmt.Errorf(
			"unexpected official defaults: interval=%s jitter=%v",
			config.LookupdPollInterval,
			config.LookupdPollJitter,
		)
	}
	delivery := make(chan []byte, 1)
	consumer, err := quietConsumer(topic, channel, config)
	if err != nil {
		return err
	}
	consumer.SetLogger(log.New(os.Stderr, "nsq: ", log.LstdFlags|log.Lmicroseconds), nsq.LogLevelInfo)
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		delivery <- append([]byte(nil), message.Body...)
		return nil
	}))
	if err := consumer.ConnectToNSQLookupd(lookupHTTP); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	// Either Discovery replica may answer the first request while it is still
	// one refresh behind. The unchanged client deliberately recovers on its next
	// jittered 60-second poll, which the bootstrap-retention contract covers.
	if err := waitForConnections(consumer, 1, 90*time.Second); err != nil {
		return err
	}

	started := time.Now()
	client := &http.Client{Timeout: 3 * time.Second}
	if err := publishWithRetry(client, newOwnerHTTP, topic, body, 5*time.Second); err != nil {
		return fmt.Errorf("publish to newly selected owner: %w", err)
	}
	received, err := receive(delivery, 95*time.Second)
	if err != nil {
		return fmt.Errorf("default lookup polling missed bootstrap message: %w", err)
	}
	if string(received) != body {
		return fmt.Errorf("unexpected bootstrap body %q", received)
	}
	if consumer.Stats().Connections < 2 {
		return fmt.Errorf("consumer did not discover the new topic owner")
	}
	fmt.Printf("default-lookup-delay=%s topic=%s\n", time.Since(started).Round(time.Millisecond), topic)
	return nil
}
