package main

import (
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
)

func testFanout(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("fanout")
	channels := []string{"workers", "audit"}
	deliveries := map[string]chan []byte{}
	consumers := make([]*nsq.Consumer, 0, len(channels))
	for _, channel := range channels {
		delivery := make(chan []byte, 4)
		deliveries[channel] = delivery
		consumer, err := quietConsumer(topic, channel, factory())
		if err != nil {
			return err
		}
		consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
			delivery <- append([]byte(nil), message.Body...)
			return nil
		}))
		if err := connectConsumer(consumer, address, httpAddress, topic, channel); err != nil {
			return err
		}
		consumers = append(consumers, consumer)
	}
	defer func() {
		for _, consumer := range consumers {
			stopConsumer(consumer)
		}
	}()
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	if err := producer.MultiPublish(topic, [][]byte{[]byte("a"), []byte("b")}); err != nil {
		return err
	}
	for _, channel := range channels {
		seen := map[string]bool{}
		for len(seen) < 2 {
			body, err := receive(deliveries[channel], 10*time.Second)
			if err != nil {
				return err
			}
			seen[string(body)] = true
		}
	}
	return nil
}

func testSampling(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("sample")
	delivered := make(chan struct{}, 100)
	config := factory()
	config.SampleRate = 50
	config.MaxInFlight = 100
	consumer, err := quietConsumer(topic, "workers", config)
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		delivered <- struct{}{}
		return nil
	}))
	if err := connectConsumer(consumer, address, httpAddress, topic, "workers"); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	messages := make([][]byte, 100)
	for index := range messages {
		messages[index] = []byte(fmt.Sprintf("sample-%03d", index))
	}
	if err := producer.MultiPublish(topic, messages); err != nil {
		return err
	}
	deadline := time.After(10 * time.Second)
	count := 0
	for count < 50 {
		select {
		case <-delivered:
			count++
		case <-deadline:
			return fmt.Errorf("sample delivered only %d of 50 expected messages", count)
		}
	}
	time.Sleep(500 * time.Millisecond)
	count += len(delivered)
	if count != 50 {
		return fmt.Errorf("sample delivered %d messages, expected 50", count)
	}
	return nil
}

func testEphemeral(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("ephemeral")
	channel := "temporary#ephemeral"
	first := make(chan []byte, 1)
	consumer, err := quietConsumer(topic, channel, factory())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		first <- append([]byte(nil), message.Body...)
		return nil
	}))
	if err := connectConsumer(consumer, address, httpAddress, topic, channel); err != nil {
		return err
	}
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	if err := producer.Publish(topic, []byte("first")); err != nil {
		return err
	}
	if _, err := receive(first, 10*time.Second); err != nil {
		return err
	}
	stopConsumer(consumer)
	if err := waitForChannel(httpAddress, topic, channel, false); err != nil {
		return err
	}
	if err := producer.Publish(topic, []byte("stale")); err != nil {
		return err
	}
	second := make(chan []byte, 2)
	consumer, err = quietConsumer(topic, channel, factory())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		second <- append([]byte(nil), message.Body...)
		return nil
	}))
	if err := connectConsumer(consumer, address, httpAddress, topic, channel); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	select {
	case body := <-second:
		return fmt.Errorf("new ephemeral channel received stale body %q", body)
	case <-time.After(300 * time.Millisecond):
	}
	if err := producer.Publish(topic, []byte("fresh")); err != nil {
		return err
	}
	body, err := receive(second, 10*time.Second)
	if err != nil {
		return err
	}
	if string(body) != "fresh" {
		return fmt.Errorf("unexpected ephemeral body %q", body)
	}
	return nil
}

func testLookup(address, httpAddress string, factory configFactory) error {
	return testLookupEndpoint(address, httpAddress, httpAddress, factory)
}

func testLookupEndpoint(address, managementAddress, lookupAddress string, factory configFactory) error {
	topic := uniqueTopic("lookup")
	if err := createTopic(managementAddress, topic); err != nil {
		return err
	}
	if err := waitForLookupProducer(lookupAddress, topic); err != nil {
		return err
	}
	delivery := make(chan []byte, 1)
	consumer, err := quietConsumer(topic, "workers", factory())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		delivery <- append([]byte(nil), message.Body...)
		return nil
	}))
	if err := consumer.ConnectToNSQLookupd(lookupAddress); err != nil {
		return err
	}
	if err := waitForChannel(lookupAddress, topic, "workers", true); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	if err := producer.Publish(topic, []byte("lookup")); err != nil {
		return err
	}
	_, err = receive(delivery, 10*time.Second)
	return err
}
