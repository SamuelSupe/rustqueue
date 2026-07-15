package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

func runCore(address, httpAddress string) error {
	for _, compression := range []string{"snappy", "deflate"} {
		factory := func() *nsq.Config {
			config := baseConfig()
			config.Snappy = compression == "snappy"
			config.Deflate = compression == "deflate"
			return config
		}
		if err := testRoundTrip(address, httpAddress, compression, factory); err != nil {
			return err
		}
	}
	factory := func() *nsq.Config { return baseConfig() }
	for name, test := range map[string]func(string, string, configFactory) error{
		"mpub+dpub": testBatchAndDeferred,
		"req":       testRequeue,
		"touch":     testTouch,
		"rdy":       testReadyLimit,
		"fanout":    testFanout,
		"sample":    testSampling,
		"ephemeral": testEphemeral,
		"lookup":    testLookup,
	} {
		if err := test(address, httpAddress, factory); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	return nil
}

func testRoundTrip(address, httpAddress, label string, factory configFactory) error {
	topic := uniqueTopic(label)
	received := make(chan []byte, 1)
	consumer, err := quietConsumer(topic, "workers", factory())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		received <- append([]byte(nil), message.Body...)
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
	body := []byte("rustqueue-" + label)
	if err := producer.Publish(topic, body); err != nil {
		return err
	}
	actual, err := receive(received, 10*time.Second)
	if err != nil {
		return err
	}
	if string(actual) != string(body) {
		return fmt.Errorf("body mismatch: %q", actual)
	}
	return nil
}

func testBatchAndDeferred(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("batch")
	deliveries := make(chan []byte, 8)
	consumer, err := quietConsumer(topic, "workers", factory())
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		deliveries <- append([]byte(nil), message.Body...)
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
	if err := producer.MultiPublish(topic, [][]byte{[]byte("one"), []byte("two"), []byte("three")}); err != nil {
		return err
	}
	started := time.Now()
	if err := producer.DeferredPublish(topic, 300*time.Millisecond, []byte("later")); err != nil {
		return err
	}
	seen := map[string]bool{}
	for len(seen) < 4 {
		body, err := receive(deliveries, 10*time.Second)
		if err != nil {
			return err
		}
		seen[string(body)] = true
		if string(body) == "later" && time.Since(started) < 250*time.Millisecond {
			return fmt.Errorf("DPUB was delivered before its delay")
		}
	}
	return nil
}

func testRequeue(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("req")
	done := make(chan uint16, 1)
	started := time.Now()
	config := factory()
	config.MaxInFlight = 1
	consumer, err := quietConsumer(topic, "workers", config)
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		message.DisableAutoResponse()
		if message.Attempts == 1 {
			started = time.Now()
			message.RequeueWithoutBackoff(150 * time.Millisecond)
			return nil
		}
		message.Finish()
		done <- message.Attempts
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
	if err := producer.Publish(topic, []byte("retry")); err != nil {
		return err
	}
	select {
	case attempts := <-done:
		if attempts < 2 || time.Since(started) < 100*time.Millisecond {
			return fmt.Errorf("REQ did not defer and redeliver")
		}
		return nil
	case <-time.After(10 * time.Second):
		return fmt.Errorf("REQ redelivery timed out")
	}
}

func testTouch(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("touch")
	finished := make(chan struct{}, 1)
	var deliveries atomic.Int32
	config := factory()
	config.MaxInFlight = 1
	config.MsgTimeout = time.Second
	consumer, err := quietConsumer(topic, "workers", config)
	if err != nil {
		return err
	}
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		deliveries.Add(1)
		message.DisableAutoResponse()
		go func() {
			time.Sleep(700 * time.Millisecond)
			message.Touch()
			time.Sleep(700 * time.Millisecond)
			message.Finish()
			finished <- struct{}{}
		}()
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
	if err := producer.Publish(topic, []byte("long-running")); err != nil {
		return err
	}
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		return fmt.Errorf("TOUCH flow timed out")
	}
	time.Sleep(1200 * time.Millisecond)
	if deliveries.Load() != 1 {
		return fmt.Errorf("TOUCH allowed %d deliveries", deliveries.Load())
	}
	return nil
}

func testReadyLimit(address, httpAddress string, factory configFactory) error {
	topic := uniqueTopic("rdy")
	config := factory()
	config.MaxInFlight = 2
	consumer, err := quietConsumer(topic, "workers", config)
	if err != nil {
		return err
	}
	var active atomic.Int32
	var maximum atomic.Int32
	reached := make(chan struct{})
	release := make(chan struct{})
	var reachedOnce sync.Once
	done := make(chan struct{}, 4)
	consumer.AddConcurrentHandlers(nsq.HandlerFunc(func(message *nsq.Message) error {
		current := active.Add(1)
		for current > maximum.Load() && !maximum.CompareAndSwap(maximum.Load(), current) {
		}
		if current == 2 {
			reachedOnce.Do(func() { close(reached) })
		}
		<-release
		active.Add(-1)
		done <- struct{}{}
		return nil
	}), 4)
	if err := connectConsumer(consumer, address, httpAddress, topic, "workers"); err != nil {
		return err
	}
	defer stopConsumer(consumer)
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	if err := producer.MultiPublish(topic, [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4")}); err != nil {
		return err
	}
	select {
	case <-reached:
		close(release)
	case <-time.After(10 * time.Second):
		close(release)
		return fmt.Errorf("RDY never allowed two concurrent messages")
	}
	for range 4 {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			return fmt.Errorf("RDY completion timed out")
		}
	}
	if maximum.Load() > 2 {
		return fmt.Errorf("RDY allowed %d concurrent messages", maximum.Load())
	}
	return nil
}
