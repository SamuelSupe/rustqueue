package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type ledger struct {
	sync.Mutex
	acknowledged map[uint64]struct{}
	consumed     map[uint64]uint64
	unexpected   uint64
	publishErrs  uint64
}

type report struct {
	Topic               string `json:"topic"`
	Acknowledged        uint64 `json:"acknowledged"`
	ConsumedUnique      uint64 `json:"consumed_unique"`
	Duplicate           uint64 `json:"duplicate"`
	Missing             uint64 `json:"missing"`
	UnconfirmedConsumed uint64 `json:"unconfirmed_consumed"`
	PublishErrors       uint64 `json:"publish_errors"`
	Unexpected          uint64 `json:"unexpected"`
	DurationSeconds     int64  `json:"duration_seconds"`
}

func main() {
	lookup := flag.String("lookup", "host.docker.internal:4151", "lookupd-compatible HTTP address")
	tcp := flag.String("tcp", "host.docker.internal:4150,host.docker.internal:5150,host.docker.internal:6150", "comma-separated NSQ TCP addresses")
	duration := flag.Duration("duration", 24*time.Hour, "publish duration")
	grace := flag.Duration("grace", 2*time.Minute, "final drain timeout")
	rate := flag.Int("rate", 1000, "target messages per second; zero means saturated")
	readyFile := flag.String("ready-file", "", "create this file after the channel is active")
	flag.Parse()
	if *duration <= 0 || *grace <= 0 || *rate < 0 {
		log.Fatal("duration and grace must be positive; rate must not be negative")
	}

	runID := strconv.FormatInt(time.Now().UnixNano(), 36)
	topic := "soak_" + runID
	channel := "ledger"
	prefix := topic + ":"
	state := &ledger{acknowledged: map[uint64]struct{}{}, consumed: map[uint64]uint64{}}
	config := nsq.NewConfig()
	config.UserAgent = "rustqueue-soak/0.4"
	config.MsgTimeout = 30 * time.Second
	config.MaxInFlight = 2500

	consumer, err := nsq.NewConsumer(topic, channel, config)
	check(err)
	consumer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		body := string(message.Body)
		if !strings.HasPrefix(body, prefix) {
			state.Lock()
			state.unexpected++
			state.Unlock()
			return nil
		}
		sequence, err := strconv.ParseUint(strings.TrimPrefix(body, prefix), 10, 64)
		state.Lock()
		if err != nil {
			state.unexpected++
		} else {
			state.consumed[sequence]++
		}
		state.Unlock()
		return nil
	}))
	check(consumer.ConnectToNSQLookupd(*lookup))
	waitConsumer(consumer, *lookup, topic, channel)
	if *readyFile != "" {
		check(os.WriteFile(*readyFile, []byte(topic), 0o600))
	}

	producers := make([]*nsq.Producer, 0)
	for _, address := range strings.Split(*tcp, ",") {
		producer, err := nsq.NewProducer(strings.TrimSpace(address), config)
		check(err)
		producer.SetLogger(log.New(io.Discard, "", 0), nsq.LogLevelError)
		producers = append(producers, producer)
	}
	if len(producers) == 0 {
		log.Fatal("at least one TCP address is required")
	}

	started := time.Now()
	deadline := started.Add(*duration)
	var sequence uint64
	var ticker *time.Ticker
	if *rate > 0 {
		interval := time.Second / time.Duration(*rate)
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
	}
	for time.Now().Before(deadline) {
		if ticker != nil {
			<-ticker.C
		}
		sequence++
		body := []byte(prefix + strconv.FormatUint(sequence, 10))
		published := false
		for offset := range producers {
			producer := producers[(int(sequence)+offset)%len(producers)]
			if producer.Publish(topic, body) == nil {
				published = true
				break
			}
		}
		state.Lock()
		if published {
			state.acknowledged[sequence] = struct{}{}
		} else {
			state.publishErrs++
		}
		state.Unlock()
	}

	drainDeadline := time.Now().Add(*grace)
	for time.Now().Before(drainDeadline) {
		state.Lock()
		missing := missingLocked(state)
		state.Unlock()
		if missing == 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	for _, producer := range producers {
		producer.Stop()
	}
	consumer.Stop()
	<-consumer.StopChan

	result := buildReport(state, topic, time.Since(started))
	check(json.NewEncoder(os.Stdout).Encode(result))
	if result.Acknowledged == 0 || result.Missing != 0 || result.Unexpected != 0 {
		os.Exit(1)
	}
}

func waitConsumer(consumer *nsq.Consumer, lookup, topic, channel string) {
	deadline := time.Now().Add(30 * time.Second)
	endpoint := fmt.Sprintf("http://%s/channels?topic=%s", lookup, url.QueryEscape(topic))
	for time.Now().Before(deadline) {
		if consumer.Stats().Connections > 0 {
			response, err := http.Get(endpoint)
			if err == nil {
				var body struct {
					Channels []string `json:"channels"`
				}
				err = json.NewDecoder(response.Body).Decode(&body)
				response.Body.Close()
				if err == nil {
					for _, current := range body.Channels {
						if current == channel {
							return
						}
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	log.Fatal("consumer did not cross the channel creation barrier")
}

func missingLocked(state *ledger) uint64 {
	var missing uint64
	for sequence := range state.acknowledged {
		if state.consumed[sequence] == 0 {
			missing++
		}
	}
	return missing
}

func buildReport(state *ledger, topic string, duration time.Duration) report {
	state.Lock()
	defer state.Unlock()
	result := report{
		Topic:           topic,
		Acknowledged:    uint64(len(state.acknowledged)),
		Missing:         missingLocked(state),
		PublishErrors:   state.publishErrs,
		Unexpected:      state.unexpected,
		DurationSeconds: int64(duration.Seconds()),
	}
	for sequence, count := range state.consumed {
		if _, acknowledged := state.acknowledged[sequence]; acknowledged {
			result.ConsumedUnique++
			if count > 1 {
				result.Duplicate += count - 1
			}
		} else {
			result.UnconfirmedConsumed += count
		}
	}
	return result
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
