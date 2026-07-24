package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/nsqio/go-nsq"
)

func main() {
	mode := argument(1, "core")
	if mode == "fake-discovery" {
		if err := runFakeDiscovery(); err != nil {
			log.Fatal(err)
		}
		return
	}
	if mode == "kodo-gateway" {
		if len(os.Args) != 4 {
			log.Fatal("kodo-gateway mode requires lookup HTTP and Gateway metrics addresses")
		}
		if err := runKodoGatewayAcceptance(os.Args[2], os.Args[3]); err != nil {
			log.Fatal(err)
		}
		fmt.Println("Kodo Gateway maximum-size PUB/DPUB and Broker failover acceptance: ok")
		return
	}
	if mode == "operational-ledger" {
		if len(os.Args) != 6 {
			log.Fatal("operational-ledger mode requires proxy HTTP, lookup HTTP, duration seconds, and minimum brokers")
		}
		durationSeconds, err := strconv.Atoi(os.Args[4])
		if err != nil || durationSeconds <= 0 {
			log.Fatal("operational ledger duration must be a positive integer")
		}
		minimumBrokers, err := strconv.Atoi(os.Args[5])
		if err != nil || minimumBrokers <= 0 {
			log.Fatal("operational ledger minimum brokers must be a positive integer")
		}
		if err := runOperationalLedger(
			os.Args[2], os.Args[3], time.Duration(durationSeconds)*time.Second, minimumBrokers,
		); err != nil {
			log.Fatal(err)
		}
		return
	}
	if mode == "ledger" {
		if len(os.Args) != 7 {
			log.Fatal("ledger mode requires lookup, topic, channel, expected file, and timeout seconds")
		}
		timeoutSeconds, err := strconv.Atoi(os.Args[6])
		if err != nil || timeoutSeconds <= 0 {
			log.Fatal("ledger timeout must be a positive integer")
		}
		if err := runLedger(
			os.Args[2],
			os.Args[3],
			os.Args[4],
			os.Args[5],
			time.Duration(timeoutSeconds)*time.Second,
		); err != nil {
			log.Fatal(err)
		}
		return
	}
	if mode == "lookup" {
		if len(os.Args) != 5 {
			log.Fatal("lookup mode requires producer TCP, management HTTP, and lookup HTTP addresses")
		}
		if err := testLookupEndpoint(os.Args[2], os.Args[3], os.Args[4], func() *nsq.Config {
			return baseConfig()
		}); err != nil {
			log.Fatal(err)
		}
		fmt.Println("go discovery lookup compatibility: ok")
		return
	}
	if mode == "lookup-default-bootstrap" {
		if len(os.Args) != 5 {
			log.Fatal("lookup-default-bootstrap mode requires lookup HTTP, seed Broker HTTP, and new-owner Broker HTTP addresses")
		}
		if err := runDefaultLookupBootstrap(os.Args[2], os.Args[3], os.Args[4]); err != nil {
			log.Fatal(err)
		}
		fmt.Println("go default lookup bootstrap retention: ok")
		return
	}
	if mode == "proxy-rotation" {
		if len(os.Args) != 4 {
			log.Fatal("proxy-rotation mode requires proxy TCP and lookup HTTP addresses")
		}
		if err := runProxyRotation(os.Args[2], os.Args[3]); err != nil {
			log.Fatal(err)
		}
		fmt.Println("go producer proxy connection rotation: ok")
		return
	}
	if mode == "consume-one" {
		if len(os.Args) != 6 {
			log.Fatal("consume-one mode requires NSQD address, topic, channel, and expected body")
		}
		if err := consumeOne(os.Args[2], os.Args[3], os.Args[4], os.Args[5]); err != nil {
			log.Fatal(err)
		}
		fmt.Println("go durable backlog recovery: ok")
		return
	}
	address := argument(2, "rustqueue-plain:4150")
	httpAddress := argument(3, "rustqueue-plain:4151")

	var err error
	switch mode {
	case "core":
		err = runCore(address, httpAddress)
	case "secure":
		if len(os.Args) != 8 {
			log.Fatal("secure mode requires CA, client certificate, client key, and server name")
		}
		err = runSecure(address, httpAddress, os.Args[4], os.Args[5], os.Args[6], os.Args[7])
	default:
		err = fmt.Errorf("unknown mode %q", mode)
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("go %s compatibility matrix: ok\n", mode)
}

func argument(index int, fallback string) string {
	if len(os.Args) > index {
		return os.Args[index]
	}
	return fallback
}
