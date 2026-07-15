package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/nsqio/go-nsq"
)

func runSecure(address, httpAddress, caPath, certPath, keyPath, serverName string) error {
	factory, err := secureFactory(caPath, certPath, keyPath, serverName, "compat-secret")
	if err != nil {
		return err
	}
	if err := testRoundTrip(address, httpAddress, "tls_auth", factory); err != nil {
		return err
	}
	for name, test := range map[string]func(string, string, configFactory) error{
		"secure mpub+dpub": testBatchAndDeferred,
		"secure req":       testRequeue,
		"secure touch":     testTouch,
		"secure fanout":    testFanout,
		"secure ephemeral": testEphemeral,
	} {
		if err := test(address, httpAddress, factory); err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
	}
	producer, err := quietProducer(address, factory())
	if err != nil {
		return err
	}
	refreshTopic := uniqueTopic("auth_refresh")
	if err := producer.Publish(refreshTopic, []byte("before-expiry")); err != nil {
		producer.Stop()
		return err
	}
	time.Sleep(1200 * time.Millisecond)
	if err := producer.Publish(refreshTopic, []byte("after-expiry")); err != nil {
		producer.Stop()
		return fmt.Errorf("AUTH TTL refresh failed: %w", err)
	}
	producer.Stop()
	badFactory, err := secureFactory(caPath, certPath, keyPath, serverName, "wrong-secret")
	if err != nil {
		return err
	}
	producer, err = quietProducer(address, badFactory())
	if err != nil {
		return err
	}
	defer producer.Stop()
	if err := producer.Publish(uniqueTopic("denied"), []byte("denied")); err == nil {
		return fmt.Errorf("AUTH accepted an invalid secret")
	}
	return nil
}

func secureFactory(caPath, certPath, keyPath, serverName, secret string) (configFactory, error) {
	ca, err := os.ReadFile(caPath)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("failed to parse CA")
	}
	certificate, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return func() *nsq.Config {
		config := baseConfig()
		config.TlsV1 = true
		config.TlsConfig = &tls.Config{
			MinVersion:   tls.VersionTLS12,
			RootCAs:      roots,
			Certificates: []tls.Certificate{certificate},
			ServerName:   serverName,
		}
		config.AuthSecret = secret
		config.Deflate = true
		return config
	}, nil
}
