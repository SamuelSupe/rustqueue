package nsq

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRustQueueStatsReplay(t *testing.T) {
	fixtureDir := os.Getenv("RUSTQUEUE_REPLAY_FIXTURES")
	if fixtureDir == "" {
		t.Fatal("RUSTQUEUE_REPLAY_FIXTURES is required")
	}

	var nodeDeletes atomic.Int32
	nodes := make([]*httptest.Server, 0, 3)
	for index := 1; index <= 3; index++ {
		stats := mustReadFixture(t, filepath.Join(fixtureDir, fmt.Sprintf("stats-%d.json", index)))
		missingStats := mustReadFixture(t, filepath.Join(fixtureDir, "stats-missing.json"))
		node := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case r.Method == http.MethodGet && r.URL.Path == "/stats":
				if r.URL.Query().Get("format") != "json" ||
					r.URL.Query().Get("include_clients") != "false" ||
					r.URL.Query().Get("topic") != "events" {
					http.Error(w, "unexpected stats query", http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Query().Get("channel") {
				case "", "workers":
					_, _ = w.Write(stats)
				case "missing":
					_, _ = w.Write(missingStats)
				default:
					http.Error(w, "unexpected channel", http.StatusBadRequest)
				}
			case r.Method == http.MethodPost && r.URL.Path == "/channel/delete":
				nodeDeletes.Add(1)
				http.Error(w, "E_NOT_FOUND Kodo cleanup compatibility is disabled", http.StatusNotFound)
			default:
				http.NotFound(w, r)
			}
		}))
		_ = node.Listener.Close()
		listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.%d:0", index))
		if err != nil {
			t.Fatal(err)
		}
		node.Listener = listener
		node.Start()
		nodes = append(nodes, node)
		t.Cleanup(node.Close)
	}

	producers := make([]map[string]any, 0, len(nodes))
	for index, node := range nodes {
		endpoint, err := url.Parse(node.URL)
		if err != nil {
			t.Fatal(err)
		}
		port, err := strconv.Atoi(endpoint.Port())
		if err != nil {
			t.Fatal(err)
		}
		producers = append(producers, map[string]any{
			"remote_address":    fmt.Sprintf("%s:4150", endpoint.Hostname()),
			"hostname":          fmt.Sprintf("rustqueue-broker-%d", index),
			"broadcast_address": endpoint.Hostname(),
			"tcp_port":          4150,
			"http_port":         port,
			"version":           "0.8.0",
			"topics":            []string{"events"},
			"tombstones":        []bool{false},
		})
	}

	var lookupDelete atomic.Int32
	lookup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/nodes":
			writeReplayJSON(w, map[string]any{"producers": producers})
		case r.Method == http.MethodGet && r.URL.Path == "/topics":
			writeReplayJSON(w, map[string]any{"topics": []string{"events"}})
		case r.Method == http.MethodGet && r.URL.Path == "/channels":
			writeReplayJSON(w, map[string]any{"channels": []string{"workers"}})
		case r.Method == http.MethodGet && r.URL.Path == "/lookup":
			writeReplayJSON(w, map[string]any{
				"channels":  []string{"workers"},
				"producers": producers,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/channel/delete":
			lookupDelete.Add(1)
			http.Error(w, "E_NOT_FOUND Kodo cleanup compatibility is disabled", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(lookup.Close)

	admin := NewNsqAdmin(lookup.URL, 5*time.Second)
	topicNodes, err := admin.GetTopicNodes("events")
	if err != nil {
		t.Fatalf("Kodo lookup replay failed: %v", err)
	}
	if len(topicNodes.Producers) != 3 {
		t.Fatalf("Kodo discovered %d topic owners, want 3", len(topicNodes.Producers))
	}
	for _, producer := range topicNodes.Producers {
		if !strings.HasPrefix(producer.Hostname, "rustqueue-broker-") {
			t.Fatalf("Kodo discovered an unexpected topic owner: %q", producer.Hostname)
		}
	}

	stats, err := admin.GetChannelStats("events", "workers")
	if err != nil {
		t.Fatalf("Kodo channel stats replay failed: %v", err)
	}
	assertReplayChannelStats(t, stats)

	byChannel, err := admin.GetTopicStats("events")
	if err != nil {
		t.Fatalf("Kodo topic stats replay failed: %v", err)
	}
	assertReplayChannelStats(t, byChannel["workers"])

	deleted, err := admin.DeleteChannels("events", []string{"workers"})
	if err == nil || !strings.Contains(err.Error(), "cleanup compatibility is disabled") {
		t.Fatalf("Kodo disabled-cleanup error = %v", err)
	}
	if len(deleted) != 0 || lookupDelete.Load() != 1 || nodeDeletes.Load() != 0 {
		t.Fatalf(
			"Kodo disabled cleanup returned deleted=%v lookup=%d nodes=%d",
			deleted,
			lookupDelete.Load(),
			nodeDeletes.Load(),
		)
	}
}

func assertReplayChannelStats(t *testing.T, stats *ChannelStats) {
	t.Helper()
	if stats == nil {
		t.Fatal("Kodo did not match channel_name=workers")
	}
	if stats.TopicName != "events" || stats.ChannelName != "workers" ||
		stats.Depth != 15 || stats.MemoryDepth != 0 || stats.BackendDepth != 15 ||
		stats.MessageCount != 60 || stats.InFlightCount != 3 ||
		stats.DeferredCount != 6 || stats.RequeueCount != 15 ||
		stats.TimeoutCount != 18 || stats.ClientCount != 0 {
		t.Fatalf("unexpected Kodo aggregate: %+v", stats)
	}
}

func mustReadFixture(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func writeReplayJSON(w http.ResponseWriter, value any) {
	if err := json.NewEncoder(w).Encode(value); err != nil {
		panic(err)
	}
}
