package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestModelQueriesReplaceTimedOutPoolAndAuditEachAttempt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer models-secret" || r.Header.Get("User-Agent") != "models-agent" {
			t.Error("model query lost upstream credentials or user-agent during retry")
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"recovered-model"}]}`)
	}))
	defer server.Close()
	db := openGatewayTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	svc := NewService(nil, nil, nil, usage, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	rt := svc.runtime()
	channel := &storage.Channel{ID: 42, SiteURL: server.URL}
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
	key := rt.modelsTransportKey(req, channel, nil)
	failed := rt.httpTransportForKey(key)
	var dials atomic.Int32
	failed.DialContext = func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, context.DeadlineExceeded
	}
	forwarding := rt.httpTransportForProxy("")
	models, err := rt.fetchUpstreamModels(context.Background(), channel, "models-secret", "models-agent")
	if err != nil || len(models) != 1 || models[0] != "recovered-model" {
		t.Fatalf("models=%v err=%v", models, err)
	}
	if dials.Load() != 1 || rt.httpTransportForKey(key) == failed {
		t.Fatal("timed-out model pool was reused")
	}
	if rt.httpTransportForProxy("") != forwarding {
		t.Fatal("model retry retired the generation pool")
	}
	page, err := usage.APICalls().List(storage.GatewayAPICallLogQuery{ChannelID: channel.ID})
	if err != nil || page.Total != 2 {
		t.Fatalf("audit page=%+v err=%v", page, err)
	}
	statuses := map[string]int{}
	for _, item := range page.Items {
		statuses[item.Status]++
		if item.Purpose != storage.GatewayAPICallPurposeChannelAPI || item.Endpoint != "/v1/models" {
			t.Fatalf("wrong model attempt attribution: %+v", item)
		}
	}
	if statuses[storage.GatewayAPICallStatusError] != 1 || statuses[storage.GatewayAPICallStatusSuccess] != 1 {
		t.Fatalf("retry audit statuses=%v", statuses)
	}
}

func TestModelQueriesRetryTruncatedBodiesForBothSources(t *testing.T) {
	for _, source := range []string{"monitor", "provider"} {
		t.Run(source, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if source == "provider" {
					if r.Header.Get("X-API-Key") != "models-secret" || r.Header.Get("X-Models-Test") != "preserved" || r.Header.Get("Authorization") != "" {
						t.Error("provider auth or extra headers changed during retry")
					}
				}
				if requests.Add(1) == 1 {
					w.Header().Set("Content-Length", "1000")
					_, _ = io.WriteString(w, `{"data":[`)
					return
				}
				_, _ = io.WriteString(w, `{"data":[{"id":"recovered-model"}]}`)
			}))
			defer server.Close()
			svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer svc.CloseIdleConnections()
			var models []string
			var err error
			if source == "provider" {
				provider := &storage.GatewayProvider{BaseURL: server.URL, AuthStyle: storage.GatewayProviderAuthXAPIKey, ExtraHeadersJSON: `{"X-Models-Test":"preserved"}`}
				models, err = svc.fetchProviderModels(context.Background(), provider, "models-secret", "models-agent")
			} else {
				models, err = svc.fetchUpstreamModels(context.Background(), &storage.Channel{SiteURL: server.URL}, "models-secret", "models-agent")
			}
			if err != nil || len(models) != 1 || models[0] != "recovered-model" || requests.Load() != 2 {
				t.Fatalf("models=%v requests=%d err=%v", models, requests.Load(), err)
			}
		})
	}
}

func TestModelQueriesDoNotRetryHTTPFailures(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"error":{"message":"upstream denied"}}`)
			}))
			defer server.Close()
			svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
			defer svc.CloseIdleConnections()
			_, err := svc.fetchUpstreamModels(context.Background(), &storage.Channel{SiteURL: server.URL}, "secret", "")
			if err == nil || !strings.Contains(err.Error(), "upstream denied") || requests.Load() != 1 {
				t.Fatalf("requests=%d err=%v", requests.Load(), err)
			}
		})
	}
}

func TestModelQueriesRetireTruncatedHTTPErrorWithoutRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Length", "1000")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"message":"upstream denied"}}`)
	}))
	defer server.Close()
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	rt := svc.runtime()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
	key := rt.modelsTransportKey(req, nil, nil)
	pool := rt.httpTransportForKey(key)
	status, _, err := rt.readUpstreamModels(req, nil, nil)
	if err == nil || status != http.StatusUnauthorized || !strings.Contains(err.Error(), "upstream denied") || requests.Load() != 1 {
		t.Fatalf("status=%d requests=%d err=%v", status, requests.Load(), err)
	}
	if rt.httpTransportForKey(key) == pool {
		t.Fatal("truncated HTTP error left the broken model pool available")
	}
}

func TestModelQueriesDoNotRetryInvalidCatalog(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, `{"data":[`)
	}))
	defer server.Close()
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	_, err := svc.fetchUpstreamModels(context.Background(), &storage.Channel{SiteURL: server.URL}, "secret", "")
	if err == nil || !strings.Contains(err.Error(), "invalid models JSON") || requests.Load() != 1 {
		t.Fatalf("requests=%d err=%v", requests.Load(), err)
	}
}

func TestModelQueryCancellationDoesNotRetireHealthyPool(t *testing.T) {
	for _, flushedHeaders := range []bool{false, true} {
		t.Run(map[bool]string{false: "waiting_for_headers", true: "reading_body"}[flushedHeaders], func(t *testing.T) {
			testModelQueryCancellation(t, flushedHeaders)
		})
	}
}

func testModelQueryCancellation(t *testing.T, flushedHeaders bool) {
	t.Helper()
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if flushedHeaders {
			_, _ = io.WriteString(w, `{"data":[`)
			w.(http.Flusher).Flush()
		}
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	rt := svc.runtime()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v1/models", nil)
	key := rt.modelsTransportKey(req, nil, nil)
	pool := rt.httpTransportForKey(key)
	result := make(chan error, 1)
	go func() {
		_, _, err := rt.readUpstreamModels(req, nil, nil)
		result <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("query did not reach upstream")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled query: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("model query ignored client cancellation")
	}
	if rt.httpTransportForKey(key) != pool {
		t.Fatal("client cancellation retired a healthy model pool")
	}
}

func TestModelQueriesStopAfterOneFreshConnectionRetry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	}))
	defer server.Close()
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	rt := svc.runtime()
	req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
	key := rt.modelsTransportKey(req, nil, nil)
	_, _, err := rt.readUpstreamModels(req, nil, nil)
	if err == nil || requests.Load() != 2 {
		t.Fatalf("requests=%d err=%v, want exactly two failed attempts", requests.Load(), err)
	}
	svc.httpTransportsMu.Lock()
	remaining := svc.httpTransports[key]
	svc.httpTransportsMu.Unlock()
	if remaining != nil {
		t.Fatal("final failed model pool was left available for the next caller")
	}
}

func TestRetiringOldModelPoolPreservesReplacementAndOtherOrigins(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	rt := svc.runtime()
	key := gatewayHTTPTransportKey{modelsOrigin: "https://models.example"}
	other := gatewayHTTPTransportKey{modelsOrigin: "https://other.example"}
	old, healthy := rt.httpTransportForKey(key), rt.httpTransportForKey(other)
	rt.retireHTTPTransport(key, old)
	replacement := rt.httpTransportForKey(key)
	rt.retireHTTPTransport(key, old)
	if rt.httpTransportForKey(key) != replacement || rt.httpTransportForKey(other) != healthy {
		t.Fatal("retiring an old model pool removed a replacement or another origin")
	}
	svc.UpdateProxyConfig(config.ProxyConfig{Enabled: true, Protocol: "http", Host: "127.0.0.1", Port: 7890})
	if rt.httpTransportForKey(key) == replacement || rt.httpTransportForKey(other) == healthy {
		t.Fatal("proxy configuration change left old model pools alive")
	}
}
