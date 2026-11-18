package gateway

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// A healthy HTTP/2 peer answers transport PINGs even while its model is silent.
// Connection health checks must not turn a slow generation into a timeout.
func TestGatewayHTTP2HealthPreservesSilentHealthyStream(t *testing.T) {
	release := make(chan struct{}, 1)
	started := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("protocol=%s, want HTTP/2", r.Proto)
		}
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-release:
			_, _ = io.WriteString(w, "real model output")
		case <-r.Context().Done():
		}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	defer close(release)

	transport := &http.Transport{TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig.Clone()}
	h2 := configureGatewayHTTP2(transport)
	if h2 == nil {
		t.Fatal("HTTP/2 health configuration failed")
	}
	h2.ReadIdleTimeout = 50 * time.Millisecond
	h2.PingTimeout = 500 * time.Millisecond
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-started
	result := make(chan error, 1)
	go func() {
		_, err := io.ReadAll(resp.Body)
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("healthy quiet stream ended before model output: %v", err)
	case <-time.After(3 * h2.PingTimeout):
	}
	// Use a separate signal rather than closing release twice in cleanup.
	release <- struct{}{}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("healthy HTTP/2 stream: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not complete")
	}
}

// Exercise a real HTTP/2 connection, including the state in which peer writes
// stop entirely. No request cancellation may be needed to remove the dead conn.
func TestGatewayHTTP2HealthClosesUnresponsivePeer(t *testing.T) {
	clientConn, peer := net.Pipe()
	defer clientConn.Close()
	defer peer.Close()
	// Use the production HTTP/2 health configuration with a shorter test clock.
	h2 := configureGatewayHTTP2(&http.Transport{})
	if h2 == nil {
		t.Fatal("HTTP/2 health configuration failed")
	}
	h2.ReadIdleTimeout = 20 * time.Millisecond
	h2.PingTimeout = 100 * time.Millisecond
	h2.WriteByteTimeout = 100 * time.Millisecond
	serverDone := make(chan error, 1)
	pingSeen := make(chan struct{}, 1)
	go func() {
		preface := make([]byte, len(http2.ClientPreface))
		if _, err := io.ReadFull(peer, preface); err != nil {
			serverDone <- err
			return
		}
		framer := http2.NewFramer(peer, peer)
		// Read client SETTINGS and WINDOW_UPDATE before sending server SETTINGS;
		// net.Pipe has no kernel send buffer to break a simultaneous write.
		for i := 0; i < 2; i++ {
			if _, err := framer.ReadFrame(); err != nil {
				serverDone <- err
				return
			}
		}
		if err := framer.WriteSettings(); err != nil {
			serverDone <- err
			return
		}
		for {
			frame, err := framer.ReadFrame()
			if err != nil {
				serverDone <- nil
				return
			}
			if ping, ok := frame.(*http2.PingFrame); ok && !ping.IsAck() {
				select {
				case pingSeen <- struct{}{}:
				default:
				}
				// Deliberately never acknowledge PING.
			}
		}
	}()
	cc, err := h2.NewClientConn(clientConn)
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Close()
	select {
	case <-pingSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("unresponsive peer was never probed")
	}
	select {
	case err := <-serverDone:
		if err != nil {
			t.Fatalf("peer setup: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("unresponsive HTTP/2 connection was not closed")
	}
	if cc.CanTakeNewRequest() {
		t.Fatal("unresponsive HTTP/2 connection still accepts requests")
	}
}

// A peer that stops reading must not leave a writer (including a canceled
// stream's RST_STREAM) blocked indefinitely while holding the connection lock.
func TestGatewayHTTP2HealthBoundsBlockedWrites(t *testing.T) {
	clientConn, peer := net.Pipe()
	defer clientConn.Close()
	defer peer.Close()
	h2 := configureGatewayHTTP2(&http.Transport{})
	if h2 == nil {
		t.Fatal("HTTP/2 health configuration failed")
	}
	h2.WriteByteTimeout = 50 * time.Millisecond
	result := make(chan error, 1)
	go func() {
		cc, err := h2.NewClientConn(clientConn)
		if cc != nil {
			_ = cc.Close()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("writing to an unread peer unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("HTTP/2 write never timed out")
	}
}

func TestGatewayModelQueriesUseSeparateConnections(t *testing.T) {
	var connections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"id":"model"}]}`)
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	defer server.Close()
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	defer svc.CloseIdleConnections()
	resp, err := svc.httpClientForTarget(nil, nil).Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
		if _, _, err := svc.runtime().readUpstreamModels(req, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections=%d, want one forwarding connection and one reused model connection", got)
	}
}
