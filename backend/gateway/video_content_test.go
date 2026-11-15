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
)

func TestVideoContentURLAndDNSRestrictions(t *testing.T) {
	for _, raw := range []string{"file:///etc/passwd", "https://user:secret@video.test/a", "https://video.test:5432/a", "/relative", "https://video.test/a#fragment"} {
		if _, err := validateVideoContentURL(raw); err == nil {
			t.Fatalf("unsafe content URL accepted: %s", raw)
		}
	}
	for _, ip := range []string{"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.0.1", "169.254.169.254", "100.100.100.200", "0.0.0.0", "192.0.2.1", "168.63.129.16", "::1", "::ffff:127.0.0.1", "fd00::1", "fe80::1", "64:ff9b::7f00:1", "2002:7f00:1::"} {
		if publicVideoContentIP(net.ParseIP(ip)) {
			t.Fatalf("non-public address accepted: %s", ip)
		}
	}
	for _, ip := range []string{"8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"} {
		if !publicVideoContentIP(net.ParseIP(ip)) {
			t.Fatalf("public address blocked: %s", ip)
		}
	}
	var dialed string
	dial := func(_ context.Context, _, address string) (net.Conn, error) {
		dialed = address
		return nil, errors.New("test connection stopped")
	}
	lookup := func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}}, nil
	}
	_, _ = dialVideoContentAddress(context.Background(), "tcp", "video.test:443", lookup, dial)
	if dialed != "8.8.8.8:443" {
		t.Fatalf("dial did not pin the validated IP: %q", dialed)
	}
	dialed = ""
	lookup = func(_ context.Context, _ string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("8.8.8.8")}, {IP: net.ParseIP("10.0.0.1")}}, nil
	}
	if _, err := dialVideoContentAddress(context.Background(), "tcp", "video.test:443", lookup, dial); err == nil || dialed != "" {
		t.Fatal("mixed public/private DNS record reached a socket")
	}
}

func TestVideoContentStreamingAndRange(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "whole", true: "range"}[partial], func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" || r.Header.Get("X-Api-Key") != "" {
					t.Error("download leaked authentication headers")
				}
				w.Header().Set("Content-Type", "video/mp4")
				w.Header().Set("Accept-Ranges", "bytes")
				if partial {
					if r.Header.Get("Range") != "bytes=0-3" {
						t.Error("byte range was lost")
					}
					w.Header().Set("Content-Range", "bytes 0-3/8")
					w.WriteHeader(http.StatusPartialContent)
					_, _ = io.WriteString(w, "vide")
				} else {
					_, _ = io.WriteString(w, "videodata")
				}
			}))
			defer upstream.Close()
			// Test the streaming helper with a deterministic in-process peer;
			// production's separate DNS transport is tested above and below.
			transport := &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, strings.TrimPrefix(upstream.URL, "http://"))
			}}
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			w := httptest.NewRecorder()
			byteRange, want, status := "", "videodata", http.StatusOK
			if partial {
				byteRange, want, status = "bytes=0-3", "vide", http.StatusPartialContent
			}
			if err := streamVideoContent(context.Background(), w, client, "http://video.test/a.mp4", byteRange); err != nil {
				t.Fatal(err)
			}
			if w.Code != status || w.Body.String() != want || w.Header().Get("Content-Type") != "video/mp4" {
				t.Fatalf("video content corrupted: %d %s %v", w.Code, w.Body.String(), w.Header())
			}
		})
	}
}

func TestVideoContentPendingAndOwnership(t *testing.T) {
	var lookups atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			lookups.Add(1)
		}
		_, _ = io.WriteString(w, `{"id":"task","status":"queued"}`)
	})
	setVideoAdapter(t, f, 0)
	w := videoAdapterRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie","prompt":"hello"}`)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed: %d %s", w.Code, w.Body.String())
	}
	w = videoAdapterRequest(f.svc, http.MethodGet, "/v1/videos/task/content", "")
	if w.Code != http.StatusConflict || lookups.Load() != 1 {
		t.Fatalf("pending download: %d %s", w.Code, w.Body.String())
	}
	w = videoAdapterRequest(f.svc, http.MethodGet, "/v1/videos/unknown/content", "")
	if w.Code != http.StatusNotFound || lookups.Load() != 1 {
		t.Fatal("unknown content request reached upstream")
	}
	w = videoTestRequest(f.svc, http.MethodGet, "/v1/videos/task/content", "", "wrong-key")
	if w.Code != http.StatusUnauthorized || lookups.Load() != 1 {
		t.Fatal("unauthorized download reached upstream")
	}
}

func TestVideoContentClientRejectsPrivatePeer(t *testing.T) {
	client := newVideoContentClient()
	defer client.CloseIdleConnections()
	// Numeric loopback resolution needs no external DNS or actual listener.
	if resp, err := client.Get("http://127.0.0.1/video.mp4"); err == nil {
		_ = resp.Body.Close()
		t.Fatal("video content client accepted a private peer")
	}
}
