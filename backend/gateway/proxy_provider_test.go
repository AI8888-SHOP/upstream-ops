package gateway

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestProxyURLForTarget_Provider(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "http",
		Host:     "127.0.0.1",
		Port:     7890,
	})

	if got := svc.proxyURLForTarget(nil, nil); got != "" {
		t.Fatalf("no source should skip proxy, got %q", got)
	}
	if got := svc.proxyURLForTarget(nil, &storage.GatewayProvider{ProxyEnabled: false}); got != "" {
		t.Fatalf("provider proxy off: got %q", got)
	}
	got := svc.proxyURLForTarget(nil, &storage.GatewayProvider{ProxyEnabled: true})
	if got == "" {
		t.Fatal("provider proxy on: empty url")
	}
	u, err := url.Parse(got)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Port() != "7890" {
		t.Fatalf("proxy url = %q err=%v", got, err)
	}

	// 监控渠道仍可用
	chGot := svc.proxyURLForChannel(&storage.Channel{ProxyEnabled: true})
	if chGot == "" {
		t.Fatal("channel proxy on: empty")
	}

	// 全局关
	svc.UpdateProxyConfig(config.ProxyConfig{Enabled: false, Protocol: "http", Host: "127.0.0.1", Port: 7890})
	if got := svc.proxyURLForTarget(nil, &storage.GatewayProvider{ProxyEnabled: true}); got != "" {
		t.Fatalf("global off should skip, got %q", got)
	}
}

func TestHTTPClientForTarget_UsesProviderProxy(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "http",
		Host:     "10.0.0.2",
		Port:     1080,
	})
	client := svc.httpClientForTarget(nil, &storage.GatewayProvider{ProxyEnabled: true})
	tr, ok := client.Transport.(*http.Transport)
	if !ok || tr.Proxy == nil {
		t.Fatal("expected transport with proxy func")
	}
	req, _ := http.NewRequest(http.MethodGet, "https://example.com", nil)
	proxyURL, err := tr.Proxy(req)
	if err != nil || proxyURL == nil || proxyURL.Hostname() != "10.0.0.2" {
		t.Fatalf("proxy = %v err=%v", proxyURL, err)
	}
}

func TestHTTPClientForTargetPoolsTransportsByEffectiveProxy(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	provider := &storage.GatewayProvider{ProxyEnabled: true}

	// Separate clients share a direct transport, preserving idle connections
	// while allowing the request timeout to remain per-client.
	direct1 := svc.httpClientForTarget(nil, nil)
	direct2 := svc.httpClientForTarget(nil, nil)
	transportDirect, ok := direct1.Transport.(*http.Transport)
	if !ok || transportDirect == nil {
		t.Fatal("direct client did not use an http.Transport")
	}
	if transportDirect != direct2.Transport {
		t.Fatal("direct clients should share a transport")
	}

	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "http",
		Host:     "127.0.0.1",
		Port:     7890,
	})
	proxy1 := svc.httpClientForTarget(nil, provider)
	proxy2 := svc.httpClientForTarget(nil, provider)
	transportProxy, ok := proxy1.Transport.(*http.Transport)
	if !ok || transportProxy == nil {
		t.Fatal("proxy client did not use an http.Transport")
	}
	if transportProxy != proxy2.Transport {
		t.Fatal("same proxy URL should share a transport")
	}
	if transportProxy == transportDirect {
		t.Fatal("direct and proxied clients must not share a transport")
	}

	// A proxy change drops the old pool. Active requests may still finish, but
	// subsequent clients must use a fresh transport.
	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "http",
		Host:     "127.0.0.1",
		Port:     7891,
	})
	proxy3 := svc.httpClientForTarget(nil, provider)
	if proxy3.Transport == transportProxy {
		t.Fatal("proxy change should invalidate the old transport")
	}

	svc.UpdateGatewayConfig(config.GatewayConfig{ForwardTimeoutSeconds: 3})
	shortTimeout := svc.httpClientForTarget(nil, provider)
	if shortTimeout.Transport != proxy3.Transport {
		t.Fatal("gateway timeout changes should not replace transports")
	}
	if shortTimeout.Timeout != 3*time.Second {
		t.Fatalf("client timeout=%s, want 3s", shortTimeout.Timeout)
	}
	svc.UpdateGatewayConfig(config.GatewayConfig{ForwardTimeoutSeconds: 7})
	longTimeout := svc.httpClientForTarget(nil, provider)
	if longTimeout.Transport != proxy3.Transport {
		t.Fatal("dynamic timeout update should keep the transport")
	}
	if longTimeout.Timeout != 7*time.Second {
		t.Fatalf("client timeout=%s, want 7s", longTimeout.Timeout)
	}

	svc.CloseIdleConnections()
	if afterClose := svc.httpClientForTarget(nil, provider); afterClose.Transport == proxy3.Transport {
		t.Fatal("CloseIdleConnections should invalidate the transport pool")
	}
}

func TestGatewayProviderProxyEnabledPersists(t *testing.T) {
	db := openGatewayTestDB(t)
	repo := storage.NewGatewayProviders(db)
	item := &storage.GatewayProvider{
		Name:         "p-proxy",
		BaseURL:      "https://api.example.com",
		APIKeyCipher: "x",
		ProxyEnabled: true,
		Enabled:      true,
	}
	if err := repo.Create(item); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := repo.FindByID(item.ID)
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if !got.ProxyEnabled {
		t.Fatal("proxy_enabled not persisted")
	}
}
