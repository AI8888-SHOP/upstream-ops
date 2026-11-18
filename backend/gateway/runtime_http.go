// 数据面：代理、超时与按目标构建 HTTP Client。
package gateway

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
	"golang.org/x/net/http2"
)

const (
	gatewayHTTP2ReadIdleTimeout  = 30 * time.Second
	gatewayHTTP2PingTimeout      = 10 * time.Second
	gatewayHTTP2WriteByteTimeout = 15 * time.Second
)

type gatewayHTTPTransportKey struct {
	proxy string
	// Empty for generation traffic; otherwise the model query's scheme+host.
	modelsOrigin string
}

func (rt *Runtime) gatewayRuntime() config.GatewayConfig {
	rt.mu.RLock()
	cfg := rt.gatewayCfg
	rt.mu.RUnlock()
	return cfg.WithDefaults()
}

func (rt *Runtime) proxyURLForChannel(ch *storage.Channel) string {
	return rt.proxyURLForTarget(ch, nil)
}

// proxyURLForTarget 监控渠道或直连 Provider 任一方开启 proxy_enabled，且全局代理启用时返回代理 URL。

func (rt *Runtime) proxyURLForTarget(ch *storage.Channel, provider *storage.GatewayProvider) string {
	rt.mu.RLock()
	pc := rt.proxyConfig
	rt.mu.RUnlock()
	if !pc.Enabled {
		return ""
	}
	useProxy := false
	if ch != nil && ch.ProxyEnabled {
		useProxy = true
	}
	if provider != nil && provider.ProxyEnabled {
		useProxy = true
	}
	if !useProxy {
		return ""
	}
	u, err := pc.ActiveURL()
	if err != nil {
		return ""
	}
	return u
}

func (rt *Runtime) httpClientForChannel(ch *storage.Channel) *http.Client {
	return rt.httpClientForTarget(ch, nil)
}

func (rt *Runtime) httpClientForTarget(ch *storage.Channel, provider *storage.GatewayProvider) *http.Client {
	// 网关转发超时由 gateway.forwardTimeoutSeconds 控制（与监控上游 timeout 分离）。
	timeout := rt.gatewayRuntime().ForwardTimeout()
	proxy := rt.proxyURLForTarget(ch, provider)
	transport := rt.httpTransportForProxy(proxy)
	return rt.httpClientWithTransport(transport, ch, provider, timeout)
}

func (rt *Runtime) httpClientWithTransport(transport *http.Transport, ch *storage.Channel, provider *storage.GatewayProvider, timeout time.Duration) *http.Client {
	var outbound http.RoundTripper = transport
	if rt.Usage != nil {
		meta := storage.GatewayAPICallLog{}
		if ch != nil {
			meta.ChannelID = ch.ID
		}
		if provider != nil {
			meta.GatewayProviderID = provider.ID
		}
		outbound = apiaudit.WrapTransport(outbound, apiaudit.Options{Store: rt.Usage.APICalls(), Purpose: storage.GatewayAPICallPurposeGateway, Metadata: meta, Log: rt.Log, OnFinish: rt.finishAPICallCost, ParseUsage: auditModelUsage})
	}
	return &http.Client{Timeout: timeout, Transport: outbound}
}

func (rt *Runtime) httpTransportForProxy(proxy string) *http.Transport {
	return rt.httpTransportForKey(gatewayHTTPTransportKey{proxy: proxy})
}

func (rt *Runtime) httpTransportForKey(key gatewayHTTPTransportKey) *http.Transport {
	if rt == nil || rt.Service == nil {
		return newGatewayHTTPTransport(key.proxy)
	}
	s := rt.Service
	s.httpTransportsMu.Lock()
	defer s.httpTransportsMu.Unlock()
	if s.httpTransports == nil {
		s.httpTransports = make(map[gatewayHTTPTransportKey]*http.Transport)
	}
	if transport := s.httpTransports[key]; transport != nil {
		return transport
	}
	transport := newGatewayHTTPTransport(key.proxy)
	s.httpTransports[key] = transport
	return transport
}

// Retire only the pool that actually failed. Concurrent failures from an old
// pool must not remove a replacement installed by another request. Existing
// requests may finish; new model queries will establish a fresh connection.
func (rt *Runtime) retireHTTPTransport(key gatewayHTTPTransportKey, failed *http.Transport) {
	if failed == nil {
		return
	}
	if rt != nil && rt.Service != nil {
		s := rt.Service
		s.httpTransportsMu.Lock()
		if s.httpTransports[key] == failed {
			delete(s.httpTransports, key)
		}
		s.httpTransportsMu.Unlock()
	}
	failed.CloseIdleConnections()
}

func (s *Service) invalidateHTTPTransports() {
	if s == nil {
		return
	}
	s.httpTransportsMu.Lock()
	transports := s.httpTransports
	s.httpTransports = make(map[gatewayHTTPTransportKey]*http.Transport)
	s.httpTransportsMu.Unlock()
	for _, transport := range transports {
		if transport != nil {
			transport.CloseIdleConnections()
		}
	}
}

func newGatewayHTTPTransport(proxy string) *http.Transport {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ForceAttemptHTTP2:     true,
	}
	if proxy != "" {
		if u, err := url.Parse(proxy); err == nil {
			transport.Proxy = http.ProxyURL(u)
		}
	}
	configureGatewayHTTP2(transport)
	return transport
}

// A request timeout only cancels its HTTP/2 stream. It does not necessarily
// close the shared TCP connection, especially when RST_STREAM writes are
// themselves blocked. Detect missing frames using PING, and bound stalled
// writes so a dead peer cannot poison the connection pool indefinitely.
// These are connection health deadlines, not model first-token deadlines:
// a healthy peer can keep a long-running generation alive by answering PING.
func configureGatewayHTTP2(transport *http.Transport) *http2.Transport {
	h2, err := http2.ConfigureTransports(transport)
	if err != nil {
		// Only possible if a caller configures the transport twice. Fall back to
		// HTTP/1.1 rather than silently enabling HTTP/2 without health checks.
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
		if transport.TLSClientConfig != nil {
			transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
		}
		return nil
	}
	h2.ReadIdleTimeout = gatewayHTTP2ReadIdleTimeout
	h2.PingTimeout = gatewayHTTP2PingTimeout
	h2.WriteByteTimeout = gatewayHTTP2WriteByteTimeout
	return h2
}

// ---------- key helpers ----------
