// 数据面：代理、超时与按目标构建 HTTP Client。
package gateway

import (
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

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
	return &http.Client{Timeout: timeout, Transport: transport}
}

func (rt *Runtime) httpTransportForProxy(proxy string) *http.Transport {
	if rt == nil || rt.Service == nil {
		return newGatewayHTTPTransport(proxy)
	}
	s := rt.Service
	s.httpTransportsMu.Lock()
	defer s.httpTransportsMu.Unlock()
	if s.httpTransports == nil {
		s.httpTransports = make(map[string]*http.Transport)
	}
	if transport := s.httpTransports[proxy]; transport != nil {
		return transport
	}
	transport := newGatewayHTTPTransport(proxy)
	s.httpTransports[proxy] = transport
	return transport
}

func (s *Service) invalidateHTTPTransports() {
	if s == nil {
		return
	}
	s.httpTransportsMu.Lock()
	transports := s.httpTransports
	s.httpTransports = make(map[string]*http.Transport)
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
	return transport
}

// ---------- key helpers ----------
