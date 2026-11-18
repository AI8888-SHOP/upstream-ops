package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	modelsQueryTimeout        = 30 * time.Second
	modelsQueryAttemptTimeout = modelsQueryTimeout / 2
)

func (rt *Runtime) modelsTransportKey(req *http.Request, ch *storage.Channel, provider *storage.GatewayProvider) gatewayHTTPTransportKey {
	return gatewayHTTPTransportKey{
		proxy:        rt.proxyURLForTarget(ch, provider),
		modelsOrigin: strings.ToLower(req.URL.Scheme + "://" + req.URL.Host),
	}
}

// Only model catalog GETs may be replayed here. Generation requests retain
// their normal attempt/failover accounting and are never retried by this layer.
// Each HTTP attempt still passes through the API audit transport.
func (rt *Runtime) readUpstreamModels(req *http.Request, ch *storage.Channel, provider *storage.GatewayProvider) (int, []byte, error) {
	if req.Method != http.MethodGet || (req.Body != nil && req.Body != http.NoBody) {
		return 0, nil, fmt.Errorf("model catalog requests must be bodyless GETs")
	}
	ctx, cancel := context.WithTimeout(req.Context(), modelsQueryTimeout)
	defer cancel()
	key := rt.modelsTransportKey(req, ch, provider)
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return 0, nil, fmt.Errorf("GET %s: %w", req.URL, err)
		}
		attemptCtx, cancelAttempt := context.WithTimeout(ctx, modelsQueryAttemptTimeout)
		transport := rt.httpTransportForKey(key)
		client := rt.httpClientWithTransport(transport, ch, provider, 0)
		resp, err := client.Do(req.Clone(attemptCtx))
		var body []byte
		var status int
		if err == nil {
			status = resp.StatusCode
			body, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		}
		// Cancel before Close: an incomplete HTTP/2 body may otherwise wait for
		// stream cleanup even though this attempt no longer needs the response.
		cancelAttempt()
		if resp != nil && resp.Body != nil {
			closeErr := resp.Body.Close()
			if err == nil {
				err = closeErr
			}
		}
		// A client cancellation is not evidence that an upstream pool is bad.
		// Per-attempt timeouts and transport/body failures do retire that pool,
		// including after the final attempt, so the next caller starts fresh.
		if err != nil {
			if req.Context().Err() != nil {
				return 0, nil, fmt.Errorf("GET %s: %w", req.URL, err)
			}
			rt.retireHTTPTransport(key, transport)
		}
		// Preserve an upstream HTTP error without replaying it, even when its
		// body was truncated. A broken body still retires the failed pool above.
		if status >= http.StatusBadRequest {
			return status, nil, fmt.Errorf("GET %s: %w", req.URL, connector.HTTPStatusError(status, body))
		}
		if err == nil {
			return status, body, nil
		}
		if attempt == 1 || ctx.Err() != nil {
			return 0, nil, fmt.Errorf("GET %s: %w", req.URL, err)
		}
	}
}
