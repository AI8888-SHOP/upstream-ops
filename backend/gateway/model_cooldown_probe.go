package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// modelCooldownProbeResult is deliberately separate from gateway usage
// results. A probe is an operational health check: it never creates a local
// usage row, consumes a gateway key quota, or participates in virtual-cache
// settlement.
type modelCooldownProbeResult struct {
	ModelTestResult
	Headers    http.Header
	Permanent  bool
	RetryAfter time.Duration
}

// RunModelCooldownProbes is called by the application scheduler. It is safe to
// call frequently: the process mutex avoids overlapping local scans and the
// storage lease prevents duplicate work across processes.
func (s *Service) RunModelCooldownProbes(ctx context.Context) {
	if s == nil || s.Routes == nil || s.Groups == nil || ctx == nil {
		return
	}
	cfg := s.gatewayRuntime()
	if !cfg.ModelCooldownProbeEnabled {
		return
	}
	if !s.modelProbeMu.TryLock() {
		return
	}
	defer s.modelProbeMu.Unlock()

	concurrency := cfg.ModelCooldownProbeConcurrency
	if concurrency <= 0 {
		concurrency = config.DefaultGatewayModelCooldownProbeConcurrency
	}
	lease := time.Duration(cfg.ModelCooldownProbeTimeoutSeconds)*time.Second + 30*time.Second
	claims, err := s.Routes.ClaimDueModelCooldownProbes(time.Now(), concurrency, lease)
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("claim gateway model cooldown probes failed", "err", err)
		}
		return
	}
	if len(claims) == 0 {
		return
	}
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, claim := range claims {
		claim := claim
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				// A scheduler shutdown can happen after the row was leased but
				// before this goroutine acquired the local semaphore. Record a
				// transient result so the lease is released immediately instead
				// of waiting for its timeout.
				s.recordModelProbeFailureCurrentMode(claim, cfg, 0, ctx.Err().Error(), false, 0)
				return
			}
			s.runClaimedModelCooldownProbe(ctx, cfg, claim)
		}()
	}
	wg.Wait()
}

func (s *Service) runClaimedModelCooldownProbe(ctx context.Context, cfg config.GatewayConfig, claim storage.GatewayRouteModelCooldown) {
	route, err := s.Routes.FindByID(claim.RouteID)
	if err != nil {
		// A deleted route is normally removed together with this row, but a
		// transient read failure must still release the lease promptly.
		s.recordModelProbeFailureCurrentMode(claim, cfg, 0, err.Error(), false, 0)
		if s.Log != nil {
			s.Log.Debug("skip model cooldown probe for missing route", "route_id", claim.RouteID, "err", err)
		}
		return
	}
	if !route.Enabled || route.RateLimitAutoDisabled {
		s.recordModelProbeFailureCurrentMode(claim, cfg, 0, "route is disabled", true, 0)
		return
	}
	group, err := s.Groups.FindByID(route.GatewayGroupID)
	if err != nil {
		// The route row can outlive a temporarily unavailable group lookup
		// (for example during a database failover). Treat this as a transient
		// probe failure and release the lease through the normal CAS update.
		s.recordModelProbeFailureCurrentMode(claim, cfg, 0, err.Error(), false, 0)
		return
	}

	result := s.probePersistedRouteModel(ctx, group, *route, claim.Model, claim.ProbeInboundProtocol, cfg)
	if result.OK {
		cleared, err := s.Routes.MarkModelProbeSuccess(claim, time.Now(), result.StatusCode)
		if err != nil && s.Log != nil {
			s.Log.Warn("record gateway model probe success failed", "route_id", claim.RouteID, "model", claim.Model, "err", err)
		}
		if cleared && s.Log != nil {
			s.Log.Info("gateway model cooldown recovered by probe", "route_id", claim.RouteID, "model", claim.Model, "status", result.StatusCode, "latency_ms", result.LatencyMS)
		}
		return
	}
	if ctx.Err() != nil {
		s.recordModelProbeFailureCurrentMode(claim, cfg, result.StatusCode, ctx.Err().Error(), false, 0)
		return
	}
	s.recordModelProbeFailureCurrentMode(claim, cfg, result.StatusCode, result.Error, result.Permanent, result.RetryAfter)
}

func (s *Service) recordModelProbeFailureCurrentMode(claim storage.GatewayRouteModelCooldown, fallback config.GatewayConfig, status int, message string, permanent bool, retryAfter time.Duration) {
	if s.gatewayRuntime().ModelCooldownProbeEnabled {
		s.recordModelProbeFailure(claim, fallback, status, message, permanent, retryAfter)
		return
	}
	s.recordManualModelProbeFailure(claim, status, message)
}

func (s *Service) recordModelProbeFailure(claim storage.GatewayRouteModelCooldown, cfg config.GatewayConfig, status int, message string, permanent bool, retryAfter time.Duration) {
	s.recordModelProbeFailureMode(claim, cfg, status, message, permanent, retryAfter, true)
}

func (s *Service) recordManualModelProbeFailure(claim storage.GatewayRouteModelCooldown, status int, message string) {
	s.recordModelProbeFailureMode(claim, s.gatewayRuntime(), status, message, false, 0, false)
}

func (s *Service) recordModelProbeFailureMode(claim storage.GatewayRouteModelCooldown, cfg config.GatewayConfig, status int, message string, permanent bool, retryAfter time.Duration, automatic bool) {
	now := time.Now()
	var (
		updated bool
		err     error
		next    time.Time
	)
	if automatic {
		next = now.Add(modelProbeBackoff(cfg, claim.ProbeFailureCount, permanent, retryAfter))
		updated, err = s.Routes.MarkModelProbeFailure(claim, now, next, status, message, permanent)
	} else {
		updated, err = s.Routes.MarkManualModelProbeFailure(claim, now, status, message)
	}
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("record gateway model probe failure failed", "route_id", claim.RouteID, "model", claim.Model, "err", err)
		}
		return
	}
	if updated && s.Log != nil {
		attrs := []any{"route_id", claim.RouteID, "model", claim.Model, "status", status, "permanent", permanent, "error", strings.TrimSpace(message)}
		if automatic {
			attrs = append(attrs, "next_probe_at", next)
		} else {
			attrs = append(attrs, "automatic", false)
		}
		s.Log.Warn("gateway model cooldown probe failed", attrs...)
	}
}

func modelProbeBackoff(cfg config.GatewayConfig, previousFailures int, permanent bool, retryAfter time.Duration) time.Duration {
	base := time.Duration(cfg.ModelCooldownProbeIntervalMinutes) * time.Minute
	if base <= 0 {
		base = time.Duration(config.DefaultGatewayModelCooldownProbeIntervalMinutes) * time.Minute
	}
	maxBackoff := time.Duration(cfg.ModelCooldownProbeMaxBackoffMinutes) * time.Minute
	if maxBackoff <= 0 {
		maxBackoff = time.Duration(config.DefaultGatewayModelCooldownProbeMaxBackoffMinutes) * time.Minute
	}
	if permanent {
		// Do not hammer a broken key/model. Keep the row visible for manual
		// attention while allowing an eventual recovery after a config fix.
		if base < 30*time.Minute {
			base = 30 * time.Minute
		}
	}
	if previousFailures < 0 {
		previousFailures = 0
	}
	if previousFailures > 10 {
		previousFailures = 10
	}
	backoff := base
	for i := 0; i < previousFailures; i++ {
		if backoff >= maxBackoff/2 {
			backoff = maxBackoff
			break
		}
		backoff *= 2
	}
	if retryAfter > backoff {
		backoff = retryAfter
	}
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	if backoff <= 0 {
		backoff = time.Minute
	}
	return backoff
}

// probePersistedRouteModel probes the final upstream model stored in the
// cooldown row. It intentionally does not apply group/route model mappings a
// second time; the row already contains the resolved upstream identifier.
func (s *Service) probePersistedRouteModel(parent context.Context, group *storage.GatewayGroup, route storage.GatewayRoute, upstreamModel, inboundProtocol string, cfg config.GatewayConfig) modelCooldownProbeResult {
	if parent == nil {
		parent = context.Background()
	}
	upstreamModel = strings.TrimSpace(upstreamModel)
	result := modelCooldownProbeResult{ModelTestResult: ModelTestResult{
		RouteID:           route.ID,
		SourceKind:        route.NormalizeSourceKind(),
		ChannelID:         route.SourceChannelID,
		GatewayProviderID: route.GatewayProviderID,
		SourceGroupID:     route.SourceGroupID,
		SourceGroupName:   strings.TrimSpace(route.SourceGroupName),
		RequestedModel:    upstreamModel,
		UpstreamModel:     upstreamModel,
	}}
	if upstreamModel == "" {
		result.Error = "model is empty"
		result.Permanent = true
		return result
	}
	if !route.Enabled || route.RateLimitAutoDisabled {
		result.Error = "route is disabled"
		result.Permanent = true
		return result
	}
	target, err := s.resolveUpstreamTarget(&route)
	if err != nil {
		result.Error = err.Error()
		result.Permanent = true
		return result
	}
	if target.Provider != nil {
		allowed, policyErr := ProviderAllowsUpstreamModel(target.Provider, upstreamModel)
		if policyErr != nil {
			result.Error = "provider model policy: " + policyErr.Error()
			result.Permanent = true
			return result
		}
		if !allowed {
			result.Error = "provider model policy rejects model"
			result.Permanent = true
			return result
		}
	}
	s.applyRouteUserAgentForAdmin(target, group, &route)
	if target.Provider != nil {
		result.ChannelName = target.Provider.Name
		result.Label = target.Provider.Name
	} else if target.Channel != nil {
		result.ChannelName = target.Channel.Name
		result.Label = s.formatChannelGroupLabel(target.Channel.Name, route.SourceGroupName, route.SourceChannelID)
	}
	if result.Label == "" {
		result.Label = fmt.Sprintf("route#%d", route.ID)
	}

	inbound := normalizeProbeInboundKind(inboundProtocol)
	routeProtocol := s.normalizeUpstreamProtocol(route.UpstreamProtocol)
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider && target.Provider != nil && routeProtocol == storage.GatewayUpstreamProtocolAuto {
		if providerProtocol := s.normalizeProviderProtocol(target.Provider.UpstreamProtocol); providerProtocol != storage.GatewayUpstreamProtocolAuto {
			routeProtocol = providerProtocol
		}
	}
	upstreamKind := protocol.ResolveUpstream(routeProtocol, inbound, upstreamModel)
	probeBody, inboundPath := modelProbeInboundRequest(inbound, upstreamModel)
	body, path, _, convErr := s.prepareUpstreamRequest(probeBody, inbound, upstreamKind, upstreamModel, false, inboundPath)
	if convErr != nil {
		result.Error = "protocol convert: " + convErr.Error()
		result.Permanent = true
		return result
	}
	result.UpstreamPath = path
	timeout := time.Duration(cfg.ModelCooldownProbeTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(config.DefaultGatewayModelCooldownProbeTimeoutSeconds) * time.Second
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	start := time.Now()
	probeHeaders := http.Header{}
	probeHeaders.Set("Accept", "application/json")
	status, headers, respBody, _, forwardErr := s.forwardOnce(ctx, &gin.Context{}, target, path, http.MethodPost, probeHeaders, body, false, upstreamKind, 0)
	result.LatencyMS = time.Since(start).Milliseconds()
	result.StatusCode = status
	result.Headers = headers
	if forwardErr != nil {
		result.Error = forwardErr.Error()
		return result
	}
	if status >= 200 && status < 300 {
		result.OK = true
		return result
	}
	result.Error = s.truncateProbeError(respBody, 240)
	if result.Error == "" {
		result.Error = fmt.Sprintf("upstream status %d", status)
	}
	result.Permanent = classifyModelProbePermanent(status, result.Error)
	result.RetryAfter = parseProbeRetryAfter(headers.Get("Retry-After"), time.Now())
	return result
}

func normalizeProbeInboundKind(raw string) protocol.Kind {
	switch protocol.NormalizeKind(protocol.Kind(strings.TrimSpace(raw))) {
	case protocol.KindAnthropic:
		return protocol.KindAnthropic
	case protocol.KindOpenAIResponses:
		return protocol.KindOpenAIResponses
	default:
		return protocol.KindOpenAIChat
	}
}

func modelProbeInboundRequest(inbound protocol.Kind, model string) ([]byte, string) {
	var payload map[string]any
	var path string
	switch protocol.NormalizeKind(inbound) {
	case protocol.KindAnthropic:
		payload = map[string]any{
			"model": model, "max_tokens": 1,
			"messages": []map[string]string{{"role": "user", "content": "ping"}},
			"stream":   false,
		}
		path = "/v1/messages"
	case protocol.KindOpenAIResponses:
		payload = map[string]any{
			"model": model, "max_output_tokens": 1, "input": "ping", "stream": false,
		}
		path = "/v1/responses"
	default:
		payload = map[string]any{
			"model": model, "max_tokens": 1,
			"messages": []map[string]string{{"role": "user", "content": "ping"}},
			"stream":   false,
		}
		path = "/v1/chat/completions"
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return []byte(`{}`), path
	}
	return body, path
}

func modelCooldownProbeSupportedRequest(path string, inbound protocol.Kind) bool {
	path = strings.TrimSuffix(strings.TrimSpace(path), "/")
	switch protocol.NormalizeKind(inbound) {
	case protocol.KindAnthropic:
		return path == "/v1/messages"
	case protocol.KindOpenAIResponses:
		return path == "/v1/responses"
	case protocol.KindOpenAIChat:
		return path == "/v1/chat/completions"
	default:
		return false
	}
}

func classifyModelProbePermanent(status int, message string) bool {
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusNotFound {
		return true
	}
	text := strings.ToLower(strings.TrimSpace(message))
	for _, marker := range []string{
		"model_not_found", "model not found", "unknown model", "does not exist",
		"invalid api key", "invalid_api_key", "incorrect api key", "permission denied",
		"not authorized", "insufficient account balance", "insufficient balance",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

func parseProbeRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}

// ProbeModelCooldownNow is the management-plane equivalent of one scheduler
// attempt. It is intentionally explicit so an operator can recover a route
// without waiting for the next polling tick.
func (s *Service) ProbeModelCooldownNow(ctx context.Context, routeID uint, model string) (*ModelTestResult, error) {
	if s == nil || s.Routes == nil || s.Groups == nil || routeID == 0 {
		return nil, errors.New("gateway model probe is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	route, err := s.Routes.FindByID(routeID)
	if err != nil {
		return nil, err
	}
	model = storage.NormalizeGatewayModel(model)
	if model == "" {
		if len(route.ModelCooldowns) != 1 {
			return nil, errors.New("model is required when the route has multiple cooldowns")
		}
		for key := range route.ModelCooldowns {
			model = key
		}
	}
	claim, err := s.Routes.ClaimModelCooldownProbe(routeID, model, time.Now(), s.modelProbeLease())
	if err != nil {
		return nil, err
	}
	if claim == nil {
		return nil, errors.New("model probe is already running")
	}
	cfg := s.gatewayRuntime()
	// Claiming invalidates the route cache. Reload so a concurrent route edit
	// cannot make this management request probe stale credentials or protocol.
	route, err = s.Routes.FindByID(routeID)
	if err != nil {
		s.recordModelProbeFailureCurrentMode(*claim, cfg, 0, err.Error(), false, 0)
		return nil, err
	}
	group, err := s.Groups.FindByID(route.GatewayGroupID)
	if err != nil {
		s.recordModelProbeFailureCurrentMode(*claim, cfg, 0, err.Error(), false, 0)
		return nil, err
	}
	result := s.probePersistedRouteModel(ctx, group, *route, claim.Model, claim.ProbeInboundProtocol, cfg)
	if result.OK {
		var cleared bool
		cleared, err = s.Routes.MarkModelProbeSuccess(*claim, time.Now(), result.StatusCode)
		if err == nil && !cleared {
			err = errors.New("model cooldown changed while probing")
		}
	} else if !s.gatewayRuntime().ModelCooldownProbeEnabled {
		message := result.Error
		if ctx.Err() != nil {
			message = ctx.Err().Error()
		}
		s.recordManualModelProbeFailure(*claim, result.StatusCode, message)
	} else if ctx.Err() == nil {
		s.recordModelProbeFailureCurrentMode(*claim, cfg, result.StatusCode, result.Error, result.Permanent, result.RetryAfter)
	} else {
		s.recordModelProbeFailureCurrentMode(*claim, cfg, result.StatusCode, ctx.Err().Error(), false, 0)
	}
	if err != nil {
		return nil, err
	}
	return &result.ModelTestResult, nil
}

func (s *Service) modelProbeLease() time.Duration {
	cfg := s.gatewayRuntime()
	timeout := time.Duration(cfg.ModelCooldownProbeTimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(config.DefaultGatewayModelCooldownProbeTimeoutSeconds) * time.Second
	}
	return timeout + 30*time.Second
}
