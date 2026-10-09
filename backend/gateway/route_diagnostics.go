package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Only safe scheduling metadata is persisted. Never serialize route objects:
// they carry encrypted credentials and provider connection information.
type routeExclusion struct {
	RouteID     uint       `json:"route_id"`
	Group       string     `json:"source_group"`
	Fallback    bool       `json:"fallback_only"`
	Model       string     `json:"upstream_model"`
	Rate        float64    `json:"billing_rate"`
	Reasons     []string   `json:"reasons"`
	CandyUntil  *time.Time `json:"candy_until,omitempty"`
	CacheUntil  *time.Time `json:"cache_until,omitempty"`
	ModelUntil  *time.Time `json:"model_until,omitempty"`
	ProbeStatus string     `json:"probe_status,omitempty"`
}

func (rt *Runtime) routeExclusions(routes []storage.GatewayRoute, group *storage.GatewayGroup, model string, adaptive *adaptiveRequest, now time.Time) []routeExclusion {
	out := make([]routeExclusion, 0, len(routes))
	for _, route := range routes {
		upstream, _ := ResolveModel(model, ParseModelMapping(route.ModelMappingJSON), ParseModelMapping(group.ModelMappingJSON))
		if upstream == "" {
			upstream = model
		}
		d := routeExclusion{RouteID: route.ID, Group: route.SourceGroupName, Fallback: route.FallbackOnly, Model: upstream, Rate: route.BillingRateMultiplier, Reasons: []string{}}
		if !route.Enabled {
			d.Reasons = append(d.Reasons, "渠道路由已关闭")
		}
		if route.RateLimitAutoDisabled {
			d.Reasons = append(d.Reasons, "倍率超过网关组上限")
		}
		if model != "" {
			allowed, err := RouteAllowsUpstreamModel(&route, upstream)
			if err != nil || !allowed {
				d.Reasons = append(d.Reasons, "渠道路由不支持请求映射后的模型")
			}
		}
		if route.CandyCheck.Blocks(now) {
			d.CandyUntil = route.CandyCheck.CooldownUntil
			d.Reasons = append(d.Reasons, "糖果题冷却："+route.CandyCheck.Status)
		}
		if route.CandyCheck.BackingOff(now) {
			d.Reasons = append(d.Reasons, "糖果检测网络异常，短暂退避等待复测")
		}
		if route.CacheHealthBlacklistedUntil != nil && route.CacheHealthBlacklistedUntil.After(now) {
			d.CacheUntil = route.CacheHealthBlacklistedUntil
			d.Reasons = append(d.Reasons, "缓存命中率限制")
		}
		if cooldown, ok := route.ModelCooldowns[storage.NormalizeGatewayModel(upstream)]; ok {
			d.ModelUntil, d.ProbeStatus = cooldown.TempUnschedulableUntil, cooldown.ProbeStatus
			if d.ModelUntil != nil && d.ModelUntil.After(now) {
				d.Reasons = append(d.Reasons, "模型冷却")
			}
			if (cooldown.ProbeStatus == storage.GatewayModelProbeStatusProbing && cooldown.ProbeLeaseUntil != nil && cooldown.ProbeLeaseUntil.After(now)) ||
				(cooldown.ProbeStatus != storage.GatewayModelProbeStatusProbing && cooldown.ProbeStatus != storage.GatewayModelProbeStatusHealthy && cooldown.ProbeStatus != storage.GatewayModelProbeStatusManual && cooldown.NextProbeAt != nil && !cooldown.NextProbeAt.After(now)) {
				d.Reasons = append(d.Reasons, "等待模型恢复探测")
			}
		}
		if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
			if rt.Providers == nil {
				d.Reasons = append(d.Reasons, "直连渠道不可用")
			} else {
				provider, err := rt.Providers.FindByID(route.GatewayProviderID)
				if err != nil {
					d.Reasons = append(d.Reasons, "直连渠道不存在")
				} else {
					if !provider.Enabled {
						d.Reasons = append(d.Reasons, "直连渠道已关闭")
					}
					if strings.TrimSpace(provider.APIKeyCipher) == "" {
						d.Reasons = append(d.Reasons, "直连渠道缺少密钥")
					}
					if model != "" {
						allowed, err := ProviderAllowsUpstreamModel(provider, upstream)
						if err != nil || !allowed {
							d.Reasons = append(d.Reasons, "直连渠道不支持请求映射后的模型")
						}
					}
				}
			}
		} else if strings.TrimSpace(route.SourceAPIKeyCipher) == "" {
			d.Reasons = append(d.Reasons, "未确保上游密钥")
		}
		if adaptive != nil && adaptive.enabled && adaptive.priced && !route.FallbackOnly && d.Rate > adaptive.ceiling {
			d.Reasons = append(d.Reasons, "超出本次调度溢价预算")
		}
		if len(d.Reasons) == 0 {
			d.Reasons = append(d.Reasons, "未进入本次候选集合，需结合调度配置排查")
		}
		out = append(out, d)
	}
	return out
}

func (rt *Runtime) failNoRoutes(c *gin.Context, key *storage.GatewayKey, group *storage.GatewayGroup, routes []storage.GatewayRoute, model, path string, kind protocolKind, stream bool, message string, adaptive *adaptiveRequest) {
	now := time.Now()
	reqID := rt.ensureGatewayRequestID(c)
	detail, _ := json.Marshal(struct {
		Message  string           `json:"message"`
		Attempts int              `json:"upstream_attempts"`
		Ceiling  float64          `json:"group_max_billing_rate"`
		Routes   []routeExclusion `json:"routes"`
	}{message, 0, group.MaxBillingRateMultiplier, rt.routeExclusions(routes, group, model, adaptive, now)})
	requestType := storage.GatewayRequestTypeSync
	if stream {
		requestType = storage.GatewayRequestTypeStream
	}
	if rt.Usage != nil {
		item := &storage.GatewayUsageLog{GatewayGroupID: group.ID, GatewayKeyID: key.ID, RequestID: reqID,
			Attempt: 1, AttemptKind: "scheduling", AttemptStatus: storage.GatewayAttemptStatusError,
			RequestedModel: model, InboundEndpoint: path, InboundProtocol: string(kind), RequestType: requestType,
			Stream: stream, StatusCode: http.StatusServiceUnavailable, ErrorType: "routing", ErrorMessage: message,
			ErrorDetail: string(detail), CreatedAt: now, IPAddress: c.ClientIP()}
		if err := rt.Usage.Create(item); err != nil && rt.Log != nil {
			rt.Log.Error("write route exclusion audit", "request_id", reqID, "err", err)
		}
	}
	rt.finalizeUsageFailure(reqID, key)
	// Clients can distinguish an empty candidate pool from an upstream 503 and
	// avoid immediately sending the same request back into the same empty pool.
	c.Header("Retry-After", "5")
	typ := "route_unavailable"
	if strings.HasPrefix(message, "no available channel for model ") {
		typ = "model_not_found"
	}
	rt.writeGatewayError(c, kind, http.StatusServiceUnavailable, typ, message)
}
