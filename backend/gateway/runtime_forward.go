// 数据面：非流式转发与上游目标解析。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// HandleForward 主转发（含故障转移）。
func (rt *Runtime) HandleForward(c *gin.Context, path string, kind protocolKind) {
	// 尽早生成/透传 request id，保证后续任意错误体都可带上
	reqID := rt.ensureGatewayRequestID(c)

	auth, err := rt.Authenticate(c)
	if err != nil {
		rt.writeAuthError(c, kind, err.Error())
		return
	}
	key, group := auth.Key, auth.Group

	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		rt.finalizeUsageFailure(reqID, key)
		rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", "failed to read body")
		return
	}
	// legacy completions 不做跨协议
	if kind == protocolOpenAI && strings.Contains(path, "/completions") && !strings.Contains(path, "/chat/") {
		// 仅透传；若路由强制 anthropic 则报错
	}

	requestInfo := analyzeRequestBody(body)
	requestedModel, stream := requestInfo.Model, requestInfo.Stream
	serviceTier, reasoningEffort := requestInfo.ServiceTier, requestInfo.ReasoningEffort
	thinkingEnabled := requestInfo.ThinkingEnabled
	_ = rt.Keys.TouchLastUsed(key.ID, time.Now())

	routes, err := rt.Routes.ListByGroupID(group.ID)
	if err != nil || len(routes) == 0 {
		rt.finalizeUsageFailure(reqID, key)
		rt.writeGatewayError(c, kind, http.StatusServiceUnavailable, "api_error", "no routes configured")
		return
	}

	groupMapping := ParseModelMapping(group.ModelMappingJSON)
	filteredRoutes, modelFilterErr := rt.filterRoutesForRequestedModel(routes, requestedModel, groupMapping)
	if modelFilterErr != nil {
		rt.finalizeUsageFailure(reqID, key)
		rt.writeGatewayError(c, kind, http.StatusInternalServerError, "api_error", modelFilterErr.Error())
		return
	}
	if strings.TrimSpace(requestedModel) != "" && len(filteredRoutes) == 0 {
		rt.finalizeUsageFailure(reqID, key)
		rt.writeGatewayError(c, kind, http.StatusServiceUnavailable, "model_not_found",
			fmt.Sprintf("no available channel for model %s", requestedModel))
		return
	}
	routes = filteredRoutes
	// Automatic restrictions must never empty an otherwise valid route pool.
	// This fail-open recovery is independent of retry/failover settings so a
	// single-route group can still make one upstream attempt while cooling.
	routes = rt.recoverWhenAllRoutesRestricted(routes, requestedModel, groupMapping, time.Now())
	routes = bindModelCooldownAliases(routes, requestedModel, groupMapping)
	// A model alias can map to a media-generation model only after route
	// selection. Disable concurrent hedge scheduling for such a request even
	// when the client-facing name itself looks like a text model.
	mappedMediaModel := mappedRouteContainsMediaModel(routes, requestedModel, groupMapping)
	affinity := rt.routeAffinityForAnalyzedRequest(c, key.ID, group.ID, string(kind), requestedModel, body, requestInfo.AffinityID)
	groupsByChannel := rt.loadGroupsByChannel(c.Request.Context(), routes)
	validator, validationErr := rt.responseValidatorForGroup(group)
	if validationErr != nil {
		rt.finalizeUsageFailure(reqID, key)
		rt.writeGatewayError(c, kind, http.StatusInternalServerError, "api_error", validationErr.Error())
		return
	}
	useCoordinator, hedgeActive, virtualCacheEligible := rt.shouldUseCoordinatedForward(group, validator, hedgeRequest{
		Path: path, Model: requestedModel, Header: c.Request.Header, Body: body, Stream: stream,
		Realtime:          strings.Contains(strings.ToLower(path), "realtime"),
		BodyMediaAnalyzed: requestInfo.Parsed, BodyGeneratesMedia: requestInfo.MediaGeneration,
	})
	if mappedMediaModel {
		hedgeActive = false
		virtualCacheEligible = false
		// Response validation may still perform its configured sequential
		// failover; only concurrent media hedging is prohibited.
		useCoordinator = validator != nil && validator.Enabled()
	}
	if useCoordinator {
		ftTimeoutSec := rt.clampFirstTokenTimeoutSec(group.FirstTokenTimeoutSec)
		var firstTokenTimeout time.Duration
		if ftTimeoutSec > 0 {
			firstTokenTimeout = time.Duration(ftTimeoutSec) * time.Second
		}
		rt.handleForwardCoordinated(coordinatedForwardRequest{
			c: c, path: path, kind: kind, key: key, group: group, body: body,
			requestedModel: requestedModel, stream: stream, serviceTier: serviceTier,
			reasoningEffort: reasoningEffort, thinkingEnabled: thinkingEnabled,
			routes: routes, validator: validator, requestID: reqID, affinity: affinity,
			firstToken: firstTokenTimeout, hedgeActive: hedgeActive,
			hedgeEligibilityKnown: true, virtualCacheEligible: virtualCacheEligible,
			prepareCache: &upstreamRequestPrepareCache{},
		})
		return
	}

	// 组级重试策略
	retryEnabled := group.RetryEnabled
	sameRouteRetries := group.RetryCount
	failoverEnabled := group.FailoverEnabled
	failoverMax := group.FailoverMax
	failoverOn4xx := group.FailoverOn4xx
	cooldownSec := group.CooldownSeconds
	ftTimeoutSec := rt.clampFirstTokenTimeoutSec(group.FirstTokenTimeoutSec)
	var firstTokenTimeout time.Duration
	if ftTimeoutSec > 0 {
		firstTokenTimeout = time.Duration(ftTimeoutSec) * time.Second
	}
	if sameRouteRetries < 0 {
		sameRouteRetries = 0
	}
	if failoverMax < 0 {
		failoverMax = 0
	}
	if cooldownSec < 0 {
		cooldownSec = 0
	}
	// 重试总开关关闭：不重试、不顺延，失败直接回显
	if !retryEnabled {
		sameRouteRetries = 0
		failoverEnabled = false
		failoverMax = 0
		failoverOn4xx = false
	}

	exclude := map[uint]struct{}{}
	var lastStatus int
	var lastBody []byte
	var lastErr error
	// 已顺延次数（换到另一条路由的次数）
	failoversDone := 0
	// 全局尝试序号（写入 usage.attempt，同一 request_id 关联）
	attemptNo := 0
	routesTried := 0
	prepareCache := &upstreamRequestPrepareCache{}
	finishRecoveryProbe := func(routeID uint) {
		if !affinity.Recovery || affinity.RecoveryRouteID != routeID {
			return
		}
		now := time.Now()
		rt.finishRouteAffinityProbe(&affinity, routeID, false, affinity.recoveryBlockedUntil(nil, now), now)
	}

	for {
		candidates := rt.sortRoutesWithAffinity(routes, groupsByChannel, group.RateSortDirection, time.Now(), exclude, &affinity, requestedModel)
		if routesTried == 0 {
			candidates = rt.orderLoadBalancedCandidates(candidates, group, &affinity)
		}
		if len(candidates) == 0 {
			break
		}
		// 非首条路由 = 顺延；超过顺延次数则停止
		if routesTried > 0 {
			if !failoverEnabled || failoversDone >= failoverMax {
				break
			}
		}

		cand := candidates[0]
		route := cand.Route
		recoveryRoute := affinity.Recovery && affinity.RecoveryRouteID == route.ID && routesTried == 0
		exclude[route.ID] = struct{}{}
		if routesTried > 0 {
			failoversDone++
		}
		routesTried++

		routeMapping := ParseModelMapping(route.ModelMappingJSON)
		upstreamModel, chain := ResolveModel(requestedModel, routeMapping, groupMapping)
		if upstreamModel == "" {
			upstreamModel = requestedModel
		}
		// 映射后的上游模型可能是 kimi/glm 等；thinking 开了但无档位时补 high
		attemptReasoningEffort := ApplyThinkingEnabledEffortFallback(
			reasoningEffort, thinkingEnabled, upstreamModel, requestedModel,
		)

		// 当前路由已进 exclude：失败后是否还有可顺延的其它路由。
		// 没有下家时关闭首字超时，最后一枪老实等上游，而不是再掐 30s 直接 502。
		remainingAfter := SortRoutesForModel(routes, groupsByChannel, group.RateSortDirection, time.Now(), exclude, requestedModel)
		attemptFTTimeout := rt.effectiveFirstTokenTimeout(
			firstTokenTimeout,
			retryEnabled, failoverEnabled,
			failoversDone, failoverMax,
			len(remainingAfter) > 0,
		)

		// 同一路由：1 次首次 + sameRouteRetries 次重试
		maxTriesOnRoute := 1 + sameRouteRetries
		if recoveryRoute {
			// A cooled route gets exactly one half-open recovery probe. If it
			// fails, continue with the configured fallback routes instead of
			// amplifying the probe through same-route retries.
			maxTriesOnRoute = 1
		}
		for tryOnRoute := 0; tryOnRoute < maxTriesOnRoute; tryOnRoute++ {
			attemptNo++
			attemptKind := attemptKindPrimary
			if tryOnRoute > 0 {
				attemptKind = attemptKindRetry
			} else if recoveryRoute {
				attemptKind = attemptKindRecovery
			} else if routesTried > 1 {
				attemptKind = attemptKindFailover
			}

			target, resolveErr := rt.resolveUpstreamTarget(&route)
			if resolveErr != nil {
				lastErr = resolveErr
				errInfo := usageErrorInfo{
					Type:    "config",
					Summary: resolveErr.Error(),
					Detail:  fmt.Sprintf("config error\nroute_id: %d\nsource_kind: %s\nerror: %s\n", route.ID, route.NormalizeSourceKind(), resolveErr.Error()),
				}
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, 0, false, errInfo, 0, nil, c, usageRecordMeta{
					InboundEndpoint: path, InboundProtocol: string(kind), ServiceTier: serviceTier, ReasoningEffort: attemptReasoningEffort,
					Attempt: attemptNo, AttemptKind: attemptKind,
				})
				finishRecoveryProbe(route.ID)
				// 配置错误：不再重试同路由，进入顺延判断
				break
			}
			// 转发：组+路由 UA 三选一（透传 / 组 / 自定义）
			rt.applyRouteUserAgent(target, group, &route)

			// 协议：路由显式优先；route=auto 且为 provider 时用 provider 默认
			routeProto := rt.normalizeUpstreamProtocol(route.UpstreamProtocol)
			if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider &&
				routeProto == storage.GatewayUpstreamProtocolAuto &&
				target.Provider != nil {
				if p := rt.normalizeProviderProtocol(target.Provider.UpstreamProtocol); p != storage.GatewayUpstreamProtocolAuto {
					routeProto = p
				}
			}
			upstreamKind := protocol.ResolveUpstream(routeProto, kind, upstreamModel)

			// legacy completions 不跨协议
			if protocol.IsOpenAIFamily(kind) && strings.Contains(path, "/completions") && !strings.Contains(path, "/chat/") {
				if upstreamKind == protocolAnthropic || upstreamKind == protocol.KindOpenAIResponses {
					message := "protocol conversion for /v1/completions is not supported; use /v1/chat/completions"
					errInfo := usageErrorInfo{
						Type: "config", Summary: message,
						Detail: fmt.Sprintf("protocol conversion error\nroute_id: %d\nupstream_protocol: %s\nerror: %s\n", route.ID, upstreamKind, message),
					}
					rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, 0, false, errInfo, 0, nil, c, usageRecordMeta{
						InboundEndpoint: path, InboundProtocol: string(kind), UpstreamProtocol: string(upstreamKind),
						UpstreamURL: target.BaseURL + path, ServiceTier: serviceTier, ReasoningEffort: attemptReasoningEffort,
						Attempt: attemptNo, AttemptKind: attemptKind,
					})
					finishRecoveryProbe(route.ID)
					rt.finalizeUsageFailure(reqID, key)
					rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", message)
					return
				}
				upstreamKind = protocol.KindOpenAIChat
			}

			fwdBody, upstreamPath, converted, convErr := prepareCache.prepare(
				rt.Service, body, kind, upstreamKind, requestedModel, upstreamModel, stream, path,
			)
			if convErr != nil {
				message := "protocol convert failed: " + convErr.Error()
				errInfo := usageErrorInfo{
					Type: "config", Summary: message,
					Detail: fmt.Sprintf("protocol conversion error\nroute_id: %d\nupstream_protocol: %s\nerror: %s\n", route.ID, upstreamKind, convErr.Error()),
				}
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, 0, false, errInfo, 0, nil, c, usageRecordMeta{
					InboundEndpoint: path, InboundProtocol: string(kind), UpstreamProtocol: string(upstreamKind),
					UpstreamURL: target.BaseURL + path, ServiceTier: serviceTier, ReasoningEffort: attemptReasoningEffort,
					Attempt: attemptNo, AttemptKind: attemptKind,
				})
				finishRecoveryProbe(route.ID)
				rt.finalizeUsageFailure(reqID, key)
				rt.writeGatewayError(c, kind, http.StatusBadRequest, "invalid_request_error", message)
				return
			}

			upstreamFullURL := target.BaseURL + upstreamPath
			usageMeta := usageRecordMeta{
				InboundEndpoint:   path,
				UpstreamEndpoint:  upstreamPath,
				InboundProtocol:   string(kind),
				UpstreamProtocol:  string(upstreamKind),
				ProtocolConverted: converted,
				ServiceTier:       serviceTier,
				ReasoningEffort:   attemptReasoningEffort,
				UpstreamURL:       upstreamFullURL,
				Attempt:           attemptNo,
				AttemptKind:       attemptKind,
			}
			providerVirtualCachePercent := 0
			if target.Provider != nil && virtualCacheEligible {
				providerVirtualCachePercent = providerVirtualCachePercentForGroup(target.Provider, upstreamModel, group)
			}

			start := time.Now()
			var (
				status                            int
				respHeaders                       http.Header
				respBody                          []byte
				firstTokenMS                      *int64
				fwdErr                            error
				streamCommitted                   bool
				streamTokens                      UsageTokens
				streamErr                         error
				clientDisconnected                bool
				streamVirtualCacheApplied         bool
				streamInputUsageRecoveryAttempted bool
			)
			if stream {
				// 真流式：边读上游 SSE 边写客户端。Committed 后禁止 retry/failover。
				res := rt.forwardStreamWithVirtualCache(
					c.Request.Context(), c, target, upstreamPath, c.Request.Method, c.Request.Header, fwdBody,
					kind, upstreamKind, upstreamModel, converted, attemptFTTimeout, providerVirtualCachePercent,
				)
				status = res.Status
				respHeaders = res.Headers
				respBody = res.Body
				firstTokenMS = res.FirstTokenMS
				fwdErr = res.Err
				streamCommitted = res.Committed
				streamTokens = res.Tokens
				streamErr = res.StreamErr
				clientDisconnected = res.ClientDisconnected
				streamVirtualCacheApplied = res.VirtualCacheApplied
				streamInputUsageRecoveryAttempted = res.InputUsageRecoveryAttempted
			} else {
				status, respHeaders, respBody, firstTokenMS, fwdErr = rt.forwardOnce(
					c.Request.Context(), c, target, upstreamPath, c.Request.Method, c.Request.Header, fwdBody, false, upstreamKind, attemptFTTimeout,
				)
			}
			duration := time.Since(start).Milliseconds()

			// 已向客户端写出 SSE：不能再重试/顺延，直接记账并结束本次请求。
			// 客户端在 commit 后、终端帧交付前断开：记 success=true + error_type=client。
			// 若已写出 [DONE]/message_stop 后客户端才关连接，forwardStream 会清掉 client 标记，记普通成功。
			if stream && streamCommitted {
				onlyClientDisconnect := rt.isClientDisconnectAfterCommit(clientDisconnected, streamErr)
				// 仅客户端断开仍算业务成功；真实 stream 错误才算失败。
				success := streamErr == nil || onlyClientDisconnect
				if success && streamTokens.InputTokens == 0 && !streamInputUsageRecoveryAttempted {
					streamTokens = rt.recoverMissingStreamInputTokens(
						c.Request.Context(), c, target, fwdBody, upstreamKind, streamTokens,
					)
				}
				errInfo := usageErrorInfo{}
				gwCfg := rt.gatewayRuntime()
				headerJSON := rt.formatDebugHeaders(respHeaders, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
				headerPlain := rt.formatHeadersPlain(respHeaders, gwCfg.UsageErrorHeaderValueRunes)
				if onlyClientDisconnect {
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"client disconnected after stream commit\nmethod: %s\nurl: %s\nnote: 已继续读取上游以同步 usage/计费；上游可能仍完整计费\n",
						c.Request.Method, upstreamFullURL,
					)
					if streamErr != nil {
						fmt.Fprintf(&detail, "stream_err: %s\n", streamErr.Error())
					}
					fmt.Fprintf(&detail,
						"tokens: input=%d output=%d cache_read=%d cache_creation=%d\n",
						streamTokens.InputTokens, streamTokens.OutputTokens,
						streamTokens.CacheReadTokens, streamTokens.CacheCreationTokens,
					)
					rt.appendUpstreamHeadersToDetail(&detail, headerPlain)
					errInfo = usageErrorInfo{
						Type:            "client",
						Summary:         "客户端主动断开（已尽量同步上游用量）",
						Detail:          detail.String(),
						UpstreamHeaders: headerJSON,
					}
					if rt.Log != nil {
						rt.Log.Info("gateway stream client disconnected after commit",
							"request_id", reqID,
							"attempt", attemptNo,
							"route_id", route.ID,
							"upstream_url", upstreamFullURL,
							"input_tokens", streamTokens.InputTokens,
							"output_tokens", streamTokens.OutputTokens,
							"cache_read", streamTokens.CacheReadTokens,
						)
					}
				} else if streamErr != nil {
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"stream error after commit\nmethod: %s\nurl: %s\nerror: %s\nclient_disconnected: %v\n",
						c.Request.Method, upstreamFullURL, streamErr.Error(), clientDisconnected,
					)
					rt.appendUpstreamHeadersToDetail(&detail, headerPlain)
					errInfo = usageErrorInfo{
						Type:            "transport",
						Summary:         streamErr.Error(),
						Detail:          detail.String(),
						UpstreamHeaders: headerJSON,
					}
				}
				if success {
					rt.noteRouteModelSuccess(&route, upstreamModel)
					rt.finishRouteAffinityProbe(&affinity, route.ID, true, nil, time.Now())
					if affinity.shouldRememberRoute(route.ID) {
						rt.rememberRouteAffinity(affinity.Keys, route.ID, time.Now())
					}
				} else {
					if recoveryRoute {
						blockUntil := affinity.recoveryBlockedUntil(nil, time.Now())
						rt.finishRouteAffinityProbe(&affinity, route.ID, false, blockUntil, time.Now())
					}
					if rt.Log != nil {
						rt.Log.Warn("gateway stream ended with error after commit",
							"request_id", reqID,
							"attempt", attemptNo,
							"route_id", route.ID,
							"upstream_url", upstreamFullURL,
							"err", errInfo.Summary,
							"client_disconnected", clientDisconnected,
							"input_tokens", streamTokens.InputTokens,
							"output_tokens", streamTokens.OutputTokens,
						)
					}
				}
				// status 通常为 200；保留已 drain 的 tokens（含客户端断开后的用量同步）。
				if status == 0 {
					status = http.StatusOK
				}
				settlement := storage.GatewayFinalizeRequestInput{}
				if success && streamVirtualCacheApplied {
					settlement = rt.buildProviderVirtualCacheSettlement(
						target.Provider, upstreamModel, streamTokens, protocol.Kind(upstreamKind),
						cand.EffectiveRate, cand.BillingRate, virtualCacheEligible, group,
					)
				}
				usageMeta.DeferSettlement = settlement.VirtualCacheReadEnabled
				usageID := rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, streamTokens, cand.EffectiveRate, cand.BillingRate, stream, status, success, errInfo, duration, firstTokenMS, c, usageMeta)
				if !success {
					rt.finalizeUsageFailure(reqID, key)
				} else if settlement.VirtualCacheReadEnabled {
					if usageID == 0 {
						rt.finalizeUsageFailure(reqID, key)
					} else if err := rt.finalizeUsageWinnerWithSettlement(reqID, key, attemptNo, usageID, settlement); err != nil {
						_ = rt.finalizeUsageWinner(reqID, key, attemptNo, usageID)
					}
				}
				return
			}

			if fwdErr != nil || rt.isFailoverStatus(status, failoverOn4xx) {
				gwCfg := rt.gatewayRuntime()
				errInfo := rt.buildUpstreamErrorInfoCfg(gwCfg, fwdErr, status, respHeaders, respBody, upstreamFullURL, c.Request.Method)
				if rt.isFirstTokenTimeout(fwdErr) {
					errInfo.Type = "transport"
					appliedSec := int(attemptFTTimeout / time.Second)
					if appliedSec <= 0 {
						appliedSec = ftTimeoutSec
					}
					errInfo.Summary = fmt.Sprintf("首字超时（%ds）：未在限定时间内收到上游首字节，已主动断开", appliedSec)
					var detail strings.Builder
					fmt.Fprintf(&detail,
						"first token timeout\nmethod: %s\nurl: %s\ntimeout: %ds\nwaited: %dms\nnote: 上游可能已开始计费；将走重试/顺延，可能增加费用\n",
						c.Request.Method, upstreamFullURL, appliedSec, duration,
					)
					// 保留已拿到的上游响应头（若有）
					rt.appendUpstreamHeadersToDetail(&detail, rt.formatHeadersPlain(respHeaders, gwCfg.UsageErrorHeaderValueRunes))
					if errInfo.UpstreamHeaders == "" {
						errInfo.UpstreamHeaders = rt.formatDebugHeaders(respHeaders, gwCfg.UsageErrorHeadersJSONBytes, gwCfg.UsageErrorHeaderValueRunes)
					}
					errInfo.Detail = detail.String()
				}
				// 客户端断开 / 请求 context 已取消：父 context 污染会导致后续重试/顺延全部变成
				// "context canceled"，且误伤路由冷却。此类错误应立刻停止，不再重试/顺延/冷却。
				clientCanceled := rt.isClientContextError(fwdErr, c) || clientDisconnected
				if clientCanceled {
					rt.annotateClientContextError(&errInfo, c, upstreamFullURL, c.Request.Method, fwdErr)
				}
				if fwdErr != nil {
					lastErr = fwdErr
				} else {
					lastStatus = status
					lastBody = rt.convertErrorBody(respBody, kind, upstreamKind, converted)
					lastErr = fmt.Errorf("upstream status %d: %s", status, errInfo.Summary)
				}
				// 最后一次同路由尝试失败才进入冷却（客户端取消 / 重试关闭时不写冷却）
				// Permanent capability/auth/balance errors do not benefit from
				// repeating the same request on the same route. Keep failover to
				// other routes available, but end this route's retry ladder now.
				if tryOnRoute < maxTriesOnRoute-1 && !isSameRouteRetryableUpstreamFailure(status, errInfo) {
					maxTriesOnRoute = tryOnRoute + 1
				}
				lastTryOnRoute := tryOnRoute >= maxTriesOnRoute-1
				var cooldownUntil *time.Time
				if retryEnabled && lastTryOnRoute && cooldownSec > 0 && !clientCanceled {
					until := time.Now().Add(time.Duration(cooldownSec) * time.Second)
					cooldownUntil = &until
					pauseReason := errInfo.Summary
					if strings.TrimSpace(errInfo.Detail) != "" {
						pauseReason = rt.truncateRunes(errInfo.Detail, 4000)
					}
					cooldownErr := rt.Routes.SetModelTempUnschedulableWithProbeProtocol(
						route.ID, upstreamModel, until, pauseReason, time.Now(), reqID,
						gwCfg.ModelCooldownProbeEnabled && modelCooldownProbeSupportedRequest(path, kind), string(kind),
					)
					if cooldownErr == nil && storage.NormalizeGatewayModel(upstreamModel) != "" {
						affinity.preservePreferredOnCooldown(route.ID)
					}
				}
				if recoveryRoute && lastTryOnRoute {
					blockUntil := affinity.recoveryBlockedUntil(cooldownUntil, time.Now())
					rt.finishRouteAffinityProbe(&affinity, route.ID, false, blockUntil, time.Now())
				}
				usageMeta.CooldownUntil = cooldownUntil
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, status, false, errInfo, duration, firstTokenMS, c, usageMeta)
				if rt.Log != nil {
					rt.Log.Warn("gateway upstream fail",
						"request_id", reqID,
						"attempt", attemptNo,
						"attempt_kind", attemptKind,
						"route_id", route.ID,
						"status", status,
						"upstream_url", upstreamFullURL,
						"err", errInfo.Summary,
						"client_canceled", clientCanceled,
					)
				}
				// 客户端已取消：立刻结束，避免用已取消 context 继续打上游
				if clientCanceled {
					goto finishError
				}
				// 重试关闭，或还可同路由重试则 continue；否则跳出到顺延
				if !retryEnabled {
					goto finishError
				}
				if tryOnRoute < maxTriesOnRoute-1 {
					continue
				}
				break // 同路由耗尽，顺延
			}

			// 默认 4xx（非 429）不重试不顺延，直接回显；组开启 failover_on_4xx 时已在上方 isFailoverStatus 分支处理。
			if status >= 400 {
				errInfo := rt.buildUpstreamErrorInfoCfg(rt.gatewayRuntime(), nil, status, respHeaders, respBody, upstreamFullURL, c.Request.Method)
				clientBody := rt.convertErrorBody(respBody, kind, upstreamKind, converted)
				clientBody = rt.injectUpstreamOpsRequestID(clientBody, reqID)
				rt.copyResponseHeaders(c.Writer.Header(), respHeaders)
				c.Writer.Header().Del("Content-Length")
				c.Header("Content-Type", "application/json")
				rt.setGatewayRequestIDHeaders(c, reqID)
				c.Status(status)
				_, _ = c.Writer.Write(clientBody)
				finishRecoveryProbe(route.ID)
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, UsageTokens{}, cand.EffectiveRate, cand.BillingRate, stream, status, false, errInfo, duration, firstTokenMS, c, usageMeta)
				rt.finalizeUsageFailure(reqID, key)
				return
			}

			// 非流式成功：整包转换后写出
			tokens := rt.parseUsageByKind(respBody, false, upstreamKind)
			clientBody := rt.convertUpstreamResponse(respBody, kind, upstreamKind, upstreamModel, false, converted)
			if converted && rt.Log != nil && len(clientBody) == 0 && len(respBody) > 0 {
				rt.Log.Warn("response convert produced empty body")
			}
			settlement := rt.buildProviderVirtualCacheSettlement(
				target.Provider, upstreamModel, tokens, protocol.Kind(upstreamKind),
				cand.EffectiveRate, cand.BillingRate, virtualCacheEligible, group,
			)
			virtualCacheChanged := false
			if settlement.VirtualCacheReadEnabled {
				if rewritten, changed := rewriteVirtualCacheResponsePercent(clientBody, kind, virtualCachePercentForSettlement(settlement, target.Provider, upstreamModel, group)); changed {
					clientBody = rewritten
					virtualCacheChanged = true
				} else {
					settlement = storage.GatewayFinalizeRequestInput{}
				}
			}
			rt.copyResponseHeaders(c.Writer.Header(), respHeaders)
			if converted {
				c.Writer.Header().Del("Content-Length")
				c.Header("Content-Type", "application/json")
			}
			if virtualCacheChanged {
				c.Writer.Header().Del("Content-Length")
			}
			c.Status(status)
			usageMeta.DeferSettlement = settlement.VirtualCacheReadEnabled
			written, writeErr := c.Writer.Write(clientBody)
			if writeErr != nil || (len(clientBody) > 0 && written != len(clientBody)) {
				if writeErr == nil {
					writeErr = io.ErrShortWrite
				}
				errInfo := usageErrorInfo{
					Type:    "client",
					Summary: "failed to deliver gateway response: " + writeErr.Error(),
					Detail:  fmt.Sprintf("downstream write error\nrequest_id: %s\nwritten_bytes: %d\nresponse_bytes: %d\nerror: %s\n", reqID, written, len(clientBody), writeErr.Error()),
				}
				finishRecoveryProbe(route.ID)
				rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, tokens, cand.EffectiveRate, cand.BillingRate, stream, status, false, errInfo, duration, firstTokenMS, c, usageMeta)
				rt.finalizeUsageFailure(reqID, key)
				return
			}
			// 成功：立刻恢复调度；连续成功达到阈值后自动清除错误残留展示
			rt.noteRouteModelSuccess(&route, upstreamModel)
			rt.finishRouteAffinityProbe(&affinity, route.ID, true, nil, time.Now())
			if affinity.shouldRememberRoute(route.ID) {
				rt.rememberRouteAffinity(affinity.Keys, route.ID, time.Now())
			}
			usageID := rt.recordUsage(key, group, &route, target, reqID, requestedModel, upstreamModel, chain, tokens, cand.EffectiveRate, cand.BillingRate, stream, status, true, usageErrorInfo{}, duration, firstTokenMS, c, usageMeta)
			if settlement.VirtualCacheReadEnabled {
				if usageID == 0 {
					rt.finalizeUsageFailure(reqID, key)
					return
				}
				if err := rt.finalizeUsageWinnerWithSettlement(reqID, key, attemptNo, usageID, settlement); err != nil {
					if rt.Log != nil {
						rt.Log.Error("finalize provider virtual cache winner failed", "request_id", reqID, "attempt", attemptNo, "err", err)
					}
					_ = rt.finalizeUsageWinner(reqID, key, attemptNo, usageID)
				}
			}
			return
		}

		// 同路由结束：判断能否顺延
		if !retryEnabled || !failoverEnabled || failoversDone >= failoverMax {
			break
		}
	}

finishError:
	rt.finalizeUsageFailure(reqID, key)
	if lastStatus > 0 && len(lastBody) > 0 {
		out := rt.injectUpstreamOpsRequestID(lastBody, reqID)
		rt.setGatewayRequestIDHeaders(c, reqID)
		c.Header("Content-Type", "application/json")
		c.Writer.Header().Del("Content-Length")
		c.Status(lastStatus)
		_, _ = c.Writer.Write(out)
		return
	}
	msg := "all upstream routes failed"
	if lastErr != nil {
		msg = lastErr.Error()
	}
	rt.writeGatewayError(c, kind, http.StatusBadGateway, "api_error", msg)
}

// prepareUpstreamRequest 按入站/上游协议准备转发 body 与 path。

func (rt *Runtime) forwardOnce(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	stream bool,
	kind protocolKind,
	firstTokenTimeout time.Duration, // 0=关闭；从发起请求起算到首字节的总等待
) (status int, respHeader http.Header, respBody []byte, firstTokenMS *int64, err error) {
	if target == nil {
		return 0, nil, nil, nil, errors.New("upstream target is nil")
	}
	release, err := rt.acquireUpstreamConcurrency(ctx, target)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	defer release()

	// 可取消：首字超时需能打断卡住的 Do / body 读
	reqCtx, abortReq := context.WithCancel(ctx)
	defer abortReq()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, target, path, method, inHeader, body, kind, stream)
	if err != nil {
		return 0, nil, nil, nil, err
	}

	if target.onUpstreamStart != nil {
		target.onUpstreamStart()
	}
	client := rt.httpClientForTarget(target.Channel, target.Provider)
	start := time.Now()
	resp, err := rt.doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, firstTokenTimeout)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	// 注意：超时断开时需主动 Close，正常路径 defer 关闭
	bodyClosed := false
	closeBody := func() {
		if !bodyClosed {
			bodyClosed = true
			_ = resp.Body.Close()
		}
	}
	defer closeBody()

	// 非 2xx 错误体整包读，不受首字超时约束
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(rt.gatewayRuntime().UsageErrorBodyBytes)+1))
		if readErr != nil && len(data) == 0 {
			return resp.StatusCode, resp.Header.Clone(), nil, nil, readErr
		}
		return resp.StatusCode, resp.Header.Clone(), data, nil, nil
	}

	// 流式/非流式成功响应：首字节超时从 start 起算剩余预算（含等响应头时间）。
	var ft *int64
	var data []byte

	bodyWait, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
	if timedOut {
		closeBody()
		abortReq()
		ms := time.Since(start).Milliseconds()
		ft = &ms
		return 0, resp.Header.Clone(), nil, ft, fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
	}

	firstChunk, firstErr := rt.readFirstChunk(resp.Body, bodyWait)
	if firstErr != nil {
		if rt.isFirstTokenTimeout(firstErr) {
			// 主动掐断连接，触发重试/顺延（上游可能已开始计费）
			closeBody()
			abortReq()
			ms := time.Since(start).Milliseconds()
			ft = &ms
			return 0, resp.Header.Clone(), nil, ft, fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
		}
		return resp.StatusCode, resp.Header.Clone(), nil, nil, firstErr
	}
	if len(firstChunk) > 0 {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		data = append(data, firstChunk...)
	}

	if stream {
		buf := make([]byte, 32*1024)
		for {
			n, readErr := resp.Body.Read(buf)
			if n > 0 {
				data = append(data, buf[:n]...)
			}
			if readErr != nil {
				if readErr == io.EOF {
					break
				}
				return resp.StatusCode, resp.Header.Clone(), data, ft, readErr
			}
		}
	} else {
		rest, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return resp.StatusCode, resp.Header.Clone(), data, ft, readErr
		}
		if len(rest) > 0 {
			data = append(data, rest...)
		}
	}
	_ = c
	return resp.StatusCode, resp.Header.Clone(), data, ft, nil
}

// remainingFirstTokenWait 计算从 start 起算，首字超时还剩多少时间。
// configured<=0 表示关闭：left=0 且 timedOut=false（调用方按「无超时」处理）。
// 已耗尽：timedOut=true。

func (rt *Runtime) resolveUpstreamTarget(route *storage.GatewayRoute) (*upstreamTarget, error) {
	if route == nil {
		return nil, errors.New("route is nil")
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if rt.Providers == nil {
			return nil, errors.New("providers not configured")
		}
		if route.GatewayProviderID == 0 {
			return nil, errors.New("gateway_provider_id is required")
		}
		p, err := rt.Providers.FindByID(route.GatewayProviderID)
		if err != nil {
			return nil, fmt.Errorf("provider not found: %w", err)
		}
		if !p.Enabled {
			return nil, fmt.Errorf("provider %q is disabled", p.Name)
		}
		secret, err := rt.Cipher.Decrypt(p.APIKeyCipher)
		if err != nil || strings.TrimSpace(secret) == "" {
			return nil, errors.New("provider api key missing or decrypt failed")
		}
		return &upstreamTarget{
			BaseURL:  strings.TrimRight(p.BaseURL, "/"),
			APIKey:   secret,
			Provider: p,
		}, nil
	}
	ch, err := rt.Channels.FindByID(route.SourceChannelID)
	if err != nil {
		return nil, fmt.Errorf("channel not found: %w", err)
	}
	secret, err := rt.Cipher.Decrypt(route.SourceAPIKeyCipher)
	if err != nil || strings.TrimSpace(secret) == "" {
		return nil, errors.New("missing upstream api key; run ensure-keys")
	}
	return &upstreamTarget{
		BaseURL: strings.TrimRight(ch.SiteURL, "/"),
		APIKey:  secret,
		Channel: ch,
	}, nil
}
