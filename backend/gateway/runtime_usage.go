// 数据面：用量落库与计费。
package gateway

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func (rt *Runtime) recordUsage(
	key *storage.GatewayKey,
	group *storage.GatewayGroup,
	route *storage.GatewayRoute,
	target *upstreamTarget,
	reqID, requestedModel, upstreamModel, chain string,
	tokens UsageTokens,
	rate, billingRate float64,
	stream bool,
	status int,
	success bool,
	errInfo usageErrorInfo,
	durationMS int64,
	firstTokenMS *int64,
	c *gin.Context,
	meta usageRecordMeta,
) uint {
	priceModel := upstreamModel
	if priceModel == "" {
		priceModel = requestedModel
	}
	// 对齐 sub2api RecordUsage：OpenAI 总输入含 cache 明细 → 拆互斥桶再计费/落库
	tokens = NormalizeUsageBuckets(tokens, protocol.Kind(meta.UpstreamProtocol))
	pricing := rt.Pricing.Resolve(priceModel)
	cost := CalculateCost(pricing, tokens, rate, billingRate)
	reqType := storage.GatewayRequestTypeSync
	if stream {
		reqType = storage.GatewayRequestTypeStream
	}
	billingMode := "token"
	if tokens.ImageOutputTokens > 0 {
		billingMode = "image"
	}
	sourceGroupName := strings.TrimSpace(route.SourceGroupName)
	if sourceGroupName == "" || rt.isSourceGroupIDPlaceholder(sourceGroupName) {
		// 运行时再解析一次：旧路由可能只有 id、name 为空
		if route.SourceGroupID != nil && *route.SourceGroupID > 0 && route.SourceChannelID > 0 {
			if gs := rt.loadGroupsByChannel(context.Background(), []storage.GatewayRoute{*route}); len(gs) > 0 {
				tmp := *route
				rt.enrichRouteSourceGroupName(&tmp, gs[route.SourceChannelID])
				if n := strings.TrimSpace(tmp.SourceGroupName); n != "" && !rt.isSourceGroupIDPlaceholder(n) {
					sourceGroupName = n
				}
			}
		}
	}
	if sourceGroupName == "" && route.SourceGroupID != nil {
		sourceGroupName = fmt.Sprintf("id:%d", *route.SourceGroupID)
	}
	var channelID uint
	var providerID uint
	providerName := ""
	sourceKeyName := strings.TrimSpace(route.SourceAPIKeyName)
	sourceKeyID := route.SourceAPIKeyID
	if target != nil {
		if target.Channel != nil {
			channelID = target.Channel.ID
		}
		if target.Provider != nil {
			providerID = target.Provider.ID
			providerName = target.Provider.Name
			if sourceKeyName == "" {
				sourceKeyName = target.Provider.APIKeyHint
			}
		}
	} else if route != nil {
		channelID = route.SourceChannelID
		providerID = route.GatewayProviderID
	}
	attempt := meta.Attempt
	if attempt <= 0 {
		attempt = 1
	}
	attemptKind := strings.TrimSpace(meta.AttemptKind)
	if attemptKind == "" {
		attemptKind = attemptKindPrimary
	}
	attemptStatus := strings.TrimSpace(meta.AttemptStatus)
	if attemptStatus == "" {
		if success {
			attemptStatus = storage.GatewayAttemptStatusAccepted
		} else {
			attemptStatus = storage.GatewayAttemptStatusError
		}
	}
	settleNow := success && !meta.DeferSettlement
	winnerCost := meta.Winner || settleNow
	var validationRuleID *uint
	if meta.Validation.RuleID > 0 {
		id := meta.Validation.RuleID
		validationRuleID = &id
	}
	validationReason := ""
	if meta.Validation.IsRejected() {
		validationReason = validationErrorInfo(meta.Validation).Summary
	}
	estimatedExtraCost := float64(0)
	if !winnerCost {
		estimatedExtraCost = cost.ActualCost
	}
	item := &storage.GatewayUsageLog{
		GatewayGroupID:    group.ID,
		GatewayKeyID:      key.ID,
		RouteID:           route.ID,
		ChannelID:         channelID,
		GatewayProviderID: providerID,
		ProviderName:      providerName,
		// 路由快照：保存路由换 id 后历史记录仍可展示
		SourceAPIKeyID:   sourceKeyID,
		SourceAPIKeyName: sourceKeyName,
		SourceGroupID:    route.SourceGroupID,
		SourceGroupName:  sourceGroupName,
		RequestID:        reqID,
		Attempt:          attempt,
		AttemptKind:      attemptKind,
		AttemptStatus:    attemptStatus,
		// Winner is marked only by the atomic finalizer after downstream
		// delivery. meta.Winner is still used above to classify extra cost.
		Winner:                false,
		ValidationRuleID:      validationRuleID,
		ValidationRuleName:    meta.Validation.RuleName,
		ValidationReason:      validationReason,
		ValidationPostCommit:  meta.Validation.PostCommit,
		CooldownUntil:         meta.CooldownUntil,
		RequestedModel:        requestedModel,
		UpstreamModel:         upstreamModel,
		ModelMappingChain:     chain,
		InboundEndpoint:       meta.InboundEndpoint,
		UpstreamEndpoint:      meta.UpstreamEndpoint,
		InboundProtocol:       meta.InboundProtocol,
		UpstreamProtocol:      meta.UpstreamProtocol,
		ProtocolConverted:     meta.ProtocolConverted,
		RequestType:           reqType,
		ServiceTier:           meta.ServiceTier,
		ReasoningEffort:       meta.ReasoningEffort,
		BillingMode:           billingMode,
		InputTokens:           tokens.InputTokens,
		OutputTokens:          tokens.OutputTokens,
		CacheCreationTokens:   tokens.CacheCreationTokens,
		CacheReadTokens:       tokens.CacheReadTokens,
		CacheCreation5mTokens: tokens.CacheCreation5mTokens,
		CacheCreation1hTokens: tokens.CacheCreation1hTokens,
		ImageOutputTokens:     tokens.ImageOutputTokens,
		ReasoningTokens:       tokens.ReasoningTokens,
		InputCost:             cost.InputCost,
		OutputCost:            cost.OutputCost,
		CacheCreationCost:     cost.CacheCreationCost,
		CacheReadCost:         cost.CacheReadCost,
		ImageOutputCost:       cost.ImageOutputCost,
		TotalCost:             cost.TotalCost,
		ActualCost:            cost.ActualCost,
		EstimatedCost:         cost.ActualCost,
		EstimatedExtraCost:    estimatedExtraCost,
		AccountStatsCost:      cost.TotalCost,
		RateMultiplier:        rate,
		BillingRateMultiplier: billingRate,
		// 与上游同步账号计费倍率一致：账户侧统计用换算后的有效倍率，而非独立默认 1
		AccountRateMultiplier: rate,
		Stream:                stream,
		StatusCode:            status,
		Success:               success,
		ErrorMessage:          rt.truncateRunes(errInfo.Summary, rt.gatewayRuntime().UsageErrorMsgRunes),
		ErrorType:             errInfo.Type,
		ErrorDetail:           errInfo.Detail,
		UpstreamURL:           meta.UpstreamURL,
		UpstreamErrorBody:     errInfo.UpstreamBody,
		UpstreamErrorHeaders:  errInfo.UpstreamHeaders,
		DurationMS:            durationMS,
		FirstTokenMS:          firstTokenMS,
		IPAddress:             c.ClientIP(),
		UserAgent:             c.Request.UserAgent(),
		CreatedAt:             time.Now(),
	}
	if err := rt.Usage.Create(item); err != nil {
		if rt.Log != nil {
			rt.Log.Error("write usage log failed", "err", err)
		}
		return 0
	}
	if settleNow {
		if _, err := rt.Usage.FinalizeRequest(storage.GatewayFinalizeRequestInput{
			RequestID: reqID, GatewayKeyID: key.ID, Delivered: true,
			WinnerAttempt: attempt, WinnerUsageLogID: item.ID,
		}); err != nil && rt.Log != nil {
			rt.Log.Error("finalize gateway winner failed", "request_id", reqID, "attempt", attempt, "err", err)
		}
	}
	return item.ID
}

func (rt *Runtime) finalizeUsageWinner(reqID string, key *storage.GatewayKey, attempt int, usageLogID uint) error {
	return rt.finalizeUsageWinnerWithSettlement(reqID, key, attempt, usageLogID, storage.GatewayFinalizeRequestInput{})
}

func (rt *Runtime) finalizeUsageWinnerWithSettlement(reqID string, key *storage.GatewayKey, attempt int, usageLogID uint, settlement storage.GatewayFinalizeRequestInput) error {
	if rt == nil || rt.Usage == nil || key == nil {
		return fmt.Errorf("gateway usage settlement is not configured")
	}
	settlement.RequestID = reqID
	settlement.GatewayKeyID = key.ID
	settlement.Delivered = true
	settlement.WinnerAttempt = attempt
	settlement.WinnerUsageLogID = usageLogID
	_, err := rt.Usage.FinalizeRequest(storage.GatewayFinalizeRequestInput{
		RequestID: settlement.RequestID, GatewayKeyID: settlement.GatewayKeyID, Delivered: settlement.Delivered,
		WinnerAttempt: settlement.WinnerAttempt, WinnerUsageLogID: settlement.WinnerUsageLogID,
		BilledCost: settlement.BilledCost, BilledCostSet: settlement.BilledCostSet,
		HedgeTriggered: settlement.HedgeTriggered, VirtualCacheReadEnabled: settlement.VirtualCacheReadEnabled,
		VirtualCacheReadTokens: settlement.VirtualCacheReadTokens,
		VirtualCacheReadCost:   settlement.VirtualCacheReadCost,
		VirtualCacheReason:     settlement.VirtualCacheReason,
	})
	return err
}

// buildVirtualCacheSettlement reclassifies the winner's fresh input as a
// cache-read only after an actual concurrent hedge was launched. The raw
// upstream usage remains in the attempt row; this value is only a user-side
// settlement override.
func (rt *Runtime) buildVirtualCacheSettlement(req *coordinatedForwardRequest, winner *coordinatedForwardAttempt) storage.GatewayFinalizeRequestInput {
	if rt == nil || req == nil || req.group == nil || winner == nil || winner.Tokens.ImageOutputTokens > 0 {
		return storage.GatewayFinalizeRequestInput{}
	}
	reason := strings.TrimSpace(req.virtualCacheReason)
	if reason == "" && req.group.HedgeVirtualCacheEnabled && req.hedgeTriggered {
		// Keep direct helper callers and older runtime tests compatible.
		reason = storage.GatewayVirtualCacheReasonHedge
	}
	switch reason {
	case storage.GatewayVirtualCacheReasonHedge:
		if !req.group.HedgeVirtualCacheEnabled {
			return storage.GatewayFinalizeRequestInput{}
		}
	case storage.GatewayVirtualCacheReasonResponseRuleFailover:
		if !req.group.ResponseValidationVirtualCacheEnabled {
			return storage.GatewayFinalizeRequestInput{}
		}
	case storage.GatewayVirtualCacheReasonProviderGlobal:
		if winner.Target == nil || winner.Target.Provider == nil {
			return storage.GatewayFinalizeRequestInput{}
		}
	default:
		return storage.GatewayFinalizeRequestInput{}
	}
	model := strings.TrimSpace(winner.UpstreamModel)
	if model == "" {
		model = strings.TrimSpace(req.requestedModel)
	}
	tokens := NormalizeUsageBuckets(winner.Tokens, protocol.Kind(winner.UsageMeta.UpstreamProtocol))
	percent := 100
	if reason == storage.GatewayVirtualCacheReasonProviderGlobal {
		var err error
		percent, err = ProviderVirtualCachePercentForModel(winner.Target.Provider, model)
		if err != nil || percent <= 0 {
			return storage.GatewayFinalizeRequestInput{}
		}
	}
	virtualTokens := virtualCacheTokenPercent(tokens.InputTokens, percent)
	if virtualTokens <= 0 {
		return storage.GatewayFinalizeRequestInput{}
	}
	pricing := ModelPricing{}
	if rt.Pricing != nil {
		pricing = rt.Pricing.Resolve(model)
	}
	rawCost := CalculateCost(pricing, tokens, winner.Plan.Candidate.EffectiveRate, winner.Plan.Candidate.BillingRate)
	virtualized := tokens
	virtualized.InputTokens -= virtualTokens
	virtualized.CacheReadTokens += virtualTokens
	billedCost := CalculateCost(pricing, virtualized, winner.Plan.Candidate.EffectiveRate, winner.Plan.Candidate.BillingRate)
	// Downstream virtual-cache signalling is a routing policy, not a function
	// of this process's optional price catalog. Unknown/equal/inverted local
	// prices must not suppress the usage rewrite that Sub2API bills from.
	// Keep the local charge non-increasing when an override is malformed.
	billedActual := billedCost.ActualCost
	if billedActual > rawCost.ActualCost {
		billedActual = rawCost.ActualCost
	}
	accountRate := billedCost.ActualCost
	virtualReadCost := float64(virtualTokens) * pricing.CacheReadPricePerToken
	if billedCost.TotalCost > 0 {
		accountRate /= billedCost.TotalCost
	} else {
		accountRate = 0
	}
	virtualReadCost *= accountRate
	if virtualReadCost > billedActual {
		virtualReadCost = billedActual
	}
	return storage.GatewayFinalizeRequestInput{
		BilledCost:              billedActual,
		BilledCostSet:           true,
		HedgeTriggered:          reason == storage.GatewayVirtualCacheReasonHedge,
		VirtualCacheReadEnabled: true,
		VirtualCacheReadTokens:  virtualTokens,
		VirtualCacheReadCost:    virtualReadCost,
		VirtualCacheReason:      reason,
	}
}

// buildProviderVirtualCacheSettlement creates the accounting override for a
// normal (non-hedged) provider request. The upstream usage remains untouched
// in the attempt log; only the winner settlement uses the virtual read bucket.
func (rt *Runtime) buildProviderVirtualCacheSettlement(
	provider *storage.GatewayProvider,
	model string,
	tokens UsageTokens,
	upstreamKind protocol.Kind,
	rate, billingRate float64,
	eligible bool,
) storage.GatewayFinalizeRequestInput {
	if rt == nil || provider == nil || !eligible || tokens.ImageOutputTokens > 0 {
		return storage.GatewayFinalizeRequestInput{}
	}
	percent, err := ProviderVirtualCachePercentForModel(provider, model)
	if err != nil || percent <= 0 {
		return storage.GatewayFinalizeRequestInput{}
	}
	tokens = NormalizeUsageBuckets(tokens, upstreamKind)
	virtualTokens := virtualCacheTokenPercent(tokens.InputTokens, percent)
	if virtualTokens <= 0 {
		return storage.GatewayFinalizeRequestInput{}
	}
	pricing := ModelPricing{}
	if rt.Pricing != nil {
		pricing = rt.Pricing.Resolve(model)
	}
	rawCost := CalculateCost(pricing, tokens, rate, billingRate)
	virtualized := tokens
	virtualized.InputTokens -= virtualTokens
	virtualized.CacheReadTokens += virtualTokens
	billedCost := CalculateCost(pricing, virtualized, rate, billingRate)
	billedActual := billedCost.ActualCost
	if billedActual > rawCost.ActualCost {
		billedActual = rawCost.ActualCost
	}
	accountRate := billedCost.ActualCost
	virtualReadCost := float64(virtualTokens) * pricing.CacheReadPricePerToken
	if billedCost.TotalCost > 0 {
		accountRate /= billedCost.TotalCost
	} else {
		accountRate = 0
	}
	virtualReadCost *= accountRate
	if virtualReadCost > billedActual {
		virtualReadCost = billedActual
	}
	return storage.GatewayFinalizeRequestInput{
		BilledCost:              billedActual,
		BilledCostSet:           true,
		VirtualCacheReadEnabled: true,
		VirtualCacheReadTokens:  virtualTokens,
		VirtualCacheReadCost:    virtualReadCost,
		VirtualCacheReason:      storage.GatewayVirtualCacheReasonProviderGlobal,
	}
}

func virtualCachePercentForSettlement(settlement storage.GatewayFinalizeRequestInput, provider *storage.GatewayProvider, model string) int {
	if settlement.VirtualCacheReason != storage.GatewayVirtualCacheReasonProviderGlobal {
		return 100
	}
	percent, _ := ProviderVirtualCachePercentForModel(provider, model)
	return percent
}

func (rt *Runtime) finalizeUsageFailure(reqID string, key *storage.GatewayKey) {
	if rt == nil || rt.Usage == nil || key == nil {
		return
	}
	if _, err := rt.Usage.FinalizeFailedRequest(reqID, key.ID); err != nil && rt.Log != nil {
		rt.Log.Error("finalize failed gateway request", "request_id", reqID, "err", err)
	}
}

// buildUpstreamErrorInfo 从转发失败结果拼装可落库的详细错误（截断上限用默认值，便于单测）。
