package gateway

import (
	"sort"

	"github.com/bejix/upstream-ops/backend/storage"
)

// cachePriorityNeutralHitRate 是样本不足时的中性分。它让新渠道既不抢占
// 已验证的高命中渠道，也不会因为还没有样本就被排到已知的低命中渠道后面。
const cachePriorityNeutralHitRate = 50.0

// cachePriorityScore 返回一条候选路由用于排序的缓存命中率分数。
// 样本数不足时退回中性分（组阈值，未配置则 50），避免冷启动渠道被误判。
func cachePriorityScore(candidate ScoredRoute, minimum int, neutral float64) float64 {
	rate := candidate.Route.CacheHealthHitRate
	samples := candidate.Route.CacheHealthRequestCount
	if minimum > 0 && samples < int64(minimum) {
		return neutral
	}
	if rate < 0 {
		return 0
	}
	if rate > 100 {
		return 100
	}
	return rate
}

// applyCacheHitRatePriority 在已排好的候选序列上，按上游缓存命中率做一次
// 稳定重排：命中率高的先试，低的往后放。
//
// 约束：
//   - 只在组开启 cache_hit_rate_priority_enabled 时生效；
//   - 普通渠道与兜底渠道分区不变，兜底永远排在普通渠道之后；
//   - 会话亲和（affinity.PreferredRouteID）锁定的路由保持原位，避免打断
//     正在复用的上游 prompt 缓存；
//   - 价格上限、冷却、黑名单等过滤已在更早的阶段完成，这里只调整相对顺序。
func (rt *Runtime) applyCacheHitRatePriority(candidates []ScoredRoute, group *storage.GatewayGroup, preferredRouteID uint) []ScoredRoute {
	if len(candidates) < 2 || group == nil || !group.CacheHitRatePriorityEnabled {
		return candidates
	}
	policy := resolveCacheHealthPolicy(rt.gatewayRuntime(), group)
	neutral := policy.ThresholdPercent
	if neutral <= 0 {
		neutral = cachePriorityNeutralHitRate
	}
	minimum := policy.MinimumRequests

	// 找到普通渠道与兜底渠道的分界：兜底分区在 adaptive 排序后位于末尾。
	boundary := len(candidates)
	for i, candidate := range candidates {
		if candidate.Route.FallbackOnly {
			boundary = i
			break
		}
	}
	// 复制成独立切片，避免排序时互相覆盖。
	normal := make([]ScoredRoute, boundary)
	copy(normal, candidates[:boundary])
	reserve := make([]ScoredRoute, len(candidates)-boundary)
	copy(reserve, candidates[boundary:])

	byScore := func(segment []ScoredRoute) {
		sort.SliceStable(segment, func(i, j int) bool {
			return cachePriorityScore(segment[i], minimum, neutral) > cachePriorityScore(segment[j], minimum, neutral)
		})
	}
	byScore(normal)
	byScore(reserve)

	// 亲和路由保持普通分区首位：它承载着正在进行的上游 prompt 缓存复用。
	if preferredRouteID != 0 {
		for i := range normal {
			if normal[i].Route.ID == preferredRouteID {
				pin := normal[i]
				normal = append(normal[:i], normal[i+1:]...)
				normal = append([]ScoredRoute{pin}, normal...)
				break
			}
		}
	}
	return append(normal, reserve...)
}
