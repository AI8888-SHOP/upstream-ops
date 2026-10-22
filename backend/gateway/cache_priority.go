package gateway

import (
	"sort"

	"github.com/bejix/upstream-ops/backend/storage"
)

// cachePriorityNeutralHitRate 是样本不足时的中性分。它让新渠道既不抢占
// 已验证的高命中渠道，也不会因为还没有样本就被排到已知的低命中渠道后面。
const cachePriorityNeutralHitRate = 50.0

// cachePriorityShare 返回优先渠道允许占用的流量比例（1..100）。
// 未配置或配置为 0/负数时按 100 处理，即不做占比限制。
func cachePriorityShare(group *storage.GatewayGroup) int {
	if group == nil || group.CacheHitRatePrioritySharePercent == nil {
		return 100
	}
	share := *group.CacheHitRatePrioritySharePercent
	if share <= 0 || share > 100 {
		return 100
	}
	return share
}

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

// cachePrioritySaturated 判断该来源当前是否已经打满并发。
// 打满的渠道即使命中率最高也不再优先，避免把请求堆在一个来源上排队。
func (rt *Runtime) cachePrioritySaturated(candidate ScoredRoute) bool {
	if rt == nil {
		return false
	}
	registry := rt.upstreamConcurrencyRegistry()
	if registry == nil {
		return false
	}
	identity := loadBalanceIdentity(&candidate.Route)
	active, limit, queued := registry.snapshot(upstreamConcurrencyKey{Kind: identity.Kind, ID: identity.ID})
	if queued > 0 {
		return true
	}
	return limit > 0 && active >= limit
}

// nextCachePrioritySlot 返回 0..99 的轮转槽位，用来把一部分流量分给后面的渠道。
func (s *Service) nextCachePrioritySlot(groupID uint) uint64 {
	if s == nil {
		return 0
	}
	s.cachePriorityMu.Lock()
	defer s.cachePriorityMu.Unlock()
	if s.cachePriorityRot == nil {
		s.cachePriorityRot = make(map[uint]uint64)
	}
	slot := s.cachePriorityRot[groupID]
	s.cachePriorityRot[groupID] = slot + 1
	return slot % 100
}

type cachePriorityEntry struct {
	candidate ScoredRoute
	score     float64
	saturated bool
}

// rankCachePriority 在段内按「未打满优先、命中率高优先」稳定重排。
func (rt *Runtime) rankCachePriority(segment []ScoredRoute, minimum int, neutral float64) []ScoredRoute {
	entries := make([]cachePriorityEntry, len(segment))
	for i, candidate := range segment {
		entries[i] = cachePriorityEntry{
			candidate: candidate,
			score:     cachePriorityScore(candidate, minimum, neutral),
			saturated: rt.cachePrioritySaturated(candidate),
		}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].saturated != entries[j].saturated {
			return !entries[i].saturated
		}
		return entries[i].score > entries[j].score
	})
	out := make([]ScoredRoute, len(entries))
	for i := range entries {
		out[i] = entries[i].candidate
	}
	return out
}

// applyCacheHitRatePriority 在已排好的候选序列上，按上游缓存命中率做一次
// 稳定重排：命中率高的先试，低的往后放。
//
// 约束：
//   - 只在组开启 cache_hit_rate_priority_enabled 时生效；
//   - 并发已打满的来源让位，不会因为命中率高就把请求堆在它身上排队；
//   - cache_hit_rate_priority_share_percent 限制优先渠道的流量占比，超出的
//     请求按顺序轮转到后面的渠道，避免其它渠道完全没有流量；
//   - 普通渠道与兜底渠道分区不变，兜底永远排在普通渠道之后；
//   - 会话亲和（affinity）锁定的路由保持普通分区首位，避免打断正在复用的
//     上游 prompt 缓存；
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
	normal := make([]ScoredRoute, boundary)
	copy(normal, candidates[:boundary])
	reserve := make([]ScoredRoute, len(candidates)-boundary)
	copy(reserve, candidates[boundary:])

	normal = rt.rankCachePriority(normal, minimum, neutral)
	reserve = rt.rankCachePriority(reserve, minimum, neutral)

	// 占比上限：优先渠道不会吃掉全部流量，超出的请求轮转到后面的渠道。
	if share := cachePriorityShare(group); share < 100 && len(normal) > 1 {
		if slot := rt.nextCachePrioritySlot(group.ID); slot >= uint64(share) {
			offset := int(slot) - share
			rotate := 1 + offset%(len(normal)-1)
			rotated := make([]ScoredRoute, 0, len(normal))
			rotated = append(rotated, normal[rotate:]...)
			rotated = append(rotated, normal[:rotate]...)
			normal = rotated
		}
	}

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
