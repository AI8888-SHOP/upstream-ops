package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// 一键优化：真实流量 → 规则诊断 → 出方案 → 人工确认 → 应用。
//
// 设计约束（与运营约定一致）：
//   - 只调「首字超时 / 路由降权」，绝不增删渠道、模型和密钥；
//   - 样本不足不动手，单次幅度封顶，改动前留快照可回滚；
//   - 方案必须可解释：每条都带「依据了哪几个数」。

const (
	OptimizerKindFirstTokenTimeout = "first_token_timeout"
	OptimizerKindRouteFallbackOnly = "route_fallback_only"
	OptimizerKindRouteDisable      = "route_disable"

	optimizerDefaultWindowHours = 24
	optimizerDefaultMinAttempts = 50
	optimizerDefaultMinSuccess  = 100
	optimizerDefaultMaxGroups   = 3
	optimizerMinTimeoutSec      = 1
	optimizerMaxTimeoutSec      = 90
	optimizerTimeoutStepSec     = 10
	// 首字超时率超过该比例，说明超时阈值正在吃掉真实请求。
	optimizerTimeoutRateFloor = 0.02
	// 路由成功率门槛：低于禁用线直接停用，低于降权线转为兜底。
	optimizerRouteDisableRate  = 0.50
	optimizerRouteFallbackRate = 0.80
	// 同一网关组的首字超时在此时长内只调整一次，避免规则自我叠加。
	optimizerRepeatGuard = 6 * time.Hour
)

// OptimizerEvidence 是单条方案的数字依据。
type OptimizerEvidence struct {
	Attempts           int64   `json:"attempts"`
	Successes          int64   `json:"successes"`
	Failures           int64   `json:"failures"`
	FirstTokenTimeouts int64   `json:"first_token_timeouts"`
	TimeoutRate        float64 `json:"timeout_rate"`
	SuccessRate        float64 `json:"success_rate"`
	FirstTokenP50MS    int64   `json:"first_token_p50_ms"`
	FirstTokenP95MS    int64   `json:"first_token_p95_ms"`
	FirstTokenP99MS    int64   `json:"first_token_p99_ms"`
}

// OptimizerChange 是一条待确认的配置调整。
type OptimizerChange struct {
	ID               string            `json:"id"`
	Kind             string            `json:"kind"`
	GatewayGroupID   uint              `json:"gateway_group_id"`
	GatewayGroupName string            `json:"gateway_group_name"`
	RouteID          uint              `json:"route_id,omitempty"`
	RouteLabel       string            `json:"route_label,omitempty"`
	Model            string            `json:"model,omitempty"`
	Field            string            `json:"field"`
	Before           string            `json:"before"`
	After            string            `json:"after"`
	Reason           string            `json:"reason"`
	Confidence       string            `json:"confidence"`
	Evidence         OptimizerEvidence `json:"evidence"`

	timeoutSec   int
	fallbackOnly *bool
	enabled      *bool
}

// OptimizerPlan 是一次分析的完整产出。
type OptimizerPlan struct {
	GeneratedAt time.Time         `json:"generated_at"`
	WindowHours int               `json:"window_hours"`
	SampleCount int               `json:"sample_count"`
	Changes     []OptimizerChange `json:"changes"`
	Notes       []string          `json:"notes"`
}

// OptimizerApplyInput 是确认后的应用请求。
type OptimizerApplyInput struct {
	ChangeIDs   []string
	WindowHours int
}

type optimizerRouteKey struct {
	group uint
	route uint
}

type optimizerRouteStat struct {
	attempts    int64
	successes   int64
	firstTokens []int64
}

type optimizerGroupStat struct {
	attempts           int64
	successes          int64
	firstTokenTimeouts int64
	firstTokens        []int64
}

// BuildOptimizerPlan 分析窗口内的真实流量并产出待确认方案。
func (s *Service) BuildOptimizerPlan(ctx context.Context, windowHours int) (*OptimizerPlan, error) {
	if s == nil || s.Usage == nil {
		return nil, fmt.Errorf("gateway usage storage is unavailable")
	}
	if windowHours <= 0 {
		windowHours = optimizerDefaultWindowHours
	}
	if windowHours > 24*14 {
		windowHours = 24 * 14
	}
	now := time.Now()
	from := now.Add(-time.Duration(windowHours) * time.Hour)
	samples, err := s.Usage.GatewayOptimizerSamples(from, now, 0)
	if err != nil {
		return nil, err
	}
	plan := &OptimizerPlan{
		GeneratedAt: now, WindowHours: windowHours, SampleCount: len(samples),
		Changes: []OptimizerChange{}, Notes: []string{},
	}
	if len(samples) == 0 {
		plan.Notes = append(plan.Notes, "窗口内没有流量样本，无法给出建议。")
		return plan, nil
	}

	groups := map[uint]*optimizerGroupStat{}
	routes := map[optimizerRouteKey]*optimizerRouteStat{}
	for _, sample := range samples {
		if sample.GatewayGroupID == 0 {
			continue
		}
		group := groups[sample.GatewayGroupID]
		if group == nil {
			group = &optimizerGroupStat{}
			groups[sample.GatewayGroupID] = group
		}
		group.attempts++
		if sample.Success {
			group.successes++
			if sample.FirstTokenMS != nil && *sample.FirstTokenMS > 0 {
				group.firstTokens = append(group.firstTokens, *sample.FirstTokenMS)
			}
		} else if optimizerIsFirstTokenTimeout(sample.ErrorMessage) {
			group.firstTokenTimeouts++
		}
		if sample.RouteID == 0 {
			continue
		}
		key := optimizerRouteKey{group: sample.GatewayGroupID, route: sample.RouteID}
		route := routes[key]
		if route == nil {
			route = &optimizerRouteStat{}
			routes[key] = route
		}
		route.attempts++
		if sample.Success {
			route.successes++
			if sample.FirstTokenMS != nil && *sample.FirstTokenMS > 0 {
				route.firstTokens = append(route.firstTokens, *sample.FirstTokenMS)
			}
		}
	}

	groupItems, err := s.Groups.List()
	if err != nil {
		return nil, err
	}
	groupByID := make(map[uint]storage.GatewayGroup, len(groupItems))
	for _, item := range groupItems {
		groupByID[item.ID] = item
	}

	// 同一个组短时间内不重复抬高首字超时，否则规则会自我叠加。
	recent, _ := s.Usage.RecentOptimizerGroupIDs(now.Add(-optimizerRepeatGuard))
	for groupID := range groups {
		if _, done := recent[groupID]; done {
			if group, ok := groupByID[groupID]; ok {
				plan.Notes = append(plan.Notes, fmt.Sprintf("网关组「%s」近期已优化过，本轮跳过首字超时调整。", group.Name))
			}
		}
	}
	for groupID, stat := range groups {
		group, ok := groupByID[groupID]
		if !ok {
			continue
		}
		if _, done := recent[groupID]; done {
			continue
		}
		if change, ok := optimizerFirstTokenChange(group, stat); ok {
			plan.Changes = append(plan.Changes, change)
		}
	}
	for key, stat := range routes {
		group, ok := groupByID[key.group]
		if !ok {
			continue
		}
		route, err := s.Routes.FindByID(key.route)
		if err != nil || route == nil {
			continue
		}
		peers, err := s.Routes.ListByGroupID(key.group)
		if err != nil {
			continue
		}
		if change, ok := optimizerRouteChange(group, *route, peers, stat); ok {
			plan.Changes = append(plan.Changes, change)
		}
	}

	sort.SliceStable(plan.Changes, func(i, j int) bool {
		return optimizerChangeWeight(plan.Changes[i]) > optimizerChangeWeight(plan.Changes[j])
	})
	if len(plan.Changes) == 0 {
		plan.Notes = append(plan.Notes, "当前配置与实测数据吻合，没有需要调整的项。")
	}
	plan.Notes = append(plan.Notes, fmt.Sprintf("分析 %d 条 attempt，覆盖 %d 个网关组、%d 条路由。", len(samples), len(groups), len(routes)))
	return plan, nil
}

// optimizerFirstTokenChange 判断首字超时是否需要放宽。
// 关键信号是「首字超时占成功+超时的比例」，而不是成功请求的分位——
// 被打断的请求不会留下 first_token_ms，只看分位会系统性低估真实等待。
func optimizerFirstTokenChange(group storage.GatewayGroup, stat *optimizerGroupStat) (OptimizerChange, bool) {
	current := group.FirstTokenTimeoutSec
	if current <= 0 || stat == nil {
		return OptimizerChange{}, false
	}
	total := stat.successes + stat.firstTokenTimeouts
	if stat.successes < optimizerDefaultMinSuccess || total == 0 {
		return OptimizerChange{}, false
	}
	timeoutRate := float64(stat.firstTokenTimeouts) / float64(total)
	if timeoutRate < optimizerTimeoutRateFloor {
		return OptimizerChange{}, false
	}
	p50, p95, p99 := optimizerPercentiles(stat.firstTokens)
	// 目标值同时看两件事：把现有成功请求的慢尾包进来（p95×2），
	// 以及相对现状至少放宽一档，避免每轮只涨一点点磨洋工。
	target := int(math.Ceil(float64(p95) * 2 / 1000))
	if target < current+optimizerTimeoutStepSec {
		target = current + optimizerTimeoutStepSec
	}
	if target > current*2 {
		target = current * 2
	}
	if target > optimizerMaxTimeoutSec {
		target = optimizerMaxTimeoutSec
	}
	if target <= current {
		return OptimizerChange{}, false
	}
	confidence := "medium"
	if timeoutRate >= 0.05 && stat.firstTokenTimeouts >= 20 {
		confidence = "high"
	}
	return OptimizerChange{
		ID:               fmt.Sprintf("%s-%d", OptimizerKindFirstTokenTimeout, group.ID),
		Kind:             OptimizerKindFirstTokenTimeout,
		GatewayGroupID:   group.ID,
		GatewayGroupName: group.Name,
		Field:            "first_token_timeout_sec",
		Before:           fmt.Sprintf("%d 秒", current),
		After:            fmt.Sprintf("%d 秒", target),
		Confidence:       confidence,
		Reason: fmt.Sprintf("近窗口内首字超时 %d 次，占可计次请求的 %.1f%%；成功请求首字 p95 为 %.1f 秒，当前阈值 %d 秒正在吃掉本可成功的请求。",
			stat.firstTokenTimeouts, timeoutRate*100, float64(p95)/1000, current),
		Evidence: OptimizerEvidence{
			Attempts: stat.attempts, Successes: stat.successes,
			Failures: stat.attempts - stat.successes, FirstTokenTimeouts: stat.firstTokenTimeouts,
			TimeoutRate: timeoutRate, SuccessRate: optimizerRate(stat.successes, stat.attempts),
			FirstTokenP50MS: p50, FirstTokenP95MS: p95, FirstTokenP99MS: p99,
		},
		timeoutSec: target,
	}, true
}

// optimizerRouteChange 判断路由是否需要降权或停用。
func optimizerRouteChange(group storage.GatewayGroup, route storage.GatewayRoute, peers []storage.GatewayRoute, stat *optimizerRouteStat) (OptimizerChange, bool) {
	if stat == nil || stat.attempts < optimizerDefaultMinAttempts || !route.Enabled {
		return OptimizerChange{}, false
	}
	// 组内唯一可用路由不自动动，否则等于把整组打死。
	available := 0
	for _, peer := range peers {
		if peer.Enabled && peer.ID != route.ID {
			available++
		}
	}
	if available == 0 {
		return OptimizerChange{}, false
	}
	rate := optimizerRate(stat.successes, stat.attempts)
	p50, p95, p99 := optimizerPercentiles(stat.firstTokens)
	base := OptimizerChange{
		GatewayGroupID:   group.ID,
		GatewayGroupName: group.Name,
		RouteID:          route.ID,
		RouteLabel:       optimizerRouteLabel(route),
		Evidence: OptimizerEvidence{
			Attempts: stat.attempts, Successes: stat.successes, Failures: stat.attempts - stat.successes,
			SuccessRate: rate, FirstTokenP50MS: p50, FirstTokenP95MS: p95, FirstTokenP99MS: p99,
		},
	}
	if rate < optimizerRouteDisableRate && !route.FallbackOnly {
		disabled := false
		base.ID = fmt.Sprintf("%s-%d", OptimizerKindRouteDisable, route.ID)
		base.Kind = OptimizerKindRouteDisable
		base.Field = "enabled"
		base.Before = "启用"
		base.After = "停用"
		base.Confidence = "high"
		base.Reason = fmt.Sprintf("近窗口 %d 次尝试成功率仅 %.1f%%，低于停用线 %.0f%%，组内还有 %d 条可用路由，停用不会切断服务。",
			stat.attempts, rate*100, optimizerRouteDisableRate*100, available)
		base.enabled = &disabled
		return base, true
	}
	if rate < optimizerRouteFallbackRate && !route.FallbackOnly {
		fallback := true
		base.ID = fmt.Sprintf("%s-%d", OptimizerKindRouteFallbackOnly, route.ID)
		base.Kind = OptimizerKindRouteFallbackOnly
		base.Field = "fallback_only"
		base.Before = "主用"
		base.After = "仅兜底"
		base.Confidence = "medium"
		base.Reason = fmt.Sprintf("近窗口 %d 次尝试成功率 %.1f%%，低于主用线 %.0f%%，建议降为兜底，让健康路由承接主力流量。",
			stat.attempts, rate*100, optimizerRouteFallbackRate*100)
		base.fallbackOnly = &fallback
		return base, true
	}
	return OptimizerChange{}, false
}

// ApplyOptimizerPlan 应用已确认的方案，逐条做漂移校验后落地。
func (s *Service) ApplyOptimizerPlan(input OptimizerApplyInput) ([]OptimizerChange, error) {
	plan, err := s.BuildOptimizerPlan(context.Background(), input.WindowHours)
	if err != nil {
		return nil, err
	}
	wanted := map[string]struct{}{}
	for _, id := range input.ChangeIDs {
		wanted[strings.TrimSpace(id)] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil, fmt.Errorf("没有选中任何调整项")
	}
	touchedGroups := map[uint]struct{}{}
	applied := make([]OptimizerChange, 0, len(wanted))
	for _, change := range plan.Changes {
		if _, ok := wanted[change.ID]; !ok {
			continue
		}
		if _, seen := touchedGroups[change.GatewayGroupID]; !seen {
			if len(touchedGroups) >= optimizerDefaultMaxGroups {
				return applied, fmt.Errorf("单次最多调整 %d 个网关组，请分批应用", optimizerDefaultMaxGroups)
			}
			touchedGroups[change.GatewayGroupID] = struct{}{}
		}
		if err := s.applyOptimizerChange(change); err != nil {
			return applied, err
		}
		applied = append(applied, change)
	}
	if len(applied) == 0 {
		return nil, fmt.Errorf("选中的调整项已不再适用，请重新分析")
	}
	groupIDs := make([]uint, 0, len(touchedGroups))
	for id := range touchedGroups {
		groupIDs = append(groupIDs, id)
	}
	if err := s.Usage.RecordOptimizerRun(plan.WindowHours, groupIDs, OptimizerPlanJSON(applied)); err != nil && s.Log != nil {
		s.Log.Warn("record optimizer run failed", "error", err)
	}
	return applied, nil
}

func (s *Service) applyOptimizerChange(change OptimizerChange) error {
	switch change.Kind {
	case OptimizerKindFirstTokenTimeout:
		group, err := s.Groups.FindByID(change.GatewayGroupID)
		if err != nil {
			return err
		}
		// 漂移校验：分析之后有人手改过，就不能按旧方案覆盖。
		if group.FirstTokenTimeoutSec > 0 && fmt.Sprintf("%d 秒", group.FirstTokenTimeoutSec) != change.Before {
			return fmt.Errorf("网关组「%s」的首字超时已被改动，请重新分析", group.Name)
		}
		group.FirstTokenTimeoutSec = change.timeoutSec
		return s.Groups.Update(group)
	case OptimizerKindRouteFallbackOnly, OptimizerKindRouteDisable:
		route, err := s.Routes.FindByID(change.RouteID)
		if err != nil {
			return err
		}
		if change.fallbackOnly != nil {
			route.FallbackOnly = *change.fallbackOnly
		}
		if change.enabled != nil {
			route.Enabled = *change.enabled
		}
		return s.Routes.Update(route)
	}
	return fmt.Errorf("未知的调整类型 %q", change.Kind)
}

func optimizerIsFirstTokenTimeout(message string) bool {
	if strings.TrimSpace(message) == "" {
		return false
	}
	return strings.Contains(message, "首字超时") || strings.Contains(message, "first token timeout")
}

func optimizerRate(part, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(part) / float64(total)
}

func optimizerPercentiles(values []int64) (p50, p95, p99 int64) {
	if len(values) == 0 {
		return 0, 0, 0
	}
	sorted := make([]int64, len(values))
	copy(sorted, values)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(ratio float64) int64 {
		index := int(math.Round(ratio * float64(len(sorted)-1)))
		if index < 0 {
			index = 0
		}
		if index >= len(sorted) {
			index = len(sorted) - 1
		}
		return sorted[index]
	}
	return at(0.50), at(0.95), at(0.99)
}

func optimizerRouteLabel(route storage.GatewayRoute) string {
	label := fmt.Sprintf("路由 #%d", route.ID)
	if route.SourceChannelID > 0 {
		label = fmt.Sprintf("路由 #%d · 渠道 %d", route.ID, route.SourceChannelID)
	} else if route.GatewayProviderID > 0 {
		label = fmt.Sprintf("路由 #%d · 直连 %d", route.ID, route.GatewayProviderID)
	}
	if name := strings.TrimSpace(route.SourceGroupName); name != "" {
		label += " · " + name
	}
	return label
}

func optimizerChangeWeight(change OptimizerChange) float64 {
	weight := change.Evidence.TimeoutRate + (1 - change.Evidence.SuccessRate)
	if change.Confidence == "high" {
		weight += 0.5
	}
	return weight
}

// OptimizerPlanJSON 便于把快照写进审计字段。
func OptimizerPlanJSON(changes []OptimizerChange) string {
	encoded, _ := json.Marshal(changes)
	return string(encoded)
}
