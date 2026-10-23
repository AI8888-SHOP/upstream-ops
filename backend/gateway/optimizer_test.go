package gateway

import (
	"context"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// 首字 30 秒吃掉了真实请求时，方案应把阈值放宽到能覆盖慢尾。
func TestOptimizerRaisesFirstTokenTimeoutWhenTimeoutsDominate(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-timeout", FirstTokenTimeoutSec: 30}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	route := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 11}
	createOptimizerRoute(t, db, route)
	// 150 次成功，首字集中在 18-22 秒；另外 20 次首字超时。
	seedOptimizerUsage(t, db, group.ID, route.ID, 150, 20*time.Second, 20, true)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	change := findOptimizerChange(plan, OptimizerKindFirstTokenTimeout)
	if change == nil {
		t.Fatalf("expected a first-token change, got %+v", plan.Changes)
	}
	if change.Before != "30 秒" {
		t.Fatalf("before=%q", change.Before)
	}
	// p95≈20 秒 → 目标 40 秒；同时不低于 current+10。
	if change.After != "40 秒" {
		t.Fatalf("after=%q want 40 秒", change.After)
	}
	if change.Evidence.FirstTokenTimeouts != 20 {
		t.Fatalf("evidence=%+v", change.Evidence)
	}
}

// 成功率高、超时少时不应动超时阈值。
func TestOptimizerLeavesHealthyTimeoutAlone(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-healthy", FirstTokenTimeoutSec: 60}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	route := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 12}
	createOptimizerRoute(t, db, route)
	seedOptimizerUsage(t, db, group.ID, route.ID, 300, 3*time.Second, 1, true)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	if change := findOptimizerChange(plan, OptimizerKindFirstTokenTimeout); change != nil {
		t.Fatalf("unexpected change: %+v", change)
	}
}

// 成功率低于停用线的路由应被建议停用；组内仍有其他可用路由。
func TestOptimizerDisablesFailingRoute(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-route", FirstTokenTimeoutSec: 60}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	bad := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 13}
	good := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 14}
	for _, route := range []*storage.GatewayRoute{bad, good} {
		createOptimizerRoute(t, db, route)
	}
	seedOptimizerUsage(t, db, group.ID, bad.ID, 20, 2*time.Second, 60, false)
	seedOptimizerUsage(t, db, group.ID, good.ID, 200, 2*time.Second, 0, true)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	change := findOptimizerChange(plan, OptimizerKindRouteDisable)
	if change == nil {
		t.Fatalf("expected a disable change, got %+v", plan.Changes)
	}
	if change.RouteID != bad.ID {
		t.Fatalf("route=%d want %d", change.RouteID, bad.ID)
	}
}

// 组内唯一可用路由不允许被自动停用，否则等于打断整组服务。
func TestOptimizerKeepsLastRoute(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-solo", FirstTokenTimeoutSec: 60}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	only := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 15}
	createOptimizerRoute(t, db, only)
	seedOptimizerUsage(t, db, group.ID, only.ID, 10, 2*time.Second, 80, false)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range plan.Changes {
		if change.RouteID == only.ID {
			t.Fatalf("solo route must not be changed: %+v", change)
		}
	}
}

// 样本不足时保持沉默，避免小流量噪声导致频繁抖动。
func TestOptimizerIgnoresLowVolume(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-quiet", FirstTokenTimeoutSec: 30}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	route := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 16}
	createOptimizerRoute(t, db, route)
	seedOptimizerUsage(t, db, group.ID, route.ID, 5, 20*time.Second, 5, false)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 0 {
		t.Fatalf("low volume must stay silent: %+v", plan.Changes)
	}
}

// 应用后配置真的落地，且分析期间被手工改动过的项会被拒绝。
func TestOptimizerApplyWritesThroughAndDetectsDrift(t *testing.T) {
	db := openGatewayTestDB(t)
	service := &Service{
		Groups: storage.NewGatewayGroups(db),
		Routes: storage.NewGatewayRoutes(db),
		Usage:  storage.NewGatewayUsageLogs(db),
	}
	group := &storage.GatewayGroup{Name: "optimizer-apply", FirstTokenTimeoutSec: 30}
	if err := service.Groups.Create(group); err != nil {
		t.Fatal(err)
	}
	route := &storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceChannelID: 17}
	createOptimizerRoute(t, db, route)
	seedOptimizerUsage(t, db, group.ID, route.ID, 150, 20*time.Second, 20, true)
	plan, err := service.BuildOptimizerPlan(context.Background(), 24)
	if err != nil {
		t.Fatal(err)
	}
	change := findOptimizerChange(plan, OptimizerKindFirstTokenTimeout)
	if change == nil {
		t.Fatalf("expected a change, got %+v", plan.Changes)
	}
	applied, err := service.ApplyOptimizerPlan(OptimizerApplyInput{ChangeIDs: []string{change.ID}, WindowHours: 24})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if len(applied) != 1 {
		t.Fatalf("applied=%+v", applied)
	}
	stored, err := service.Groups.FindByID(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.FirstTokenTimeoutSec != 40 {
		t.Fatalf("timeout=%d want 40", stored.FirstTokenTimeoutSec)
	}
	// 漂移：有人把配置改回 30 秒后，旧方案不得再次覆盖。
	if _, err := service.ApplyOptimizerPlan(OptimizerApplyInput{ChangeIDs: []string{change.ID}, WindowHours: 24}); err == nil {
		t.Fatal("stale change should be rejected")
	}
}

// 解释层默认走确定性规则文案，不依赖外部模型。
func TestOptimizerExplainFallsBackToRules(t *testing.T) {
	service := &Service{}
	plan := &OptimizerPlan{
		WindowHours: 24, SampleCount: 320,
		Changes: []OptimizerChange{{
			ID: "first_token_timeout-1", Kind: OptimizerKindFirstTokenTimeout,
			GatewayGroupID: 1, GatewayGroupName: "g", Before: "30 秒", After: "60 秒",
			Reason: "首字超时 12 次",
		}},
	}
	text, source := service.ExplainOptimizerPlan(context.Background(), plan, nil)
	if source != "rule" {
		t.Fatalf("source=%q", source)
	}
	if text == "" {
		t.Fatal("empty explanation")
	}
}

func findOptimizerChange(plan *OptimizerPlan, kind string) *OptimizerChange {
	for index := range plan.Changes {
		if plan.Changes[index].Kind == kind {
			return &plan.Changes[index]
		}
	}
	return nil
}

// seedOptimizerUsage 写入 successes 条成功样本与 timeouts 条首字超时样本。
func seedOptimizerUsage(t *testing.T, db interface {
	Create(value any) *gorm.DB
}, groupID, routeID uint, successes int, firstToken time.Duration, timeouts int, healthy bool) {
	t.Helper()
	now := time.Now()
	rows := make([]storage.GatewayUsageLog, 0, successes+timeouts)
	for i := 0; i < successes; i++ {
		ms := firstToken.Milliseconds()
		rows = append(rows, storage.GatewayUsageLog{
			GatewayGroupID: groupID, RouteID: routeID, RequestID: "opt-ok",
			AttemptStatus: storage.GatewayAttemptStatusAccepted, Success: true,
			FirstTokenMS: &ms, UpstreamModel: "m", RequestedModel: "m",
			CreatedAt: now.Add(-time.Duration(i) * time.Minute),
		})
	}
	for i := 0; i < timeouts; i++ {
		rows = append(rows, storage.GatewayUsageLog{
			GatewayGroupID: groupID, RouteID: routeID, RequestID: "opt-timeout",
			AttemptStatus: storage.GatewayAttemptStatusError, Success: false,
			ErrorMessage:  "首字超时（30s）：未在限定时间内收到上游首字节，已主动断开",
			UpstreamModel: "m", RequestedModel: "m",
			CreatedAt: now.Add(-time.Duration(i) * time.Minute),
		})
	}
	for index := range rows {
		if err := db.Create(&rows[index]).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func createOptimizerRoute(t *testing.T, db *gorm.DB, route *storage.GatewayRoute) {
	t.Helper()
	// position 在组内唯一，按当前数量顺排，避免测试之间互相踩索引。
	var count int64
	if err := db.Model(&storage.GatewayRoute{}).Where("gateway_group_id = ?", route.GatewayGroupID).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	route.Position = int(count)
	if err := db.Create(route).Error; err != nil {
		t.Fatal(err)
	}
}
