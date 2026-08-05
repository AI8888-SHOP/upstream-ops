package gateway

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

type fakeChannelAPIForResort struct {
	groups map[uint][]connector.APIKeyGroup
}

func (f *fakeChannelAPIForResort) ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return &connector.APIKeyPage{}, nil
}

func (f *fakeChannelAPIForResort) ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error) {
	return f.groups[channelID], nil
}

func (f *fakeChannelAPIForResort) CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	return nil, nil
}

func (f *fakeChannelAPIForResort) UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	return nil, nil
}

func (f *fakeChannelAPIForResort) RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error) {
	return "", nil
}

func openGatewayTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "gateway-test.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestResortRoutesOnRateScan_OnlyEnabledGroups(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)

	gOn := &storage.GatewayGroup{
		Name:              "resort-on",
		Status:            storage.GatewayGroupStatusActive,
		RateSortDirection: "asc",
		RateResortEnabled: true,
	}
	gOff := &storage.GatewayGroup{
		Name:              "resort-off",
		Status:            storage.GatewayGroupStatusActive,
		RateSortDirection: "asc",
		RateResortEnabled: false,
	}
	if err := groupsRepo.Create(gOn); err != nil {
		t.Fatalf("create on: %v", err)
	}
	if err := groupsRepo.Create(gOff); err != nil {
		t.Fatalf("create off: %v", err)
	}

	gid1, gid2 := int64(11), int64(22)
	// 初始顺序：高倍率在前（与 asc 相反）
	onRoutes := []storage.GatewayRoute{
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &gid1, SourceGroupName: "high", Position: 0, Weight: 1,
			Enabled: true, RateConvertMode: "raw", BillingRateMultiplier: 0.3,
			SourceAPIKeyCipher: "k1",
		},
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &gid2, SourceGroupName: "low", Position: 1, Weight: 1,
			Enabled: true, RateConvertMode: "raw", BillingRateMultiplier: 0.1,
			SourceAPIKeyCipher: "k2",
		},
	}
	offRoutes := []storage.GatewayRoute{
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &gid1, SourceGroupName: "high", Position: 0, Weight: 1,
			Enabled: true, RateConvertMode: "raw", BillingRateMultiplier: 0.3,
			SourceAPIKeyCipher: "k1",
		},
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &gid2, SourceGroupName: "low", Position: 1, Weight: 1,
			Enabled: true, RateConvertMode: "raw", BillingRateMultiplier: 0.1,
			SourceAPIKeyCipher: "k2",
		},
	}
	if err := routesRepo.SaveForGroup(gOn.ID, onRoutes); err != nil {
		t.Fatalf("save on routes: %v", err)
	}
	if err := routesRepo.SaveForGroup(gOff.ID, offRoutes); err != nil {
		t.Fatalf("save off routes: %v", err)
	}

	fake := &fakeChannelAPIForResort{
		groups: map[uint][]connector.APIKeyGroup{
			1: {
				{ID: &gid1, Name: "high", Ratio: 0.3},
				{ID: &gid2, Name: "low", Ratio: 0.1},
			},
		},
	}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, fake, nil, nil)

	svc.ResortRoutesOnRateScan(context.Background())

	onList, err := routesRepo.ListByGroupID(gOn.ID)
	if err != nil {
		t.Fatalf("list on: %v", err)
	}
	if len(onList) != 2 {
		t.Fatalf("on routes len=%d", len(onList))
	}
	// asc：0.1 应在前
	if onList[0].SourceGroupName != "low" || onList[1].SourceGroupName != "high" {
		t.Fatalf("enabled group not resorteded: %+v / %+v", onList[0], onList[1])
	}
	if onList[0].BillingRateMultiplier != 0.1 || onList[1].BillingRateMultiplier != 0.3 {
		t.Fatalf("billing rates not refreshed: %v / %v", onList[0].BillingRateMultiplier, onList[1].BillingRateMultiplier)
	}

	offList, err := routesRepo.ListByGroupID(gOff.ID)
	if err != nil {
		t.Fatalf("list off: %v", err)
	}
	if offList[0].SourceGroupName != "high" || offList[1].SourceGroupName != "low" {
		t.Fatalf("disabled resort group should keep order: %+v / %+v", offList[0], offList[1])
	}
}

func TestUpdateGroup_RateResortTurnedOnReorders(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)

	g := &storage.GatewayGroup{
		Name:              "toggle-on",
		Status:            storage.GatewayGroupStatusActive,
		RateSortDirection: "asc",
		RateResortEnabled: false,
	}
	if err := groupsRepo.Create(g); err != nil {
		t.Fatalf("create: %v", err)
	}
	gidA, gidB := int64(1), int64(2)
	if err := routesRepo.SaveForGroup(g.ID, []storage.GatewayRoute{
		{
			SourceChannelID: 1, SourceGroupID: &gidA, SourceGroupName: "a",
			Position: 0, Weight: 1, Enabled: true, RateConvertMode: "raw",
			BillingRateMultiplier: 0.5, SourceAPIKeyCipher: "x",
		},
		{
			SourceChannelID: 1, SourceGroupID: &gidB, SourceGroupName: "b",
			Position: 1, Weight: 1, Enabled: true, RateConvertMode: "raw",
			BillingRateMultiplier: 0.2, SourceAPIKeyCipher: "y",
		},
	}); err != nil {
		t.Fatalf("save routes: %v", err)
	}

	fake := &fakeChannelAPIForResort{
		groups: map[uint][]connector.APIKeyGroup{
			1: {
				{ID: &gidA, Name: "a", Ratio: 0.5},
				{ID: &gidB, Name: "b", Ratio: 0.2},
			},
		},
	}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, fake, nil, nil)

	on := true
	if _, err := svc.UpdateGroup(g.ID, UpdateGroupInput{RateResortEnabled: &on}); err != nil {
		t.Fatalf("update: %v", err)
	}
	list, err := routesRepo.ListByGroupID(g.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if list[0].SourceGroupName != "b" || list[1].SourceGroupName != "a" {
		t.Fatalf("turn-on should reorder: %+v / %+v", list[0], list[1])
	}
}

func TestSaveRoutes_PersistsLiveGroupRateByID(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)

	group := &storage.GatewayGroup{
		Name:              "save-live-group-rate",
		Status:            storage.GatewayGroupStatusActive,
		RateSortDirection: "asc",
	}
	if err := groupsRepo.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}

	lowID, middleID, highID := int64(60), int64(63), int64(70)
	fake := &fakeChannelAPIForResort{
		groups: map[uint][]connector.APIKeyGroup{
			1: {
				{ID: &highID, Name: "high", Ratio: 0.07},
				// 上游名带尾随空格，保存输入会 trim；应优先按稳定的分组 ID 匹配。
				{ID: &middleID, Name: "Team/plus典韦 🪓 ", Ratio: 0.063},
				{ID: &lowID, Name: "low", Ratio: 0.06},
			},
		},
	}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, fake, nil, nil)

	_, err := svc.SaveRoutes(group.ID, []RouteInput{
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &highID, SourceGroupName: "high", Weight: 1,
			RateConvertMode: "raw", BillingRateMultiplier: 1, Enabled: true,
		},
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &middleID, SourceGroupName: "Team/plus典韦 🪓", Weight: 1,
			RateConvertMode: "raw", BillingRateMultiplier: 1, Enabled: true,
		},
		{
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1,
			SourceGroupID: &lowID, SourceGroupName: "low", Weight: 1,
			RateConvertMode: "raw", BillingRateMultiplier: 1, Enabled: true,
		},
	})
	if err != nil {
		t.Fatalf("SaveRoutes: %v", err)
	}

	persisted, err := routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list persisted routes: %v", err)
	}
	if len(persisted) != 3 {
		t.Fatalf("persisted routes len=%d, want 3", len(persisted))
	}
	if persisted[0].SourceGroupID == nil || *persisted[0].SourceGroupID != lowID ||
		persisted[1].SourceGroupID == nil || *persisted[1].SourceGroupID != middleID ||
		persisted[2].SourceGroupID == nil || *persisted[2].SourceGroupID != highID {
		t.Fatalf("persisted order should be 0.06 < 0.063 < 0.07: %+v", persisted)
	}
	if persisted[1].SourceGroupName != "Team/plus典韦 🪓" {
		t.Fatalf("middle source group name=%q", persisted[1].SourceGroupName)
	}
	if persisted[0].BillingRateMultiplier != 0.06 ||
		persisted[1].BillingRateMultiplier != 0.063 ||
		persisted[2].BillingRateMultiplier != 0.07 {
		t.Fatalf("persisted billing should be 0.06 / 0.063 / 0.07, got %v / %v / %v",
			persisted[0].BillingRateMultiplier,
			persisted[1].BillingRateMultiplier,
			persisted[2].BillingRateMultiplier)
	}
}

func TestRateLimitAutoDisablesOverLimitRoutesWithoutChangingManualState(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{
		Name:                     "rate-limit",
		Status:                   storage.GatewayGroupStatusActive,
		RateSortDirection:        "asc",
		MaxBillingRateMultiplier: 0.5,
	}
	if err := groupsRepo.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	lowID, highID, manualID := int64(1), int64(2), int64(3)
	if err := routesRepo.SaveForGroup(group.ID, []storage.GatewayRoute{
		{SourceChannelID: 1, SourceGroupID: &lowID, SourceGroupName: "low", Position: 0, Enabled: true, RateConvertMode: "raw", SourceAPIKeyCipher: "low"},
		{SourceChannelID: 1, SourceGroupID: &highID, SourceGroupName: "high", Position: 1, Enabled: true, RateConvertMode: "raw", SourceAPIKeyCipher: "high"},
		{SourceChannelID: 1, SourceGroupID: &manualID, SourceGroupName: "manual", Position: 2, Enabled: false, RateConvertMode: "raw", SourceAPIKeyCipher: "manual"},
	}); err != nil {
		t.Fatalf("save routes: %v", err)
	}
	// GORM applies the model's default:true while inserting a zero bool; make
	// the manual-disabled fixture explicit before the guard is evaluated.
	seeded, err := routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list seeded routes: %v", err)
	}
	for i := range seeded {
		seeded[i].SourceAPIKeyCipher = "fixture-key"
		if seeded[i].SourceGroupName == "manual" {
			seeded[i].Enabled = false
		}
		if err := routesRepo.Update(&seeded[i]); err != nil {
			t.Fatalf("prepare route fixture: %v", err)
		}
	}
	fake := &fakeChannelAPIForResort{groups: map[uint][]connector.APIKeyGroup{
		1: {
			{ID: &lowID, Name: "low", Ratio: 0.2},
			{ID: &highID, Name: "high", Ratio: 0.8},
			{ID: &manualID, Name: "manual", Ratio: 0.9},
		},
	}}
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, fake, nil, nil)
	svc.ResortRoutesOnRateScan(context.Background())

	routes, err := routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list routes: %v", err)
	}
	byGroup := make(map[string]storage.GatewayRoute, len(routes))
	for _, route := range routes {
		byGroup[route.SourceGroupName] = route
	}
	if byGroup["high"].Enabled != true || !byGroup["high"].RateLimitAutoDisabled {
		t.Fatalf("high route should be auto-disabled without changing Enabled: %+v", byGroup["high"])
	}
	if byGroup["low"].RateLimitAutoDisabled {
		t.Fatalf("low route should remain schedulable: %+v", byGroup["low"])
	}
	if byGroup["manual"].Enabled || !byGroup["manual"].RateLimitAutoDisabled {
		t.Fatalf("manual disabled route should preserve manual state and auto marker: %+v", byGroup["manual"])
	}

	groupsByChannel := map[uint][]connector.APIKeyGroup{1: fake.groups[1]}
	candidates := SortRoutes(routes, groupsByChannel, "asc", time.Now(), nil)
	if len(candidates) != 1 || candidates[0].Route.SourceGroupName != "low" {
		t.Fatalf("only low route should be schedulable, got %+v", candidates)
	}

	cleared := 1.0
	if _, err := svc.UpdateGroup(group.ID, UpdateGroupInput{MaxBillingRateMultiplier: &cleared}); err != nil {
		t.Fatalf("clear rate limit: %v", err)
	}
	routes, err = routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list routes after recovery: %v", err)
	}
	for _, route := range routes {
		if route.SourceGroupName == "high" && route.RateLimitAutoDisabled {
			t.Fatalf("high route should recover below new limit: %+v", route)
		}
		if route.SourceGroupName == "manual" && route.Enabled {
			t.Fatalf("manual disabled route was unexpectedly re-enabled: %+v", route)
		}
	}
}

func TestRateLimitStateUsesStrictGreaterThanAndZeroDisables(t *testing.T) {
	if disabled, reason := rateLimitState(0.5, 0.5); disabled || reason != "" {
		t.Fatalf("limit equality should remain schedulable: disabled=%v reason=%q", disabled, reason)
	}
	if disabled, reason := rateLimitState(0.5001, 0.5); !disabled || reason == "" {
		t.Fatalf("rate above limit should be disabled: disabled=%v reason=%q", disabled, reason)
	}
	if disabled, reason := rateLimitState(100, 0); disabled || reason != "" {
		t.Fatalf("zero limit should disable the guard: disabled=%v reason=%q", disabled, reason)
	}
}
