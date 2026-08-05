package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestApplyProviderRouteBilling_RawUsesDefault(t *testing.T) {
	p := &storage.GatewayProvider{DefaultBillingRate: 0.01}
	route := &storage.GatewayRoute{
		RateConvertMode:       "raw",
		RateConvertValue:      1,
		BillingRateMultiplier: 1, // 旧前端错误落库
	}
	(*Service)(nil).applyProviderRouteBilling(route, p)
	if route.BillingRateMultiplier != 0.01 {
		t.Fatalf("billing=%v want 0.01", route.BillingRateMultiplier)
	}
	if route.RateConvertValue != 1 {
		t.Fatalf("raw convert value should stay placeholder 1, got %v", route.RateConvertValue)
	}
}

func TestApplyProviderRouteBilling_CustomKeepsValue(t *testing.T) {
	p := &storage.GatewayProvider{DefaultBillingRate: 0.01}
	route := &storage.GatewayRoute{
		RateConvertMode:       "custom",
		RateConvertValue:      0.2,
		BillingRateMultiplier: 1,
	}
	(*Service)(nil).applyProviderRouteBilling(route, p)
	if route.BillingRateMultiplier != 0.2 || route.RateConvertValue != 0.2 {
		t.Fatalf("custom: convert=%v billing=%v", route.RateConvertValue, route.BillingRateMultiplier)
	}
}

func TestApplyProviderRouteBilling_CustomEmptyFallsBack(t *testing.T) {
	p := &storage.GatewayProvider{DefaultBillingRate: 0.05}
	route := &storage.GatewayRoute{
		RateConvertMode:       "custom",
		RateConvertValue:      0,
		BillingRateMultiplier: 0,
	}
	(*Service)(nil).applyProviderRouteBilling(route, p)
	if route.RateConvertValue != 0.05 || route.BillingRateMultiplier != 0.05 {
		t.Fatalf("got convert=%v billing=%v", route.RateConvertValue, route.BillingRateMultiplier)
	}
}

func TestSaveRoutes_ProviderRawBillingFromProviderDefault(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	providersRepo := storage.NewGatewayProviders(db)

	g := &storage.GatewayGroup{
		Name:              "provider-billing",
		Status:            storage.GatewayGroupStatusActive,
		RateSortDirection: "asc",
	}
	if err := groupsRepo.Create(g); err != nil {
		t.Fatalf("create group: %v", err)
	}
	p := &storage.GatewayProvider{
		Name:               "yaohuo",
		BaseURL:            "https://example.test",
		APIKeyCipher:       "cipher",
		DefaultBillingRate: 0.01,
		Enabled:            true,
	}
	if err := providersRepo.Create(p); err != nil {
		t.Fatalf("create provider: %v", err)
	}

	// 监控渠道 0.05：无真实 channel 拉分组时走已落库 billing
	// 直连 raw 误传 billing=1，保存后应被纠正为 0.01 并排到前面
	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, &fakeChannelAPIForResort{}, nil, nil)
	svc.SetProviders(providersRepo)

	saved, err := svc.SaveRoutes(g.ID, []RouteInput{
		{
			SourceKind:            storage.GatewayRouteSourceMonitor,
			SourceChannelID:       9,
			Weight:                1,
			RateConvertMode:       "custom",
			RateConvertValue:      0.05,
			BillingRateMultiplier: 0.05,
			Enabled:               true,
		},
		{
			SourceKind:            storage.GatewayRouteSourceProvider,
			GatewayProviderID:     p.ID,
			Weight:                20,
			RateConvertMode:       "raw",
			RateConvertValue:      1,
			BillingRateMultiplier: 1,
			Enabled:               true,
		},
	})
	if err != nil {
		t.Fatalf("SaveRoutes: %v", err)
	}
	if len(saved) != 2 {
		t.Fatalf("len=%d", len(saved))
	}
	// asc：0.01 provider 应在 0.05 monitor 前
	if saved[0].NormalizeSourceKind() != storage.GatewayRouteSourceProvider {
		t.Fatalf("first should be provider, got kind=%s id=%d billing=%v",
			saved[0].SourceKind, saved[0].ID, saved[0].BillingRateMultiplier)
	}
	if saved[0].BillingRateMultiplier != 0.01 {
		t.Fatalf("provider billing=%v want 0.01", saved[0].BillingRateMultiplier)
	}
	if saved[0].Weight != 20 {
		t.Fatalf("weight=%d", saved[0].Weight)
	}
	if saved[1].BillingRateMultiplier != 0.05 {
		t.Fatalf("monitor billing=%v", saved[1].BillingRateMultiplier)
	}
}

func TestProviderBillingLimitAutoDisablesAndRecoversRoute(t *testing.T) {
	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	routesRepo := storage.NewGatewayRoutes(db)
	providersRepo := storage.NewGatewayProviders(db)

	group := &storage.GatewayGroup{
		Name:                     "provider-rate-limit",
		Status:                   storage.GatewayGroupStatusActive,
		MaxBillingRateMultiplier: 0.5,
	}
	if err := groupsRepo.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	provider := &storage.GatewayProvider{
		Name:               "expensive-provider",
		BaseURL:            "https://example.test",
		APIKeyCipher:       "cipher",
		DefaultBillingRate: 0.8,
		Enabled:            true,
	}
	if err := providersRepo.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}

	svc := NewService(groupsRepo, storage.NewGatewayKeys(db), routesRepo, nil, nil, nil, &fakeChannelAPIForResort{}, nil, nil)
	svc.SetProviders(providersRepo)
	if _, err := svc.SaveRoutes(group.ID, []RouteInput{{
		SourceKind:        storage.GatewayRouteSourceProvider,
		GatewayProviderID: provider.ID,
		RateConvertMode:   "raw",
		Enabled:           true,
	}}); err != nil {
		t.Fatalf("save provider route: %v", err)
	}
	routes, err := routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list provider route: %v", err)
	}
	if len(routes) != 1 || !routes[0].RateLimitAutoDisabled || !routes[0].Enabled {
		t.Fatalf("provider route should be auto-disabled without changing manual state: %+v", routes)
	}

	recovered := 0.2
	if _, err := svc.UpdateProvider(provider.ID, UpdateProviderInput{DefaultBillingRate: &recovered}); err != nil {
		t.Fatalf("update provider rate: %v", err)
	}
	routes, err = routesRepo.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list recovered route: %v", err)
	}
	if len(routes) != 1 || routes[0].RateLimitAutoDisabled || routes[0].BillingRateMultiplier != recovered {
		t.Fatalf("provider route should recover after rate update: %+v", routes)
	}
}
