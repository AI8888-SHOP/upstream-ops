package storage

import (
	"testing"
	"time"
)

func TestGatewayGroupsCloneCopiesConfigurationAndResetsRuntimeState(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	routes := NewGatewayRoutes(db)
	rules := NewGatewayResponseRules(db)
	keys := NewGatewayKeys(db)

	source := &GatewayGroup{
		Name:                      "clone-source",
		Description:               "source description",
		Position:                  3,
		Status:                    GatewayGroupStatusDisabled,
		RateSortDirection:         "desc",
		RateResortEnabled:         true,
		MaxBillingRateMultiplier:  1.25,
		LoadBalanceRouteCount:     3,
		ModelMappingJSON:          `{"alias":"upstream"}`,
		ModelsJSON:                `["model-a"]`,
		ModelsMode:                GatewayModelsModeManual,
		RetryEnabled:              false,
		RetryCount:                2,
		FailoverEnabled:           true,
		FailoverMax:               4,
		FailoverOn4xx:             true,
		CooldownSeconds:           17,
		FirstTokenTimeoutSec:      9,
		HedgeEnabled:              true,
		HedgeDelaySeconds:         1.5,
		HedgeMaxParallel:          3,
		HedgeMaxAttempts:          5,
		HedgeVirtualCacheEnabled:  true,
		ResponseValidationEnabled: true,
		ResponseValidationStreamMode: "prefix",
		ResponseValidationPrefixBytes: 1024,
		ResponseValidationPrefixTimeoutMS: 250,
		UserAgent: "clone-agent",
	}
	if err := groups.Create(source); err != nil {
		t.Fatalf("create source group: %v", err)
	}
	sourceKey := &GatewayKey{
		GroupID:         source.ID,
		Name:            "client",
		KeyHash:         "source-hash",
		KeyPrefix:       "sk-source",
		KeyCipher:       "encrypted-source",
		Status:          GatewayKeyStatusDisabled,
		Quota:           42,
		QuotaUsed:       13,
		IPWhitelistJSON: `["10.0.0.1"]`,
		IPBlacklistJSON: `["10.0.0.2"]`,
	}
	if err := keys.Create(sourceKey); err != nil {
		t.Fatalf("create source key: %v", err)
	}
	requestID := "clone-cooldown-request"
	pausedAt := time.Now().Add(-time.Minute)
	until := time.Now().Add(time.Minute)
	route := &GatewayRoute{
		GatewayGroupID:              source.ID,
		Position:                    0,
		SourceKind:                  GatewayRouteSourceProvider,
		GatewayProviderID:           7,
		SourceAPIKeyID:              9,
		SourceAPIKeyName:            "provider-key",
		SourceAPIKeyCipher:          "encrypted-upstream-key",
		ModelMappingJSON:            `{"model-a":"provider-a"}`,
		BillingRateMultiplier:       0.75,
		RateLimitAutoDisabled:       true,
		RateLimitAutoDisabledReason: "over multiplier",
		TempUnschedulableUntil:      &until,
		TempUnschedulableReason:     "capacity",
		TempUnschedulableAt:         &pausedAt,
		TempUnschedulableRequestID:  requestID,
		RecoverSuccessStreak:        2,
		Concurrency:                 6,
	}
	if err := routes.SaveForGroup(source.ID, []GatewayRoute{*route}); err != nil {
		t.Fatalf("save source route: %v", err)
	}
	sourceRoutes, err := routes.ListByGroupID(source.ID)
	if err != nil || len(sourceRoutes) != 1 {
		t.Fatalf("list source routes: %v %#v", err, sourceRoutes)
	}
	if err := db.Model(&GatewayRoute{}).Where("id = ?", sourceRoutes[0].ID).Updates(map[string]any{
		"rate_limit_auto_disabled":        true,
		"rate_limit_auto_disabled_reason": "over multiplier",
		"temp_unschedulable_until":        until,
		"temp_unschedulable_reason":       "capacity",
		"temp_unschedulable_at":            pausedAt,
		"temp_unschedulable_request_id":   requestID,
		"recover_success_streak":           2,
	}).Error; err != nil {
		t.Fatalf("seed source route runtime state: %v", err)
	}
	if err := db.Create(&GatewayRouteModelCooldown{
		RouteID:                    sourceRoutes[0].ID,
		Model:                      "model-a",
		TempUnschedulableUntil:     &until,
		TempUnschedulableReason:    "capacity",
		TempUnschedulableAt:        &pausedAt,
		TempUnschedulableRequestID: requestID,
		RecoverSuccessStreak:       2,
	}).Error; err != nil {
		t.Fatalf("create source model cooldown: %v", err)
	}
	if err := rules.Create(&GatewayResponseRule{
		GatewayGroupID: source.ID,
		Name:           "capacity",
		Enabled:        true,
		Priority:       10,
		Pattern:        `(?i)capacity`,
		Target:         GatewayResponseRuleTargetAssistantText,
		ModelsJSON:     `["model-a"]`,
		ProtocolsJSON:  `["openai"]`,
	}); err != nil {
		t.Fatalf("create source response rule: %v", err)
	}

	existing := &GatewayGroup{Name: "requested-copy", Status: GatewayGroupStatusActive}
	if err := groups.Create(existing); err != nil {
		t.Fatalf("create existing group: %v", err)
	}
	cloned, err := groups.Clone(source.ID, "requested-copy", []GatewayGroupCloneKey{{
		SourceID:  sourceKey.ID,
		KeyHash:   "clone-hash",
		KeyPrefix: "sk-clone",
		KeyCipher: "encrypted-clone",
	}})
	if err != nil {
		t.Fatalf("clone group: %v", err)
	}
	if cloned.Group.ID == source.ID || cloned.Group.Name != "requested-copy (2)" {
		t.Fatalf("clone group identity = %#v", cloned.Group)
	}
	if cloned.Group.ModelMappingJSON != source.ModelMappingJSON || cloned.Group.ModelsJSON != source.ModelsJSON ||
		cloned.Group.HedgeMaxParallel != source.HedgeMaxParallel || !cloned.Group.HedgeVirtualCacheEnabled {
		t.Fatalf("clone group configuration was not copied: %#v", cloned.Group)
	}

	clonedRoutes, err := routes.ListByGroupID(cloned.Group.ID)
	if err != nil || len(clonedRoutes) != 1 {
		t.Fatalf("list cloned routes: %v %#v", err, clonedRoutes)
	}
	clonedRoute := clonedRoutes[0]
	if clonedRoute.ID == sourceRoutes[0].ID || clonedRoute.GatewayGroupID != cloned.Group.ID ||
		clonedRoute.ModelMappingJSON != route.ModelMappingJSON || clonedRoute.SourceAPIKeyCipher != route.SourceAPIKeyCipher {
		t.Fatalf("clone route configuration = %#v", clonedRoute)
	}
	if clonedRoute.RateLimitAutoDisabled || clonedRoute.RateLimitAutoDisabledReason != "" ||
		clonedRoute.TempUnschedulableUntil != nil || clonedRoute.TempUnschedulableReason != "" ||
		clonedRoute.TempUnschedulableAt != nil || clonedRoute.TempUnschedulableRequestID != "" ||
		clonedRoute.RecoverSuccessStreak != 0 || len(clonedRoute.ModelCooldowns) != 0 {
		t.Fatalf("clone route retained runtime state = %#v", clonedRoute)
	}
	var cooldownCount int64
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", clonedRoute.ID).Count(&cooldownCount).Error; err != nil {
		t.Fatalf("count clone cooldowns: %v", err)
	}
	if cooldownCount != 0 {
		t.Fatalf("clone has %d model cooldown rows", cooldownCount)
	}

	clonedRules, err := rules.ListByGroupID(cloned.Group.ID)
	if err != nil || len(clonedRules) != 1 || clonedRules[0].Pattern != `(?i)capacity` {
		t.Fatalf("clone response rules = %v %#v", err, clonedRules)
	}
	if len(cloned.Keys) != 1 || cloned.Keys[0].Name != "client (copy)" || cloned.Keys[0].KeyHash != "clone-hash" ||
		cloned.Keys[0].KeyCipher != "encrypted-clone" || cloned.Keys[0].QuotaUsed != 0 || cloned.Keys[0].LastUsedAt != nil ||
		cloned.Keys[0].Quota != sourceKey.Quota || cloned.Keys[0].IPWhitelistJSON != sourceKey.IPWhitelistJSON {
		t.Fatalf("clone keys = %#v", cloned.Keys)
	}
}

func TestGatewayGroupsCloneRollsBackOnKeyTemplateMismatch(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	keys := NewGatewayKeys(db)
	source := &GatewayGroup{Name: "clone-rollback", Status: GatewayGroupStatusActive}
	if err := groups.Create(source); err != nil {
		t.Fatalf("create source group: %v", err)
	}
	if err := keys.Create(&GatewayKey{
		GroupID: source.ID, Name: "rollback-key", KeyHash: "rollback-hash", KeyPrefix: "sk-rb", KeyCipher: "encrypted",
	}); err != nil {
		t.Fatalf("create source key: %v", err)
	}
	if _, err := groups.Clone(source.ID, "rollback-target", nil); err == nil {
		t.Fatal("clone with missing key template unexpectedly succeeded")
	}
	if _, err := groups.FindByName("rollback-target"); err == nil {
		t.Fatal("failed clone left target group behind")
	}
}
