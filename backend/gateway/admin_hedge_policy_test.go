package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestCreateAndUpdateGroupHedgePolicyBounds(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := NewService(
		groups,
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		nil,
		nil,
	)

	enabled := true
	delay := 0.01
	parallel := 999
	attempts := 1
	mode := "full_buffer"
	prefixBytes := 1
	prefixTimeout := 999999
	virtualCache := true
	group, err := svc.CreateGroup(CreateGroupInput{
		Name:                              "hedge-bounds",
		HedgeEnabled:                      &enabled,
		HedgeDelaySeconds:                 &delay,
		HedgeMaxParallel:                  &parallel,
		HedgeMaxAttempts:                  &attempts,
		ResponseValidationEnabled:         &enabled,
		ResponseValidationStreamMode:      &mode,
		ResponseValidationPrefixBytes:     &prefixBytes,
		ResponseValidationPrefixTimeoutMS: &prefixTimeout,
		HedgeVirtualCacheEnabled:          &virtualCache,
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if !group.HedgeEnabled || group.HedgeDelaySeconds != 0.1 ||
		group.HedgeMaxParallel != config.MaxGatewayHedgeParallel ||
		group.HedgeMaxAttempts != config.MaxGatewayHedgeParallel {
		t.Fatalf("hedge policy = %#v", group)
	}
	if !group.HedgeVirtualCacheEnabled {
		t.Fatal("hedge_virtual_cache_enabled = false, want true")
	}
	if !group.ResponseValidationEnabled || group.ResponseValidationStreamMode != "prefix" ||
		group.ResponseValidationPrefixBytes != 1024 || group.ResponseValidationPrefixTimeoutMS != 30000 {
		t.Fatalf("response validation policy = %#v", group)
	}

	delay = 500
	parallel = 1
	attempts = 2
	virtualCache = false
	updated, err := svc.UpdateGroup(group.ID, UpdateGroupInput{
		HedgeDelaySeconds:        &delay,
		HedgeMaxParallel:         &parallel,
		HedgeMaxAttempts:         &attempts,
		HedgeVirtualCacheEnabled: &virtualCache,
	})
	if err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	if updated.HedgeDelaySeconds != 300 || updated.HedgeMaxParallel != 1 || updated.HedgeMaxAttempts != 2 {
		t.Fatalf("updated hedge policy = %#v", updated)
	}
	if updated.HedgeVirtualCacheEnabled {
		t.Fatal("updated hedge_virtual_cache_enabled = true, want false")
	}
	defaultGroup, err := svc.CreateGroup(CreateGroupInput{Name: "hedge-default"})
	if err != nil {
		t.Fatalf("CreateGroup default: %v", err)
	}
	if defaultGroup.HedgeVirtualCacheEnabled {
		t.Fatal("default hedge_virtual_cache_enabled = true, want false")
	}
}

func TestGatewayGroupVirtualCachePercentValidation(t *testing.T) {
	db := openGatewayTestDB(t)
	svc := NewService(
		storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db),
		nil, nil, nil,
	)
	percent := 37
	group, err := svc.CreateGroup(CreateGroupInput{Name: "virtual-cache-percent", VirtualCachePercent: &percent})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if group.VirtualCachePercent != 37 {
		t.Fatalf("virtual_cache_percent = %d, want 37", group.VirtualCachePercent)
	}
	percent = 101
	if _, err := svc.UpdateGroup(group.ID, UpdateGroupInput{VirtualCachePercent: &percent}); err == nil {
		t.Fatal("UpdateGroup accepted virtual_cache_percent > 100")
	}
	percent = 0
	updated, err := svc.UpdateGroup(group.ID, UpdateGroupInput{VirtualCachePercent: &percent})
	if err != nil {
		t.Fatalf("disable virtual cache: %v", err)
	}
	if updated.VirtualCachePercent != 0 {
		t.Fatalf("virtual_cache_percent = %d, want 0", updated.VirtualCachePercent)
	}
}

func TestResponseValidationRetryCountIsIndependentAndCompatible(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := NewService(
		groups,
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		nil,
		nil,
	)

	retryCount := 3
	responseRetries := 99
	group, err := svc.CreateGroup(CreateGroupInput{
		Name:                         "response-retry-policy",
		RetryCount:                   &retryCount,
		ResponseValidationRetryCount: &responseRetries,
	})
	if err != nil {
		t.Fatalf("CreateGroup: %v", err)
	}
	if group.RetryCount != 3 || group.ResponseValidationRetryCount != 10 {
		t.Fatalf("retry policy = retry=%d response=%d, want 3/10", group.RetryCount, group.ResponseValidationRetryCount)
	}

	zero := 0
	updated, err := svc.UpdateGroup(group.ID, UpdateGroupInput{ResponseValidationRetryCount: &zero})
	if err != nil {
		t.Fatalf("UpdateGroup: %v", err)
	}
	if updated.ResponseValidationRetryCount != 0 || updated.RetryCount != 3 {
		t.Fatalf("updated retry policy = retry=%d response=%d, want 3/0", updated.RetryCount, updated.ResponseValidationRetryCount)
	}

	defaultGroup, err := svc.CreateGroup(CreateGroupInput{Name: "response-retry-default"})
	if err != nil {
		t.Fatalf("CreateGroup default: %v", err)
	}
	if defaultGroup.ResponseValidationRetryCount != -1 {
		t.Fatalf("default response retry count = %d, want -1", defaultGroup.ResponseValidationRetryCount)
	}
}
