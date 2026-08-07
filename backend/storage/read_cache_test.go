package storage

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTTLReadCacheExactInvalidationDoesNotDiscardOtherInflightKey(t *testing.T) {
	cache := newTTLReadCache[string, string](time.Minute, func(value string) string { return value })
	started := make(chan struct{})
	release := make(chan struct{})
	result := make(chan string, 1)
	var loads atomic.Int32
	go func() {
		value, _ := cache.load("group-b", func() (string, error) {
			loads.Add(1)
			close(started)
			<-release
			return "loaded-b", nil
		}, nil)
		result <- value
	}()
	<-started
	cache.invalidate("group-a")
	close(release)
	if got := <-result; got != "loaded-b" {
		t.Fatalf("in-flight result=%q, want loaded-b", got)
	}
	second, err := cache.load("group-b", func() (string, error) {
		loads.Add(1)
		return "unexpected-reload", nil
	}, nil)
	if err != nil || second != "loaded-b" || loads.Load() != 1 {
		t.Fatalf("unrelated invalidation discarded cache: value=%q loads=%d err=%v", second, loads.Load(), err)
	}
}

func TestFindByIDReadCachesHitAndInvalidate(t *testing.T) {
	db := openTestDB(t)

	t.Run("channel", func(t *testing.T) {
		rechargeMultiplier := 2.5
		repo := NewChannels(db)
		item := &Channel{
			Name: "cached-channel", Type: ChannelTypeNewAPI,
			SiteURL: "https://channel.example", Username: "user",
			PasswordCipher: "cipher", MonitorEnabled: true,
			RechargeMultiplier: &rechargeMultiplier,
		}
		if err := repo.Create(item); err != nil {
			t.Fatalf("create channel: %v", err)
		}
		first, err := repo.FindByID(item.ID)
		if err != nil {
			t.Fatalf("find channel: %v", err)
		}
		if err := db.Model(&Channel{}).Where("id = ?", item.ID).UpdateColumn("username", "database-user").Error; err != nil {
			t.Fatalf("update channel directly: %v", err)
		}
		first.Username = "caller-user"
		*first.RechargeMultiplier = 99
		cached, err := repo.FindByID(item.ID)
		if err != nil {
			t.Fatalf("find cached channel: %v", err)
		}
		if cached.Username != "user" || cached.RechargeMultiplier == nil || *cached.RechargeMultiplier != rechargeMultiplier {
			t.Fatalf("channel cache was not isolated: %+v", cached)
		}
		cached.Username = "repository-user"
		if err := repo.Update(cached); err != nil {
			t.Fatalf("update channel through repository: %v", err)
		}
		updated, err := repo.FindByID(item.ID)
		if err != nil || updated.Username != "repository-user" {
			t.Fatalf("channel write did not invalidate cache: %+v err=%v", updated, err)
		}
	})

	t.Run("provider", func(t *testing.T) {
		repo := NewGatewayProviders(db)
		item := &GatewayProvider{
			Name: "cached-provider", BaseURL: "https://provider.example",
			APIKeyCipher: "cipher", Enabled: true,
		}
		if err := repo.Create(item); err != nil {
			t.Fatalf("create provider: %v", err)
		}
		first, err := repo.FindByID(item.ID)
		if err != nil {
			t.Fatalf("find provider: %v", err)
		}
		if err := db.Model(&GatewayProvider{}).Where("id = ?", item.ID).UpdateColumn("base_url", "https://database.example").Error; err != nil {
			t.Fatalf("update provider directly: %v", err)
		}
		cached, err := repo.FindByID(item.ID)
		if err != nil || cached.BaseURL != first.BaseURL {
			t.Fatalf("provider cache miss: %+v err=%v", cached, err)
		}
		cached.BaseURL = "https://repository.example"
		if err := repo.Update(cached); err != nil {
			t.Fatalf("update provider through repository: %v", err)
		}
		updated, err := repo.FindByID(item.ID)
		if err != nil || updated.BaseURL != cached.BaseURL {
			t.Fatalf("provider write did not invalidate cache: %+v err=%v", updated, err)
		}
	})
}

func TestGatewayGroupReadCacheTTLAndWriteInvalidation(t *testing.T) {
	db := openTestDB(t)
	repo := NewGatewayGroups(db)
	now := time.Unix(1_800_000_000, 0)
	repo.readCaches.gatewayGroups.setClockForTest(func() time.Time { return now })

	item := &GatewayGroup{Name: "cached-group", Description: "initial", Status: GatewayGroupStatusActive}
	if err := repo.Create(item); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if _, err := repo.FindByID(item.ID); err != nil {
		t.Fatalf("prime group cache: %v", err)
	}
	if err := db.Model(&GatewayGroup{}).Where("id = ?", item.ID).UpdateColumn("description", "database").Error; err != nil {
		t.Fatalf("update group directly: %v", err)
	}
	cached, err := repo.FindByID(item.ID)
	if err != nil || cached.Description != "initial" {
		t.Fatalf("group cache miss: %+v err=%v", cached, err)
	}

	cached.Description = "repository"
	if err := repo.Update(cached); err != nil {
		t.Fatalf("update group through repository: %v", err)
	}
	updated, err := repo.FindByID(item.ID)
	if err != nil || updated.Description != "repository" {
		t.Fatalf("group write did not invalidate cache: %+v err=%v", updated, err)
	}

	if err := db.Model(&GatewayGroup{}).Where("id = ?", item.ID).UpdateColumn("description", "after-ttl").Error; err != nil {
		t.Fatalf("update group for ttl: %v", err)
	}
	now = now.Add(storageReadCacheTTL)
	afterTTL, err := repo.FindByID(item.ID)
	if err != nil || afterTTL.Description != "after-ttl" {
		t.Fatalf("expired group cache was reused: %+v err=%v", afterTTL, err)
	}
}

func TestGatewayRouteReadCacheInvalidationAndDeepCopy(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	const groupID = 71
	if err := routes.SaveForGroup(groupID, []GatewayRoute{{SourceChannelID: 9, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	initial, err := routes.ListByGroupID(groupID)
	if err != nil || len(initial) != 1 {
		t.Fatalf("list route: %+v err=%v", initial, err)
	}
	routeID := initial[0].ID
	until := time.Now().Add(time.Minute)
	if err := routes.SetModelTempUnschedulable(routeID, "model-a", until, "capacity", time.Now(), "request-a"); err != nil {
		t.Fatalf("set model cooldown: %v", err)
	}

	withCooldown, err := routes.ListByGroupID(groupID)
	if err != nil || len(withCooldown) != 1 {
		t.Fatalf("list route with cooldown: %+v err=%v", withCooldown, err)
	}
	cooldown := withCooldown[0].ModelCooldowns["model-a"]
	if cooldown.TempUnschedulableReason != "capacity" || cooldown.TempUnschedulableUntil == nil {
		t.Fatalf("cooldown not loaded after invalidation: %+v", cooldown)
	}
	withCooldown[0].Weight = 99
	*cooldown.TempUnschedulableUntil = time.Time{}
	cooldown.TempUnschedulableReason = "caller mutation"
	withCooldown[0].ModelCooldowns["model-a"] = cooldown
	withCooldown[0].ModelCooldowns["caller-model"] = GatewayRouteModelCooldown{Model: "caller-model"}

	again, err := routes.ListByGroupID(groupID)
	if err != nil || len(again) != 1 {
		t.Fatalf("list cached route: %+v err=%v", again, err)
	}
	storedCooldown := again[0].ModelCooldowns["model-a"]
	if again[0].Weight != 1 || storedCooldown.TempUnschedulableReason != "capacity" || storedCooldown.TempUnschedulableUntil == nil || storedCooldown.TempUnschedulableUntil.IsZero() {
		t.Fatalf("route cache shared caller-owned data: %+v", again[0])
	}
	if _, exists := again[0].ModelCooldowns["caller-model"]; exists {
		t.Fatal("caller-added model leaked into route cache")
	}

	if err := routes.SetModelTempUnschedulable(routeID, "model-a", until.Add(time.Minute), "updated capacity", time.Now(), "request-b"); err != nil {
		t.Fatalf("update model cooldown: %v", err)
	}
	updated, err := routes.ListByGroupID(groupID)
	if err != nil || len(updated) != 1 || updated[0].ModelCooldowns["model-a"].TempUnschedulableReason != "updated capacity" {
		t.Fatalf("cooldown write did not invalidate route cache: %+v err=%v", updated, err)
	}
}

func TestGatewayKeyReadCacheOnlyStoresUnlimitedKeys(t *testing.T) {
	db := openTestDB(t)
	repo := NewGatewayKeys(db)
	lastUsed := time.Now().Add(-time.Hour)
	unlimited := &GatewayKey{
		GroupID: 1, Name: "unlimited-key", KeyHash: "unlimited-hash",
		KeyPrefix: "sk-unlimited", KeyCipher: "cipher", Status: GatewayKeyStatusActive,
		Quota: 0, LastUsedAt: &lastUsed,
	}
	if err := repo.Create(unlimited); err != nil {
		t.Fatalf("create unlimited key: %v", err)
	}
	first, err := repo.FindByHash(unlimited.KeyHash)
	if err != nil {
		t.Fatalf("find unlimited key: %v", err)
	}
	if err := db.Model(&GatewayKey{}).Where("id = ?", unlimited.ID).UpdateColumn("name", "database-unlimited").Error; err != nil {
		t.Fatalf("update unlimited key directly: %v", err)
	}
	*first.LastUsedAt = time.Time{}
	cached, err := repo.FindByHash(unlimited.KeyHash)
	if err != nil || cached.Name != "unlimited-key" || cached.LastUsedAt == nil || cached.LastUsedAt.IsZero() {
		t.Fatalf("unlimited key cache was not isolated: %+v err=%v", cached, err)
	}
	cached.Name = "repository-unlimited"
	if err := repo.Update(cached); err != nil {
		t.Fatalf("update unlimited key through repository: %v", err)
	}
	updated, err := repo.FindByHash(unlimited.KeyHash)
	if err != nil || updated.Name != "repository-unlimited" {
		t.Fatalf("unlimited key write did not invalidate cache: %+v err=%v", updated, err)
	}

	limited := &GatewayKey{
		GroupID: 1, Name: "limited-key", KeyHash: "limited-hash",
		KeyPrefix: "sk-limited", KeyCipher: "cipher", Status: GatewayKeyStatusActive,
		Quota: 10,
	}
	if err := repo.Create(limited); err != nil {
		t.Fatalf("create limited key: %v", err)
	}
	if _, err := repo.FindByHash(limited.KeyHash); err != nil {
		t.Fatalf("find limited key: %v", err)
	}
	if err := db.Model(&GatewayKey{}).Where("id = ?", limited.ID).UpdateColumn("quota_used", 7.5).Error; err != nil {
		t.Fatalf("update limited key directly: %v", err)
	}
	fresh, err := repo.FindByHash(limited.KeyHash)
	if err != nil || fresh.QuotaUsed != 7.5 {
		t.Fatalf("limited key was cached: %+v err=%v", fresh, err)
	}
}

func TestGatewayGroupDeleteInvalidatesSharedRouteAndKeyCaches(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	routes := NewGatewayRoutes(db)
	keys := NewGatewayKeys(db)
	group := &GatewayGroup{Name: "delete-cached-group", Status: GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{SourceChannelID: 3, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	key := &GatewayKey{
		GroupID: group.ID, Name: "delete-cached-key", KeyHash: "delete-cached-hash",
		KeyPrefix: "sk-delete", KeyCipher: "cipher", Status: GatewayKeyStatusActive,
	}
	if err := keys.Create(key); err != nil {
		t.Fatalf("create key: %v", err)
	}
	if list, err := routes.ListByGroupID(group.ID); err != nil || len(list) != 1 {
		t.Fatalf("prime route cache: %+v err=%v", list, err)
	}
	if _, err := keys.FindByHash(key.KeyHash); err != nil {
		t.Fatalf("prime key cache: %v", err)
	}

	if err := groups.Delete(group.ID); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if list, err := routes.ListByGroupID(group.ID); err != nil || len(list) != 0 {
		t.Fatalf("deleted routes remained cached: %+v err=%v", list, err)
	}
	if _, err := keys.FindByHash(key.KeyHash); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("deleted key remained cached: %v", err)
	}
}
