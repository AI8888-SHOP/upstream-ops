package storage

import (
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Exercise real PostgreSQL row locks, not SQLite's test-only lock behavior.
// Each run owns a separate schema and never reads or truncates application data.
func TestCandyCheckPostgresExclusiveLease(t *testing.T) {
	dsn := os.Getenv("UPSTREAM_OPS_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("PostgreSQL integration DSN not configured")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminSQL.Close() })
	schema := fmt.Sprintf("candy_test_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error; err != nil {
			t.Error(err)
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	db, err := gorm.Open(postgres.Open(u.String()), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
	group := &GatewayGroup{Name: "pg-candy", GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "model", CandyCheckIntervalMinutes: 1, CandyCheckCooldownMinutes: 3}}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1}}); err != nil {
		t.Fatal(err)
	}
	list, err := routes.ListCandyCheckRoutes()
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %d %v", len(list), err)
	}
	key := GatewayCandyCheckConfigKey(group, &list[0], nil)
	start := make(chan struct{})
	claims := make(chan *GatewayRouteCandyCheck, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		manual := i%2 == 0
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimCheck := NewGatewayRoutes(db).ClaimCandyCheck
			if manual {
				claimCheck = NewGatewayRoutes(db).ClaimManualCandyCheck
			}
			claim, err := claimCheck(list[0].ID, key, "model", time.Now(), time.Minute)
			if err != nil {
				t.Error(err)
			}
			if claim != nil {
				claims <- claim
			}
		}()
	}
	close(start)
	wg.Wait()
	close(claims)
	if len(claims) != 1 {
		t.Fatalf("concurrent claims=%d want 1", len(claims))
	}
	claim := <-claims
	if ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "error"}, time.Now()); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	loaded, err := routes.FindByID(list[0].ID)
	if err != nil || loaded.CandyCheck.Blocks(time.Now()) || !loaded.CandyCheck.BackingOff(time.Now()) {
		t.Fatalf("transport backoff missing or mistaken for quality cooldown: %+v %v", loaded, err)
	}
	if err := routes.ClearCandyCheck(list[0].ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	if ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "error"}, time.Now()); err != nil || ok {
		t.Fatalf("stale result restored cooldown: %v %v", ok, err)
	}
}
