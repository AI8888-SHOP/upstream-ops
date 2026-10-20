package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func openPriceRepricingPostgres(t *testing.T) *gorm.DB {
	t.Helper()
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
	schema := fmt.Sprintf("price_repricing_test_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error; err != nil {
			t.Error(err)
		}
	})
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " search_path=" + schema
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(5)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&ModelPriceOverride{}, &ModelPriceRepricingJob{}, &GatewayUsageLog{}, &GatewayRequestFinalization{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPriceRepricingPostgres(t *testing.T) {
	t.Run("reference_revenue_backfill", func(t *testing.T) {
		assertReferenceRevenueBackfillSchedulesOnce(t, openPriceRepricingPostgres(t))
	})
	t.Run("cache_ttl_field_mapping", func(t *testing.T) {
		assertPriceRepricingCacheTTLFieldMapping(t, openPriceRepricingPostgres(t))
	})

	t.Run("two_workers_share_committed_cursor", func(t *testing.T) {
		db := openPriceRepricingPostgres(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		repo := NewModelPriceOverrides(db)
		rows := make([]GatewayUsageLog, 401)
		for i := range rows {
			rows[i] = GatewayUsageLog{RequestID: fmt.Sprintf("parallel-price-%d", i), RequestedModel: "x", ActualCost: 1, BilledCost: 0.75, InputTokens: 10, CreatedAt: time.Now()}
		}
		if err := db.CreateInBatches(rows, 100).Error; err != nil {
			t.Fatal(err)
		}
		if _, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "x", InputPricePerToken: 2, Enabled: true}}, 0, false); err != nil {
			t.Fatal(err)
		}
		var mu sync.Mutex
		visits := map[uint]int{}
		calculate := func(prices []ModelPriceOverride) func(GatewayUsageLog) map[string]any {
			return func(row GatewayUsageLog) map[string]any {
				mu.Lock()
				visits[row.ID]++
				mu.Unlock()
				return map[string]any{"actual_cost": prices[0].InputPricePerToken, "billed_cost": 99, "input_tokens": 999}
			}
		}
		start := make(chan struct{})
		results := make(chan error, 2)
		for i := 0; i < 2; i++ {
			go func() {
				worker := NewModelPriceOverrides(db)
				<-start
				for {
					done, err := worker.RepriceBatch(ctx, calculate)
					if err != nil || done {
						results <- err
						return
					}
				}
			}()
		}
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-results; err != nil {
				t.Error(err)
			}
		}
		job, err := repo.RepricingStatus(ctx)
		if err != nil || job.Status != "completed" || job.UpdatedRows != 401 || job.SkippedRows != 0 || job.LastID != rows[len(rows)-1].ID {
			t.Fatalf("job=%+v err=%v", job, err)
		}
		if len(visits) != len(rows) {
			t.Fatalf("visited=%d want=%d", len(visits), len(rows))
		}
		for id, count := range visits {
			if count != 1 {
				t.Errorf("row %d calculated %d times", id, count)
			}
		}
		var invalid int64
		if err := db.Model(&GatewayUsageLog{}).Where("actual_cost <> ? OR billed_cost <> ? OR input_tokens <> ?", 2, 0.75, 10).Count(&invalid).Error; err != nil || invalid != 0 {
			t.Fatalf("invalid=%d err=%v", invalid, err)
		}
		// PostgreSQL ON CONFLICT must preserve a case-insensitive manual override.
		changed, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "X", InputPricePerToken: 3, Enabled: true}, {ModelName: "new-model", InputPricePerToken: 4, Enabled: true}}, 0, true)
		if err != nil || changed != 1 {
			t.Fatalf("sync changed=%d err=%v", changed, err)
		}
		price, err := repo.FindByModel("x")
		if err != nil || price.InputPricePerToken != 2 || price.Source != "manual" {
			t.Fatalf("manual=%+v err=%v", price, err)
		}
	})

	t.Run("finalization_between_page_and_terminal_read", func(t *testing.T) {
		db := openPriceRepricingPostgres(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		repo := NewModelPriceOverrides(db)
		row := GatewayUsageLog{RequestID: "winner-race", AttemptStatus: "accepted", ActualCost: 1, EstimatedExtraCost: 1, BilledCost: 0.75, CreatedAt: time.Now()}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if _, _, err := repo.ApplyWithRepricing(ctx, nil, 0, false); err != nil {
			t.Fatal(err)
		}
		var injected atomic.Bool
		callback := "test:price-finalization-race"
		err := db.Callback().Query().After("gorm:after_query").Register(callback, func(tx *gorm.DB) {
			page, ok := tx.Statement.Dest.(*[]GatewayUsageLog)
			if !ok || len(*page) != 1 || (*page)[0].ID != row.ID || !injected.CompareAndSwap(false, true) {
				return
			}
			// Commit through another connection after the page captured Winner=false.
			err := db.WithContext(ctx).Transaction(func(writer *gorm.DB) error {
				if err := writer.Model(&GatewayUsageLog{}).Where("id = ?", row.ID).Update("winner", true).Error; err != nil {
					return err
				}
				return writer.Create(&GatewayRequestFinalization{RequestID: row.RequestID, GatewayKeyID: row.GatewayKeyID, Delivered: true, WinnerUsageLogID: row.ID, ActualCost: 1, BilledCost: 0.75, VirtualCacheReadTokens: 7, CreatedAt: time.Now()}).Error
			})
			if err != nil {
				tx.AddError(err)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Query().Remove(callback) })
		sawWinner := false
		done, err := repo.RepriceBatch(ctx, func(_ []ModelPriceOverride) func(GatewayUsageLog) map[string]any {
			return func(row GatewayUsageLog) map[string]any {
				sawWinner = row.Winner
				if row.VirtualCacheReadTokens != 7 {
					t.Errorf("late finalization lost virtual tokens: %+v", row)
				}
				extra := 2
				if row.Winner {
					extra = 0
				}
				return map[string]any{"actual_cost": 2, "estimated_extra_cost": extra}
			}
		})
		if err != nil || !done || !injected.Load() || !sawWinner {
			t.Fatalf("done=%v injected=%v winner=%v err=%v", done, injected.Load(), sawWinner, err)
		}
		if err := db.First(&row, row.ID).Error; err != nil || row.EstimatedExtraCost != 0 || row.BilledCost != 0.75 {
			t.Fatalf("winner=%+v err=%v", row, err)
		}
	})
}
