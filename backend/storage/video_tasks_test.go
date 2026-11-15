package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestVideoTaskBindings(t *testing.T) {
	runVideoTaskBindings(t, openTestDB(t))
}

func TestVideoTaskPostgres(t *testing.T) {
	db := openPriceRepricingPostgres(t)
	if err := db.AutoMigrate(&GatewayVideoTaskBinding{}); err != nil {
		t.Fatal(err)
	}
	runVideoTaskBindings(t, db)
	// Concurrent polls/submissions must not overwrite an existing task's
	// original credential snapshot, including under PostgreSQL MVCC.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := NewGatewayRoutes(db).SaveVideoTaskBinding(context.Background(), &GatewayVideoTaskBinding{
				GatewayGroupID: 1, GatewayKeyID: 2, TaskID: "concurrent", TargetFingerprint: "same-target",
				SnapshotCipher: "encrypted", ExpiresAt: time.Now().Add(time.Hour),
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var count int64
	if err := db.Model(&GatewayVideoTaskBinding{}).Where("task_id = ?", "concurrent").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("concurrent binding count=%d err=%v", count, err)
	}
}

func runVideoTaskBindings(t *testing.T, db *gorm.DB) {
	t.Helper()
	repo := NewGatewayRoutes(db)
	ctx := context.Background()
	now := time.Now()
	newBinding := func() *GatewayVideoTaskBinding {
		return &GatewayVideoTaskBinding{GatewayGroupID: 1, GatewayKeyID: 2, TaskID: "task-original",
			TargetFingerprint: "target-original", SnapshotCipher: "encrypted-original", ExpiresAt: now.Add(time.Hour)}
	}
	if err := repo.SaveVideoTaskBinding(ctx, newBinding()); err != nil {
		t.Fatal(err)
	}
	// The same task ID can exist for another owner, never be read by them.
	for _, owner := range [][2]uint{{1, 3}, {2, 2}} {
		if _, err := repo.FindVideoTaskBinding(ctx, owner[0], owner[1], "task-original", now); !errors.Is(err, gorm.ErrRecordNotFound) {
			t.Fatalf("owner isolation: %v", err)
		}
	}
	if _, err := repo.FindVideoTaskBinding(ctx, 1, 2, "task-original", now.Add(2*time.Hour)); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expired task returned: %v", err)
	}
	idempotent := newBinding()
	idempotent.SnapshotCipher = "new-encryption-of-same-target"
	if err := repo.SaveVideoTaskBinding(ctx, idempotent); err != nil {
		t.Fatalf("idempotent save failed: %v", err)
	}
	conflict := newBinding()
	conflict.TargetFingerprint = "another-site"
	if err := repo.SaveVideoTaskBinding(ctx, conflict); !errors.Is(err, ErrVideoTaskBindingConflict) {
		t.Fatalf("task collision overwritten: %v", err)
	}
	// A new repository/process reads the same original snapshot.
	got, err := NewGatewayRoutes(db).FindVideoTaskBinding(ctx, 1, 2, "task-original", now)
	if err != nil || got.SnapshotCipher != "encrypted-original" {
		t.Fatalf("snapshot changed: err=%v", err)
	}
	other := newBinding()
	other.GatewayKeyID = 3
	if err := repo.SaveVideoTaskBinding(ctx, other); err != nil {
		t.Fatalf("independent owner collided: %v", err)
	}
	if err := repo.PruneVideoTaskBindings(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.FindVideoTaskBinding(ctx, 1, 2, "task-original", now); err != nil {
		t.Fatal("cleanup removed unexpired task")
	}
	if err := repo.PruneVideoTaskBindings(ctx, now.Add(2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&GatewayVideoTaskBinding{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("expired cleanup count=%d err=%v", count, err)
	}
}
