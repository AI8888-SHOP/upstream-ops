package storage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// GatewayCandyCheckSharedResult stores one upstream observation shared by all
// routes that resolve to the same source/request fingerprint. It intentionally
// has no gateway-group or route scheduling fields: freshness and cooldown are
// owned by each route state.
type GatewayCandyCheckSharedResult struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	CacheKey        string     `gorm:"size:64;not null;uniqueIndex" json:"-"`
	SourceRouteID   uint       `gorm:"not null;index" json:"source_route_id"`
	SourceRequestID string     `gorm:"size:96;not null;default:''" json:"source_request_id,omitempty"`
	Model           string     `gorm:"size:256;not null;default:''" json:"model"`
	Status          string     `gorm:"size:16;not null;default:'pending'" json:"status"`
	AnswerPreview   string     `gorm:"type:text" json:"answer_preview,omitempty"`
	Reason          string     `gorm:"type:text" json:"reason,omitempty"`
	StatusCode      int        `gorm:"not null;default:0" json:"status_code"`
	LatencyMS       int64      `gorm:"not null;default:0" json:"latency_ms"`
	ObservedAt      *time.Time `gorm:"index" json:"observed_at,omitempty"`
	RetryUntil      *time.Time `gorm:"index" json:"retry_until,omitempty"`
	LeaseUntil      *time.Time `json:"-"`
	LeaseToken      string     `gorm:"size:64;not null;default:''" json:"-"`
}

func (GatewayCandyCheckSharedResult) TableName() string { return "gateway_candy_check_shared_results" }

// GatewayCandyCheckSharedLease fences one in-flight upstream request. The
// lease is separate from the per-route lease so distinct routes can wait for
// one source request while stale workers remain unable to publish.
type GatewayCandyCheckSharedLease struct {
	CacheKey   string
	LeaseToken string
	LeaseUntil time.Time
	RouteID    uint
	ConfigKey  string
}

// GatewayCandyCheckSharedAcquire is the result of trying to use or own a
// shared observation. Cached is returned only when it is valid for maxAge (or
// is a short-lived transport error). InFlight means another instance owns the
// source request and callers should wait, then retry acquisition.
type GatewayCandyCheckSharedAcquire struct {
	Cached             *GatewayCandyCheckSharedResult
	Lease              *GatewayCandyCheckSharedLease
	InFlight           bool
	PreviousObservedAt *time.Time
	PreviousRequestID  string
}

func newCandySharedLeaseToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func cloneCandySharedResult(row *GatewayCandyCheckSharedResult) *GatewayCandyCheckSharedResult {
	if row == nil {
		return nil
	}
	copy := *row
	if row.ObservedAt != nil {
		v := *row.ObservedAt
		copy.ObservedAt = &v
	}
	if row.RetryUntil != nil {
		v := *row.RetryUntil
		copy.RetryUntil = &v
	}
	if row.LeaseUntil != nil {
		v := *row.LeaseUntil
		copy.LeaseUntil = &v
	}
	return &copy
}

func candySharedResultUsable(row *GatewayCandyCheckSharedResult, now time.Time, maxAge time.Duration) bool {
	if row == nil || row.ObservedAt == nil || row.ObservedAt.After(now) {
		return false
	}
	observed := row.ObservedAt.UTC()
	switch strings.ToLower(strings.TrimSpace(row.Status)) {
	case "correct", "incorrect":
		return maxAge > 0 && observed.Add(maxAge).After(now)
	case "error":
		return row.RetryUntil != nil && row.RetryUntil.After(now)
	default:
		return false
	}
}

// AcquireSharedCandyCheck atomically returns a fresh shared observation or
// creates/claims the source lease. force bypasses a previous fresh result but
// never steals an active lease; manual refresh therefore merges with an
// in-flight request instead of issuing a second HTTP request.
func (r *GatewayRoutes) AcquireSharedCandyCheck(cacheKey string, now time.Time, lease, maxAge time.Duration, force bool) (*GatewayCandyCheckSharedAcquire, error) {
	cacheKey = strings.TrimSpace(cacheKey)
	if r == nil || r.db == nil || cacheKey == "" {
		return nil, errors.New("shared candy check storage is unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	var acquired *GatewayCandyCheckSharedAcquire
	err := r.db.Transaction(func(tx *gorm.DB) error {
		// Insert the stable row first. ON CONFLICT makes two fresh instances
		// converge without relying on a process-local mutex.
		seed := GatewayCandyCheckSharedResult{CacheKey: cacheKey, Status: "pending"}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
			return err
		}
		var row GatewayCandyCheckSharedResult
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("cache_key = ?", cacheKey).First(&row).Error; err != nil {
			return err
		}
		if row.LeaseUntil != nil && row.LeaseUntil.After(now) {
			acquired = &GatewayCandyCheckSharedAcquire{InFlight: true, PreviousObservedAt: row.ObservedAt, PreviousRequestID: row.SourceRequestID}
			return nil
		}
		if !force && candySharedResultUsable(&row, now, maxAge) {
			acquired = &GatewayCandyCheckSharedAcquire{Cached: cloneCandySharedResult(&row)}
			return nil
		}
		token, err := newCandySharedLeaseToken()
		if err != nil {
			return err
		}
		until := now.Add(lease)
		updates := map[string]any{"lease_until": until, "lease_token": token}
		if err := tx.Model(&GatewayCandyCheckSharedResult{}).Where("id = ?", row.ID).Updates(updates).Error; err != nil {
			return err
		}
		acquired = &GatewayCandyCheckSharedAcquire{Lease: &GatewayCandyCheckSharedLease{CacheKey: cacheKey, LeaseToken: token, LeaseUntil: until}}
		return nil
	})
	return acquired, err
}

func (r *GatewayRoutes) FindSharedCandyCheck(cacheKey string) (*GatewayCandyCheckSharedResult, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("shared candy check storage is unavailable")
	}
	var row GatewayCandyCheckSharedResult
	if err := r.db.Where("cache_key = ?", strings.TrimSpace(cacheKey)).First(&row).Error; err != nil {
		return nil, err
	}
	return &row, nil
}

// finishSharedCandyCheckTx publishes only while the source lease token is
// current. The caller may run it inside the route-fenced transaction.
func finishSharedCandyCheckTx(tx *gorm.DB, lease GatewayCandyCheckSharedLease, result GatewayRouteCandyCheck, now time.Time) (bool, error) {
	var row GatewayCandyCheckSharedResult
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("cache_key = ?", lease.CacheKey).First(&row).Error; err != nil {
		return false, err
	}
	if row.LeaseToken != lease.LeaseToken || row.LeaseUntil == nil || !row.LeaseUntil.After(now) {
		return false, nil
	}
	observed := now
	if result.ObservedAt != nil && !result.ObservedAt.IsZero() {
		observed = *result.ObservedAt
	}
	updates := map[string]any{
		"source_route_id": result.SourceRouteID, "source_request_id": result.SourceRequestID,
		"model": result.Model, "status": result.Status, "answer_preview": result.AnswerPreview,
		"reason": result.Reason, "status_code": result.StatusCode, "latency_ms": result.LatencyMS,
		"observed_at": observed, "lease_until": nil, "lease_token": "",
	}
	if result.Status == "error" {
		updates["retry_until"] = now.Add(15 * time.Second)
	} else {
		updates["retry_until"] = nil
	}
	q := tx.Model(&GatewayCandyCheckSharedResult{}).Where("id = ? AND cache_key = ? AND lease_token = ?", row.ID, lease.CacheKey, lease.LeaseToken).Updates(updates)
	return q.RowsAffected > 0, q.Error
}

// FinishSharedCandyCheck publishes a source observation using its lease token.
func (r *GatewayRoutes) FinishSharedCandyCheck(lease GatewayCandyCheckSharedLease, result GatewayRouteCandyCheck, now time.Time) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("shared candy check storage is unavailable")
	}
	var updated bool
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var err error
		updated, err = finishSharedCandyCheckTx(tx, lease, result, now)
		return err
	})
	return updated, err
}

func (r *GatewayRoutes) ReleaseSharedCandyCheck(lease GatewayCandyCheckSharedLease) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("shared candy check storage is unavailable")
	}
	q := r.db.Model(&GatewayCandyCheckSharedResult{}).Where("cache_key = ? AND lease_token = ?", lease.CacheKey, lease.LeaseToken).
		Updates(map[string]any{"lease_until": nil, "lease_token": ""})
	return q.RowsAffected > 0, q.Error
}

// WaitForSharedCandyCheck polls the durable row, allowing consumers in other
// service instances to observe completion without an in-process notification.
func (r *GatewayRoutes) WaitForSharedCandyCheck(ctx context.Context, cacheKey string, poll time.Duration) (*GatewayCandyCheckSharedResult, error) {
	if poll <= 0 {
		poll = 50 * time.Millisecond
	}
	for {
		row, err := r.FindSharedCandyCheck(cacheKey)
		if err == nil && (row.LeaseUntil == nil || !row.LeaseUntil.After(time.Now())) {
			return row, nil
		}
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, err
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
