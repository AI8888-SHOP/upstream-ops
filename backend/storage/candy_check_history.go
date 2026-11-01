package storage

import (
	"time"

	"gorm.io/gorm"
)

// Keep a bounded history independently from the replaceable probe state.
// Saving group settings must not erase evidence of a previous outage.
type GatewayCandyCheckEvent struct {
	ID              uint       `gorm:"primaryKey;index:idx_candy_history_route,priority:2" json:"id"`
	RouteID         uint       `gorm:"not null;index:idx_candy_history_route,priority:1" json:"route_id"`
	Model           string     `gorm:"size:256" json:"model"`
	Status          string     `gorm:"size:16" json:"status"`
	Reason          string     `gorm:"type:text" json:"reason"`
	StatusCode      int        `json:"status_code"`
	LatencyMS       int64      `json:"latency_ms"`
	Manual          bool       `json:"manual"`
	Reused          bool       `gorm:"not null;default:false" json:"reused"`
	SourceRouteID   uint       `gorm:"not null;default:0" json:"source_route_id,omitempty"`
	SourceRequestID string     `gorm:"size:96;not null;default:''" json:"source_request_id,omitempty"`
	ObservedAt      *time.Time `json:"observed_at,omitempty"`
	CooldownUntil   *time.Time `json:"cooldown_until,omitempty"`
	BackoffUntil    *time.Time `json:"backoff_until,omitempty"`
	CreatedAt       time.Time  `gorm:"not null" json:"created_at"`
}

func appendCandyCheckEvent(tx *gorm.DB, event GatewayCandyCheckEvent) error {
	if err := tx.Create(&event).Error; err != nil {
		return err
	}
	// Keep audit history for 90 days. The API still returns its newest bounded
	// page, but an old burst must not delete recent evidence by count alone.
	cutoff := event.CreatedAt.Add(-90 * 24 * time.Hour)
	return tx.Where("route_id = ? AND created_at < ?", event.RouteID, cutoff).
		Delete(&GatewayCandyCheckEvent{}).Error
}

func (r *GatewayRoutes) CandyCheckHistory(routeID uint) ([]GatewayCandyCheckEvent, error) {
	var events []GatewayCandyCheckEvent
	err := r.db.Where("route_id = ?", routeID).Order("id DESC").Limit(100).Find(&events).Error
	return events, err
}
