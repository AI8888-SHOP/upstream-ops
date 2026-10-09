package storage

import (
	"time"

	"gorm.io/gorm"
)

// Keep a bounded history independently from the replaceable probe state.
// Saving group settings must not erase evidence of a previous outage.
type GatewayCandyCheckEvent struct {
	ID            uint       `gorm:"primaryKey;index:idx_candy_history_route,priority:2" json:"id"`
	RouteID       uint       `gorm:"not null;index:idx_candy_history_route,priority:1" json:"route_id"`
	Model         string     `gorm:"size:256" json:"model"`
	Status        string     `gorm:"size:16" json:"status"`
	Reason        string     `gorm:"type:text" json:"reason"`
	StatusCode    int        `json:"status_code"`
	LatencyMS     int64      `json:"latency_ms"`
	Manual        bool       `json:"manual"`
	CooldownUntil *time.Time `json:"cooldown_until,omitempty"`
	BackoffUntil  *time.Time `json:"backoff_until,omitempty"`
	CreatedAt     time.Time  `gorm:"not null" json:"created_at"`
}

func appendCandyCheckEvent(tx *gorm.DB, event GatewayCandyCheckEvent) error {
	if err := tx.Create(&event).Error; err != nil {
		return err
	}
	// Per-route locking is held by the caller; concurrent probes/clears cannot
	// prune each other's uncommitted events. No growing full-table scan.
	var oldest []uint
	if err := tx.Model(&GatewayCandyCheckEvent{}).Where("route_id = ?", event.RouteID).
		Order("id DESC").Offset(99).Limit(1).Pluck("id", &oldest).Error; err != nil {
		return err
	}
	if len(oldest) == 0 {
		return nil
	}
	return tx.Where("route_id = ? AND id < ?", event.RouteID, oldest[0]).Delete(&GatewayCandyCheckEvent{}).Error
}

func (r *GatewayRoutes) CandyCheckHistory(routeID uint) ([]GatewayCandyCheckEvent, error) {
	var events []GatewayCandyCheckEvent
	err := r.db.Where("route_id = ?", routeID).Order("id DESC").Limit(100).Find(&events).Error
	return events, err
}
