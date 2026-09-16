package storage

// GatewaySchedulerPolicy is embedded in GatewayGroup. Legacy groups retain
// cost ordering until an administrator explicitly enables adaptive routing.
type GatewaySchedulerPolicy struct {
	SchedulingMode           string  `gorm:"size:16;not null;default:'cost'" json:"scheduling_mode"`
	SchedulingPremiumPercent float64 `gorm:"not null;default:0" json:"scheduling_premium_percent"`
	SchedulingWindowMinutes  int     `gorm:"not null;default:5" json:"scheduling_window_minutes"`
	SchedulingMinSamples     int     `gorm:"not null;default:10" json:"scheduling_min_samples"`
	SchedulingTargetTTFTSec  int     `gorm:"not null;default:10" json:"scheduling_target_ttft_sec"`
}
