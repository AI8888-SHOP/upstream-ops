package storage

// GatewayStreamPreconnectPolicy controls an optional SSE connection handshake.
// It never represents model output or changes real first-token measurements.
// Defaults keep existing groups on the original deferred-response behavior.
type GatewayStreamPreconnectPolicy struct {
	StreamPreconnectEnabled    bool `gorm:"not null;default:false" json:"stream_preconnect_enabled"`
	StreamPreconnectMinDelayMS int  `gorm:"not null;default:0" json:"stream_preconnect_min_delay_ms"`
	StreamPreconnectMaxDelayMS int  `gorm:"not null;default:0" json:"stream_preconnect_max_delay_ms"`
}
