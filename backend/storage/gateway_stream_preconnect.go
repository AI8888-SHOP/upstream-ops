package storage

// GatewayStreamPreconnectPolicy controls an optional SSE connection handshake.
// Its optional stateless Responses prefix changes downstream semantic timing,
// not actual model output or the gateway's real first-token measurements.
// Defaults keep existing groups on the original deferred-response behavior.
type GatewayStreamPreconnectPolicy struct {
	StreamPreconnectEnabled bool `gorm:"not null;default:false" json:"stream_preconnect_enabled"`
	// Opt-in lifecycle prefix for sub2api semantic TTFT, only for eligible store:false Responses requests.
	StreamPreconnectSub2apiEnabled bool `gorm:"not null;default:false" json:"stream_preconnect_sub2api_enabled"`
	StreamPreconnectMinDelayMS     int  `gorm:"not null;default:0" json:"stream_preconnect_min_delay_ms"`
	StreamPreconnectMaxDelayMS     int  `gorm:"not null;default:0" json:"stream_preconnect_max_delay_ms"`
}
