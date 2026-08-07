package config

import (
	"path/filepath"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestLoadAppliesUpstreamDefaults(t *testing.T) {
	cfg, err := LoadFile(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if cfg.Upstream.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.Upstream.TimeoutSeconds)
	}
	if cfg.Upstream.UserAgent != DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", cfg.Upstream.UserAgent)
	}
	if cfg.Scheduler.Retention.GatewayUsageLogsDays != 90 {
		t.Fatalf("gateway usage retention = %d", cfg.Scheduler.Retention.GatewayUsageLogsDays)
	}
}

func TestDatabaseConfigUsesDriverSpecificDefaultPort(t *testing.T) {
	postgres := (DatabaseConfig{Driver: string(storage.DBDriverPostgres)}).ToStorageConfig()
	if postgres.Port != 5432 {
		t.Fatalf("postgres default port = %d, want 5432", postgres.Port)
	}
	mysql := (DatabaseConfig{Driver: string(storage.DBDriverMySQL)}).ToStorageConfig()
	if mysql.Port != 3306 {
		t.Fatalf("mysql default port = %d, want 3306", mysql.Port)
	}
}

func TestUpstreamConfigWithDefaultsKeepsCustomUserAgent(t *testing.T) {
	cfg := UpstreamConfig{
		TimeoutSeconds: 0,
		UserAgent:      "custom-agent",
	}.WithDefaults()
	if cfg.TimeoutSeconds != DefaultUpstreamTimeoutSeconds {
		t.Fatalf("timeout seconds = %d", cfg.TimeoutSeconds)
	}
	if cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", cfg.UserAgent)
	}
}

func TestGatewayConfigWithDefaults(t *testing.T) {
	cfg := GatewayConfig{}.WithDefaults()
	if cfg.TempPauseSeconds != DefaultGatewayTempPauseSeconds {
		t.Fatalf("temp pause = %d", cfg.TempPauseSeconds)
	}
	if cfg.ForwardTimeoutSeconds != DefaultGatewayForwardTimeoutSeconds {
		t.Fatalf("forward timeout = %d", cfg.ForwardTimeoutSeconds)
	}
	if cfg.RouteBatchConcurrency != DefaultGatewayRouteBatchConcurrency {
		t.Fatalf("batch concurrency = %d", cfg.RouteBatchConcurrency)
	}
	custom := GatewayConfig{RouteBatchConcurrency: 16, ForwardTimeoutSeconds: 120}.WithDefaults()
	if custom.RouteBatchConcurrency != 16 || custom.ForwardTimeoutSeconds != 120 {
		t.Fatalf("custom = %#v", custom)
	}
	if custom.ModelsCacheTTLSeconds != DefaultGatewayModelsCacheTTLSeconds {
		t.Fatalf("models cache ttl = %d", custom.ModelsCacheTTLSeconds)
	}
	if cfg.Hedge.DelaySeconds != DefaultGatewayHedgeDelaySeconds ||
		cfg.Hedge.MaxParallel != DefaultGatewayHedgeMaxParallel ||
		cfg.Hedge.MaxAttempts != DefaultGatewayHedgeMaxAttempts {
		t.Fatalf("hedge defaults = %#v", cfg.Hedge)
	}
	if cfg.ResponseValidation.StreamMode != DefaultGatewayResponseValidationMode ||
		cfg.ResponseValidation.PrefixBytes != DefaultGatewayResponseValidationBytes ||
		cfg.ResponseValidation.PrefixTimeoutMS != DefaultGatewayResponseValidationTimeoutMS {
		t.Fatalf("response validation defaults = %#v", cfg.ResponseValidation)
	}
}

func TestGatewayHedgeAndValidationBoundsNormalize(t *testing.T) {
	cfg := GatewayConfig{
		Hedge: GatewayHedgeConfig{DelaySeconds: 999, MaxParallel: 999, MaxAttempts: 1},
		ResponseValidation: GatewayResponseValidationConfig{
			StreamMode: "full_buffer", PrefixBytes: 1, PrefixTimeoutMS: 999999,
		},
	}.WithDefaults()
	if cfg.Hedge.DelaySeconds != 300 || cfg.Hedge.MaxParallel != MaxGatewayHedgeParallel || cfg.Hedge.MaxAttempts != MaxGatewayHedgeParallel {
		t.Fatalf("normalized hedge = %#v", cfg.Hedge)
	}
	if cfg.ResponseValidation.StreamMode != "prefix" || cfg.ResponseValidation.PrefixBytes != 1024 || cfg.ResponseValidation.PrefixTimeoutMS != 30000 {
		t.Fatalf("normalized validation = %#v", cfg.ResponseValidation)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("normalized config should validate: %v", err)
	}
}
