package api

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/gin-gonic/gin"
)

func TestSaveSettingsKeepsAppVersion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	path := filepath.Join(t.TempDir(), "config.yaml")
	cfg := &config.Config{
		App: config.AppConfig{
			Title:              "Old",
			NotificationPrefix: "[Old] ",
		},
	}
	if err := config.Save(path, cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerSettings(api, &Deps{
		Runtime: runtimeconfig.New(path, "", nil, nil, nil, nil, nil, nil, config.ProxyConfig{}, config.UpstreamConfig{}, config.GatewayConfig{}, nil),
	})

	body := `{
		"app":{"title":"New","notificationPrefix":"[New] "},
		"auth":{"enabled":false,"username":"admin","password":"","tokenSecret":"","sessionTTLHours":168},
		"scheduler":{"balanceCron":"37 */15 * * * *","rateCron":"13 */30 * * * *","concurrency":4,"retention":{"cron":"0 17 3 * * *","monitorLogsDays":30,"balanceSnapshotsDays":90,"notificationLogsDays":90,"announcementsDays":90,"gatewayUsageLogsDays":45}},
		"notifications":{"batchRateChanges":true,"minChangePct":0,"balanceLowCooldownMinutes":60,"subscriptionDailyRemainingThresholdPct":0,"subscriptionWeeklyRemainingThresholdPct":0,"subscriptionMonthlyRemainingThresholdPct":0,"subscriptionExpiryThresholdHours":0,"subscriptionAlertCooldownMinutes":1440,"sendMaxAttempts":3},
		"proxy":{"enabled":true,"versionCheckEnabled":true,"protocol":"socks5","host":"127.0.0.1","port":1080,"username":"u","password":"p"},
		"upstream":{"timeoutSeconds":45,"userAgent":"custom-agent"},
		"gateway":{"tempPauseSeconds":30,"forwardTimeoutSeconds":600,"modelsCacheTTLSeconds":60,"maxFailoverSwitches":8,"routeBatchConcurrency":8,"usageErrorBodyBytes":32768,"usageErrorMsgRunes":500,"usageErrorHeaderValueRunes":8192,"usageErrorHeadersJSONBytes":65536,"cacheHitRateMinimumRequests":25}
	}`
	req := httptest.NewRequest(http.MethodPut, "/api/settings/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	got, err := config.LoadFile(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got.App.Title != "New" {
		t.Fatalf("title = %q", got.App.Title)
	}
	if got.App.NotificationPrefix != "[New] " {
		t.Fatalf("notification prefix = %q", got.App.NotificationPrefix)
	}
	if !got.Proxy.Enabled || !got.Proxy.VersionCheckEnabled || got.Proxy.Protocol != "socks5" || got.Proxy.Host != "127.0.0.1" || got.Proxy.Port != 1080 || got.Proxy.Username != "u" || got.Proxy.Password != "p" {
		t.Fatalf("proxy = %#v", got.Proxy)
	}
	if got.Upstream.TimeoutSeconds != 45 || got.Upstream.UserAgent != "custom-agent" {
		t.Fatalf("upstream = %#v", got.Upstream)
	}
	if got.Scheduler.Retention.GatewayUsageLogsDays != 45 {
		t.Fatalf("gateway usage retention = %d", got.Scheduler.Retention.GatewayUsageLogsDays)
	}
	if got.Gateway.RouteBatchConcurrency != 8 || got.Gateway.ForwardTimeoutSeconds != 600 {
		t.Fatalf("gateway = %#v", got.Gateway)
	}
	if got.Gateway.CacheHitRateMinimumRequests != 25 {
		t.Fatalf("cache health minimum requests = %d, want 25", got.Gateway.CacheHitRateMinimumRequests)
	}
}
