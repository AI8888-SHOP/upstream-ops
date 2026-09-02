package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayGroupCacheHealthOverridesCanBeClearedAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := gateway.NewService(
		groups,
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		nil,
		nil,
	)
	router := gin.New()
	registerGatewayAdmin(router.Group("/api"), &Deps{DB: db, Gateway: svc})

	created := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/gateway/groups",
		strings.NewReader(`{
			"name":"cache-health-policy-api",
			"cache_hit_rate_window_minutes":30,
			"cache_hit_rate_threshold_percent":62.5,
			"cache_hit_rate_blacklist_minutes":20,
			"cache_hit_rate_minimum_requests":25
		}`),
	)
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(created, request)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.Code, created.Body.String())
	}
	var group storage.GatewayGroup
	if err := json.Unmarshal(created.Body.Bytes(), &group); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if group.CacheHitRateWindowMinutes == nil || *group.CacheHitRateWindowMinutes != 30 ||
		group.CacheHitRateThresholdPercent == nil || *group.CacheHitRateThresholdPercent != 62.5 ||
		group.CacheHitRateBlacklistMinutes == nil || *group.CacheHitRateBlacklistMinutes != 20 ||
		group.CacheHitRateMinimumRequests == nil || *group.CacheHitRateMinimumRequests != 25 {
		t.Fatalf("created cache health overrides = %+v", group)
	}

	updated := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPut,
		fmt.Sprintf("/api/gateway/groups/%d", group.ID),
		strings.NewReader(`{
			"cache_hit_rate_window_minutes":null,
			"cache_hit_rate_threshold_percent":null,
			"cache_hit_rate_blacklist_minutes":null,
			"cache_hit_rate_minimum_requests":null
		}`),
	)
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(updated, request)
	if updated.Code != http.StatusOK {
		t.Fatalf("update status = %d, body=%s", updated.Code, updated.Body.String())
	}

	loaded, err := groups.FindByID(group.ID)
	if err != nil {
		t.Fatalf("load updated group: %v", err)
	}
	if loaded.CacheHitRateWindowMinutes != nil || loaded.CacheHitRateThresholdPercent != nil ||
		loaded.CacheHitRateBlacklistMinutes != nil || loaded.CacheHitRateMinimumRequests != nil {
		t.Fatalf("cleared cache health overrides = %+v", loaded)
	}
}
