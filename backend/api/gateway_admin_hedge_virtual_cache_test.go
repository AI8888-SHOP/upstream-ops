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

func TestGatewayGroupVirtualCacheFlagAPI(t *testing.T) {
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
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc})

	created := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/gateway/groups",
		strings.NewReader(`{"name":"virtual-cache-api","hedge_enabled":true,"hedge_virtual_cache_enabled":true}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(created, request)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.Code, created.Body.String())
	}
	var group storage.GatewayGroup
	if err := json.Unmarshal(created.Body.Bytes(), &group); err != nil {
		t.Fatalf("decode create response: %v", err)
	}
	if !group.HedgeEnabled || !group.HedgeVirtualCacheEnabled {
		t.Fatalf("created group hedge policy = %+v", group)
	}

	updated := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPut,
		fmt.Sprintf("/api/gateway/groups/%d", group.ID),
		strings.NewReader(`{"hedge_virtual_cache_enabled":false}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(updated, request)
	if updated.Code != http.StatusOK {
		t.Fatalf("update status = %d, body=%s", updated.Code, updated.Body.String())
	}
	var updatedGroup storage.GatewayGroup
	if err := json.Unmarshal(updated.Body.Bytes(), &updatedGroup); err != nil {
		t.Fatalf("decode update response: %v", err)
	}
	if updatedGroup.HedgeVirtualCacheEnabled {
		t.Fatalf("updated virtual cache flag = true, want false")
	}

	loaded := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/gateway/groups/%d", group.ID), nil)
	r.ServeHTTP(loaded, request)
	if loaded.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", loaded.Code, loaded.Body.String())
	}
	var loadedGroup storage.GatewayGroup
	if err := json.Unmarshal(loaded.Body.Bytes(), &loadedGroup); err != nil {
		t.Fatalf("decode get response: %v", err)
	}
	if loadedGroup.HedgeVirtualCacheEnabled {
		t.Fatalf("persisted virtual cache flag = true, want false")
	}
}
