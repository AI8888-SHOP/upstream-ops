package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayCacheHealthAPIListsProviderStats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	provider := &storage.GatewayProvider{
		Name: "cache-health-api-provider", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	if err := usage.Create(&storage.GatewayUsageLog{
		GatewayProviderID: provider.ID, RequestID: "cache-health-api-request", Success: true,
		InputTokens: 25, CacheReadTokens: 75, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	svc := gateway.NewService(nil, nil, storage.NewGatewayRoutes(db), usage, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc, GatewayUsage: usage})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/gateway/cache-health?source_kind=provider&source_id=%d", provider.ID), nil)
	r.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Items []gateway.CacheHealthStat `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].SourceID != provider.ID || body.Items[0].HitRate != 75 {
		t.Fatalf("items = %+v", body.Items)
	}
}

func TestGatewayCacheHealthAPIClearsProviderBlacklist(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	provider := &storage.GatewayProvider{
		Name: "cache-health-api-clear", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	until := time.Now().Add(time.Hour)
	if err := usage.UpsertCacheHealth(&storage.GatewayChannelCacheHealth{
		SourceKind: storage.GatewayRouteSourceProvider, SourceID: provider.ID,
		HitRate: 12, RequestCount: 5, BlacklistedUntil: &until, BlacklistReason: "low cache hit rate",
	}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	svc := gateway.NewService(nil, nil, storage.NewGatewayRoutes(db), usage, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc, GatewayUsage: usage})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/providers/%d/cache-health/clear", provider.ID),
		strings.NewReader(""),
	)
	r.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
	rows, err := usage.CacheHealthStates(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(rows) != 1 || rows[0].BlacklistedUntil != nil || rows[0].BlacklistReason != "" {
		t.Fatalf("cleared state = %#v err=%v", rows, err)
	}
	if rows[0].HitRate != 12 || rows[0].RequestCount != 5 {
		t.Fatalf("clear changed rolling statistics: %+v", rows[0])
	}
}
