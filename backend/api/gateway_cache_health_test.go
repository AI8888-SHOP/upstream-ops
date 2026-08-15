package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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
