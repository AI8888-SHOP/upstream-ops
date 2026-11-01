package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/auth"
	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/runtimeconfig"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayAPICallQueryParsesFiltersAndUnixSeconds(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/gateway/api-calls?purpose=candy_check&channel_id=7&provider_id=8&source_group_id=9&source_api_key_name=key&model=m&status=pending&since=1700000000&until=1700000100&page=2&page_size=999", nil)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = request
	q := parseGatewayAPICallQuery(ctx)
	if q.Purpose != storage.GatewayAPICallPurposeCandyCheck || q.ChannelID != 7 || q.GatewayProviderID != 8 || q.SourceGroupID == nil || *q.SourceGroupID != 9 || q.Page != 2 || q.PageSize != 999 {
		t.Fatalf("query = %+v", q)
	}
	if q.Since == nil || q.Since.Unix() != 1700000000 || q.Until == nil || q.Until.Unix() != 1700000100 {
		t.Fatalf("times = since=%v until=%v", q.Since, q.Until)
	}
}

func TestGatewayAPICallAdminAPIRequiresAuthAndReturnsStats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	if err := usage.APICalls().Create(&storage.GatewayAPICallLog{
		RequestID: "api-auth-audit", Purpose: storage.GatewayAPICallPurposeGateway,
		Status: storage.GatewayAPICallStatusSuccess, UsageKnown: true,
		StartedAt: time.Unix(1700000000, 0).UTC(),
	}); err != nil {
		t.Fatalf("seed audit: %v", err)
	}
	authSvc, err := auth.New("admin", "password", "api-audit-secret", time.Hour)
	if err != nil {
		t.Fatalf("auth service: %v", err)
	}
	runtime := runtimeconfig.New("", "", nil, nil, nil, nil, authSvc, nil, config.ProxyConfig{}, config.UpstreamConfig{}, config.GatewayConfig{}, nil)
	service := gateway.NewService(nil, nil, nil, usage, nil, nil, nil, nil, nil)
	router := gin.New()
	Register(router, &Deps{DB: db, Runtime: runtime, Gateway: service, GatewayUsage: usage})
	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/api/gateway/api-calls/stats", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d body=%s", unauthorized.Code, unauthorized.Body.String())
	}
	token, _, err := authSvc.Login("admin", "password")
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/gateway/api-calls/stats?purpose=gateway", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("stats status = %d body=%s", response.Code, response.Body.String())
	}
	var stats storage.GatewayAPICallLogStats
	if err := json.Unmarshal(response.Body.Bytes(), &stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	if stats.TotalCalls != 1 || stats.SuccessCalls != 1 || stats.PendingCalls != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}
