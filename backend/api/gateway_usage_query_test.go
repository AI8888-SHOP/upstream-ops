package api

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestParseGatewayUsageQueryKeepsLegacyAggregateDefaults(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest("GET", "/api/gateway/usage/stats", nil)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request

	query := parseGatewayUsageQuery(context)
	if !query.IncludeSum || !query.IncludeEndpoints {
		t.Fatalf("legacy aggregate defaults = sum=%v endpoints=%v, want both enabled", query.IncludeSum, query.IncludeEndpoints)
	}
}

func TestParseGatewayUsageQueryAllowsFrontendAggregateOptOut(t *testing.T) {
	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest("GET", "/api/gateway/usage?include_sum=0&include_endpoints=false", nil)
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	context.Request = request

	query := parseGatewayUsageQuery(context)
	if query.IncludeSum || query.IncludeEndpoints {
		t.Fatalf("explicit aggregate opt-out = sum=%v endpoints=%v, want both disabled", query.IncludeSum, query.IncludeEndpoints)
	}
}

