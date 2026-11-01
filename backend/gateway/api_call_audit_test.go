package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestModelCallsAuditUsageCostAndPurposeWithoutCustomerBilling(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer audit-secret" {
			t.Error("missing auth")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"content":"21"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":60}}}`)
	}))
	defer upstream.Close()
	db := openGatewayTestDB(t)
	cipher, e := crypto.NewCipher("audit-fixture")
	if e != nil {
		t.Fatal(e)
	}
	secret, _ := cipher.Encrypt("audit-secret")
	groups, routes, usage := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db), storage.NewGatewayUsageLogs(db)
	providers := storage.NewGatewayProviders(db)
	provider := &storage.GatewayProvider{Name: "audit", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, DefaultBillingRate: .12}
	if e = providers.Create(provider); e != nil {
		t.Fatal(e)
	}
	group := &storage.GatewayGroup{Name: "audit", GatewayCandyCheckPolicy: storage.GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "gpt-4o", CandyCheckIntervalMinutes: 2, CandyCheckCooldownMinutes: 3}}
	if e = groups.Create(group); e != nil {
		t.Fatal(e)
	}
	if e = routes.SaveForGroup(group.ID, []storage.GatewayRoute{{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID, RateConvertMode: "custom", RateConvertValue: .12, BillingRateMultiplier: .12}}); e != nil {
		t.Fatal(e)
	}
	svc := NewService(groups, nil, routes, usage, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	list, _ := routes.ListByGroupID(group.ID)
	route := list[0]
	// Admin model tests, candy, recovery probes and regular forwarding are all
	// visible in the operator audit and do not themselves debit customer keys.
	result := svc.Admin.probeRouteModel(context.Background(), group, route, "gpt-4o", nil)
	if !result.OK {
		t.Fatalf("admin result=%+v", result)
	}
	candy, e := svc.RunCandyCheckNow(context.Background(), route.ID)
	if e != nil || candy.Status != "correct" {
		t.Fatalf("candy=%+v err=%v", candy, e)
	}
	target, e := svc.resolveUpstreamTarget(&route)
	if e != nil {
		t.Fatal(e)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/chat/completions", nil)
	c.Set(ctxKeyUpstreamOpsRequestID, "abcdef012345abcdef012345")
	for _, purpose := range []string{storage.GatewayAPICallPurposeModelProbe, storage.GatewayAPICallPurposeGateway} {
		ctx := apiCallPurpose(context.Background(), purpose)
		_, _, _, _, e = svc.forwardOnce(ctx, c, target, "/v1/chat/completions", "POST", nil, []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"private"}]}`), false, protocol.KindOpenAIChat, 0)
		if e != nil {
			t.Fatal(e)
		}
	}
	page, e := usage.APICalls().List(storage.GatewayAPICallLogQuery{})
	if e != nil || page.Total != 4 {
		t.Fatalf("page=%+v err=%v", page, e)
	}
	kinds := map[string]int{}
	for _, r := range page.Items {
		kinds[r.Purpose]++
		if r.Status != "success" || !r.UsageKnown || r.InputTokens != 40 || r.CacheReadTokens != 60 || r.OutputTokens != 7 || r.RouteID != route.ID || r.EstimatedCost == nil || *r.EstimatedCost <= 0 {
			t.Fatalf("row=%+v", r)
		}
		if r.Purpose == storage.GatewayAPICallPurposeGateway && r.GatewayRequestID != "abcdef012345abcdef012345" {
			t.Fatal("lost gateway request id")
		}
		if strings.Contains(fmt.Sprint(r), "audit-secret") {
			t.Fatal("secret persisted")
		}
	}
	for _, p := range []string{storage.GatewayAPICallPurposeAdminModelTest, storage.GatewayAPICallPurposeCandyCheck, storage.GatewayAPICallPurposeModelProbe, storage.GatewayAPICallPurposeGateway} {
		if kinds[p] != 1 {
			t.Fatalf("purpose=%s count=%d", p, kinds[p])
		}
	}
	var customer int64
	db.Model(&storage.GatewayUsageLog{}).Count(&customer)
	if customer != 0 {
		t.Fatalf("audit created %d customer charges", customer)
	}
}

func TestAuditLargeRequestModelMetadata(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	req, e := svc.runtime().buildUpstreamHTTPRequest(context.Background(), &upstreamTarget{}, "/v1/chat/completions", "POST", http.Header{}, []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"`+strings.Repeat("x", 3<<20)+`"}]}`), protocol.KindOpenAIChat, false)
	if e != nil {
		t.Fatal(e)
	}
	b, _ := io.ReadAll(req.Body)
	if len(b) < 3<<20 {
		t.Fatal("request body changed")
	}
}
