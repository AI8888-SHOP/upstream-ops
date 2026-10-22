package gateway

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func multipartImageBody(t *testing.T, model string) (string, []byte) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	if err := writer.WriteField("model", model); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteField("prompt", "add a hat"); err != nil {
		t.Fatal(err)
	}
	part, err := writer.CreateFormFile("image", "mask.png")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write([]byte("\x89PNG\r\n\x1a\nbinary-image-bytes")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return writer.FormDataContentType(), body.Bytes()
}

func TestExtractMultipartModel(t *testing.T) {
	contentType, body := multipartImageBody(t, "gpt-image-1")
	if got := extractMultipartModel(contentType, body); got != "gpt-image-1" {
		t.Fatalf("model=%q want gpt-image-1", got)
	}
	if got := extractMultipartModel("application/json", []byte(`{"model":"gpt-image-1"}`)); got != "" {
		t.Fatalf("JSON body must not be parsed as multipart: %q", got)
	}
	if got := extractMultipartModel(contentType, body[:len(body)/3]); got != "" {
		t.Fatalf("truncated multipart body must not invent a model: %q", got)
	}
	missing, err := func() (string, error) {
		var buf bytes.Buffer
		writer := multipart.NewWriter(&buf)
		if err := writer.WriteField("prompt", "no model"); err != nil {
			return "", err
		}
		if err := writer.Close(); err != nil {
			return "", err
		}
		return writer.FormDataContentType(), nil
	}()
	if err != nil {
		t.Fatal(err)
	}
	if got := extractMultipartModel(missing, nil); got != "" {
		t.Fatalf("empty body must not yield a model: %q", got)
	}
}

func TestValidateModelPriceBillingMode(t *testing.T) {
	token := storage.ModelPriceOverride{ModelName: "m", InputPricePerToken: 0.001}
	if err := storage.ValidateModelPrice(&token); err != nil {
		t.Fatal(err)
	}
	if token.BillingMode != "token" || token.PerRequestPrice != nil {
		t.Fatalf("token mode not normalised: %+v", token)
	}
	perRequest := storage.ModelPriceOverride{ModelName: "gpt-image-1", BillingMode: "per_request"}
	if err := storage.ValidateModelPrice(&perRequest); err != nil {
		t.Fatal(err)
	}
	if perRequest.PerRequestPrice == nil || *perRequest.PerRequestPrice != 0 {
		t.Fatalf("per-request price not defaulted: %+v", perRequest)
	}
	negative := -1.0
	if err := storage.ValidateModelPrice(&storage.ModelPriceOverride{ModelName: "m", BillingMode: "per_request", PerRequestPrice: &negative}); err == nil {
		t.Fatal("negative per-request price must be rejected")
	}
	if err := storage.ValidateModelPrice(&storage.ModelPriceOverride{ModelName: "m", BillingMode: "per_token"}); err == nil {
		t.Fatal("unknown billing mode must be rejected")
	}
}

func TestCalculateCostPerRequestIgnoresTokens(t *testing.T) {
	price := 0.04
	pricing := ModelPricing{BillingMode: "per_request", PerRequestPrice: &price}
	cost := CalculateCost(pricing, UsageTokens{InputTokens: 1000, OutputTokens: 500}, 1, 1)
	if cost.TotalCost != 0.04 || cost.ActualCost != 0.04 {
		t.Fatalf("per-request cost=%+v", cost)
	}
	if cost.InputCost != 0 || cost.OutputCost != 0 || cost.CacheReadCost != 0 || cost.ImageOutputCost != 0 {
		t.Fatalf("token buckets must stay zero: %+v", cost)
	}
	// 无任何用量（图片接口）同样收费，倍率照乘。
	empty := CalculateCost(pricing, UsageTokens{}, 2, 2)
	if empty.ActualCost != 0.08 {
		t.Fatalf("multiplied per-request cost=%v want 0.08", empty.ActualCost)
	}
	tokenPriced := ModelPricing{InputPricePerToken: 0.000001}
	if got := CalculateCost(tokenPriced, UsageTokens{InputTokens: 1000}, 1, 1).TotalCost; got != 0.001 {
		t.Fatalf("token pricing regressed: %v", got)
	}
}

// 图片编辑走 multipart：模型名必须从表单读出，并且按次计费落账。
func TestImageEditsMultipartModelAndPerRequestBilling(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/images/edits" {
			t.Errorf("upstream path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"created":1700000000,"data":[{"url":"https://example/image.png"}]}`))
	}))
	t.Cleanup(upstream.Close)

	db := openGatewayTestDB(t)
	cipher, err := crypto.NewCipher("image-per-request")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("upstream-key")
	if err != nil {
		t.Fatal(err)
	}
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	group := &storage.GatewayGroup{Name: "image-per-request", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc"}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("image-client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	provider := &storage.GatewayProvider{Name: "image-source", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	// 路由白名单只列文本模型：图片模型不在其中，也必须照旧可用（模型名只用于定价）。
	if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{{
		GatewayGroupID: group.ID, Position: 0, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID,
		Enabled: true, RateConvertMode: "custom", RateConvertValue: 1, BillingRateMultiplier: 1,
		ModelPolicy: storage.GatewayProviderModelPolicyAllowlist, AllowedModelsJSON: `["gpt-4o"]`,
	}}); err != nil {
		t.Fatal(err)
	}
	prices := storage.NewModelPriceOverrides(db)
	price := 0.04
	if _, _, err := prices.ApplyWithRepricing(nil, []storage.ModelPriceOverride{{
		ModelName: "gpt-image-1", BillingMode: "per_request", PerRequestPrice: &price, Enabled: true, Source: "manual",
	}}, 0, false); err != nil {
		t.Fatal(err)
	}

	svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), prices, storage.NewChannels(db), nil, cipher, nil)
	svc.SetProviders(providers)

	contentType, body := multipartImageBody(t, "gpt-image-1")
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/edits", bytes.NewReader(body))
	c.Request.Header.Set("Authorization", "Bearer image-client-key")
	c.Request.Header.Set("Content-Type", contentType)
	svc.runtime().HandleForward(c, "/v1/images/edits", protocol.KindOpenAIChat)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "image.png") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls=%d", calls.Load())
	}
	var logs []storage.GatewayUsageLog
	if err := db.Order("attempt").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 {
		t.Fatalf("usage logs=%d", len(logs))
	}
	log := logs[0]
	if log.RequestedModel != "gpt-image-1" {
		t.Fatalf("requested model=%q want gpt-image-1", log.RequestedModel)
	}
	if log.BillingMode != "per_request" {
		t.Fatalf("billing mode=%q want per_request", log.BillingMode)
	}
	if log.TotalCost != 0.04 || log.ActualCost != 0.04 {
		t.Fatalf("cost total=%v actual=%v want 0.04", log.TotalCost, log.ActualCost)
	}
	if !log.Success {
		t.Fatalf("image request not recorded as success: %+v", log)
	}
}
