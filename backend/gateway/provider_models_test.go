package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestNormalizeProviderModelConfiguration(t *testing.T) {
	policy, err := NormalizeProviderModelPolicy(" ALLOWLIST ")
	if err != nil || policy != storage.GatewayProviderModelPolicyAllowlist {
		t.Fatalf("policy=%q err=%v", policy, err)
	}
	if _, err := NormalizeProviderModelPolicy("denylist"); err == nil {
		t.Fatal("invalid policy should fail")
	}

	normalized, err := NormalizeProviderAllowedModelsJSON(
		`[" model-b ","model-a","model-b",""]`,
	)
	if err != nil {
		t.Fatalf("normalize allowed models: %v", err)
	}
	if normalized != `["model-b","model-a"]` {
		t.Fatalf("normalized=%s", normalized)
	}
	for _, raw := range []string{`null`, `{}`, `["ok",1]`, `not-json`} {
		if _, err := NormalizeProviderAllowedModelsJSON(raw); err == nil {
			t.Fatalf("invalid allowed_models_json should fail: %s", raw)
		}
	}
}

func TestProviderAllowsAndFiltersUpstreamModels(t *testing.T) {
	provider := &storage.GatewayProvider{
		ModelPolicy:       storage.GatewayProviderModelPolicyAllowlist,
		AllowedModelsJSON: `["model-b","model-c"]`,
	}
	allowed, err := ProviderAllowsUpstreamModel(provider, " model-b ")
	if err != nil || !allowed {
		t.Fatalf("model-b allowed=%v err=%v", allowed, err)
	}
	allowed, err = ProviderAllowsUpstreamModel(provider, "model-a")
	if err != nil || allowed {
		t.Fatalf("model-a allowed=%v err=%v", allowed, err)
	}
	filtered, err := FilterProviderModels(provider, []string{"model-a", "model-b", "model-b", " model-c "})
	if err != nil {
		t.Fatalf("filter: %v", err)
	}
	if len(filtered) != 2 || filtered[0] != "model-b" || filtered[1] != "model-c" {
		t.Fatalf("filtered=%v", filtered)
	}

	provider.AllowedModelsJSON = `[]`
	allowed, err = ProviderAllowsUpstreamModel(provider, "model-b")
	if err != nil || allowed {
		t.Fatalf("empty allowlist should reject all: allowed=%v err=%v", allowed, err)
	}
	provider.ModelPolicy = ""
	allowed, err = ProviderAllowsUpstreamModel(provider, "anything")
	if err != nil || !allowed {
		t.Fatalf("empty legacy policy should allow all: allowed=%v err=%v", allowed, err)
	}
}

func TestProviderVirtualCachePercentForModel(t *testing.T) {
	provider := &storage.GatewayProvider{
		VirtualCacheEnabled:   true,
		VirtualCachePercent:   50,
		VirtualCacheModelsJSON: `[]`,
	}
	percent, err := ProviderVirtualCachePercentForModel(provider, "gpt-text")
	if err != nil || percent != 50 {
		t.Fatalf("all-model policy percent=%d err=%v", percent, err)
	}
	percent, err = ProviderVirtualCachePercentForModel(provider, "")
	if err != nil || percent != 0 {
		t.Fatalf("empty model percent=%d err=%v", percent, err)
	}
	provider.VirtualCacheModelsJSON = `["gpt-text"]`
	percent, err = ProviderVirtualCachePercentForModel(provider, "other")
	if err != nil || percent != 0 {
		t.Fatalf("filtered model percent=%d err=%v", percent, err)
	}
	percent, err = ProviderVirtualCachePercentForModel(provider, "gpt-text")
	if err != nil || percent != 50 {
		t.Fatalf("selected model percent=%d err=%v", percent, err)
	}
	provider.VirtualCachePercent = 0
	percent, err = ProviderVirtualCachePercentForModel(provider, "gpt-text")
	if err != nil || percent != 0 {
		t.Fatalf("disabled percentage=%d err=%v", percent, err)
	}
}

func TestProviderCRUDNormalizesModelsAndInvalidatesCache(t *testing.T) {
	db := openGatewayTestDB(t)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("test-provider-model-crud")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)

	created, err := svc.CreateProvider(CreateProviderInput{
		Name:               "model-provider",
		BaseURL:            "https://example.test",
		APIKey:             "sk-model-provider",
		ModelPolicy:        " ALLOWLIST ",
		AllowedModelsJSON:  `[" model-b ","model-a","model-b"]`,
		DefaultBillingRate: 1,
	})
	if err != nil {
		t.Fatalf("CreateProvider: %v", err)
	}
	if created.ModelPolicy != storage.GatewayProviderModelPolicyAllowlist ||
		created.AllowedModelsJSON != `["model-b","model-a"]` {
		t.Fatalf("created policy=%q models=%s", created.ModelPolicy, created.AllowedModelsJSON)
	}

	svc.modelsCacheMu.Lock()
	svc.modelsCache[99] = modelsCacheEntry{body: []byte(`{"cached":true}`)}
	svc.modelsCacheMu.Unlock()
	policy := "allowlist"
	allowedJSON := `[" model-c ","model-c"]`
	updated, err := svc.UpdateProvider(created.ID, UpdateProviderInput{
		ModelPolicy:       &policy,
		AllowedModelsJSON: &allowedJSON,
	})
	if err != nil {
		t.Fatalf("UpdateProvider: %v", err)
	}
	if updated.AllowedModelsJSON != `["model-c"]` {
		t.Fatalf("updated models=%s", updated.AllowedModelsJSON)
	}
	svc.modelsCacheMu.Lock()
	cacheLen := len(svc.modelsCache)
	svc.modelsCacheMu.Unlock()
	if cacheLen != 0 {
		t.Fatalf("models cache len=%d, want 0", cacheLen)
	}

	bad := `{}`
	if _, err := svc.UpdateProvider(created.ID, UpdateProviderInput{AllowedModelsJSON: &bad}); err == nil {
		t.Fatal("invalid update should fail")
	}
	persisted, err := providers.FindByID(created.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	if persisted.AllowedModelsJSON != `["model-c"]` {
		t.Fatalf("invalid update changed persisted models: %s", persisted.AllowedModelsJSON)
	}
}

func TestPreviewAndPullProviderModelsPreserveTargetConfig(t *testing.T) {
	var requestCount int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization=%q, want empty for x-api-key", got)
		}
		if got := r.Header.Get("x-api-key"); got != "sk-provider-preview" {
			t.Errorf("x-api-key=%q", got)
		}
		if got := r.Header.Get("X-Provider-Test"); got != "preserved" {
			t.Errorf("X-Provider-Test=%q", got)
		}
		if got := r.Header.Get("User-Agent"); got == "" {
			t.Error("User-Agent should not be empty")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data": []map[string]string{
				{"id": "model-a"},
				{"id": "model-b"},
				{"id": "model-b"},
			},
		})
	}))
	t.Cleanup(upstream.Close)

	db := openGatewayTestDB(t)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("test-provider-model-preview")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	cipherText, err := cipher.Encrypt("sk-provider-preview")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	provider := &storage.GatewayProvider{
		Name:                "preview-provider",
		BaseURL:             upstream.URL,
		APIKeyCipher:        cipherText,
		AuthStyle:           storage.GatewayProviderAuthXAPIKey,
		ExtraHeadersJSON:    `{"X-Provider-Test":"preserved"}`,
		ModelPolicy:         storage.GatewayProviderModelPolicyAllowlist,
		AllowedModelsJSON:   `["model-b","missing-model"]`,
		DefaultBillingRate:  1,
		Enabled:             true,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)

	preview, err := svc.PreviewProviderModels(context.Background(), provider.ID)
	if err != nil {
		t.Fatalf("PreviewProviderModels: %v", err)
	}
	if len(preview.Available) != 2 || preview.Available[0] != "model-a" || preview.Available[1] != "model-b" {
		t.Fatalf("available=%v", preview.Available)
	}
	if len(preview.AllowedModels) != 2 || preview.AllowedModels[0] != "model-b" || preview.AllowedModels[1] != "missing-model" {
		t.Fatalf("allowed=%v", preview.AllowedModels)
	}

	pull := svc.pullRouteModels(context.Background(), &storage.GatewayGroup{}, storage.GatewayRoute{
		ID:                7,
		SourceKind:        storage.GatewayRouteSourceProvider,
		GatewayProviderID: provider.ID,
		Enabled:           true,
	})
	if !pull.rr.OK || pull.rr.ModelCount != 1 || len(pull.models) != 1 || pull.models[0] != "model-b" {
		t.Fatalf("pull result=%+v models=%v", pull.rr, pull.models)
	}
	if requestCount != 2 {
		t.Fatalf("request count=%d, want 2", requestCount)
	}
}

func TestFetchProviderModelsUsesProviderProxy(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Host != "models.invalid" || r.URL.Path != "/v1/models" {
			t.Errorf("proxy target=%s", r.URL.String())
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-proxy-models" {
			t.Errorf("Authorization=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]string{{"id": "model-via-proxy"}},
		})
	}))
	t.Cleanup(proxy.Close)

	proxyURL, err := url.Parse(proxy.URL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	proxyPort, err := strconv.Atoi(proxyURL.Port())
	if err != nil {
		t.Fatalf("parse proxy port: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "http",
		Host:     proxyURL.Hostname(),
		Port:     proxyPort,
	})
	models, err := svc.fetchProviderModels(context.Background(), &storage.GatewayProvider{
		BaseURL:     "http://models.invalid",
		AuthStyle:   storage.GatewayProviderAuthBearer,
		ProxyEnabled: true,
	}, "sk-proxy-models", "")
	if err != nil {
		t.Fatalf("fetchProviderModels: %v", err)
	}
	if len(models) != 1 || models[0] != "model-via-proxy" {
		t.Fatalf("models=%v", models)
	}
}
