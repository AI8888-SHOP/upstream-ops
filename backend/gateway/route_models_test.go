package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestRouteModelPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, policy, models, model string
		allowed, invalid            bool
	}{
		{"legacy", "", "", "anything", true, false},
		{"all", "all", `[]`, "anything", true, false},
		{"selected", "allowlist", `["model-a"]`, "model-a", true, false},
		{"trim", " ALLOWLIST ", `[" model-a "]`, " model-a ", true, false},
		{"not selected", "allowlist", `["model-a"]`, "model-b", false, false},
		{"case sensitive", "allowlist", `["Model-A"]`, "model-a", false, false},
		{"empty allowlist", "allowlist", `[]`, "model-a", false, false},
		{"blank allowlist", "allowlist", "", "model-a", false, false},
		{"no model", "allowlist", `["model-a"]`, "", false, false},
		{"not a glob", "allowlist", `["model-*"]`, "model-a", false, false},
		{"invalid policy", "denylist", `[]`, "model-a", false, true},
		{"invalid JSON", "allowlist", `{}`, "model-a", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			route := &storage.GatewayRoute{ModelPolicy: tc.policy, AllowedModelsJSON: tc.models}
			allowed, err := RouteAllowsUpstreamModel(route, tc.model)
			if (err != nil) != tc.invalid || allowed != tc.allowed {
				t.Fatalf("allowed=%v err=%v", allowed, err)
			}
		})
	}
	filtered, err := FilterRouteModels(&storage.GatewayRoute{ModelPolicy: "allowlist", AllowedModelsJSON: `["model-b"]`}, []string{"model-a", "model-b", " model-b "})
	if err != nil || !reflect.DeepEqual(filtered, []string{"model-b"}) {
		t.Fatalf("filtered=%v err=%v", filtered, err)
	}
}

func TestFilterMonitorRoutesForRequestedModelUsesFinalModel(t *testing.T) {
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	routes := []storage.GatewayRoute{
		{ID: 1, ModelPolicy: "allowlist", AllowedModelsJSON: `["upstream-model"]`, ModelMappingJSON: `{"alias":"intermediate"}`},
		{ID: 2, ModelPolicy: "allowlist", AllowedModelsJSON: `["alias"]`},
		{ID: 3, ModelPolicy: "allowlist", AllowedModelsJSON: `[]`},
		{ID: 4},
	}
	filtered, err := svc.runtime().filterRoutesForRequestedModel(routes, "alias", map[string]string{"intermediate": "upstream-model", "alias": "upstream-model"})
	if err != nil || len(filtered) != 2 || filtered[0].ID != 1 || filtered[1].ID != 4 {
		t.Fatalf("filtered=%+v err=%v", filtered, err)
	}
	// Discovery and model-less resource endpoints retain their existing behavior.
	filtered, err = svc.runtime().filterRoutesForRequestedModel(routes, "", nil)
	if err != nil || len(filtered) != len(routes) {
		t.Fatalf("model-less filtered=%+v err=%v", filtered, err)
	}
}

func TestRoutePolicyIntersectsDirectProviderPolicy(t *testing.T) {
	db := openGatewayTestDB(t)
	providers := storage.NewGatewayProviders(db)
	provider := &storage.GatewayProvider{Name: "intersection", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true, ModelPolicy: "allowlist", AllowedModelsJSON: `["model-a","model-b"]`}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	routes := []storage.GatewayRoute{{ID: 1, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, ModelPolicy: "allowlist", AllowedModelsJSON: `["model-b","model-c"]`}}
	for _, model := range []string{"model-a", "model-b", "model-c"} {
		filtered, err := svc.runtime().filterRoutesForRequestedModel(routes, model, nil)
		if err != nil || (len(filtered) == 1) != (model == "model-b") {
			t.Fatalf("model=%s filtered=%+v err=%v", model, filtered, err)
		}
	}
}

func TestSaveRouteModelPolicyRoundTripAndIsolation(t *testing.T) {
	db := openGatewayTestDB(t)
	groups, routes := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db)
	svc := NewService(groups, nil, routes, nil, nil, nil, &fakeChannelAPIForResort{}, nil, nil)
	group := &storage.GatewayGroup{Name: "route-models", Status: storage.GatewayGroupStatusActive}
	other := &storage.GatewayGroup{Name: "other-route-models", Status: storage.GatewayGroupStatusActive}
	for _, item := range []*storage.GatewayGroup{group, other} {
		if err := groups.Create(item); err != nil {
			t.Fatal(err)
		}
	}
	input := RouteInput{SourceChannelID: 1, Enabled: true, SourceGroupName: "same-source", ModelPolicy: " ALLOWLIST ", AllowedModelsJSON: `[" model-b ","model-a","model-b",""]`}
	saved, err := svc.SaveRoutes(group.ID, []RouteInput{input})
	if err != nil || len(saved) != 1 {
		t.Fatalf("save=%+v err=%v", saved, err)
	}
	if saved[0].ModelPolicy != "allowlist" || saved[0].AllowedModelsJSON != `["model-b","model-a"]` {
		t.Fatalf("not normalized: %+v", saved[0])
	}
	input.ID = saved[0].ID
	// Editing policy must preserve the route identity and its existing key.
	saved[0].SourceAPIKeyCipher = "original-key-cipher"
	if err := routes.Update(&saved[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := routes.ListByGroupID(group.ID); err != nil {
		t.Fatal(err)
	}
	svc.modelsCache[group.ID] = modelsCacheEntry{body: []byte(`{"stale":true}`)}
	input.AllowedModelsJSON = `[]`
	updated, err := svc.SaveRoutes(group.ID, []RouteInput{input})
	if err != nil || updated[0].ID != input.ID || updated[0].SourceAPIKeyCipher != "original-key-cipher" || updated[0].AllowedModelsJSON != `[]` {
		t.Fatalf("update=%+v err=%v", updated, err)
	}
	if _, cached := svc.modelsCache[group.ID]; cached {
		t.Fatal("model listing cache was not invalidated")
	}
	for _, bad := range []RouteInput{
		{ID: input.ID, SourceChannelID: 1, ModelPolicy: "bogus"},
		{ID: input.ID, SourceChannelID: 1, ModelPolicy: "allowlist", AllowedModelsJSON: `{}`},
		{ID: input.ID, SourceChannelID: 1, ModelPolicy: "allowlist", AllowedModelsJSON: `["ok",1]`},
	} {
		if _, err := svc.SaveRoutes(group.ID, []RouteInput{bad}); err == nil {
			t.Fatal("invalid policy was saved")
		}
	}
	persisted, err := routes.FindByID(input.ID)
	if err != nil || persisted.ModelPolicy != "allowlist" || persisted.AllowedModelsJSON != `[]` {
		t.Fatalf("invalid writes changed policy: %+v err=%v", persisted, err)
	}
	defaults, err := svc.SaveRoutes(other.ID, []RouteInput{{SourceChannelID: 1, SourceGroupName: "same-source", Enabled: true}})
	if err != nil || defaults[0].ModelPolicy != "all" || defaults[0].AllowedModelsJSON != `[]` {
		t.Fatalf("defaults or group isolation broken: %+v err=%v", defaults, err)
	}
	clone, err := groups.Clone(group.ID, "cloned-route-models", nil)
	if err != nil {
		t.Fatal(err)
	}
	cloned, err := routes.ListByGroupID(clone.Group.ID)
	if err != nil || len(cloned) != 1 || cloned[0].ModelPolicy != "allowlist" || cloned[0].AllowedModelsJSON != `[]` {
		t.Fatalf("clone lost model policy: %+v err=%v", cloned, err)
	}
}

func TestRouteModelPreviewAndPublicModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var pulls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pulls.Add(1)
		if r.URL.Path == "/v1/messages/count_tokens" {
			var body struct {
				Model string `json:"model"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Model != "model-b" {
				t.Errorf("token counting must send the allowed mapped model: %+v err=%v", body, err)
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprint(w, `{"input_tokens":7}`)
			return
		}
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Errorf("unexpected preview request: path=%s auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"data":[{"id":"model-a"},{"id":"model-b"},{"id":"model-b"}]}`)
	}))
	defer upstream.Close()
	db := openGatewayTestDB(t)
	groups, routes, keys := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db), storage.NewGatewayKeys(db)
	cipher, err := crypto.NewCipher("route-model-preview")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("upstream-key")
	if err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayGroup{Name: "preview", Status: storage.GatewayGroupStatusActive, ModelsMode: storage.GatewayModelsModeAuto}
	other := &storage.GatewayGroup{Name: "preview-other", Status: storage.GatewayGroupStatusActive}
	for _, item := range []*storage.GatewayGroup{group, other} {
		if err := groups.Create(item); err != nil {
			t.Fatal(err)
		}
	}
	channel := &storage.Channel{Name: "preview-monitor", SiteURL: upstream.URL, Type: storage.ChannelTypeNewAPI}
	if err := db.Create(channel).Error; err != nil {
		t.Fatal(err)
	}
	route := storage.GatewayRoute{GatewayGroupID: group.ID, SourceChannelID: channel.ID, SourceAPIKeyCipher: secret, Enabled: true, ModelPolicy: "allowlist", AllowedModelsJSON: `["model-b"]`, ModelMappingJSON: `{"alias-b":"model-b"}`}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, nil, nil, storage.NewChannels(db), nil, cipher, nil)
	preview, err := svc.PreviewRouteModels(context.Background(), group.ID, route.ID)
	if err != nil || !reflect.DeepEqual(preview.Available, []string{"model-a", "model-b"}) {
		t.Fatalf("preview=%+v err=%v", preview, err)
	}
	encoded, err := json.Marshal(preview)
	if err != nil || strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "upstream-key") {
		t.Fatal("preview exposed credential material")
	}
	before := pulls.Load()
	if _, err := svc.PreviewRouteModels(context.Background(), other.ID, route.ID); err == nil || pulls.Load() != before {
		t.Fatal("cross-group preview reached upstream")
	}
	collected, _, err := svc.collectGroupModels(context.Background(), group.ID)
	if err != nil || len(collected) != 1 || collected[0].ID != "model-b" {
		t.Fatalf("group model sync ignores policy: %+v err=%v", collected, err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-key"), Status: storage.GatewayKeyStatusActive}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	c.Request.Header.Set("Authorization", "Bearer client-key")
	svc.HandleModels(c)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"alias-b"`) || strings.Contains(w.Body.String(), `"model-a"`) {
		t.Fatalf("model listing=%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(`{"model":"alias-b","messages":[{"role":"user","content":"hi"}]}`))
	c.Request.Header.Set("Authorization", "Bearer client-key")
	before = pulls.Load()
	svc.runtime().HandleCountTokens(c)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"input_tokens":7`) || pulls.Load() != before+1 {
		t.Fatalf("count_tokens did not reach mapped allowed model: %d %s", w.Code, w.Body.String())
	}
	// Fetching candidates did not overwrite the persisted restriction.
	persisted, err := routes.FindByID(route.ID)
	if err != nil || persisted.ModelPolicy != "allowlist" || persisted.AllowedModelsJSON != `["model-b"]` {
		t.Fatalf("preview mutated policy: %+v err=%v", persisted, err)
	}
	before = pulls.Load()
	result := svc.probeRouteModel(context.Background(), group, route, "model-a", nil)
	recovery := svc.probePersistedRouteModel(context.Background(), group, route, "model-a", "", config.GatewayConfig{})
	if result.OK || recovery.OK || !recovery.Permanent || !strings.Contains(result.Error, "route model policy") || !strings.Contains(recovery.Error, "route model policy") || pulls.Load() != before {
		t.Fatalf("excluded model reached probe upstream: manual=%+v recovery=%+v", result, recovery)
	}
}

func TestMonitorRouteAllowlistEnforcedOnBothForwardPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, coordinated := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("coordinated=%v/stream=%v", coordinated, stream), func(t *testing.T) {
				db := openGatewayTestDB(t)
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				cipher, err := crypto.NewCipher("route-policy-forward")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("upstream-key")
				if err != nil {
					t.Fatal(err)
				}
				group := &storage.GatewayGroup{Name: "policy-forward", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc", RetryEnabled: true, RetryCount: 2, FailoverEnabled: true, FailoverMax: 8, RequestMaxAttempts: 2, ResponseValidationEnabled: coordinated, ResponseValidationStreamMode: "prefix"}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				if err := keys.Create(&storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-key"), Status: storage.GatewayKeyStatusActive}); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "unused", Enabled: true, Target: "error_message", Pattern: "never-match"}); err != nil {
					t.Fatal(err)
				}
				var calls [3]atomic.Int32
				for index := 0; index < 3; index++ {
					index := index
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls[index].Add(1)
						w.Header().Set("Content-Type", "application/json")
						if index != 2 {
							w.WriteHeader(http.StatusServiceUnavailable)
							_, _ = fmt.Fprint(w, `{"error":{"message":"busy"}}`)
							return
						}
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
						}
						body := zeroUsageTestBody(protocol.KindOpenAIResponses, stream, "allowed answer", 2, true, true)
						_, _ = fmt.Fprint(w, strings.ReplaceAll(body, `"input_tokens":0`, `"input_tokens":10`))
					}))
					t.Cleanup(upstream.Close)
					channel := &storage.Channel{Name: fmt.Sprintf("monitor-%d", index), SiteURL: upstream.URL, Type: storage.ChannelTypeNewAPI}
					if err := db.Create(channel).Error; err != nil {
						t.Fatal(err)
					}
					rate := float64(index+1) / 10
					allowed := `["gpt-test"]`
					if index == 0 {
						allowed = `["other-model"]`
					}
					route := storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceChannelID: channel.ID, SourceAPIKeyCipher: secret, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate, ModelPolicy: "allowlist", AllowedModelsJSON: allowed}
					if err := db.Create(&route).Error; err != nil {
						t.Fatal(err)
					}
				}
				svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
				svc.SetResponseRules(rules)
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"gpt-test","stream":%v,"input":"hi"}`, stream)))
				c.Request.Header.Set("Authorization", "Bearer client-key")
				svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "allowed answer") || calls[0].Load() != 0 || calls[1].Load() != 1 || calls[2].Load() != 1 {
					t.Fatalf("policy/failover broken: status=%d calls=%d/%d/%d body=%s", w.Code, calls[0].Load(), calls[1].Load(), calls[2].Load(), w.Body.String())
				}
				w = httptest.NewRecorder()
				c, _ = gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"unsupported-model","input":"hi"}`))
				c.Request.Header.Set("Authorization", "Bearer client-key")
				svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
				if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "model_not_found") || calls[0].Load() != 0 || calls[1].Load() != 1 || calls[2].Load() != 1 {
					t.Fatalf("no eligible model must not bypass policy: status=%d body=%s", w.Code, w.Body.String())
				}
			})
		}
	}
}

func TestSharedModelProbeSkipsRouteThatNoLongerAllowsModel(t *testing.T) {
	db := openGatewayTestDB(t)
	groups, routes := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db)
	var saved []storage.GatewayRoute
	for index, models := range []string{`["other-model"]`, `["target-model"]`} {
		group := &storage.GatewayGroup{Name: fmt.Sprintf("shared-policy-%d", index), Status: storage.GatewayGroupStatusActive}
		if err := groups.Create(group); err != nil {
			t.Fatal(err)
		}
		route := storage.GatewayRoute{GatewayGroupID: group.ID, SourceChannelID: 1, SourceGroupName: "same-source", SourceAPIKeyID: 77, SourceAPIKeyCipher: "shared-key", Enabled: true, ModelPolicy: "allowlist", AllowedModelsJSON: models}
		if err := db.Create(&route).Error; err != nil {
			t.Fatal(err)
		}
		saved = append(saved, route)
	}
	accept := func(route *storage.GatewayRoute) bool {
		allowed, err := RouteAllowsUpstreamModel(route, "target-model")
		return err == nil && allowed
	}
	scope := storage.GatewaySharedModelCooldownScope(&saved[0])
	selected, err := routes.FindActiveRouteForSharedModelCooldown(scope, saved[0].ID, accept)
	if err != nil || selected == nil || selected.ID != saved[1].ID {
		t.Fatalf("shared probe did not choose eligible sibling: %+v err=%v", selected, err)
	}
	saved[1].AllowedModelsJSON = `[]`
	if err := routes.Update(&saved[1]); err != nil {
		t.Fatal(err)
	}
	if selected, err := routes.FindActiveRouteForSharedModelCooldown(scope, saved[0].ID, accept); err != nil || selected != nil {
		t.Fatalf("shared probe bypassed both allowlists: %+v err=%v", selected, err)
	}
}
