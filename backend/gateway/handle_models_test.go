package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestPublicRouteModelIDs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		known        []string
		allowed      []string
		route, group ModelMap
		want         []string
	}{
		{
			name:  "unmapped catalog retains stable order and deduplicates",
			known: []string{" model-b ", "model-a", "model-b", ""}, allowed: []string{"model-a", "model-b"},
			want: []string{"model-b", "model-a"},
		},
		{
			name: "all aliases from both layers are exposed", known: []string{"upstream"}, allowed: []string{"upstream"},
			route: ModelMap{"alias-b": "upstream", "alias-a": "upstream"},
			group: ModelMap{"group-b": "upstream", "group-a": "upstream"},
			want:  []string{"upstream", "alias-a", "alias-b", "group-a", "group-b"},
		},
		{
			name: "route then group chain", known: []string{"final"}, allowed: []string{"final"},
			route: ModelMap{"client-b": "bridge", "client-a": "bridge"}, group: ModelMap{"bridge": "final"},
			want: []string{"final", "bridge", "client-a", "client-b"},
		},
		{
			name: "route exact mapping prevents conflicting group alias", known: []string{"final"}, allowed: []string{"final"},
			route: ModelMap{"shared": "unavailable"}, group: ModelMap{"shared": "final", "valid": "final"},
			want: []string{"final", "valid"},
		},
		{
			name: "group remaps route target outside final allowlist", known: []string{"final", "blocked"}, allowed: []string{"final"},
			route: ModelMap{"client": "final"}, group: ModelMap{"final": "blocked"}, want: []string{},
		},
		{
			name: "aliases cannot invent unknown upstream targets", known: []string{"final"}, allowed: []string{"final"},
			route: ModelMap{"unknown": "missing"}, want: []string{"final"},
		},
		{
			name:  "route wildcard is bounded and exact mapping wins",
			known: []string{"final", "known-client", "bad"}, allowed: []string{"final"},
			route: ModelMap{"*": "final", "bad": "unavailable"},
			group: ModelMap{"explicit": "unavailable"},
			want:  []string{"final", "known-client", "explicit"},
		},
		{
			name: "group wildcard applies after route exact mapping", known: []string{"final"}, allowed: []string{"final"},
			route: ModelMap{"client": "bridge"}, group: ModelMap{"*": "final", "blocked": "unavailable"},
			want: []string{"final", "client"},
		},
		{
			name: "wildcard alone does not manufacture catalog entries", known: nil, allowed: nil,
			route: ModelMap{"*": "unknown", "client": "unknown"}, want: []string{},
		},
		{
			name: "each layer resolves once not recursively", known: []string{"final"}, allowed: []string{"final"},
			route: ModelMap{"client": "bridge", "bridge": "final"}, want: []string{"final", "bridge"},
		},
		{
			name: "model targets remain case sensitive", known: []string{"Final"}, allowed: []string{"Final"},
			route: ModelMap{"lower": "final", "matching": "Final"}, want: []string{"Final", "matching"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Repeated resolution guards against nondeterministic Go map order.
			for repeat := 0; repeat < 10; repeat++ {
				got := publicRouteModelIDs(tc.known, tc.allowed, tc.route, tc.group)
				if !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("models=%v want=%v", got, tc.want)
				}
				for _, candidate := range got {
					resolved, _ := ResolveModel(candidate, tc.route, tc.group)
					found := false
					for _, allowed := range tc.allowed {
						found = found || allowed == resolved
					}
					if candidate == "*" || !found {
						t.Fatalf("advertised uncallable model %q resolving to %q", candidate, resolved)
					}
				}
			}
		})
	}
}

func TestHandleModels_AggregatesProviderRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer sk-upstream-test" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data": []map[string]any{
				{"id": "deepseek-v4-flash"},
				{"id": "kimi-k2.5"},
			},
		})
	}))
	t.Cleanup(up.Close)

	db := openGatewayTestDB(t)
	groupsRepo := storage.NewGatewayGroups(db)
	keysRepo := storage.NewGatewayKeys(db)
	routesRepo := storage.NewGatewayRoutes(db)
	providersRepo := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("test-secret-handle-models")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	g := &storage.GatewayGroup{
		Name:              "ds-test",
		Status:            storage.GatewayGroupStatusActive,
		ModelsMode:        storage.GatewayModelsModeAuto,
		RateSortDirection: "asc",
	}
	if err := groupsRepo.Create(g); err != nil {
		t.Fatalf("create group: %v", err)
	}

	cipherText, err := cipher.Encrypt("sk-upstream-test")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	p := &storage.GatewayProvider{
		Name:         "opencode",
		BaseURL:      up.URL,
		APIKeyCipher: cipherText,
		APIKeyHint:   "sk-u…test",
		Enabled:      true,
	}
	if err := providersRepo.Create(p); err != nil {
		t.Fatalf("create provider: %v", err)
	}

	if err := routesRepo.SaveForGroup(g.ID, []storage.GatewayRoute{{
		GatewayGroupID:    g.ID,
		Position:          0,
		SourceKind:        storage.GatewayRouteSourceProvider,
		GatewayProviderID: p.ID,
		Weight:            1,
		Enabled:           true,
		RateConvertMode:   "custom",
		RateConvertValue:  1,
	}}); err != nil {
		t.Fatalf("save routes: %v", err)
	}

	clientKey := "sk-5f613bdceadebf61"
	keyCipher, err := cipher.Encrypt(clientKey)
	if err != nil {
		t.Fatalf("encrypt client key: %v", err)
	}
	gk := &storage.GatewayKey{
		GroupID:   g.ID,
		Name:      "client",
		KeyHash:   HashAPIKey(clientKey),
		KeyPrefix: KeyPrefix(clientKey),
		KeyCipher: keyCipher,
		Status:    storage.GatewayKeyStatusActive,
	}
	if err := keysRepo.Create(gk); err != nil {
		t.Fatalf("create key: %v", err)
	}

	svc := NewService(groupsRepo, keysRepo, routesRepo, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providersRepo)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+clientKey)
	c.Request = req

	svc.HandleModels(c)

	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var payload struct {
		Object string `json:"object"`
		Data   []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v body=%s", err, w.Body.String())
	}
	if len(payload.Data) != 2 {
		t.Fatalf("want 2 models, got %d body=%s", len(payload.Data), w.Body.String())
	}
	got := map[string]bool{}
	for _, m := range payload.Data {
		got[m.ID] = true
	}
	if !got["deepseek-v4-flash"] || !got["kimi-k2.5"] {
		t.Fatalf("models=%v", payload.Data)
	}
}

func TestHandleModels_AliasesStayWithinRouteAndGroupPolicies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openGatewayTestDB(t)
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("model-discovery-aliases")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("upstream-model-key")
	if err != nil {
		t.Fatal(err)
	}
	newProvider := func(name string, models []string, allowed string) *storage.GatewayProvider {
		t.Helper()
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer upstream-model-key" {
				http.Error(w, "unexpected discovery request", http.StatusBadRequest)
				return
			}
			data := make([]map[string]string, 0, len(models))
			for _, model := range models {
				data = append(data, map[string]string{"id": model})
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
		}))
		t.Cleanup(upstream.Close)
		provider := &storage.GatewayProvider{Name: name, BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: "allowlist", AllowedModelsJSON: allowed}
		if err := providers.Create(provider); err != nil {
			t.Fatal(err)
		}
		return provider
	}
	first := newProvider("source-a", []string{"target-a", "provider-denied", "route-denied"}, `["target-a","route-denied"]`)
	second := newProvider("source-b", []string{"target-b"}, `["target-b"]`)
	group := &storage.GatewayGroup{Name: "aliases", Status: storage.GatewayGroupStatusActive, ModelsMode: storage.GatewayModelsModeAuto, ModelMappingJSON: `{"bridge":"target-a","shared":"target-a"}`}
	other := &storage.GatewayGroup{Name: "separate-aliases", Status: storage.GatewayGroupStatusActive, ModelsMode: storage.GatewayModelsModeAuto}
	for _, item := range []*storage.GatewayGroup{group, other} {
		if err := groups.Create(item); err != nil {
			t.Fatal(err)
		}
		key := &storage.GatewayKey{GroupID: item.ID, Name: item.Name, KeyHash: HashAPIKey(item.Name), Status: storage.GatewayKeyStatusActive}
		if err := keys.Create(key); err != nil {
			t.Fatal(err)
		}
	}
	if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{
		{GatewayGroupID: group.ID, Position: 0, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: first.ID, Enabled: true, Weight: 1, RateConvertMode: "custom", RateConvertValue: 1, ModelPolicy: "allowlist", AllowedModelsJSON: `["target-a","provider-denied"]`, ModelMappingJSON: `{"alias-a":"bridge","alias-b":"bridge","shared":"missing","provider-denied-alias":"provider-denied","route-denied-alias":"route-denied"}`},
		{GatewayGroupID: group.ID, Position: 1, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: second.ID, Enabled: true, Weight: 1, RateConvertMode: "custom", RateConvertValue: 2, ModelPolicy: "allowlist", AllowedModelsJSON: `["target-b"]`, ModelMappingJSON: `{"other-alias":"target-b","must-not-leak":"target-a"}`},
	}); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(other.ID, []storage.GatewayRoute{{
		GatewayGroupID: other.ID, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: first.ID, Enabled: true, Weight: 1, RateConvertMode: "custom", RateConvertValue: 1, ModelPolicy: "allowlist", AllowedModelsJSON: `["target-a"]`, ModelMappingJSON: `{"separate-alias":"target-a"}`,
	}}); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	check := func(key string, want []string) {
		t.Helper()
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/models", nil)
		c.Request.Header.Set("Authorization", "Bearer "+key)
		svc.HandleModels(c)
		var response struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &response) != nil {
			t.Fatalf("models status=%d body=%s", w.Code, w.Body.String())
		}
		got := make([]string, 0, len(response.Data))
		for _, model := range response.Data {
			got = append(got, model.ID)
		}
		sort.Strings(got)
		sort.Strings(want)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("key=%s models=%v want=%v", key, got, want)
		}
	}
	check(group.Name, []string{"target-a", "bridge", "alias-a", "alias-b", "target-b", "other-alias"})
	check(other.Name, []string{"target-a", "separate-alias"})
	// A cached catalog is group-local and does not acquire the other group's aliases.
	check(group.Name, []string{"target-a", "bridge", "alias-a", "alias-b", "target-b", "other-alias"})
}
