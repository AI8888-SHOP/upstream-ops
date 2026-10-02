package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayRouteModelPolicyAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"data":[{"id":"model-a"},{"id":"model-b"}]}`)
	}))
	defer upstream.Close()
	db := openTestDB(t)
	groups, routes := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("route-model-api")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("test-key")
	if err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayGroup{Name: "route-model-api", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	provider := &storage.GatewayProvider{Name: "route-model-api", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	svc := gateway.NewService(groups, storage.NewGatewayKeys(db), routes, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc})
	input := map[string]any{"source_kind": "provider", "gateway_provider_id": provider.ID, "enabled": true, "model_policy": "allowlist", "allowed_models_json": `[" model-b ","model-b"]`}
	save := func() *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(map[string]any{"routes": []any{input}})
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/api/gateway/groups/%d/routes", group.ID), bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	w := save()
	if w.Code != http.StatusOK {
		t.Fatalf("save status=%d body=%s", w.Code, w.Body.String())
	}
	var list struct {
		Items []storage.GatewayRoute `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil || len(list.Items) != 1 {
		t.Fatalf("decode routes: %v body=%s", err, w.Body.String())
	}
	route := list.Items[0]
	if route.ModelPolicy != "allowlist" || route.AllowedModelsJSON != `["model-b"]` {
		t.Fatalf("round-trip policy=%+v", route)
	}
	preview := httptest.NewRecorder()
	r.ServeHTTP(preview, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/gateway/groups/%d/routes/%d/models/preview", group.ID, route.ID), nil))
	var models gateway.RouteModelsPreview
	if preview.Code != http.StatusOK || json.Unmarshal(preview.Body.Bytes(), &models) != nil || len(models.Available) != 2 {
		t.Fatalf("preview status=%d body=%s", preview.Code, preview.Body.String())
	}
	input["id"] = route.ID
	input["allowed_models_json"] = `{}`
	if w := save(); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON accepted: %d %s", w.Code, w.Body.String())
	}
	input["allowed_models_json"] = `[]`
	input["model_policy"] = "all"
	if w := save(); w.Code != http.StatusOK {
		t.Fatalf("restore all failed: %d %s", w.Code, w.Body.String())
	}
	persisted, err := routes.FindByID(route.ID)
	if err != nil || persisted.ModelPolicy != "all" || persisted.AllowedModelsJSON != `[]` {
		t.Fatalf("restore not persisted: %+v err=%v", persisted, err)
	}
}
