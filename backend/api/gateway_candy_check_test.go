package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayCandyCheckAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups, routes := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db)
	svc := gateway.NewService(groups, nil, routes, nil, nil, nil, nil, nil, nil)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	for _, body := range []string{
		`{"name":"bad","candy_check_enabled":true}`,
		`{"name":"bad","candy_check_interval_minutes":0}`,
		`{"name":"bad","candy_check_cooldown_minutes":43201}`,
		`{"name":"bad","candy_check_reasoning_effort":"extreme"}`,
	} {
		if w := request(http.MethodPost, "/api/gateway/groups", body); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid config accepted: %d %s", w.Code, w.Body.String())
		}
	}
	w := request(http.MethodPost, "/api/gateway/groups", `{"name":"candy-api","candy_check_enabled":true,"candy_check_model":"test-model","candy_check_interval_minutes":2,"candy_check_cooldown_minutes":9,"candy_check_reasoning_effort":"default"}`)
	if w.Code != http.StatusOK && w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	group, err := groups.FindByName("candy-api")
	if err != nil {
		t.Fatal(err)
	}
	if !group.CandyCheckEnabled || group.CandyCheckIntervalMinutes != 2 || group.CandyCheckCooldownMinutes != 9 || group.CandyCheckReasoningEffort != "default" {
		t.Fatalf("policy not persisted: %+v", group)
	}
	if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{{Enabled: true, SourceChannelID: 1, SourceAPIKeyCipher: "cipher"}}); err != nil {
		t.Fatal(err)
	}
	list, _ := routes.ListByGroupID(group.ID)
	route := list[0]
	key := storage.GatewayCandyCheckConfigKey(group, &route, nil)
	claim, err := routes.ClaimCandyCheck(route.ID, key, "test-model", time.Now(), time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := routes.FinishCandyCheck(*claim, storage.GatewayRouteCandyCheck{Status: "incorrect", AnswerPreview: "22"}, time.Now()); err != nil {
		t.Fatal(err)
	}
	w = request(http.MethodGet, fmt.Sprintf("/api/gateway/groups/%d/routes", group.ID), "")
	var result struct {
		Items []storage.GatewayRoute `json:"items"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || len(result.Items) != 1 || result.Items[0].CandyCheck == nil {
		t.Fatalf("state missing: %s", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "lease_token") || strings.Contains(w.Body.String(), "config_key") || strings.Contains(w.Body.String(), "cipher") {
		t.Fatalf("private probe state exposed: %s", w.Body.String())
	}
	w = request(http.MethodGet, fmt.Sprintf("/api/gateway/groups/%d/candy-checks", group.ID), "")
	var states struct {
		Items []struct {
			RouteID    uint                            `json:"route_id"`
			CandyCheck *storage.GatewayRouteCandyCheck `json:"candy_check"`
		} `json:"items"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &states) != nil || len(states.Items) != 1 || states.Items[0].RouteID != route.ID || states.Items[0].CandyCheck.Status != "incorrect" {
		t.Fatalf("runtime state endpoint: %s", w.Body.String())
	}
	w = request(http.MethodPost, fmt.Sprintf("/api/gateway/routes/%d/candy-check/clear", route.ID), "")
	if w.Code != http.StatusOK {
		t.Fatalf("clear: %s", w.Body.String())
	}
	loaded, err := routes.FindByID(route.ID)
	if err != nil || loaded.CandyCheck.Blocks(time.Now()) || loaded.CandyCheck.Status != "manual" {
		t.Fatalf("clear failed: %+v %v", loaded, err)
	}
	w = request(http.MethodPut, fmt.Sprintf("/api/gateway/groups/%d", group.ID), `{"candy_check_enabled":false}`)
	if w.Code != http.StatusOK {
		t.Fatalf("disable: %s", w.Body.String())
	}
	group, _ = groups.FindByID(group.ID)
	if group.CandyCheckEnabled || group.CandyCheckModel != "test-model" {
		t.Fatalf("partial update: %+v", group)
	}
}

func TestManualCandyCheckAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var correct atomic.Bool
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer manual-test-key" {
			t.Errorf("wrong route: %s", r.URL.Path)
		}
		answer := "22"
		if correct.Load() {
			answer = "21"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":%q},"finish_reason":"stop"}]}`, answer)
	}))
	defer upstream.Close()
	db := openTestDB(t)
	groups, routes, providers := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db), storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("manual-candy-test")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("manual-test-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := &storage.GatewayProvider{Name: "manual-test", BaseURL: upstream.URL, Enabled: true, APIKeyCipher: secret}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayGroup{Name: "manual-candy", GatewayCandyCheckPolicy: storage.GatewayCandyCheckPolicy{CandyCheckModel: "test-model", CandyCheckIntervalMinutes: 3, CandyCheckCooldownMinutes: 10}}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID}}); err != nil {
		t.Fatal(err)
	}
	list, err := routes.ListByGroupID(group.ID)
	if err != nil || len(list) != 1 {
		t.Fatalf("routes: %v", err)
	}
	route := list[0]
	svc := gateway.NewService(groups, nil, routes, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	router := gin.New()
	registerGatewayAdmin(router.Group("/api"), &Deps{DB: db, Gateway: svc})
	run := func(id string, status int) *storage.GatewayRouteCandyCheck {
		t.Helper()
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/gateway/routes/"+id+"/candy-check/run", nil))
		if w.Code != status {
			t.Fatalf("manual test HTTP %d: %s", w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "manual-test-key") || strings.Contains(w.Body.String(), "lease_token") || strings.Contains(w.Body.String(), "config_key") {
			t.Fatalf("private state exposed: %s", w.Body.String())
		}
		if status != http.StatusOK {
			return nil
		}
		var response struct {
			Result *storage.GatewayRouteCandyCheck `json:"result"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.Result == nil {
			t.Fatalf("missing result: %s", w.Body.String())
		}
		return response.Result
	}
	id := fmt.Sprint(route.ID)
	run("bad", http.StatusBadRequest)
	run("0", http.StatusBadRequest)
	run("999999", http.StatusNotFound)
	result := run(id, http.StatusOK)
	if result.Status != "incorrect" || !result.Active || result.CooldownUntil != nil || result.NextCheckAt != nil {
		t.Fatalf("manual-only mode cooled or scheduled route: %+v", result)
	}
	svc.RunCandyChecks(context.Background())
	if calls.Load() != 1 {
		t.Fatal("manual-only mode started periodic checks")
	}
	group.CandyCheckEnabled = true
	if err := groups.Update(group); err != nil {
		t.Fatal(err)
	}
	result = run(id, http.StatusOK)
	if !result.Blocks(time.Now()) || result.Status != "incorrect" {
		t.Fatalf("enabled policy did not cool route: %+v", result)
	}
	key := storage.GatewayCandyCheckConfigKey(group, &route, provider)
	claim, err := routes.ClaimManualCandyCheck(route.ID, key, "test-model", time.Now(), time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("manual claim while cooling: %v", err)
	}
	loaded, err := routes.FindByID(route.ID)
	if err != nil || !loaded.CandyCheck.Blocks(time.Now()) {
		t.Fatal("starting a retest prematurely released cooldown")
	}
	run(id, http.StatusConflict)
	svc.RunCandyChecks(context.Background())
	if calls.Load() != 2 {
		t.Fatal("duplicate manual/scheduled probe reached upstream")
	}
	if ok, err := routes.FinishManualCandyCheck(*claim, storage.GatewayRouteCandyCheck{Status: "deferred"}, time.Now()); err != nil || !ok {
		t.Fatalf("defer: %v %v", ok, err)
	}
	if err := routes.SetModelTempUnschedulable(route.ID, "other-model", time.Now().Add(time.Hour), "unrelated", time.Now(), "other-request"); err != nil {
		t.Fatal(err)
	}
	correct.Store(true)
	result = run(id, http.StatusOK)
	if result.Status != "correct" || result.CooldownUntil != nil || result.AnswerPreview != "21" || calls.Load() != 3 {
		t.Fatalf("manual recovery: %+v calls=%d", result, calls.Load())
	}
	loaded, err = routes.FindByID(route.ID)
	if err != nil || gateway.IsRouteSchedulableForModel(loaded, "other-model", time.Now()) {
		t.Fatal("manual candy success cleared an unrelated model cooldown")
	}
	var usageCount int64
	if err := db.Model(&storage.GatewayUsageLog{}).Count(&usageCount).Error; err != nil || usageCount != 0 {
		t.Fatalf("manual probe entered customer usage: %d %v", usageCount, err)
	}
	group.CandyCheckEnabled, group.CandyCheckModel = false, ""
	if err := groups.Update(group); err != nil {
		t.Fatal(err)
	}
	run(id, http.StatusBadRequest)
	if calls.Load() != 3 {
		t.Fatal("missing test model reached upstream")
	}
}
