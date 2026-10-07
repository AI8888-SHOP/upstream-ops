package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
