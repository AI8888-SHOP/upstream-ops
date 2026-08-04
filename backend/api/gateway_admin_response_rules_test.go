package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayResponseRulesCRUDAndRegexValidation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	group := &storage.GatewayGroup{Name: "response-rules", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	rules := storage.NewGatewayResponseRules(db)
	svc := gateway.NewService(
		groups,
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		nil,
		nil,
	)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc, GatewayResponseRules: rules})

	invalid := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules", group.ID),
		strings.NewReader(`{"name":"invalid","enabled":true,"pattern":"[","target":"assistant_text"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(invalid, request)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid regex status = %d, body=%s", invalid.Code, invalid.Body.String())
	}

	created := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules", group.ID),
		strings.NewReader(`{"name":"declined","enabled":true,"priority":3,"pattern":"(?i)declined","target":"assistant_text","models_json":"[\"gpt-4o\"]","protocols_json":"[\"OpenAI\"]"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(created, request)
	if created.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", created.Code, created.Body.String())
	}
	var rule storage.GatewayResponseRule
	if err := json.Unmarshal(created.Body.Bytes(), &rule); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	if rule.GatewayGroupID != group.ID || rule.ProtocolsJSON != `["openai"]` {
		t.Fatalf("created rule = %#v", rule)
	}

	updated := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPut,
		fmt.Sprintf("/api/gateway/response-rules/%d", rule.ID),
		strings.NewReader(`{"enabled":false,"target":"raw_body","priority":1}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(updated, request)
	if updated.Code != http.StatusOK {
		t.Fatalf("update status = %d, body=%s", updated.Code, updated.Body.String())
	}

	listed := httptest.NewRecorder()
	r.ServeHTTP(listed, httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules", group.ID),
		nil,
	))
	if listed.Code != http.StatusOK || !strings.Contains(listed.Body.String(), `"target":"raw_body"`) {
		t.Fatalf("list status = %d, body=%s", listed.Code, listed.Body.String())
	}

	deleted := httptest.NewRecorder()
	r.ServeHTTP(deleted, httptest.NewRequest(
		http.MethodDelete,
		fmt.Sprintf("/api/gateway/response-rules/%d", rule.ID),
		nil,
	))
	if deleted.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", deleted.Code, deleted.Body.String())
	}
}
