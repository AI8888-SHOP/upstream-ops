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

func TestGatewayResponseRulesExportImportAcrossGroups(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	source := &storage.GatewayGroup{Name: "rules-export-source", Status: storage.GatewayGroupStatusActive}
	target := &storage.GatewayGroup{Name: "rules-export-target", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(source); err != nil {
		t.Fatalf("create source group: %v", err)
	}
	if err := groups.Create(target); err != nil {
		t.Fatalf("create target group: %v", err)
	}
	rules := storage.NewGatewayResponseRules(db)
	if err := rules.Create(&storage.GatewayResponseRule{
		GatewayGroupID: source.ID,
		Name:           "capacity",
		Enabled:        true,
		Priority:       7,
		Pattern:        `(?i)selected model is at capacity`,
		Target:         storage.GatewayResponseRuleTargetAssistantText,
		ModelsJSON:     `["gpt-5"]`,
		ProtocolsJSON:  `["OpenAI"]`,
	}); err != nil {
		t.Fatalf("create source rule: %v", err)
	}
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

	exported := httptest.NewRecorder()
	r.ServeHTTP(exported, httptest.NewRequest(
		http.MethodGet,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules/export", source.ID),
		nil,
	))
	if exported.Code != http.StatusOK {
		t.Fatalf("export status = %d, body=%s", exported.Code, exported.Body.String())
	}
	if !strings.Contains(exported.Header().Get("Content-Disposition"), "response-rules-") {
		t.Fatalf("export content disposition = %q", exported.Header().Get("Content-Disposition"))
	}
	var bundle storage.GatewayResponseRuleBundle
	if err := json.Unmarshal(exported.Body.Bytes(), &bundle); err != nil {
		t.Fatalf("decode export: %v", err)
	}
	if bundle.Kind != storage.GatewayResponseRuleBundleKind || bundle.Version != storage.GatewayResponseRuleBundleVersion || len(bundle.Rules) != 1 {
		t.Fatalf("export bundle = %#v", bundle)
	}
	if strings.Contains(exported.Body.String(), "gateway_group_id") || strings.Contains(exported.Body.String(), `"id"`) {
		t.Fatalf("export leaked database fields: %s", exported.Body.String())
	}

	body, err := json.Marshal(map[string]any{
		"kind":    bundle.Kind,
		"version": bundle.Version,
		"rules":   bundle.Rules,
	})
	if err != nil {
		t.Fatalf("encode import: %v", err)
	}
	imported := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules/import?strategy=skip", target.ID),
		strings.NewReader(string(body)),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(imported, request)
	if imported.Code != http.StatusOK || !strings.Contains(imported.Body.String(), `"created":1`) {
		t.Fatalf("import status = %d, body=%s", imported.Code, imported.Body.String())
	}

	skipped := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules/import", target.ID),
		strings.NewReader(string(body)),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(skipped, request)
	if skipped.Code != http.StatusOK || !strings.Contains(skipped.Body.String(), `"skipped":1`) {
		t.Fatalf("skip status = %d, body=%s", skipped.Code, skipped.Body.String())
	}

	replacedBody := `{"kind":"` + bundle.Kind + `","version":1,"strategy":"replace","rules":[{"name":"capacity","enabled":false,"priority":99,"pattern":"overloaded","target":"raw_body","models":[],"protocols":[]}]}`
	replaced := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules/import", target.ID),
		strings.NewReader(replacedBody),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(replaced, request)
	if replaced.Code != http.StatusOK || !strings.Contains(replaced.Body.String(), `"replaced":1`) {
		t.Fatalf("replace status = %d, body=%s", replaced.Code, replaced.Body.String())
	}

	invalidBody := `{"kind":"` + bundle.Kind + `","version":1,"rules":[{"name":"new-valid","pattern":"ok","target":"assistant_text"},{"name":"new-invalid","pattern":"[","target":"assistant_text"}]}`
	invalid := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/response-rules/import", target.ID),
		strings.NewReader(invalidBody),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(invalid, request)
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("invalid import status = %d, body=%s", invalid.Code, invalid.Body.String())
	}
	list, err := rules.ListByGroupID(target.ID)
	if err != nil {
		t.Fatalf("list target rules: %v", err)
	}
	for _, item := range list {
		if item.Name == "new-valid" {
			t.Fatal("invalid import partially committed a valid rule")
		}
	}
}
