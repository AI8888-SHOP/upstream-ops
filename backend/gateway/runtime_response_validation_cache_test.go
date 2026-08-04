package gateway

import (
	"net/http"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestResponseValidatorCacheReusesCompiledRulesUntilInvalidated(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	rules := storage.NewGatewayResponseRules(db)
	svc := NewService(
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
	svc.SetResponseRules(rules)
	enabled := true
	group, err := svc.CreateGroup(CreateGroupInput{Name: "validator-cache", ResponseValidationEnabled: &enabled})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	rule := &storage.GatewayResponseRule{
		GatewayGroupID: group.ID,
		Name:           "first-pattern",
		Enabled:        true,
		Pattern:        "first",
		Target:         storage.GatewayResponseRuleTargetRawBody,
		ModelsJSON:     "[]",
		ProtocolsJSON:  "[]",
	}
	if err := rules.Create(rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}

	first, err := svc.Runtime.responseValidatorForGroup(group)
	if err != nil {
		t.Fatalf("load first validator: %v", err)
	}
	if result := first.Validate([]byte("first"), http.Header{}, "openai_chat", "gpt-test"); !result.IsRejected() {
		t.Fatalf("first pattern was not rejected: %+v", result)
	}

	stored, err := rules.FindByID(rule.ID)
	if err != nil {
		t.Fatalf("find rule: %v", err)
	}
	stored.Pattern = "second"
	if err := rules.Update(stored); err != nil {
		t.Fatalf("update rule directly: %v", err)
	}
	withoutInvalidation, err := svc.Runtime.responseValidatorForGroup(group)
	if err != nil {
		t.Fatalf("load cached validator: %v", err)
	}
	if withoutInvalidation != first {
		t.Fatal("validator was rebuilt before explicit invalidation")
	}
	if result := withoutInvalidation.Validate([]byte("second"), http.Header{}, "openai_chat", "gpt-test"); result.IsRejected() {
		t.Fatalf("direct rule update unexpectedly bypassed cache: %+v", result)
	}

	svc.InvalidateResponseValidator(group.ID)
	refreshed, err := svc.Runtime.responseValidatorForGroup(group)
	if err != nil {
		t.Fatalf("load refreshed validator: %v", err)
	}
	if refreshed == first {
		t.Fatal("validator was not rebuilt after invalidation")
	}
	if result := refreshed.Validate([]byte("second"), http.Header{}, "openai_chat", "gpt-test"); !result.IsRejected() {
		t.Fatalf("refreshed pattern was not rejected: %+v", result)
	}
}

func TestResponseValidatorCacheInvalidatesOnGroupPolicyUpdate(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	rules := storage.NewGatewayResponseRules(db)
	svc := NewService(groups, nil, nil, nil, nil, nil, nil, nil, nil)
	svc.SetResponseRules(rules)
	enabled := true
	group, err := svc.CreateGroup(CreateGroupInput{Name: "validator-policy-cache", ResponseValidationEnabled: &enabled})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := rules.Create(&storage.GatewayResponseRule{
		GatewayGroupID: group.ID,
		Name:           "policy-rule",
		Enabled:        true,
		Pattern:        "blocked",
		Target:         storage.GatewayResponseRuleTargetRawBody,
		ModelsJSON:     "[]",
		ProtocolsJSON:  "[]",
	}); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	first, err := svc.Runtime.responseValidatorForGroup(group)
	if err != nil {
		t.Fatalf("load first validator: %v", err)
	}
	bytes := group.ResponseValidationPrefixBytes + 1024
	updated, err := svc.UpdateGroup(group.ID, UpdateGroupInput{ResponseValidationPrefixBytes: &bytes})
	if err != nil {
		t.Fatalf("update group policy: %v", err)
	}
	second, err := svc.Runtime.responseValidatorForGroup(updated)
	if err != nil {
		t.Fatalf("load updated validator: %v", err)
	}
	if second == first {
		t.Fatal("group policy update did not invalidate validator")
	}
}
