package storage

import "testing"

func TestNormalizeZeroUsageRuleUsesFixedCondition(t *testing.T) {
	for _, pattern := range []string{"", "[", "anything"} {
		rule := GatewayResponseRule{
			GatewayGroupID: 1, Name: "zero", Enabled: true,
			Target: " ZERO_USAGE ", Pattern: pattern,
			ModelsJSON: `["model-main"]`, ProtocolsJSON: `["OpenAI_Responses"]`,
		}
		if err := normalizeGatewayResponseRule(&rule); err != nil {
			t.Fatal(err)
		}
		if rule.Target != GatewayResponseRuleTargetZeroUsage || rule.Pattern != GatewayResponseRuleZeroUsagePattern || rule.ProtocolsJSON != `["openai_responses"]` {
			t.Fatalf("normalized rule=%+v", rule)
		}
	}
	for _, target := range []string{GatewayResponseRuleTargetAssistantText, GatewayResponseRuleTargetResponseModel} {
		rule := GatewayResponseRule{GatewayGroupID: 1, Name: "regex", Target: target}
		if err := normalizeGatewayResponseRule(&rule); err == nil {
			t.Fatalf("target %q accepted an empty regex", target)
		}
	}
}

func TestZeroUsageRuleStorageAndPortableBundle(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	source := &GatewayGroup{Name: "zero-source", Status: GatewayGroupStatusActive}
	target := &GatewayGroup{Name: "zero-target", Status: GatewayGroupStatusActive}
	for _, group := range []*GatewayGroup{source, target} {
		if err := groups.Create(group); err != nil {
			t.Fatal(err)
		}
	}
	rules := NewGatewayResponseRules(db)
	rule := &GatewayResponseRule{GatewayGroupID: source.ID, Name: "zero", Enabled: true, Target: GatewayResponseRuleTargetZeroUsage}
	if err := rules.Create(rule); err != nil {
		t.Fatal(err)
	}
	rule.Pattern = ""
	rule.ModelsJSON = `["model-main"]`
	if err := rules.Update(rule); err != nil {
		t.Fatal(err)
	}
	bundle, err := rules.Export(source.ID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := rules.Import(target.ID, bundle, GatewayResponseRuleImportSkip)
	if err != nil {
		t.Fatal(err)
	}
	if result.Created != 1 || len(result.Items) != 1 {
		t.Fatalf("import result=%+v", result)
	}
	got := result.Items[0]
	if got.GatewayGroupID != target.ID || got.Target != GatewayResponseRuleTargetZeroUsage || got.Pattern != GatewayResponseRuleZeroUsagePattern || got.ModelsJSON != rule.ModelsJSON {
		t.Fatalf("imported rule=%+v", got)
	}
}
