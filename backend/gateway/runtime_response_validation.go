package gateway

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

func (rt *Runtime) responseValidatorForGroup(group *storage.GatewayGroup) (*responseValidator, error) {
	if group == nil || !group.ResponseValidationEnabled {
		return newResponseValidator(responseValidationConfig{}, nil)
	}
	if cached := rt.cachedResponseValidator(group); cached != nil {
		return cached, nil
	}
	if rt.ResponseRules == nil {
		return nil, fmt.Errorf("response validation repository is not configured")
	}
	rows, err := rt.ResponseRules.ListEnabledByGroupID(group.ID)
	if err != nil {
		return nil, fmt.Errorf("load response validation rules: %w", err)
	}
	rules := make([]responseRuleSpec, 0, len(rows))
	for _, row := range rows {
		models, err := decodeResponseRuleSelectors(row.ModelsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode response rule %q models: %w", row.Name, err)
		}
		protocols, err := decodeResponseRuleSelectors(row.ProtocolsJSON)
		if err != nil {
			return nil, fmt.Errorf("decode response rule %q protocols: %w", row.Name, err)
		}
		rules = append(rules, responseRuleSpec{
			ID: row.ID, Name: row.Name, Enabled: row.Enabled, Priority: row.Priority,
			Pattern: row.Pattern, Target: row.Target, Models: models, Protocols: protocols,
		})
	}
	validator, err := newResponseValidator(responseValidationConfigForGroup(group), rules)
	if err != nil {
		return nil, err
	}
	rt.cacheResponseValidator(group, validator)
	return validator, nil
}

func decodeResponseRuleSelectors(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return nil, nil
	}
	var result []string
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		return nil, err
	}
	return result, nil
}

func validationErrorInfo(result validationResult) usageErrorInfo {
	reason := "response validation rejected the upstream response"
	if strings.TrimSpace(result.RuleName) != "" {
		reason = fmt.Sprintf("response matched rejection rule %q", result.RuleName)
	}
	return usageErrorInfo{
		Type:    "validation",
		Summary: reason,
		Detail: fmt.Sprintf(
			"response validation rejection\nrule_id: %d\nrule: %s\ntarget: %s\npattern: %s\npost_commit: %v\n",
			result.RuleID, result.RuleName, result.Target, result.Pattern, result.PostCommit,
		),
	}
}
