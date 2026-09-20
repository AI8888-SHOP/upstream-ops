package gateway

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// Keep presence separately from value: missing usage is not an explicit zero.
// Observations are attempt-local and precede conversion, recovery and virtual
// cache rewriting. Positive usage is sticky across split/duplicated SSE frames.
type responseUsageObservation struct {
	inputSeen  bool
	outputSeen bool
	nonZero    bool
	invalid    bool
}

func (u responseUsageObservation) IsZero() bool {
	return u.inputSeen && u.outputSeen && !u.nonZero && !u.invalid
}

func (u *responseUsageObservation) ObserveStreamData(data string) {
	// Avoid allocating a byte copy for each ordinary text/tool delta, or after
	// positive usage has already ruled out this condition for the attempt.
	if u.nonZero || u.invalid || (!strings.Contains(data, `"usage"`) && !strings.Contains(data, `\u`)) {
		return
	}
	u.Observe([]byte(data))
}

func (u *responseUsageObservation) Observe(payload []byte) {
	if u.nonZero || u.invalid || (!bytes.Contains(payload, []byte(`"usage"`)) && !bytes.Contains(payload, []byte(`\u`))) || !json.Valid(payload) {
		return
	}
	observeObject := func(object []byte) {
		value, ok := partialJSONRootMember(object, "usage")
		if !ok {
			return
		}
		var usage map[string]json.RawMessage
		// The member helper returns the remaining object, so decode exactly one
		// value rather than unmarshalling that suffix as a whole document.
		if json.NewDecoder(bytes.NewReader(value)).Decode(&usage) == nil {
			u.observeFields(usage, false)
		}
	}
	observeObject(payload)
	for _, key := range []string{"response", "message", "data"} {
		if object, ok := partialJSONRootMember(payload, key); ok {
			observeObject(object)
		}
	}
}

func (u *responseUsageObservation) observeFields(usage map[string]json.RawMessage, details bool) {
	for key, raw := range usage {
		if !details {
			switch key {
			case "input_tokens_details", "prompt_tokens_details", "output_tokens_details", "completion_tokens_details", "cache_creation":
				var nested map[string]json.RawMessage
				if json.Unmarshal(raw, &nested) == nil {
					u.observeFields(nested, true)
				}
				continue
			}
		}
		if !strings.HasSuffix(key, "_tokens") && key != "tokens" {
			continue
		}
		var number json.Number
		if json.Unmarshal(raw, &number) != nil {
			u.invalid = true
			continue
		}
		value, err := number.Float64()
		if err != nil || value < 0 || math.IsInf(value, 0) || math.IsNaN(value) {
			u.invalid = true
			continue
		}
		u.nonZero = u.nonZero || value > 0
		if !details {
			switch key {
			case "input_tokens", "prompt_tokens":
				u.inputSeen = true
			case "output_tokens", "completion_tokens":
				u.outputSeen = true
			}
		}
	}
}

func (u *responseUsageObservation) ObserveBody(body []byte) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return
	}
	if trimmed[0] == '{' {
		u.Observe(trimmed)
		return
	}
	for _, payload := range sseDataPayloads(body) {
		u.Observe(payload)
	}
}

func (v *responseValidator) ValidateZeroUsage(protocolName, model string, usage responseUsageObservation) validationResult {
	if !v.Enabled() || !usage.IsZero() {
		return acceptedValidation()
	}
	for _, rule := range v.rules {
		if rule.Target == "zero_usage" && responseRuleApplies(rule, protocolName, model) {
			return validationResult{
				Decision: validationRejected, RuleID: rule.ID, RuleName: rule.Name,
				Target: rule.Target, Pattern: rule.Pattern, MatchedOn: "input_tokens=0;output_tokens=0",
			}
		}
	}
	return acceptedValidation()
}

func (v *responseValidator) ValidateBodyUsage(body []byte, protocolName, model string) validationResult {
	if !v.Enabled() || !v.needsTarget("zero_usage", protocolName, model) {
		return acceptedValidation()
	}
	var usage responseUsageObservation
	usage.ObserveBody(body)
	return v.ValidateZeroUsage(protocolName, model, usage)
}

func streamZeroUsageEnabled(c *gin.Context) bool {
	if c == nil {
		return false
	}
	gate, ok := c.Writer.(*streamPrefixGateWriter)
	if !ok {
		return false
	}
	// These configuration fields are immutable for the lifetime of the gate.
	s := gate.validator
	return s != nil && s.validator.StreamEnabled() && s.validator.needsTarget("zero_usage", s.protocolName, s.model)
}

func validateStreamZeroUsage(c *gin.Context, usage responseUsageObservation) error {
	if !usage.IsZero() {
		return nil
	}
	return validateStreamMetadata(c, func(s *streamResponseValidator) validationResult {
		return s.validator.ValidateZeroUsage(s.protocolName, s.model, usage)
	})
}

// Hold only recognized metadata until the first meaningful output. Unknown
// payloads are released conservatively; never buffer a whole answer awaiting
// final usage. This classifier runs only while a zero-usage rule holds a prefix.
func zeroUsageMetadataEvent(kind protocol.Kind, eventName, data string) bool {
	var payload map[string]json.RawMessage
	if json.Unmarshal([]byte(data), &payload) != nil || payload == nil {
		return false
	}
	if _, ok := payload["error"]; ok {
		return false
	}
	empty := func(raw json.RawMessage) bool {
		value := bytes.TrimSpace(raw)
		if len(value) >= 2 && (value[0] == '[' && value[len(value)-1] == ']' || value[0] == '{' && value[len(value)-1] == '}') {
			return len(bytes.TrimSpace(value[1:len(value)-1])) == 0
		}
		return len(value) == 0 || bytes.Equal(value, []byte("null")) || bytes.Equal(value, []byte(`""`))
	}
	metadataFields := func(raw json.RawMessage, ignored string) bool {
		if empty(raw) {
			return true
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return false
		}
		for key, value := range fields {
			if key != ignored && !empty(value) {
				return false
			}
		}
		return true
	}
	if kind == protocol.KindAnthropic {
		if eventName == "" {
			_ = json.Unmarshal(payload["type"], &eventName)
		}
		switch eventName {
		case "ping", "message_delta", "content_block_stop":
			return true
		case "message_start":
			var message struct {
				Content []json.RawMessage `json:"content"`
			}
			if json.Unmarshal(payload["message"], &message) != nil {
				return false
			}
			for _, block := range message.Content {
				if !metadataFields(block, "type") {
					return false
				}
			}
			return true
		case "content_block_start":
			return metadataFields(payload["content_block"], "type")
		case "content_block_delta":
			return metadataFields(payload["delta"], "type")
		default:
			return false
		}
	}
	if kind != protocol.KindOpenAIChat && kind != protocol.KindOpenAI {
		return false
	}
	for key := range payload {
		switch key {
		case "id", "object", "created", "model", "choices", "usage", "system_fingerprint", "service_tier":
		default:
			return false
		}
	}
	var choices []struct {
		Delta   json.RawMessage `json:"delta"`
		Message json.RawMessage `json:"message"`
		Text    json.RawMessage `json:"text"`
	}
	raw, exists := payload["choices"]
	if !exists {
		_, hasUsage := payload["usage"]
		return hasUsage
	}
	if json.Unmarshal(raw, &choices) != nil {
		return false
	}
	for _, choice := range choices {
		if !metadataFields(choice.Delta, "role") || !metadataFields(choice.Message, "role") || !empty(choice.Text) {
			return false
		}
	}
	return true
}
