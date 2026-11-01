package gateway

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/bejix/upstream-ops/backend/apiaudit"
)

// auditModelUsage retains raw upstream buckets. The transport normalizes them
// once, after the entire response has been consumed.
func auditModelUsage(payload []byte, kind string, previous apiaudit.UsageSnapshot) apiaudit.UsageSnapshot {
	var raw map[string]json.RawMessage
	if json.Unmarshal(payload, &raw) != nil {
		return previous
	}
	object := auditUsageObject(raw)
	if object == nil {
		return previous
	}
	usage := auditUsageNumbers(object)
	inputSeen := auditUsageFieldPresent(usage, "input_tokens", "prompt_tokens")
	outputSeen := auditUsageFieldPresent(usage, "output_tokens", "completion_tokens")
	previous.InputSeen = previous.InputSeen || inputSeen
	previous.OutputSeen = previous.OutputSeen || outputSeen

	// Reuse the same alias priorities, cache buckets, and image/reasoning
	// extraction as customer usage. Numeric presence stays separate so a
	// partial/total-only object never turns absent counters into known zero.
	next := parseUsageMapSub2API(usage)
	if kind == "anthropic" {
		// Anthropic exposes TTL buckets in a nested cache_creation object as
		// well as flat compatibility aliases. The shared parser accepts flat
		// aliases; retain the protocol-native breakdown before merging.
		if breakdown, ok := usage["cache_creation"].(map[string]any); ok {
			if next.CacheCreation5mTokens == 0 {
				next.CacheCreation5mTokens = firstPositiveInt(breakdown, "ephemeral_5m_input_tokens")
			}
			if next.CacheCreation1hTokens == 0 {
				next.CacheCreation1hTokens = firstPositiveInt(breakdown, "ephemeral_1h_input_tokens")
			}
			if next.CacheCreationTokens == 0 {
				next.CacheCreationTokens = next.CacheCreation5mTokens + next.CacheCreation1hTokens
			}
		}
	}
	old := UsageTokens{
		InputTokens: previous.InputTokens, OutputTokens: previous.OutputTokens,
		CacheReadTokens: previous.CacheReadTokens, CacheCreationTokens: previous.CacheCreationTokens,
		CacheCreation5mTokens: previous.CacheCreation5mTokens, CacheCreation1hTokens: previous.CacheCreation1hTokens,
		ImageOutputTokens: previous.ImageOutputTokens, ReasoningTokens: previous.ReasoningTokens,
	}
	cumulative := mergeUsagePreferNewer(old, next)
	// Complete OpenAI usage objects are authoritative snapshots, including an
	// explicit final zero. Anthropic message_delta commonly carries stale
	// input/cache zero aliases, so keep its positive start counters instead.
	if kind != "anthropic" && inputSeen && outputSeen {
		cumulative = next
	}
	previous.InputTokens = cumulative.InputTokens
	previous.OutputTokens = cumulative.OutputTokens
	previous.CacheReadTokens = cumulative.CacheReadTokens
	previous.CacheCreationTokens = cumulative.CacheCreationTokens
	previous.CacheCreation5mTokens = cumulative.CacheCreation5mTokens
	previous.CacheCreation1hTokens = cumulative.CacheCreation1hTokens
	previous.ImageOutputTokens = cumulative.ImageOutputTokens
	previous.ReasoningTokens = cumulative.ReasoningTokens
	previous.Known = previous.InputSeen && previous.OutputSeen
	return previous
}

func auditUsageObject(raw map[string]json.RawMessage) map[string]json.RawMessage {
	var usage map[string]json.RawMessage
	if json.Unmarshal(raw["usage"], &usage) == nil && usage != nil {
		return usage
	}
	for _, key := range []string{"response", "message", "data"} {
		var nested map[string]json.RawMessage
		if json.Unmarshal(raw[key], &nested) == nil && nested != nil {
			if json.Unmarshal(nested["usage"], &usage) == nil && usage != nil {
				return usage
			}
		}
	}
	// A few compatible providers return a bare usage object.
	for _, key := range []string{"input_tokens", "prompt_tokens", "output_tokens", "completion_tokens"} {
		if _, ok := raw[key]; ok {
			return raw
		}
	}
	return nil
}

// Decode only nonnegative integral counters accepted by the audit contract.
// In particular, null must not count as an explicit zero and fractional or
// overflowing input must not become a priceable truncated integer.
func auditUsageNumbers(raw map[string]json.RawMessage) map[string]any {
	out := make(map[string]any, len(raw))
	for key, value := range raw {
		var nested map[string]json.RawMessage
		if json.Unmarshal(value, &nested) == nil && nested != nil {
			out[key] = auditUsageNumbers(nested)
			continue
		}
		trimmed := bytes.TrimSpace(value)
		if bytes.Equal(trimmed, []byte("null")) {
			continue
		}
		var number json.Number
		if json.Unmarshal(trimmed, &number) == nil {
			integer, err := number.Int64()
			if err == nil && integer >= 0 && integer <= int64(^uint(0)>>1) {
				out[key] = int(integer)
				continue
			}
		}
		// Match the business parser's legacy numeric-string support, while
		// keeping explicit null/non-integral/overflowing values unknown.
		var text string
		if json.Unmarshal(trimmed, &text) == nil {
			integer, err := strconv.ParseInt(text, 10, 64)
			if err == nil && integer >= 0 && integer <= int64(^uint(0)>>1) {
				out[key] = int(integer)
			}
		}
	}
	return out
}

func auditUsageFieldPresent(usage map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, ok := usage[key].(int); ok {
			return true
		}
	}
	return false
}
