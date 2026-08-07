package gateway

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

// rewriteVirtualCacheResponse moves only fresh input into the cache-read
// bucket exposed to the downstream caller. OpenAI total input fields include
// cache tokens, while Anthropic input_tokens is already the fresh-input bucket.
func rewriteVirtualCacheResponse(body []byte, kind protocol.Kind) ([]byte, bool) {
	return rewriteVirtualCacheResponsePercent(body, kind, 100)
}

// rewriteVirtualCacheResponsePercent moves a percentage of fresh input into
// the cache-read bucket exposed to the downstream caller.
func rewriteVirtualCacheResponsePercent(body []byte, kind protocol.Kind, percent int) ([]byte, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return body, false
	}
	percent = normalizeVirtualCachePercent(percent)
	if percent <= 0 {
		return body, false
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, false
	}
	changed := rewriteVirtualCacheValuePercent(payload, protocol.NormalizeKind(kind), percent)
	if !changed {
		return body, false
	}
	out, err := json.Marshal(payload)
	if err != nil {
		return body, false
	}
	return out, true
}

func rewriteVirtualCacheValue(value any, kind protocol.Kind) bool {
	return rewriteVirtualCacheValuePercent(value, kind, 100)
}

func rewriteVirtualCacheValuePercent(value any, kind protocol.Kind, percent int) bool {
	changed := false
	switch current := value.(type) {
	case map[string]any:
		if usage, ok := current["usage"].(map[string]any); ok {
			changed = rewriteVirtualCacheUsagePercent(usage, kind, percent) || changed
		}
		for key, child := range current {
			if key == "usage" {
				continue
			}
			changed = rewriteVirtualCacheValuePercent(child, kind, percent) || changed
		}
	case []any:
		for _, child := range current {
			changed = rewriteVirtualCacheValuePercent(child, kind, percent) || changed
		}
	}
	return changed
}

func rewriteVirtualCacheUsage(usage map[string]any, kind protocol.Kind) bool {
	return rewriteVirtualCacheUsagePercent(usage, kind, 100)
}

func rewriteVirtualCacheUsagePercent(usage map[string]any, kind protocol.Kind, percent int) bool {
	if usage == nil {
		return false
	}
	percent = normalizeVirtualCachePercent(percent)
	if percent <= 0 {
		return false
	}
	switch protocol.NormalizeKind(kind) {
	case protocol.KindAnthropic:
		fresh := max0(mapInt(usage, "input_tokens"))
		if fresh <= 0 {
			return false
		}
		virtual := virtualCacheTokenPercent(fresh, percent)
		if virtual <= 0 {
			return false
		}
		usage["input_tokens"] = fresh - virtual
		usage["cache_read_input_tokens"] = max0(mapInt(usage, "cache_read_input_tokens")) + virtual
		return true
	case protocol.KindOpenAIResponses:
		return rewriteOpenAIVirtualCacheUsagePercent(usage, percent)
	default:
		return rewriteOpenAIVirtualCacheUsagePercent(usage, percent)
	}
}

func rewriteOpenAIVirtualCacheUsage(usage map[string]any) bool {
	return rewriteOpenAIVirtualCacheUsagePercent(usage, 100)
}

func rewriteOpenAIVirtualCacheUsagePercent(usage map[string]any, percent int) bool {
	// Match Sub2API's compatibility parser exactly: input_tokens wins when it
	// is positive, otherwise prompt_tokens is the fallback. Some OpenAI-compatible
	// providers return the opposite alias for the requested endpoint.
	totalInput := max0(mapInt(usage, "input_tokens"))
	inputKey := "input_tokens"
	if totalInput <= 0 {
		totalInput = max0(mapInt(usage, "prompt_tokens"))
		inputKey = "prompt_tokens"
	}
	if totalInput <= 0 {
		return false
	}
	cacheRead := openAICacheReadTokensFromUsage(usage)
	cacheCreation := openAICacheCreationTokensFromUsage(usage)
	if cacheCreation > 0 {
		// Creation tokens are never eligible for virtual read. Synchronize the
		// first nested alias so Sub2API cannot hide this bucket behind a stale 0.
		syncOpenAICacheCreationDetails(usage, cacheCreation)
	}
	fresh := totalInput - cacheRead - cacheCreation
	if fresh <= 0 {
		return false
	}
	virtual := virtualCacheTokenPercent(fresh, percent)
	if virtual <= 0 {
		return false
	}
	virtualCacheRead := cacheRead + virtual
	// Keep the canonical top-level aliases coherent with the nested details.
	// Sub2API currently prefers *_tokens_details, while other OpenAI-compatible
	// clients read one of these top-level fields instead.
	usage["cache_read_input_tokens"] = virtualCacheRead
	for _, key := range []string{
		"cache_read_tokens", "cached_tokens", "prompt_cache_hit_tokens", "cache_hit_tokens",
	} {
		if _, exists := usage[key]; exists {
			usage[key] = virtualCacheRead
		}
	}
	updatedDetails := false
	// Sub2API checks input_tokens_details before prompt_tokens_details. Update
	// every existing details object so a stale compatibility alias cannot mask
	// the rewritten value. If neither exists, create the object corresponding
	// to the total-input field that was actually used.
	for _, detailsKey := range []string{"input_tokens_details", "prompt_tokens_details"} {
		details, _ := usage[detailsKey].(map[string]any)
		if details == nil {
			continue
		}
		details["cached_tokens"] = virtualCacheRead
		updatedDetails = true
	}
	if !updatedDetails {
		detailsKey := "prompt_tokens_details"
		if inputKey == "input_tokens" {
			detailsKey = "input_tokens_details"
		}
		usage[detailsKey] = map[string]any{"cached_tokens": virtualCacheRead}
	}
	return true
}

func normalizeVirtualCachePercent(percent int) int {
	if percent < 0 {
		return 0
	}
	if percent > 100 {
		return 100
	}
	return percent
}

func virtualCacheTokenPercent(fresh, percent int) int {
	if fresh <= 0 || percent <= 0 {
		return 0
	}
	percent = normalizeVirtualCachePercent(percent)
	// Token counts are normally small, but avoid an integer overflow if a
	// malformed upstream usage value is received.
	if percent > 0 && fresh > int(^uint(0)>>1)/percent {
		return fresh
	}
	return fresh * percent / 100
}

// Sub2API treats the first present nested creation alias as authoritative,
// including an explicit zero. If a provider also supplies a positive top-level
// compatibility field, update that first nested alias so the downstream parser
// does not misclassify the creation bucket as fresh input.
func syncOpenAICacheCreationDetails(usage map[string]any, cacheCreation int) {
	for _, nested := range [][2]string{
		{"input_tokens_details", "cache_write_tokens"},
		{"prompt_tokens_details", "cache_write_tokens"},
		{"input_tokens_details", "cache_creation_tokens"},
		{"prompt_tokens_details", "cache_creation_tokens"},
	} {
		details, _ := usage[nested[0]].(map[string]any)
		if _, exists := details[nested[1]]; exists {
			details[nested[1]] = cacheCreation
			return
		}
	}
	// Sub2API does not currently read the duration-specific 5m/1h aliases.
	// Always expose a canonical flat total when no nested creation field exists.
	usage["cache_creation_input_tokens"] = cacheCreation
}

// virtualCacheSSETransformer accepts arbitrary write boundaries and rewrites
// complete SSE data events. An incomplete final event is flushed unchanged or
// rewritten when Finish is called.
type virtualCacheSSETransformer struct {
	kind    protocol.Kind
	percent int
	pending []byte
	applied bool
}

func newVirtualCacheSSETransformer(kind protocol.Kind) *virtualCacheSSETransformer {
	return newVirtualCacheSSETransformerPercent(kind, 100)
}

func newVirtualCacheSSETransformerPercent(kind protocol.Kind, percent int) *virtualCacheSSETransformer {
	return &virtualCacheSSETransformer{
		kind:    protocol.NormalizeKind(kind),
		percent: normalizeVirtualCachePercent(percent),
	}
}

func (t *virtualCacheSSETransformer) Transform(payload []byte, final bool) []byte {
	if t == nil {
		return payload
	}
	// Most stream chunks are complete content events and contain no usage
	// fields. When no prior partial event is pending, return those bytes
	// untouched; this avoids both the pending-buffer copy and bytes.Buffer
	// allocation on the hottest write path. A possible usage marker is still
	// buffered so a marker split across network writes cannot be missed.
	if len(t.pending) == 0 && len(payload) > 0 && !mayContainUsageFieldsBytes(payload, protocolKind(t.kind)) {
		if final || lastSSEEventEnd(payload) == len(payload) {
			return payload
		}
		if end := lastSSEEventEnd(payload); end > 0 {
			t.pending = append(t.pending[:0], payload[end:]...)
			return payload[:end]
		}
		if final {
			return payload
		}
		t.pending = append(t.pending[:0], payload...)
		return nil
	}
	t.pending = append(t.pending, payload...)
	var out bytes.Buffer
	for {
		end, separator := nextSSEEventBoundary(t.pending)
		if end < 0 {
			break
		}
		event := t.pending[:end]
		rewritten := rewriteVirtualCacheSSEEventPercent(event, separator, t.kind, t.percent)
		if !bytes.Equal(rewritten, event) {
			t.applied = true
		}
		out.Write(rewritten)
		t.pending = t.pending[end:]
	}
	if final && len(t.pending) > 0 {
		rewritten := rewriteVirtualCacheSSEEventPercent(t.pending, nil, t.kind, t.percent)
		if !bytes.Equal(rewritten, t.pending) {
			t.applied = true
		}
		out.Write(rewritten)
		t.pending = nil
	}
	return out.Bytes()
}

func lastSSEEventEnd(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}
	lf := bytes.LastIndex(payload, []byte("\n\n"))
	crlf := bytes.LastIndex(payload, []byte("\r\n\r\n"))
	end := 0
	if lf >= 0 {
		end = lf + 2
	}
	if crlf >= 0 && crlf+4 > end {
		end = crlf + 4
	}
	return end
}

func (t *virtualCacheSSETransformer) Applied() bool {
	return t != nil && t.applied
}

func nextSSEEventBoundary(payload []byte) (int, []byte) {
	lf := bytes.Index(payload, []byte("\n\n"))
	crlf := bytes.Index(payload, []byte("\r\n\r\n"))
	switch {
	case lf < 0 && crlf < 0:
		return -1, nil
	case crlf >= 0 && (lf < 0 || crlf < lf):
		return crlf + 4, []byte("\r\n\r\n")
	default:
		return lf + 2, []byte("\n\n")
	}
}

func rewriteVirtualCacheSSEEvent(event, separator []byte, kind protocol.Kind) []byte {
	return rewriteVirtualCacheSSEEventPercent(event, separator, kind, 100)
}

func rewriteVirtualCacheSSEEventPercent(event, separator []byte, kind protocol.Kind, percent int) []byte {
	body := event
	if len(separator) > 0 && len(body) >= len(separator) {
		body = body[:len(body)-len(separator)]
	}
	lineEnding := "\n"
	if bytes.Contains(body, []byte("\r\n")) || bytes.Equal(separator, []byte("\r\n\r\n")) {
		lineEnding = "\r\n"
	}
	// Most events are content deltas. Filter before splitting data lines so the
	// virtual-cache writer adds only a cheap marker scan to those hot writes.
	if !mayContainUsageFieldsBytes(body, protocolKind(kind)) {
		return event
	}
	rawBody := string(body)
	normalized := strings.ReplaceAll(rawBody, "\r\n", "\n")
	lines := strings.Split(normalized, "\n")
	dataIndexes := make([]int, 0, 1)
	dataParts := make([]string, 0, 1)
	for index, line := range lines {
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		dataIndexes = append(dataIndexes, index)
		part := strings.TrimPrefix(line, "data:")
		part = strings.TrimPrefix(part, " ")
		dataParts = append(dataParts, part)
	}
	if len(dataIndexes) == 0 {
		return event
	}
	data := strings.Join(dataParts, "\n")
	if strings.TrimSpace(data) == "[DONE]" {
		return event
	}
	rewritten, changed := rewriteVirtualCacheResponsePercent([]byte(data), kind, percent)
	if !changed {
		return event
	}
	firstData := dataIndexes[0]
	lines[firstData] = "data: " + string(rewritten)
	for index := len(dataIndexes) - 1; index > 0; index-- {
		lineIndex := dataIndexes[index]
		lines = append(lines[:lineIndex], lines[lineIndex+1:]...)
	}
	result := []byte(strings.Join(lines, lineEnding))
	return append(result, separator...)
}
