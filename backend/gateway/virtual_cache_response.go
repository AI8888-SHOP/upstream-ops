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
	if len(bytes.TrimSpace(body)) == 0 {
		return body, false
	}
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return body, false
	}
	changed := rewriteVirtualCacheValue(payload, protocol.NormalizeKind(kind))
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
	changed := false
	switch current := value.(type) {
	case map[string]any:
		if usage, ok := current["usage"].(map[string]any); ok {
			changed = rewriteVirtualCacheUsage(usage, kind) || changed
		}
		for key, child := range current {
			if key == "usage" {
				continue
			}
			changed = rewriteVirtualCacheValue(child, kind) || changed
		}
	case []any:
		for _, child := range current {
			changed = rewriteVirtualCacheValue(child, kind) || changed
		}
	}
	return changed
}

func rewriteVirtualCacheUsage(usage map[string]any, kind protocol.Kind) bool {
	if usage == nil {
		return false
	}
	switch protocol.NormalizeKind(kind) {
	case protocol.KindAnthropic:
		fresh := max0(mapInt(usage, "input_tokens"))
		if fresh <= 0 {
			return false
		}
		usage["input_tokens"] = 0
		usage["cache_read_input_tokens"] = max0(mapInt(usage, "cache_read_input_tokens")) + fresh
		return true
	case protocol.KindOpenAIResponses:
		return rewriteOpenAIVirtualCacheUsage(usage)
	default:
		return rewriteOpenAIVirtualCacheUsage(usage)
	}
}

func rewriteOpenAIVirtualCacheUsage(usage map[string]any) bool {
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
	fresh := totalInput - cacheRead - cacheCreation
	if fresh <= 0 {
		return false
	}
	virtualCacheRead := cacheRead + fresh
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

// virtualCacheSSETransformer accepts arbitrary write boundaries and rewrites
// complete SSE data events. An incomplete final event is flushed unchanged or
// rewritten when Finish is called.
type virtualCacheSSETransformer struct {
	kind    protocol.Kind
	pending []byte
	applied bool
}

func newVirtualCacheSSETransformer(kind protocol.Kind) *virtualCacheSSETransformer {
	return &virtualCacheSSETransformer{kind: protocol.NormalizeKind(kind)}
}

func (t *virtualCacheSSETransformer) Transform(payload []byte, final bool) []byte {
	if t == nil {
		return payload
	}
	t.pending = append(t.pending, payload...)
	var out bytes.Buffer
	for {
		end, separator := nextSSEEventBoundary(t.pending)
		if end < 0 {
			break
		}
		event := append([]byte(nil), t.pending[:end]...)
		t.pending = t.pending[end:]
		rewritten := rewriteVirtualCacheSSEEvent(event, separator, t.kind)
		if !bytes.Equal(rewritten, event) {
			t.applied = true
		}
		out.Write(rewritten)
	}
	if final && len(t.pending) > 0 {
		rewritten := rewriteVirtualCacheSSEEvent(t.pending, nil, t.kind)
		if !bytes.Equal(rewritten, t.pending) {
			t.applied = true
		}
		out.Write(rewritten)
		t.pending = nil
	}
	return out.Bytes()
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
	rewritten, changed := rewriteVirtualCacheResponse([]byte(data), kind)
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
