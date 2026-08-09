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
	rewritten, changed := appendVirtualCacheResponsePercent(nil, nil, body, kind, percent, true, 0)
	if !changed {
		return body, false
	}
	return rewritten, true
}

func appendVirtualCacheResponsePercent(dst, prefix, body []byte, kind protocol.Kind, percent int, validateJSON bool, extraCapacity int) ([]byte, bool) {
	if len(bytes.TrimSpace(body)) == 0 {
		return dst, false
	}
	percent = normalizeVirtualCachePercent(percent)
	if percent <= 0 {
		return dst, false
	}
	if !mayContainVirtualCacheUsageObject(body) || (validateJSON && !json.Valid(body)) {
		return dst, false
	}
	return appendVirtualCacheUsageObjects(dst, prefix, body, protocol.NormalizeKind(kind), percent, extraCapacity)
}

type virtualCacheJSONSpan struct {
	start int
	end   int
}

var (
	virtualCacheUsageKey       = []byte("usage")
	virtualCacheQuotedUsageKey = []byte(`"usage"`)
	jsonUnicodeEscape          = []byte(`\u`)
)

// mayContainVirtualCacheUsageObject is narrower than the accounting usage
// prefilter. Virtual-cache rewriting can only change an object-valued "usage"
// member, so token field names in ordinary model output are irrelevant.
func mayContainVirtualCacheUsageObject(data []byte) bool {
	searchFrom := 0
	for searchFrom < len(data) {
		relative := bytes.Index(data[searchFrom:], virtualCacheQuotedUsageKey)
		if relative < 0 {
			break
		}
		pos := searchFrom + relative + len(virtualCacheQuotedUsageKey)
		skipJSONSpaceBytes(data, &pos)
		if pos < len(data) && data[pos] == ':' {
			pos++
			skipJSONSpaceBytes(data, &pos)
			if pos < len(data) && data[pos] == '{' {
				return true
			}
		}
		searchFrom += relative + len(virtualCacheQuotedUsageKey)
	}
	// Preserve support for escaped JSON keys such as us\u0061ge. The escaped-key
	// scanner below remains authoritative for this uncommon form.
	return bytes.Contains(data, jsonUnicodeEscape)
}

func appendVirtualCacheUsageObjects(dst, prefix, body []byte, kind protocol.Kind, percent, extraCapacity int) ([]byte, bool) {
	spans := findVirtualCacheUsageObjectSpans(body)
	if len(spans) == 0 {
		return dst, false
	}
	out := dst
	changed := false
	cursor := 0
	for _, span := range spans {
		var usage map[string]any
		if err := json.Unmarshal(body[span.start:span.end], &usage); err != nil {
			continue
		}
		if !rewriteVirtualCacheUsagePercent(usage, kind, percent) {
			continue
		}
		rewritten, err := json.Marshal(usage)
		if err != nil {
			continue
		}
		if !changed {
			if out == nil {
				out = make([]byte, 0, len(prefix)+len(body)+len(rewritten)-(span.end-span.start)+extraCapacity)
			}
			out = append(out, prefix...)
		}
		out = append(out, body[cursor:span.start]...)
		out = append(out, rewritten...)
		cursor = span.end
		changed = true
	}
	if !changed {
		return dst, false
	}
	out = append(out, body[cursor:]...)
	return out, true
}

// findVirtualCacheUsageObjectSpans walks JSON strings without materializing
// the surrounding response. It skips each matched usage object, matching the
// old recursive implementation's behavior of not descending into usage data.
func findVirtualCacheUsageObjectSpans(body []byte) []virtualCacheJSONSpan {
	literal := findLiteralVirtualCacheUsageObjectSpans(body)
	if !bytes.Contains(body, jsonUnicodeEscape) {
		return literal
	}
	escaped := findEscapedVirtualCacheUsageObjectSpans(body)
	return mergeVirtualCacheJSONSpans(literal, escaped)
}

func findLiteralVirtualCacheUsageObjectSpans(body []byte) []virtualCacheJSONSpan {
	var spans []virtualCacheJSONSpan
	for searchFrom := 0; searchFrom < len(body); {
		relative := bytes.Index(body[searchFrom:], virtualCacheQuotedUsageKey)
		if relative < 0 {
			break
		}
		keyStart := searchFrom + relative
		searchFrom = keyStart + len(virtualCacheQuotedUsageKey)
		if jsonQuoteIsEscaped(body, keyStart) {
			continue
		}
		valueStart := searchFrom
		skipJSONSpaceBytes(body, &valueStart)
		if valueStart >= len(body) || body[valueStart] != ':' {
			continue
		}
		valueStart++
		skipJSONSpaceBytes(body, &valueStart)
		if valueStart >= len(body) || body[valueStart] != '{' {
			continue
		}
		valueEnd := valueStart
		if !skipJSONValueBytes(body, &valueEnd) {
			return nil
		}
		spans = append(spans, virtualCacheJSONSpan{start: valueStart, end: valueEnd})
		searchFrom = valueEnd
	}
	return spans
}

func findEscapedVirtualCacheUsageObjectSpans(body []byte) []virtualCacheJSONSpan {
	var spans []virtualCacheJSONSpan
	for searchFrom := 0; searchFrom < len(body); {
		relative := bytes.IndexByte(body[searchFrom:], ':')
		if relative < 0 {
			break
		}
		colon := searchFrom + relative
		searchFrom = colon + 1
		keyEnd := trimJSONSpaceEnd(body, colon)
		if keyEnd == 0 || body[keyEnd-1] != '"' || jsonQuoteIsEscaped(body, keyEnd-1) {
			continue
		}
		closingQuote := keyEnd - 1
		openingQuote := previousUnescapedJSONQuote(body, closingQuote)
		if openingQuote < 0 {
			continue
		}
		rawKey := body[openingQuote+1 : closingQuote]
		if !bytes.Contains(rawKey, jsonUnicodeEscape) || !virtualCacheJSONKeyIsUsage(body[openingQuote:keyEnd], rawKey) {
			continue
		}
		valueStart := colon + 1
		skipJSONSpaceBytes(body, &valueStart)
		if valueStart >= len(body) || body[valueStart] != '{' {
			continue
		}
		valueEnd := valueStart
		if !skipJSONValueBytes(body, &valueEnd) {
			return nil
		}
		spans = append(spans, virtualCacheJSONSpan{start: valueStart, end: valueEnd})
		searchFrom = valueEnd
	}
	return spans
}

func trimJSONSpaceEnd(data []byte, end int) int {
	for end > 0 {
		switch data[end-1] {
		case ' ', '\t', '\r', '\n':
			end--
		default:
			return end
		}
	}
	return end
}

func previousUnescapedJSONQuote(data []byte, before int) int {
	for before > 0 {
		quote := bytes.LastIndexByte(data[:before], '"')
		if quote < 0 {
			return -1
		}
		if !jsonQuoteIsEscaped(data, quote) {
			return quote
		}
		before = quote
	}
	return -1
}

func mergeVirtualCacheJSONSpans(left, right []virtualCacheJSONSpan) []virtualCacheJSONSpan {
	if len(left) == 0 {
		return right
	}
	if len(right) == 0 {
		return left
	}
	merged := make([]virtualCacheJSONSpan, 0, len(left)+len(right))
	for len(left) > 0 || len(right) > 0 {
		var next virtualCacheJSONSpan
		if len(right) == 0 || len(left) > 0 && left[0].start <= right[0].start {
			next, left = left[0], left[1:]
		} else {
			next, right = right[0], right[1:]
		}
		if len(merged) > 0 && next.start < merged[len(merged)-1].end {
			continue
		}
		merged = append(merged, next)
	}
	return merged
}

func jsonQuoteIsEscaped(data []byte, quote int) bool {
	backslashes := 0
	for index := quote - 1; index >= 0 && data[index] == '\\'; index-- {
		backslashes++
	}
	return backslashes%2 != 0
}

func virtualCacheJSONKeyIsUsage(quoted, raw []byte) bool {
	if bytes.Equal(raw, virtualCacheUsageKey) {
		return true
	}
	if bytes.IndexByte(raw, '\\') < 0 {
		return false
	}
	var key string
	return json.Unmarshal(quoted, &key) == nil && key == "usage"
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
	// Runtime writes normally contain complete SSE events. Transform those
	// directly so a large terminal Responses frame is never copied into pending
	// and then copied again through bytes.Buffer.
	if len(t.pending) == 0 && len(payload) > 0 {
		end := lastSSEEventEnd(payload)
		if end == len(payload) {
			return t.transformCompleteEvents(payload)
		}
		if !mayContainVirtualCacheUsageObject(payload) {
			if final {
				return payload
			}
			if end > 0 {
				t.pending = append(t.pending[:0], payload[end:]...)
				return payload[:end]
			}
			t.pending = append(t.pending[:0], payload...)
			return nil
		}
	}
	t.pending = append(t.pending, payload...)
	var out bytes.Buffer
	for {
		end, separator := nextSSEEventBoundary(t.pending)
		if end < 0 {
			break
		}
		event := t.pending[:end]
		rewritten, changed := rewriteVirtualCacheSSEEventPercent(event, separator, t.kind, t.percent)
		if changed {
			t.applied = true
		}
		out.Write(rewritten)
		t.pending = t.pending[end:]
	}
	if final && len(t.pending) > 0 {
		rewritten, changed := rewriteVirtualCacheSSEEventPercent(t.pending, nil, t.kind, t.percent)
		if changed {
			t.applied = true
		}
		out.Write(rewritten)
		t.pending = nil
	}
	return out.Bytes()
}

func (t *virtualCacheSSETransformer) transformCompleteEvents(payload []byte) []byte {
	if !mayContainVirtualCacheUsageObject(payload) {
		return payload
	}
	var out []byte
	for cursor := 0; cursor < len(payload); {
		end, separator := nextSSEEventBoundary(payload[cursor:])
		if end < 0 {
			if out == nil {
				return payload
			}
			return append(out, payload[cursor:]...)
		}
		event := payload[cursor : cursor+end]
		rewritten, changed := rewriteVirtualCacheSSEEventPercent(event, separator, t.kind, t.percent)
		if changed {
			t.applied = true
			if out == nil && cursor == 0 && end == len(payload) {
				return rewritten
			}
			if out == nil {
				out = make([]byte, 0, len(payload))
				out = append(out, payload[:cursor]...)
			}
			out = append(out, rewritten...)
		} else if out != nil {
			out = append(out, event...)
		}
		cursor += end
	}
	if out == nil {
		return payload
	}
	return out
}

var (
	sseLFSeparator   = []byte("\n\n")
	sseCRLFSeparator = []byte("\r\n\r\n")
	sseDataPrefix    = []byte("data:")
)

func lastSSEEventEnd(payload []byte) int {
	if len(payload) == 0 {
		return 0
	}
	if bytes.HasSuffix(payload, sseCRLFSeparator) || bytes.HasSuffix(payload, sseLFSeparator) {
		return len(payload)
	}
	lf := bytes.LastIndex(payload, sseLFSeparator)
	crlf := bytes.LastIndex(payload, sseCRLFSeparator)
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
	lf := bytes.Index(payload, sseLFSeparator)
	crlf := bytes.Index(payload, sseCRLFSeparator)
	switch {
	case lf < 0 && crlf < 0:
		return -1, nil
	case crlf >= 0 && (lf < 0 || crlf < lf):
		return crlf + len(sseCRLFSeparator), sseCRLFSeparator
	default:
		return lf + len(sseLFSeparator), sseLFSeparator
	}
}

func rewriteVirtualCacheSSEEvent(event, separator []byte, kind protocol.Kind) []byte {
	rewritten, _ := rewriteVirtualCacheSSEEventPercent(event, separator, kind, 100)
	return rewritten
}

func rewriteVirtualCacheSSEEventPercent(event, separator []byte, kind protocol.Kind, percent int) ([]byte, bool) {
	body := event
	if len(separator) > 0 && len(body) >= len(separator) {
		body = body[:len(body)-len(separator)]
	}
	// Most events are content deltas. Filter before splitting data lines so the
	// virtual-cache writer adds only one targeted marker scan to hot writes.
	if !mayContainVirtualCacheUsageObject(body) {
		return event, false
	}
	if dataStart, dataEnd, ok := singleSSEDataPayload(body); ok {
		data := body[dataStart:dataEnd]
		if bytes.Equal(bytes.TrimSpace(data), []byte("[DONE]")) {
			return event, false
		}
		result, changed := appendVirtualCacheResponsePercent(nil, event[:dataStart], data, kind, percent, false, len(event)-dataEnd)
		if !changed {
			return event, false
		}
		result = append(result, event[dataEnd:]...)
		return result, true
	}

	// Multiline data events are legal but uncommon. Retain the compatibility
	// path that joins their data fields before rewriting.
	lineEnding := "\n"
	if bytes.Contains(body, []byte("\r\n")) || bytes.Equal(separator, sseCRLFSeparator) {
		lineEnding = "\r\n"
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
		return event, false
	}
	data := strings.Join(dataParts, "\n")
	if strings.TrimSpace(data) == "[DONE]" {
		return event, false
	}
	rewritten, changed := rewriteVirtualCacheResponsePercent([]byte(data), kind, percent)
	if !changed {
		return event, false
	}
	firstData := dataIndexes[0]
	lines[firstData] = "data: " + string(rewritten)
	for index := len(dataIndexes) - 1; index > 0; index-- {
		lineIndex := dataIndexes[index]
		lines = append(lines[:lineIndex], lines[lineIndex+1:]...)
	}
	result := []byte(strings.Join(lines, lineEnding))
	return append(result, separator...), true
}

// singleSSEDataPayload returns the payload span for the common one-data-line
// event. Multiple data lines fall back to the compatibility join above.
func singleSSEDataPayload(body []byte) (start, end int, ok bool) {
	start = -1
	for lineStart := 0; lineStart <= len(body); {
		relativeEnd := bytes.IndexByte(body[lineStart:], '\n')
		lineEnd := len(body)
		nextLine := len(body) + 1
		if relativeEnd >= 0 {
			lineEnd = lineStart + relativeEnd
			nextLine = lineEnd + 1
		}
		contentEnd := lineEnd
		if contentEnd > lineStart && body[contentEnd-1] == '\r' {
			contentEnd--
		}
		line := body[lineStart:contentEnd]
		if bytes.HasPrefix(line, sseDataPrefix) {
			if start >= 0 {
				return 0, 0, false
			}
			start = lineStart + len(sseDataPrefix)
			if start < contentEnd && body[start] == ' ' {
				start++
			}
			end = contentEnd
		}
		if relativeEnd < 0 {
			break
		}
		lineStart = nextLine
	}
	return start, end, start >= 0
}
