package gateway

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

// 本文件 usage 解析对齐 sub2api：
//   - openAIUsageFromGJSON / openAICacheReadTokensFromUsage / openAICacheCreationTokensFromUsage
//     (tmp/sub2api/backend/internal/service/openai_gateway_response_handling.go)
//   - stream_options.include_usage 强制打开
//     (tmp/sub2api/backend/internal/service/openai_gateway_chat_completions_raw.go)
//   - SSE 按「空行分事件」拼 data: 行，再抽 usage
//     (extractCCStreamUsage：末帧 usage 覆盖)

// ParseOpenAIUsage 从 OpenAI 兼容 JSON（含 stream chunk）解析 usage。
func ParseOpenAIUsage(body []byte) UsageTokens {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return UsageTokens{}
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return UsageTokens{}
	}
	usageObj, _ := raw["usage"].(map[string]any)
	if usageObj == nil {
		if data, ok := raw["data"].(map[string]any); ok {
			usageObj, _ = data["usage"].(map[string]any)
		}
	}
	if usageObj == nil {
		if hasAnyTokenField(raw) {
			usageObj = raw
		} else {
			return UsageTokens{}
		}
	}
	return parseUsageMapSub2API(usageObj)
}

func hasAnyTokenField(m map[string]any) bool {
	for _, k := range []string{
		"prompt_tokens", "completion_tokens", "input_tokens", "output_tokens",
		"cache_read_input_tokens", "cached_tokens", "cache_read_tokens",
		"prompt_tokens_details", "input_tokens_details",
		"prompt_cache_hit_tokens",
	} {
		if _, ok := m[k]; ok {
			return true
		}
	}
	return false
}

// parseUsageMapSub2API 对齐 sub2api openAIUsageFromGJSON。
func parseUsageMapSub2API(u map[string]any) UsageTokens {
	out := UsageTokens{}

	// input: 优先 input_tokens，否则 prompt_tokens
	if v := mapInt(u, "input_tokens"); v != 0 {
		out.InputTokens = max0(v)
	} else {
		out.InputTokens = max0(mapInt(u, "prompt_tokens"))
	}
	// output: 优先 output_tokens，否则 completion_tokens
	if v := mapInt(u, "output_tokens"); v != 0 {
		out.OutputTokens = max0(v)
	} else {
		out.OutputTokens = max0(mapInt(u, "completion_tokens"))
	}

	out.CacheReadTokens = openAICacheReadTokensFromUsage(u)
	out.CacheCreationTokens = openAICacheCreationTokensFromUsage(u)

	// 图像输出 + 推理 token（展示用，费用含在 output 内，不与 output 互斥拆分）
	if d, ok := u["output_tokens_details"].(map[string]any); ok {
		out.ImageOutputTokens = max0(mapInt(d, "image_tokens"))
		out.ReasoningTokens = max0(mapInt(d, "reasoning_tokens"))
	}
	if d, ok := u["completion_tokens_details"].(map[string]any); ok {
		if out.ImageOutputTokens == 0 {
			out.ImageOutputTokens = max0(mapInt(d, "image_tokens"))
		}
		if out.ReasoningTokens == 0 {
			out.ReasoningTokens = max0(mapInt(d, "reasoning_tokens"))
		}
	}

	c5 := max0(mapInt(u, "cache_creation_5m_input_tokens", "cache_creation_5m_tokens"))
	c1h := max0(mapInt(u, "cache_creation_1h_input_tokens", "cache_creation_1h_tokens"))
	out.CacheCreation5mTokens = c5
	out.CacheCreation1hTokens = c1h
	if out.CacheCreationTokens == 0 && (c5 > 0 || c1h > 0) {
		out.CacheCreationTokens = c5 + c1h
	}
	return out
}

// openAICacheReadTokensFromUsage follows Sub2API's nested-field order while
// ignoring stale zero aliases for upstream cost accounting.
func openAICacheReadTokensFromUsage(u map[string]any) int {
	for _, key := range []string{"input_tokens_details", "prompt_tokens_details"} {
		if d, ok := u[key].(map[string]any); ok {
			if _, exists := d["cached_tokens"]; exists {
				if value := max0(mapInt(d, "cached_tokens")); value > 0 {
					return value
				}
			}
		}
	}
	return firstPositiveInt(u,
		"cache_read_input_tokens",
		"cache_read_tokens",
		"cached_tokens",
		"prompt_cache_hit_tokens",
		"cache_hit_tokens",
	)
}

func openAICacheCreationTokensFromUsage(u map[string]any) int {
	// Keep the nested priority aligned with Sub2API's gjson parser, but ignore
	// stale zero aliases so local cost accounting can retain a positive
	// compatibility bucket that the response rewrite will expose downstream.
	for _, nested := range [][2]string{
		{"input_tokens_details", "cache_write_tokens"},
		{"prompt_tokens_details", "cache_write_tokens"},
		{"input_tokens_details", "cache_creation_tokens"},
		{"prompt_tokens_details", "cache_creation_tokens"},
	} {
		details, _ := u[nested[0]].(map[string]any)
		if _, exists := details[nested[1]]; exists {
			if value := max0(mapInt(details, nested[1])); value > 0 {
				return value
			}
		}
	}
	if value := firstPositiveInt(u,
		"cache_write_tokens",
		"cache_creation_input_tokens",
		"cache_write_input_tokens",
		"cache_creation_tokens",
	); value > 0 {
		return value
	}
	// Some compatible providers expose only the Anthropic-style cache
	// creation duration buckets. Keep them out of fresh input, and let the
	// downstream response rewriter synthesize a Sub2API-compatible total.
	return firstPositiveInt(u, "cache_creation_5m_input_tokens", "cache_creation_5m_tokens") +
		firstPositiveInt(u, "cache_creation_1h_input_tokens", "cache_creation_1h_tokens")
}

func max0(n int) int {
	if n < 0 {
		return 0
	}
	return n
}

func mapInt(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch n := v.(type) {
			case float64:
				return int(n)
			case int:
				return n
			case int64:
				return int(n)
			case json.Number:
				if i, err := n.Int64(); err == nil {
					return int(i)
				}
			case string:
				// 少数上游把数字当字符串
				var f float64
				if err := json.Unmarshal([]byte(n), &f); err == nil {
					return int(f)
				}
			}
		}
	}
	return 0
}

func firstPositiveInt(m map[string]any, keys ...string) int {
	for _, k := range keys {
		if n := mapInt(m, k); n > 0 {
			return n
		}
	}
	return 0
}

// ParseAnthropicUsage 从 Anthropic messages JSON 解析 usage。
func ParseAnthropicUsage(body []byte) UsageTokens {
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return UsageTokens{}
	}
	usageObj, _ := raw["usage"].(map[string]any)
	if usageObj == nil {
		return UsageTokens{}
	}
	u := parseUsageMapSub2API(usageObj)
	if u.InputTokens == 0 {
		u.InputTokens = max0(mapInt(usageObj, "input_tokens"))
	}
	if u.OutputTokens == 0 {
		u.OutputTokens = max0(mapInt(usageObj, "output_tokens"))
	}
	if u.CacheReadTokens == 0 {
		u.CacheReadTokens = max0(mapInt(usageObj, "cache_read_input_tokens"))
	}
	if u.CacheCreationTokens == 0 {
		u.CacheCreationTokens = max0(mapInt(usageObj, "cache_creation_input_tokens"))
	}
	return u
}

// ParseOpenAISSEUsage 从 OpenAI SSE 提取 usage（按事件块解析，末次 usage 覆盖）。
func ParseOpenAISSEUsage(stream []byte) UsageTokens {
	var last UsageTokens
	var has bool

	// 先按标准 SSE 事件（空行分隔）解析
	for _, eventData := range splitSSEDataPayloads(stream) {
		if eventData == "[DONE]" {
			continue
		}
		// 优先：chunk 含 usage 对象则整段覆盖（sub2api extractCCStreamUsage）
		if chunkHasUsage([]byte(eventData)) {
			last = ParseOpenAIUsage([]byte(eventData))
			has = true
			continue
		}
		u := ParseOpenAIUsage([]byte(eventData))
		if usageNonEmpty(u) {
			last = mergeUsagePreferNewer(last, u)
			has = true
		}
	}

	// 兜底：整包当 JSON（上游误返回非 SSE）
	if !has {
		u := ParseOpenAIUsage(stream)
		if usageNonEmpty(u) {
			return u
		}
	}
	// 再兜底：全文扫 "usage":{...} 最后一个对象
	if !has || (last.CacheReadTokens == 0 && last.InputTokens > 0) {
		if u := extractLastUsageObject(stream); usageNonEmpty(u) {
			if !has {
				return u
			}
			// 已有主字段时只补 cache
			if last.CacheReadTokens == 0 && u.CacheReadTokens > 0 {
				last.CacheReadTokens = u.CacheReadTokens
			}
			if last.CacheCreationTokens == 0 && u.CacheCreationTokens > 0 {
				last.CacheCreationTokens = u.CacheCreationTokens
			}
			if last.InputTokens == 0 && u.InputTokens > 0 {
				last.InputTokens = u.InputTokens
			}
			if last.OutputTokens == 0 && u.OutputTokens > 0 {
				last.OutputTokens = u.OutputTokens
			}
		}
	}
	return last
}

func usageNonEmpty(u UsageTokens) bool {
	return u.InputTokens > 0 || u.OutputTokens > 0 || u.CacheReadTokens > 0 ||
		u.CacheCreationTokens > 0 || u.ImageOutputTokens > 0 || u.ReasoningTokens > 0
}

// splitSSEDataPayloads 按 SSE 事件拆分，合并同一事件多行 data:。
func splitSSEDataPayloads(stream []byte) []string {
	var out []string
	var dataLines []string
	flush := func() {
		if len(dataLines) == 0 {
			return
		}
		out = append(out, strings.Join(dataLines, "\n"))
		dataLines = dataLines[:0]
	}
	sc := bufio.NewScanner(bytes.NewReader(stream))
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimRight(sc.Text(), "\r")
		if line == "" {
			flush()
			continue
		}
		if strings.HasPrefix(line, "data:") {
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
		// 忽略 event:/id:/retry: 等
	}
	flush()
	// 若完全不像 SSE，尝试按行 data:
	if len(out) == 0 {
		sc = bufio.NewScanner(bytes.NewReader(stream))
		sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for sc.Scan() {
			line := sc.Text()
			if strings.HasPrefix(line, "data:") {
				p := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if p != "" {
					out = append(out, p)
				}
			}
		}
	}
	return out
}

func chunkHasUsage(payload []byte) bool {
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		return false
	}
	_, ok := raw["usage"].(map[string]any)
	return ok
}

// extractLastUsageObject 从原始流里找最后一个 "usage":{...} 并解析（容错异常 SSE）。
func extractLastUsageObject(stream []byte) UsageTokens {
	key := []byte(`"usage"`)
	idx := bytes.LastIndex(stream, key)
	if idx < 0 {
		return UsageTokens{}
	}
	// 找到 usage 后的 {
	rest := stream[idx+len(key):]
	brace := bytes.IndexByte(rest, '{')
	if brace < 0 {
		return UsageTokens{}
	}
	obj, ok := extractJSONObject(rest[brace:])
	if !ok {
		return UsageTokens{}
	}
	var usage map[string]any
	if err := json.Unmarshal(obj, &usage); err != nil {
		return UsageTokens{}
	}
	return parseUsageMapSub2API(usage)
}

func extractJSONObject(b []byte) ([]byte, bool) {
	if len(b) == 0 || b[0] != '{' {
		return nil, false
	}
	depth := 0
	inStr := false
	esc := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if inStr {
			if esc {
				esc = false
				continue
			}
			if c == '\\' {
				esc = true
				continue
			}
			if c == '"' {
				inStr = false
			}
			continue
		}
		switch c {
		case '"':
			inStr = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return b[:i+1], true
			}
		}
	}
	return nil, false
}

func mergeUsagePreferNewer(prev, next UsageTokens) UsageTokens {
	out := prev
	if next.InputTokens > 0 {
		out.InputTokens = next.InputTokens
	}
	if next.OutputTokens > 0 {
		out.OutputTokens = next.OutputTokens
	}
	if next.CacheReadTokens > 0 {
		out.CacheReadTokens = next.CacheReadTokens
	}
	if next.CacheCreationTokens > 0 {
		out.CacheCreationTokens = next.CacheCreationTokens
	}
	if next.CacheCreation5mTokens > 0 {
		out.CacheCreation5mTokens = next.CacheCreation5mTokens
	}
	if next.CacheCreation1hTokens > 0 {
		out.CacheCreation1hTokens = next.CacheCreation1hTokens
	}
	if next.ImageOutputTokens > 0 {
		out.ImageOutputTokens = next.ImageOutputTokens
	}
	if next.ReasoningTokens > 0 {
		out.ReasoningTokens = next.ReasoningTokens
	}
	if !usageNonEmpty(next) {
		return prev
	}
	return out
}

// ParseAnthropicSSEUsage 从 Anthropic SSE 解析 usage。
func ParseAnthropicSSEUsage(stream []byte) UsageTokens {
	var out UsageTokens
	for _, payload := range splitSSEDataPayloads(stream) {
		if payload == "" {
			continue
		}
		var evt struct {
			Type    string `json:"type"`
			Message *struct {
				Usage map[string]any `json:"usage"`
			} `json:"message"`
			Usage map[string]any `json:"usage"`
		}
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			out = mergeUsagePreferNewer(out, ParseOpenAIUsage([]byte(payload)))
			continue
		}
		switch evt.Type {
		case "message_start":
			if evt.Message != nil && evt.Message.Usage != nil {
				out = mergeUsagePreferNewer(out, parseUsageMapSub2API(evt.Message.Usage))
			}
		case "message_delta":
			if evt.Usage != nil {
				out = mergeUsagePreferNewer(out, parseUsageMapSub2API(evt.Usage))
			}
		default:
			if evt.Usage != nil {
				out = mergeUsagePreferNewer(out, parseUsageMapSub2API(evt.Usage))
			}
		}
	}
	return out
}

// ParseUsage 按协议与是否 SSE 统一解析 usage（gateway 转发落库入口）。
func ParseUsage(body []byte, stream bool, kind protocol.Kind) UsageTokens {
	k := protocol.NormalizeKind(kind)
	if stream {
		if k == protocol.KindAnthropic {
			return ParseAnthropicSSEUsage(body)
		}
		// OpenAI Chat / Responses / 其它：按 SSE 抽 usage（sub2api 同款）
		return ParseOpenAISSEUsage(body)
	}
	if k == protocol.KindAnthropic {
		return ParseAnthropicUsage(body)
	}
	return ParseOpenAIUsage(body)
}

// EnsureStreamUsageOption 对齐 sub2api ensureOpenAIChatStreamUsage：
// 强制 stream_options.include_usage=true（覆盖 false），保证末帧带完整 usage。
func EnsureStreamUsageOption(body []byte, stream bool) []byte {
	if !stream || len(body) == 0 {
		return body
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	if s, ok := m["stream"].(bool); ok && !s {
		return body
	}
	m["stream"] = true
	so, _ := m["stream_options"].(map[string]any)
	if so == nil {
		so = map[string]any{}
	}
	so["include_usage"] = true
	m["stream_options"] = so
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}
