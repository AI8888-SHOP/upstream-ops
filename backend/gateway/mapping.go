package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
)

// ModelMap 模型名映射（客户端 → 上游）；键 "*" 表示通配。
type ModelMap map[string]string

// ParseModelMapping 解析 JSON 对象为 ModelMap。
func ParseModelMapping(raw string) ModelMap {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "{}" || raw == "null" {
		return nil
	}
	var anyMap map[string]any
	if err := json.Unmarshal([]byte(raw), &anyMap); err != nil {
		// 也尝试直接 string map
		var strMap map[string]string
		if err2 := json.Unmarshal([]byte(raw), &strMap); err2 != nil {
			return nil
		}
		return ModelMap(strMap)
	}
	out := make(ModelMap, len(anyMap))
	for k, v := range anyMap {
		switch t := v.(type) {
		case string:
			if strings.TrimSpace(t) != "" {
				out[k] = t
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// Resolve 按本映射解析客户端模型名（精确匹配优先，其次 "*"）。
func (m ModelMap) Resolve(requested string) (upstream string, mapped bool) {
	upstream = strings.TrimSpace(requested)
	if upstream == "" || len(m) == 0 {
		return upstream, false
	}
	if v, ok := m[upstream]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v), true
	}
	if v, ok := m["*"]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v), true
	}
	return upstream, false
}

// ResolveModel 按多层映射把客户端模型名解析为上游模型名。
// 优先精确匹配，其次 "*" 通配；无映射时返回原名。
// chain 记录 "A->B" 便于 usage 展示。
func ResolveModel(requested string, maps ...map[string]string) (upstream string, chain string) {
	upstream = strings.TrimSpace(requested)
	if upstream == "" {
		return "", ""
	}
	current := upstream
	parts := []string{current}
	for _, m := range maps {
		next, ok := ModelMap(m).Resolve(current)
		if !ok {
			continue
		}
		current = next
		parts = append(parts, current)
	}
	if len(parts) == 1 {
		return current, ""
	}
	return current, strings.Join(parts, "->")
}

// RewriteModelInBody 若 body 是 JSON 对象则改写 model 字段。
func RewriteModelInBody(body []byte, upstreamModel string) []byte {
	if len(body) == 0 || strings.TrimSpace(upstreamModel) == "" {
		return body
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(body, &obj); err != nil {
		return body
	}
	raw, err := json.Marshal(upstreamModel)
	if err != nil {
		return body
	}
	obj["model"] = raw
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}

type requestThinking struct {
	Type string `json:"type"`
}

type requestBodyInfo struct {
	Model           string `json:"model"`
	Stream          bool   `json:"stream"`
	ServiceTier     string `json:"service_tier"`
	ReasoningEffort string `json:"reasoning_effort"`
	Reasoning       *struct {
		Effort string `json:"effort"`
	} `json:"reasoning"`
	OutputConfig *struct {
		Effort string `json:"effort"`
	} `json:"output_config"`
	Thinking *requestThinking `json:"thinking"`

	SessionID          string `json:"session_id"`
	SessionIDCamel     string `json:"sessionId"`
	ConversationID     string `json:"conversation_id"`
	ConversationIDCamel string `json:"conversationId"`
	ThreadID           string `json:"thread_id"`
	ThreadIDCamel      string `json:"threadId"`
	PromptCacheKey     string `json:"prompt_cache_key"`
	PromptCacheKeyCamel string `json:"promptCacheKey"`
	PreviousResponseID string `json:"previous_response_id"`
	PreviousResponseIDCamel string `json:"previousResponseId"`

	ResponseModalities      []string `json:"response_modalities"`
	ResponseModalitiesCamel []string `json:"responseModalities"`
	OutputModalities        []string `json:"output_modalities"`
	OutputModalitiesCamel   []string `json:"outputModalities"`
	Modalities              []string `json:"modalities"`
	ResponseMIMEType        string   `json:"response_mime_type"`
	ResponseMIMETypeCamel   string   `json:"responseMimeType"`
	OutputMIMEType          string   `json:"output_mime_type"`
	OutputMIMETypeCamel     string   `json:"outputMimeType"`
	ImageGeneration         json.RawMessage `json:"image_generation"`
	ImageGenerationCamel    json.RawMessage `json:"imageGeneration"`
	VideoGeneration         json.RawMessage `json:"video_generation"`
	VideoGenerationCamel    json.RawMessage `json:"videoGeneration"`
	Task                     string          `json:"task"`
	Operation                string          `json:"operation"`
	Tools                    []struct {
		Type string `json:"type"`
	} `json:"tools"`
}

type requestAnalysis struct {
	Model           string
	Stream          bool
	ServiceTier     string
	ReasoningEffort string
	ThinkingEnabled bool
	MediaGeneration bool
	AffinityID      string
	Parsed          bool
}

func parseRequestBodyInfo(body []byte) (requestBodyInfo, bool) {
	var info requestBodyInfo
	if len(body) == 0 || json.Unmarshal(body, &info) != nil {
		return requestBodyInfo{}, false
	}
	return info, true
}

func analyzeRequestBody(body []byte) requestAnalysis {
	info, ok := parseRequestBodyInfo(body)
	if !ok {
		return requestAnalysis{}
	}
	serviceTier, reasoningEffort := requestMetaFromInfo(info)
	return requestAnalysis{
		Model:           strings.TrimSpace(info.Model),
		Stream:          info.Stream,
		ServiceTier:     serviceTier,
		ReasoningEffort: reasoningEffort,
		ThinkingEnabled: bodyThinkingEnabled(info.Thinking),
		MediaGeneration: requestInfoGeneratesMedia(info),
		AffinityID:      requestInfoAffinityID(info),
		Parsed:          true,
	}
}

// ExtractRequestInfo parses the request envelope once for the forwarding hot
// path. Large conversation arrays are skipped by encoding/json because they are
// not fields on requestBodyInfo.
func ExtractRequestInfo(body []byte) (model string, stream bool, serviceTier, reasoningEffort string, thinkingEnabled bool) {
	analysis := analyzeRequestBody(body)
	if !analysis.Parsed {
		return "", false, "", "", false
	}
	model = analysis.Model
	stream = analysis.Stream
	serviceTier = analysis.ServiceTier
	reasoningEffort = analysis.ReasoningEffort
	thinkingEnabled = analysis.ThinkingEnabled
	return
}

func requestInfoAffinityID(info requestBodyInfo) string {
	for _, value := range []string{
		info.SessionID, info.SessionIDCamel,
		info.ConversationID, info.ConversationIDCamel,
		info.ThreadID, info.ThreadIDCamel,
		info.PromptCacheKey, info.PromptCacheKeyCamel,
		info.PreviousResponseID, info.PreviousResponseIDCamel,
	} {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func requestInfoGeneratesMedia(info requestBodyInfo) bool {
	if mediaGenerationModel(info.Model) {
		return true
	}
	for _, modalities := range [][]string{
		info.ResponseModalities, info.ResponseModalitiesCamel,
		info.OutputModalities, info.OutputModalitiesCamel, info.Modalities,
	} {
		for _, modality := range modalities {
			switch strings.ToLower(strings.TrimSpace(modality)) {
			case "image", "video":
				return true
			}
		}
	}
	for _, mimeType := range []string{
		info.ResponseMIMEType, info.ResponseMIMETypeCamel,
		info.OutputMIMEType, info.OutputMIMETypeCamel,
	} {
		if mediaMIMEType(mimeType) {
			return true
		}
	}
	for _, option := range []json.RawMessage{
		info.ImageGeneration, info.ImageGenerationCamel,
		info.VideoGeneration, info.VideoGenerationCamel,
	} {
		trimmed := bytes.TrimSpace(option)
		if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte("false")) {
			return true
		}
	}
	if mediaGenerationType(info.Task) || mediaGenerationType(info.Operation) {
		return true
	}
	for _, tool := range info.Tools {
		if mediaGenerationType(tool.Type) {
			return true
		}
	}
	return false
}

// ExtractModelFromBody 从 JSON 请求体取 model。
func ExtractModelFromBody(body []byte) string {
	info, ok := parseRequestBodyInfo(body)
	if !ok {
		return ""
	}
	return strings.TrimSpace(info.Model)
}

// ExtractStreamFlag 从请求体判断是否 stream。
func ExtractStreamFlag(body []byte) bool {
	info, ok := parseRequestBodyInfo(body)
	return ok && info.Stream
}

// ExtractMetaFromBody 提取 service_tier / reasoning_effort（尽力而为）。
// reasoning_effort 提取优先级对齐 sub2api：
//  1. reasoning.effort（Responses / Codex 嵌套）
//  2. reasoning_effort（Chat Completions 扁平）
//  3. output_config.effort（Claude / Claude Code）
//  4. 模型名后缀推导（如 gpt-5-high / o3-xhigh）
//  5. 国产 thinking 开启但无档位时默认 "high"（见 ApplyThinkingEnabledEffortFallback；
//     此处仅用 body.model 做初判，路由映射后的上游模型在转发路径再补一次）
func ExtractMetaFromBody(body []byte) (serviceTier, reasoningEffort string) {
	info, ok := parseRequestBodyInfo(body)
	if !ok {
		return "", ""
	}
	return requestMetaFromInfo(info)
}

func requestMetaFromInfo(info requestBodyInfo) (serviceTier, reasoningEffort string) {
	serviceTier = strings.TrimSpace(info.ServiceTier)

	if info.Reasoning != nil {
		if e := normalizeOpenAIReasoningEffort(info.Reasoning.Effort); e != "" {
			return serviceTier, e
		}
	}
	if e := normalizeOpenAIReasoningEffort(info.ReasoningEffort); e != "" {
		return serviceTier, e
	}
	if info.OutputConfig != nil {
		if e := normalizeClaudeOutputEffort(info.OutputConfig.Effort); e != "" {
			return serviceTier, e
		}
	}
	if e := deriveReasoningEffortFromModel(info.Model); e != "" {
		return serviceTier, e
	}
	// 初判：thinking 已开且 body.model 属于国产 passback 族 → high
	// （映射后的上游模型在 attempt 侧再走一遍 ApplyThinkingEnabledEffortFallback）
	if bodyThinkingEnabled(info.Thinking) {
		if e := defaultEffortForThinkingEnabled(info.Model); e != "" {
			return serviceTier, e
		}
	}
	return serviceTier, ""
}

// bodyHasThinkingEnabled 检测入站 body 是否开启 thinking（Anthropic / 国产兼容）。
// 对齐 sub2api：thinking.type 为 enabled 或 adaptive。
func bodyHasThinkingEnabled(body []byte) bool {
	info, ok := parseRequestBodyInfo(body)
	return ok && bodyThinkingEnabled(info.Thinking)
}

func bodyThinkingEnabled(thinking *requestThinking) bool {
	if thinking == nil {
		return false
	}
	typ := strings.ToLower(strings.TrimSpace(thinking.Type))
	return typ == "enabled" || typ == "adaptive"
}

// ApplyThinkingEnabledEffortFallback 对齐 sub2api ApplyThinkingEnabledFallback：
// 已有 effort 不覆盖；thinking 未开不填；仅国产 passback-required 模型族默认 "high"。
// modelCandidates 按优先级尝试（通常 upstreamModel, requestedModel）。
func ApplyThinkingEnabledEffortFallback(effort string, thinkingEnabled bool, modelCandidates ...string) string {
	if strings.TrimSpace(effort) != "" {
		return effort
	}
	if !thinkingEnabled {
		return ""
	}
	for _, m := range modelCandidates {
		if e := defaultEffortForThinkingEnabled(m); e != "" {
			return e
		}
	}
	return ""
}

// defaultEffortForThinkingEnabled 对齐 sub2api DefaultEffortForThinkingEnabled。
// Kimi/GLM/MiniMax/Qwen-thinking 等无 effort 档位的国产模型，thinking 开启时 usage 记 "high"；
// DeepSeek 虽属 passback 族但有原生 reasoning_effort，不注入默认值。
func defaultEffortForThinkingEnabled(model string) string {
	if !isPassbackRequiredThinkingModel(model) {
		return ""
	}
	id := modelIDBase(model)
	if strings.HasPrefix(id, "deepseek-") {
		return ""
	}
	return "high"
}

// isPassbackRequiredThinkingModel 对齐 sub2api ResolveThinkingProtocol == PassbackRequired。
func isPassbackRequiredThinkingModel(model string) bool {
	id := modelIDBase(model)
	if id == "" {
		return false
	}
	switch {
	case strings.HasPrefix(id, "deepseek-"),
		strings.HasPrefix(id, "kimi-"),
		strings.HasPrefix(id, "moonshot-"),
		strings.HasPrefix(id, "glm-"):
		return true
	}
	// MiniMax M 系列：minimax-m2 / minimax-m2.5 等
	if strings.HasPrefix(id, "minimax-m") {
		return true
	}
	// Qwen thinking 变体：qwen-/qwen2-/qwen3-/qwen4- + 含 -thinking
	if (strings.HasPrefix(id, "qwen-") ||
		strings.HasPrefix(id, "qwen2-") ||
		strings.HasPrefix(id, "qwen3-") ||
		strings.HasPrefix(id, "qwen4-")) && strings.Contains(id, "-thinking") {
		return true
	}
	return false
}

// modelIDBase 取 provider/model 的最后一段并 lower。
func modelIDBase(model string) string {
	id := strings.TrimSpace(model)
	if id == "" {
		return ""
	}
	if strings.Contains(id, "/") {
		parts := strings.Split(id, "/")
		id = parts[len(parts)-1]
	}
	return strings.ToLower(strings.TrimSpace(id))
}

// normalizeOpenAIReasoningEffort 对齐 sub2api：仅保留已知档位；
// none/minimal 记空；x-high/x_high/extrahigh/max → xhigh。
func normalizeOpenAIReasoningEffort(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return ""
	}
	value = strings.NewReplacer("-", "", "_", "", " ", "").Replace(value)
	switch value {
	case "none", "minimal":
		return ""
	case "low", "medium", "high":
		return value
	case "xhigh", "extrahigh", "max":
		return "xhigh"
	default:
		return ""
	}
}

// normalizeClaudeOutputEffort 对齐 sub2api NormalizeClaudeOutputEffort：
// Claude 的 output_config.effort 允许 max（与 OpenAI 归一到 xhigh 不同）。
func normalizeClaudeOutputEffort(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	if value == "" {
		return ""
	}
	value = strings.NewReplacer("-", "", "_", "", " ", "").Replace(value)
	switch value {
	case "low", "medium", "high", "xhigh", "max":
		return value
	case "extrahigh":
		return "xhigh"
	default:
		return ""
	}
}

// deriveReasoningEffortFromModel 从模型名末段推导 effort（如 o3-high、gpt-5-xhigh）。
// 对齐 sub2api deriveOpenAIReasoningEffortFromModel：取 path 最后一段，再按 -/_/空格拆分取末 token。
func deriveReasoningEffortFromModel(model string) string {
	modelID := strings.TrimSpace(model)
	if modelID == "" {
		return ""
	}
	if strings.Contains(modelID, "/") {
		parts := strings.Split(modelID, "/")
		modelID = parts[len(parts)-1]
	}
	parts := strings.FieldsFunc(strings.ToLower(modelID), func(r rune) bool {
		switch r {
		case '-', '_', ' ':
			return true
		default:
			return false
		}
	})
	if len(parts) == 0 {
		return ""
	}
	return normalizeOpenAIReasoningEffort(parts[len(parts)-1])
}
