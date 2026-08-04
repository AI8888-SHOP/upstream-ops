package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"
)

const (
	defaultValidationPrefixBytes   = 8192
	defaultValidationPrefixTimeout = 2 * time.Second
	maxValidationPrefixBytes       = 1 << 20
	postCommitValidationBytes      = 64 << 10
)

type validationDecision uint8

const (
	validationPending validationDecision = iota
	validationAccepted
	validationRejected
)

type validationResult struct {
	Decision   validationDecision
	RuleID     uint
	RuleName   string
	Target     string
	Pattern    string
	MatchedOn  string
	PostCommit bool
}

func (r validationResult) IsPending() bool  { return r.Decision == validationPending }
func (r validationResult) IsAccepted() bool { return r.Decision == validationAccepted }
func (r validationResult) IsRejected() bool { return r.Decision == validationRejected }

type responseRejectedError struct {
	Result validationResult
}

func (e *responseRejectedError) Error() string {
	if e == nil || strings.TrimSpace(e.Result.RuleName) == "" {
		return "upstream response rejected by response validation"
	}
	return fmt.Sprintf("upstream response rejected by response rule %q", e.Result.RuleName)
}

type responseValidationConfig struct {
	Enabled       bool
	StreamMode    string
	PrefixBytes   int
	PrefixTimeout time.Duration
}

type responseRuleSpec struct {
	ID        uint
	Name      string
	Enabled   bool
	Priority  int
	Pattern   string
	Target    string
	Models    []string
	Protocols []string
}

type compiledResponseRule struct {
	responseRuleSpec
	re *regexp.Regexp
}

type responseValidator struct {
	enabled       bool
	streamMode    string
	prefixBytes   int
	prefixTimeout time.Duration
	rules         []compiledResponseRule
}

func newResponseValidator(cfg responseValidationConfig, rules []responseRuleSpec) (*responseValidator, error) {
	v := &responseValidator{
		enabled:       cfg.Enabled,
		streamMode:    strings.ToLower(strings.TrimSpace(cfg.StreamMode)),
		prefixBytes:   cfg.PrefixBytes,
		prefixTimeout: cfg.PrefixTimeout,
	}
	if v.streamMode == "" {
		v.streamMode = "prefix"
	}
	if v.streamMode != "prefix" && v.streamMode != "disabled" {
		return nil, fmt.Errorf("unsupported response validation stream mode %q", cfg.StreamMode)
	}
	if v.prefixBytes <= 0 {
		v.prefixBytes = defaultValidationPrefixBytes
	}
	if v.prefixBytes > maxValidationPrefixBytes {
		v.prefixBytes = maxValidationPrefixBytes
	}
	if v.prefixTimeout <= 0 {
		v.prefixTimeout = defaultValidationPrefixTimeout
	}

	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}
		return rules[i].ID < rules[j].ID
	})
	for i, rule := range rules {
		if !rule.Enabled {
			continue
		}
		rule.Name = strings.TrimSpace(rule.Name)
		rule.Pattern = strings.TrimSpace(rule.Pattern)
		if rule.Pattern == "" {
			return nil, fmt.Errorf("response rule %d has an empty pattern", i)
		}
		rule.Target = strings.ToLower(strings.TrimSpace(rule.Target))
		if rule.Target == "" {
			rule.Target = "assistant_text"
		}
		switch rule.Target {
		case "assistant_text", "raw_body", "error_message":
		default:
			return nil, fmt.Errorf("response rule %q has unsupported target %q", rule.Name, rule.Target)
		}
		re, err := regexp.Compile(rule.Pattern)
		if err != nil {
			return nil, fmt.Errorf("compile response rule %q: %w", rule.Name, err)
		}
		v.rules = append(v.rules, compiledResponseRule{responseRuleSpec: rule, re: re})
	}
	return v, nil
}

func (v *responseValidator) Enabled() bool {
	return v != nil && v.enabled && len(v.rules) > 0
}

func (v *responseValidator) StreamEnabled() bool {
	return v.Enabled() && v.streamMode == "prefix"
}

func (v *responseValidator) PrefixBytes() int {
	if v == nil || v.prefixBytes <= 0 {
		return defaultValidationPrefixBytes
	}
	return v.prefixBytes
}

func (v *responseValidator) PrefixTimeout() time.Duration {
	if v == nil || v.prefixTimeout <= 0 {
		return defaultValidationPrefixTimeout
	}
	return v.prefixTimeout
}

// Validate checks the client-side, protocol-converted response. The caller
// must invoke it before writing headers or body to the downstream client.
func (v *responseValidator) Validate(body []byte, headers http.Header, protocolName, model string) validationResult {
	if !v.Enabled() {
		return acceptedValidation()
	}
	assistantText := ""
	errorMessage := ""
	if v.needsTarget("assistant_text", protocolName, model) {
		assistantText = extractAssistantText(body, headers)
	}
	if v.needsTarget("error_message", protocolName, model) {
		errorMessage = extractResponseErrorMessage(body, headers)
	}
	return v.match(protocolName, model, body, []byte(assistantText), []byte(errorMessage), false)
}

func (v *responseValidator) needsTarget(target, protocolName, model string) bool {
	for _, rule := range v.rules {
		if rule.Target == target && responseRuleApplies(rule, protocolName, model) {
			return true
		}
	}
	return false
}

func (v *responseValidator) match(protocolName, model string, raw, assistant, errorMessage []byte, postCommit bool) validationResult {
	if !v.Enabled() {
		return acceptedValidation()
	}
	for _, rule := range v.rules {
		if !responseRuleApplies(rule, protocolName, model) {
			continue
		}
		var candidate []byte
		switch rule.Target {
		case "raw_body":
			candidate = raw
		case "error_message":
			candidate = errorMessage
		default:
			candidate = assistant
		}
		if len(candidate) == 0 || !rule.re.Match(candidate) {
			continue
		}
		matched := string(candidate)
		if len(matched) > 4096 {
			matched = matched[:4096]
		}
		return validationResult{
			Decision: validationRejected, RuleID: rule.ID, RuleName: rule.Name,
			Target: rule.Target, Pattern: rule.Pattern, MatchedOn: matched, PostCommit: postCommit,
		}
	}
	return acceptedValidation()
}

func responseRuleApplies(rule compiledResponseRule, protocolName, model string) bool {
	return selectorMatches(rule.Protocols, protocolName) && selectorMatches(rule.Models, model)
}

func selectorMatches(selectors []string, value string) bool {
	if len(selectors) == 0 {
		return true
	}
	value = strings.TrimSpace(value)
	for _, raw := range selectors {
		selector := strings.TrimSpace(raw)
		if selector == "" {
			continue
		}
		if selector == "*" || strings.EqualFold(selector, value) {
			return true
		}
		if strings.ContainsAny(selector, "*?") {
			ok, err := path.Match(strings.ToLower(selector), strings.ToLower(value))
			if err == nil && ok {
				return true
			}
		}
	}
	return false
}

func pendingValidation() validationResult  { return validationResult{Decision: validationPending} }
func acceptedValidation() validationResult { return validationResult{Decision: validationAccepted} }

// streamResponseValidator buffers and validates only the configured prefix.
// Once Ready accepts it, later matches are returned with PostCommit=true for
// audit and never switch the already-visible response.
type streamResponseValidator struct {
	validator      *responseValidator
	protocolName   string
	model          string
	firstContentAt time.Time
	bytesSeen      int
	prefixReady    bool
	committed      bool
	prefixRaw      []byte
	postRaw        []byte
	result         validationResult
	postResult     validationResult
}

func (v *responseValidator) NewStreamValidator(protocolName, model string) *streamResponseValidator {
	s := &streamResponseValidator{
		validator: v, protocolName: strings.TrimSpace(protocolName), model: strings.TrimSpace(model),
		result: pendingValidation(),
	}
	if !v.StreamEnabled() {
		s.result = acceptedValidation()
		s.prefixReady = true
	}
	return s
}

func (s *streamResponseValidator) Consume(chunk []byte) validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if len(chunk) == 0 {
		return s.currentResult()
	}
	if s.postResult.IsRejected() {
		return s.postResult
	}
	if s.firstContentAt.IsZero() && streamChunkHasPayload(chunk) {
		s.firstContentAt = time.Now()
	}

	if s.prefixReady || s.committed {
		s.appendPost(chunk)
		return s.matchPostCommit()
	}
	remaining := s.validator.PrefixBytes() - s.bytesSeen
	if remaining < 0 {
		remaining = 0
	}
	prefixLen := len(chunk)
	if prefixLen > remaining {
		prefixLen = remaining
	}
	if prefixLen > 0 {
		s.prefixRaw = append(s.prefixRaw, chunk[:prefixLen]...)
		s.bytesSeen += prefixLen
		if result := s.matchPrefix(); result.IsRejected() {
			s.result = result
			return result
		}
	}
	if s.bytesSeen >= s.validator.PrefixBytes() {
		if result := s.matchPrefix(); result.IsRejected() {
			s.result = result
			return result
		}
		s.prefixReady = true
		s.result = acceptedValidation()
	}
	if prefixLen < len(chunk) {
		s.appendPost(chunk[prefixLen:])
		if result := s.matchPostCommit(); result.IsRejected() {
			return result
		}
	}
	return s.currentResult()
}

func (s *streamResponseValidator) Ready(now time.Time) validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if s.result.IsRejected() {
		return s.result
	}
	if s.prefixReady {
		return acceptedValidation()
	}
	if s.bytesSeen >= s.validator.PrefixBytes() ||
		(!s.firstContentAt.IsZero() && !now.Before(s.firstContentAt.Add(s.validator.PrefixTimeout()))) {
		if result := s.matchPrefix(); result.IsRejected() {
			s.result = result
			return result
		}
		s.prefixReady = true
		s.result = acceptedValidation()
		return s.result
	}
	return pendingValidation()
}

func (s *streamResponseValidator) Finalize() validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if s.result.IsRejected() {
		return s.result
	}
	if result := s.matchPrefix(); result.IsRejected() {
		s.result = result
		return result
	}
	s.prefixReady = true
	s.result = acceptedValidation()
	return s.result
}

func (s *streamResponseValidator) Commit() {
	if s == nil {
		return
	}
	s.prefixReady = true
	s.committed = true
	if !s.result.IsRejected() {
		s.result = acceptedValidation()
	}
}

func (s *streamResponseValidator) FirstContentAt() time.Time {
	if s == nil {
		return time.Time{}
	}
	return s.firstContentAt
}

func (s *streamResponseValidator) BytesSeen() int {
	if s == nil {
		return 0
	}
	return s.bytesSeen
}

func (s *streamResponseValidator) currentResult() validationResult {
	if s.postResult.IsRejected() {
		return s.postResult
	}
	if s.result.IsRejected() {
		return s.result
	}
	if s.prefixReady {
		return acceptedValidation()
	}
	return pendingValidation()
}

func (s *streamResponseValidator) matchPrefix() validationResult {
	assistant := []byte(extractAssistantText(s.prefixRaw, http.Header{"Content-Type": []string{"text/event-stream"}}))
	errorMessage := []byte(extractResponseErrorMessage(s.prefixRaw, nil))
	return s.validator.match(s.protocolName, s.model, s.prefixRaw, assistant, errorMessage, false)
}

func (s *streamResponseValidator) appendPost(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	s.postRaw = append(s.postRaw, chunk...)
	if len(s.postRaw) > postCommitValidationBytes {
		trimmed := make([]byte, postCommitValidationBytes)
		copy(trimmed, s.postRaw[len(s.postRaw)-postCommitValidationBytes:])
		s.postRaw = trimmed
	}
}

func (s *streamResponseValidator) matchPostCommit() validationResult {
	if len(s.postRaw) == 0 {
		return acceptedValidation()
	}
	combined := make([]byte, 0, len(s.prefixRaw)+len(s.postRaw))
	combined = append(combined, s.prefixRaw...)
	combined = append(combined, s.postRaw...)
	assistant := []byte(extractAssistantText(combined, http.Header{"Content-Type": []string{"text/event-stream"}}))
	errorMessage := []byte(extractResponseErrorMessage(combined, nil))
	result := s.validator.match(s.protocolName, s.model, combined, assistant, errorMessage, true)
	if result.IsRejected() {
		s.postResult = result
	}
	return result
}

const maxPartialJSONDepth = 64

var assistantJSONFieldPriority = [...]string{
	"choices", "candidates", "output", "message", "item", "part", "content",
	"parts", "delta", "output_text", "text", "completion",
}

// extractPartialJSONAssistantText handles an SSE data payload that ends before
// the JSON event has closed. It deliberately traverses only known assistant
// fields, so arbitrary metadata strings are not treated as visible text.
func extractPartialJSONAssistantText(data []byte) string {
	p := partialJSONParser{data: data}
	return p.parseValue(true, 0)
}

func extractPartialJSONErrorMessage(data []byte) string {
	for _, key := range []string{"error", "message", "detail", "error_description"} {
		value, ok := partialJSONRootMember(data, key)
		if !ok {
			continue
		}
		p := partialJSONParser{data: value}
		if text := p.parseValue(true, 0); text != "" {
			return text
		}
	}
	return ""
}

type partialJSONParser struct {
	data []byte
	pos  int
}

func partialJSONRootMember(data []byte, wanted string) ([]byte, bool) {
	p := partialJSONParser{data: data}
	p.skipSpace()
	if p.pos >= len(data) || data[p.pos] != '{' {
		return nil, false
	}
	p.pos++
	for {
		p.skipSpace()
		if p.pos >= len(data) || data[p.pos] == '}' {
			return nil, false
		}
		if data[p.pos] == ',' {
			p.pos++
			continue
		}
		key, complete := p.parseString()
		if !complete {
			return nil, false
		}
		p.skipSpace()
		if p.pos >= len(data) || data[p.pos] != ':' {
			return nil, false
		}
		p.pos++
		p.skipSpace()
		start := p.pos
		if key == wanted {
			return data[start:], true
		}
		before := p.pos
		p.parseValue(false, 1)
		if p.pos == before {
			return nil, false
		}
	}
}

func (p *partialJSONParser) parseValue(collect bool, depth int) string {
	p.skipSpace()
	if p.pos >= len(p.data) || depth > maxPartialJSONDepth {
		p.pos = len(p.data)
		return ""
	}
	switch p.data[p.pos] {
	case '{':
		return p.parseObject(collect, depth+1)
	case '[':
		return p.parseArray(collect, depth+1)
	case '"':
		value, _ := p.parseString()
		if collect {
			return value
		}
		return ""
	default:
		p.skipPrimitive()
		return ""
	}
}

func (p *partialJSONParser) parseObject(collect bool, depth int) string {
	p.pos++
	values := make(map[string]string)
	for {
		p.skipSpace()
		if p.pos >= len(p.data) {
			return selectPartialAssistantField(values)
		}
		switch p.data[p.pos] {
		case '}':
			p.pos++
			return selectPartialAssistantField(values)
		case ',':
			p.pos++
			continue
		case '"':
		default:
			return selectPartialAssistantField(values)
		}
		key, complete := p.parseString()
		if !complete {
			return selectPartialAssistantField(values)
		}
		p.skipSpace()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			return selectPartialAssistantField(values)
		}
		p.pos++
		childCollect := collect && isAssistantJSONField(key)
		value := p.parseValue(childCollect, depth)
		if childCollect {
			values[key] += value
		}
	}
}

func (p *partialJSONParser) parseArray(collect bool, depth int) string {
	p.pos++
	var out strings.Builder
	for {
		p.skipSpace()
		if p.pos >= len(p.data) {
			return out.String()
		}
		switch p.data[p.pos] {
		case ']':
			p.pos++
			return out.String()
		case ',':
			p.pos++
			continue
		}
		before := p.pos
		value := p.parseValue(collect, depth)
		if collect {
			out.WriteString(value)
		}
		if p.pos == before {
			return out.String()
		}
	}
}

func (p *partialJSONParser) parseString() (string, bool) {
	if p.pos >= len(p.data) || p.data[p.pos] != '"' {
		return "", false
	}
	p.pos++
	var out strings.Builder
	for p.pos < len(p.data) {
		current := p.data[p.pos]
		switch {
		case current == '"':
			p.pos++
			return out.String(), true
		case current == '\\':
			p.pos++
			if p.pos >= len(p.data) {
				return out.String(), false
			}
			escape := p.data[p.pos]
			p.pos++
			switch escape {
			case '"', '\\', '/':
				out.WriteByte(escape)
			case 'b':
				out.WriteByte('\b')
			case 'f':
				out.WriteByte('\f')
			case 'n':
				out.WriteByte('\n')
			case 'r':
				out.WriteByte('\r')
			case 't':
				out.WriteByte('\t')
			case 'u':
				r, ok := p.parseUnicodeEscape()
				if !ok {
					return out.String(), false
				}
				out.WriteRune(r)
			default:
				return out.String(), false
			}
		case current < utf8.RuneSelf:
			if current < 0x20 {
				return out.String(), false
			}
			out.WriteByte(current)
			p.pos++
		default:
			remaining := p.data[p.pos:]
			if !utf8.FullRune(remaining) {
				p.pos = len(p.data)
				return out.String(), false
			}
			r, size := utf8.DecodeRune(remaining)
			out.WriteRune(r)
			p.pos += size
		}
	}
	return out.String(), false
}

func (p *partialJSONParser) parseUnicodeEscape() (rune, bool) {
	first, ok := p.parseHexRune()
	if !ok {
		return 0, false
	}
	if first < 0xD800 || first > 0xDFFF {
		return first, true
	}
	if first > 0xDBFF || p.pos+6 > len(p.data) || p.data[p.pos] != '\\' || p.data[p.pos+1] != 'u' {
		return utf8.RuneError, true
	}
	p.pos += 2
	second, ok := p.parseHexRune()
	if !ok || second < 0xDC00 || second > 0xDFFF {
		return utf8.RuneError, ok
	}
	return utf16.DecodeRune(first, second), true
}

func (p *partialJSONParser) parseHexRune() (rune, bool) {
	if p.pos+4 > len(p.data) {
		p.pos = len(p.data)
		return 0, false
	}
	var value rune
	for i := 0; i < 4; i++ {
		digit := p.data[p.pos+i]
		value <<= 4
		switch {
		case digit >= '0' && digit <= '9':
			value += rune(digit - '0')
		case digit >= 'a' && digit <= 'f':
			value += rune(digit-'a') + 10
		case digit >= 'A' && digit <= 'F':
			value += rune(digit-'A') + 10
		default:
			p.pos += i
			return 0, false
		}
	}
	p.pos += 4
	return value, true
}

func (p *partialJSONParser) skipSpace() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.pos++
		default:
			return
		}
	}
}

func (p *partialJSONParser) skipPrimitive() {
	for p.pos < len(p.data) {
		switch p.data[p.pos] {
		case ',', ']', '}', ' ', '\t', '\r', '\n':
			return
		default:
			p.pos++
		}
	}
}

func isAssistantJSONField(key string) bool {
	for _, candidate := range assistantJSONFieldPriority {
		if key == candidate {
			return true
		}
	}
	return false
}

func selectPartialAssistantField(values map[string]string) string {
	for _, key := range assistantJSONFieldPriority {
		if value, ok := values[key]; ok {
			if value != "" || key == "content" || key == "output" || key == "choices" || key == "candidates" {
				return value
			}
		}
	}
	return ""
}

func streamChunkHasPayload(chunk []byte) bool {
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) == 0 || bytes.HasPrefix(trimmed, []byte(":")) {
		return false
	}
	return true
}

func extractAssistantText(body []byte, headers http.Header) string {
	if len(body) == 0 {
		return ""
	}
	contentType := ""
	if headers != nil {
		contentType = strings.ToLower(headers.Get("Content-Type"))
	}
	if strings.Contains(contentType, "text/event-stream") || looksLikeSSEBody(body) {
		var out strings.Builder
		for _, payload := range sseDataPayloads(body) {
			if payload == "" || payload == "[DONE]" {
				continue
			}
			var value any
			if json.Unmarshal([]byte(payload), &value) == nil {
				appendAssistantJSON(&out, value)
			} else if trimmed := strings.TrimSpace(payload); strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				// A prefix can end in the middle of an SSE event or JSON string.
				// Parse only assistant-bearing fields so metadata does not trigger
				// an assistant_text rule while the event is incomplete.
				out.WriteString(extractPartialJSONAssistantText([]byte(trimmed)))
			} else {
				out.WriteString(payload)
			}
		}
		if out.Len() > 0 {
			return out.String()
		}
	}
	var value any
	if json.Unmarshal(body, &value) == nil {
		var out strings.Builder
		appendAssistantJSON(&out, value)
		return out.String()
	}
	return string(body)
}

func appendAssistantJSON(out *strings.Builder, value any) {
	object, ok := value.(map[string]any)
	if !ok {
		return
	}
	// Responses incremental event.
	if eventType, _ := object["type"].(string); strings.Contains(eventType, "output_text") {
		if delta, _ := object["delta"].(string); delta != "" {
			out.WriteString(delta)
		}
		if text, _ := object["text"].(string); text != "" {
			out.WriteString(text)
		}
	}
	if choices, ok := object["choices"].([]any); ok {
		for _, item := range choices {
			choice, _ := item.(map[string]any)
			appendMessageText(out, choice["message"])
			appendMessageText(out, choice["delta"])
			if text, _ := choice["text"].(string); text != "" {
				out.WriteString(text)
			}
		}
	}
	if content, ok := object["content"].([]any); ok {
		appendContentParts(out, content)
	}
	if delta, ok := object["delta"].(map[string]any); ok {
		if text, _ := delta["text"].(string); text != "" {
			out.WriteString(text)
		}
	}
	if output, ok := object["output"].([]any); ok {
		for _, item := range output {
			message, _ := item.(map[string]any)
			if content, ok := message["content"].([]any); ok {
				appendContentParts(out, content)
			}
		}
	}
	if text, _ := object["output_text"].(string); text != "" {
		out.WriteString(text)
	}
	if candidates, ok := object["candidates"].([]any); ok {
		for _, item := range candidates {
			candidate, _ := item.(map[string]any)
			content, _ := candidate["content"].(map[string]any)
			if parts, ok := content["parts"].([]any); ok {
				appendContentParts(out, parts)
			}
		}
	}
}

func appendMessageText(out *strings.Builder, value any) {
	message, ok := value.(map[string]any)
	if !ok {
		return
	}
	switch content := message["content"].(type) {
	case string:
		out.WriteString(content)
	case []any:
		appendContentParts(out, content)
	}
}

func appendContentParts(out *strings.Builder, parts []any) {
	for _, item := range parts {
		part, _ := item.(map[string]any)
		kind, _ := part["type"].(string)
		if kind != "" && !strings.Contains(kind, "text") {
			continue
		}
		if text, _ := part["text"].(string); text != "" {
			out.WriteString(text)
		}
	}
}

func extractResponseErrorMessage(body []byte, headers http.Header) string {
	if len(body) == 0 {
		return ""
	}
	if looksLikeSSEBody(body) || (headers != nil && strings.Contains(strings.ToLower(headers.Get("Content-Type")), "text/event-stream")) {
		for _, payload := range sseDataPayloads(body) {
			if msg := extractResponseErrorMessage([]byte(payload), nil); msg != "" {
				return msg
			}
			trimmed := strings.TrimSpace(payload)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				if msg := extractPartialJSONErrorMessage([]byte(trimmed)); msg != "" {
					return msg
				}
			}
		}
		return ""
	}
	var object map[string]any
	if json.Unmarshal(body, &object) != nil {
		return ""
	}
	if errObj, ok := object["error"].(map[string]any); ok {
		if message, _ := errObj["message"].(string); message != "" {
			return message
		}
		if detail, _ := errObj["detail"].(string); detail != "" {
			return detail
		}
	}
	if message, _ := object["message"].(string); message != "" {
		return message
	}
	return ""
}

func looksLikeSSEBody(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	return bytes.HasPrefix(trimmed, []byte("data:")) || bytes.HasPrefix(trimmed, []byte("event:")) || bytes.HasPrefix(trimmed, []byte(":"))
}

func sseDataPayloads(body []byte) []string {
	normalized := strings.ReplaceAll(string(body), "\r\n", "\n")
	events := strings.Split(normalized, "\n\n")
	result := make([]string, 0, len(events))
	for _, event := range events {
		var data strings.Builder
		for _, line := range strings.Split(event, "\n") {
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			if data.Len() > 0 {
				data.WriteByte('\n')
			}
			data.WriteString(strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
		if data.Len() > 0 {
			result = append(result, data.String())
		}
	}
	return result
}
