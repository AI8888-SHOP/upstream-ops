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
	// Responses may emit lifecycle-only SSE frames for a long time before the
	// first client-visible event. Bound both the aggregate gate buffer and an
	// incomplete classifier frame so a malformed or metadata-heavy stream
	// cannot grow attempt-local memory without limit.
	maxResponsesPreCommitBytes = maxValidationPrefixBytes + postCommitValidationBytes
	maxResponsesClassifyBytes  = 64 << 10
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
		if result := matchCompiledResponseRule(rule, raw, assistant, errorMessage, postCommit); result.IsRejected() {
			return result
		}
	}
	return acceptedValidation()
}

func (v *responseValidator) matchCompiled(rules []compiledResponseRule, raw, assistant, errorMessage []byte, postCommit bool) validationResult {
	if !v.Enabled() {
		return acceptedValidation()
	}
	for _, rule := range rules {
		if result := matchCompiledResponseRule(rule, raw, assistant, errorMessage, postCommit); result.IsRejected() {
			return result
		}
	}
	return acceptedValidation()
}

func matchCompiledResponseRule(rule compiledResponseRule, raw, assistant, errorMessage []byte, postCommit bool) validationResult {
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
		return acceptedValidation()
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

// streamResponseValidator validates the configured prefix before a stream is
// exposed. After the prefix is accepted, later bytes are retained only for a
// single end-of-stream audit; they never switch an already-visible response.
type streamResponseValidator struct {
	validator      *responseValidator
	protocolName   string
	model          string
	rules          []compiledResponseRule
	needsAssistant bool
	needsError     bool
	firstContentAt time.Time
	commitEligible bool
	timeoutElapsed bool
	terminalSeen   bool
	bytesSeen      int
	prefixReady    bool
	committed      bool
	prefixRaw      []byte
	postRaw        []byte
	postStart      int
	prefixDirty    bool
	preCommitDirty bool
	prefixResult   validationResult
	postAuditDone  bool
	result         validationResult
	postResult     validationResult
	responsesSSE   responsesSSEClassifier
	responsesPrefixOverflow bool
}

func (v *responseValidator) NewStreamValidator(protocolName, model string) *streamResponseValidator {
	protocolName = strings.TrimSpace(protocolName)
	model = strings.TrimSpace(model)
	s := &streamResponseValidator{
		validator: v, protocolName: protocolName, model: model,
		prefixResult: pendingValidation(), result: pendingValidation(),
	}
	if v != nil && v.StreamEnabled() {
		for _, rule := range v.rules {
			if !responseRuleApplies(rule, protocolName, model) {
				continue
			}
			s.rules = append(s.rules, rule)
			switch rule.Target {
			case "assistant_text":
				s.needsAssistant = true
			case "error_message":
				s.needsError = true
			}
		}
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
	if s.prefixReady || s.committed {
		s.appendPost(chunk)
		return s.currentResult()
	}
	observation := s.observeResponsesSSE(chunk)
	if observation.overflow {
		s.responsesPrefixOverflow = true
		return pendingValidation()
	}
	if observation.commitEligible {
		s.commitEligible = true
	}
	if observation.commitEligible && s.firstContentAt.IsZero() {
		// Lifecycle metadata is not client-visible output. Start the configured
		// validation window only when a content or terminal frame arrives.
		s.firstContentAt = time.Now()
	}
	if observation.terminal {
		s.terminalSeen = true
	}
	remaining := s.validator.PrefixBytes() - len(s.prefixRaw)
	if remaining < 0 {
		remaining = 0
	}
	prefixLen := len(chunk)
	if prefixLen > remaining {
		prefixLen = remaining
	}
	if prefixLen > 0 {
		s.prefixRaw = append(s.prefixRaw, chunk[:prefixLen]...)
		s.prefixDirty = true
		s.preCommitDirty = true
		if result := s.matchPrefix(); result.IsRejected() {
			s.result = result
			return result
		}
	}
	if prefixLen < len(chunk) {
		s.appendPost(chunk[prefixLen:])
	}
	s.bytesSeen += observation.contentBytes
	// Check every complete Responses content or terminal frame while all bytes
	// are still held. Waiting for the release threshold can miss a retryable
	// frame when an earlier lifecycle event consumed the raw prefix.
	if len(observation.candidateRaw) > 0 {
		if result := s.matchPreCommitBuffered(); result.IsRejected() {
			s.result = result
			return result
		}
		if result := s.matchPreCommitCandidate(observation.candidateRaw); result.IsRejected() {
			s.result = result
			return result
		}
	}
	if (s.bytesSeen >= s.validator.PrefixBytes() || s.timeoutElapsed || s.terminalSeen) && s.canCommitPrefix() {
		if result := s.matchPreCommitBuffered(); result.IsRejected() {
			s.result = result
			return result
		}
		s.acceptPrefix()
	}
	return s.currentResult()
}

func (s *streamResponseValidator) Ready(now time.Time) validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if s.responsesPrefixOverflow {
		return pendingValidation()
	}
	if s.result.IsRejected() {
		return s.result
	}
	if s.prefixReady {
		return acceptedValidation()
	}
	if s.bytesSeen >= s.validator.PrefixBytes() ||
		(!s.firstContentAt.IsZero() && !now.Before(s.firstContentAt.Add(s.validator.PrefixTimeout()))) {
		s.timeoutElapsed = true
		if !s.canCommitPrefix() {
			return pendingValidation()
		}
		if result := s.matchPreCommitBuffered(); result.IsRejected() {
			s.result = result
			return result
		}
		return s.acceptPrefix()
	}
	return pendingValidation()
}

func (s *streamResponseValidator) Finalize() validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if s.responsesPrefixOverflow {
		return pendingValidation()
	}
	if s.result.IsRejected() {
		return s.result
	}
	if result := s.matchPreCommitBuffered(); result.IsRejected() {
		s.result = result
		return result
	}
	return s.acceptPrefix()
}

// AuditPostCommit performs the informational post-commit check at most once.
// A post-commit match is never a route switch signal because the response may
// already have been exposed to the client.
func (s *streamResponseValidator) AuditPostCommit() validationResult {
	if s == nil || s.validator == nil || !s.validator.StreamEnabled() {
		return acceptedValidation()
	}
	if s.postAuditDone {
		if s.postResult.IsRejected() {
			return s.postResult
		}
		return acceptedValidation()
	}
	s.postAuditDone = true
	if len(s.postRaw) == 0 {
		return acceptedValidation()
	}
	combined := make([]byte, 0, len(s.prefixRaw)+len(s.postRaw))
	combined = append(combined, s.prefixRaw...)
	combined = s.appendPostRawTo(combined)
	var assistant, errorMessage []byte
	if s.needsAssistant {
		assistant = []byte(extractAssistantText(combined, http.Header{"Content-Type": []string{"text/event-stream"}}))
	}
	if s.needsError {
		errorMessage = []byte(extractResponseErrorMessage(combined, nil))
	}
	result := s.validator.matchCompiled(s.rules, combined, assistant, errorMessage, true)
	if result.IsRejected() {
		s.postResult = result
	}
	return result
}

func (s *streamResponseValidator) Commit() {
	if s == nil {
		return
	}
	s.prefixReady = true
	s.committed = true
	s.responsesSSE.reset()
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

func (s *streamResponseValidator) acceptPrefix() validationResult {
	s.prefixReady = true
	s.result = acceptedValidation()
	s.responsesSSE.reset()
	return s.result
}

func (s *streamResponseValidator) requiresCommitEligiblePayload() bool {
	return s != nil && strings.EqualFold(strings.TrimSpace(s.protocolName), "openai_responses")
}

func (s *streamResponseValidator) canCommitPrefix() bool {
	return !s.requiresCommitEligiblePayload() || s.commitEligible
}

func (s *streamResponseValidator) holdsResponsesPrefix() bool {
	return s != nil && s.requiresCommitEligiblePayload() && !s.prefixReady && !s.committed && !s.result.IsRejected()
}

func (s *streamResponseValidator) responsesPreCommitOverflow() bool {
	return s != nil && s.responsesPrefixOverflow
}

func (s *streamResponseValidator) matchPrefix() validationResult {
	if !s.prefixDirty {
		return s.prefixResult
	}
	var assistant, errorMessage []byte
	if s.needsAssistant {
		assistant = []byte(extractAssistantText(s.prefixRaw, http.Header{"Content-Type": []string{"text/event-stream"}}))
	}
	if s.needsError {
		errorMessage = []byte(extractResponseErrorMessage(s.prefixRaw, nil))
	}
	s.prefixResult = s.validator.matchCompiled(s.rules, s.prefixRaw, assistant, errorMessage, false)
	s.prefixDirty = false
	if len(s.postRaw) == 0 {
		s.preCommitDirty = false
	}
	return s.prefixResult
}

func (s *streamResponseValidator) matchPreCommitBuffered() validationResult {
	if len(s.postRaw) == 0 {
		return s.matchPrefix()
	}
	if !s.preCommitDirty {
		return s.prefixResult
	}
	combined := make([]byte, 0, len(s.prefixRaw)+len(s.postRaw))
	combined = append(combined, s.prefixRaw...)
	combined = s.appendPostRawTo(combined)
	var assistant, errorMessage []byte
	if s.needsAssistant {
		assistant = []byte(extractAssistantText(combined, http.Header{"Content-Type": []string{"text/event-stream"}}))
	}
	if s.needsError {
		errorMessage = []byte(extractResponseErrorMessage(combined, nil))
	}
	s.prefixResult = s.validator.matchCompiled(s.rules, combined, assistant, errorMessage, false)
	s.prefixDirty = false
	s.preCommitDirty = false
	return s.prefixResult
}

func (s *streamResponseValidator) matchPreCommitCandidate(raw []byte) validationResult {
	if len(raw) == 0 {
		return acceptedValidation()
	}
	var assistant, errorMessage []byte
	if s.needsAssistant {
		assistant = []byte(extractAssistantText(raw, http.Header{"Content-Type": []string{"text/event-stream"}}))
	}
	if s.needsError {
		errorMessage = []byte(extractResponseErrorMessage(raw, nil))
	}
	return s.validator.matchCompiled(s.rules, raw, assistant, errorMessage, false)
}

func (s *streamResponseValidator) appendPost(chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	s.preCommitDirty = true
	if len(chunk) >= postCommitValidationBytes {
		if cap(s.postRaw) < postCommitValidationBytes {
			s.postRaw = make([]byte, postCommitValidationBytes)
		} else {
			s.postRaw = s.postRaw[:postCommitValidationBytes]
		}
		copy(s.postRaw, chunk[len(chunk)-postCommitValidationBytes:])
		s.postStart = 0
		return
	}
	if len(s.postRaw) < postCommitValidationBytes {
		available := postCommitValidationBytes - len(s.postRaw)
		if available > len(chunk) {
			available = len(chunk)
		}
		s.postRaw = append(s.postRaw, chunk[:available]...)
		chunk = chunk[available:]
		if len(chunk) == 0 {
			return
		}
	}
	first := copy(s.postRaw[s.postStart:], chunk)
	copy(s.postRaw, chunk[first:])
	s.postStart = (s.postStart + len(chunk)) % postCommitValidationBytes
}

func (s *streamResponseValidator) appendPostRawTo(destination []byte) []byte {
	if len(s.postRaw) < postCommitValidationBytes || s.postStart == 0 {
		return append(destination, s.postRaw...)
	}
	destination = append(destination, s.postRaw[s.postStart:]...)
	return append(destination, s.postRaw[:s.postStart]...)
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
	return extractPartialJSONErrorMessageDepth(data, 0)
}

func extractPartialJSONErrorMessageDepth(data []byte, depth int) string {
	if depth > 4 {
		return ""
	}
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
	for _, key := range []string{"response", "incomplete_details"} {
		value, ok := partialJSONRootMember(data, key)
		if !ok {
			continue
		}
		if text := extractPartialJSONErrorMessageDepth(value, depth+1); text != "" {
			return text
		}
	}
	if value, ok := partialJSONRootMember(data, "reason"); ok {
		parser := partialJSONParser{data: value}
		if text := parser.parseValue(true, 0); text != "" {
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

type responsesSSEObservation struct {
	commitEligible bool
	contentBytes   int
	terminal       bool
	candidateRaw   []byte
	overflow       bool
}

type responsesSSEFrameClass uint8

const (
	responsesSSEFrameEmpty responsesSSEFrameClass = iota
	responsesSSEFrameLifecycle
	responsesSSEFrameContent
	responsesSSEFrameTerminal
)

// responsesSSEClassifier owns the incomplete frame across Write calls. SSE
// frame boundaries are transport-independent, so one Write may contain a
// partial frame, one frame, or several frames.
type responsesSSEClassifier struct {
	pending  []byte
	scanFrom int
}

func (p *responsesSSEClassifier) reset() {
	if p == nil {
		return
	}
	p.pending = nil
	p.scanFrom = 0
}

func (s *streamResponseValidator) observeResponsesSSE(chunk []byte) responsesSSEObservation {
	if !s.requiresCommitEligiblePayload() {
		if streamChunkHasPayload(chunk) {
			return responsesSSEObservation{commitEligible: true, contentBytes: len(chunk)}
		}
		return responsesSSEObservation{}
	}
	return s.responsesSSE.consume(chunk)
}

func (p *responsesSSEClassifier) consume(chunk []byte) (observation responsesSSEObservation) {
	for len(chunk) > 0 {
		available := maxResponsesPreCommitBytes - len(p.pending)
		if available <= 0 {
			observation.overflow = true
			p.reset()
			return observation
		}
		take := len(chunk)
		if take > available {
			take = available
		}
		p.pending = append(p.pending, chunk[:take]...)
		chunk = chunk[take:]
		p.classifyAvailable(&observation)
		if len(p.pending) >= maxResponsesPreCommitBytes {
			observation.overflow = true
			p.reset()
			return observation
		}
	}
	return observation
}

func (p *responsesSSEClassifier) classifyAvailable(observation *responsesSSEObservation) {
	if p == nil || observation == nil || len(p.pending) == 0 {
		return
	}
	consumed := 0
	searchFrom := p.scanFrom
	if searchFrom < 0 || searchFrom > len(p.pending) {
		searchFrom = 0
	}
	for {
		frameEnd, separatorBytes := nextSSEFrameBoundary(p.pending, searchFrom)
		if frameEnd < 0 {
			break
		}
		frame := p.pending[consumed:frameEnd]
		class := classifyResponsesSSEFrame(frame)
		frameBytes := frameEnd + separatorBytes - consumed
		switch class {
		case responsesSSEFrameTerminal:
			observation.terminal = true
			observation.commitEligible = true
			observation.contentBytes += frameBytes
			observation.candidateRaw = appendResponsesCandidateRaw(observation.candidateRaw, frame)
		case responsesSSEFrameContent:
			observation.commitEligible = true
			observation.contentBytes += frameBytes
			observation.candidateRaw = appendResponsesCandidateRaw(observation.candidateRaw, frame)
		}
		consumed = frameEnd + separatorBytes
		searchFrom = consumed
	}
	if consumed > 0 {
		copy(p.pending, p.pending[consumed:])
		p.pending = p.pending[:len(p.pending)-consumed]
	}
	// A separator is at most four bytes, so only this suffix can combine
	// with the next chunk to form a new frame boundary.
	p.scanFrom = len(p.pending) - 3
	if p.scanFrom < 0 {
		p.scanFrom = 0
	}
}

func appendResponsesCandidateRaw(destination, frame []byte) []byte {
	if len(destination) >= maxResponsesPreCommitBytes || len(frame) == 0 {
		return destination
	}
	if len(destination) > 0 {
		remaining := maxResponsesPreCommitBytes - len(destination)
		if remaining < 2 {
			return destination
		}
		destination = append(destination, '\n', '\n')
	}
	remaining := maxResponsesPreCommitBytes - len(destination)
	if remaining > len(frame) {
		remaining = len(frame)
	}
	return append(destination, frame[:remaining]...)
}

func nextSSEFrameBoundary(data []byte, from int) (int, int) {
	if from < 0 {
		from = 0
	}
	if from >= len(data) {
		return -1, 0
	}
	bestIndex, bestSize := -1, 0
	for _, separator := range [][]byte{[]byte("\r\n\r\n"), []byte("\n\r\n"), []byte("\n\n"), []byte("\r\r")} {
		if offset := bytes.Index(data[from:], separator); offset >= 0 {
			index := from + offset
			if bestIndex < 0 || index < bestIndex {
				bestIndex = index
				bestSize = len(separator)
			}
		}
	}
	return bestIndex, bestSize
}

func classifyResponsesSSEFrame(frame []byte) responsesSSEFrameClass {
	var eventName string
	data := make([]byte, 0, minResponseInt(len(frame), maxResponsesClassifyBytes))
	sawData := false
	malformed := false
	lines := bytes.Split(frame, []byte{'\n'})
	if len(lines) == 1 && bytes.Contains(frame, []byte{'\r'}) {
		lines = bytes.Split(frame, []byte{'\r'})
	}
	for _, rawLine := range lines {
		line := bytes.TrimSuffix(rawLine, []byte{'\r'})
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		field, value, found := bytes.Cut(line, []byte{':'})
		if !found {
			malformed = true
			continue
		}
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		switch string(field) {
		case "event":
			eventName = strings.TrimSpace(string(value))
		case "data":
			if sawData && len(data) < maxResponsesClassifyBytes {
				data = append(data, '\n')
			}
			sawData = true
			remaining := maxResponsesClassifyBytes - len(data)
			if remaining > len(value) {
				remaining = len(value)
			}
			if remaining > 0 {
				data = append(data, value[:remaining]...)
			}
		case "id", "retry":
			// SSE control fields do not make a Responses frame visible.
		default:
			malformed = true
		}
	}

	payload := bytes.TrimSpace(data)
	if bytes.Equal(payload, []byte("[DONE]")) {
		return responsesSSEFrameTerminal
	}
	dataType := responsesPayloadType(payload)
	if isResponsesTerminalEvent(eventName) || isResponsesTerminalEvent(dataType) {
		return responsesSSEFrameTerminal
	}
	if eventName != "" && !isResponsesLifecycleEvent(eventName) {
		return responsesSSEFrameContent
	}
	if dataType != "" && !isResponsesLifecycleEvent(dataType) {
		return responsesSSEFrameContent
	}
	if eventName != "" || dataType != "" {
		return responsesSSEFrameLifecycle
	}
	if sawData || malformed {
		// A complete but unknown/malformed frame must not deadlock the gate.
		return responsesSSEFrameContent
	}
	return responsesSSEFrameEmpty
}

func responsesPayloadType(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	if value, ok := partialJSONRootMember(payload, "type"); ok {
		parser := partialJSONParser{data: value}
		parser.skipSpace()
		if eventType, complete := parser.parseString(); complete {
			return strings.TrimSpace(eventType)
		}
	}
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(payload, &envelope) == nil {
		return strings.TrimSpace(envelope.Type)
	}
	return ""
}

func isResponsesLifecycleEvent(eventType string) bool {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "response.created", "response.in_progress", "response.queued",
		"response.output_item.added", "response.content_part.added",
		"response.reasoning_summary_part.added":
		return true
	default:
		return false
	}
}

func isResponsesTerminalEvent(eventType string) bool {
	switch strings.ToLower(strings.TrimSpace(eventType)) {
	case "error", "response.error", "response.failed", "response.incomplete",
		"response.completed", "response.done", "response.canceled", "response.cancelled":
		return true
	default:
		return false
	}
}

func minResponseInt(left, right int) int {
	if left < right {
		return left
	}
	return right
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
	return extractErrorMessageObject(object)
}

func extractErrorMessageObject(object map[string]any) string {
	if object == nil {
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
	// OpenAI Responses terminal events keep the API error under
	// response.error rather than at the event root.
	if response, ok := object["response"].(map[string]any); ok {
		if message := extractErrorMessageObject(response); message != "" {
			return message
		}
	}
	if details, ok := object["incomplete_details"].(map[string]any); ok {
		if reason, _ := details["reason"].(string); reason != "" {
			return reason
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
