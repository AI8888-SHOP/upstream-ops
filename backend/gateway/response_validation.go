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
	initialValidationScanBytes     = 64
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

// responseRulePrefilters are only a cheap "could this target match?" gate.
// The original ordered rule loop remains authoritative so a prefilter can
// never change rule priority, selectors, or the matched text recorded in the
// validation result.
type responseRulePrefilters map[string]*regexp.Regexp

type responsePrefilterMatches struct {
	rawBody       bool
	assistantText bool
	errorMessage  bool
}

type responseValidator struct {
	enabled       bool
	streamMode    string
	prefixBytes   int
	prefixTimeout time.Duration
	rules         []compiledResponseRule
	prefilters    responseRulePrefilters
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
	v.prefilters = buildResponseRulePrefilters(v.rules)
	return v, nil
}

// buildResponseRulePrefilters combines rules by target so a no-match stream
// does one RE2 scan per target instead of scanning every rule independently.
// Each branch is wrapped in its own capture group because Go's regexp package
// does not support non-capturing groups. The capture values are deliberately
// ignored; this regexp is only a boolean prefilter.
func buildResponseRulePrefilters(rules []compiledResponseRule) responseRulePrefilters {
	branches := make(map[string][]string)
	for _, rule := range rules {
		if rule.re == nil || strings.TrimSpace(rule.Pattern) == "" {
			continue
		}
		branches[rule.Target] = append(branches[rule.Target], "("+rule.Pattern+")")
	}
	prefilters := make(responseRulePrefilters, len(branches))
	for target, targetBranches := range branches {
		// A single rule would be scanned twice (prefilter + authoritative
		// match), so only build a prefilter when it removes real duplication.
		if len(targetBranches) < 2 {
			continue
		}
		combined, err := regexp.Compile(strings.Join(targetBranches, "|"))
		if err != nil {
			// Individual rules have already compiled successfully. A combined
			// expression can still be rejected by a regexp program-size or
			// capture-name limit; simply fall back to the original path.
			continue
		}
		prefilters[target] = combined
	}
	return prefilters
}

func (v *responseValidator) prefilterMatches(raw, assistant, errorMessage []byte) responsePrefilterMatches {
	if v == nil {
		return responsePrefilterMatches{rawBody: true, assistantText: true, errorMessage: true}
	}
	return prefilterMatches(v.prefilters, raw, assistant, errorMessage)
}

func prefilterMatches(prefilters responseRulePrefilters, raw, assistant, errorMessage []byte) responsePrefilterMatches {
	possible := responsePrefilterMatches{rawBody: true, assistantText: true, errorMessage: true}
	if prefilter := prefilters["raw_body"]; prefilter != nil {
		possible.rawBody = prefilter.Match(raw)
	}
	if prefilter := prefilters["assistant_text"]; prefilter != nil {
		possible.assistantText = prefilter.Match(assistant)
	}
	if prefilter := prefilters["error_message"]; prefilter != nil {
		possible.errorMessage = prefilter.Match(errorMessage)
	}
	return possible
}

func (p responsePrefilterMatches) allows(target string) bool {
	switch target {
	case "raw_body":
		return p.rawBody
	case "assistant_text":
		return p.assistantText
	case "error_message":
		return p.errorMessage
	default:
		return true
	}
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
	possible := v.prefilterMatches(raw, assistant, errorMessage)
	for _, rule := range v.rules {
		if !responseRuleApplies(rule, protocolName, model) {
			continue
		}
		if !possible.allows(rule.Target) {
			continue
		}
		if result := matchCompiledResponseRule(rule, raw, assistant, errorMessage, postCommit); result.IsRejected() {
			return result
		}
	}
	return acceptedValidation()
}

func (v *responseValidator) matchCompiled(rules []compiledResponseRule, raw, assistant, errorMessage []byte, postCommit bool) validationResult {
	return v.matchCompiledWithPrefilters(rules, v.prefilters, raw, assistant, errorMessage, postCommit)
}

// matchCompiledWithPrefilters is the stream-aware variant of matchCompiled.
// A stream usually has only a subset of the gateway rules (after protocol and
// model selectors are applied), so using a prefilter built for that subset
// avoids scanning unrelated expressions on every SSE chunk.
func (v *responseValidator) matchCompiledWithPrefilters(rules []compiledResponseRule, prefilters responseRulePrefilters, raw, assistant, errorMessage []byte, postCommit bool) validationResult {
	if !v.Enabled() {
		return acceptedValidation()
	}
	possible := prefilterMatches(prefilters, raw, assistant, errorMessage)
	for _, rule := range rules {
		if !possible.allows(rule.Target) {
			continue
		}
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
	// Empty candidates remain valid inputs: a rule such as ^$ deliberately
	// matches an empty upstream response.
	if !rule.re.Match(candidate) {
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
	validator               *responseValidator
	protocolName            string
	model                   string
	rules                   []compiledResponseRule
	needsAssistant          bool
	needsError              bool
	firstContentAt          time.Time
	commitEligible          bool
	timeoutElapsed          bool
	terminalSeen            bool
	bytesSeen               int
	prefixReady             bool
	committed               bool
	prefixRaw               []byte
	postRaw                 []byte
	postStart               int
	prefixDirty             bool
	preCommitDirty          bool
	preCommitBytesSeen      int
	nextPreCommitScanBytes  int
	scanEveryChunk          bool
	prefixResult            validationResult
	postAuditDone           bool
	result                  validationResult
	postResult              validationResult
	prefixCandidates        streamPrefixCandidateCache
	prefilters              responseRulePrefilters
	responsesSSE            responsesSSEClassifier
	responsesPrefixOverflow bool
}

// streamPrefixCandidateCache avoids parsing the same prefix more than once
// when the gate asks for a result repeatedly without appending new bytes.
// prefixRaw is append-only for the lifetime of a pre-commit stream, so its
// length is a stable identity for the cached extraction.
type streamPrefixCandidateCache struct {
	valid        bool
	rawLen       int
	assistant    []byte
	errorMessage []byte
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
			if responseRuleNeedsEveryChunkScan(rule.Pattern) {
				s.scanEveryChunk = true
			}
			switch rule.Target {
			case "assistant_text":
				s.needsAssistant = true
			case "error_message":
				s.needsError = true
			}
		}
		// Reuse the validator-level compiled prefilters.  Recompiling a combined
		// regexp for every upstream attempt costs more CPU than the scan it is
		// meant to avoid.  Restrict the map to targets used by this stream so
		// inactive target payloads are not parsed at all.
		if len(v.prefilters) > 0 {
			s.prefilters = make(responseRulePrefilters, len(v.prefilters))
			for _, rule := range s.rules {
				if prefilter := v.prefilters[rule.Target]; prefilter != nil {
					s.prefilters[rule.Target] = prefilter
				}
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
	s.preCommitBytesSeen += len(chunk)
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
	}
	if prefixLen < len(chunk) {
		s.appendPost(chunk[prefixLen:])
	}
	s.bytesSeen += observation.contentBytes
	if s.shouldScanPreCommit() {
		if result := s.matchPreCommitBuffered(); result.IsRejected() {
			s.result = result
			return result
		}
	}
	// Check every complete Responses content or terminal frame while all bytes
	// are still held. First scan the isolated candidate, which is normally much
	// smaller than the accumulated prefix. Only a possible match pays for the
	// authoritative full-buffer scan needed to preserve global rule priority.
	if len(observation.candidateRaw) > 0 {
		if result := s.matchPreCommitCandidate(observation.candidateRaw); result.IsRejected() {
			if fullResult := s.matchPreCommitBuffered(); fullResult.IsRejected() {
				s.result = fullResult
				return fullResult
			}
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
	result := s.validator.matchCompiledWithPrefilters(s.rules, s.prefilters, combined, assistant, errorMessage, true)
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
	assistant, errorMessage := s.cachedPrefixCandidates()
	s.prefixResult = s.validator.matchCompiledWithPrefilters(s.rules, s.prefilters, s.prefixRaw, assistant, errorMessage, false)
	s.prefixDirty = false
	s.markPreCommitScanned()
	if len(s.postRaw) == 0 {
		s.preCommitDirty = false
	}
	return s.prefixResult
}

func (s *streamResponseValidator) cachedPrefixCandidates() (assistant, errorMessage []byte) {
	if s == nil {
		return nil, nil
	}
	if cache := &s.prefixCandidates; cache.valid && cache.rawLen == len(s.prefixRaw) {
		return cache.assistant, cache.errorMessage
	}
	cache := streamPrefixCandidateCache{valid: true, rawLen: len(s.prefixRaw)}
	if s.needsAssistant {
		cache.assistant = []byte(extractAssistantText(s.prefixRaw, http.Header{"Content-Type": []string{"text/event-stream"}}))
	}
	if s.needsError {
		cache.errorMessage = []byte(extractResponseErrorMessage(s.prefixRaw, nil))
	}
	s.prefixCandidates = cache
	return cache.assistant, cache.errorMessage
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
	s.prefixResult = s.validator.matchCompiledWithPrefilters(s.rules, s.prefilters, combined, assistant, errorMessage, false)
	s.prefixDirty = false
	s.preCommitDirty = false
	s.markPreCommitScanned()
	return s.prefixResult
}

// shouldScanPreCommit keeps stable no-match streams near-linear: the
// complete accumulated prefix is scanned when it first arrives and then only
// after crossing 64, 128, 256, ... bytes. Ready, Finalize, and the commit path
// still call matchPreCommitBuffered directly, so no bytes can be released
// without one final authoritative scan.
func (s *streamResponseValidator) shouldScanPreCommit() bool {
	if s == nil || !s.preCommitDirty {
		return false
	}
	return s.scanEveryChunk || s.nextPreCommitScanBytes == 0 || s.preCommitBytesSeen >= s.nextPreCommitScanBytes
}

func (s *streamResponseValidator) markPreCommitScanned() {
	if s == nil {
		return
	}
	threshold := initialValidationScanBytes
	for threshold <= s.preCommitBytesSeen && threshold < maxResponsesPreCommitBytes {
		threshold <<= 1
	}
	if threshold <= s.preCommitBytesSeen {
		threshold = s.preCommitBytesSeen + 1
	}
	s.nextPreCommitScanBytes = threshold
}

// Matches involving the end of the currently buffered text can be transient:
// for example, blocked$ matches one write and stops matching after the next.
// Keep exact per-write semantics for these uncommon rules while batching the
// ordinary phrase-matching rules used for capacity and overload detection.
func responseRuleNeedsEveryChunkScan(pattern string) bool {
	return strings.Contains(pattern, "$") || strings.Contains(pattern, `\z`) ||
		strings.Contains(pattern, `\b`) || strings.Contains(pattern, `\B`)
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
	return s.validator.matchCompiledWithPrefilters(s.rules, s.prefilters, raw, assistant, errorMessage, false)
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
	text, _ := extractPartialJSONAssistantTextWithShape(data)
	return text
}

func extractPartialJSONAssistantTextWithShape(data []byte) (string, bool) {
	p := partialJSONParser{data: data}
	return p.parseValue(true, 0), p.sawAssistantField
}

func extractPartialJSONErrorMessage(data []byte) string {
	text, _ := extractPartialJSONErrorMessageDepthFound(data, 0)
	return text
}

var errorJSONFieldMarkers = [...][]byte{
	[]byte(`"error"`), []byte(`"message"`), []byte(`"detail"`),
	[]byte(`"error_description"`), []byte(`"response"`),
	[]byte(`"incomplete_details"`), []byte(`"reason"`),
}

func mayContainErrorJSONField(data []byte) bool {
	for _, marker := range errorJSONFieldMarkers {
		if bytes.Contains(data, marker) {
			return true
		}
	}
	return false
}

func extractPartialJSONErrorMessageDepth(data []byte, depth int) string {
	text, _ := extractPartialJSONErrorMessageDepthFound(data, depth)
	return text
}

// extractPartialJSONErrorMessageDepthFound returns whether an error-bearing
// field was present separately from its text. That lets the SSE hot path avoid
// a full encoding/json decode for ordinary content events that cannot contain
// an error message.
func extractPartialJSONErrorMessageDepthFound(data []byte, depth int) (string, bool) {
	if depth > 4 {
		return "", false
	}
	found := false
	for _, key := range []string{"error", "message", "detail", "error_description"} {
		value, ok := partialJSONRootMember(data, key)
		if !ok {
			continue
		}
		found = true
		if key == "error" {
			if nested, nestedFound := extractPartialJSONErrorMessageDepthFound(value, depth+1); nestedFound {
				if nested != "" {
					return nested, true
				}
				found = true
			}
		}
		p := partialJSONParser{data: value}
		if text := p.parseValue(true, 0); text != "" {
			return text, true
		}
	}
	for _, key := range []string{"response", "incomplete_details"} {
		value, ok := partialJSONRootMember(data, key)
		if !ok {
			continue
		}
		found = true
		if text, nestedFound := extractPartialJSONErrorMessageDepthFound(value, depth+1); nestedFound {
			if text != "" {
				return text, true
			}
		}
	}
	if value, ok := partialJSONRootMember(data, "reason"); ok {
		found = true
		parser := partialJSONParser{data: value}
		if text := parser.parseValue(true, 0); text != "" {
			return text, true
		}
	}
	return "", found
}

type partialJSONParser struct {
	data              []byte
	pos               int
	sawAssistantField bool
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
		if childCollect {
			p.sawAssistantField = true
		}
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

// These field names are shared by every Responses SSE frame. Keeping them as
// package-level byte slices avoids constructing a new []byte for each line in
// the classifier's hot path.
var (
	sseFieldEvent = []byte("event")
	sseFieldData  = []byte("data")
	sseFieldID    = []byte("id")
	sseFieldRetry = []byte("retry")
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
	// Scan once instead of running four bytes.Index calls (and allocating a
	// temporary separator slice) for every classifier invocation. The scan
	// starts a few bytes before the end of an incomplete frame, so all valid
	// separators that cross a Write boundary are still considered.
	for index := from; index < len(data); index++ {
		switch data[index] {
		case '\r':
			if index+3 < len(data) && data[index+1] == '\n' && data[index+2] == '\r' && data[index+3] == '\n' {
				return index, 4
			}
			if index+1 < len(data) && data[index+1] == '\r' {
				return index, 2
			}
		case '\n':
			if index+2 < len(data) && data[index+1] == '\r' && data[index+2] == '\n' {
				return index, 3
			}
			if index+1 < len(data) && data[index+1] == '\n' {
				return index, 2
			}
		}
	}
	return -1, 0
}

func classifyResponsesSSEFrame(frame []byte) responsesSSEFrameClass {
	var eventName []byte
	var firstData, data []byte
	sawData := false
	malformed := false
	for offset := 0; offset < len(frame); {
		lineStart := offset
		for offset < len(frame) && frame[offset] != '\n' && frame[offset] != '\r' {
			offset++
		}
		line := frame[lineStart:offset]
		if offset < len(frame) {
			if frame[offset] == '\r' {
				offset++
				if offset < len(frame) && frame[offset] == '\n' {
					offset++
				}
			} else {
				offset++
			}
		}
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		fieldEnd := bytes.IndexByte(line, ':')
		if fieldEnd < 0 {
			malformed = true
			continue
		}
		field, value := line[:fieldEnd], line[fieldEnd+1:]
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		switch {
		case bytes.Equal(field, sseFieldEvent):
			eventName = bytes.TrimSpace(value)
		case bytes.Equal(field, sseFieldData):
			if !sawData {
				limit := minResponseInt(len(value), maxResponsesClassifyBytes)
				firstData = value[:limit:limit]
				sawData = true
				continue
			}
			// A second data line needs a private buffer because append must not
			// overwrite the source frame while it is still being classified.
			if data == nil {
				data = make([]byte, 0, minResponseInt(len(frame), maxResponsesClassifyBytes))
				data = append(data, firstData...)
			}
			if len(data) < maxResponsesClassifyBytes {
				data = append(data, '\n')
				remaining := maxResponsesClassifyBytes - len(data)
				if remaining > len(value) {
					remaining = len(value)
				}
				data = append(data, value[:remaining]...)
			}
		case bytes.Equal(field, sseFieldID), bytes.Equal(field, sseFieldRetry):
			// SSE control fields do not make a Responses frame visible.
		default:
			malformed = true
		}
	}

	if data == nil {
		data = firstData
	}
	payload := bytes.TrimSpace(data)
	if bytes.Equal(payload, []byte("[DONE]")) {
		return responsesSSEFrameTerminal
	}
	dataType := responsesPayloadTypeBytes(payload)
	if isResponsesTerminalEventBytes(eventName) || isResponsesTerminalEventBytes(dataType) {
		return responsesSSEFrameTerminal
	}
	if len(eventName) > 0 && !isResponsesLifecycleEventBytes(eventName) {
		return responsesSSEFrameContent
	}
	if len(dataType) > 0 && !isResponsesLifecycleEventBytes(dataType) {
		return responsesSSEFrameContent
	}
	if len(eventName) > 0 || len(dataType) > 0 {
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

// responsesPayloadTypeBytes extracts a root JSON string member without
// unmarshalling the complete payload. Responses lifecycle frames often carry
// large response objects; decoding those objects solely to classify the frame
// was a significant source of allocations on the streaming path. The returned
// slice aliases payload and is only valid for the duration of classification.
func responsesPayloadTypeBytes(payload []byte) []byte {
	if len(payload) == 0 {
		return nil
	}
	pos := 0
	skipJSONSpaceBytes(payload, &pos)
	if pos >= len(payload) || payload[pos] != '{' {
		return nil
	}
	pos++
	for {
		skipJSONSpaceBytes(payload, &pos)
		if pos >= len(payload) || payload[pos] == '}' {
			return nil
		}
		if payload[pos] == ',' {
			pos++
			continue
		}
		key, ok := scanJSONStringRaw(payload, &pos)
		if !ok {
			return nil
		}
		skipJSONSpaceBytes(payload, &pos)
		if pos >= len(payload) || payload[pos] != ':' {
			return nil
		}
		pos++
		skipJSONSpaceBytes(payload, &pos)
		if bytes.EqualFold(key, []byte("type")) {
			value, ok := scanJSONStringRaw(payload, &pos)
			if !ok {
				return nil
			}
			// The fast scanner intentionally keeps common ASCII values aliased
			// to the payload. Escaped keys or values need the existing JSON
			// decoder so sequences such as ty\u0070e and response.\u0066ailed
			// retain their old classification semantics.
			if bytes.IndexByte(key, '\\') >= 0 || bytes.IndexByte(value, '\\') >= 0 {
				return []byte(responsesPayloadType(payload))
			}
			return value
		}
		if bytes.IndexByte(key, '\\') >= 0 {
			return []byte(responsesPayloadType(payload))
		}
		if !skipJSONValueBytes(payload, &pos) {
			return nil
		}
	}
}

func skipJSONSpaceBytes(data []byte, pos *int) {
	for *pos < len(data) {
		switch data[*pos] {
		case ' ', '\t', '\r', '\n':
			(*pos)++
		default:
			return
		}
	}
}

// scanJSONStringRaw advances over a JSON string and returns its raw contents
// (without the surrounding quotes). Event type values are ASCII and normally
// unescaped, so retaining the raw slice avoids a string allocation.
func scanJSONStringRaw(data []byte, pos *int) ([]byte, bool) {
	if *pos >= len(data) || data[*pos] != '"' {
		return nil, false
	}
	start := *pos + 1
	*pos = start
	for *pos < len(data) {
		switch data[*pos] {
		case '"':
			value := data[start:*pos]
			(*pos)++
			return value, true
		case '\\':
			(*pos)++
			if *pos >= len(data) {
				return nil, false
			}
			(*pos)++
		default:
			(*pos)++
		}
	}
	return nil, false
}

func skipJSONValueBytes(data []byte, pos *int) bool {
	if *pos >= len(data) {
		return false
	}
	switch data[*pos] {
	case '"':
		_, ok := scanJSONStringRaw(data, pos)
		return ok
	case '{', '[':
		open := data[*pos]
		close := byte('}')
		if open == '[' {
			close = ']'
		}
		depth := 0
		inString := false
		escaped := false
		for *pos < len(data) {
			current := data[*pos]
			if inString {
				if escaped {
					escaped = false
				} else if current == '\\' {
					escaped = true
				} else if current == '"' {
					inString = false
				}
				(*pos)++
				continue
			}
			switch current {
			case '"':
				inString = true
			case open:
				depth++
			case close:
				depth--
				if depth == 0 {
					(*pos)++
					return true
				}
			}
			(*pos)++
		}
		return false
	default:
		start := *pos
		for *pos < len(data) {
			switch data[*pos] {
			case ',', '}', ']', ' ', '\t', '\r', '\n':
				return *pos > start
			default:
				(*pos)++
			}
		}
		return *pos > start
	}
}

func isResponsesLifecycleEventBytes(eventType []byte) bool {
	eventType = bytes.TrimSpace(eventType)
	switch {
	case bytes.EqualFold(eventType, []byte("response.created")),
		bytes.EqualFold(eventType, []byte("response.in_progress")),
		bytes.EqualFold(eventType, []byte("response.queued")),
		bytes.EqualFold(eventType, []byte("response.output_item.added")),
		bytes.EqualFold(eventType, []byte("response.content_part.added")),
		bytes.EqualFold(eventType, []byte("response.reasoning_summary_part.added")):
		return true
	default:
		return false
	}
}

func isResponsesTerminalEventBytes(eventType []byte) bool {
	eventType = bytes.TrimSpace(eventType)
	switch {
	case bytes.EqualFold(eventType, []byte("error")),
		bytes.EqualFold(eventType, []byte("response.error")),
		bytes.EqualFold(eventType, []byte("response.failed")),
		bytes.EqualFold(eventType, []byte("response.incomplete")),
		bytes.EqualFold(eventType, []byte("response.completed")),
		bytes.EqualFold(eventType, []byte("response.done")),
		bytes.EqualFold(eventType, []byte("response.canceled")),
		bytes.EqualFold(eventType, []byte("response.cancelled")):
		return true
	default:
		return false
	}
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
			trimmed := bytes.TrimSpace(payload)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
				continue
			}
			if text, handled := extractSSEAssistantPayload(trimmed); handled {
				out.WriteString(text)
				continue
			}
			var value any
			if json.Unmarshal(trimmed, &value) == nil {
				appendAssistantJSON(&out, value)
			} else if bytes.HasPrefix(trimmed, []byte("{")) || bytes.HasPrefix(trimmed, []byte("[")) {
				// A prefix can end in the middle of an SSE event or JSON string.
				// Parse only assistant-bearing fields so metadata does not trigger
				// an assistant_text rule while the event is incomplete.
				out.WriteString(extractPartialJSONAssistantText(trimmed))
			} else {
				out.Write(trimmed)
			}
		}
		if out.Len() > 0 {
			return out.String()
		}
		// A valid SSE stream with only metadata must not turn the entire raw
		// envelope into assistant text. Besides avoiding false regex matches,
		// this keeps lifecycle frames off the JSON decoder fallback path.
		return ""
	}
	var value any
	if json.Unmarshal(body, &value) == nil {
		var out strings.Builder
		appendAssistantJSON(&out, value)
		return out.String()
	}
	return string(body)
}

// extractSSEAssistantPayload handles the common JSON shapes without decoding
// the complete event into interface{} maps. Responses lifecycle and terminal
// events are metadata/error frames; only output_text events contribute visible
// assistant text. The bool reports that the payload shape was recognized, so
// callers can reserve json.Unmarshal for unusual provider payloads.
func extractSSEAssistantPayload(payload []byte) (string, bool) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return "", true
	}
	if payload[0] == '"' {
		parser := partialJSONParser{data: payload}
		if text, complete := parser.parseString(); complete {
			return text, true
		}
	}
	if payload[0] != '{' && payload[0] != '[' {
		return string(payload), true
	}

	if eventType := responsesPayloadTypeBytes(payload); len(eventType) > 0 {
		lowerType := strings.ToLower(string(eventType))
		if strings.HasPrefix(lowerType, "response.") {
			if strings.Contains(lowerType, "output_text") {
				text, _ := extractPartialJSONAssistantTextWithShape(payload)
				return text, true
			}
			return "", true
		}
		if isResponsesLifecycleEventBytes(eventType) || isResponsesTerminalEventBytes(eventType) {
			return "", true
		}
	}
	if text, recognized := extractPartialJSONAssistantTextWithShape(payload); recognized {
		return text, true
	}
	return "", false
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
			trimmed := bytes.TrimSpace(payload)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
				continue
			}
			if bytes.HasPrefix(trimmed, []byte("{")) || bytes.HasPrefix(trimmed, []byte("[")) {
				if bytes.HasPrefix(trimmed, []byte("{")) && !mayContainErrorJSONField(trimmed) {
					continue
				}
				if msg, found := extractPartialJSONErrorMessageDepthFound(trimmed, 0); msg != "" {
					return msg
				} else if found {
					// The payload contains an error-bearing key, but its value is
					// incomplete. Do not decode unrelated metadata; the next event
					// may carry the complete error.
					continue
				}
				if bytes.HasPrefix(trimmed, []byte("{")) {
					// A root object with none of the error-bearing fields cannot
					// produce a match. Avoid decoding every ordinary JSON delta.
					continue
				}
				// Unknown valid provider shapes are rare. Preserve compatibility
				// with the old map decoder only for those shapes.
				var object map[string]any
				if json.Unmarshal(trimmed, &object) == nil {
					if msg := extractErrorMessageObject(object); msg != "" {
						return msg
					}
				}
				continue
			}
			if msg := strings.TrimSpace(string(trimmed)); msg != "" {
				return msg
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

func sseDataPayloads(body []byte) [][]byte {
	result := make([][]byte, 0, 8)
	var first []byte
	var joined []byte
	flush := func() {
		if len(joined) > 0 {
			result = append(result, joined)
		} else if len(first) > 0 {
			result = append(result, first)
		}
		first = nil
		joined = nil
	}
	for start := 0; start <= len(body); {
		end := bytes.IndexByte(body[start:], '\n')
		lineEnd := len(body)
		next := len(body)
		if end >= 0 {
			lineEnd = start + end
			next = lineEnd + 1
		}
		line := body[start:lineEnd]
		line = bytes.TrimSuffix(line, []byte{'\r'})
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) == 0 {
			flush()
		} else if bytes.HasPrefix(trimmed, []byte("data:")) {
			value := bytes.TrimSpace(trimmed[len("data:"):])
			if len(value) > 0 {
				if first == nil {
					first = value
				} else {
					if joined == nil {
						joined = make([]byte, 0, len(first)+len(value)+1)
						joined = append(joined, first...)
					}
					joined = append(joined, '\n')
					joined = append(joined, value...)
				}
			}
		}
		if end < 0 {
			break
		}
		start = next
	}
	flush()
	return result
}
