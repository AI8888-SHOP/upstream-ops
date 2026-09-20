package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
)

// One observation belongs to one upstream attempt, before protocol conversion
// can replace model metadata with the requested model. Never inspect assistant
// text, tool arguments, or arbitrary nested objects for a model declaration.
type responseModelAudit struct {
	Model    string
	Conflict bool
	last     string
}

func responseModelFromJSON(payload []byte) string {
	if (!bytes.Contains(payload, []byte(`"model"`)) && !bytes.Contains(payload, []byte(`\u`))) || !json.Valid(payload) {
		return ""
	}
	read := func(object []byte) string {
		value, ok := partialJSONRootMember(object, "model")
		if !ok {
			return ""
		}
		// partialJSONRootMember returns the remaining object, not just the
		// member value. Bound and decode only this complete JSON string.
		end := 0
		if _, complete := scanJSONStringNonEmpty(value, &end); !complete || end > 2048 {
			return ""
		}
		var model string
		if json.Unmarshal(value[:end], &model) != nil {
			return ""
		}
		model = strings.TrimSpace(model)
		if utf8.RuneCountInString(model) > 256 || strings.ContainsAny(model, "\r\n\x00") {
			return ""
		}
		return model
	}
	// Responses SSE: response.model; Anthropic message_start: message.model.
	for _, key := range []string{"response", "message"} {
		if object, ok := partialJSONRootMember(payload, key); ok {
			if model := read(object); model != "" {
				return model
			}
		}
	}
	return read(payload)
}

// Observe retains the first declaration unless a terminal event supplies a
// final one. Conflicting intermediate declarations remain visible in the log.
// Returning only changed declarations avoids running regexes on every chunk.
func (a *responseModelAudit) Observe(payload []byte, terminal bool) string {
	model := responseModelFromJSON(payload)
	if model == "" {
		return ""
	}
	if a.Model != "" && !strings.EqualFold(a.Model, model) {
		a.Conflict = true
	}
	if a.Model == "" || terminal {
		a.Model = model
	}
	if model == a.last {
		return ""
	}
	a.last = model
	return model
}

// Some upstreams return SSE even for a non-streaming request. Inspect every
// declaration before conversion folds that body into a single response.
func (a *responseModelAudit) ObserveBody(body []byte, onModel func(string) error) error {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil
	}
	jsonBody := trimmed[0] == '{'
	payloads := [][]byte{body}
	if !jsonBody {
		payloads = sseDataPayloads(body)
	}
	var firstErr error
	for _, payload := range payloads {
		terminal := jsonBody || isResponsesTerminalEventBytes(responsesPayloadTypeBytes(payload))
		if model := a.Observe(payload, terminal); model != "" && onModel != nil && firstErr == nil {
			firstErr = onModel(model)
		}
	}
	return firstErr
}

func responseModelMismatch(sent, response string) *bool {
	if strings.TrimSpace(response) == "" || strings.TrimSpace(sent) == "" {
		return nil
	}
	mismatch := !strings.EqualFold(strings.TrimSpace(sent), strings.TrimSpace(response))
	return &mismatch
}

func (v *responseValidator) ValidateResponseModel(protocolName, requested, response string) validationResult {
	if !v.Enabled() || response == "" {
		return acceptedValidation()
	}
	for _, rule := range v.rules {
		if rule.Target != "response_model" || !responseRuleApplies(rule, protocolName, requested) || !rule.re.MatchString(response) {
			continue
		}
		return validationResult{
			Decision: validationRejected, RuleID: rule.ID, RuleName: rule.Name,
			Target: rule.Target, Pattern: rule.Pattern, MatchedOn: response,
		}
	}
	return acceptedValidation()
}

// Validate before the raw event enters any converter or downstream writer.
// The gate serializes this with timer/commit decisions. A late match is audit
// only: switching after output would splice two different answers together.
func validateStreamResponseModel(c *gin.Context, response string) error {
	if response == "" {
		return nil
	}
	return validateStreamMetadata(c, func(s *streamResponseValidator) validationResult {
		return s.validator.ValidateResponseModel(s.protocolName, s.model, response)
	})
}

func validateStreamMetadata(c *gin.Context, validate func(*streamResponseValidator) validationResult) error {
	return validateStreamMetadataWithPolicy(c, validate, false)
}

// A strict final-usage rejection fails an already committed stream. Other
// metadata rules retain their audit-only behavior after commit. The caller
// must send a terminal error for a strict late rejection, never retry it.
func validateStreamMetadataWithPolicy(c *gin.Context, validate func(*streamResponseValidator) validationResult, rejectAfterCommit bool) error {
	if c == nil {
		return nil
	}
	gate, ok := c.Writer.(*streamPrefixGateWriter)
	if !ok {
		return nil
	}
	gate.mu.Lock()
	defer gate.mu.Unlock()
	s := gate.validator
	if s == nil || !s.validator.StreamEnabled() {
		return nil
	}
	match := validate(s)
	if !match.IsRejected() {
		return nil
	}
	if gate.committed {
		match.PostCommit = true
		if rejectAfterCommit || !gate.lateMatch.IsRejected() {
			gate.lateMatch = match
		}
		if rejectAfterCommit {
			return &responseRejectedError{Result: match}
		}
		return nil
	}
	gate.rejection = match
	gate.stopTimerLocked()
	gate.signalReadyLocked(match)
	return &responseRejectedError{Result: match}
}
