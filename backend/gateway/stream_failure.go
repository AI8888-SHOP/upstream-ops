package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errUpstreamStreamFailure = errors.New("upstream stream failed")

// Inspect only protocol envelopes, never assistant text or tool arguments.
// The caller invokes this on terminal events, not on each text delta.
func upstreamStreamFailure(eventName, data string) error {
	failed := func(value string) bool {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "error", "failed", "canceled", "cancelled", "response.error", "response.failed", "response.canceled", "response.cancelled":
			return true
		}
		return false
	}
	var envelope struct {
		Type     string          `json:"type"`
		Error    json.RawMessage `json:"error"`
		Response struct {
			Status string          `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"response"`
	}
	_ = json.Unmarshal([]byte(data), &envelope)
	if !failed(eventName) && !failed(envelope.Type) && !failed(envelope.Response.Status) &&
		!streamErrorValuePresent(envelope.Error) && !streamErrorValuePresent(envelope.Response.Error) {
		// Incomplete due to max_output_tokens/content_filter is a normal stop,
		// unless the envelope also explicitly reports an error.
		return nil
	}
	message := extractResponseErrorMessage([]byte(data), nil)
	if message == "" {
		message = strings.TrimSpace(envelope.Type)
	}
	if message == "" {
		message = strings.TrimSpace(eventName)
	}
	if message == "" {
		message = envelope.Response.Status
	}
	return fmt.Errorf("%w: %s", errUpstreamStreamFailure, message)
}

func streamErrorValuePresent(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) &&
		!bytes.Equal(raw, []byte("false")) && !bytes.Equal(raw, []byte(`""`))
}

func streamEnvelopeHasError(payload []byte) bool {
	value, ok := partialJSONRootMember(payload, "error")
	if !ok {
		return false
	}
	var raw json.RawMessage
	return json.NewDecoder(bytes.NewReader(value)).Decode(&raw) == nil && streamErrorValuePresent(raw)
}
