package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	streamPreconnectReasoningPrefix = "rs_uops_preconnect_"
	streamPreconnectResponsePrefix  = "resp_uops_preconnect_"
)

// preconnectResponsesStream is deliberately limited to explicitly stateless
// Responses requests. The response ID is local and must never be used as an
// upstream continuation ID. Source auditing, usage, and real TTFT run before
// this downstream-only adapter; only the wire framing changes here.
//
// Empty reasoning is semantic TTFT in stock sub2api, but is not visible output
// and does not disarm its first-output guard. The synthetic item stays open
// until real output arrives. Unknown/non-standard events are not invented.
type preconnectResponsesStream struct {
	responseID  string
	reasoningID string
	createdAt   int64
	sequence    int64
	prefix      []byte
	buffer      []byte
	lineStart   int
	skipLF      bool
	itemClosed  bool
	terminal    bool
	failed      bool
	items       map[int64]*preconnectResponsesItem
	itemBytes   int
	sawOutput   bool
}

type preconnectResponsesItem struct {
	id    string
	kind  string
	added bool
	done  json.RawMessage
}

func newPreconnectResponsesStream() (*preconnectResponsesStream, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, fmt.Errorf("create Responses preconnect identity: %w", err)
	}
	suffix := hex.EncodeToString(entropy[:])
	s := &preconnectResponsesStream{
		responseID:  streamPreconnectResponsePrefix + suffix,
		reasoningID: streamPreconnectReasoningPrefix + suffix,
		createdAt:   time.Now().Unix(),
	}
	// Cache the preamble: asking for it twice cannot consume sequence numbers
	// or create a second response identity.
	for _, eventType := range []string{"response.created", "response.in_progress"} {
		s.prefix = append(s.prefix, s.frame(eventType, map[string]json.RawMessage{
			"response": s.baseline("in_progress"),
		})...)
	}
	s.prefix = append(s.prefix, s.frame("response.output_item.added", map[string]json.RawMessage{
		"output_index": json.RawMessage("0"),
		"item":         s.reasoning("in_progress"),
	})...)
	return s, nil
}

func (s *preconnectResponsesStream) preamble() []byte { return s.prefix }

func (s *preconnectResponsesStream) pending() bool { return len(s.buffer) != 0 }

func (s *preconnectResponsesStream) done() bool { return s.terminal }

func (s *preconnectResponsesStream) discardPending() {
	s.buffer = nil
	s.lineStart = 0
	s.skipLF = false
	s.failed = false
}

// feed scans each incoming byte once, including partial UTF-8 and CRLF split
// across writes. Only one bounded SSE event is retained. A malformed event is
// never forwarded raw after a synthetic lifecycle has already been sent.
func (s *preconnectResponsesStream) feed(payload []byte, emit func([]byte) error) error {
	if s.terminal {
		return nil // Ignore trailing [DONE]/bytes after the authoritative terminal.
	}
	if s.failed {
		return errors.New("Responses preconnect stream is invalid")
	}
	if emit == nil {
		return errors.New("Responses preconnect emitter is nil")
	}
	for _, ch := range payload {
		if s.skipLF {
			s.skipLF = false
			if ch == '\n' {
				continue
			}
		}
		if len(s.buffer) >= maxSSELineSize {
			s.failed = true
			return errors.New("Responses preconnect SSE event exceeds size limit")
		}
		if ch != '\n' && ch != '\r' {
			s.buffer = append(s.buffer, ch)
			continue
		}
		s.skipLF = ch == '\r'
		lineEnd := len(s.buffer)
		s.buffer = append(s.buffer, '\n')
		if lineEnd != s.lineStart {
			s.lineStart = len(s.buffer)
			continue
		}
		if err := s.event(s.buffer, emit); err != nil {
			s.failed = true
			return err
		}
		// Do not retain a multi-megabyte terminal/image allocation for the rest
		// of a long response. Normal token-sized buffers remain reusable.
		if cap(s.buffer) > 64<<10 {
			s.buffer = nil
		} else {
			s.buffer = s.buffer[:0]
		}
		s.lineStart = 0
		if s.terminal {
			return nil
		}
	}
	return nil
}

func (s *preconnectResponsesStream) event(wire []byte, emit func([]byte) error) error {
	var eventName string
	var data []byte
	dataSeen := false
	for rest := wire; len(rest) > 0; {
		line, next, _ := bytes.Cut(rest, []byte{'\n'})
		rest = next
		if len(line) == 0 || line[0] == ':' {
			continue
		}
		field, value, _ := bytes.Cut(line, []byte{':'})
		if len(value) > 0 && value[0] == ' ' {
			value = value[1:]
		}
		switch string(field) {
		case "event":
			eventName = strings.TrimSpace(string(value))
		case "data":
			if dataSeen {
				data = append(data, '\n')
			}
			data = append(data, value...)
			dataSeen = true
		case "id", "retry":
			// Transport fields do not participate in JSON event identity.
		default:
			return fmt.Errorf("invalid Responses preconnect SSE field %q", field)
		}
	}
	if !dataSeen {
		return emit(wire) // Preserve pure comments and transport-only fields.
	}
	data = bytes.TrimSpace(data)
	if bytes.Equal(data, []byte("[DONE]")) {
		return errors.New("Responses preconnect stream ended without a terminal event")
	}
	if !utf8.Valid(data) {
		return errors.New("Responses preconnect event contains invalid UTF-8")
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil || envelope == nil {
		return errors.New("invalid Responses preconnect event JSON object")
	}
	eventType := eventName
	if raw, ok := envelope["type"]; ok {
		var payloadType string
		if err := json.Unmarshal(raw, &payloadType); err != nil {
			return errors.New("invalid Responses preconnect event type")
		}
		if payloadType = strings.TrimSpace(payloadType); payloadType != "" {
			eventType = payloadType
		}
	}
	if eventType == "" {
		return errors.New("Responses preconnect event has no type")
	}
	var response map[string]json.RawMessage
	if raw, ok := envelope["response"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if err := json.Unmarshal(raw, &response); err != nil || response == nil {
			return errors.New("invalid Responses preconnect response object")
		}
	}
	if eventType == "response.created" || eventType == "response.in_progress" {
		_, terminal, _ := classifyResponsesSSEEventWithPayload(eventType, data)
		if terminal {
			return errors.New("Responses preconnect lifecycle contains terminal state")
		}
		return nil // Keep the local identity and lifecycle emitted by the timer.
	}
	if eventType == "keepalive" || eventType == "ping" {
		return emit(wire)
	}
	terminal, failed := false, false
	switch eventType {
	case "response.done":
		eventType = "response.completed"
		fallthrough
	case "response.completed", "response.incomplete":
		terminal = true
	case "error", "response.error", "response.canceled", "response.cancelled":
		eventType = "response.failed"
		fallthrough
	case "response.failed":
		terminal, failed = true, true
	}
	// Some upstreams report response.completed while the response status or
	// error body actually indicates failure. Match runtime classification so
	// adapting a committed stream cannot turn that failure into success.
	if terminal && upstreamStreamFailure(eventType, string(data)) != nil {
		eventType, failed = "response.failed", true
	}
	if raw, ok := envelope["output_index"]; ok {
		index, err := strconv.ParseInt(string(raw), 10, 64)
		if err != nil || index < 0 || index >= 9007199254740991 {
			return errors.New("invalid Responses preconnect output index")
		}
		if err := s.observeItem(eventType, index, envelope["item"]); err != nil {
			return err
		}
		envelope["output_index"] = json.RawMessage(strconv.FormatInt(index+1, 10))
	} else if eventType == "response.output_item.added" || eventType == "response.output_item.done" {
		return errors.New("Responses preconnect output item has no index")
	}
	if terminal {
		if response == nil {
			if !failed {
				return errors.New("Responses preconnect terminal has no response")
			}
			response = make(map[string]json.RawMessage)
		}
		if failed {
			s.normalizeFailure(response, envelope)
		} else {
			expectedStatus := strings.TrimPrefix(eventType, "response.")
			if raw, ok := response["status"]; ok {
				var status string
				if json.Unmarshal(raw, &status) != nil || status != expectedStatus {
					return errors.New("Responses preconnect successful terminal has inconsistent status")
				}
			} else {
				// Protocol converters can omit status; the terminal event itself
				// supplies it without inventing usage or model output.
				response["status"] = preconnectResponsesJSON(expectedStatus)
			}
		}
		if err := s.prependReasoning(response, failed); err != nil {
			return err
		}
	}
	if response != nil {
		response["id"] = preconnectResponsesJSON(s.responseID)
		if _, ok := response["object"]; !ok {
			response["object"] = preconnectResponsesJSON("response")
		}
		envelope["response"] = preconnectResponsesJSON(response)
	}
	if _, ok := envelope["response_id"]; ok {
		envelope["response_id"] = preconnectResponsesJSON(s.responseID)
	}
	// A failed first real event must remain retryable in sub2api: emitting
	// output_item.done before it would incorrectly count as client progress.
	if !failed && eventType != "response.queued" && !s.itemClosed {
		if err := emit(s.frame("response.output_item.done", map[string]json.RawMessage{
			"output_index": json.RawMessage("0"),
			"item":         s.reasoning("completed"),
		})); err != nil {
			return err
		}
		s.itemClosed = true
	}
	if err := emit(s.frame(eventType, envelope)); err != nil {
		return err
	}
	s.terminal = terminal
	return nil
}

func (s *preconnectResponsesStream) prependReasoning(response map[string]json.RawMessage, failed bool) error {
	raw, ok := response["output"]
	var output []json.RawMessage
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		if failed {
			// Preserve SDK recovery of partial real output after failure. Before
			// any real output, there is only our empty transport reasoning item.
			if !s.itemClosed {
				response["output"] = preconnectResponsesJSON([]json.RawMessage{s.reasoning("completed")})
			}
			return nil
		}
		// Recover only complete, contiguous real items, never just the
		// synthetic one. Delta-only malformed streams fail closed.
		for index := int64(0); index < int64(len(s.items)); index++ {
			item := s.items[index]
			if item == nil || len(item.done) == 0 {
				return errors.New("Responses preconnect terminal is missing completed output")
			}
			output = append(output, item.done)
		}
	} else if err := json.Unmarshal(raw, &output); err != nil {
		return errors.New("invalid Responses preconnect terminal output")
	}
	if !failed {
		for index, state := range s.items {
			if index >= int64(len(output)) {
				return errors.New("Responses preconnect terminal omitted streamed output")
			}
			var identity struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			}
			if err := json.Unmarshal(output[index], &identity); err != nil ||
				(state.id != "" && identity.ID != state.id) || (state.kind != "" && identity.Type != state.kind) {
				return errors.New("Responses preconnect terminal output identity changed")
			}
		}
		if len(output) == 0 && !s.sawOutput && !preconnectResponsesHasUsage(response["usage"]) {
			return errors.New("Responses preconnect received empty success without usage")
		}
	}
	response["output"] = preconnectResponsesJSON(append([]json.RawMessage{s.reasoning("completed")}, output...))
	return nil
}

func (s *preconnectResponsesStream) observeItem(eventType string, index int64, raw json.RawMessage) error {
	s.sawOutput = true
	if s.items == nil {
		s.items = make(map[int64]*preconnectResponsesItem)
	}
	state := s.items[index]
	if state == nil {
		// Bound map overhead as well as retained JSON bytes.
		if len(s.items) >= 4096 {
			return errors.New("Responses preconnect output item count exceeds limit")
		}
		state = &preconnectResponsesItem{}
		s.items[index] = state
	}
	if eventType != "response.output_item.added" && eventType != "response.output_item.done" {
		return nil
	}
	var identity struct {
		ID   string `json:"id"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &identity); err != nil || identity.Type == "" {
		return errors.New("invalid Responses preconnect output item")
	}
	if (state.id != "" && identity.ID != state.id) || (state.kind != "" && identity.Type != state.kind) {
		return errors.New("Responses preconnect output item identity changed")
	}
	if eventType == "response.output_item.added" {
		if state.added || len(state.done) > 0 {
			return errors.New("duplicate Responses preconnect output index")
		}
		state.added = true
	}
	if state.kind == "" {
		s.itemBytes += len(identity.ID) + len(identity.Type)
		state.id, state.kind = identity.ID, identity.Type
	}
	if eventType == "response.output_item.done" {
		s.itemBytes += len(raw) - len(state.done)
		state.done = raw
	}
	if s.itemBytes > maxSSELineSize {
		return errors.New("Responses preconnect completed output exceeds size limit")
	}
	return nil
}

func preconnectResponsesHasUsage(raw json.RawMessage) bool {
	var usage map[string]json.RawMessage
	if json.Unmarshal(raw, &usage) != nil {
		return false
	}
	for _, key := range []string{"input_tokens", "output_tokens", "total_tokens"} {
		var number json.Number
		if json.Unmarshal(usage[key], &number) == nil {
			value, err := number.Float64()
			if err == nil && value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value) {
				return true
			}
		}
	}
	return false
}

func (s *preconnectResponsesStream) normalizeFailure(response, envelope map[string]json.RawMessage) {
	response["status"] = preconnectResponsesJSON("failed")
	if raw, ok := response["error"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return
	}
	if raw, ok := envelope["error"]; ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		response["error"] = raw
		return
	}
	err := map[string]json.RawMessage{
		"type":    preconnectResponsesJSON("upstream_error"),
		"code":    preconnectResponsesJSON("upstream_error"),
		"message": preconnectResponsesJSON("Upstream response failed"),
	}
	for _, key := range []string{"code", "message", "param"} {
		if raw, ok := envelope[key]; ok {
			err[key] = raw
		}
	}
	response["error"] = preconnectResponsesJSON(err)
}

// failure is used for malformed/truncated streams. It never claims successful
// model output, manufactures usage, or emits item.done before the failure.
func (s *preconnectResponsesStream) failure(message string) []byte {
	if s.terminal {
		return nil
	}
	s.discardPending()
	s.terminal = true
	var response map[string]json.RawMessage
	_ = json.Unmarshal(s.baseline("failed"), &response)
	if s.itemClosed {
		// Keep the client's already accumulated real/partial items. Replacing
		// them with a synthetic-only output array would lose visible content.
		delete(response, "output")
	} else {
		response["output"] = preconnectResponsesJSON([]json.RawMessage{s.reasoning("completed")})
	}
	response["error"] = preconnectResponsesJSON(map[string]string{
		"type": "upstream_error", "code": "stream_protocol_error", "message": message,
	})
	return s.frame("response.failed", map[string]json.RawMessage{"response": preconnectResponsesJSON(response)})
}

func (s *preconnectResponsesStream) reasoning(status string) json.RawMessage {
	return preconnectResponsesJSON(map[string]any{
		"id": s.reasoningID, "type": "reasoning", "summary": []any{}, "status": status,
	})
}

func (s *preconnectResponsesStream) baseline(status string) json.RawMessage {
	return preconnectResponsesJSON(map[string]any{
		"id": s.responseID, "object": "response", "created_at": s.createdAt,
		"status": status, "model": "", "output": []any{}, "store": false,
		"usage": nil, "error": nil, "incomplete_details": nil,
		"parallel_tool_calls": true, "tool_choice": "auto", "tools": []any{},
	})
}

func (s *preconnectResponsesStream) frame(eventType string, envelope map[string]json.RawMessage) []byte {
	envelope["type"] = preconnectResponsesJSON(eventType)
	envelope["sequence_number"] = json.RawMessage(strconv.FormatInt(s.sequence, 10))
	s.sequence++
	data := preconnectResponsesJSON(envelope)
	frame := make([]byte, 0, len(eventType)+len(data)+16)
	frame = append(frame, "event: "...)
	frame = append(frame, eventType...)
	frame = append(frame, "\ndata: "...)
	frame = append(frame, data...)
	return append(frame, '\n', '\n')
}

func preconnectResponsesJSON(value any) json.RawMessage {
	// All callers use strings, primitive values, and validated RawMessages.
	// No arbitrary application value or float is serialized by this adapter.
	encoded, _ := json.Marshal(value)
	return encoded
}
