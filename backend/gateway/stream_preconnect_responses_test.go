package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

type preconnectResponsesTestEvent struct {
	name string
	data []byte
	root map[string]json.RawMessage
}

func parsePreconnectResponsesTestEvents(t *testing.T, wire []byte) []preconnectResponsesTestEvent {
	t.Helper()
	wire = bytes.ReplaceAll(wire, []byte("\r\n"), []byte("\n"))
	var events []preconnectResponsesTestEvent
	for _, frame := range bytes.Split(wire, []byte("\n\n")) {
		var name string
		var data [][]byte
		for _, line := range bytes.Split(frame, []byte{'\n'}) {
			if bytes.HasPrefix(line, []byte("event:")) {
				name = strings.TrimSpace(string(line[len("event:"):]))
			}
			if bytes.HasPrefix(line, []byte("data:")) {
				data = append(data, bytes.TrimPrefix(line[len("data:"):], []byte{' '}))
			}
		}
		if len(data) == 0 {
			continue
		}
		payload := bytes.Join(data, []byte{'\n'})
		if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
			continue
		}
		var root map[string]json.RawMessage
		if err := json.Unmarshal(payload, &root); err != nil {
			t.Fatalf("invalid emitted JSON: %s: %v", payload, err)
		}
		typ := preconnectResponsesTestString(t, root["type"])
		if name == "" {
			name = typ
		}
		if typ != name {
			t.Fatalf("event name %q does not match JSON type %q", name, typ)
		}
		events = append(events, preconnectResponsesTestEvent{name: name, data: payload, root: root})
	}
	return events
}

func preconnectResponsesTestString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("expected string, got %s: %v", raw, err)
	}
	return value
}

func preconnectResponsesTestObject(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var value map[string]json.RawMessage
	if err := json.Unmarshal(raw, &value); err != nil || value == nil {
		t.Fatalf("expected object, got %s: %v", raw, err)
	}
	return value
}

func preconnectResponsesTestInt(t *testing.T, raw json.RawMessage) int {
	t.Helper()
	var value int
	if len(raw) == 0 {
		t.Fatal("expected an integer field, got no value")
	}
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatalf("expected integer, got %s: %v", raw, err)
	}
	return value
}

func preconnectResponsesTestStart(t *testing.T) (*preconnectResponsesStream, *bytes.Buffer, string, string) {
	t.Helper()
	stream, err := newPreconnectResponsesStream()
	if err != nil {
		t.Fatal(err)
	}
	wire := bytes.NewBuffer(stream.preamble())
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	if len(events) != 3 {
		t.Fatalf("prefix has %d events, want 3: %s", len(events), wire)
	}
	response := preconnectResponsesTestObject(t, events[0].root["response"])
	item := preconnectResponsesTestObject(t, events[2].root["item"])
	return stream, wire, preconnectResponsesTestString(t, response["id"]), preconnectResponsesTestString(t, item["id"])
}

func preconnectResponsesTestEmit(wire *bytes.Buffer) func([]byte) error {
	return func(payload []byte) error {
		_, err := wire.Write(payload)
		return err
	}
}

func assertPreconnectResponsesTestSequence(t *testing.T, events []preconnectResponsesTestEvent) {
	t.Helper()
	previous := -1
	for _, event := range events {
		sequence := preconnectResponsesTestInt(t, event.root["sequence_number"])
		if sequence <= previous {
			t.Fatalf("sequence did not increase: %d after %d in %s", sequence, previous, event.data)
		}
		previous = sequence
	}
}

func TestPreconnectResponsesPreambleSemanticOnly(t *testing.T) {
	stream, wire, responseID, itemID := preconnectResponsesTestStart(t)
	if responseID == "" || itemID == "" || responseID == itemID || stream.done() || stream.pending() {
		t.Fatalf("invalid initial state: response=%q item=%q done=%v pending=%v", responseID, itemID, stream.done(), stream.pending())
	}
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	wantNames := []string{"response.created", "response.in_progress", "response.output_item.added"}
	for i, event := range events {
		if event.name != wantNames[i] || preconnectResponsesTestInt(t, event.root["sequence_number"]) != i {
			t.Fatalf("prefix event %d = %s", i, event.data)
		}
		if streamEventHasVisibleOutput(protocol.KindOpenAIResponses, event.name, event.data) {
			t.Fatalf("synthetic event counted as real visible output: %s", event.data)
		}
		if len(event.root["delta"]) != 0 || len(event.root["usage"]) != 0 {
			t.Fatalf("prefix fabricated delta or usage: %s", event.data)
		}
		if i < 2 {
			response := preconnectResponsesTestObject(t, event.root["response"])
			if preconnectResponsesTestString(t, response["id"]) != responseID || preconnectResponsesTestString(t, response["model"]) != "" {
				t.Fatalf("prefix ID mismatch or fabricated model: %s", event.data)
			}
			if usage := bytes.TrimSpace(response["usage"]); len(usage) > 0 && !bytes.Equal(usage, []byte("null")) {
				t.Fatalf("prefix fabricated response usage: %s", event.data)
			}
		}
	}
	item := preconnectResponsesTestObject(t, events[2].root["item"])
	if preconnectResponsesTestString(t, item["type"]) != "reasoning" || preconnectResponsesTestInt(t, events[2].root["output_index"]) != 0 || string(item["summary"]) != "[]" {
		t.Fatalf("prefix must contain an empty reasoning item: %s", events[2].data)
	}
	// Stock sub2api semantic excludes created/in_progress/keepalive, but accepts
	// output_item.added. The gateway's visible classifier above stays false.
	semanticStarts := func(event string) bool {
		return event != "response.created" && event != "response.in_progress" && event != "keepalive" && event != "response.failed"
	}
	if semanticStarts(events[0].name) || semanticStarts(events[1].name) || !semanticStarts(events[2].name) {
		t.Fatal("prefix no longer matches the documented sub2api semantic contract")
	}
}

func TestPreconnectResponsesMultihopLifecycleAndRealPayload(t *testing.T) {
	stream, err := newPreconnectResponsesStream(true)
	if err != nil {
		t.Fatal(err)
	}
	wire := bytes.NewBuffer(append([]byte(nil), stream.preamble()...))
	prefix := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	wantPrefix := []string{"response.created", "response.in_progress", "response.output_item.added", "response.reasoning_summary_part.added", "response.reasoning_summary_text.delta"}
	if len(prefix) != len(wantPrefix) {
		t.Fatalf("prefix has %d events, want %d: %s", len(prefix), len(wantPrefix), wire)
	}
	for i, event := range prefix {
		if event.name != wantPrefix[i] {
			t.Fatalf("prefix event %d = %s, want %s", i, event.name, wantPrefix[i])
		}
		if strings.Contains(event.name, "output_text") || len(event.root["usage"]) > 0 {
			t.Fatalf("prefix fabricated answer or usage: %s", event.data)
		}
	}
	item := preconnectResponsesTestObject(t, prefix[2].root["item"])
	id := preconnectResponsesTestString(t, item["id"])
	if !strings.HasPrefix(id, streamPreconnectSpaceReasoningPrefix) || string(item["summary"]) != "[]" {
		t.Fatalf("multihop reasoning identity/initial summary invalid: %s", prefix[2].data)
	}
	for _, event := range prefix[3:] {
		if preconnectResponsesTestString(t, event.root["item_id"]) != id || preconnectResponsesTestInt(t, event.root["output_index"]) != 0 || preconnectResponsesTestInt(t, event.root["summary_index"]) != 0 {
			t.Fatalf("invalid summary event identity: %s", event.data)
		}
	}
	part := preconnectResponsesTestObject(t, prefix[3].root["part"])
	if preconnectResponsesTestString(t, part["text"]) != "" || preconnectResponsesTestString(t, prefix[4].root["delta"]) != " " {
		t.Fatalf("multihop must contain only one space delta: %s", wire)
	}
	if !bytes.Equal(stream.preamble(), wire.Bytes()) {
		t.Fatal("preamble changed on repeat access")
	}
	const answer = "actual answer without a leading space"
	real := []byte(preconnectForwardResponse(answer))
	// Every-byte delivery also exercises split JSON, lines, and SSE delimiters.
	for _, b := range real {
		if err := stream.feed([]byte{b}, preconnectResponsesTestEmit(wire)); err != nil {
			t.Fatal(err)
		}
	}
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	assertPreconnectResponsesTestSequence(t, events)
	wantClose := []string{"response.reasoning_summary_text.done", "response.reasoning_summary_part.done", "response.output_item.done"}
	for i, name := range wantClose {
		if events[len(prefix)+i].name != name {
			t.Fatalf("synthetic closure event %d = %s, want %s", i, events[len(prefix)+i].name, name)
		}
	}
	counts := make(map[string]int)
	var realDelta string
	for _, event := range events {
		counts[event.name]++
		if event.name == "response.output_text.delta" {
			realDelta += preconnectResponsesTestString(t, event.root["delta"])
			if preconnectResponsesTestInt(t, event.root["output_index"]) != 1 {
				t.Fatalf("real item index not shifted: %s", event.data)
			}
		}
	}
	if realDelta != answer || counts["response.reasoning_summary_text.delta"] != 1 || counts["response.reasoning_summary_text.done"] != 1 || counts["response.reasoning_summary_part.done"] != 1 {
		t.Fatalf("answer changed or synthetic summary duplicated: delta=%q counts=%v", realDelta, counts)
	}
	terminal := events[len(events)-1]
	response := preconnectResponsesTestObject(t, terminal.root["response"])
	var output []json.RawMessage
	if err := json.Unmarshal(response["output"], &output); err != nil || len(output) != 2 {
		t.Fatalf("terminal output = %s, err=%v", response["output"], err)
	}
	if !streamPreconnectIsReplayItem(output[0]) || !bytes.Equal(output[0], events[len(prefix)+2].root["item"]) {
		t.Fatalf("synthetic final item does not match completed event: %s", output[0])
	}
	realEvents := parsePreconnectResponsesTestEvents(t, real)
	realResponse := preconnectResponsesTestObject(t, realEvents[len(realEvents)-1].root["response"])
	var actualOutput []json.RawMessage
	_ = json.Unmarshal(realResponse["output"], &actualOutput)
	if !bytes.Equal(output[1], actualOutput[0]) || !bytes.Equal(response["usage"], realResponse["usage"]) || preconnectResponsesTestString(t, response["model"]) != "m" {
		t.Fatalf("real output, usage, or model changed: %s", terminal.data)
	}
}

func TestPreconnectResponsesMultihopFailureClosesOnlySyntheticSpace(t *testing.T) {
	for _, protocolFailure := range []bool{false, true} {
		t.Run(fmt.Sprintf("protocolFailure=%v", protocolFailure), func(t *testing.T) {
			stream, err := newPreconnectResponsesStream(true)
			if err != nil {
				t.Fatal(err)
			}
			wire := bytes.NewBuffer(append([]byte(nil), stream.preamble()...))
			if protocolFailure {
				wire.Write(stream.failure("truncated source"))
			} else {
				payload := preconnectResponsesJSON(map[string]any{"type": "response.failed", "response": map[string]any{"status": "failed", "error": map[string]string{"type": "upstream_error", "message": "no route"}}})
				if err := stream.feed(append(append([]byte("data: "), payload...), '\n', '\n'), preconnectResponsesTestEmit(wire)); err != nil {
					t.Fatal(err)
				}
			}
			events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
			want := []string{"response.reasoning_summary_text.done", "response.reasoning_summary_part.done", "response.output_item.done", "response.failed"}
			if len(events) != 9 {
				t.Fatalf("failure lifecycle has %d events: %s", len(events), wire)
			}
			for i, kind := range want {
				if events[5+i].name != kind {
					t.Fatalf("failure event %d = %s, want %s", i, events[5+i].name, kind)
				}
			}
			response := preconnectResponsesTestObject(t, events[8].root["response"])
			var output []json.RawMessage
			if err := json.Unmarshal(response["output"], &output); err != nil || len(output) != 1 || !streamPreconnectIsReplayItem(output[0]) {
				t.Fatalf("failed terminal lost synthetic space: %s", events[8].data)
			}
			if preconnectResponsesTestString(t, response["status"]) != "failed" || preconnectResponsesHasUsage(response["usage"]) || strings.Contains(wire.String(), "response.completed") {
				t.Fatalf("failure fabricated success or usage: %s", wire)
			}
			assertPreconnectResponsesTestSequence(t, events)
		})
	}
}

func TestPreconnectResponsesPreservesPayloadAndTerminal(t *testing.T) {
	stream, wire, responseID, itemID := preconnectResponsesTestStart(t)
	const toolArgs = "{\"城市\":\"维也纳\",\"n\":9007199254740993}"
	const answer = "你好 🌍\n第二行"
	argsJSON, _ := json.Marshal(toolArgs)
	answerJSON, _ := json.Marshal(answer)
	actualTool := "{\"id\":\"fc_live\",\"type\":\"function_call\",\"call_id\":\"call_live\",\"name\":\"lookup\",\"arguments\":" + string(argsJSON) + ",\"status\":\"completed\"}"
	actualMessage := "{\"id\":\"msg_live\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":" + string(answerJSON) + ",\"annotations\":[]}]}"
	const actualUsage = "{\"input_tokens\":9007199254740993,\"output_tokens\":123,\"total_tokens\":9007199254741116,\"input_tokens_details\":{\"cached_tokens\":9007199254740991}}"
	payloads := []string{
		"{\"type\":\"response.created\",\"sequence_number\":91,\"response\":{\"id\":\"resp_real\",\"model\":\"real-model\",\"status\":\"in_progress\",\"output\":[]}}",
		"{\"type\":\"response.in_progress\",\"sequence_number\":92,\"response\":{\"id\":\"resp_real\",\"status\":\"in_progress\"}}",
		"{\"type\":\"response.output_item.added\",\"sequence_number\":93,\"output_index\":0,\"item\":{\"id\":\"fc_live\",\"type\":\"function_call\",\"call_id\":\"call_live\",\"name\":\"lookup\",\"arguments\":\"\",\"status\":\"in_progress\"}}",
		"{\"type\":\"response.function_call_arguments.delta\",\"sequence_number\":94,\"output_index\":0,\"item_id\":\"fc_live\",\"delta\":" + string(argsJSON) + "}",
		"{\"type\":\"response.output_item.done\",\"sequence_number\":95,\"output_index\":0,\"item\":" + actualTool + "}",
		"{\"type\":\"response.output_item.added\",\"sequence_number\":96,\"output_index\":1,\"item\":{\"id\":\"msg_live\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}",
		"{\"type\":\"response.output_text.delta\",\"sequence_number\":97,\"output_index\":1,\"content_index\":0,\"item_id\":\"msg_live\",\"delta\":" + string(answerJSON) + "}",
		"{\"type\":\"response.output_item.done\",\"sequence_number\":98,\"output_index\":1,\"item\":" + actualMessage + "}",
		"{\"type\":\"response.completed\",\"sequence_number\":99,\"response\":{\"id\":\"resp_real\",\"object\":\"response\",\"model\":\"real-model\",\"status\":\"completed\",\"usage\":" + actualUsage + ",\"output\":[" + actualTool + "," + actualMessage + "]}}",
	}
	for _, payload := range payloads {
		if err := stream.feed([]byte("data: "+payload+"\n\n"), preconnectResponsesTestEmit(wire)); err != nil {
			t.Fatal(err)
		}
	}
	if !stream.done() || stream.pending() {
		t.Fatalf("terminal state done=%v pending=%v", stream.done(), stream.pending())
	}
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	assertPreconnectResponsesTestSequence(t, events)
	createdCount, inProgressCount, syntheticAdded, syntheticDone := 0, 0, 0, 0
	for _, event := range events {
		switch event.name {
		case "response.created":
			createdCount++
		case "response.in_progress":
			inProgressCount++
		case "response.output_item.added", "response.output_item.done":
			item := preconnectResponsesTestObject(t, event.root["item"])
			id := preconnectResponsesTestString(t, item["id"])
			switch id {
			case itemID:
				if preconnectResponsesTestInt(t, event.root["output_index"]) != 0 {
					t.Fatalf("synthetic item index changed: %s", event.data)
				}
				if event.name == "response.output_item.added" {
					syntheticAdded++
				} else {
					syntheticDone++
				}
			case "fc_live":
				if preconnectResponsesTestInt(t, event.root["output_index"]) != 1 || preconnectResponsesTestString(t, item["call_id"]) != "call_live" {
					t.Fatalf("tool index or identity changed: %s", event.data)
				}
			case "msg_live":
				if preconnectResponsesTestInt(t, event.root["output_index"]) != 2 {
					t.Fatalf("message index not shifted: %s", event.data)
				}
			default:
				t.Fatalf("unexpected rewritten item identity: %s", event.data)
			}
		case "response.function_call_arguments.delta":
			if preconnectResponsesTestInt(t, event.root["output_index"]) != 1 || preconnectResponsesTestString(t, event.root["item_id"]) != "fc_live" || preconnectResponsesTestString(t, event.root["delta"]) != toolArgs {
				t.Fatalf("tool argument delta changed: %s", event.data)
			}
		case "response.output_text.delta":
			if preconnectResponsesTestInt(t, event.root["output_index"]) != 2 || preconnectResponsesTestString(t, event.root["item_id"]) != "msg_live" || preconnectResponsesTestString(t, event.root["delta"]) != answer {
				t.Fatalf("text delta changed: %s", event.data)
			}
		}
	}
	if createdCount != 1 || inProgressCount != 1 || syntheticAdded != 1 || syntheticDone != 1 {
		t.Fatalf("duplicate lifecycle: created=%d progress=%d synthetic added=%d done=%d", createdCount, inProgressCount, syntheticAdded, syntheticDone)
	}
	last := events[len(events)-1]
	if last.name != "response.completed" {
		t.Fatalf("last event = %s", last.data)
	}
	response := preconnectResponsesTestObject(t, last.root["response"])
	if preconnectResponsesTestString(t, response["id"]) != responseID || preconnectResponsesTestString(t, response["model"]) != "real-model" {
		t.Fatalf("terminal response identity/model = %s", last.data)
	}
	if !bytes.Equal(bytes.TrimSpace(response["usage"]), []byte(actualUsage)) {
		t.Fatalf("nested usage precision or values changed: %s", response["usage"])
	}
	var output []json.RawMessage
	if err := json.Unmarshal(response["output"], &output); err != nil || len(output) != 3 {
		t.Fatalf("terminal output = %s: %v", response["output"], err)
	}
	inserted := preconnectResponsesTestObject(t, output[0])
	if preconnectResponsesTestString(t, inserted["id"]) != itemID || preconnectResponsesTestString(t, inserted["type"]) != "reasoning" || preconnectResponsesTestString(t, inserted["status"]) != "completed" || string(inserted["summary"]) != "[]" {
		t.Fatalf("terminal synthetic item = %s", output[0])
	}
	if !bytes.Equal(output[1], []byte(actualTool)) || !bytes.Equal(output[2], []byte(actualMessage)) {
		t.Fatalf("real output items changed: %s", response["output"])
	}
	before := wire.Len()
	if err := stream.feed([]byte("data: [DONE]\n\ndata: {\"type\":\"response.completed\"}\n\n"), preconnectResponsesTestEmit(wire)); err != nil || wire.Len() != before {
		t.Fatalf("post-terminal bytes produced another result: %v, %s", err, wire.Bytes()[before:])
	}
}

func TestPreconnectResponsesEverySplitMultilineCRLFAndUnicode(t *testing.T) {
	const actualItem = "{\"id\":\"msg_源\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"你好 🌍\\n第二行\"}]}"
	const source = ": upstream heartbeat\r\n\r\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_源\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\r\n\r\n" +
		"event: response.output_text.delta\r\n" +
		"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":17,\r\n" +
		"data: \"output_index\":0,\"item_id\":\"msg_源\",\"content_index\":0,\"delta\":\"你好 🌍\\n第二行\"}\r\n\r\n" +
		"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":" + actualItem + "}\r\n\r\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":18,\"response\":{\"id\":\"resp_up\",\"status\":\"completed\",\"model\":\"原始模型\",\"output\":[" + actualItem + "]}}\r\n\r\n"
	for split := 0; split <= len(source); split++ {
		t.Run(fmt.Sprintf("split_%d", split), func(t *testing.T) {
			stream, wire, _, _ := preconnectResponsesTestStart(t)
			for _, chunk := range []string{source[:split], source[split:]} {
				if err := stream.feed([]byte(chunk), preconnectResponsesTestEmit(wire)); err != nil {
					t.Fatal(err)
				}
			}
			if stream.pending() || !stream.done() {
				t.Fatalf("split left pending bytes or missed terminal: pending=%v done=%v", stream.pending(), stream.done())
			}
			events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
			assertPreconnectResponsesTestSequence(t, events)
			found := 0
			for _, event := range events {
				if event.name != "response.output_text.delta" {
					continue
				}
				found++
				if preconnectResponsesTestString(t, event.root["delta"]) != "你好 🌍\n第二行" || preconnectResponsesTestString(t, event.root["item_id"]) != "msg_源" || preconnectResponsesTestInt(t, event.root["output_index"]) != 1 {
					t.Fatalf("split changed content or index: %s", event.data)
				}
			}
			if found != 1 {
				t.Fatalf("split emitted %d deltas, want 1", found)
			}
		})
	}
}

func TestPreconnectResponsesFirstFailureNeverEmitsItemDone(t *testing.T) {
	for _, payload := range []string{
		"{\"type\":\"response.failed\",\"response\":{\"id\":\"resp_real\",\"status\":\"failed\",\"error\":{\"type\":\"upstream_error\",\"code\":\"capacity\",\"message\":\"no capacity\"},\"output\":[]}}",
		"{\"type\":\"error\",\"error\":{\"type\":\"upstream_error\",\"code\":\"capacity\",\"message\":\"no capacity\"}}",
		"{\"type\":\"response.error\",\"error\":{\"type\":\"upstream_error\",\"code\":\"capacity\",\"message\":\"no capacity\"}}",
	} {
		stream, wire, responseID, _ := preconnectResponsesTestStart(t)
		if err := stream.feed([]byte("data: "+payload+"\n\n"), preconnectResponsesTestEmit(wire)); err != nil {
			t.Fatalf("failed to forward failure: %v", err)
		}
		events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
		if len(events) != 4 || events[3].name != "response.failed" || !stream.done() || stream.pending() {
			t.Fatalf("failure manufactured progress or missed terminal: %s", wire)
		}
		assertPreconnectResponsesTestSequence(t, events)
		response := preconnectResponsesTestObject(t, events[3].root["response"])
		detail := preconnectResponsesTestObject(t, response["error"])
		if preconnectResponsesTestString(t, response["id"]) != responseID || preconnectResponsesTestString(t, response["status"]) != "failed" || preconnectResponsesTestString(t, detail["code"]) != "capacity" || preconnectResponsesTestString(t, detail["message"]) != "no capacity" || preconnectResponsesTestString(t, detail["type"]) != "upstream_error" {
			t.Fatalf("failure identity or error changed: %s", events[3].data)
		}
	}
}

func TestPreconnectResponsesTerminalWithoutDeltaClosesArtificialItemOnce(t *testing.T) {
	stream, wire, responseID, itemID := preconnectResponsesTestStart(t)
	const payload = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_real\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1},\"output\":[{\"id\":\"msg_up\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}]}}\n\n"
	if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err != nil {
		t.Fatal(err)
	}
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	if len(events) != 5 || events[3].name != "response.output_item.done" || events[4].name != "response.completed" {
		t.Fatalf("terminal lifecycle = %s", wire)
	}
	item := preconnectResponsesTestObject(t, events[3].root["item"])
	response := preconnectResponsesTestObject(t, events[4].root["response"])
	if preconnectResponsesTestString(t, item["id"]) != itemID || preconnectResponsesTestString(t, item["status"]) != "completed" || preconnectResponsesTestString(t, response["id"]) != responseID {
		t.Fatalf("synthetic terminal identity mismatch: %s", wire)
	}
	before := wire.Len()
	if failure := stream.failure("late local error"); len(failure) != 0 || wire.Len() != before {
		t.Fatalf("terminal stream produced a second failure: %s", failure)
	}
}

func TestPreconnectResponsesRejectsInvalidOrOversizedFrames(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire string
	}{
		{"done without terminal", "data: [DONE]\n\n"},
		{"malformed JSON", "data: {\"type\":\"response.output_text.delta\",broken}\n\n"},
		{"non-object JSON", "data: [1,2,3]\n\n"},
		{"hidden lifecycle error", "data: {\"type\":\"response.created\",\"response\":{\"id\":\"up\",\"error\":{\"message\":\"failed\"}}}\n\n"},
		{"hidden terminal lifecycle", "data: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"up\",\"status\":\"failed\"}}\n\n"},
		{"empty zero-usage success", "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\n"},
		{"oversized line", "data: " + strings.Repeat("x", maxSSELineSize+1)},
		{"oversized multiline pending event", strings.Repeat("data: "+strings.Repeat("x", 1024)+"\n", maxSSELineSize/1024+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, wire, responseID, _ := preconnectResponsesTestStart(t)
			before := wire.Len()
			if err := stream.feed([]byte(tc.wire), preconnectResponsesTestEmit(wire)); err == nil {
				t.Fatal("invalid stream was accepted")
			}
			if len(stream.buffer) > maxSSELineSize {
				t.Fatalf("retained SSE buffer exceeded bound: %d > %d", len(stream.buffer), maxSSELineSize)
			}
			if wire.Len() != before {
				t.Fatalf("invalid frame emitted model progress: %s", wire.Bytes()[before:])
			}
			stream.discardPending()
			if stream.pending() {
				t.Fatal("discardPending retained incomplete or oversized bytes")
			}
			failure := stream.failure("adapter rejected upstream stream")
			events := parsePreconnectResponsesTestEvents(t, failure)
			if len(events) != 1 || events[0].name != "response.failed" || !stream.done() {
				t.Fatalf("invalid stream fabricated success or progress: %s", failure)
			}
			response := preconnectResponsesTestObject(t, events[0].root["response"])
			if preconnectResponsesTestString(t, response["id"]) != responseID {
				t.Fatalf("failure lost synthetic response identity: %s", failure)
			}
		})
	}
}

func TestPreconnectResponsesPendingDiscardAndWriterError(t *testing.T) {
	stream, wire, _, _ := preconnectResponsesTestStart(t)
	before := wire.Len()
	if err := stream.feed([]byte("data: {\"type\":\"response.output_text.delta\","), preconnectResponsesTestEmit(wire)); err != nil || !stream.pending() || wire.Len() != before {
		t.Fatalf("incomplete frame not buffered atomically: %v, pending=%v", err, stream.pending())
	}
	stream.discardPending()
	if stream.pending() {
		t.Fatal("discard did not clear partial frame")
	}
	sentinel := errors.New("downstream write refused")
	err := stream.feed([]byte("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"answer\"}\n\n"), func([]byte) error { return sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("writer error not propagated: %v", err)
	}
}

func TestPreconnectResponsesReconstructsOmittedTerminalOutput(t *testing.T) {
	const item = "{\"id\":\"msg_up\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}"
	for _, terminalOutput := range []string{"", ",\"output\":null"} {
		stream, wire, _, itemID := preconnectResponsesTestStart(t)
		payload := "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_up\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":" + item + "}\n\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_up\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}" + terminalOutput + "}}\n\n"
		if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err != nil {
			t.Fatal(err)
		}
		events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
		last := events[len(events)-1]
		response := preconnectResponsesTestObject(t, last.root["response"])
		var output []json.RawMessage
		if err := json.Unmarshal(response["output"], &output); err != nil || len(output) != 2 {
			t.Fatalf("omitted terminal output lost real item: %s, %v", response["output"], err)
		}
		inserted := preconnectResponsesTestObject(t, output[0])
		if last.name != "response.completed" || preconnectResponsesTestString(t, inserted["id"]) != itemID || !bytes.Equal(output[1], []byte(item)) {
			t.Fatalf("reconstructed output identity changed: %s", last.data)
		}
	}
}

func TestPreconnectResponsesRejectsIncompleteTerminalItemState(t *testing.T) {
	const added = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_up\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n"
	const item = "{\"id\":\"msg_up\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}"
	const done = "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":" + item + "}\n\n"
	for _, tc := range []struct {
		name     string
		prefix   string
		terminal string
	}{
		{"announced but unfinished", added, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":\"completed\",\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n"},
		{"explicit empty contradicts completed item", added + done, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":1}}}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream, wire, _, _ := preconnectResponsesTestStart(t)
			if err := stream.feed([]byte(tc.prefix), preconnectResponsesTestEmit(wire)); err != nil {
				t.Fatal(err)
			}
			before := wire.Len()
			if err := stream.feed([]byte(tc.terminal), preconnectResponsesTestEmit(wire)); err == nil || stream.done() || wire.Len() != before {
				t.Fatalf("contradictory terminal was accepted: %v, %s", err, wire.Bytes()[before:])
			}
		})
	}
}

func TestPreconnectResponsesNormalizesHiddenTerminalFailure(t *testing.T) {
	for _, status := range []string{"failed", "error", "canceled", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			stream, wire, responseID, _ := preconnectResponsesTestStart(t)
			before := wire.Len()
			payload := fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":%q}}\n\n", status)
			if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err != nil {
				t.Fatal(err)
			}
			events := parsePreconnectResponsesTestEvents(t, wire.Bytes()[before:])
			if len(events) != 1 || events[0].name != "response.failed" || !stream.done() {
				t.Fatalf("hidden failure became successful progress: %s", wire.Bytes()[before:])
			}
			response := preconnectResponsesTestObject(t, events[0].root["response"])
			if preconnectResponsesTestString(t, response["status"]) != "failed" || preconnectResponsesTestString(t, response["id"]) != responseID {
				t.Fatalf("normalized failure lost status or identity: %s", events[0].data)
			}
		})
	}
	stream, wire, _, _ := preconnectResponsesTestStart(t)
	const failure = `{"type":"request_timeout","code":"timeout","message":"upstream timed out"}`
	payload := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":\"completed\",\"error\":" + failure + "}}\n\n"
	if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err != nil {
		t.Fatal(err)
	}
	events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
	last := events[len(events)-1]
	response := preconnectResponsesTestObject(t, last.root["response"])
	if last.name != "response.failed" || !bytes.Equal(response["error"], []byte(failure)) {
		t.Fatalf("terminal error body was not preserved: %s", last.data)
	}
}

func TestPreconnectResponsesValidatesSuccessfulTerminalStatus(t *testing.T) {
	for _, status := range []string{`"in_progress"`, `"queued"`, `"incomplete"`, `""`, `null`, `123`} {
		t.Run(status, func(t *testing.T) {
			stream, wire, _, _ := preconnectResponsesTestStart(t)
			before := wire.Len()
			payload := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"up\",\"status\":" + status + ",\"output\":[],\"usage\":{\"input_tokens\":10}}}\n\n"
			if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err == nil || stream.done() || wire.Len() != before {
				t.Fatalf("invalid terminal status accepted: %v, %s", err, wire.Bytes()[before:])
			}
		})
	}
	for _, status := range []string{"completed", "incomplete"} {
		stream, wire, _, _ := preconnectResponsesTestStart(t)
		payload := fmt.Sprintf("data: {\"type\":%q,\"response\":{\"id\":\"up\",\"output\":[],\"usage\":{\"input_tokens\":10}}}\n\n", "response."+status)
		if err := stream.feed([]byte(payload), preconnectResponsesTestEmit(wire)); err != nil {
			t.Fatal(err)
		}
		events := parsePreconnectResponsesTestEvents(t, wire.Bytes())
		last := events[len(events)-1]
		response := preconnectResponsesTestObject(t, last.root["response"])
		if last.name != "response."+status || preconnectResponsesTestString(t, response["status"]) != status {
			t.Fatalf("omitted status not supplied from terminal type: %s", last.data)
		}
	}
}
