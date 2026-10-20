package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type preconnectRelayReceipt struct {
	event    string
	semantic bool
	visible  bool
	flushed  bool
}

// This fixture mirrors stock sub2api's distinct semantic/visible TTFT and
// first-client-output gates. It deliberately buffers metadata and empty added
// items even after semantic TTFT has started, matching the multi-hop regression.
// It uses real HTTP Flush boundaries, not final-body string assertions.
func preconnectRelayEventDecision(payload []byte) (kind string, semantic, visible, startsOutput bool) {
	var event map[string]json.RawMessage
	if json.Unmarshal(payload, &event) != nil {
		return "", false, false, false
	}
	readString := func(raw json.RawMessage) string {
		var value string
		_ = json.Unmarshal(raw, &value)
		return value
	}
	readObject := func(raw json.RawMessage) map[string]json.RawMessage {
		var value map[string]json.RawMessage
		_ = json.Unmarshal(raw, &value)
		return value
	}
	kind = readString(event["type"])
	switch kind {
	case "response.created", "response.in_progress", "keepalive", "response.failed":
		return kind, false, false, false
	}
	semantic, startsOutput = true, true
	if strings.HasSuffix(kind, ".delta") {
		visible = readString(event["delta"]) != ""
	}
	switch kind {
	case "response.output_item.added":
		item := readObject(event["item"])
		switch readString(item["type"]) {
		case "reasoning", "message":
			startsOutput = readString(item["encrypted_content"]) != ""
			for _, field := range []string{"summary", "content"} {
				var parts []map[string]json.RawMessage
				_ = json.Unmarshal(item[field], &parts)
				for _, part := range parts {
					if readString(part["text"]) != "" || readString(part["refusal"]) != "" {
						startsOutput, visible = true, true
					}
				}
			}
		case "function_call":
			startsOutput = readString(item["arguments"]) != ""
		}
	case "response.content_part.added", "response.reasoning_summary_part.added":
		part := readObject(event["part"])
		startsOutput = readString(part["text"]) != ""
		visible = startsOutput
	}
	return kind, semantic, visible, startsOutput
}

func preconnectRelayReadFrame(reader *bufio.Reader) ([]byte, []byte, error) {
	var frame, payload []byte
	for {
		line, err := reader.ReadBytes('\n')
		frame = append(frame, line...)
		if bytes.HasPrefix(line, []byte("data: ")) {
			payload = append(payload, bytes.TrimSpace(line[len("data: "):])...)
		}
		if bytes.Equal(line, []byte{'\n'}) {
			return frame, payload, nil
		}
		if err != nil {
			return frame, payload, err
		}
	}
}

func newPreconnectBufferingRelay(t *testing.T, upstream string, receipts chan<- preconnectRelayReceipt, failures chan<- error) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream, nil)
		if err != nil {
			failures <- err
			return
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			if r.Context().Err() == nil {
				failures <- err
			}
			return
		}
		defer response.Body.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		reader := bufio.NewReader(response.Body)
		var pending bytes.Buffer
		started := false
		for {
			frame, payload, err := preconnectRelayReadFrame(reader)
			if err != nil {
				if err != io.EOF && r.Context().Err() == nil {
					failures <- err
				}
				return
			}
			kind, semantic, visible, startsOutput := preconnectRelayEventDecision(payload)
			pending.Write(frame)
			if startsOutput {
				started = true
			}
			if started {
				if _, err := w.Write(pending.Bytes()); err != nil {
					return
				}
				pending.Reset()
				w.(http.Flusher).Flush()
			}
			receipts <- preconnectRelayReceipt{event: kind, semantic: semantic, visible: visible, flushed: started}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestPreconnectResponsesMultihopFlushesThroughTwoBufferingRelays(t *testing.T) {
	for _, multihop := range []bool{false, true} {
		t.Run(fmt.Sprintf("multihop=%v", multihop), func(t *testing.T) {
			stream, err := newPreconnectResponsesStream(multihop)
			if err != nil {
				t.Fatal(err)
			}
			releaseReal := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseReal) }) }
			failures := make(chan error, 8)
			source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write(stream.preamble())
				w.(http.Flusher).Flush()
				select {
				case <-releaseReal:
				case <-r.Context().Done():
					return
				}
				if err := stream.feed([]byte(preconnectForwardResponse("real answer")), func(frame []byte) error {
					_, err := w.Write(frame)
					w.(http.Flusher).Flush()
					return err
				}); err != nil && r.Context().Err() == nil {
					failures <- err
				}
			}))
			t.Cleanup(source.Close)
			firstReceipts := make(chan preconnectRelayReceipt, 64)
			secondReceipts := make(chan preconnectRelayReceipt, 64)
			first := newPreconnectBufferingRelay(t, source.URL, firstReceipts, failures)
			second := newPreconnectBufferingRelay(t, first.URL, secondReceipts, failures)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			t.Cleanup(cancel)
			t.Cleanup(release)
			clientReceipts := make(chan preconnectRelayReceipt, 64)
			clientDone := make(chan []byte, 1)
			go func() {
				request, err := http.NewRequestWithContext(ctx, http.MethodPost, second.URL, nil)
				if err != nil {
					failures <- err
					return
				}
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					failures <- err
					return
				}
				defer response.Body.Close()
				reader := bufio.NewReader(response.Body)
				var received bytes.Buffer
				for {
					frame, payload, err := preconnectRelayReadFrame(reader)
					if err != nil {
						if err != io.EOF {
							failures <- err
						}
						clientDone <- received.Bytes()
						return
					}
					received.Write(frame)
					kind, semantic, visible, _ := preconnectRelayEventDecision(payload)
					clientReceipts <- preconnectRelayReceipt{event: kind, semantic: semantic, visible: visible, flushed: true}
				}
			}()
			waitFor := func(label, event string, receipts <-chan preconnectRelayReceipt) preconnectRelayReceipt {
				t.Helper()
				for {
					select {
					case got := <-receipts:
						if got.event == event {
							return got
						}
					case err := <-failures:
						t.Fatalf("%s relay error: %v", label, err)
					case <-ctx.Done():
						t.Fatalf("%s did not receive %s before real upstream release: %v", label, event, ctx.Err())
					}
				}
			}
			if multihop {
				for _, stage := range []struct {
					name     string
					receipts <-chan preconnectRelayReceipt
				}{{"first layer", firstReceipts}, {"second layer", secondReceipts}, {"final client", clientReceipts}} {
					got := waitFor(stage.name, "response.reasoning_summary_text.delta", stage.receipts)
					if !got.semantic || !got.visible || !got.flushed {
						t.Fatalf("%s did not start both TTFT modes and flush: %+v", stage.name, got)
					}
				}
			} else {
				got := waitFor("first layer", "response.output_item.added", firstReceipts)
				if !got.semantic || got.visible || got.flushed {
					t.Fatalf("legacy empty event must start semantic TTFT only, without forwarding: %+v", got)
				}
				select {
				case got := <-secondReceipts:
					t.Fatalf("legacy empty event unexpectedly escaped buffering: %+v", got)
				default:
				}
			}
			// Real upstream output is released only after the assertions above.
			// Thus early receipt cannot accidentally be credited to the final body.
			release()
			select {
			case wire := <-clientDone:
				events := parsePreconnectResponsesTestEvents(t, wire)
				if len(events) == 0 || events[len(events)-1].name != "response.completed" {
					t.Fatalf("two-hop stream did not complete: %s", wire)
				}
				assertPreconnectResponsesTestSequence(t, events)
			case err := <-failures:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		})
	}
}
