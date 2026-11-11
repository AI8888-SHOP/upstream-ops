package protocol

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func decodeImageTest(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("invalid JSON %s: %v", body, err)
	}
	return result
}

func TestImageGenerationKindAndPath(t *testing.T) {
	if NormalizeKind(" OPENAI_IMAGES ") != KindOpenAIImages {
		t.Fatal("image kind did not resolve")
	}
	for _, path := range []string{"", "/v1/chat/completions", "/v1/responses", "/v1/messages"} {
		if got := PathFor(KindOpenAIImages, path); got != "/v1/images/generations" {
			t.Fatalf("PathFor(%q) = %q", path, got)
		}
	}
	if got := PathFor(KindOpenAIImages, "/v1/images/edits"); got != "/v1/images/edits" {
		t.Fatalf("passthrough changed: %s", got)
	}
}

func TestImageGenerationRequests(t *testing.T) {
	tests := []struct {
		name         string
		kind         Kind
		body, prompt string
	}{
		{"chat", KindOpenAIChat, `{"messages":[{"role":"system","content":"draw well"},{"role":"user","content":[{"type":"text","text":"cat"}]},{"role":"assistant","content":"old response"},{"role":"user","content":"blue"}],"stream":true,"max_tokens":400,"n":2,"size":"1024x1024","quality":"high","response_format":"b64_json","style":"vivid","background":"transparent","output_format":"webp","output_compression":80,"moderation":"low","user":"alice","temperature":1}`, "system: draw well\n\nuser: cat\n\nuser: blue"},
		{"responses", KindOpenAIResponses, `{"instructions":"draw well","input":[{"role":"user","content":[{"type":"input_text","text":"cat"}]},{"role":"assistant","content":[{"type":"output_text","text":"history"}]}]}`, "system: draw well\n\nuser: cat"},
		{"responses string", KindOpenAIResponses, `{"input":"cat"}`, "user: cat"},
		{"anthropic", KindAnthropic, `{"system":[{"type":"text","text":"draw well"}],"messages":[{"role":"user","content":"cat"},{"role":"assistant","content":"history"}]}`, "system: draw well\n\nuser: cat"},
		{"native", KindOpenAIImages, `{"model":"original","prompt":"cat","stream":true}`, "cat"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, converted, err := ConvertRequest(tt.kind, KindOpenAIImages, []byte(tt.body), "image-model", true)
			if err != nil || !converted {
				t.Fatalf("conversion failed: %v %v", converted, err)
			}
			got := decodeImageTest(t, result)
			if got["prompt"] != tt.prompt || got["model"] != "image-model" {
				t.Fatalf("wrong payload: %s", result)
			}
			for _, key := range []string{"stream", "max_tokens", "messages", "input", "temperature"} {
				if _, exists := got[key]; exists {
					t.Errorf("leaked field %s", key)
				}
			}
			if tt.name == "chat" {
				for _, key := range []string{"n", "size", "quality", "response_format", "style", "background", "output_format", "output_compression", "moderation", "user"} {
					if _, exists := got[key]; !exists {
						t.Errorf("missing %s", key)
					}
				}
			}
		})
	}
}

func TestImageGenerationRejectsLossyRequests(t *testing.T) {
	tests := []struct {
		kind Kind
		body string
	}{
		{KindOpenAIChat, `{"messages":[{"role":"user","content":[{"type":"text","text":"edit"},{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]}`},
		{KindOpenAIResponses, `{"input":[{"role":"system","content":[{"type":"input_image","file_id":"file_1"}]}]}`},
		{KindAnthropic, `{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"a"}}]}]}`},
		{KindOpenAIChat, `{"prompt":"cat","messages":[{"role":"assistant","content":[{"type":"image_url","image_url":"a"}]}]}`},
		{KindOpenAIChat, `{"prompt":"cat","tools":[{"type":"function"}]}`},
		{KindOpenAIChat, `{"messages":[{"role":"assistant","tool_calls":[{"id":"a"}]}]}`},
		{KindOpenAIResponses, `{"input":[{"type":"function_call","name":"draw"}]}`},
		{KindAnthropic, `{"messages":[{"role":"user","content":[{"type":"tool_result","content":"cat"}]}]}`},
		{KindOpenAIImages, `{"prompt":"edit","image":"a"}`},
		{KindOpenAIChat, `{"messages":[{"role":"assistant","content":"history"}]}`},
		{KindOpenAIImages, `{"prompt":" "}`},
	}
	for _, tt := range tests {
		if _, converted, err := ConvertRequest(tt.kind, KindOpenAIImages, []byte(tt.body), "", true); err == nil || converted {
			t.Errorf("expected clear error: %s", tt.body)
		}
	}
}

const imageGenerationFixture = `{"created":1700000000,"output_format":"webp","data":[{"url":"https://example.com/a.png"},{"b64_json":"YWJj"}],"usage":{"input_tokens":11,"output_tokens":23,"total_tokens":34,"input_tokens_details":{"text_tokens":3,"image_tokens":8},"output_tokens_details":{"image_tokens":23}}}`

func TestImageGenerationResponses(t *testing.T) {
	for _, kind := range []Kind{KindOpenAIChat, KindOpenAIResponses, KindAnthropic} {
		result := ConvertResponse(kind, KindOpenAIImages, []byte(imageGenerationFixture), "image-model")
		got := decodeImageTest(t, result)
		if !strings.Contains(string(result), "![image 1](https://example.com/a.png)") || !strings.Contains(string(result), "data:image/webp;base64,YWJj") {
			t.Fatalf("missing markdown: %s", result)
		}
		created := "created"
		if kind == KindOpenAIResponses {
			created = "created_at"
		}
		if got[created] != float64(1700000000) {
			t.Errorf("lost timestamp: %s", result)
		}
		usage, _ := got["usage"].(map[string]any)
		if kind == KindOpenAIChat && (usage["prompt_tokens"] != float64(11) || usage["completion_tokens"] != float64(23)) {
			t.Errorf("missing chat token aliases: %s", result)
		}
		if usage["input_tokens"] != float64(11) || usage["output_tokens"] != float64(23) || usage["output_tokens_details"] == nil {
			t.Errorf("lost usage: %s", result)
		}
	}
}

func TestImageGenerationStreams(t *testing.T) {
	for _, kind := range []Kind{KindOpenAIChat, KindOpenAIResponses, KindAnthropic} {
		result := ConvertStreamResponse(kind, KindOpenAIImages, []byte(imageGenerationFixture), "image-model", nil, nil)
		events := parseSSEEvents(string(result))
		if len(events) == 0 {
			t.Fatalf("missing SSE for %s: %s", kind, result)
		}
		switch kind {
		case KindOpenAIChat:
			if len(events) != 4 {
				t.Fatalf("incomplete chat SSE: %s", result)
			}
			role := decodeImageTest(t, []byte(events[0].Data))["choices"].([]any)[0].(map[string]any)["delta"].(map[string]any)["role"]
			if role != "assistant" {
				t.Fatalf("missing role: %s", result)
			}
			last := decodeImageTest(t, []byte(events[3].Data))
			if len(last["choices"].([]any)) != 0 || last["usage"] == nil {
				t.Fatalf("missing usage frame: %s", result)
			}
		case KindOpenAIResponses:
			var names []string
			for i, event := range events {
				names = append(names, event.Event)
				payload := decodeImageTest(t, []byte(event.Data))
				if payload["sequence_number"] != float64(i) {
					t.Errorf("bad sequence: %s", event.Data)
				}
			}
			want := []string{"response.created", "response.output_item.added", "response.content_part.added", "response.output_text.delta", "response.output_text.done", "response.content_part.done", "response.output_item.done", "response.completed"}
			if !reflect.DeepEqual(names, want) {
				t.Fatalf("events = %v", names)
			}
			completed := decodeImageTest(t, []byte(events[len(events)-1].Data))["response"].(map[string]any)
			if completed["created_at"] != float64(1700000000) || completed["usage"].(map[string]any)["output_tokens_details"] == nil {
				t.Fatalf("lost metadata: %s", result)
			}
			if len(completed["output"].([]any)) != 1 {
				t.Fatalf("missing output: %s", result)
			}
		case KindAnthropic:
			if events[0].Event != "message_start" || events[len(events)-1].Event != "message_stop" || !strings.Contains(string(result), "text_delta") {
				t.Fatalf("incomplete anthropic SSE: %s", result)
			}
		}
	}
}

func TestImageGenerationErrorsRemainErrors(t *testing.T) {
	for _, kind := range []Kind{KindOpenAIChat, KindOpenAIResponses, KindAnthropic, KindOpenAIImages} {
		for _, stream := range []bool{false, true} {
			for _, body := range []string{`{"error":{"message":"upstream failed","code":"quota"}}`, `{"type":"error","error":"failed"}`, `{}`, `{"data":[]}`, `{"data":[{}]}`, `null`, ``} {
				var result []byte
				if stream {
					result = ConvertStreamResponse(kind, KindOpenAIImages, []byte(body), "", nil, nil)
				} else {
					result = ConvertResponse(kind, KindOpenAIImages, []byte(body), "")
				}
				got := decodeImageTest(t, result)
				if got["error"] == nil || got["choices"] != nil || got["output"] != nil {
					t.Errorf("error became success: %s", result)
				}
				if strings.Contains(body, `"error"`) && string(result) != body {
					t.Errorf("upstream error changed: %s", result)
				}
			}
		}
	}
}
