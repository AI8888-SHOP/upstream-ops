package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ImageGenerationRequest converts text-only requests. Images generations never
// receives stream: client streaming is synthesized from the buffered result.
func ImageGenerationRequest(inbound Kind, body []byte, model string) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil || in == nil {
		return nil, fmt.Errorf("invalid image generation request: expected a JSON object")
	}
	for _, key := range []string{"tools", "functions", "tool_calls", "function_call", "image", "images", "image_url", "input_image", "mask"} {
		if v := in[key]; imageGenerationHasValue(v) {
			return nil, fmt.Errorf("image generation accepts text only; images/tools (%s) require /v1/images/edits for editing or a tool-capable endpoint", key)
		}
	}
	var prompts []string
	add := func(role string, content any) error {
		text, err := imageGenerationText(content)
		if err != nil {
			return err
		}
		if role == "assistant" {
			return nil
		}
		if role != "user" && role != "system" && role != "developer" {
			return fmt.Errorf("image generation does not support role %q; use /v1/images/edits for image editing", role)
		}
		if strings.TrimSpace(text) != "" {
			prompts = append(prompts, role+": "+text)
		}
		return nil
	}
	if v, exists := in["prompt"]; exists {
		text, ok := v.(string)
		if !ok {
			return nil, fmt.Errorf("image generation prompt must be text")
		}
		if strings.TrimSpace(text) != "" {
			prompts = append(prompts, text)
		}
	}
	if NormalizeKind(inbound) == KindAnthropic {
		if err := add("system", in["system"]); err != nil {
			return nil, err
		}
	}
	if NormalizeKind(inbound) == KindOpenAIResponses {
		if err := add("system", in["instructions"]); err != nil {
			return nil, err
		}
		switch input := in["input"].(type) {
		case string:
			if err := add("user", input); err != nil {
				return nil, err
			}
		case []any:
			for _, item := range input {
				if text, ok := item.(string); ok {
					if err := add("user", text); err != nil {
						return nil, err
					}
					continue
				}
				msg, ok := item.(map[string]any)
				if !ok {
					return nil, fmt.Errorf("invalid image generation input item")
				}
				if typ, _ := msg["type"].(string); typ != "" && typ != "message" {
					return nil, fmt.Errorf("image generation accepts text messages only (got %q); use /v1/images/edits for image editing", typ)
				}
				role, _ := msg["role"].(string)
				if role == "" {
					role = "user"
				}
				if err := imageGenerationValidateMessage(msg); err != nil {
					return nil, err
				}
				if err := add(role, msg["content"]); err != nil {
					return nil, err
				}
			}
		case nil:
		default:
			return nil, fmt.Errorf("image generation input must be text or text messages")
		}
	}
	if raw, exists := in["messages"]; exists {
		messages, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("image generation messages must be an array")
		}
		for _, raw := range messages {
			msg, ok := raw.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("invalid image generation message")
			}
			if err := imageGenerationValidateMessage(msg); err != nil {
				return nil, err
			}
			role, _ := msg["role"].(string)
			if err := add(role, msg["content"]); err != nil {
				return nil, err
			}
		}
	}
	if len(prompts) == 0 {
		return nil, fmt.Errorf("image generation requires a non-empty text prompt; use /v1/images/edits for image editing")
	}
	if model == "" {
		model, _ = in["model"].(string)
	}
	out := map[string]any{"model": model, "prompt": strings.Join(prompts, "\n\n")}
	for _, key := range []string{"n", "size", "quality", "response_format", "style", "background", "output_format", "output_compression", "moderation", "user"} {
		if v, ok := in[key]; ok {
			out[key] = v
		}
	}
	return json.Marshal(out)
}

func imageGenerationHasValue(v any) bool {
	switch v := v.(type) {
	case nil:
		return false
	case []any:
		return len(v) > 0
	case string:
		return strings.TrimSpace(v) != ""
	default:
		return true
	}
}

func imageGenerationValidateMessage(msg map[string]any) error {
	for _, key := range []string{"tool_calls", "tool_call_id", "function_call", "image", "images"} {
		if imageGenerationHasValue(msg[key]) {
			return fmt.Errorf("image generation accepts text only; %s is unsupported; use /v1/images/edits for image editing", key)
		}
	}
	return nil
}

func imageGenerationText(content any) (string, error) {
	switch v := content.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []any:
		var texts []string
		for _, raw := range v {
			part, ok := raw.(map[string]any)
			if !ok {
				return "", fmt.Errorf("image generation content must contain text parts only; use /v1/images/edits for image editing")
			}
			typ, _ := part["type"].(string)
			if typ != "text" && typ != "input_text" && typ != "output_text" {
				return "", fmt.Errorf("image generation does not support content type %q; use /v1/images/edits for image editing", typ)
			}
			text, ok := part["text"].(string)
			if !ok {
				return "", fmt.Errorf("image generation text part must contain text")
			}
			texts = append(texts, text)
		}
		return strings.Join(texts, "\n"), nil
	default:
		return "", fmt.Errorf("image generation content must be text; use /v1/images/edits for image editing")
	}
}

func imageGenerationError(message string) []byte {
	return mustJSON(map[string]any{"error": map[string]any{"type": "upstream_error", "code": "invalid_image_response", "message": message}})
}

// imageGenerationChat preserves error envelopes verbatim. The bool indicates
// an error body, so callers never wrap it in a successful completion or SSE.
func imageGenerationChat(body []byte, model string) ([]byte, bool) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil || in == nil {
		return imageGenerationError("invalid image generation response: expected a JSON object"), true
	}
	if _, exists := in["error"]; exists {
		return body, true
	}
	if in["type"] == "error" {
		return body, true
	}
	data, _ := in["data"].([]any)
	if len(data) == 0 {
		return imageGenerationError("image generation response has missing or empty data"), true
	}
	var images []string
	for i, raw := range data {
		item, _ := raw.(map[string]any)
		url, _ := item["url"].(string)
		if strings.TrimSpace(url) == "" {
			b64, _ := item["b64_json"].(string)
			if strings.TrimSpace(b64) == "" {
				return imageGenerationError(fmt.Sprintf("image generation data[%d] has no url or b64_json", i)), true
			}
			format, _ := item["output_format"].(string)
			if format == "" {
				format, _ = in["output_format"].(string)
			}
			switch format {
			case "jpg", "jpeg":
				format = "jpeg"
			case "webp":
			default:
				format = "png"
			}
			url = "data:image/" + format + ";base64," + b64
		}
		images = append(images, fmt.Sprintf("![image %d](%s)", i+1, url))
	}
	if model == "" {
		model, _ = in["model"].(string)
	}
	out := map[string]any{"id": generateResponsesStreamID("chatcmpl_"), "object": "chat.completion", "model": model,
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": strings.Join(images, "\n\n")}, "finish_reason": "stop"}}}
	if created, ok := in["created"]; ok {
		out["created"] = created
	}
	if usage, ok := in["usage"].(map[string]any); ok {
		// Keep original image usage and expose Chat's canonical aliases.
		converted := openAIUsageToChat(usage)
		for key, value := range usage {
			converted[key] = value
		}
		out["usage"] = converted
	}
	return mustJSON(out), false
}

func imageGenerationResponse(inbound Kind, body []byte, model string, stream bool) []byte {
	chat, failed := imageGenerationChat(body, model)
	if failed {
		return chat
	}
	if NormalizeKind(inbound) == KindOpenAIImages {
		return body
	}
	var original map[string]any
	_ = json.Unmarshal(chat, &original)
	if !stream {
		out := chat
		var err error
		switch NormalizeKind(inbound) {
		case KindAnthropic:
			out, err = OpenAIToAnthropicResponse(chat, model)
		case KindOpenAIResponses:
			out, err = OpenAIChatToResponsesResponse(chat, model)
		}
		if err != nil {
			return imageGenerationError(err.Error())
		}
		var converted map[string]any
		_ = json.Unmarshal(out, &converted)
		imageGenerationMetadata(converted, original, NormalizeKind(inbound) == KindOpenAIResponses)
		return mustJSON(converted)
	}
	chunks := imageGenerationChatChunks(original)
	if NormalizeKind(inbound) == KindOpenAIResponses {
		converter := NewChatToResponsesStream(model)
		var frames [][]byte
		for _, chunk := range chunks {
			frames = append(frames, converter.FeedData(string(chunk))...)
		}
		frames = append(frames, converter.Close()...)
		// Existing converter emits all required response.* events. Restore image
		// usage details and timestamp on response objects it constructs.
		var result [][]byte
		for _, event := range parseSSEEvents(string(JoinSSEFrames(frames))) {
			var payload map[string]any
			_ = json.Unmarshal([]byte(event.Data), &payload)
			if response, ok := payload["response"].(map[string]any); ok {
				if created, exists := original["created"]; exists {
					response["created_at"] = created
				}
				if event.Event == "response.completed" {
					imageGenerationMetadata(response, original, true)
				}
			}
			result = append(result, encodeSSEFrame(event.Event, payload))
		}
		return JoinSSEFrames(result)
	}
	var frames [][]byte
	for _, chunk := range chunks {
		frames = append(frames, openAISSEFrame(chunk))
	}
	frames = append(frames, []byte("data: [DONE]\n\n"))
	sse := JoinSSEFrames(frames)
	if NormalizeKind(inbound) == KindAnthropic {
		var result [][]byte
		for _, event := range parseSSEEvents(string(OpenAISSEToAnthropicSSE(sse, model))) {
			var payload map[string]any
			_ = json.Unmarshal([]byte(event.Data), &payload)
			if message, ok := payload["message"].(map[string]any); ok {
				if created, exists := original["created"]; exists {
					message["created"] = created
				}
			}
			if event.Event == "message_delta" {
				imageGenerationMetadata(payload, original, false)
			}
			result = append(result, encodeSSEFrame(event.Event, payload))
		}
		return JoinSSEFrames(result)
	}
	return sse
}

func imageGenerationMetadata(out, original map[string]any, responses bool) {
	if created, ok := original["created"]; ok {
		if responses {
			out["created_at"] = created
		} else {
			out["created"] = created
		}
	}
	if usage, ok := original["usage"].(map[string]any); ok {
		merged := map[string]any{}
		for key, value := range usage {
			merged[key] = value
		}
		// Retain canonical token names generated for the client protocol as well as
		// image-specific details (including image_tokens and text_tokens).
		if converted, ok := out["usage"].(map[string]any); ok {
			for key, value := range converted {
				merged[key] = value
			}
		}
		out["usage"] = merged
	}
}

func imageGenerationChatChunks(chat map[string]any) [][]byte {
	choices := chat["choices"].([]any)
	message := choices[0].(map[string]any)["message"].(map[string]any)
	chunk := func(delta map[string]any, finish any, usage bool) []byte {
		out := map[string]any{"id": chat["id"], "object": "chat.completion.chunk", "model": chat["model"]}
		if created, ok := chat["created"]; ok {
			out["created"] = created
		}
		if usage {
			out["choices"] = []any{}
			out["usage"] = chat["usage"]
		} else {
			out["choices"] = []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}
		}
		return mustJSON(out)
	}
	chunks := [][]byte{chunk(map[string]any{"role": "assistant"}, nil, false), chunk(map[string]any{"content": message["content"]}, nil, false), chunk(map[string]any{}, "stop", false)}
	if _, ok := chat["usage"]; ok {
		chunks = append(chunks, chunk(nil, nil, true))
	}
	return chunks
}
