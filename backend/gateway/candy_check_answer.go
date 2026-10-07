package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strings"
)

var candyFinalAnswer = regexp.MustCompile(`^(?:(?:最终答案|答案|结果)\s*(?:是|为|[:：])?\s*|(?:the\s+)?answer\s*(?:is|:)?\s*)?(?:21|二十一)\s*(?:颗|粒|个)?\s*(?:糖果|糖|candies)?[。.!！]?$`)

func candyCheckAnswerCorrect(answer string) bool {
	answer = strings.TrimSpace(strings.ToLower(answer))
	answer = strings.Map(func(r rune) rune {
		if r >= '０' && r <= '９' {
			return r - '０' + '0'
		}
		return r
	}, answer)
	for _, wrap := range [][2]string{{"**", "**"}, {"`", "`"}, {"\"", "\""}, {"“", "”"}} {
		if len(answer) >= len(wrap[0])+len(wrap[1]) && strings.HasPrefix(answer, wrap[0]) && strings.HasSuffix(answer, wrap[1]) {
			answer = strings.TrimSpace(answer[len(wrap[0]) : len(answer)-len(wrap[1])])
		}
	}
	return candyFinalAnswer.MatchString(answer)
}

// Parse complete envelopes, never arbitrary raw text, reasoning fields or a
// substring of a partially delivered answer. SSE deltas and terminal snapshots
// often contain the same answer; only one representation is graded.
func parseCandyCheckAnswer(body []byte, headers http.Header) (string, error) {
	invalid := errors.New("检测响应格式无效、未正常结束或没有最终答案")
	sse := strings.Contains(strings.ToLower(headers.Get("Content-Type")), "text/event-stream") || looksLikeSSEBody(body)
	if !sse {
		var obj map[string]any
		if json.Unmarshal(body, &obj) != nil {
			return "", invalid
		}
		return candyJSONAnswer(obj)
	}
	var out strings.Builder
	completed := false
	var snapshot map[string]any
	for _, raw := range sseDataPayloads(body) {
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 {
			continue
		}
		if bytes.Equal(raw, []byte("[DONE]")) {
			continue
		} // a marker alone is not a successful finish
		var obj map[string]any
		if json.Unmarshal(raw, &obj) != nil {
			return out.String(), invalid
		}
		if err := upstreamStreamFailure("", string(raw)); err != nil {
			return out.String(), errors.New("检测上游返回流式错误")
		}
		typ, _ := obj["type"].(string)
		switch typ {
		case "response.completed", "response.done":
			snapshot, _ = obj["response"].(map[string]any)
			completed = true
		case "response.incomplete":
			return out.String(), invalid
		case "response.output_text.delta":
			text, _ := obj["delta"].(string)
			out.WriteString(text)
		case "content_block_start":
			block, _ := obj["content_block"].(map[string]any)
			if block["type"] == "text" {
				text, _ := block["text"].(string)
				out.WriteString(text)
			}
		case "content_block_delta":
			delta, _ := obj["delta"].(map[string]any)
			if delta["type"] == "text_delta" {
				text, _ := delta["text"].(string)
				out.WriteString(text)
			}
		case "message_delta":
			delta, _ := obj["delta"].(map[string]any)
			reason, _ := delta["stop_reason"].(string)
			if reason != "" && reason != "end_turn" && reason != "stop_sequence" {
				return out.String(), invalid
			}
		case "message_stop":
			completed = true
		default:
			if choices, ok := obj["choices"].([]any); ok {
				if len(choices) > 1 {
					return out.String(), invalid
				}
				for _, value := range choices {
					choice, _ := value.(map[string]any)
					if index, ok := choice["index"].(float64); ok && index != 0 {
						return out.String(), invalid
					}
					if reason, _ := choice["finish_reason"].(string); reason != "" {
						if reason != "stop" {
							return out.String(), invalid
						}
						completed = true
					}
					appendMessageText(&out, choice["delta"])
				}
			}
		}
	}
	if snapshot != nil {
		answer, err := candyJSONAnswer(snapshot)
		if err == nil {
			if out.Len() > 0 && strings.TrimSpace(out.String()) != answer {
				return out.String(), invalid
			}
			return answer, nil
		}
		// A completed event may omit output after sending deltas, but its
		// status/error still has to be successful.
		if snapshot["status"] != "completed" || snapshot["error"] != nil {
			return out.String(), invalid
		}
	}
	answer := strings.TrimSpace(out.String())
	if !completed || answer == "" {
		return answer, invalid
	}
	return answer, nil
}

func candyJSONAnswer(obj map[string]any) (string, error) {
	invalid := errors.New("检测响应格式无效、未正常结束或没有最终答案")
	if obj == nil || obj["error"] != nil {
		return "", invalid
	}
	if nested, ok := obj["response"].(map[string]any); ok {
		obj = nested
	}
	if obj == nil || obj["error"] != nil {
		return "", invalid
	}
	var out strings.Builder
	switch {
	case obj["choices"] != nil:
		choices, _ := obj["choices"].([]any)
		if len(choices) != 1 {
			return "", invalid
		}
		choice, _ := choices[0].(map[string]any)
		if choice["finish_reason"] != "stop" {
			return "", invalid
		}
		appendMessageText(&out, choice["message"])
	case obj["output"] != nil || obj["output_text"] != nil || obj["object"] == "response":
		if obj["status"] != "completed" {
			return "", invalid
		}
		if output, ok := obj["output"].([]any); ok {
			for _, value := range output {
				item, _ := value.(map[string]any)
				if item["type"] == "message" && item["role"] == "assistant" {
					appendMessageText(&out, item)
				}
			}
		}
		if out.Len() == 0 {
			text, _ := obj["output_text"].(string)
			out.WriteString(text)
		}
	case obj["content"] != nil:
		if obj["stop_reason"] != "end_turn" && obj["stop_reason"] != "stop_sequence" {
			return "", invalid
		}
		appendMessageText(&out, obj)
	default:
		return "", invalid
	}
	answer := strings.TrimSpace(out.String())
	if answer == "" {
		return "", invalid
	}
	return answer, nil
}
