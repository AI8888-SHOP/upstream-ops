package gateway

import (
	"net/http"
	"testing"
)

func TestCandyCheckFinalAnswer(t *testing.T) {
	for _, answer := range []string{"21", " 21\n", "**21**", "`21`", "２１", "最终答案：21", "二十一颗糖果", "The answer is 21."} {
		if !candyCheckAnswerCorrect(answer) {
			t.Errorf("rejected correct answer %q", answer)
		}
	}
	for _, answer := range []string{"", "121", "210", "21.0", "2 1", "2\n1", "21 或 22", "不是21", "答案是21，但也可能22", "21\n21", "reasoning: 21", "{\"answer\":21}"} {
		if candyCheckAnswerCorrect(answer) {
			t.Errorf("accepted invalid answer %q", answer)
		}
	}
}

func TestCandyCheckParseCompleteResponses(t *testing.T) {
	cases := []struct {
		name, body, want string
		bad              bool
	}{
		{"chat", `{"choices":[{"message":{"content":"21","reasoning_content":"22"},"finish_reason":"stop"}]}`, "21", false},
		{"messages", `{"content":[{"type":"thinking","thinking":"22"},{"type":"text","text":"21"}],"stop_reason":"end_turn"}`, "21", false},
		{"responses duplicate convenience", `{"object":"response","status":"completed","output_text":"21","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"21"}]}]}`, "21", false},
		{"raw", `21`, "", true},
		{"truncated", `{"choices":[{"message":{"content":"21"},"finish_reason":"length"}]}`, "", true},
		{"incomplete", `{"status":"incomplete","output_text":"21"}`, "", true},
		{"error", `{"error":{"message":"21"},"choices":[{"message":{"content":"21"},"finish_reason":"stop"}]}`, "", true},
		{"empty", `{"status":"completed","output":[]}`, "", true},
		{"responses stream", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.output_text.done\",\"text\":\"21\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output_text\":\"21\"}}\n\n", "21", false},
		{"missing terminal", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\n", "21", true},
		{"conflicting snapshot", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"22\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output_text\":\"21\"}}\n\n", "22", true},
		{"multiple chat choices", "data: {\"choices\":[{\"delta\":{\"content\":\"2\"},\"finish_reason\":\"stop\"},{\"delta\":{\"content\":\"1\"},\"finish_reason\":\"stop\"}]}\n\n", "", true},
		{"stream error after answer", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"21\"}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\"}}\n\n", "21", true},
		{"chat stream", "data: {\"choices\":[{\"delta\":{\"content\":\"21\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n", "21", false},
		{"anthropic stream", "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"21\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n", "21", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answer, err := parseCandyCheckAnswer([]byte(tc.body), http.Header{})
			if (err != nil) != tc.bad || answer != tc.want {
				t.Fatalf("answer=%q err=%v", answer, err)
			}
		})
	}
}
