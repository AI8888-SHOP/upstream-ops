package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

const streamPreconnectChatPrefix = "chatcmpl-uops_preconnect_"

// preconnectChatStream 在真实上游产出之前先下发一个协议合法的 Chat Completions
// chunk，让按「第一个 data 事件」统计首字的中转立即开始计时。
//
// chunk 只携带 assistant 角色（多层模式再带一个 ASCII 空格），不伪造正文、
// 用量或结束原因；客户端拼装回答时它贡献零个字符。
type preconnectChatStream struct {
	id       string
	created  int64
	model    string
	space    bool
	preamble []byte
	rewrote  bool
}

func newPreconnectChatStream(model string, multihop bool) (*preconnectChatStream, error) {
	var entropy [16]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return nil, errors.New("create Chat Completions preconnect identity: " + err.Error())
	}
	s := &preconnectChatStream{
		id:      streamPreconnectChatPrefix + hex.EncodeToString(entropy[:]),
		created: time.Now().Unix(),
		model:   model,
		space:   multihop,
	}
	// 与 OpenAI 原生首帧一致：显式给空 content，拼装结果仍是零字符。
	delta := map[string]any{"role": "assistant", "content": ""}
	if s.space {
		delta["content"] = " "
	}
	chunk := map[string]any{
		"id":      s.id,
		"object":  "chat.completion.chunk",
		"created": s.created,
		"model":   model,
		"choices": []any{map[string]any{"index": 0, "delta": delta, "finish_reason": nil}},
	}
	raw, err := json.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	s.preamble = append(append([]byte("data: "), raw...), '\n', '\n')
	return s, nil
}

func (s *preconnectChatStream) bytes() []byte {
	if s == nil {
		return nil
	}
	return s.preamble
}

// rewriteFirst 让第一个真实 chunk 采用合成 ID，整条流看起来仍是同一个响应。
// 之后的分片原样透传：上游在整条流里保持同一个 id。
func (s *preconnectChatStream) rewriteFirst(payload []byte) []byte {
	if s == nil || s.rewrote {
		return payload
	}
	// 只在整段就是单个完整事件时改写，避免丢掉同一批里的后续事件。
	if !bytes.HasSuffix(payload, []byte("\n\n")) || bytes.Count(payload, []byte("\n\n")) != 1 {
		return payload
	}
	trimmed := bytes.TrimSpace(payload)
	if !bytes.HasPrefix(trimmed, []byte("data:")) || !bytes.Contains(trimmed, []byte(`"chat.completion.chunk"`)) {
		return payload
	}
	body := bytes.TrimSpace(bytes.TrimPrefix(trimmed, []byte("data:")))
	var chunk map[string]json.RawMessage
	if err := json.Unmarshal(body, &chunk); err != nil || chunk == nil {
		return payload
	}
	chunk["id"] = json.RawMessage(strconv.Quote(s.id))
	raw, err := json.Marshal(chunk)
	if err != nil {
		return payload
	}
	s.rewrote = true
	return append(append([]byte("data: "), raw...), '\n', '\n')
}

// streamPreconnectChatModel 取出请求体里的 model，供合成 chunk 回填。
func streamPreconnectChatModel(body []byte) string {
	object, ok := streamPreconnectRequestObject(body)
	if !ok {
		return ""
	}
	var model string
	if raw, exists := object["model"]; exists {
		if json.Unmarshal(raw, &model) != nil {
			return ""
		}
	}
	return model
}
