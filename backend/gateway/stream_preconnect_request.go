package gateway

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
)

const streamPreconnectReplayIDPrefix = "rs_uops_preconnect_"

// A synthetic Responses output item cannot be persisted by the upstream. Only
// explicitly stateless requests can safely receive it: omitting store must not
// silently opt into a mode that changes server-side conversation identity.
func streamPreconnectStatelessEligible(body []byte) bool {
	object, ok := streamPreconnectRequestObject(body)
	if !ok || !streamPreconnectRawEquals(object["store"], "false") {
		return false
	}
	if raw, exists := object["previous_response_id"]; exists && !streamPreconnectRawEquals(raw, "null") {
		value, valid := streamPreconnectRequestString(raw)
		if !valid || value != "" {
			return false
		}
	}
	if raw, exists := object["conversation"]; exists && !streamPreconnectRawEquals(raw, "null") {
		return false
	}
	if raw, exists := object["background"]; exists && !streamPreconnectRawEquals(raw, "null") && !streamPreconnectRawEquals(raw, "false") {
		return false
	}
	return true
}

// Clients may replay prior Responses output as the next request's input. Strip
// only our exact, empty synthetic item (or its exact item_reference), even if
// preconnect has since been disabled. Real reasoning, unknown shapes, and all
// unrelated request fields remain untouched. RawMessage preserves number
// precision instead of converting the request through float64.
func stripStreamPreconnectReplay(body []byte) []byte {
	// The normal path has no synthetic ID. A Unicode-escaped ID still needs to
	// be decoded, so do not take this shortcut when any Unicode escape exists.
	if !bytes.Contains(body, []byte(streamPreconnectReplayIDPrefix)) && !bytes.Contains(body, []byte{92, 'u'}) {
		return body
	}
	object, ok := streamPreconnectRequestObject(body)
	if !ok {
		return body
	}
	input := bytes.TrimSpace(object["input"])
	if len(input) == 0 || input[0] != '[' {
		return body
	}
	var items []json.RawMessage
	if err := json.Unmarshal(input, &items); err != nil {
		return body
	}
	kept := make([]json.RawMessage, 0, len(items))
	for _, item := range items {
		if !streamPreconnectIsReplayItem(item) {
			kept = append(kept, item)
		}
	}
	if len(kept) == len(items) {
		return body
	}
	rewrittenInput, err := json.Marshal(kept)
	if err != nil {
		return body
	}
	object["input"] = rewrittenInput
	rewrittenBody, err := json.Marshal(object)
	if err != nil {
		return body
	}
	return rewrittenBody
}

func streamPreconnectIsReplayItem(raw []byte) bool {
	item, ok := streamPreconnectRequestObject(raw)
	if !ok {
		return false
	}
	id, ok := streamPreconnectRequestString(item["id"])
	if !ok || !streamPreconnectIsReplayID(id) {
		return false
	}
	kind, ok := streamPreconnectRequestString(item["type"])
	if !ok {
		return false
	}
	switch kind {
	case "item_reference":
		return len(item) == 2
	case "reasoning":
		for key := range item {
			switch key {
			case "id", "type", "summary", "content", "encrypted_content", "status":
			default:
				return false
			}
		}
		if !streamPreconnectEmptyArray(item["summary"]) {
			return false
		}
		if content, exists := item["content"]; exists && !streamPreconnectRawEquals(content, "null") && !streamPreconnectEmptyArray(content) {
			return false
		}
		if encrypted, exists := item["encrypted_content"]; exists && !streamPreconnectRawEquals(encrypted, "null") {
			value, valid := streamPreconnectRequestString(encrypted)
			if !valid || value != "" {
				return false
			}
		}
		if status, exists := item["status"]; exists {
			value, valid := streamPreconnectRequestString(status)
			if !valid || value != "completed" {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func streamPreconnectIsReplayID(id string) bool {
	if len(id) != len(streamPreconnectReplayIDPrefix)+32 || !strings.HasPrefix(id, streamPreconnectReplayIDPrefix) {
		return false
	}
	for _, ch := range id[len(streamPreconnectReplayIDPrefix):] {
		if (ch < '0' || ch > '9') && (ch < 'a' || ch > 'f') {
			return false
		}
	}
	return true
}

func streamPreconnectEmptyArray(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) >= 2 && trimmed[0] == '[' && trimmed[len(trimmed)-1] == ']' && len(bytes.TrimSpace(trimmed[1:len(trimmed)-1])) == 0
}

func streamPreconnectRawEquals(raw []byte, value string) bool {
	return bytes.Equal(bytes.TrimSpace(raw), []byte(value))
}

func streamPreconnectRequestString(raw []byte) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", false
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", false
	}
	return value, true
}

// Reject duplicate keys instead of letting a decoder's last-value-wins policy
// turn an ambiguous request or item into an eligible one.
func streamPreconnectRequestObject(raw []byte) (map[string]json.RawMessage, bool) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, false
		}
		key, ok := token.(string)
		if !ok {
			return nil, false
		}
		if _, exists := object[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, false
		}
		object[key] = value
	}
	end, err := decoder.Token()
	if err != nil || end != json.Delim('}') {
		return nil, false
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	return object, true
}
