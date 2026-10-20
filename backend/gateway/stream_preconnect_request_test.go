package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestStreamPreconnectStatelessEligible(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"explicit stateless", `{"store":false}`, true},
		{"normal request", `{"model":"model","store":false,"input":[{"role":"user","content":"hello"}],"stream":true}`, true},
		{"nullable optional fields", `{"store":false,"previous_response_id":null,"conversation":null,"background":null}`, true},
		{"empty prior id", `{"store":false,"previous_response_id":"","background":false}`, true},
		{"whitespace", `  { "store" : false, "previous_response_id" : "" }  `, true},
		{"omitted store inherits stateful default", `{"input":"hello"}`, false},
		{"store true", `{"store":true}`, false},
		{"store null", `{"store":null}`, false},
		{"store string", `{"store":"false"}`, false},
		{"store number", `{"store":0}`, false},
		{"store array", `{"store":[]}`, false},
		{"previous response", `{"store":false,"previous_response_id":"resp_real"}`, false},
		{"previous whitespace is not empty", `{"store":false,"previous_response_id":" "}`, false},
		{"previous wrong type", `{"store":false,"previous_response_id":false}`, false},
		{"conversation id", `{"store":false,"conversation":"conv_real"}`, false},
		{"empty conversation string", `{"store":false,"conversation":""}`, false},
		{"conversation object", `{"store":false,"conversation":{}}`, false},
		{"conversation wrong type", `{"store":false,"conversation":false}`, false},
		{"background request", `{"store":false,"background":true}`, false},
		{"background string", `{"store":false,"background":"false"}`, false},
		{"background number", `{"store":false,"background":0}`, false},
		{"duplicate store", `{"store":true,"store":false}`, false},
		{"duplicate escaped key", `{"store":false,"\u0073tore":false}`, false},
		{"duplicate previous id", `{"store":false,"previous_response_id":"resp_real","previous_response_id":null}`, false},
		{"trailing object", `{"store":false}{}`, false},
		{"trailing junk", `{"store":false} junk`, false},
		{"malformed", `{"store":false,}`, false},
		{"array root", `[{"store":false}]`, false},
		{"null root", "null", false},
		{"empty", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := streamPreconnectStatelessEligible([]byte(tc.body)); got != tc.want {
				t.Fatalf("eligible = %v, want %v for %s", got, tc.want, tc.body)
			}
		})
	}
}

func TestStripStreamPreconnectReplayRecognizesOnlyEmptySyntheticItems(t *testing.T) {
	const id = "rs_uops_preconnect_0123456789abcdef0123456789abcdef"
	for _, tc := range []struct {
		name   string
		item   string
		remove bool
	}{
		{"minimal reasoning", `{"id":"ID","type":"reasoning","summary":[]}`, true},
		{"completed reasoning", `{"id":"ID","type":"reasoning","summary":[],"status":"completed"}`, true},
		{"explicit empty payload", `{"id":"ID","type":"reasoning","summary":[ ],"content":[],"encrypted_content":""}`, true},
		{"nullable payload", `{"id":"ID","type":"reasoning","summary":[],"content":null,"encrypted_content":null}`, true},
		{"reference", `{"id":"ID","type":"item_reference"}`, true},
		{"escaped id", `{"id":"rs_\u0075ops_preconnect_0123456789abcdef0123456789abcdef","type":"reasoning","summary":[]}`, true},
		{"real id", `{"id":"rs_real","type":"reasoning","summary":[]}`, false},
		{"uppercase hex", `{"id":"rs_uops_preconnect_0123456789ABCDEF0123456789abcdef","type":"reasoning","summary":[]}`, false},
		{"short id", `{"id":"rs_uops_preconnect_0123456789abcdef","type":"reasoning","summary":[]}`, false},
		{"long id", `{"id":"IDa","type":"reasoning","summary":[]}`, false},
		{"nonhex id", `{"id":"rs_uops_preconnect_0123456789abcdef0123456789abcdeg","type":"reasoning","summary":[]}`, false},
		{"extra prefix", `{"id":"prefix_ID","type":"reasoning","summary":[]}`, false},
		{"different type", `{"id":"ID","type":"message","summary":[]}`, false},
		{"missing summary", `{"id":"ID","type":"reasoning"}`, false},
		{"null summary", `{"id":"ID","type":"reasoning","summary":null}`, false},
		{"summary text", `{"id":"ID","type":"reasoning","summary":[{"type":"summary_text","text":"real"}]}`, false},
		{"content text", `{"id":"ID","type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"real"}]}`, false},
		{"content string", `{"id":"ID","type":"reasoning","summary":[],"content":""}`, false},
		{"encrypted real", `{"id":"ID","type":"reasoning","summary":[],"encrypted_content":"encrypted"}`, false},
		{"encrypted wrong type", `{"id":"ID","type":"reasoning","summary":[],"encrypted_content":[]}`, false},
		{"unknown field", `{"id":"ID","type":"reasoning","summary":[],"future_field":null}`, false},
		{"in progress", `{"id":"ID","type":"reasoning","summary":[],"status":"in_progress"}`, false},
		{"null status", `{"id":"ID","type":"reasoning","summary":[],"status":null}`, false},
		{"reference extra field", `{"id":"ID","type":"item_reference","status":"completed"}`, false},
		{"duplicate payload", `{"id":"ID","type":"reasoning","summary":["real"],"summary":[]}`, false},
		{"duplicate id", `{"id":"rs_real","id":"ID","type":"reasoning","summary":[]}`, false},
		{"numeric item", "123", false},
		{"string item", `"ID"`, false},
		{"null item", "null", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item := strings.ReplaceAll(tc.item, "ID", id)
			body := []byte(`{"input":[` + item + `],"store":false}`)
			got := stripStreamPreconnectReplay(body)
			if !tc.remove {
				if !bytes.Equal(got, body) || &got[0] != &body[0] {
					t.Fatalf("legitimate/ambiguous item changed: %s", got)
				}
				return
			}
			var object map[string]json.RawMessage
			if err := json.Unmarshal(got, &object); err != nil {
				t.Fatal(err)
			}
			if string(object["input"]) != "[]" {
				t.Fatalf("synthetic item not removed: %s", got)
			}
		})
	}
}

func TestStripStreamPreconnectReplayPreservesNoOpBytes(t *testing.T) {
	const marker = "rs_uops_preconnect_0123456789abcdef0123456789abcdef"
	for _, body := range []string{
		"",
		`  { "input" : [ ], "number" : 9007199254740993 }  `,
		`{"input":"` + marker + `"}`,
		`{"input":null,"metadata":"` + marker + `"}`,
		`{"metadata":"` + marker + `"}`,
		`{"input":[{"id":"` + marker + `","type":"reasoning","summary":[]}],"input":[]}`,
		`{"input":[{"id":"` + marker + `","type":"reasoning","summary":[]}]} {}`,
		`{"input":[{"id":"` + marker + `","type":"reasoning","summary":[]}],}`,
		`[{"id":"` + marker + `","type":"reasoning","summary":[]}]`,
	} {
		original := []byte(body)
		got := stripStreamPreconnectReplay(original)
		if !bytes.Equal(got, original) {
			t.Fatalf("no-op changed request bytes: %q -> %q", original, got)
		}
	}
}

func TestStripStreamPreconnectReplayPreservesRealItemsAndNumberPrecision(t *testing.T) {
	body := []byte(`{
		"model":"model", "store":true, "background":true,
		"big":9007199254740993123456789,
		"decimal":0.12345678901234567890123456789,
		"exponent":1e+100,
		"input":[
			{"id":"rs_real","type":"reasoning","summary":[],"encrypted_content":"REAL"},
			{"id":"rs_uops_preconnect_0123456789abcdef0123456789abcdef","type":"reasoning","summary":[]},
			{"role":"user","content":"hello","number":9007199254740993},
			{"id":"rs_uops_preconnect_abcdef0123456789abcdef0123456789","type":"item_reference"},
			{"type":"item_reference","id":"rs_real"}
		]
	}`)
	got := stripStreamPreconnectReplay(body)
	if bytes.Equal(got, body) {
		t.Fatal("synthetic items were not removed")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(got, &object); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{
		"big": "9007199254740993123456789", "decimal": "0.12345678901234567890123456789", "exponent": "1e+100", "store": "true", "background": "true",
	} {
		if string(object[key]) != want {
			t.Fatalf("%s = %s, want %s", key, object[key], want)
		}
	}
	var items []json.RawMessage
	if err := json.Unmarshal(object["input"], &items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("kept %d items, want 3: %s", len(items), object["input"])
	}
	for i, want := range []string{
		`{"id":"rs_real","type":"reasoning","summary":[],"encrypted_content":"REAL"}`,
		`{"role":"user","content":"hello","number":9007199254740993}`,
		`{"type":"item_reference","id":"rs_real"}`,
	} {
		if string(items[i]) != want {
			t.Fatalf("real item %d changed: %s, want %s", i, items[i], want)
		}
	}
	if again := stripStreamPreconnectReplay(got); !bytes.Equal(again, got) || &again[0] != &got[0] {
		t.Fatalf("cleanup is not idempotent: %s", again)
	}
}
