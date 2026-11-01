package apiaudit

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func auditDB(t *testing.T) (*storage.GatewayAPICallLogs, *gorm.DB) {
	t.Helper()
	db, e := gorm.Open(sqlite.Open("file:"+NewRequestID()+"?mode=memory&cache=shared"), &gorm.Config{})
	if e != nil {
		t.Fatal(e)
	}
	if e = db.AutoMigrate(&storage.GatewayAPICallLog{}); e != nil {
		t.Fatal(e)
	}
	sql, _ := db.DB()
	sql.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sql.Close() })
	return storage.NewGatewayAPICallLogs(db), db
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestAuditPendingAndStreamingUsageTail(t *testing.T) {
	repo, db := auditDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var n int64
		db.Model(&storage.GatewayAPICallLog{}).Where("status = ?", "pending").Count(&n)
		if n != 1 {
			t.Errorf("pending before HTTP=%d", n)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Request-Id", "up-123")
		fmt.Fprint(w, "data: {\"choices\":[{\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":60}}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()
	client := &http.Client{Transport: WrapTransport(nil, Options{Store: repo, Purpose: storage.GatewayAPICallPurposeCandyCheck})}
	req, _ := http.NewRequestWithContext(context.Background(), "POST", upstream.URL+"/v1/chat/completions?api_key=VERY_SECRET", strings.NewReader(`{"model":"test-model","stream":true,"messages":[{"role":"user","content":"PRIVATE"}]}`))
	resp, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	_, e = io.ReadAll(resp.Body)
	if e != nil {
		t.Fatal(e)
	}
	_ = resp.Body.Close()
	page, e := repo.List(storage.GatewayAPICallLogQuery{})
	if e != nil || len(page.Items) != 1 {
		t.Fatalf("%v %+v", e, page)
	}
	v := page.Items[0]
	if v.Status != "success" || !v.UsageKnown || v.InputTokens != 40 || v.CacheReadTokens != 60 || v.OutputTokens != 7 || v.UpstreamRequestID != "up-123" || v.Model != "test-model" || !v.Stream || v.EstimatedCost != nil {
		t.Fatalf("row=%+v", v)
	}
	if v.Endpoint != "/v1/chat/completions" || strings.Contains(fmt.Sprintf("%+v", v), "SECRET") || strings.Contains(fmt.Sprintf("%+v", v), "PRIVATE") {
		t.Fatalf("sensitive audit=%+v", v)
	}
}
func TestAuditTransportFailureAndEarlyClose(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport roundTripFunc
		want      string
	}{
		{"network", func(*http.Request) (*http.Response, error) { return nil, errors.New("token=PRIVATE") }, "error"},
		{"incomplete", func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))}, nil
		}, "incomplete"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := auditDB(t)
			req, _ := http.NewRequest("POST", "http://fixture/v1/responses", nil)
			resp, _ := (&http.Client{Transport: WrapTransport(tc.transport, Options{Store: repo, Purpose: "model_probe"})}).Do(req)
			if resp != nil {
				_, _ = io.ReadAll(resp.Body)
				_ = resp.Body.Close()
			}
			page, e := repo.List(storage.GatewayAPICallLogQuery{})
			if e != nil || len(page.Items) != 1 {
				t.Fatal(e)
			}
			v := page.Items[0]
			if v.Status != tc.want || v.UsageKnown || v.EstimatedCost != nil || strings.Contains(v.ErrorMessage, "PRIVATE") {
				t.Fatalf("%+v", v)
			}
		})
	}
}
func TestAuditAnthropicUsageAndNestedResponses(t *testing.T) {
	for _, tc := range []struct {
		name, body, ct       string
		input, output, cache int
	}{
		{"anthropic", "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":15,\"output_tokens\":0,\"cache_read_input_tokens\":90}}}\n\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":8}}\n\ndata: {\"type\":\"message_stop\"}\n\n", "text/event-stream", 15, 8, 90},
		{"responses", `{"object":"response","usage":{"input_tokens":100,"output_tokens":9,"input_tokens_details":{"cached_tokens":40}}}`, "application/json", 60, 9, 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo, _ := auditDB(t)
			tr := roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{tc.ct}}, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})
			path := "/v1/responses"
			if tc.name == "anthropic" {
				path = "/v1/messages"
			}
			req, _ := http.NewRequest("POST", "http://fixture"+path, nil)
			resp, e := (&http.Client{Transport: WrapTransport(tr, Options{Store: repo, Purpose: "gateway"})}).Do(req)
			if e != nil {
				t.Fatal(e)
			}
			_, _ = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			p, _ := repo.List(storage.GatewayAPICallLogQuery{})
			v := p.Items[0]
			if v.InputTokens != tc.input || v.OutputTokens != tc.output || v.CacheReadTokens != tc.cache || v.Status != "success" {
				t.Fatalf("%+v", v)
			}
		})
	}
}
func TestAuditRetryEveryPhysicalAttemptGetsUniqueRow(t *testing.T) {
	repo, _ := auditDB(t)
	var calls atomic.Int32
	tr := WrapTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	}), Options{Store: repo, Purpose: "channel_api"})
	id := NewRequestID()
	ctx := WithMetadata(context.Background(), storage.GatewayAPICallLog{RequestID: id, ChannelID: 9})
	for i := 0; i < 2; i++ {
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://fixture/api/subscription/PRIVATE_KEY", nil)
		resp, e := tr.RoundTrip(req)
		if e != nil {
			t.Fatal(e)
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}
	p, _ := repo.List(storage.GatewayAPICallLogQuery{})
	if calls.Load() != 2 || p.Total != 2 || p.Items[0].RequestID == p.Items[1].RequestID || p.Items[1].RequestID != id {
		t.Fatalf("%+v calls=%d", p, calls.Load())
	}
	if strings.Contains(p.Items[0].Endpoint, "PRIVATE") {
		t.Fatal("secret in URL path")
	}
}
func TestAuditUnavailableStoragePreventsUntrackedHTTP(t *testing.T) {
	repo, db := auditDB(t)
	sql, _ := db.DB()
	_ = sql.Close()
	var calls atomic.Int32
	tr := WrapTransport(roundTripFunc(func(*http.Request) (*http.Response, error) { calls.Add(1); return nil, nil }), Options{Store: repo, Purpose: "gateway"})
	req, _ := http.NewRequest("GET", "http://fixture/test", nil)
	_, e := tr.RoundTrip(req)
	if e == nil || calls.Load() != 0 {
		t.Fatalf("err=%v calls=%d", e, calls.Load())
	}
}

func TestAuditRedirectKeepsEachAttemptAndLogicalRequest(t *testing.T) {
	repo, _ := auditDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/start" {
			w.Header().Set("Location", "/end?token=PRIVATE")
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"usage":{"input_tokens":10,"output_tokens":2}}`)
	}))
	defer upstream.Close()
	ctx := WithMetadata(context.Background(), storage.GatewayAPICallLog{RequestID: NewRequestID(), GatewayRequestID: "logical-1"})
	req, _ := http.NewRequestWithContext(ctx, "POST", upstream.URL+"/start", strings.NewReader(`{"model":"fixture"}`))
	client := &http.Client{Transport: WrapTransport(nil, Options{Store: repo, Purpose: "gateway"})}
	resp, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	page, e := repo.List(storage.GatewayAPICallLogQuery{})
	if e != nil || page.Total != 2 {
		t.Fatalf("page=%+v err=%v", page, e)
	}
	if page.Items[0].RequestID == page.Items[1].RequestID {
		t.Fatal("redirect collided")
	}
	if page.Items[0].StatusCode != 200 || page.Items[1].StatusCode != 307 {
		t.Fatalf("rows=%+v", page.Items)
	}
	for _, r := range page.Items {
		if r.GatewayRequestID != "logical-1" || strings.Contains(fmt.Sprint(r), "PRIVATE") {
			t.Fatalf("row=%+v", r)
		}
	}
}
func TestManagementResponsesDoNotInventModelUsage(t *testing.T) {
	repo, _ := auditDB(t)
	tr := WrapTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"data":{"usage":{"input_tokens":500,"output_tokens":20}},"token":"PRIVATE"}`))}, nil
	}), Options{Store: repo, Purpose: storage.GatewayAPICallPurposeChannelAPI})
	req, _ := http.NewRequest("GET", "http://fixture/api/accounts", nil)
	resp, e := tr.RoundTrip(req)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	page, _ := repo.List(storage.GatewayAPICallLogQuery{})
	v := page.Items[0]
	if v.UsageKnown || v.InputTokens != 0 || v.EstimatedCost != nil || v.Status != "success" {
		t.Fatalf("%+v", v)
	}
}
