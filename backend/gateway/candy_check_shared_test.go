package gateway

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestCandySharedKeyUsesResolvedRequestIdentity(t *testing.T) {
	base := &upstreamTarget{BaseURL: "https://source.example", APIKey: "key-a", Provider: &storage.GatewayProvider{ID: 1}}
	headers := http.Header{"User-Agent": {"same-UA"}, "X-Behavior": {"same"}, "Authorization": {"Bearer key-a"}}
	body, path := candyCheckRequest(protocol.KindOpenAIChat, "m", "medium")
	key := candyCheckSharedKey(base, protocol.KindOpenAIChat, "m", "medium", path, body, headers, "")
	clone := *base
	clone.Provider = &storage.GatewayProvider{ID: 999, ExtraHeadersJSON: `{"X-Behavior":"same"}`}
	if got := candyCheckSharedKey(&clone, protocol.KindOpenAIChat, "m", "medium", path, body, headers, ""); got != key {
		t.Fatal("provider ID/config spelling changed effective source identity")
	}
	cases := []struct {
		name                string
		target              *upstreamTarget
		kind                protocol.Kind
		model, effort, path string
		body                []byte
		headers             http.Header
		proxy               string
	}{
		{"key", &upstreamTarget{BaseURL: base.BaseURL, APIKey: "key-b"}, protocol.KindOpenAIChat, "m", "medium", path, body, headers, ""},
		{"source", &upstreamTarget{BaseURL: "https://other.example", APIKey: base.APIKey}, protocol.KindOpenAIChat, "m", "medium", path, body, headers, ""},
		{"model", base, protocol.KindOpenAIChat, "other", "medium", path, body, headers, ""},
		{"effort", base, protocol.KindOpenAIChat, "m", "high", path, body, headers, ""},
		{"protocol", base, protocol.KindOpenAIResponses, "m", "medium", path, body, headers, ""},
		{"body", base, protocol.KindOpenAIChat, "m", "medium", path, []byte(`{"different":true}`), headers, ""},
		{"headers", base, protocol.KindOpenAIChat, "m", "medium", path, body, http.Header{"User-Agent": {"same-UA"}, "X-Behavior": {"different"}}, ""},
		{"UA", base, protocol.KindOpenAIChat, "m", "medium", path, body, http.Header{"User-Agent": {"other-UA"}, "X-Behavior": {"same"}}, ""},
		{"proxy", base, protocol.KindOpenAIChat, "m", "medium", path, body, headers, "http://proxy.example:8080"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := candyCheckSharedKey(tc.target, tc.kind, tc.model, tc.effort, tc.path, tc.body, tc.headers, tc.proxy); got == key {
				t.Fatal("different upstream request reused cache key")
			}
		})
	}
	if strings.Contains(key, "key-a") || len(key) != 64 {
		t.Fatalf("key stores credential or malformed hash: %q", key)
	}
}

func TestCandySharedAcrossServicesRoutesRestartAndManualRefresh(t *testing.T) {
	var calls atomic.Int32
	var correct atomic.Bool
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var once sync.Once
	resume := func() { once.Do(func() { close(release) }) }
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		if n == 1 {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		answer := "22"
		if correct.Load() {
			answer = "21"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":1}}`, answer)
	}))
	defer upstream.Close()
	defer resume()
	db := openGatewayTestDB(t)
	groups, routes, providers := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db), storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("shared-candy")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("shared-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := &storage.GatewayProvider{Name: "shared", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	newService := func() *Service {
		s := NewService(storage.NewGatewayGroups(db), nil, storage.NewGatewayRoutes(db), storage.NewGatewayUsageLogs(db), nil, nil, nil, cipher, nil)
		s.SetProviders(storage.NewGatewayProviders(db))
		return s
	}
	var saved []storage.GatewayRoute
	for _, interval := range []int{2, 5, 10} {
		g := &storage.GatewayGroup{Name: fmt.Sprintf("shared-%d", interval), GatewayCandyCheckPolicy: storage.GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "m", CandyCheckReasoningEffort: "medium", CandyCheckIntervalMinutes: interval, CandyCheckCooldownMinutes: interval + 1}}
		if err := groups.Create(g); err != nil {
			t.Fatal(err)
		}
		if err := routes.SaveForGroup(g.ID, []storage.GatewayRoute{{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID}}); err != nil {
			t.Fatal(err)
		}
		list, err := routes.ListByGroupID(g.ID)
		if err != nil {
			t.Fatal(err)
		}
		saved = append(saved, list[0])
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 2)
	go func() { _, err := newService().RunCandyCheckNow(ctx, saved[0].ID); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first request missing")
	}
	go func() { _, err := newService().RunCandyCheckNow(ctx, saved[1].ID); done <- err }()
	// Hold the first request until the second service has a route lease and
	// has had a full polling turn to join the shared in-flight request.
	for {
		var state storage.GatewayRouteCandyCheck
		if db.First(&state, "route_id = ?", saved[1].ID).Error == nil && state.LeaseUntil != nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("second route lease missing")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-time.After(100 * time.Millisecond):
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	resume()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("same-source HTTP calls=%d want 1", calls.Load())
	}
	// A freshly constructed service/repository consumes persisted source state.
	if _, err := newService().runCandyCheck(ctx, saved[2], false); err != nil {
		t.Fatal(err)
	}
	var requestID string
	for i, route := range saved {
		got, err := routes.FindByID(route.ID)
		if err != nil {
			t.Fatal(err)
		}
		state := got.CandyCheck
		if state == nil || state.Status != "incorrect" || state.ObservedAt == nil || state.SourceRequestID == "" {
			t.Fatalf("route state: %+v", state)
		}
		if i == 0 {
			requestID = state.SourceRequestID
		} else if !state.Reused || state.SourceRequestID != requestID {
			t.Fatalf("reuse attribution: %+v", state)
		}
		if !state.CooldownUntil.Equal(state.ObservedAt.Add(time.Duration([]int{3, 6, 11}[i]) * time.Minute)) {
			t.Fatalf("group cooldown was not independent: %+v", state)
		}
		events, err := routes.CandyCheckHistory(route.ID)
		if err != nil || len(events) != 1 || events[0].SourceRequestID != requestID {
			t.Fatalf("history: %+v %v", events, err)
		}
	}
	var auditCount, usageCount int64
	if err := db.Model(&storage.GatewayAPICallLog{}).Where("purpose = ?", storage.GatewayAPICallPurposeCandyCheck).Count(&auditCount).Error; err != nil || auditCount != 1 {
		t.Fatalf("audit calls=%d %v", auditCount, err)
	}
	if err := db.Model(&storage.GatewayUsageLog{}).Count(&usageCount).Error; err != nil || usageCount != 0 {
		t.Fatalf("customer billed=%d %v", usageCount, err)
	}
	correct.Store(true)
	fresh, err := newService().RunCandyCheckNow(ctx, saved[2].ID)
	if err != nil || fresh == nil || fresh.Reused || fresh.Status != "correct" || fresh.SourceRequestID == requestID || calls.Load() != 2 {
		t.Fatalf("manual fresh result=%+v calls=%d err=%v", fresh, calls.Load(), err)
	}
}

func TestCandySharedWaitRejectsReleasedOldOrPendingResult(t *testing.T) {
	now := time.Now()
	old := now.Add(-time.Hour)
	for _, status := range []string{"pending", "skipped", "deferred", "correct"} {
		row := &storage.GatewayCandyCheckSharedResult{Status: status, ObservedAt: &old, SourceRequestID: "old"}
		a := &storage.GatewayCandyCheckSharedAcquire{InFlight: true, PreviousObservedAt: &old, PreviousRequestID: "old"}
		if candyCheckSharedUsable(row, now, 5*time.Minute) && sharedCandyObservationChanged(row, a) {
			t.Fatalf("released %s accepted", status)
		}
	}
}

func TestCandyStreamDoesNotStopOnInitialUsageOrNullUsage(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"chat", "data: {\"choices\":[{\"delta\":{},\"finish_reason\":null}],\"usage\":null}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"21\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n"},
		{"anthropic", "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"21\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\ndata: {\"type\":\"message_stop\"}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(tc.body)
			got, err := (&Runtime{}).readCandyCheckBody(r, http.Header{"Content-Type": {"text/event-stream"}})
			if err != nil || string(got) != tc.body {
				t.Fatalf("stopped early: %q %v", got, err)
			}
			answer, err := parseCandyCheckAnswer(got, http.Header{"Content-Type": {"text/event-stream"}})
			if err != nil || answer != "21" {
				t.Fatalf("answer=%q err=%v", answer, err)
			}
		})
	}
}
