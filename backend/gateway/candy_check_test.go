package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestCandyCheckWorkerScopeMappingAndRecovery(t *testing.T) {
	var calls atomic.Int32
	var correct atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/v1/chat/completions" || body["model"] != "upstream-model" || body["stream"] != false || r.Header.Get("Authorization") != "Bearer candy-test-key" {
			t.Errorf("bad probe: path=%s body=%v", r.URL.Path, body)
		}
		if !strings.Contains(fmt.Sprint(body["messages"]), "圆形 7 9 8") {
			t.Error("missing candy puzzle")
		}
		answer := "22"
		if correct.Load() {
			answer = "21"
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"choices":[{"message":{"role":"assistant","content":%q},"finish_reason":"stop"}]}`, answer)
	}))
	defer upstream.Close()
	db := openGatewayTestDB(t)
	groups, routes := storage.NewGatewayGroups(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("candy-test")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("candy-test-key")
	if err != nil {
		t.Fatal(err)
	}
	provider := &storage.GatewayProvider{Name: "candy", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, nil, routes, storage.NewGatewayUsageLogs(db), nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	group := &storage.GatewayGroup{Name: "candy-enabled", ModelMappingJSON: `{"alias":"upstream-model"}`, GatewayCandyCheckPolicy: storage.GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "alias", CandyCheckIntervalMinutes: 3, CandyCheckCooldownMinutes: 7}}
	other := &storage.GatewayGroup{Name: "candy-other"}
	for _, g := range []*storage.GatewayGroup{group, other} {
		if err := groups.Create(g); err != nil {
			t.Fatal(err)
		}
		if err := routes.SaveForGroup(g.ID, []storage.GatewayRoute{{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID}}); err != nil {
			t.Fatal(err)
		}
	}
	svc.RunCandyChecks(context.Background())
	list, err := routes.ListByGroupID(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	route := list[0]
	if calls.Load() != 1 || route.CandyCheck == nil || route.CandyCheck.Status != "incorrect" {
		t.Fatalf("calls=%d state=%+v", calls.Load(), route.CandyCheck)
	}
	for _, model := range []string{"upstream-model", "unrelated-model", ""} {
		if IsRouteSchedulableForModel(&route, model, time.Now()) {
			t.Fatalf("cooled route serves model %s", model)
		}
	}
	recovered := svc.runtime().recoverWhenAllRoutesRestricted(list, "alias", ParseModelMapping(group.ModelMappingJSON), time.Now())
	if len(SortRoutesForModel(recovered, nil, "asc", time.Now(), nil, "alias")) != 0 {
		t.Fatal("emergency recovery bypassed candy cooldown")
	}
	otherRoutes, _ := routes.ListByGroupID(other.ID)
	if !IsRouteSchedulableForModel(&otherRoutes[0], "upstream-model", time.Now()) {
		t.Fatal("cooldown leaked into another gateway group")
	}
	svc.RunCandyChecks(context.Background())
	if calls.Load() != 1 {
		t.Fatal("repeated probe during cooldown")
	}
	if err := routes.ClearCandyCheck(route.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	svc.RunCandyChecks(context.Background())
	if calls.Load() != 1 {
		t.Fatal("manual release immediately reprobed")
	}
	list, _ = routes.ListByGroupID(group.ID)
	if !IsRouteSchedulable(&list[0], time.Now()) {
		t.Fatal("manual release did not restore route")
	}
	correct.Store(true)
	if err := db.Model(&storage.GatewayRouteCandyCheck{}).Where("route_id = ?", route.ID).Update("next_check_at", time.Now().Add(-time.Minute)).Error; err != nil {
		t.Fatal(err)
	}
	svc.RunCandyChecks(context.Background())
	list, _ = routes.ListByGroupID(group.ID)
	if calls.Load() != 2 || list[0].CandyCheck.Status != "correct" || !IsRouteSchedulable(&list[0], time.Now()) {
		t.Fatalf("recovery: calls=%d state=%+v", calls.Load(), list[0].CandyCheck)
	}
	var usageCount int64
	if err := db.Model(&storage.GatewayUsageLog{}).Count(&usageCount).Error; err != nil || usageCount != 0 {
		t.Fatalf("probe billed to user: %d %v", usageCount, err)
	}
	// Editing the model allowlist skips the unsupported probe, not a request to
	// another route that could conceal this route's actual result.
	list[0].ModelPolicy, list[0].AllowedModelsJSON = "allowlist", `["other"]`
	if err := routes.SaveForGroup(group.ID, list); err != nil {
		t.Fatal(err)
	}
	svc.RunCandyChecks(context.Background())
	list, _ = routes.ListByGroupID(group.ID)
	if calls.Load() != 2 || list[0].CandyCheck.Status != "skipped" {
		t.Fatalf("allowlist ignored: calls=%d state=%+v", calls.Load(), list[0].CandyCheck)
	}
}

func TestCandyCheckProbeProtocolsAndTerminalWithoutEOF(t *testing.T) {
	for _, tc := range []struct {
		kind                    protocol.Kind
		setting, path, response string
	}{
		{protocol.KindOpenAIChat, "openai", "/v1/chat/completions", `{"choices":[{"message":{"content":"21"},"finish_reason":"stop"}]}`},
		{protocol.KindAnthropic, "anthropic", "/v1/messages", `{"content":[{"type":"text","text":"21"}],"stop_reason":"end_turn"}`},
		{protocol.KindOpenAIResponses, "responses", "/v1/responses", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output_text\":\"21\"}}\n\n"},
	} {
		t.Run(tc.setting, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != tc.path {
					t.Errorf("path=%s want=%s", r.URL.Path, tc.path)
				}
				if tc.kind == protocol.KindOpenAIResponses {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, tc.response)
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
						t.Error("waited for EOF after completed event")
					}
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, tc.response)
			}))
			defer upstream.Close()
			db := openGatewayTestDB(t)
			cipher, err := crypto.NewCipher("candy-protocol")
			if err != nil {
				t.Fatal(err)
			}
			secret, err := cipher.Encrypt("key")
			if err != nil {
				t.Fatal(err)
			}
			provider := &storage.GatewayProvider{Name: "candy", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true}
			providers := storage.NewGatewayProviders(db)
			if err := providers.Create(provider); err != nil {
				t.Fatal(err)
			}
			svc := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil)
			svc.SetProviders(providers)
			route := storage.GatewayRoute{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID, UpstreamProtocol: tc.setting}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			result := svc.probeCandyRoute(ctx, &storage.GatewayGroup{}, route, "test-model")
			if result.Status != "correct" || result.AnswerPreview != "21" {
				t.Fatalf("result=%+v", result)
			}
		})
	}
}
