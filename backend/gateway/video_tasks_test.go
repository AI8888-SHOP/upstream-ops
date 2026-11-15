package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type videoTestFixture struct {
	svc       *Service
	db        *gorm.DB
	group     *storage.GatewayGroup
	key       *storage.GatewayKey
	providers []*storage.GatewayProvider
}

func newVideoTestFixture(t *testing.T, coordinated bool, handlers ...http.HandlerFunc) *videoTestFixture {
	t.Helper()
	db := openGatewayTestDB(t)
	cipher, err := crypto.NewCipher("video-task-test")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("original-upstream-key")
	if err != nil {
		t.Fatal(err)
	}
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{Name: "video", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
		RetryEnabled: true, RetryCount: 2, FailoverEnabled: true, FailoverMax: 3, RequestMaxAttempts: 5,
		ResponseValidationEnabled: true, ResponseValidationRetryCount: 2, ResponseValidationStreamMode: "prefix",
		HedgeEnabled: true, HedgeDelaySeconds: 0.001, HedgeMaxParallel: 2, HedgeMaxAttempts: 5}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "owner", KeyHash: HashAPIKey("video-client"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
	svc.SetProviders(storage.NewGatewayProviders(db))
	rules := storage.NewGatewayResponseRules(db)
	svc.SetResponseRules(rules)
	for _, target := range []string{"zero_usage", "response_model", "assistant_text"} {
		if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: target, Enabled: true, Target: target, Pattern: ".*"}); err != nil {
			t.Fatal(err)
		}
	}
	if coordinated {
		if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "raw-deny", Enabled: true, Target: "raw_body", Pattern: "blocked-video-response"}); err != nil {
			t.Fatal(err)
		}
	}
	f := &videoTestFixture{svc: svc, db: db, group: group, key: key}
	var inputs []storage.GatewayRoute
	for index, handler := range handlers {
		upstream := httptest.NewServer(handler)
		t.Cleanup(upstream.Close)
		p := &storage.GatewayProvider{Name: fmt.Sprintf("video-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret,
			Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll, AuthStyle: storage.GatewayProviderAuthBearer,
			ExtraHeadersJSON: `{"X-Video-Account":"original"}`}
		if err := svc.Providers.Create(p); err != nil {
			t.Fatal(err)
		}
		f.providers = append(f.providers, p)
		rate := float64(index + 1)
		inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider,
			GatewayProviderID: p.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIChat,
			RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
	}
	if err := routes.SaveForGroup(group.ID, inputs); err != nil {
		t.Fatal(err)
	}
	return f
}

func videoTestRequest(svc *Service, method, path, body, key string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(method, path, strings.NewReader(body))
	c.Request.Header.Set("Authorization", "Bearer "+key)
	c.Request.Header.Set("Content-Type", "application/json")
	svc.HandleForward(c, path, protocol.KindOpenAIChat)
	return w
}

func TestVideoTaskBindingSurvivesRestartAndSourceChanges(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			var creates, polls, otherCalls atomic.Int32
			f := newVideoTestFixture(t, coordinated, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer original-upstream-key" || r.Header.Get("X-Video-Account") != "original" {
					t.Error("task used different upstream credentials")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					creates.Add(1)
					w.WriteHeader(http.StatusAccepted)
					_, _ = io.WriteString(w, `{"request_id":"task-original","model":"upstream-video-alias","status":"pending"}`)
				} else {
					polls.Add(1)
					if r.URL.Path != "/v1/videos/task-original" {
						t.Errorf("wrong lookup path %q", r.URL.Path)
					}
					_, _ = io.WriteString(w, `{"status":"done","video":{"url":"https://video.example/result.mp4"}}`)
				}
			}, func(w http.ResponseWriter, r *http.Request) {
				otherCalls.Add(1)
				w.WriteHeader(http.StatusNotFound)
			})
			w := videoTestRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie","prompt":"hello"}`, "video-client")
			if w.Code != http.StatusAccepted || !strings.Contains(w.Body.String(), "task-original") {
				t.Fatalf("create: status=%d body=%s", w.Code, w.Body.String())
			}
			var binding storage.GatewayVideoTaskBinding
			if err := f.db.First(&binding).Error; err != nil || strings.Contains(binding.SnapshotCipher, "original-upstream-key") {
				t.Fatalf("missing/encrypted binding: err=%v", err)
			}
			p := f.providers[0]
			p.BaseURL = f.providers[1].BaseURL
			p.APIKeyCipher, _ = f.svc.Cipher.Encrypt("rotated-key")
			p.AuthStyle = storage.GatewayProviderAuthXAPIKey
			p.ExtraHeadersJSON = `{"X-Video-Account":"changed"}`
			if err := f.svc.Providers.Update(p); err != nil {
				t.Fatal(err)
			}
			// A new process has no route-affinity or validator cache. Even
			// removal of the original routing entry must not select a new site.
			if err := f.svc.Routes.SaveForGroup(f.group.ID, nil); err != nil {
				t.Fatal(err)
			}
			restarted := NewService(storage.NewGatewayGroups(f.db), storage.NewGatewayKeys(f.db), storage.NewGatewayRoutes(f.db),
				storage.NewGatewayUsageLogs(f.db), storage.NewModelPriceOverrides(f.db), storage.NewChannels(f.db), nil, f.svc.Cipher, nil)
			restarted.SetProviders(storage.NewGatewayProviders(f.db))
			restarted.SetResponseRules(storage.NewGatewayResponseRules(f.db))
			for i := 0; i < 2; i++ {
				w = videoTestRequest(restarted, http.MethodGet, "/v1/videos/task-original", "", "video-client")
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "result.mp4") {
					t.Fatalf("lookup: status=%d body=%s", w.Code, w.Body.String())
				}
			}
			var usageCount int64
			if err := f.db.Model(&storage.GatewayUsageLog{}).Count(&usageCount).Error; err != nil {
				t.Fatal(err)
			}
			if creates.Load() != 1 || polls.Load() != 2 || otherCalls.Load() != 0 || usageCount != 1 {
				t.Fatalf("creates=%d polls=%d other=%d usage=%d", creates.Load(), polls.Load(), otherCalls.Load(), usageCount)
			}
			// A different key in the same group cannot enumerate this task.
			foreign := &storage.GatewayKey{GroupID: f.group.ID, Name: "other", KeyHash: HashAPIKey("other-client"), KeyCipher: p.APIKeyCipher, Status: storage.GatewayKeyStatusActive}
			if err := restarted.Keys.Create(foreign); err != nil {
				t.Fatal(err)
			}
			for _, test := range []struct{ path, key string }{{"/v1/videos/task-original", "other-client"}, {"/v1/videos/unknown", "video-client"}} {
				w = videoTestRequest(restarted, http.MethodGet, test.path, "", test.key)
				if w.Code != http.StatusNotFound || polls.Load() != 2 || otherCalls.Load() != 0 {
					t.Fatalf("unknown/foreign task reached upstream: status=%d", w.Code)
				}
			}
			// Completion polling does not require a second generation credit.
			f.key.Quota, f.key.QuotaUsed = 1, 1
			if err := restarted.Keys.Update(f.key); err != nil {
				t.Fatal(err)
			}
			w = videoTestRequest(restarted, http.MethodGet, "/v1/videos/task-original", "", "video-client")
			if w.Code != http.StatusOK || polls.Load() != 3 {
				t.Fatalf("last-credit task cannot be read: %d %s", w.Code, w.Body.String())
			}
			w = videoTestRequest(restarted, http.MethodPost, "/v1/videos/generations", `{"model":"movie"}`, "video-client")
			if w.Code < 400 || creates.Load() != 1 {
				t.Fatal("video lookup quota exception allowed a new generation")
			}
			p.Enabled = false
			if err := restarted.Providers.Update(p); err != nil {
				t.Fatal(err)
			}
			w = videoTestRequest(restarted, http.MethodGet, "/v1/videos/task-original", "", "video-client")
			if w.Code != http.StatusServiceUnavailable || polls.Load() != 3 || otherCalls.Load() != 0 {
				t.Fatal("disabled provider was used or replaced")
			}
			f.key.Status = storage.GatewayKeyStatusDisabled
			if err := restarted.Keys.Update(f.key); err != nil {
				t.Fatal(err)
			}
			w = videoTestRequest(restarted, http.MethodGet, "/v1/videos/task-original", "", "video-client")
			if w.Code != http.StatusUnauthorized || polls.Load() != 3 {
				t.Fatal("quota exception bypassed disabled key authentication")
			}
		})
	}
}

func TestVideoBindingFailureDoesNotCreateAnotherTask(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			var calls atomic.Int32
			handler := func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.WriteString(w, `{"request_id":"accepted-but-not-saved"}`)
			}
			f := newVideoTestFixture(t, coordinated, handler, handler)
			if err := f.db.Migrator().DropTable(&storage.GatewayVideoTaskBinding{}); err != nil {
				t.Fatal(err)
			}
			w := videoTestRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie"}`, "video-client")
			if w.Code < 500 || calls.Load() != 1 || strings.Contains(w.Body.String(), "accepted-but-not-saved") {
				t.Fatalf("failed persistence retried/published task: status=%d calls=%d body=%s", w.Code, calls.Load(), w.Body.String())
			}
		})
	}
}

func TestVideoRawRuleStillRejectsAndFailsOver(t *testing.T) {
	var calls [2]atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		calls[0].Add(1)
		_, _ = io.WriteString(w, `{"error":{"message":"blocked-video-response"}}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		calls[1].Add(1)
		_, _ = io.WriteString(w, `{"id":"accepted-video"}`)
	})
	w := videoTestRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie"}`, "video-client")
	if w.Code != http.StatusOK || calls[0].Load() != 1 || calls[1].Load() != 1 || !strings.Contains(w.Body.String(), "accepted-video") {
		t.Fatalf("raw validation/failover: status=%d calls=%d/%d body=%s", w.Code, calls[0].Load(), calls[1].Load(), w.Body.String())
	}
	var rejected storage.GatewayUsageLog
	if err := f.db.Where("attempt = ?", 1).First(&rejected).Error; err != nil || rejected.ValidationRuleName != "raw-deny" {
		t.Fatalf("raw rule was bypassed: err=%v rule=%q", err, rejected.ValidationRuleName)
	}
}

func TestVideoLookupErrorsStayOnOriginalUpstream(t *testing.T) {
	var lookups, otherCalls atomic.Int32
	var status atomic.Int32
	var pollBody atomic.Value
	pollBody.Store(`{"status":"failed","error":{"message":"generation failed"}}`)
	status.Store(http.StatusNotFound)
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			_, _ = io.WriteString(w, `{"request_id":"task"}`)
			return
		}
		lookups.Add(1)
		w.WriteHeader(int(status.Load()))
		if status.Load() == http.StatusOK {
			_, _ = io.WriteString(w, pollBody.Load().(string))
		} else {
			_, _ = io.WriteString(w, `{"error":{"message":"private upstream error"}}`)
		}
	}, func(w http.ResponseWriter, r *http.Request) { otherCalls.Add(1) })
	w := videoTestRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie"}`, "video-client")
	if w.Code != http.StatusOK {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	for _, upstreamStatus := range []int{http.StatusNotFound, http.StatusUnauthorized, http.StatusServiceUnavailable} {
		status.Store(int32(upstreamStatus))
		w = videoTestRequest(f.svc, http.MethodGet, "/v1/videos/task", "", "video-client")
		if w.Code < 400 || strings.Contains(w.Body.String(), "private upstream error") || otherCalls.Load() != 0 {
			t.Fatalf("poll error leaked or rerouted: status=%d body=%s other=%d", w.Code, w.Body.String(), otherCalls.Load())
		}
	}
	status.Store(http.StatusOK)
	w = videoTestRequest(f.svc, http.MethodGet, "/v1/videos/task", "", "video-client")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"failed"`) || lookups.Load() != 4 || otherCalls.Load() != 0 {
		t.Fatalf("failed task state replaced/recreated: status=%d body=%s", w.Code, w.Body.String())
	}
	for _, body := range []string{`{"status":"pending","note":"blocked-video-response"}`, `{"error":{"message":"private upstream error"}}`} {
		pollBody.Store(body)
		w = videoTestRequest(f.svc, http.MethodGet, "/v1/videos/task", "", "video-client")
		if w.Code != http.StatusBadGateway || strings.Contains(w.Body.String(), "private upstream error") || otherCalls.Load() != 0 {
			t.Fatalf("poll response rejection failed: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestVideoStreamSubmissionRejectedBeforeUpstream(t *testing.T) {
	var calls atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	w := videoTestRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie","stream":true}`, "video-client")
	if w.Code != http.StatusBadRequest || calls.Load() != 0 {
		t.Fatalf("stream task reached upstream: status=%d calls=%d", w.Code, calls.Load())
	}
}

func TestVideoRuleIsolationDoesNotMutateTextValidator(t *testing.T) {
	v, err := newResponseValidator(responseValidationConfig{Enabled: true}, []responseRuleSpec{
		{Enabled: true, Target: "zero_usage", Pattern: ".*"},
		{Enabled: true, Target: "response_model", Pattern: "mini"},
		{Enabled: true, Target: "assistant_text", Pattern: "^$"},
		{Enabled: true, Target: "raw_body", Pattern: "deny-raw"},
		{Enabled: true, Target: "error_message", Pattern: "deny-error"},
	})
	if err != nil {
		t.Fatal(err)
	}
	video := v.forEndpoint("/v1/videos/generations")
	if video.ValidateBodyUsage([]byte(`{"request_id":"task"}`), "openai_chat", "movie").IsRejected() ||
		video.ValidateResponseModel("openai_chat", "movie", "mini").IsRejected() ||
		video.Validate([]byte(`{"request_id":"task"}`), nil, "openai_chat", "movie").IsRejected() {
		t.Fatal("text-only validation rejected video task")
	}
	for _, body := range []string{`{"note":"deny-raw"}`, `{"error":{"message":"deny-error"}}`} {
		if !video.Validate([]byte(body), nil, "openai_chat", "movie").IsRejected() {
			t.Fatalf("video error rule not retained: %s", body)
		}
	}
	if v.forEndpoint("/v1/chat/completions") != v || !v.ValidateBodyUsage([]byte(`{}`), "openai_chat", "video-model-name").IsRejected() ||
		!v.ValidateResponseModel("openai_chat", "movie", "mini").IsRejected() || len(v.rules) != 5 {
		t.Fatal("video exception changed shared text validation")
	}
}

func TestVideoTaskIDAndSnapshotBinding(t *testing.T) {
	for _, body := range []string{`{"request_id":"task-1"}`, `{"id":"task-1"}`, `{"task_id":"task-1"}`, `{"data":{"id":"task-1"}}`} {
		if id, err := videoTaskIDFromResponse([]byte(body)); err != nil || id != "task-1" {
			t.Fatalf("task extraction: id=%q err=%v", id, err)
		}
	}
	for _, body := range []string{`{"id":"../other"}`, `{"id":"x?api_key=secret"}`, `{"id":1}`, `null`, `not json`, `{"status":"pending"}`} {
		if _, err := videoTaskIDFromResponse([]byte(body)); err == nil {
			t.Fatalf("unsafe response accepted: %s", body)
		}
	}
	if id, err := videoTaskIDFromResponse([]byte(`{"data":[{"url":"https://video.example/result"}]}`)); id != "" || err != nil {
		t.Fatalf("synchronous result rejected: id=%q err=%v", id, err)
	}

	// Monitor routes must preserve their key too, independently of later
	// ensure-keys operations that change SourceAPIKeyCipher on the route.
	f := newVideoTestFixture(t, false)
	channel := &storage.Channel{Name: "monitor-video", SiteURL: "https://video.example"}
	if err := f.svc.Channels.Create(channel); err != nil {
		t.Fatal(err)
	}
	route := &storage.GatewayRoute{ID: 17, SourceChannelID: channel.ID, GatewayGroupID: f.group.ID}
	target := &upstreamTarget{BaseURL: channel.SiteURL, APIKey: "original-monitor-key", Channel: channel}
	if err := f.svc.runtime().bindVideoTask(context.Background(), "/v1/videos/edits", 200, []byte(`{"id":"monitor-task"}`),
		f.key, f.group, route, target, protocol.KindOpenAIChat, "movie", nil); err != nil {
		t.Fatal(err)
	}
	binding, err := f.svc.Routes.FindVideoTaskBinding(context.Background(), f.group.ID, f.key.ID, "monitor-task", time.Now())
	if err != nil || binding.SnapshotCipher == "" {
		t.Fatalf("monitor binding: %v", err)
	}
	channel.SiteURL = "https://different.example"
	if err := f.svc.Channels.Update(channel); err != nil {
		t.Fatal(err)
	}
	plain, err := f.svc.Cipher.Decrypt(binding.SnapshotCipher)
	var snapshot videoTaskSnapshot
	if err != nil || json.Unmarshal([]byte(plain), &snapshot) != nil {
		t.Fatal("monitor task snapshot cannot be decoded")
	}
	bound, err := f.svc.runtime().boundVideoTarget(snapshot)
	if err != nil || bound.APIKey != "original-monitor-key" || bound.BaseURL != "https://video.example" || bound.Channel.ID != channel.ID {
		t.Fatalf("monitor task switched source: %v", err)
	}
}
