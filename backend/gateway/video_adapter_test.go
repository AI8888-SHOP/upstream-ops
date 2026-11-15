package gateway

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestVideosAdapterRequest(t *testing.T) {
	target := &upstreamTarget{Provider: &storage.GatewayProvider{VideoUpstreamMode: videoModeVideos}}
	for _, tc := range []struct{ name, body, field, want string }{
		{"single image", `{"model":"mapped-video","prompt":"hello","duration":8,"aspect_ratio":"16:9","image":{"url":"https://example.test/image.png"},"stream":false}`, "input_image", `"https://example.test/image.png"`},
		{"legacy image", `{"model":"mapped-video","image":{"image_url":"data:image/png;base64,AAAA"}}`, "input_image", `"data:image/png;base64,AAAA"`},
		{"images", `{"model":"mapped-video","images":[{"url":"https://example.test/1.png"},"https://example.test/2.png"]}`, "images", `["https://example.test/1.png","https://example.test/2.png"]`},
		{"native Veo references", `{"model":"mapped-video","reference_images":[{"url":"https://example.test/1.png","role":"first_frame"}],"duration":8,"resolution":"720p"}`, "reference_images", `[{"url":"https://example.test/1.png","role":"first_frame"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(tc.body)
			body, path, adapted, err := prepareVideoUpstreamRequest(target, "/v1/videos/generations", input, "/v1/videos/generations")
			if err != nil || !adapted || path != "/v1/videos" || string(input) != tc.body {
				t.Fatalf("prepare: path=%s adapted=%v err=%v input=%s", path, adapted, err, input)
			}
			var object map[string]json.RawMessage
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatal(err)
			}
			if videoJSONString(object["model"]) != "mapped-video" || string(object[tc.field]) != tc.want || object["image"] != nil || object["stream"] != nil {
				t.Fatalf("incorrect converted request: %s", body)
			}
		})
	}
	for _, body := range []string{`null`, `[]`, `not JSON`, `{"image":{}}`, `{"images":{}}`, `{"image":"a","input_image":"b"}`} {
		if _, _, _, err := prepareVideoUpstreamRequest(target, "/v1/videos/generations", []byte(body), ""); err == nil {
			t.Fatalf("invalid request accepted: %s", body)
		}
	}
	for _, path := range []string{"/v1/chat/completions", "/v1/images/generations", "/v1/videos", "/v1/videos/edits", "/v1/videos/extensions"} {
		body, gotPath, adapted, err := prepareVideoUpstreamRequest(target, path, []byte(`{"model":"keep"}`), path)
		if err != nil || adapted || gotPath != path || string(body) != `{"model":"keep"}` {
			t.Fatalf("unrelated endpoint %s was changed", path)
		}
	}
	for _, path := range []string{"/v1/videos", "/videos", "/v1/videos/generations"} {
		if hedgeEligible(hedgeRequest{Path: path, Model: "custom-name"}) {
			t.Fatalf("video endpoint may hedge: %s", path)
		}
	}
}

func TestVideosAdapterStatus(t *testing.T) {
	snapshot := videoTaskSnapshot{ResponseFormat: videoResponseGrok, RequestedModel: "client-model", DurationSeconds: 8}
	for _, tc := range []struct {
		name, body, status string
		duration           float64
	}{
		{"queued", `{"id":"task","status":"queued","progress":2}`, "pending", 0},
		{"processing no id", `{"status":"processing"}`, "pending", 0},
		{"in progress", `{"id":"task","status":"in_progress"}`, "pending", 0},
		{"completed root", `{"id":"task","status":"completed","url":"https://video.test/a.mp4","duration":12,"model":"upstream-alias"}`, "done", 12},
		{"completed output", `{"id":"task","status":"completed","output":[{"url":"https://video.test/a.mp4"}]}`, "done", 8},
		{"completed metadata", `{"id":"task","status":"completed","metadata":{"url":"https://video.test/a.mp4","duration":"9"}}`, "done", 9},
		{"nested", `{"data":{"id":"task","status":"completed","url":"https://video.test/a.mp4"}}`, "done", 8},
		{"grok shape", `{"status":"done","video":{"url":"https://video.test/a.mp4","duration":6,"respect_moderation":true}}`, "done", 6},
		{"failed", `{"status":"failed","error":"generation failed"}`, "failed", 0},
		{"cancelled", `{"status":"cancelled","error":{"message":"cancelled"},"url":"https://video.test/stale.mp4"}`, "failed", 0},
		{"expired", `{"status":"expired"}`, "expired", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, err := videoLookupClientBody([]byte(tc.body), "task", snapshot)
			if err != nil {
				t.Fatal(err)
			}
			var object struct {
				RequestID string `json:"request_id"`
				Model     string `json:"model"`
				Status    string `json:"status"`
				Video     *struct {
					URL      string  `json:"url"`
					Duration float64 `json:"duration"`
				} `json:"video"`
			}
			if err := json.Unmarshal(body, &object); err != nil {
				t.Fatal(err)
			}
			if object.RequestID != "task" || object.Status != tc.status || object.Model != "client-model" {
				t.Fatalf("wrong sub2api status: %s", body)
			}
			// This is the same success predicate used by sub2api's
			// IsGrokVideoStatusBillable: done AND non-empty video.url.
			if tc.status == "done" {
				if object.Video == nil || object.Video.URL != "https://video.test/a.mp4" || object.Video.Duration != tc.duration {
					t.Fatalf("missing completion/billing metadata: %s", body)
				}
			} else if object.Video != nil {
				t.Fatalf("non-completed task contains a billable video: %s", body)
			}
		})
	}
	for _, body := range []string{`null`, `{}`, `not JSON`, `{"status":"mystery"}`, `{"status":"completed"}`,
		`{"status":"completed","url":"file:///private"}`, `{"id":"other-task","status":"queued"}`, `{"id":123,"status":"queued"}`} {
		if _, err := videoLookupClientBody([]byte(body), "task", snapshot); err == nil {
			t.Fatalf("invalid status accepted: %s", body)
		}
	}
}

func videoAdapterRequest(svc *Service, method, path, body string) *httptest.ResponseRecorder {
	router := gin.New()
	RegisterPublic(router, svc)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer video-client")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "video-business-001")
	req.Header.Set("X-Xiaojuzi-Media-Contract", "1")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func setVideoAdapter(t *testing.T, f *videoTestFixture, index int) {
	t.Helper()
	mode := videoModeVideos
	if _, err := f.svc.UpdateProvider(f.providers[index].ID, UpdateProviderInput{VideoUpstreamMode: &mode}); err != nil {
		t.Fatal(err)
	}
}

func TestVideosAdapterForwardAndBoundPolling(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			var creates, polls, wrongSource atomic.Int32
			var pollBody atomic.Value
			pollBody.Store(`{"id":"task","status":"queued"}`)
			f := newVideoTestFixture(t, coordinated, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Header.Get("Authorization") != "Bearer original-upstream-key" {
					t.Error("original task credential was lost")
				}
				if r.Method == http.MethodPost {
					creates.Add(1)
					if r.URL.Path != "/v1/videos" || r.Header.Get("Idempotency-Key") != "video-business-001" || r.Header.Get("X-Xiaojuzi-Media-Contract") != "1" {
						t.Errorf("wrong video creation path or headers: %s", r.URL.Path)
					}
					var body map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil || videoJSONString(body["model"]) != "upstream-video" {
						t.Errorf("mapped video request was lost: body=%s err=%v", body, err)
					}
					w.Header().Set("Location", "https://private-upstream.test/v1/videos/task")
					w.Header().Set("ETag", "upstream-body-hash")
					_, _ = io.WriteString(w, `{"id":"task","status":"queued","model":"upstream-video"}`)
				} else {
					polls.Add(1)
					if r.URL.Path != "/v1/videos/task" {
						t.Errorf("wrong task path: %s", r.URL.Path)
					}
					_, _ = io.WriteString(w, pollBody.Load().(string))
				}
			}, func(w http.ResponseWriter, r *http.Request) {
				wrongSource.Add(1)
				w.WriteHeader(http.StatusNotFound)
			})
			setVideoAdapter(t, f, 0)
			routes, err := f.svc.Routes.ListByGroupID(f.group.ID)
			if err != nil {
				t.Fatal(err)
			}
			routes[0].ModelMappingJSON = `{"client-video":"upstream-video"}`
			// A different text protocol must not turn video task JSON into chat.
			routes[0].UpstreamProtocol = storage.GatewayUpstreamProtocolOpenAIResponses
			if err := f.svc.Routes.SaveForGroup(f.group.ID, routes); err != nil {
				t.Fatal(err)
			}
			w := videoAdapterRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"client-video","prompt":"hello","duration":8}`)
			if w.Code != http.StatusOK || w.Body.String() != `{"request_id":"task"}` || w.Header().Get("Location") != "/v1/videos/task" || w.Header().Get("ETag") != "" {
				t.Fatalf("create: status=%d body=%s headers=%v", w.Code, w.Body.String(), w.Header())
			}
			mode, moved := videoModePassthrough, f.providers[1].BaseURL
			if _, err := f.svc.UpdateProvider(f.providers[0].ID, UpdateProviderInput{VideoUpstreamMode: &mode, BaseURL: &moved}); err != nil {
				t.Fatal(err)
			}
			if err := f.svc.Routes.SaveForGroup(f.group.ID, nil); err != nil {
				t.Fatal(err)
			}
			restarted := NewService(storage.NewGatewayGroups(f.db), storage.NewGatewayKeys(f.db), storage.NewGatewayRoutes(f.db),
				storage.NewGatewayUsageLogs(f.db), storage.NewModelPriceOverrides(f.db), storage.NewChannels(f.db), nil, f.svc.Cipher, nil)
			restarted.SetProviders(storage.NewGatewayProviders(f.db))
			restarted.SetResponseRules(storage.NewGatewayResponseRules(f.db))
			for _, tc := range []struct{ body, want string }{
				{`{"id":"task","status":"processing"}`, `"status":"pending"`},
				{`{"id":"task","status":"completed","url":"https://video.test/result.mp4"}`, `"video":{"duration":8,"url":"/v1/videos/task/content"}`},
				{`{"id":"task","status":"failed","error":"render failed"}`, `"status":"failed"`},
			} {
				pollBody.Store(tc.body)
				w = videoAdapterRequest(restarted, http.MethodGet, "/v1/videos/task", "")
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), tc.want) || !strings.Contains(w.Body.String(), `"model":"client-video"`) {
					t.Fatalf("query: status=%d body=%s", w.Code, w.Body.String())
				}
			}
			pollBody.Store(`{"id":"task","status":"completed"}`)
			if w = videoAdapterRequest(restarted, http.MethodGet, "/v1/videos/task", ""); w.Code != http.StatusBadGateway {
				t.Fatalf("completion without video was accepted: %d %s", w.Code, w.Body.String())
			}
			var logs []storage.GatewayUsageLog
			if err := f.db.Find(&logs).Error; err != nil {
				t.Fatal(err)
			}
			if creates.Load() != 1 || polls.Load() != 4 || wrongSource.Load() != 0 || len(logs) != 1 ||
				logs[0].UpstreamEndpoint != "/v1/videos" || !logs[0].ProtocolConverted || !logs[0].Success {
				t.Fatalf("unexpected routing/accounting: creates=%d polls=%d other=%d logs=%+v", creates.Load(), polls.Load(), wrongSource.Load(), logs)
			}
		})
	}
}

func TestVideosAdapterDoesNotChangeNativeFallback(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			f := newVideoTestFixture(t, coordinated, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/videos" {
					t.Errorf("adapter path=%s", r.URL.Path)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			}, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if r.URL.Path != "/v1/videos/generations" || !strings.Contains(string(body), `"image":{"url"`) || strings.Contains(string(body), "input_image") {
					t.Errorf("native fallback inherited adapter: %s %s", r.URL.Path, body)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"request_id":"native-task"}`)
			})
			setVideoAdapter(t, f, 0)
			w := videoAdapterRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie","image":{"url":"https://image.test/1.png"}}`)
			if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "native-task") {
				t.Fatalf("fallback: status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestVideosAdapterAcceptedWithoutIDStopsSubmission(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			var calls atomic.Int32
			f := newVideoTestFixture(t, coordinated, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{}`)
			}, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.WriteString(w, `{"id":"second-paid-task"}`)
			})
			setVideoAdapter(t, f, 0)
			w := videoAdapterRequest(f.svc, http.MethodPost, "/v1/videos/generations", `{"model":"movie","prompt":"hello"}`)
			if calls.Load() != 1 || w.Code != http.StatusServiceUnavailable {
				t.Fatalf("accepted task was retried: calls=%d status=%d body=%s", calls.Load(), w.Code, w.Body.String())
			}
		})
	}
}

func TestNativeVideosEndpointAndProviderModeCRUD(t *testing.T) {
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			if r.URL.Path != "/v1/videos" {
				t.Errorf("native path=%s", r.URL.Path)
			}
			_, _ = io.WriteString(w, `{"id":"native-task","status":"queued"}`)
		} else {
			_, _ = io.WriteString(w, `{"id":"native-task","status":"completed","url":"https://video.test/a.mp4"}`)
		}
	})
	setVideoAdapter(t, f, 0)
	for _, path := range []string{"/v1/videos", "/videos"} {
		w := videoAdapterRequest(f.svc, http.MethodPost, path, `{"model":"movie","prompt":"hello"}`)
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"id":"native-task"`) || strings.Contains(w.Body.String(), "request_id") {
			t.Fatalf("native creation was converted: %d %s", w.Code, w.Body.String())
		}
	}
	w := videoAdapterRequest(f.svc, http.MethodGet, "/v1/videos/native-task", "")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"completed"`) {
		t.Fatalf("native poll was converted: %d %s", w.Code, w.Body.String())
	}
	created, err := f.svc.CreateProvider(CreateProviderInput{Name: "mode-default", BaseURL: "https://video.test", APIKey: "secret"})
	if err != nil || created.VideoUpstreamMode != videoModePassthrough {
		t.Fatalf("mode default: provider=%+v err=%v", created, err)
	}
	mode := " VIDEOS "
	if _, err := f.svc.UpdateProvider(created.ID, UpdateProviderInput{VideoUpstreamMode: &mode}); err != nil {
		t.Fatal(err)
	}
	var stored storage.GatewayProvider
	if err := f.db.First(&stored, created.ID).Error; err != nil || stored.VideoUpstreamMode != videoModeVideos {
		t.Fatalf("mode not persisted: %q err=%v", stored.VideoUpstreamMode, err)
	}
	invalid := "unknown"
	if _, err := f.svc.UpdateProvider(created.ID, UpdateProviderInput{VideoUpstreamMode: &invalid}); err == nil {
		t.Fatal("invalid mode accepted")
	}
	note := "unrelated change"
	updated, err := f.svc.UpdateProvider(created.ID, UpdateProviderInput{Notes: &note})
	if err != nil || updated.VideoUpstreamMode != videoModeVideos {
		t.Fatalf("unrelated edit reset video mode: %+v %v", updated, err)
	}
}
