package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func nativeVideoRequest(svc *Service, method, path, contentType string, body []byte, headers http.Header) *httptest.ResponseRecorder {
	router := gin.New()
	RegisterPublic(router, svc)
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer video-client")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	for key, values := range headers {
		req.Header[key] = values
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	return w
}

func TestNativeVideoCompletedWithoutURLAndBoundContent(t *testing.T) {
	var creates, downloads, other atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer original-upstream-key" || r.Header.Get("X-Video-Account") != "original" {
			t.Error("native request lost original account")
		}
		switch r.URL.Path {
		case "/v1/videos":
			creates.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = io.WriteString(w, `{"id":"native-task","status":"queued","object":"video"}`)
		case "/v1/videos/native-task":
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"native-task","status":"completed","progress":100}`)
		case "/v1/videos/native-task/content":
			downloads.Add(1)
			if r.Header.Get("Cookie") != "" || r.Header.Get("X-Client-Secret") != "" {
				t.Error("client credentials leaked to content API")
			}
			w.Header().Set("Content-Type", "video/mp4")
			w.Header().Set("ETag", `"video-v1"`)
			if r.Header.Get("Range") != "" {
				if r.Header.Get("Range") != "bytes=0-3" || r.Header.Get("If-Range") != `"video-v1"` {
					t.Error("range or validator lost")
				}
				w.Header().Set("Content-Range", "bytes 0-3/9")
				w.Header().Set("Content-Length", "4")
				w.WriteHeader(http.StatusPartialContent)
				if r.Method != http.MethodHead {
					_, _ = io.WriteString(w, "vide")
				}
			} else {
				w.Header().Set("Content-Length", "9")
				if r.Method != http.MethodHead {
					_, _ = io.WriteString(w, "videodata")
				}
			}
		default:
			t.Errorf("unexpected native path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}, func(w http.ResponseWriter, r *http.Request) {
		other.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/json", []byte(`{"model":"video","prompt":"hello","seconds":5}`), nil)
	if w.Code != http.StatusAccepted || !strings.Contains(w.Body.String(), `"id":"native-task"`) {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	// Change the live provider's URL, key, auth and extra headers, remove its
	// routes, then start a fresh runtime. Download must still use the binding.
	p := f.providers[0]
	p.BaseURL = f.providers[1].BaseURL
	p.APIKeyCipher, _ = f.svc.Cipher.Encrypt("rotated-key")
	p.AuthStyle, p.ExtraHeadersJSON = storage.GatewayProviderAuthXAPIKey, `{"X-Video-Account":"changed"}`
	if err := f.svc.Providers.Update(p); err != nil {
		t.Fatal(err)
	}
	if err := f.svc.Routes.SaveForGroup(f.group.ID, nil); err != nil {
		t.Fatal(err)
	}
	f.key.Quota, f.key.QuotaUsed = 1, 1
	if err := f.svc.Keys.Update(f.key); err != nil {
		t.Fatal(err)
	}
	restarted := NewService(f.svc.Groups, f.svc.Keys, f.svc.Routes, f.svc.Usage, storage.NewModelPriceOverrides(f.db), f.svc.Channels, nil, f.svc.Cipher, nil)
	restarted.SetProviders(storage.NewGatewayProviders(f.db))
	restarted.SetResponseRules(storage.NewGatewayResponseRules(f.db))
	w = nativeVideoRequest(restarted, http.MethodGet, "/v1/videos/native-task", "", nil, nil)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status":"completed"`) {
		t.Fatalf("completion without URL: %d %s", w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		method, prefix, want string
		partial              bool
	}{
		{http.MethodGet, "/v1", "videodata", false},
		{http.MethodGet, "", "vide", true},
		{http.MethodHead, "/v1", "", false},
		{http.MethodHead, "", "", true},
	} {
		headers := http.Header{"Cookie": {"secret=downstream"}, "X-Client-Secret": {"private"}}
		status, length := http.StatusOK, "9"
		if tc.partial {
			headers.Set("Range", "bytes=0-3")
			headers.Set("If-Range", `"video-v1"`)
			status, length = http.StatusPartialContent, "4"
		}
		w = nativeVideoRequest(restarted, tc.method, tc.prefix+"/videos/native-task/content", "", nil, headers)
		if w.Code != status || w.Body.String() != tc.want || w.Header().Get("Content-Length") != length || w.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatalf("download %s: %d %s %v", tc.method, w.Code, w.Body.String(), w.Header())
		}
	}
	foreign := &storage.GatewayKey{GroupID: f.group.ID, Name: "foreign", KeyHash: HashAPIKey("foreign-key"), KeyCipher: p.APIKeyCipher, Status: storage.GatewayKeyStatusActive}
	if err := restarted.Keys.Create(foreign); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ path, key string }{{"native-task", "foreign-key"}, {"unknown", "video-client"}} {
		w = nativeVideoRequest(restarted, http.MethodGet, "/v1/videos/"+tc.path+"/content", "", nil, http.Header{"Authorization": {"Bearer " + tc.key}})
		if w.Code != http.StatusNotFound {
			t.Fatalf("ownership: %d %s", w.Code, w.Body.String())
		}
	}
	var logs int64
	if err := f.db.Model(&storage.GatewayUsageLog{}).Count(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if creates.Load() != 1 || downloads.Load() != 4 || other.Load() != 0 || logs != 1 {
		t.Fatalf("unexpected routing/accounting: creates=%d downloads=%d other=%d logs=%d", creates.Load(), downloads.Load(), other.Load(), logs)
	}
	p.Enabled = false
	if err := restarted.Providers.Update(p); err != nil {
		t.Fatal(err)
	}
	w = nativeVideoRequest(restarted, http.MethodGet, "/v1/videos/native-task/content", "", nil, nil)
	if w.Code != http.StatusServiceUnavailable || downloads.Load() != 4 {
		t.Fatal("disabled provider still served content")
	}
}

func TestNativeVideoContentFailureDoesNotRecreate(t *testing.T) {
	for _, code := range []int{http.StatusOK, http.StatusUnauthorized, http.StatusNotFound, http.StatusConflict, http.StatusRequestedRangeNotSatisfiable} {
		t.Run(fmt.Sprint(code), func(t *testing.T) {
			var creates, downloads atomic.Int32
			f := newVideoTestFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method == http.MethodPost {
					creates.Add(1)
					_, _ = io.WriteString(w, `{"id":"task","status":"queued"}`)
					return
				}
				downloads.Add(1)
				if code == http.StatusRequestedRangeNotSatisfiable {
					w.Header().Set("Content-Range", "bytes */9")
				}
				w.WriteHeader(code)
				_, _ = io.WriteString(w, `{"error":"private-upstream-error"}`)
			})
			nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/json", []byte(`{"model":"video","prompt":"test"}`), nil)
			w := nativeVideoRequest(f.svc, http.MethodGet, "/v1/videos/task/content", "", nil, nil)
			want := code
			if code == http.StatusOK || code == http.StatusUnauthorized {
				want = http.StatusBadGateway
			}
			if w.Code != want || strings.Contains(w.Body.String(), "private-upstream-error") || creates.Load() != 1 || downloads.Load() != 1 {
				t.Fatalf("failed download leaked or retried: %d %s", w.Code, w.Body.String())
			}
			if code == http.StatusRequestedRangeNotSatisfiable && (w.Header().Get("Content-Range") != "bytes */9" || w.Body.Len() != 0) {
				t.Fatal("range failure lost its size or exposed upstream body")
			}
		})
	}
}

func TestNativeVideoContentRedirectDoesNotForwardCredentials(t *testing.T) {
	var contacted atomic.Int32
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { contacted.Add(1) }))
	defer sink.Close()
	f := newVideoTestFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"id":"task","status":"queued"}`)
			return
		}
		// A standard http.Client would follow this with both the bearer key
		// and custom account headers. Our relay must block private redirects.
		w.Header().Set("Location", sink.URL+"/secret")
		w.WriteHeader(http.StatusFound)
	})
	nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/json", []byte(`{"model":"video"}`), nil)
	w := nativeVideoRequest(f.svc, http.MethodGet, "/v1/videos/task/content", "", nil, nil)
	if w.Code != http.StatusBadGateway || contacted.Load() != 0 {
		t.Fatalf("unsafe redirect followed: %d %s calls=%d", w.Code, w.Body.String(), contacted.Load())
	}
}

func TestNativeVideoFormsPreserveFilesAndMapModels(t *testing.T) {
	file := []byte{0, 255, 1, 2, '\r', '\n', 128}
	for _, coordinated := range []bool{false, true} {
		for _, multipartBody := range []bool{false, true} {
			t.Run(fmt.Sprintf("coordinated=%v/multipart=%v", coordinated, multipartBody), func(t *testing.T) {
				f := newVideoTestFixture(t, coordinated, func(w http.ResponseWriter, r *http.Request) {
					if multipartBody {
						if err := r.ParseMultipartForm(1 << 20); err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						defer r.MultipartForm.RemoveAll()
						src, header, err := r.FormFile("video")
						if err != nil {
							t.Error(err)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						got, _ := io.ReadAll(src)
						_ = src.Close()
						if !bytes.Equal(got, file) || header.Filename != "reference.mp4" {
							t.Error("reference file corrupted")
						}
					} else if err := r.ParseForm(); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if r.FormValue("model") != "upstream-video" || r.FormValue("seconds") != "8" || r.FormValue("resolution") != "768p" || r.FormValue("prompt") != "你好 & hello" {
						t.Errorf("native form fields changed: %v", r.Form)
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"form-task","status":"queued"}`)
				})
				routes, _ := f.svc.Routes.ListByGroupID(f.group.ID)
				routes[0].ModelMappingJSON = `{"public-video":"upstream-video"}`
				if err := f.svc.Routes.SaveForGroup(f.group.ID, routes); err != nil {
					t.Fatal(err)
				}
				fields := url.Values{"model": {"public-video"}, "prompt": {"你好 & hello"}, "seconds": {"8"}, "resolution": {"768p"}}
				body, contentType := []byte(fields.Encode()), "application/x-www-form-urlencoded"
				if multipartBody {
					var buf bytes.Buffer
					writer := multipart.NewWriter(&buf)
					for name, values := range fields {
						_ = writer.WriteField(name, values[0])
					}
					part, _ := writer.CreateFormFile("video", "reference.mp4")
					_, _ = part.Write(file)
					_ = writer.Close()
					body, contentType = buf.Bytes(), writer.FormDataContentType()
				}
				w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", contentType, body, nil)
				if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "form-task") {
					t.Fatalf("form submission: %d %s", w.Code, w.Body.String())
				}
				var logs []storage.GatewayUsageLog
				if err := f.db.Find(&logs).Error; err != nil || len(logs) != 1 || logs[0].RequestedModel != "public-video" || logs[0].UpstreamModel != "upstream-video" {
					t.Fatalf("form usage lost model: %+v %v", logs, err)
				}
			})
		}
	}
}

func TestNativeVideoJSONReferenceFieldsPreserved(t *testing.T) {
	body := `{"model":"video","prompt":"reference","duration":8,"aspect_ratio":"16:9","resolution":"768p","images":[{"url":"https://media.test/i.png","duration_seconds":2}],"videos":["https://media.test/v.mp4"],"audios":["https://media.test/a.mp3"],"input_reference":"https://media.test/first.png","omni_reference_task_type":"reference"}`
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		got, _ := io.ReadAll(r.Body)
		var input, actual map[string]any
		_ = json.Unmarshal([]byte(body), &input)
		_ = json.Unmarshal(got, &actual)
		a, _ := json.Marshal(input)
		b, _ := json.Marshal(actual)
		if !bytes.Equal(a, b) {
			t.Errorf("native references changed: %s", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"task","status":"queued"}`)
	})
	w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/json", []byte(body), nil)
	if w.Code != http.StatusOK {
		t.Fatalf("native JSON failed: %d %s", w.Code, w.Body.String())
	}
}
