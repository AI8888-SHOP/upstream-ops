package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func videoMediaTestBody(t *testing.T, field, name string, data []byte) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	part, err := writer.CreateFormFile(field, name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes(), writer.FormDataContentType()
}

func TestVideoMediaUploadThenNativeGeneration(t *testing.T) {
	var uploads, creates atomic.Int32
	file := []byte{0, 0, 1, 255, 128, '\r', '\n'}
	const reference = "https://media.test/reference.mp4?signature=abc"
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer original-upstream-key" || r.Header.Get("Cookie") != "" {
			t.Error("wrong upload credentials")
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/v1/media" {
			uploads.Add(1)
			if r.URL.RawQuery != "" {
				t.Error("gateway route selector leaked upstream")
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			defer r.MultipartForm.RemoveAll()
			src, header, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			got, _ := io.ReadAll(src)
			_ = src.Close()
			if !bytes.Equal(got, file) || header.Filename != "reference.mp4" {
				t.Error("uploaded file was changed")
			}
			_, _ = io.WriteString(w, `{"url":"`+reference+`","expires_at":1900000000}`)
			return
		}
		if r.URL.Path != "/v1/videos" {
			t.Errorf("unexpected generation path %s", r.URL.Path)
		}
		creates.Add(1)
		var request struct {
			Videos []string `json:"videos"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Videos) != 1 || request.Videos[0] != reference {
			t.Errorf("uploaded reference was lost: %+v %v", request, err)
		}
		_, _ = io.WriteString(w, `{"id":"task","status":"queued"}`)
	})
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	t.Setenv("TMP", temp)
	t.Setenv("TEMP", temp)
	body, contentType := videoMediaTestBody(t, "file", "reference.mp4", file)
	for _, path := range []string{"/v1/media", "/media"} {
		w := nativeVideoRequest(f.svc, http.MethodPost, path, contentType, body, http.Header{"Cookie": {"downstream=secret"}})
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), reference) || !strings.Contains(w.Body.String(), "expires_at") {
			t.Fatalf("upload: %d %s", w.Code, w.Body.String())
		}
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files left behind: %v %v", entries, err)
	}
	var logs int64
	if err := f.db.Model(&storage.GatewayUsageLog{}).Count(&logs).Error; err != nil || logs != 0 {
		t.Fatalf("upload counted as generation: %d %v", logs, err)
	}
	w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/json", []byte(`{"model":"video","prompt":"continue","videos":["`+reference+`"]}`), nil)
	if w.Code != http.StatusOK || creates.Load() != 1 || uploads.Load() != 2 {
		t.Fatalf("upload to generation: %d %s creates=%d uploads=%d", w.Code, w.Body.String(), creates.Load(), uploads.Load())
	}
}

func TestVideoMediaRouteSelectionAndFailureNoRetry(t *testing.T) {
	var first, second atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		first.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"https://media.test/a.mp4","expires_at":1900000000}`)
	}, func(w http.ResponseWriter, r *http.Request) {
		second.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "private upstream failure")
	})
	body, contentType := videoMediaTestBody(t, "file", "reference.mp4", []byte("video"))
	for _, path := range []string{"/v1/media", "/v1/media?route_id=99999999", "/v1/media?route_id=bad"} {
		w := nativeVideoRequest(f.svc, http.MethodPost, path, contentType, body, nil)
		if w.Code != http.StatusBadRequest || first.Load() != 0 || second.Load() != 0 {
			t.Fatalf("ambiguous/unknown route uploaded: %s %d", path, w.Code)
		}
	}
	routes, err := f.svc.Routes.ListByGroupID(f.group.ID)
	if err != nil || len(routes) != 2 {
		t.Fatalf("routes: %v %v", routes, err)
	}
	w := nativeVideoRequest(f.svc, http.MethodPost, fmt.Sprintf("/v1/media?route_id=%d", routes[1].ID), contentType, body, nil)
	if w.Code != http.StatusBadGateway || first.Load() != 0 || second.Load() != 1 || strings.Contains(w.Body.String(), "private upstream") {
		t.Fatalf("upload failure retried or leaked: %d %s first=%d second=%d", w.Code, w.Body.String(), first.Load(), second.Load())
	}
	w = nativeVideoRequest(f.svc, http.MethodPost, fmt.Sprintf("/v1/media?route_id=%d", routes[0].ID), contentType, body, nil)
	if w.Code != http.StatusOK || first.Load() != 1 || second.Load() != 1 {
		t.Fatalf("explicit route failed: %d %s", w.Code, w.Body.String())
	}
	p := f.providers[0]
	p.Enabled = false
	if err := f.svc.Providers.Update(p); err != nil {
		t.Fatal(err)
	}
	w = nativeVideoRequest(f.svc, http.MethodPost, fmt.Sprintf("/v1/media?route_id=%d", routes[0].ID), contentType, body, nil)
	if w.Code != http.StatusBadRequest || first.Load() != 1 || second.Load() != 1 {
		t.Fatal("disabled route uploaded a file")
	}
}

func TestVideoMediaUploadValidationAndCleanup(t *testing.T) {
	var calls atomic.Int32
	f := newVideoTestFixture(t, false, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"error":{"message":"private upstream error"}}`)
	})
	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	t.Setenv("TMP", temp)
	t.Setenv("TEMP", temp)
	for _, tc := range []struct{ field, data string }{{"image", "data"}, {"file", ""}} {
		body, contentType := videoMediaTestBody(t, tc.field, "test.mp4", []byte(tc.data))
		w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/media", contentType, body, nil)
		if w.Code != http.StatusBadRequest || calls.Load() != 0 {
			t.Fatalf("invalid file reached upstream: %d %s", w.Code, w.Body.String())
		}
	}
	body, contentType := videoMediaTestBody(t, "file", "test.mp4", []byte("data"))
	w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/media", contentType, body, nil)
	if w.Code != http.StatusBadGateway || calls.Load() != 1 || strings.Contains(w.Body.String(), "private upstream") {
		t.Fatalf("HTTP 200 error accepted: %d %s", w.Code, w.Body.String())
	}
	entries, err := os.ReadDir(temp)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed upload leaked files: %v %v", entries, err)
	}
}

func TestNativeVideoFormInvalidStreamAndDuplicates(t *testing.T) {
	var calls atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	for _, body := range []string{"model=video&stream=true", "model=video&model=other", "model=video&stream=false&stream=true", "model=%FF%ZZ"} {
		w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", "application/x-www-form-urlencoded", []byte(body), nil)
		if w.Code != http.StatusBadRequest || calls.Load() != 0 {
			t.Fatalf("invalid video form reached upstream: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestNativeVideoFormHonorsModelAllowlist(t *testing.T) {
	var calls atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) { calls.Add(1) })
	p := f.providers[0]
	p.ModelPolicy, p.AllowedModelsJSON = storage.GatewayProviderModelPolicyAllowlist, `["allowed-video"]`
	if err := f.svc.Providers.Update(p); err != nil {
		t.Fatal(err)
	}
	for _, multipartBody := range []bool{false, true} {
		body, contentType := []byte(url.Values{"model": {"forbidden-video"}}.Encode()), "application/x-www-form-urlencoded"
		if multipartBody {
			var buf bytes.Buffer
			writer := multipart.NewWriter(&buf)
			_ = writer.WriteField("model", "forbidden-video")
			_ = writer.Close()
			body, contentType = buf.Bytes(), writer.FormDataContentType()
		}
		w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/videos", contentType, body, nil)
		if w.Code < 400 || calls.Load() != 0 {
			t.Fatalf("video form bypassed model allowlist: %d %s", w.Code, w.Body.String())
		}
	}
}

func TestVideoMediaUploadRespectsRawRejectionRule(t *testing.T) {
	var calls atomic.Int32
	f := newVideoTestFixture(t, true, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"url":"https://media.test/blocked-video-response.mp4","expires_at":1900000000}`)
	})
	body, contentType := videoMediaTestBody(t, "file", "test.mp4", []byte("video"))
	w := nativeVideoRequest(f.svc, http.MethodPost, "/v1/media", contentType, body, nil)
	if w.Code != http.StatusBadGateway || calls.Load() != 1 || strings.Contains(w.Body.String(), "https://media.test") {
		t.Fatalf("upload rejection rule ignored: %d %s", w.Code, w.Body.String())
	}
}
