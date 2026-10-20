package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayStreamPreconnectPolicyAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := gateway.NewService(groups, storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db), storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, nil, nil)
	router := gin.New()
	registerGatewayAdmin(router.Group("/api"), &Deps{DB: db, Gateway: svc})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)
		return w
	}
	decode := func(w *httptest.ResponseRecorder, status int) storage.GatewayGroup {
		t.Helper()
		if w.Code != status {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		var group storage.GatewayGroup
		if err := json.Unmarshal(w.Body.Bytes(), &group); err != nil {
			t.Fatal(err)
		}
		return group
	}
	defaults := decode(request(http.MethodPost, "/api/gateway/groups", `{"name":"preconnect-default"}`), http.StatusCreated)
	if defaults.GatewayStreamPreconnectPolicy != (storage.GatewayStreamPreconnectPolicy{}) {
		t.Fatalf("unsafe default: %+v", defaults.GatewayStreamPreconnectPolicy)
	}
	group := decode(request(http.MethodPost, "/api/gateway/groups", `{"name":"preconnect-enabled","stream_preconnect_enabled":true,"stream_preconnect_sub2api_enabled":true,"stream_preconnect_multihop_enabled":true,"stream_preconnect_min_delay_ms":25,"stream_preconnect_max_delay_ms":125}`), http.StatusCreated)
	want := storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true, StreamPreconnectMultihopEnabled: true, StreamPreconnectMinDelayMS: 25, StreamPreconnectMaxDelayMS: 125}
	if group.GatewayStreamPreconnectPolicy != want {
		t.Fatalf("create policy: %+v", group.GatewayStreamPreconnectPolicy)
	}
	path := fmt.Sprintf("/api/gateway/groups/%d", group.ID)
	updated := decode(request(http.MethodPut, path, `{"description":"older client update"}`), http.StatusOK)
	if updated.GatewayStreamPreconnectPolicy != want {
		t.Fatalf("omitted policy fields changed: %+v", updated.GatewayStreamPreconnectPolicy)
	}
	for _, body := range []string{
		`{"stream_preconnect_min_delay_ms":-1}`,
		`{"stream_preconnect_min_delay_ms":126}`,
		`{"stream_preconnect_max_delay_ms":24}`,
		`{"stream_preconnect_max_delay_ms":60001}`,
		`{"stream_preconnect_min_delay_ms":1.5}`,
		`{"stream_preconnect_max_delay_ms":"100"}`,
		`{"stream_preconnect_multihop_enabled":"true"}`,
		`{"stream_preconnect_multihop_enabled":1}`,
	} {
		if w := request(http.MethodPut, path, body); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid update %s accepted: %d %s", body, w.Code, w.Body.String())
		}
		loaded := decode(request(http.MethodGet, path, ""), http.StatusOK)
		if loaded.GatewayStreamPreconnectPolicy != want {
			t.Fatalf("rejected update persisted: %+v", loaded.GatewayStreamPreconnectPolicy)
		}
	}
	if w := request(http.MethodPost, "/api/gateway/groups", `{"name":"preconnect-invalid","stream_preconnect_min_delay_ms":1,"stream_preconnect_max_delay_ms":0}`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid create accepted: %d %s", w.Code, w.Body.String())
	}
	updated = decode(request(http.MethodPut, path, `{"stream_preconnect_sub2api_enabled":false}`), http.StatusOK)
	want.StreamPreconnectSub2apiEnabled = false
	if updated.GatewayStreamPreconnectPolicy != want {
		t.Fatalf("parent disable lost independent multihop preference: %+v", updated.GatewayStreamPreconnectPolicy)
	}
	updated = decode(request(http.MethodPut, path, `{"stream_preconnect_enabled":false,"stream_preconnect_multihop_enabled":false,"stream_preconnect_min_delay_ms":0,"stream_preconnect_max_delay_ms":0}`), http.StatusOK)
	if updated.GatewayStreamPreconnectPolicy != (storage.GatewayStreamPreconnectPolicy{}) {
		t.Fatalf("explicit disable/reset failed: %+v", updated.GatewayStreamPreconnectPolicy)
	}
}
