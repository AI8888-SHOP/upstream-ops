package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// 一键优化接口：只读分析、确认后应用、解释文案兜底。
func TestGatewayOptimizerAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	svc := gateway.NewService(
		storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db),
		nil, nil, nil,
	)
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

	// 空库：应返回 200 与空方案，而不是报错。
	w := request(http.MethodGet, "/api/gateway/optimizer/plan?window_hours=24", "")
	if w.Code != http.StatusOK {
		t.Fatalf("plan status=%d body=%s", w.Code, w.Body.String())
	}
	var plan gateway.OptimizerPlan
	if err := json.Unmarshal(w.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Changes) != 0 {
		t.Fatalf("empty db must yield no changes: %+v", plan.Changes)
	}

	// 未选择任何项时必须拒绝，避免误触发全量应用。
	w = request(http.MethodPost, "/api/gateway/optimizer/apply", `{"change_ids":[]}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("apply empty status=%d body=%s", w.Code, w.Body.String())
	}

	// 解释接口即使没有数据也要给出可读文案。
	w = request(http.MethodPost, "/api/gateway/optimizer/explain", `{"window_hours":24}`)
	if w.Code != http.StatusOK {
		t.Fatalf("explain status=%d body=%s", w.Code, w.Body.String())
	}
	var explained struct {
		Explanation string `json:"explanation"`
		Source      string `json:"source"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &explained); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(explained.Explanation) == "" {
		t.Fatal("empty explanation")
	}
	if explained.Source != "rule" {
		t.Fatalf("source=%q want rule", explained.Source)
	}
}
