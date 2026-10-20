package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayPriceCRUDSchedulesHistoryAndValidatesTiers(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	prices := storage.NewModelPriceOverrides(db)
	usage := storage.NewGatewayUsageLogs(db)
	service := gateway.NewService(nil, nil, nil, usage, prices, nil, nil, nil, nil)
	router := gin.New()
	registerGatewayAdmin(router.Group("/api"), &Deps{DB: db, ModelPrices: prices, Gateway: service})
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		r := httptest.NewRequest(method, path, bytes.NewBufferString(body))
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		return w
	}
	for _, body := range []string{`{"model_name":"x","input_price_per_token":-1}`, `{"model_name":"x","context_tiers_json":"[{\"above_input_tokens\":0}]"}`} {
		if w := request(http.MethodPut, "/api/gateway/prices", body); w.Code != http.StatusBadRequest {
			t.Fatalf("invalid status=%d body=%s", w.Code, w.Body.String())
		}
	}
	w := request(http.MethodPut, "/api/gateway/prices", `{"model_name":"test-price","input_price_per_token":0.000002,"cache_creation_1h_price_per_token":0,"enabled":false,"context_tiers_json":"[{\"above_input_tokens\":272000,\"input_price_per_token\":0.000004}]"}`)
	var price storage.ModelPriceOverride
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &price) != nil || price.ID == 0 || price.Enabled || price.CacheCreation1hPricePerToken == nil || *price.CacheCreation1hPricePerToken != 0 {
		t.Fatalf("price response=%d %s", w.Code, w.Body.String())
	}
	w = request(http.MethodGet, "/api/gateway/prices/repricing", "")
	var job storage.ModelPriceRepricingJob
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &job) != nil || job.Status != "pending" {
		t.Fatalf("job=%s", w.Body.String())
	}
	w = request(http.MethodDelete, "/api/gateway/prices/"+strconv.Itoa(int(price.ID)), "")
	if w.Code != 200 {
		t.Fatalf("delete=%s", w.Body.String())
	}
	if service.Pricing.Resolve("test-price").HasTokenPrice() {
		t.Fatal("deleted unknown price still active")
	}
	w = request(http.MethodPost, "/api/gateway/prices/repricing", "")
	if w.Code != http.StatusAccepted {
		t.Fatalf("restart=%s", w.Body.String())
	}
}
