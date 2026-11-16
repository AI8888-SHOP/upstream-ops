package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func comparisonNumber(v float64) *float64 { return &v }

func TestComparisonCostsAreComparableAndNotDoubleConverted(t *testing.T) {
	in := comparisonRequest{Currency: "CNY", Unit: "张"}
	candidates := []comparisonCandidate{
		{ID: 1, Description: "2K 最终实付 CNY 1/张", RawGroupRatio: 3, RechargeMultiplier: comparisonNumber(5)},
		{ID: 2, Description: "2K 扣费 CNY 4/张", RawGroupRatio: 9, RechargeMultiplier: comparisonNumber(8), RechargeMode: "divide"},
		{ID: 3, Description: "2K 基础 CNY 10/张", RawGroupRatio: 0.2, RechargeMultiplier: comparisonNumber(0.25), RechargeMode: "multiply"},
		{ID: 4, Description: "2K USD 0.01/张"},
		{ID: 5, Description: "2K CNY 0.01/秒"},
		{ID: 6, Description: "只有倍率"},
		{ID: 7, Description: "2K CNY 0.01/张，口径不明"},
		{ID: 8, Description: "1K 最终实付 CNY 0.01/张"},
		{ID: 9, Description: "2K 扣费 CNY 0.01/张"},
	}
	quotes := []comparisonQuote{
		{ID: 1, Price: comparisonNumber(1), Currency: "CNY", Unit: "张", Basis: "cash", MatchesSpecification: true, Evidence: candidates[0].Description},
		{ID: 2, Price: comparisonNumber(4), Currency: "CNY", Unit: "张", Basis: "balance", MatchesSpecification: true, Evidence: candidates[1].Description},
		{ID: 3, Price: comparisonNumber(10), Currency: "CNY", Unit: "张", Basis: "base_balance", MatchesSpecification: true, Evidence: candidates[2].Description},
		{ID: 4, Price: comparisonNumber(0.01), Currency: "USD", Unit: "张", Basis: "cash", MatchesSpecification: true, Evidence: candidates[3].Description},
		{ID: 5, Price: comparisonNumber(0.01), Currency: "CNY", Unit: "秒", Basis: "cash", MatchesSpecification: true, Evidence: candidates[4].Description},
		{ID: 6, Price: comparisonNumber(0.01), Currency: "CNY", Unit: "张", Basis: "cash", MatchesSpecification: true, Evidence: "伪造的价格原文"},
		{ID: 7, Price: comparisonNumber(0.01), Currency: "CNY", Unit: "张", Basis: "unknown", MatchesSpecification: true, Evidence: candidates[6].Description},
		{ID: 8, Price: comparisonNumber(0.01), Currency: "CNY", Unit: "张", Basis: "cash", MatchesSpecification: false, Evidence: candidates[7].Description},
		{ID: 9, Price: comparisonNumber(0.01), Currency: "CNY", Unit: "张", Basis: "balance", MatchesSpecification: true, Evidence: candidates[8].Description},
	}
	items, err := rankComparison(in, candidates, quotes)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []uint{2, 3, 1, 4, 5, 6, 7, 8, 9} {
		if items[i].Candidate.ID != want {
			t.Fatalf("order[%d]=%d want %d", i, items[i].Candidate.ID, want)
		}
	}
	if *items[0].Cost != .5 || *items[1].Cost != .5 || *items[2].Cost != 1 || items[0].Priority != items[1].Priority || items[2].Priority <= items[1].Priority {
		t.Fatalf("incorrect costs or tiers: %+v", items)
	}
	for _, item := range items[3:] {
		if item.Cost != nil || item.Warning == "" {
			t.Fatalf("unsafe comparison accepted: %+v", item)
		}
	}
	for _, malformed := range [][]comparisonQuote{{{ID: 99}}, {quotes[0], quotes[0]}} {
		if _, err := rankComparison(in, candidates, malformed); err == nil {
			t.Fatal("accepted invented/duplicate candidate")
		}
	}
	missing, err := rankComparison(in, candidates, nil)
	if err != nil || len(missing) != len(candidates) || missing[0].Cost != nil {
		t.Fatal("missing quotes must retain all candidates as unknown")
	}
}

func TestComparisonCandidateUndoesDisplayRatioAndOmitsSecrets(t *testing.T) {
	ch := storage.Channel{ID: 1, Name: "source", Username: "private-user", PasswordCipher: "private-password", LoginExtraParams: "private-session", GroupMultiplier: comparisonNumber(100), GroupMultiplierMode: "divide"}
	c := makeComparisonCandidate(ch, storage.RateSnapshot{ID: 1, ChannelID: 1, ModelName: "images", Ratio: .02})
	if c.RawGroupRatio != 2 {
		t.Fatalf("raw ratio = %v", c.RawGroupRatio)
	}
	encoded, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "private-") {
		t.Fatalf("credentials leaked to model: %s", encoded)
	}
	changed := c
	changed.Description = "new price"
	if comparisonFingerprint(c) == comparisonFingerprint(changed) {
		t.Fatal("price change did not invalidate fingerprint")
	}
}

func TestComparisonStrategySaveExpiryAndStaleSource(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	ch := storage.Channel{Name: "price-source", Type: storage.ChannelTypeNewAPI, SiteURL: "https://example.invalid", Username: "u", PasswordCipher: "x"}
	if err := db.Create(&ch).Error; err != nil {
		t.Fatal(err)
	}
	rate := storage.RateSnapshot{ChannelID: ch.ID, ModelName: "images", Description: "2K CNY 1/张", Ratio: 1}
	if err := db.Create(&rate).Error; err != nil {
		t.Fatal(err)
	}
	candidate := makeComparisonCandidate(ch, rate)
	items, _ := json.Marshal([]comparisonItem{{Candidate: candidate, Priority: 1, Fingerprint: comparisonFingerprint(candidate)}})
	row := storage.PriceComparisonStrategy{ID: "draft", ItemsJSON: string(items), ExpiresAt: time.Now().Add(time.Minute)}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerPriceComparison(router.Group("/api"), &Deps{DB: db})
	call := func(path string, want int) *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/price-comparison/strategies/draft/"+path, nil))
		if w.Code != want {
			t.Fatalf("%s: status %d body %s", path, w.Code, w.Body.String())
		}
		return w
	}
	call("apply", 410)
	call("save", 200)
	if err := db.First(&row, "id = ?", "draft").Error; err != nil {
		t.Fatal(err)
	}
	if row.SavedAt == nil || row.ExpiresAt.Sub(*row.SavedAt) != comparisonTTL {
		t.Fatalf("invalid save lifetime %+v", row)
	}
	expires := row.ExpiresAt
	call("save", 200)
	if err := db.First(&row, "id = ?", "draft").Error; err != nil {
		t.Fatal(err)
	}
	if !row.ExpiresAt.Equal(expires) {
		t.Fatal("repeat save extended TTL")
	}
	call("apply", 200)
	if err := db.Model(&rate).Update("description", "price changed").Error; err != nil {
		t.Fatal(err)
	}
	call("apply", 409)
	if err := db.Model(&row).Update("expires_at", time.Now().Add(-time.Second)).Error; err != nil {
		t.Fatal(err)
	}
	call("save", 410)
	call("apply", 410)
}

func TestComparisonModelTransportAndSecretConfig(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer model-secret" {
			t.Error("model key missing")
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"stream":false`) {
			t.Error("expected non-stream request")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"choices":[{"message":{"content":"{\"analysis\":\"ok\",\"quotes\":[{\"id\":1}]}"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	result, err := requestComparisonModel(context.Background(), storage.PriceComparisonConfig{Endpoint: server.URL, Model: "model"}, "model-secret", []byte(`{"candidates":[]}`))
	if err != nil || result.Analysis != "ok" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	db := openTestDB(t)
	cipher, err := crypto.NewCipher("comparison-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	registerPriceComparison(router.Group("/api"), &Deps{DB: db, Cipher: cipher})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/price-comparison/config", strings.NewReader(`{"endpoint":"https://example.invalid/v1/chat/completions","model":"model","api_key":"model-secret"}`)))
	if w.Code != 200 || strings.Contains(w.Body.String(), "model-secret") {
		t.Fatalf("config response: %d %s", w.Code, w.Body.String())
	}
	cfg, err := loadComparisonConfig(&Deps{DB: db})
	if err != nil || cfg.APIKeyCipher == "model-secret" || cfg.APIKeyCipher == "" {
		t.Fatal("key was not encrypted")
	}
	w = httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/api/price-comparison/config", strings.NewReader(`{"endpoint":"https://changed.invalid/v1/chat/completions","model":"model"}`)))
	if w.Code != 400 {
		t.Fatal("old key must not silently follow an endpoint change")
	}
}
