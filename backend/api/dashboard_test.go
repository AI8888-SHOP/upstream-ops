package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/channel"
	_ "github.com/bejix/upstream-ops/backend/connector/sub2api"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/monitor"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/progress"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "api-test.db"),
	})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestDashboardSummaryIncludesCosts(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)
	announcements := storage.NewUpstreamAnnouncements(db)

	balance1 := 20.0
	today1 := 1.25
	total1 := 10.5
	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
		LastBalance:    &balance1,
		TodayCost:      &today1,
		TotalCost:      &total1,
	}); err != nil {
		t.Fatalf("create channel1: %v", err)
	}

	balance2 := 15.0
	today2 := 2.75
	total2 := 30.0
	if err := channels.Create(&storage.Channel{
		Name:           "b",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://b.example.com",
		Username:       "u2",
		PasswordCipher: "y",
		MonitorEnabled: true,
		LastBalance:    &balance2,
		TodayCost:      &today2,
		TotalCost:      &total2,
	}); err != nil {
		t.Fatalf("create channel2: %v", err)
	}
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "ann-1",
		Title:       "公告",
		Content:     "维护通知",
		FirstSeenAt: time.Now(),
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerDashboard(api, &Deps{
		Channels:      channels,
		Notifies:      notifies,
		Announcements: announcements,
		Rates:         rates,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/summary", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			TodayTotalCost float64 `json:"today_total_cost"`
			TotalCost      float64 `json:"total_cost"`
			Channels       []struct {
				TodayCost float64 `json:"today_cost"`
				TotalCost float64 `json:"total_cost"`
			} `json:"channels"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.TodayTotalCost != 4.0 {
		t.Fatalf("today total cost = %v, want 4", resp.Data.TodayTotalCost)
	}
	if resp.Data.TotalCost != 40.5 {
		t.Fatalf("total cost = %v, want 40.5", resp.Data.TotalCost)
	}
	if len(resp.Data.Channels) != 2 {
		t.Fatalf("channels len = %d, want 2", len(resp.Data.Channels))
	}
}

func TestNotificationsLogsPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	notifies := storage.NewNotifications(db)
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            1,
		Name:          "alpha",
		Type:          storage.NotifyTelegram,
		ConfigCipher:  "x",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel1: %v", err)
	}
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            2,
		Name:          "beta",
		Type:          storage.NotifyWebhook,
		ConfigCipher:  "y",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel2: %v", err)
	}
	if err := notifies.CreateChannel(&storage.NotificationChannel{
		ID:            3,
		Name:          "gamma",
		Type:          storage.NotifyEmail,
		ConfigCipher:  "z",
		Subscriptions: "[]",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("create notify channel3: %v", err)
	}
	now := time.Now()
	for i := 0; i < 3; i++ {
		if err := notifies.AppendLog(&storage.NotificationLog{
			ChannelID: uint(i + 1),
			Event:     storage.EventAnnouncement,
			Subject:   "test",
			Success:   i%2 == 0,
			SentAt:    now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append log: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerNotifications(api, &Deps{Notifies: notifies})

	req := httptest.NewRequest(http.MethodGet, "/api/notifications/logs?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []struct {
				ChannelID   uint   `json:"channel_id"`
				ChannelName string `json:"channel_name"`
				ChannelType string `json:"channel_type"`
			} `json:"items"`
			Total int64 `json:"total"`
			Page  int   `json:"page"`
			Pages int   `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].ChannelID != 3 || resp.Data.Items[0].ChannelName != "gamma" || resp.Data.Items[0].ChannelType != "email" {
		t.Fatalf("unexpected first item: %#v", resp.Data.Items[0])
	}
}

func TestChannelsPageAll(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	for i := 0; i < 3; i++ {
		if err := channels.Create(&storage.Channel{
			Name:           "channel-" + strconv.Itoa(i),
			Type:           storage.ChannelTypeNewAPI,
			SiteURL:        "https://example.com",
			Username:       "u",
			PasswordCipher: "x",
			MonitorEnabled: true,
		}); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{Channels: channels})

	req := httptest.NewRequest(http.MethodGet, "/api/channels?page=1&page_size=-1", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items    []storage.Channel `json:"items"`
			Total    int64             `json:"total"`
			PageSize int               `json:"page_size"`
			Pages    int               `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 3 || resp.Data.Total != 3 || resp.Data.PageSize != -1 || resp.Data.Pages != 1 {
		t.Fatalf("unexpected all page response: %#v", resp.Data)
	}
}

func TestChannelsListSortOrder(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	items := []storage.Channel{
		{Name: "low", Type: storage.ChannelTypeNewAPI, SiteURL: "https://a.example.com", Username: "u", PasswordCipher: "x", SortOrder: 1, MonitorEnabled: true},
		{Name: "high", Type: storage.ChannelTypeNewAPI, SiteURL: "https://b.example.com", Username: "u", PasswordCipher: "x", SortOrder: 9, MonitorEnabled: true},
	}
	for i := range items {
		if err := channels.Create(&items[i]); err != nil {
			t.Fatalf("create channel: %v", err)
		}
	}

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{Channels: channels})

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Data []storage.Channel `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 2 || resp.Data[0].Name != "high" || resp.Data[1].Name != "low" {
		t.Fatalf("unexpected order: %#v", resp.Data)
	}
}

func TestChannelsListIncludesNewAPIUserID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	enc, err := cipher.Encrypt(`{"cookie":"session=secret","user_id":"123"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if err := channels.Create(&storage.Channel{
		Name:           "newapi-token",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{Channels: channels, Cipher: cipher})

	req := httptest.NewRequest(http.MethodGet, "/api/channels", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "session=secret") {
		t.Fatalf("response leaked cookie: %s", rec.Body.String())
	}
	var resp struct {
		Data []struct {
			UserID string `json:"user_id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 1 || resp.Data[0].UserID != "123" {
		t.Fatalf("unexpected response: %#v", resp.Data)
	}
}

func TestAnnouncementsPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	now := time.Now()
	publishedOld := now.Add(-3 * time.Hour)
	publishedMid := now.Add(-2 * time.Hour)
	publishedNew := now.Add(-1 * time.Hour)
	items := []storage.UpstreamAnnouncement{
		{ChannelID: 1, SourceKey: "ann-0", Title: "公告", Content: "维护通知", PublishedAt: &publishedMid, FirstSeenAt: now.Add(3 * time.Minute)},
		{ChannelID: 1, SourceKey: "ann-1", Title: "公告", Content: "维护通知", PublishedAt: &publishedNew, FirstSeenAt: now.Add(1 * time.Minute)},
		{ChannelID: 1, SourceKey: "ann-2", Title: "公告", Content: "维护通知", PublishedAt: &publishedOld, FirstSeenAt: now.Add(2 * time.Minute)},
	}
	for _, item := range items {
		item := item
		if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{item}); err != nil {
			t.Fatalf("sync announcements: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerAnnouncements(api, &Deps{Announcements: announcements})

	req := httptest.NewRequest(http.MethodGet, "/api/announcements?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.UpstreamAnnouncement `json:"items"`
			Total int64                          `json:"total"`
			Page  int                            `json:"page"`
			Pages int                            `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].SourceKey != "ann-1" || resp.Data.Items[1].SourceKey != "ann-0" {
		t.Fatalf("unexpected announcement order: %#v", resp.Data.Items)
	}
}

func TestRateChangesPage(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	rates := storage.NewRates(db)
	now := time.Now()
	for i := 0; i < 3; i++ {
		if err := rates.AppendChange(&storage.RateChangeLog{
			ChannelID: uint((i % 2) + 1),
			ModelName: "gpt-" + strconv.Itoa(i),
			NewRatio:  float64(i + 1),
			ChangedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerRates(api, &Deps{Rates: rates})

	req := httptest.NewRequest(http.MethodGet, "/api/rate-changes?page=1&page_size=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.RateChangeLog `json:"items"`
			Total int64                   `json:"total"`
			Page  int                     `json:"page"`
			Pages int                     `json:"pages"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data.Items) != 2 || resp.Data.Total != 3 || resp.Data.Page != 1 || resp.Data.Pages != 2 {
		t.Fatalf("unexpected page response: %#v", resp.Data)
	}
	if resp.Data.Items[0].ModelName != "gpt-2" || resp.Data.Items[1].ModelName != "gpt-1" {
		t.Fatalf("unexpected order: %#v", resp.Data.Items)
	}
}

func TestRateChangesPageFiltersChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	rates := storage.NewRates(db)
	now := time.Now()
	for i := 0; i < 4; i++ {
		channelID := uint(1)
		if i >= 2 {
			channelID = 2
		}
		if err := rates.AppendChange(&storage.RateChangeLog{
			ChannelID: channelID,
			ModelName: "model-" + strconv.Itoa(i),
			NewRatio:  float64(i + 1),
			ChangedAt: now.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("append rate change: %v", err)
		}
	}

	r := gin.New()
	api := r.Group("/api")
	registerRates(api, &Deps{Rates: rates})

	req := httptest.NewRequest(http.MethodGet, "/api/rate-changes?page=1&page_size=10&channel_id=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data struct {
			Items []storage.RateChangeLog `json:"items"`
			Total int64                   `json:"total"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Data.Total != 2 || len(resp.Data.Items) != 2 {
		t.Fatalf("unexpected filtered response: %#v", resp.Data)
	}
	for _, item := range resp.Data.Items {
		if item.ChannelID != 2 {
			t.Fatalf("unexpected channel id: %#v", resp.Data.Items)
		}
	}
}

func TestDashboardCostTrend(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)

	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel1: %v", err)
	}
	if err := channels.Create(&storage.Channel{
		Name:           "b",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://b.example.com",
		Username:       "u2",
		PasswordCipher: "y",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel2: %v", err)
	}

	trendLoc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		trendLoc = time.FixedZone("Asia/Shanghai", 8*60*60)
	}
	now := time.Now().In(trendLoc)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLoc)
	day1 := day0.AddDate(0, 0, -1)
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 1, TodayCost: 1.2, SampledAt: day1.Add(10 * time.Hour)}); err != nil {
		t.Fatalf("append cost1: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 1, TodayCost: 2.2, SampledAt: day1.Add(22 * time.Hour)}); err != nil {
		t.Fatalf("append cost2: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 2, TodayCost: 0.8, SampledAt: day1.Add(20 * time.Hour)}); err != nil {
		t.Fatalf("append cost3: %v", err)
	}
	if err := rates.AppendCost(&storage.CostSnapshot{ChannelID: 2, TodayCost: 3.5, SampledAt: day0.Add(9 * time.Hour)}); err != nil {
		t.Fatalf("append cost4: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerDashboard(api, &Deps{
		Channels: channels,
		Notifies: notifies,
		Rates:    rates,
	})

	req := httptest.NewRequest(http.MethodGet, "/api/dashboard/cost-trend?days=2", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Data []struct {
			Day  string  `json:"day"`
			Cost float64 `json:"cost"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Data) != 2 {
		t.Fatalf("trend len = %d, want 2", len(resp.Data))
	}
	if resp.Data[0].Cost != 3.0 {
		t.Fatalf("day1 cost = %v, want 3.0", resp.Data[0].Cost)
	}
	if resp.Data[1].Cost != 3.5 {
		t.Fatalf("day0 cost = %v, want 3.5", resp.Data[1].Cost)
	}
}

func TestSyncAllChannelsEmitsChannelMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)

	if err := channels.Create(&storage.Channel{
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u1",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{
		Channels: channels,
		Rates:    rates,
		Notifies: notifies,
		Monitor:  &stubMonitor{},
	})

	req := httptest.NewRequest(http.MethodPost, "/api/channels/sync-all", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content-type = %q", ct)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected sse body")
	}
	first := findFirstJSONBody(rec.Body.String())
	if !json.Valid([]byte(first)) {
		t.Fatal("invalid sse payload")
	}
	var ev struct {
		Stage     string `json:"stage"`
		ChannelID uint   `json:"channel_id"`
		Index     int    `json:"index"`
		Total     int    `json:"total"`
	}
	if err := json.Unmarshal([]byte(first), &ev); err != nil {
		t.Fatalf("decode sse payload: %v", err)
	}
	if ev.ChannelID != 1 || ev.Index != 1 || ev.Total != 1 {
		t.Fatalf("metadata mismatch: %#v", ev)
	}
}

func TestSyncChannelChecksSubscriptionAlerts(t *testing.T) {
	syncSubscriptionAlertTest(t, "/api/channels/1/sync")
}

func TestSyncAllChannelsChecksSubscriptionAlerts(t *testing.T) {
	syncSubscriptionAlertTest(t, "/api/channels/sync-all")
}

func syncSubscriptionAlertTest(t *testing.T, path string) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	rates := storage.NewRates(db)
	monitorLogs := storage.NewMonitorLogs(db)
	notifies := storage.NewNotifications(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}

	webhookSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer webhookSrv.Close()

	if err := notifies.CreateChannel(&storage.NotificationChannel{
		Name:         "webhook",
		Type:         storage.NotifyWebhook,
		ConfigCipher: mustEncryptAPI(t, cipher, `{"url":"`+webhookSrv.URL+`"}`),
		Enabled:      true,
	}); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}

	apiSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/auth/login":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","expires_in":3600}}`))
		case "/api/v1/auth/me":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"balance":10}}`))
		case "/api/v1/usage/dashboard/stats":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"today_actual_cost":1,"total_actual_cost":2}}`))
		case "/api/v1/subscriptions/progress":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","expires_at":"2026-06-22T00:00:00Z","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","daily":{"limit_usd":1,"used_usd":0.5,"remaining_usd":0.5,"percentage":50}}}]}`))
		case "/api/v1/groups/available":
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"name":"pro","rate_multiplier":1}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiSrv.Close()

	if err := channels.Create(&storage.Channel{
		Name:                "sub",
		Type:                storage.ChannelTypeSub2API,
		SiteURL:             apiSrv.URL,
		Username:            "u",
		PasswordCipher:      mustEncryptAPI(t, cipher, "p"),
		CredentialMode:      storage.CredentialModePassword,
		SubscriptionEnabled: true,
		MonitorEnabled:      true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monitorLogs, cipher)
	dispatcher := notify.NewDispatcher(notifies, cipher, slog.New(slog.NewTextHandler(io.Discard, nil)), notify.Policy{
		SubscriptionDailyRemainingThresholdPct: 90,
		SubscriptionAlertCooldown:              time.Hour,
		SendMaxAttempts:                        1,
	})
	monitorSvc := monitor.NewService(channels, announcements, rates, monitorLogs, channelSvc, dispatcher, slog.New(slog.NewTextHandler(io.Discard, nil)))

	r := gin.New()
	registerChannels(r.Group("/api"), &Deps{
		Channels: channels,
		Rates:    rates,
		Notifies: notifies,
		Monitor:  monitorSvc,
	})

	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}

	logs, err := notifies.ListLogs(10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	found := false
	for _, log := range logs {
		if log.Event == storage.EventSubscriptionDailyLow && log.Success {
			found = true
		}
	}
	if !found {
		t.Fatalf("subscription alert log not found: %#v", logs)
	}
}

type stubMonitor struct{}

func (s *stubMonitor) RefreshBalance(ctx context.Context, c *storage.Channel) error {
	progress.OK(ctx, progress.StageBalance, "ok")
	return nil
}

func (s *stubMonitor) RefreshRates(ctx context.Context, c *storage.Channel) error {
	return nil
}

func (s *stubMonitor) CheckSubscriptionUsageAlerts(ctx context.Context, c *storage.Channel) error {
	return nil
}

func mustEncryptAPI(t *testing.T, cipher *crypto.Cipher, plain string) string {
	t.Helper()
	out, err := cipher.Encrypt(plain)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return out
}

func findFirstJSONBody(s string) string {
	start := -1
	for i := 0; i < len(s); i++ {
		if s[i] == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	end := start
	depth := 0
	for i := start; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				end = i + 1
				return s[start:end]
			}
		}
	}
	return ""
}

func TestDeleteChannelCleansHistories(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	rates := storage.NewRates(db)
	monLogs := storage.NewMonitorLogs(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher)

	if err := channels.Create(&storage.Channel{
		ID:             1,
		Name:           "a",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://a.example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "a",
		Content:     "one",
		FirstSeenAt: time.Now(),
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if err := notifies.AppendLog(&storage.NotificationLog{
		ChannelID:         99,
		UpstreamChannelID: 1,
		Event:             storage.EventBalanceLow,
		Subject:           "a 余额低于阈值",
		Success:           true,
		SentAt:            time.Now(),
	}); err != nil {
		t.Fatalf("append log: %v", err)
	}

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{Channels: channels, ChannelSvc: channelSvc})

	req := httptest.NewRequest(http.MethodDelete, "/api/channels/1", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("announcements should be deleted: total=%d list=%#v", total, list)
	}
	logs, total, err := notifies.ListLogsPage(1, 10)
	if err != nil {
		t.Fatalf("list logs: %v", err)
	}
	if total != 0 || len(logs) != 0 {
		t.Fatalf("notification logs should be deleted: total=%d list=%#v", total, logs)
	}
}

func TestChannelCreateAndUpdateIgnoreAnnouncements(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db := openTestDB(t)
	channels := storage.NewChannels(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)
	authSessions := storage.NewAuthSessions(db)
	captchas := storage.NewCaptchas(db)
	monLogs := storage.NewMonitorLogs(db)
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	channelSvc := channel.NewService(channels, authSessions, captchas, rates, monLogs, cipher)

	r := gin.New()
	api := r.Group("/api")
	registerChannels(api, &Deps{Channels: channels, ChannelSvc: channelSvc, Notifies: notifies})

	createReq := httptest.NewRequest(http.MethodPost, "/api/channels", strings.NewReader(`{
			"name":"demo",
			"type":"sub2api",
			"site_url":"https://a.example.com",
			"username":"u",
			"password":"p",
		"ignore_announcements":true,
		"subscription_enabled":true
	}`))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	r.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("create status = %d body = %s", createRec.Code, createRec.Body.String())
	}

	ch, err := channels.FindByID(1)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if !ch.IgnoreAnnouncements {
		t.Fatalf("ignore_announcements = false, want true")
	}
	if !ch.SubscriptionEnabled {
		t.Fatalf("subscription_enabled = false, want true")
	}

	updateReq := httptest.NewRequest(http.MethodPut, "/api/channels/1", strings.NewReader(`{
		"ignore_announcements":false,
		"subscription_enabled":false
	}`))
	updateReq.Header.Set("Content-Type", "application/json")
	updateRec := httptest.NewRecorder()
	r.ServeHTTP(updateRec, updateReq)
	if updateRec.Code != http.StatusOK {
		t.Fatalf("update status = %d body = %s", updateRec.Code, updateRec.Body.String())
	}
	ch, err = channels.FindByID(1)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if ch.IgnoreAnnouncements {
		t.Fatalf("ignore_announcements = true, want false")
	}
	if ch.SubscriptionEnabled {
		t.Fatalf("subscription_enabled = true, want false")
	}

	newAPIReq := httptest.NewRequest(http.MethodPost, "/api/channels", strings.NewReader(`{
		"name":"demo-newapi",
		"type":"newapi",
		"site_url":"https://b.example.com",
		"username":"u",
		"password":"p",
		"subscription_enabled":true
	}`))
	newAPIReq.Header.Set("Content-Type", "application/json")
	newAPIRec := httptest.NewRecorder()
	r.ServeHTTP(newAPIRec, newAPIReq)
	if newAPIRec.Code != http.StatusOK {
		t.Fatalf("newapi create status = %d body = %s", newAPIRec.Code, newAPIRec.Body.String())
	}
	newAPICh, err := channels.FindByID(2)
	if err != nil {
		t.Fatalf("find newapi channel: %v", err)
	}
	if newAPICh.SubscriptionEnabled {
		t.Fatalf("newapi subscription_enabled = true, want false")
	}
}
