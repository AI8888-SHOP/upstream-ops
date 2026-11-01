package sub2api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestSetHTTPConfigAppliesUserAgentAndTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "custom-agent" {
			t.Fatalf("user agent = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{}}`))
	}))
	defer srv.Close()

	c := New()
	c.SetHTTPConfig(connector.HTTPConfig{
		Timeout:   45 * time.Second,
		UserAgent: "custom-agent",
	})
	if c.http.GetClient().Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", c.http.GetClient().Timeout)
	}
	if _, err := c.getJSON(context.Background(), srv.URL, nil); err != nil {
		t.Fatalf("getJSON: %v", err)
	}
}

func TestAuditTransportSurvivesProxyReset(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("secret"); got != "top-secret" && r.URL.Query().Get("token") != "second-secret" {
			t.Fatalf("unexpected upstream query: %q", r.URL.RawQuery)
		}
		_, _ = io.WriteString(w, `{"code":0,"message":"ok","data":{"balance":3.5}}`)
	}))
	defer upstream.Close()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outReq := r.Clone(r.Context())
		outReq.URL.Scheme = "http"
		outReq.URL.Host = strings.TrimPrefix(upstream.URL, "http://")
		outReq.RequestURI = ""
		resp, err := http.DefaultTransport.RoundTrip(outReq)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	db, err := storage.Open(storage.DBConfig{Driver: storage.DBDriverSQLite, Path: filepath.Join(t.TempDir(), "audit.db")})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("migrate db: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get db: %v", err)
	}
	defer sqlDB.Close()

	logs := storage.NewGatewayAPICallLogs(db)
	c := New()
	c.SetAPIAudit(apiaudit.Options{
		Store:   logs,
		Purpose: storage.GatewayAPICallPurposeChannelAPI,
		Metadata: storage.GatewayAPICallLog{
			Purpose:   storage.GatewayAPICallPurposeChannelAPI,
			ChannelID: 42,
		},
	})
	c.SetProxy(proxy.URL)
	if _, err := c.getJSON(context.Background(), upstream.URL+"/api/v1/test?secret=top-secret", nil); err != nil {
		t.Fatalf("first getJSON through proxy: %v", err)
	}
	c.SetProxy(proxy.URL)
	if _, err := c.getJSON(context.Background(), upstream.URL+"/api/v1/auth/me?token=second-secret", nil); err != nil {
		t.Fatalf("second getJSON through proxy: %v", err)
	}
	page, err := logs.List(storage.GatewayAPICallLogQuery{Purpose: storage.GatewayAPICallPurposeChannelAPI})
	if err != nil {
		t.Fatalf("list audit logs: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("audit page = %+v", page)
	}
	for _, item := range page.Items {
		if item.ChannelID != 42 || item.Purpose != storage.GatewayAPICallPurposeChannelAPI {
			t.Fatalf("metadata = %+v", item)
		}
		if item.Endpoint != "/api/v1/test" && item.Endpoint != "/api/v1/auth/me" {
			t.Fatalf("endpoint = %q", item.Endpoint)
		}
		if strings.Contains(item.Endpoint, "secret") {
			t.Fatalf("query secret leaked in endpoint = %q", item.Endpoint)
		}
	}
}

func TestLoginAddsExtraParams(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/login", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["email"] != "u" || body["password"] != "p" || body["device_id"] != "d1" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"token","refresh_token":"refresh","expires_in":3600}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.Login(context.Background(), &connector.Channel{
		SiteURL:          srv.URL,
		Username:         "u",
		Password:         "p",
		LoginExtraParams: map[string]any{"device_id": "d1"},
	})
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if session.AccessToken != "token" {
		t.Fatalf("session = %#v", session)
	}
	if session.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", session.RefreshToken)
	}
}

func TestRefreshSession(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/refresh", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["refresh_token"] != "old-refresh" {
			t.Fatalf("refresh_token = %q", body["refresh_token"])
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"access_token":"new-token","refresh_token":"new-refresh","expires_in":3600}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session, err := c.RefreshSession(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{RefreshToken: "old-refresh"})
	if err != nil {
		t.Fatalf("RefreshSession: %v", err)
	}
	if session.AccessToken != "new-token" {
		t.Fatalf("access token = %q, want new-token", session.AccessToken)
	}
	if session.RefreshToken != "new-refresh" {
		t.Fatalf("refresh token = %q, want new-refresh", session.RefreshToken)
	}
	if time.Until(session.ExpiresAt) <= 0 {
		t.Fatalf("expires at = %s, want future", session.ExpiresAt)
	}
}

func TestGetCosts(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":1.23,"total_actual_cost":45.67}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 1.23 {
		t.Fatalf("today cost = %v, want 1.23", res.TodayCost)
	}
	if res.TotalCost != 45.67 {
		t.Fatalf("total cost = %v, want 45.67", res.TotalCost)
	}
}

func TestGetCostsAppliesUpstreamRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/usage/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"today_actual_cost":14.4,"total_actual_cost":72}}`))
	})
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance_recharge_multiplier":7.2}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	res, err := c.GetCosts(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetCosts: %v", err)
	}
	if res.TodayCost != 2 {
		t.Fatalf("today cost = %v, want 2", res.TodayCost)
	}
	if res.TotalCost != 10 {
		t.Fatalf("total cost = %v, want 10", res.TotalCost)
	}
}

func TestGetBalanceAppliesManualRechargeMultiplier(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/auth/me", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"balance":12.5}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	multiplier := 3.0
	res, err := c.GetBalance(context.Background(), &connector.Channel{
		SiteURL:                srv.URL,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeMultiply,
	}, &connector.AuthSession{
		AccessToken: "token",
	})
	if err != nil {
		t.Fatalf("GetBalance: %v", err)
	}
	if res.Balance != 37.5 {
		t.Fatalf("balance = %v, want 37.5", res.Balance)
	}
}

func TestGetRechargeInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay_direct":{"payment_type":"alipay","currency":"CNY","fee_rate":0,"single_min":5,"single_max":100},"wxpay":{"payment_type":"wxpay","currency":"CNY","fee_rate":0,"single_min":8,"single_max":80},"stripe":{"payment_type":"stripe","single_min":10,"single_max":90}},"global_min":5,"global_max":100,"help_text":"请联系客服","help_image_url":"https://img.example/help.png","alipay_force_qrcode":true}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 2 {
		t.Fatalf("methods len = %d, want 2", len(info.Methods))
	}
	if info.Methods[0].Type != "alipay" || info.Methods[1].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
	if !info.AlipayForceQRCode {
		t.Fatal("want force qrcode")
	}
}

func TestGetRechargeInfoFiltersUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay":{"single_min":5,"single_max":100,"available":false},"wxpay":{"single_min":8,"single_max":80,"available":true}},"global_min":5,"global_max":100}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 1 || info.Methods[0].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
}

func TestGetRechargeInfoFallsBackToAvailableAlias(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay":{"single_min":50,"single_max":60,"available":false},"alipay_direct":{"single_min":5,"single_max":100,"available":true}},"global_min":5,"global_max":100}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetRechargeInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetRechargeInfo: %v", err)
	}
	if len(info.Methods) != 1 || info.Methods[0].Type != "alipay" || info.Methods[0].MinAmount != 5 {
		t.Fatalf("methods = %#v", info.Methods)
	}
}

func TestCreateRechargePrefersQRCodeOnDesktop(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test","expires_at":"2026-01-02T03:04:05Z"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      false,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargePrefersRedirectOnMobile(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeUsesPaymentModeRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"redirect","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      false,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeUsesPaymentModeQRCode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"native","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateRecharge: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateRechargeRejectsComplexFlow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"result_type":"oauth_required","oauth":{"authorize_url":"/api/v1/auth/oauth/wechat/payment/start"}}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateRecharge(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.RechargeRequest{
		Amount:        12.5,
		PaymentMethod: "wxpay",
	})
	if err == nil || !strings.Contains(err.Error(), "暂不支持") {
		t.Fatalf("err = %v, want unsupported error", err)
	}
}

func TestGetSubscriptionInfo(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/checkout-info", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"methods":{"alipay_direct":{"payment_type":"alipay","currency":"CNY","single_min":5,"single_max":100,"available":true},"wxpay":{"payment_type":"wxpay","currency":"CNY","single_min":8,"single_max":80,"available":true},"stripe":{"payment_type":"stripe","available":true}},"plans":[{"id":7,"group_id":3,"group_name":"pro","name":"Pro","description":"专业版","price":29.9,"daily_limit_usd":10,"weekly_limit_usd":50,"monthly_limit_usd":200,"validity_days":30,"validity_unit":"day","features":["高速","独享"],"for_sale":true}]}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionInfo(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionInfo: %v", err)
	}
	if len(info.Plans) != 1 || info.Plans[0].ID != "7" || info.Plans[0].GroupName != "pro" || len(info.Plans[0].Features) != 2 {
		t.Fatalf("plans = %#v", info.Plans)
	}
	if info.Plans[0].DailyLimitUSD == nil || *info.Plans[0].DailyLimitUSD != 10 || info.Plans[0].WeeklyLimitUSD == nil || *info.Plans[0].WeeklyLimitUSD != 50 || info.Plans[0].MonthlyLimitUSD == nil || *info.Plans[0].MonthlyLimitUSD != 200 {
		t.Fatalf("limits = %#v", info.Plans[0])
	}
	if len(info.Methods) != 2 || info.Methods[0].Type != "alipay" || info.Methods[1].Type != "wxpay" {
		t.Fatalf("methods = %#v", info.Methods)
	}
	if got := strings.Join(info.Plans[0].PaymentMethods, ","); got != "alipay,wxpay" {
		t.Fatalf("payment methods = %q", got)
	}
}

func TestGetSubscriptionUsage(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":9,"group_id":3,"status":"active","starts_at":"2026-01-01T00:00:00Z","expires_at":"2026-02-01T00:00:00Z","group":{"id":3,"name":"pro"}},"progress":{"id":9,"group_name":"pro","expires_at":"2026-02-01T00:00:00Z","expires_in_days":12,"daily":{"limit_usd":10,"used_usd":8,"remaining_usd":2,"percentage":80,"window_start":"2026-01-02T00:00:00Z","resets_at":"2026-01-03T00:00:00Z","resets_in_seconds":3600},"weekly":{"limit_usd":0,"used_usd":0,"remaining_usd":0,"percentage":0},"monthly":{"limit_usd":100,"used_usd":95,"remaining_usd":5,"percentage":95,"window_start":"2026-01-01T00:00:00Z","resets_at":"2026-02-01T00:00:00Z","resets_in_seconds":86400}}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.ID != 9 || item.GroupID != 3 || item.GroupName != "pro" || item.Status != "active" {
		t.Fatalf("item = %#v", item)
	}
	if item.Daily == nil || item.Daily.UsedPercent != 80 || item.Daily.RemainingPercent != 20 || item.Daily.RemainingUSD != 2 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil {
		t.Fatalf("weekly should be hidden for unlimited limit: %#v", item.Weekly)
	}
	if item.Monthly == nil || item.Monthly.RemainingPercent != 5 {
		t.Fatalf("monthly = %#v", item.Monthly)
	}
}

func TestGetSubscriptionUsageDirectSubscriptionList(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":21,"user_id":1,"group_id":14,"starts_at":"2026-06-17T20:08:51.441599+08:00","expires_at":"2026-06-18T20:08:51.441599+08:00","status":"active","daily_window_start":null,"weekly_window_start":null,"monthly_window_start":null,"daily_usage_usd":25,"weekly_usage_usd":0,"monthly_usage_usd":0,"group":{"id":14,"name":"Codex 100刀","daily_limit_usd":100,"weekly_limit_usd":0,"monthly_limit_usd":0}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.ID != 21 || item.GroupID != 14 || item.GroupName != "Codex 100刀" || item.Status != "active" {
		t.Fatalf("item = %#v", item)
	}
	if item.Daily == nil || item.Daily.LimitUSD != 100 || item.Daily.UsedUSD != 25 || item.Daily.RemainingUSD != 75 || item.Daily.RemainingPercent != 75 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil || item.Monthly != nil {
		t.Fatalf("unlimited windows should be hidden: weekly=%#v monthly=%#v", item.Weekly, item.Monthly)
	}
}

func TestGetSubscriptionUsageSubscriptionFallbackLimits(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/subscriptions/progress", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"subscription":{"id":21,"group_id":14,"status":"active","starts_at":"2026-06-17T20:08:51.441599+08:00","expires_at":"2026-06-18T20:08:51.441599+08:00","daily_usage_usd":0,"weekly_usage_usd":0,"monthly_usage_usd":0,"group":{"id":14,"name":"Codex 100刀","daily_limit_usd":100,"weekly_limit_usd":0,"monthly_limit_usd":0}},"progress":{"id":21,"group_name":"Codex 100刀","expires_at":"2026-06-18T20:08:51.441599+08:00","expires_in_days":1}}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	info, err := c.GetSubscriptionUsage(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetSubscriptionUsage: %v", err)
	}
	if len(info.Items) != 1 {
		t.Fatalf("items = %#v", info.Items)
	}
	item := info.Items[0]
	if item.Daily == nil || item.Daily.LimitUSD != 100 || item.Daily.UsedUSD != 0 || item.Daily.RemainingUSD != 100 || item.Daily.RemainingPercent != 100 {
		t.Fatalf("daily = %#v", item.Daily)
	}
	if item.Weekly != nil || item.Monthly != nil {
		t.Fatalf("unlimited windows should be hidden: weekly=%#v monthly=%#v", item.Weekly, item.Monthly)
	}
}

func TestCreateSubscriptionQRCode(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body["order_type"] != "subscription" || body["plan_id"] != float64(7) || body["payment_type"] != "wxpay" {
			t.Fatalf("body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"native","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test","expires_at":"2026-01-02T03:04:05Z"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "wxpay",
		IsMobile:      true,
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if launch.Mode != "qrcode" || launch.QRCode == "" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateSubscriptionRedirect(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"payment_mode":"redirect","pay_url":"https://pay.example.com/redirect","qr_code":"weixin://wxpay/bizpayurl?pr=test"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	launch, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "alipay",
	})
	if err != nil {
		t.Fatalf("CreateSubscription: %v", err)
	}
	if launch.Mode != "redirect" || launch.PayURL != "https://pay.example.com/redirect" {
		t.Fatalf("launch = %#v", launch)
	}
}

func TestCreateSubscriptionRejectsComplexFlow(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/payment/orders", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"","data":{"client_secret":"secret"}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	_, err := c.CreateSubscription(context.Background(), &connector.Channel{
		SiteURL: srv.URL,
	}, &connector.AuthSession{AccessToken: "token"}, connector.SubscriptionRequest{
		PlanID:        "7",
		PaymentMethod: "wxpay",
	})
	if err == nil || !strings.Contains(err.Error(), "暂不支持") {
		t.Fatalf("err = %v, want unsupported error", err)
	}
}

func TestListAPIKeys(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","description":"专业组","rate_multiplier":1.2}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"3":1.5}}`))
	})
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("page"); got != "2" {
			t.Fatalf("page = %q, want 2", got)
		}
		if got := r.URL.Query().Get("page_size"); got != "10" {
			t.Fatalf("page_size = %q, want 10", got)
		}
		if got := r.URL.Query().Get("search"); got != "main" {
			t.Fatalf("search = %q, want main", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{"id":8,"key":"sk-full","name":"main","group_id":3,"status":"active","ip_whitelist":["1.1.1.1"],"ip_blacklist":[],"quota":10,"quota_used":2,"rate_limit_5h":1,"usage_5h":0.5}],"total":1,"page":2,"page_size":10,"pages":1}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	page, err := c.ListAPIKeys(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"}, connector.APIKeyQuery{
		Page:     2,
		PageSize: 10,
		Search:   "main",
	})
	if err != nil {
		t.Fatalf("ListAPIKeys: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Key != "sk-full" || page.Items[0].GroupID == nil || *page.Items[0].GroupID != 3 || page.Items[0].GroupName != "pro" || page.Items[0].GroupRatio != 1.5 {
		t.Fatalf("page = %#v", page)
	}
}

func TestListAPIKeyGroups(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/groups/available", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":3,"name":"pro","description":"专业组","rate_multiplier":1.2}]}`))
	})
	mux.HandleFunc("/api/v1/groups/rates", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"3":1.5}}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	groups, err := c.ListAPIKeyGroups(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("ListAPIKeyGroups: %v", err)
	}
	if len(groups) != 1 || groups[0].ID == nil || *groups[0].ID != 3 || groups[0].Name != "pro" || groups[0].Ratio != 1.5 {
		t.Fatalf("groups = %#v", groups)
	}
}

func TestGetAnnouncements(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/announcements", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer token" {
			t.Fatalf("authorization = %q", got)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":[{"id":9,"title":"维护公告","content":"今晚维护","notify_mode":"popup","created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T04:04:05Z"}]}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	items, err := c.GetAnnouncements(context.Background(), &connector.Channel{SiteURL: srv.URL}, &connector.AuthSession{AccessToken: "token"})
	if err != nil {
		t.Fatalf("GetAnnouncements: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	if items[0].SourceKey != "9" || items[0].Title != "维护公告" || items[0].Type != "popup" {
		t.Fatalf("item = %#v", items[0])
	}
	if items[0].PublishedAt == nil || items[0].PublishedAt.Format("2006-01-02T15:04:05Z") != "2026-01-02T03:04:05Z" {
		t.Fatalf("published at = %#v", items[0].PublishedAt)
	}
	if items[0].SourceUpdatedAt == nil || items[0].SourceUpdatedAt.Format("2006-01-02T15:04:05Z") != "2026-01-02T04:04:05Z" {
		t.Fatalf("updated at = %#v", items[0].SourceUpdatedAt)
	}
}

func TestCreateUpdateDeleteRevealAPIKey(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/keys", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode create: %v", err)
		}
		if body["name"] != "main" || body["custom_key"] != "sk-custom" {
			t.Fatalf("create body = %#v", body)
		}
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"active"}}`))
	})
	mux.HandleFunc("/api/v1/keys/8", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode update: %v", err)
			}
			if body["status"] != "inactive" {
				t.Fatalf("update body = %#v", body)
			}
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"disabled"}}`))
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"message":"ok"}}`))
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"id":8,"key":"sk-custom","name":"main","status":"active"}}`))
		default:
			t.Fatalf("unexpected method %s", r.Method)
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := New()
	session := &connector.AuthSession{AccessToken: "token"}
	created, err := c.CreateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, connector.APIKeyCreateRequest{
		Name:      "main",
		CustomKey: "sk-custom",
	})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if created.Key != "sk-custom" {
		t.Fatalf("created = %#v", created)
	}
	updated, err := c.UpdateAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8, connector.APIKeyUpdateRequest{
		Status: strPtr("disabled"),
	})
	if err != nil {
		t.Fatalf("UpdateAPIKey: %v", err)
	}
	if updated.Status != "disabled" {
		t.Fatalf("updated status = %q", updated.Status)
	}
	if err := c.DeleteAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8); err != nil {
		t.Fatalf("DeleteAPIKey: %v", err)
	}
	key, err := c.RevealAPIKey(context.Background(), &connector.Channel{SiteURL: srv.URL}, session, 8)
	if err != nil {
		t.Fatalf("RevealAPIKey: %v", err)
	}
	if key != "sk-custom" {
		t.Fatalf("key = %q", key)
	}
}

func TestBuildSub2UpdateAPIKeyPreservesUnspecifiedIPLists(t *testing.T) {
	groupID := int64(3)
	body, err := buildSub2UpdateAPIKey(connector.APIKeyUpdateRequest{GroupID: &groupID})
	if err != nil {
		t.Fatalf("build group update: %v", err)
	}
	if body["group_id"] != groupID {
		t.Fatalf("group_id = %#v", body["group_id"])
	}
	if _, ok := body["ip_whitelist"]; ok {
		t.Fatalf("group-only update contains ip_whitelist: %#v", body)
	}
	if _, ok := body["ip_blacklist"]; ok {
		t.Fatalf("group-only update contains ip_blacklist: %#v", body)
	}

	body, err = buildSub2UpdateAPIKey(connector.APIKeyUpdateRequest{
		IPWhitelist: []string{},
		IPBlacklist: []string{},
	})
	if err != nil {
		t.Fatalf("build explicit empty ip update: %v", err)
	}
	if _, ok := body["ip_whitelist"]; !ok {
		t.Fatalf("explicit empty ip_whitelist omitted: %#v", body)
	}
	if _, ok := body["ip_blacklist"]; !ok {
		t.Fatalf("explicit empty ip_blacklist omitted: %#v", body)
	}
}

func strPtr(v string) *string {
	return &v
}
