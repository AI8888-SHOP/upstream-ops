package channel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/connector"
	_ "github.com/bejix/upstream-ops/backend/connector/newapi"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

type fakeHTTPConfigConnector struct {
	cfg connector.HTTPConfig
}

func (f *fakeHTTPConfigConnector) SetHTTPConfig(cfg connector.HTTPConfig) {
	f.cfg = cfg
}

func testService(t *testing.T) (*Service, *crypto.Cipher) {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "test.db"),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	cipher, err := crypto.NewCipher("12345678901234567890123456789012")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	svc := NewService(
		storage.NewChannels(db),
		storage.NewAuthSessions(db),
		storage.NewCaptchas(db),
		storage.NewRates(db),
		storage.NewMonitorLogs(db),
		cipher,
	)
	return svc, cipher
}

func TestResolveAppliesProxyOnlyWhenEnabled(t *testing.T) {
	svc, cipher := testService(t)
	svc.UpdateProxyConfig(config.ProxyConfig{
		Enabled:  true,
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     1080,
		Username: "u",
		Password: "p",
	})

	enc, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	withProxy := &storage.Channel{
		ID:             1,
		Name:           "with-proxy",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		ProxyEnabled:   true,
	}
	resolved, err := svc.Resolve(context.Background(), withProxy)
	if err != nil {
		t.Fatalf("resolve with proxy: %v", err)
	}
	if resolved.ProxyURL != "socks5://u:p@127.0.0.1:1080" {
		t.Fatalf("proxy url = %q", resolved.ProxyURL)
	}

	withoutProxy := *withProxy
	withoutProxy.ProxyEnabled = false
	resolved, err = svc.Resolve(context.Background(), &withoutProxy)
	if err != nil {
		t.Fatalf("resolve without proxy: %v", err)
	}
	if resolved.ProxyURL != "" {
		t.Fatalf("proxy url = %q, want empty", resolved.ProxyURL)
	}
}

func TestResolveSkipsProxyWhenGlobalProxyDisabled(t *testing.T) {
	svc, cipher := testService(t)
	svc.UpdateProxyConfig(config.ProxyConfig{
		Protocol: "socks5",
		Host:     "127.0.0.1",
		Port:     1080,
	})
	enc, err := cipher.Encrypt("secret")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	resolved, err := svc.Resolve(context.Background(), &storage.Channel{
		ID:             1,
		Name:           "with-proxy",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		ProxyEnabled:   true,
	})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if resolved.ProxyURL != "" {
		t.Fatalf("proxy url = %q, want empty", resolved.ProxyURL)
	}
}

func TestApplyHTTPConfigUsesUpstreamConfig(t *testing.T) {
	svc, _ := testService(t)
	svc.UpdateUpstreamConfig(config.UpstreamConfig{
		TimeoutSeconds: 45,
		UserAgent:      "custom-agent",
	})

	conn := &fakeHTTPConfigConnector{}
	svc.ApplyHTTPConfig(conn)

	if conn.cfg.Timeout != 45*time.Second {
		t.Fatalf("timeout = %s", conn.cfg.Timeout)
	}
	if conn.cfg.UserAgent != "custom-agent" {
		t.Fatalf("user agent = %q", conn.cfg.UserAgent)
	}
}

func TestApplyHTTPConfigUsesDefaults(t *testing.T) {
	svc, _ := testService(t)
	conn := &fakeHTTPConfigConnector{}
	svc.ApplyHTTPConfig(conn)

	if conn.cfg.Timeout != time.Duration(config.DefaultUpstreamTimeoutSeconds)*time.Second {
		t.Fatalf("timeout = %s", conn.cfg.Timeout)
	}
	if conn.cfg.UserAgent != config.DefaultUpstreamUserAgent {
		t.Fatalf("user agent = %q", conn.cfg.UserAgent)
	}
}

func TestBuildSessionFromSub2APITokenCredentialIncludesRefreshToken(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt(`{"access_token":"access","refresh_token":"refresh"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	session, err := svc.buildSessionFromToken(&storage.Channel{
		Type:           storage.ChannelTypeSub2API,
		PasswordCipher: enc,
	})
	if err != nil {
		t.Fatalf("buildSessionFromToken: %v", err)
	}
	if session.AccessToken != "access" {
		t.Fatalf("access token = %q, want access", session.AccessToken)
	}
	if session.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", session.RefreshToken)
	}
}

func TestPersistSessionStoresRefreshToken(t *testing.T) {
	svc, _ := testService(t)
	session := &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}
	if err := svc.persistSession(7, session); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	saved, err := svc.AuthSessions.FindByChannel(7)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved == nil {
		t.Fatal("saved session is nil")
	}
	if saved.RefreshTokenCipher == "" {
		t.Fatal("refresh token cipher is empty")
	}

	decrypted, err := svc.decryptSession(saved)
	if err != nil {
		t.Fatalf("decryptSession: %v", err)
	}
	if decrypted.RefreshToken != "refresh" {
		t.Fatalf("refresh token = %q, want refresh", decrypted.RefreshToken)
	}
}

func TestPersistTokenCredentialStoresSub2APIRefreshToken(t *testing.T) {
	svc, cipher := testService(t)
	oldCred, err := cipher.Encrypt(`{"access_token":"old","refresh_token":"old-refresh"}`)
	if err != nil {
		t.Fatalf("encrypt old credential: %v", err)
	}
	ch := &storage.Channel{
		Name:           "sub",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: oldCred,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	err = svc.persistTokenCredential(ch, &connector.AuthSession{
		AccessToken:  "new",
		RefreshToken: "new-refresh",
	})
	if err != nil {
		t.Fatalf("persistTokenCredential: %v", err)
	}

	updated, err := svc.Channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("FindByID: %v", err)
	}
	raw, err := cipher.Decrypt(updated.PasswordCipher)
	if err != nil {
		t.Fatalf("decrypt credential: %v", err)
	}
	var cred Sub2APITokenCredential
	if err := json.Unmarshal([]byte(raw), &cred); err != nil {
		t.Fatalf("unmarshal credential: %v", err)
	}
	if cred.AccessToken != "new" {
		t.Fatalf("access token = %q, want new", cred.AccessToken)
	}
	if cred.RefreshToken != "new-refresh" {
		t.Fatalf("refresh token = %q, want new-refresh", cred.RefreshToken)
	}
}

func TestClearLoginInfoClearsTokenCredentialAndSession(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt(`{"cookie":"session=abc","user_id":"1"}`)
	if err != nil {
		t.Fatalf("encrypt credential: %v", err)
	}
	ch := &storage.Channel{
		Name:           "newapi-token",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "memo",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModeToken,
		MonitorEnabled: true,
		LastError:      "old error",
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := svc.persistSession(ch.ID, &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		Cookie:       "session=abc",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	updated, err := svc.ClearLoginInfo(ch.ID)
	if err != nil {
		t.Fatalf("ClearLoginInfo: %v", err)
	}
	if updated.PasswordCipher != "" {
		t.Fatalf("password cipher = %q, want empty", updated.PasswordCipher)
	}
	if updated.LastError != "" {
		t.Fatalf("last error = %q, want empty", updated.LastError)
	}
	saved, err := svc.AuthSessions.FindByChannel(ch.ID)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved != nil {
		t.Fatalf("auth session = %#v, want nil", saved)
	}
	if _, err := svc.buildSessionFromToken(updated); err == nil {
		t.Fatal("buildSessionFromToken after clear succeeded, want error")
	}
}

func TestClearLoginInfoKeepsPasswordCredential(t *testing.T) {
	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("password")
	if err != nil {
		t.Fatalf("encrypt password: %v", err)
	}
	ch := &storage.Channel{
		Name:           "password-channel",
		Type:           storage.ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModePassword,
		MonitorEnabled: true,
		LastError:      "old error",
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := svc.persistSession(ch.ID, &connector.AuthSession{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("persistSession: %v", err)
	}

	updated, err := svc.ClearLoginInfo(ch.ID)
	if err != nil {
		t.Fatalf("ClearLoginInfo: %v", err)
	}
	if updated.PasswordCipher != enc {
		t.Fatal("password credential was cleared in password mode")
	}
	saved, err := svc.AuthSessions.FindByChannel(ch.ID)
	if err != nil {
		t.Fatalf("FindByChannel: %v", err)
	}
	if saved != nil {
		t.Fatalf("auth session = %#v, want nil", saved)
	}
}

func TestTestLoginFailsWhenSessionAuthFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "bad"})
		_, _ = w.Write([]byte(`{"success":true,"message":"","data":{"id":7}}`))
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"success":false,"message":"Unauthorized, not logged in and no access token provided"}`, http.StatusUnauthorized)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	svc, cipher := testService(t)
	enc, err := cipher.Encrypt("p")
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	ch := &storage.Channel{
		Name:           "bad-session",
		Type:           storage.ChannelTypeNewAPI,
		SiteURL:        srv.URL,
		Username:       "u",
		PasswordCipher: enc,
		CredentialMode: storage.CredentialModePassword,
	}
	if err := svc.Channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	err = svc.TestLogin(context.Background(), ch.ID)
	if err == nil {
		t.Fatal("TestLogin succeeded, want auth failure")
	}
	if !strings.Contains(err.Error(), "登录后鉴权失败") {
		t.Fatalf("error = %q", err.Error())
	}
	saved, err := svc.AuthSessions.FindByChannel(ch.ID)
	if err != nil {
		t.Fatalf("find session: %v", err)
	}
	if saved != nil {
		t.Fatalf("saved session = %#v, want nil", saved)
	}
}

func TestApplyGroupMultiplierToAPIKey(t *testing.T) {
	multiplier := 2.0
	key := &connector.APIKey{GroupRatio: 1.23456}
	applyGroupMultiplierToAPIKey(key, &connector.Channel{
		GroupMultiplier:     &multiplier,
		GroupMultiplierMode: connector.RechargeMultiplierModeMultiply,
	})
	if key.GroupRatio != 2.4691 {
		t.Fatalf("group ratio = %v, want 2.4691", key.GroupRatio)
	}
}

func TestApplyGroupMultiplierToAPIKeyWithoutConfiguration(t *testing.T) {
	key := &connector.APIKey{GroupRatio: 1.234567}
	applyGroupMultiplierToAPIKey(key, &connector.Channel{})
	if key.GroupRatio != 1.234567 {
		t.Fatalf("group ratio = %v, want upstream value", key.GroupRatio)
	}
}

func TestChannelGroupMultiplierCRUD(t *testing.T) {
	svc, _ := testService(t)
	multiplier := 2.0
	created, err := svc.Create(CreateInput{
		Name:              "group-ratio",
		Type:              storage.ChannelTypeNewAPI,
		SiteURL:           "https://example.com",
		Username:          "user",
		Password:          "password",
		CredentialMode:    storage.CredentialModePassword,
		GroupMultiplier:   &multiplier,
		GroupMultiplierMode: connector.RechargeMultiplierModeMultiply,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GroupMultiplier == nil || *created.GroupMultiplier != 2 || created.GroupMultiplierMode != connector.RechargeMultiplierModeMultiply {
		t.Fatalf("created group multiplier = %#v mode=%q", created.GroupMultiplier, created.GroupMultiplierMode)
	}

	updatedMultiplier := 4.0
	updatedMode := "invalid"
	updated, err := svc.Update(created.ID, UpdateInput{
		GroupMultiplier:     &updatedMultiplier,
		GroupMultiplierMode: &updatedMode,
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.GroupMultiplier == nil || *updated.GroupMultiplier != 4 || updated.GroupMultiplierMode != connector.RechargeMultiplierModeDivide {
		t.Fatalf("updated group multiplier = %#v mode=%q", updated.GroupMultiplier, updated.GroupMultiplierMode)
	}

	clear := 0.0
	updated, err = svc.Update(created.ID, UpdateInput{GroupMultiplier: &clear})
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	if updated.GroupMultiplier != nil {
		t.Fatalf("cleared group multiplier = %v, want nil", *updated.GroupMultiplier)
	}
}

func TestChannelRechargeMultiplierChangeClearsCollectedMetrics(t *testing.T) {
	svc, _ := testService(t)
	multiplier := 2.0
	created, err := svc.Create(CreateInput{
		Name:                   "recharge-metrics",
		Type:                   storage.ChannelTypeNewAPI,
		SiteURL:                "https://example.com",
		Username:               "user",
		Password:               "password",
		CredentialMode:         storage.CredentialModePassword,
		RechargeMultiplier:     &multiplier,
		RechargeMultiplierMode: connector.RechargeMultiplierModeDivide,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	lastBalance := 10.0
	todayCost := 2.0
	totalCost := 20.0
	created.LastBalance = &lastBalance
	created.LastBalanceAt = ptrTime(time.Now())
	created.TodayCost = &todayCost
	created.TotalCost = &totalCost
	if err := svc.Channels.Update(created); err != nil {
		t.Fatalf("seed metrics: %v", err)
	}
	renamed := "recharge-metrics-renamed"
	unchanged, err := svc.Update(created.ID, UpdateInput{Name: &renamed})
	if err != nil {
		t.Fatalf("update unrelated field: %v", err)
	}
	if unchanged.LastBalance == nil || unchanged.TodayCost == nil || unchanged.TotalCost == nil {
		t.Fatalf("unrelated update cleared collected metrics: %#v", unchanged)
	}

	updatedMultiplier := 4.0
	updated, err := svc.Update(created.ID, UpdateInput{RechargeMultiplier: &updatedMultiplier})
	if err != nil {
		t.Fatalf("update multiplier: %v", err)
	}
	if updated.LastBalance != nil || updated.LastBalanceAt != nil || updated.TodayCost != nil || updated.TotalCost != nil {
		t.Fatalf("metrics after multiplier change = balance=%v at=%v today=%v total=%v, want cleared",
			updated.LastBalance, updated.LastBalanceAt, updated.TodayCost, updated.TotalCost)
	}

	refetched, err := svc.Channels.FindByID(created.ID)
	if err != nil {
		t.Fatalf("refetch: %v", err)
	}
	if refetched.LastBalance != nil || refetched.TodayCost != nil || refetched.TotalCost != nil {
		t.Fatalf("persisted metrics after multiplier change = %#v, want cleared", refetched)
	}

	refetched.LastBalance = &lastBalance
	refetched.LastBalanceAt = ptrTime(time.Now())
	refetched.TodayCost = &todayCost
	refetched.TotalCost = &totalCost
	if err := svc.Channels.Update(refetched); err != nil {
		t.Fatalf("reseed metrics: %v", err)
	}
	multiplyMode := connector.RechargeMultiplierModeMultiply
	updated, err = svc.Update(created.ID, UpdateInput{RechargeMultiplierMode: &multiplyMode})
	if err != nil {
		t.Fatalf("update multiplier mode: %v", err)
	}
	if updated.LastBalance != nil || updated.LastBalanceAt != nil || updated.TodayCost != nil || updated.TotalCost != nil {
		t.Fatalf("metrics after multiplier mode change = %#v, want cleared", updated)
	}
}

func ptrTime(v time.Time) *time.Time {
	return &v
}
