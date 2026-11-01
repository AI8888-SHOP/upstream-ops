// Package sub2api 实现 sub2api 风格上游站点的 connector，参考 docs/USER_BALANCE_GROUP_RATE_AUTH_API_CN-sub2api.md。
package sub2api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/go-resty/resty/v2"
)

func init() {
	connector.Register(connector.TypeSub2API, func() connector.Connector { return New() })
}

type Client struct {
	http          *resty.Client
	baseTransport *http.Transport
	audit         *apiaudit.Options
}

func New() *Client {
	rc := resty.New().
		SetTimeout(30*time.Second).
		SetHeader("User-Agent", "upstream-ops/0.1").
		SetHeader("Accept", "application/json")
	base, _ := rc.Transport()
	return &Client{http: rc, baseTransport: base}
}

func (c *Client) SetProxy(proxyURL string) {
	if strings.TrimSpace(proxyURL) == "" || c.baseTransport == nil {
		return
	}
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return
	}
	c.baseTransport.Proxy = http.ProxyURL(parsed)
	c.rebuildTransport()
}

// SetAPIAudit installs the audit wrapper around the connector's inner
// transport. Rebuilding from baseTransport keeps it intact across SetProxy.
func (c *Client) SetAPIAudit(opts apiaudit.Options) {
	if opts.Store == nil {
		c.audit = nil
	} else {
		copy := opts
		c.audit = &copy
	}
	c.rebuildTransport()
}

func (c *Client) rebuildTransport() {
	if c.baseTransport == nil {
		return
	}
	var transport http.RoundTripper = c.baseTransport
	if c.audit != nil {
		transport = apiaudit.WrapTransport(transport, *c.audit)
	}
	c.http.SetTransport(transport)
}

func (c *Client) SetHTTPConfig(cfg connector.HTTPConfig) {
	if cfg.Timeout > 0 {
		c.http.SetTimeout(cfg.Timeout)
	}
	if strings.TrimSpace(cfg.UserAgent) != "" {
		c.http.SetHeader("User-Agent", cfg.UserAgent)
	}
}

// sub2Resp sub2api 统一响应外壳：{ code, message, data }。code 0 = 成功。
type sub2Resp struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

func (c *Client) GetTurnstileSiteKey(ctx context.Context, ch *connector.Channel) (string, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/settings/public", nil)
	if err != nil {
		return "", fmt.Errorf("sub2api public settings: %w", err)
	}
	var settings struct {
		TurnstileEnabled bool   `json:"turnstile_enabled"`
		TurnstileSiteKey string `json:"turnstile_site_key"`
	}
	if err := json.Unmarshal(body, &settings); err != nil {
		return "", fmt.Errorf("sub2api public settings decode: %w", err)
	}
	if !settings.TurnstileEnabled {
		return "", nil
	}
	return settings.TurnstileSiteKey, nil
}

func (c *Client) Login(ctx context.Context, ch *connector.Channel) (*connector.AuthSession, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	body := map[string]any{
		"email":    ch.Username,
		"password": ch.Password,
	}
	for k, v := range ch.LoginExtraParams {
		body[k] = v
	}
	if ch.TurnstileToken != "" {
		body["turnstile_token"] = ch.TurnstileToken
	}

	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(body).
		Post(site + "/api/v1/auth/login")
	if err != nil {
		return nil, fmt.Errorf("sub2api login http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api login: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api login decode: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api login: %s", wrapped.Message)
	}

	var data struct {
		Requires2FA  bool   `json:"requires_2fa"`
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(wrapped.Data, &data); err != nil {
		return nil, fmt.Errorf("sub2api login data: %w", err)
	}
	if data.Requires2FA {
		return nil, errors.New("sub2api account requires 2FA; please disable it for monitoring accounts")
	}
	if data.AccessToken == "" {
		return nil, errors.New("sub2api login: empty access_token")
	}

	expires := time.Now().Add(time.Duration(data.ExpiresIn) * time.Second)
	if data.ExpiresIn <= 0 {
		expires = time.Now().Add(time.Hour)
	}
	return &connector.AuthSession{
		AccessToken:  data.AccessToken,
		RefreshToken: data.RefreshToken,
		ExpiresAt:    expires,
	}, nil
}

func (c *Client) RefreshSession(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.AuthSession, error) {
	if session == nil || strings.TrimSpace(session.RefreshToken) == "" {
		return nil, errors.New("missing sub2api refresh_token")
	}
	site := strings.TrimRight(ch.SiteURL, "/")
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(map[string]string{"refresh_token": strings.TrimSpace(session.RefreshToken)}).
		Post(site + "/api/v1/auth/refresh")
	if err != nil {
		return nil, fmt.Errorf("sub2api refresh token http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api refresh token: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api refresh token decode: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api refresh token: %s", wrapped.Message)
	}
	var data struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(wrapped.Data, &data); err != nil {
		return nil, fmt.Errorf("sub2api refresh token data: %w", err)
	}
	if strings.TrimSpace(data.AccessToken) == "" {
		return nil, errors.New("sub2api refresh token: empty access_token")
	}
	expires := time.Now().Add(time.Duration(data.ExpiresIn) * time.Second)
	if data.ExpiresIn <= 0 {
		expires = time.Now().Add(time.Hour)
	}
	refreshToken := strings.TrimSpace(data.RefreshToken)
	if refreshToken == "" {
		refreshToken = strings.TrimSpace(session.RefreshToken)
	}
	return &connector.AuthSession{
		AccessToken:  strings.TrimSpace(data.AccessToken),
		RefreshToken: refreshToken,
		ExpiresAt:    expires,
	}, nil
}

func (c *Client) CheckAuth(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) error {
	if session == nil || session.AccessToken == "" {
		return errors.New("missing sub2api access_token")
	}
	_, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/auth/me", session)
	return err
}

func (c *Client) GetBalance(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.BalanceResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/auth/me", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api me: %w", err)
	}
	var me struct {
		Balance float64 `json:"balance"`
	}
	if err := json.Unmarshal(body, &me); err != nil {
		return nil, fmt.Errorf("sub2api me decode: %w", err)
	}
	multiplier := c.rechargeMultiplier(ctx, ch, session)
	return &connector.BalanceResult{
		Balance:   connector.ApplyRechargeMultiplier(me.Balance, multiplier, ch.RechargeMultiplierMode),
		SampledAt: time.Now(),
	}, nil
}

func (c *Client) GetCosts(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.CostResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/usage/dashboard/stats", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api dashboard stats: %w", err)
	}
	var stats struct {
		TodayActualCost float64 `json:"today_actual_cost"`
		TotalActualCost float64 `json:"total_actual_cost"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		return nil, fmt.Errorf("sub2api dashboard stats decode: %w", err)
	}
	multiplier := c.rechargeMultiplier(ctx, ch, session)
	return &connector.CostResult{
		TodayCost: connector.ApplyRechargeMultiplier(stats.TodayActualCost, multiplier, ch.RechargeMultiplierMode),
		TotalCost: connector.ApplyRechargeMultiplier(stats.TotalActualCost, multiplier, ch.RechargeMultiplierMode),
	}, nil
}

func (c *Client) rechargeMultiplier(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) *float64 {
	if ch.RechargeMultiplier != nil && *ch.RechargeMultiplier > 0 {
		return ch.RechargeMultiplier
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil
	}
	var raw struct {
		BalanceRechargeMultiplier float64 `json:"balance_recharge_multiplier"`
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.BalanceRechargeMultiplier <= 0 {
		return nil
	}
	return &raw.BalanceRechargeMultiplier
}

func (c *Client) GetRates(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.RateResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")

	availBody, err := c.getJSON(ctx, site+"/api/v1/groups/available", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api groups available: %w", err)
	}
	var groups []struct {
		ID             uint64  `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	}
	if err := json.Unmarshal(availBody, &groups); err != nil {
		return nil, fmt.Errorf("sub2api groups available decode: %w", err)
	}

	overrides := map[string]float64{}
	if ratesBody, err := c.getJSON(ctx, site+"/api/v1/groups/rates", session); err == nil {
		_ = json.Unmarshal(ratesBody, &overrides)
	}

	out := make([]connector.RateResult, 0, len(groups))
	for _, g := range groups {
		rate := g.RateMultiplier
		if v, ok := overrides[strconv.FormatUint(g.ID, 10)]; ok {
			rate = v
		}
		out = append(out, connector.RateResult{
			GroupID:     ptrInt64(g.ID),
			ModelName:   g.Name,
			Description: g.Description,
			Ratio:       rate,
		})
	}
	return out, nil
}

func ptrInt64(v uint64) *int64 {
	id := int64(v)
	return &id
}

func (c *Client) GetAnnouncements(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.AnnouncementResult, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/announcements", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api announcements: %w", err)
	}
	var raw []struct {
		ID         int64  `json:"id"`
		Title      string `json:"title"`
		Content    string `json:"content"`
		NotifyMode string `json:"notify_mode"`
		StartsAt   string `json:"starts_at"`
		EndsAt     string `json:"ends_at"`
		CreatedAt  string `json:"created_at"`
		UpdatedAt  string `json:"updated_at"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api announcements decode: %w", err)
	}
	out := make([]connector.AnnouncementResult, 0, len(raw))
	for _, a := range raw {
		if a.ID == 0 && strings.TrimSpace(a.Content) == "" && strings.TrimSpace(a.Title) == "" {
			continue
		}
		publishedAt := parseAnnouncementTime(a.StartsAt, a.CreatedAt)
		updatedAt := parseAnnouncementTime(a.UpdatedAt)
		sourceKey := strconv.FormatInt(a.ID, 10)
		if a.ID == 0 {
			sourceKey = hashAnnouncementKey(a.Title, a.Content, a.CreatedAt, a.UpdatedAt)
		}
		out = append(out, connector.AnnouncementResult{
			SourceKey:       sourceKey,
			Title:           strings.TrimSpace(a.Title),
			Content:         strings.TrimSpace(a.Content),
			Type:            strings.TrimSpace(a.NotifyMode),
			PublishedAt:     publishedAt,
			SourceUpdatedAt: updatedAt,
		})
	}
	return out, nil
}

func (c *Client) RedeemCode(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, code string) (*connector.RedeemResult, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]string{"code": code}).
		Post(site + "/api/v1/redeem")
	if err != nil {
		return nil, fmt.Errorf("sub2api redeem http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api redeem: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("sub2api redeem decode: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, fmt.Errorf("sub2api redeem: %s", wrapped.Message)
	}

	var raw struct {
		Message        string   `json:"message"`
		Type           string   `json:"type"`
		Value          float64  `json:"value"`
		NewBalance     *float64 `json:"new_balance"`
		NewConcurrency *int     `json:"new_concurrency"`
		GroupName      *string  `json:"group_name"`
		ValidityDays   *int     `json:"validity_days"`
		Group          *struct {
			Name string `json:"name"`
		} `json:"group"`
	}
	if err := json.Unmarshal(wrapped.Data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api redeem data: %w", err)
	}

	res := &connector.RedeemResult{
		Message:        raw.Message,
		Type:           raw.Type,
		Value:          raw.Value,
		NewBalance:     raw.NewBalance,
		NewConcurrency: raw.NewConcurrency,
		GroupName:      raw.GroupName,
		ValidityDays:   raw.ValidityDays,
	}
	if res.GroupName == nil && raw.Group != nil && raw.Group.Name != "" {
		name := raw.Group.Name
		res.GroupName = &name
	}
	if res.Type == "" {
		if raw.Group != nil && raw.ValidityDays != nil {
			res.Type = "subscription"
		} else if raw.NewConcurrency != nil {
			res.Type = "concurrency"
		} else {
			res.Type = "balance"
		}
	}
	if res.Message == "" {
		res.Message = "兑换成功"
	}
	if res.Type == "balance" {
		multiplier := c.rechargeMultiplier(ctx, ch, session)
		res.Value = connector.ApplyRechargeMultiplier(res.Value, multiplier, ch.RechargeMultiplierMode)
		if res.NewBalance != nil {
			balance := connector.ApplyRechargeMultiplier(*res.NewBalance, multiplier, ch.RechargeMultiplierMode)
			res.NewBalance = &balance
		}
	}
	return res, nil
}

func (c *Client) GetRechargeInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.RechargeInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api checkout info: %w", err)
	}
	var raw struct {
		Methods           map[string]sub2MethodLimit `json:"methods"`
		GlobalMin         float64                    `json:"global_min"`
		GlobalMax         float64                    `json:"global_max"`
		HelpText          string                     `json:"help_text"`
		HelpImageURL      string                     `json:"help_image_url"`
		AlipayForceQRCode bool                       `json:"alipay_force_qrcode"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api checkout info decode: %w", err)
	}
	methods := make([]connector.RechargeMethod, 0, 2)
	addMethod := func(visibleType, name string) {
		best, ok := pickVisibleMethodLimit(raw.Methods, visibleType)
		if !ok || (best.Available != nil && !*best.Available) {
			return
		}
		methods = append(methods, connector.RechargeMethod{
			Type:      visibleType,
			Name:      name,
			MinAmount: best.SingleMin,
			MaxAmount: best.SingleMax,
		})
	}
	addMethod("alipay", "支付宝")
	addMethod("wxpay", "微信支付")
	if len(methods) == 0 {
		return nil, errors.New("上游未配置可用的支付宝/微信支付方式")
	}
	return &connector.RechargeInfo{
		AmountLabel:       "充值金额",
		AmountStep:        0.01,
		MinAmount:         raw.GlobalMin,
		MaxAmount:         raw.GlobalMax,
		PresetAmounts:     []float64{},
		HelpText:          raw.HelpText,
		HelpImageURL:      raw.HelpImageURL,
		AlipayForceQRCode: raw.AlipayForceQRCode,
		Methods:           methods,
	}, nil
}

func (c *Client) GetSubscriptionInfo(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/payment/checkout-info", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api checkout info: %w", err)
	}
	var raw struct {
		Methods map[string]sub2MethodLimit     `json:"methods"`
		Plans   []sub2SubscriptionCheckoutPlan `json:"plans"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api checkout info decode: %w", err)
	}

	methods := make([]connector.SubscriptionMethod, 0, 2)
	availableTypes := map[string]bool{}
	addMethod := func(visibleType, name string) {
		best, ok := pickVisibleMethodLimit(raw.Methods, visibleType)
		if !ok || (best.Available != nil && !*best.Available) {
			return
		}
		availableTypes[visibleType] = true
		methods = append(methods, connector.SubscriptionMethod{
			Type: visibleType,
			Name: name,
		})
	}
	addMethod("alipay", "支付宝")
	addMethod("wxpay", "微信支付")

	plans := make([]connector.SubscriptionPlan, 0, len(raw.Plans))
	for _, plan := range raw.Plans {
		if plan.ID <= 0 {
			continue
		}
		paymentMethods := make([]string, 0, len(methods))
		for _, method := range methods {
			if availableTypes[method.Type] {
				paymentMethods = append(paymentMethods, method.Type)
			}
		}
		plans = append(plans, connector.SubscriptionPlan{
			ID:              strconv.FormatInt(plan.ID, 10),
			Name:            strings.TrimSpace(plan.Name),
			Description:     strings.TrimSpace(plan.Description),
			Price:           plan.Price,
			Currency:        "CNY",
			Validity:        formatSub2SubscriptionValidity(plan.ValidityDays, plan.ValidityUnit),
			GroupName:       strings.TrimSpace(plan.GroupName),
			DailyLimitUSD:   plan.DailyLimitUSD,
			WeeklyLimitUSD:  plan.WeeklyLimitUSD,
			MonthlyLimitUSD: plan.MonthlyLimitUSD,
			Features:        safeStringSlice(plan.Features),
			PaymentMethods:  paymentMethods,
		})
	}
	return &connector.SubscriptionInfo{Plans: plans, Methods: methods}, nil
}

func (c *Client) CreateRecharge(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.RechargeRequest) (*connector.RechargeLaunch, error) {
	if req.PaymentMethod != "alipay" && req.PaymentMethod != "wxpay" {
		return nil, errors.New("sub2api 仅支持 alipay 或 wxpay")
	}
	if req.Amount <= 0 {
		return nil, errors.New("充值金额必须大于 0")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]any{
			"amount":         round2(req.Amount),
			"payment_type":   req.PaymentMethod,
			"order_type":     "balance",
			"is_mobile":      req.IsMobile,
			"payment_source": "hosted_redirect",
		}).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/payment/orders")
	if err != nil {
		return nil, fmt.Errorf("sub2api create recharge http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create recharge: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	raw, err := decodeSub2OrderPaymentResult(resp.Body(), "sub2api create recharge")
	if err != nil {
		return nil, err
	}
	return raw.toLaunch(req.IsMobile)
}

func (c *Client) CreateSubscription(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.SubscriptionRequest) (*connector.SubscriptionLaunch, error) {
	planID, err := strconv.ParseInt(strings.TrimSpace(req.PlanID), 10, 64)
	if err != nil || planID <= 0 {
		return nil, errors.New("sub2api 订阅套餐 ID 无效")
	}
	method := strings.TrimSpace(req.PaymentMethod)
	if method != "alipay" && method != "wxpay" {
		return nil, errors.New("sub2api 订阅仅支持 alipay 或 wxpay")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(map[string]any{
			"payment_type":   method,
			"order_type":     "subscription",
			"plan_id":        planID,
			"is_mobile":      req.IsMobile,
			"payment_source": "hosted_redirect",
		}).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/payment/orders")
	if err != nil {
		return nil, fmt.Errorf("sub2api create subscription http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create subscription: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	raw, err := decodeSub2OrderPaymentResult(resp.Body(), "sub2api create subscription")
	if err != nil {
		return nil, err
	}
	return raw.toLaunch(req.IsMobile)
}

func (c *Client) GetSubscriptionUsage(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (*connector.SubscriptionUsageInfo, error) {
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/subscriptions/progress", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api subscription progress: %w", err)
	}
	var raw []sub2SubscriptionProgressItem
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api subscription progress decode: %w", err)
	}
	items := make([]connector.SubscriptionUsage, 0, len(raw))
	for _, item := range raw {
		usage := item.toConnector()
		if usage.ID <= 0 {
			continue
		}
		items = append(items, usage)
	}
	return &connector.SubscriptionUsageInfo{Items: items}, nil
}

func (c *Client) ListAPIKeys(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	page, pageSize := normalizeAPIKeyPage(query.Page, query.PageSize)
	params := url.Values{}
	params.Set("page", strconv.Itoa(page))
	params.Set("page_size", strconv.Itoa(pageSize))
	params.Set("sort_by", "created_at")
	params.Set("sort_order", "desc")
	if search := strings.TrimSpace(query.Search); search != "" {
		params.Set("search", search)
	}
	if status := strings.TrimSpace(query.Status); status != "" {
		params.Set("status", status)
	}
	if groupID := strings.TrimSpace(query.GroupID); groupID != "" {
		params.Set("group_id", groupID)
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/keys?"+params.Encode(), session)
	if err != nil {
		return nil, fmt.Errorf("sub2api api keys: %w", err)
	}
	var raw struct {
		Items    []sub2APIKey `json:"items"`
		Total    int64        `json:"total"`
		Page     int          `json:"page"`
		PageSize int          `json:"page_size"`
		Pages    int          `json:"pages"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api api keys decode: %w", err)
	}
	groups, _ := c.sub2APIGroupMap(ctx, ch, session)
	items := make([]connector.APIKey, 0, len(raw.Items))
	for _, item := range raw.Items {
		key := item.toConnector()
		if key.GroupID != nil {
			if g, ok := groups[*key.GroupID]; ok {
				key.GroupName = g.Name
				key.GroupDescription = g.Description
				key.GroupRatio = g.Ratio
			}
		}
		items = append(items, key)
	}
	if raw.Page <= 0 {
		raw.Page = page
	}
	if raw.PageSize <= 0 {
		raw.PageSize = pageSize
	}
	if raw.Pages <= 0 {
		raw.Pages = pagesFromTotal(raw.Total, raw.PageSize)
	}
	return &connector.APIKeyPage{
		Items:    items,
		Total:    raw.Total,
		Page:     raw.Page,
		PageSize: raw.PageSize,
		Pages:    raw.Pages,
	}, nil
}

func (c *Client) ListAPIKeyGroups(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) ([]connector.APIKeyGroup, error) {
	groups, err := c.sub2APIGroupMap(ctx, ch, session)
	if err != nil {
		return nil, err
	}
	out := make([]connector.APIKeyGroup, 0, len(groups))
	for _, g := range groups {
		out = append(out, g)
	}
	return out, nil
}

func (c *Client) CreateAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	if strings.TrimSpace(req.Name) == "" {
		return nil, errors.New("密钥名称不能为空")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(buildSub2CreateAPIKey(req)).
		Post(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys")
	if err != nil {
		return nil, fmt.Errorf("sub2api create api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api create api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeSub2WriteData(resp.Body(), "sub2api create api key")
	if err != nil {
		return nil, err
	}
	var raw sub2APIKey
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api create api key data: %w", err)
	}
	out := raw.toConnector()
	return &out, nil
}

func (c *Client) UpdateAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	if id <= 0 {
		return nil, errors.New("密钥 ID 无效")
	}
	body, err := buildSub2UpdateAPIKey(req)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		SetBody(body).
		Put(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys/" + strconv.FormatInt(id, 10))
	if err != nil {
		return nil, fmt.Errorf("sub2api update api key http: %w", err)
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sub2api update api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	data, err := decodeSub2WriteData(resp.Body(), "sub2api update api key")
	if err != nil {
		return nil, err
	}
	var raw sub2APIKey
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("sub2api update api key data: %w", err)
	}
	out := raw.toConnector()
	return &out, nil
}

func (c *Client) DeleteAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) error {
	if id <= 0 {
		return errors.New("密钥 ID 无效")
	}
	resp, err := c.http.R().
		SetContext(ctx).
		SetHeader("Authorization", "Bearer "+session.AccessToken).
		Delete(strings.TrimRight(ch.SiteURL, "/") + "/api/v1/keys/" + strconv.FormatInt(id, 10))
	if err != nil {
		return fmt.Errorf("sub2api delete api key http: %w", err)
	}
	if resp.IsError() {
		return fmt.Errorf("sub2api delete api key: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return decodeSub2Write(resp.Body(), "sub2api delete api key")
}

func (c *Client) RevealAPIKey(ctx context.Context, ch *connector.Channel, session *connector.AuthSession, id int64) (string, error) {
	if id <= 0 {
		return "", errors.New("密钥 ID 无效")
	}
	body, err := c.getJSON(ctx, strings.TrimRight(ch.SiteURL, "/")+"/api/v1/keys/"+strconv.FormatInt(id, 10), session)
	if err != nil {
		return "", fmt.Errorf("sub2api reveal api key: %w", err)
	}
	var raw sub2APIKey
	if err := json.Unmarshal(body, &raw); err != nil {
		return "", fmt.Errorf("sub2api reveal api key data: %w", err)
	}
	if strings.TrimSpace(raw.Key) == "" {
		return "", errors.New("sub2api 未返回完整密钥")
	}
	return raw.Key, nil
}

func (c *Client) getJSON(ctx context.Context, url string, session *connector.AuthSession) ([]byte, error) {
	req := c.http.R().SetContext(ctx)
	if session != nil && session.AccessToken != "" {
		req.SetHeader("Authorization", "Bearer "+session.AccessToken)
	}
	resp, err := req.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, connector.HTTPStatusError(resp.StatusCode(), resp.Body())
	}
	var wrapped sub2Resp
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("decode: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, errors.New(wrapped.Message)
	}
	return wrapped.Data, nil
}

type sub2MethodLimit struct {
	PaymentType string  `json:"payment_type"`
	Currency    string  `json:"currency"`
	FeeRate     float64 `json:"fee_rate"`
	DailyLimit  float64 `json:"daily_limit"`
	SingleMin   float64 `json:"single_min"`
	SingleMax   float64 `json:"single_max"`
	Available   *bool   `json:"available"`
}

type sub2SubscriptionCheckoutPlan struct {
	ID              int64    `json:"id"`
	GroupID         int64    `json:"group_id"`
	GroupName       string   `json:"group_name"`
	Name            string   `json:"name"`
	Description     string   `json:"description"`
	Price           float64  `json:"price"`
	OriginalPrice   *float64 `json:"original_price"`
	DailyLimitUSD   *float64 `json:"daily_limit_usd"`
	WeeklyLimitUSD  *float64 `json:"weekly_limit_usd"`
	MonthlyLimitUSD *float64 `json:"monthly_limit_usd"`
	ValidityDays    int      `json:"validity_days"`
	ValidityUnit    string   `json:"validity_unit"`
	Features        []string `json:"features"`
}

type sub2OrderPaymentResult struct {
	PayURL       string          `json:"pay_url"`
	QRCode       string          `json:"qr_code"`
	ExpiresAt    string          `json:"expires_at"`
	ResultType   string          `json:"result_type"`
	PaymentMode  string          `json:"payment_mode"`
	PaymentEnv   string          `json:"payment_env"`
	ClientSecret string          `json:"client_secret"`
	IntentID     string          `json:"intent_id"`
	OAuth        json.RawMessage `json:"oauth"`
	JSAPI        json.RawMessage `json:"jsapi"`
	JSAPIPayload json.RawMessage `json:"jsapi_payload"`
}

type sub2SubscriptionProgressItem struct {
	Subscription sub2UserSubscription   `json:"subscription"`
	Progress     sub2Progress           `json:"progress"`
	ID           int64                  `json:"id"`
	GroupID      int64                  `json:"group_id"`
	Status       string                 `json:"status"`
	StartsAt     *time.Time             `json:"starts_at"`
	ExpiresAt    *time.Time             `json:"expires_at"`
	DailyStart   *time.Time             `json:"daily_window_start"`
	WeeklyStart  *time.Time             `json:"weekly_window_start"`
	MonthlyStart *time.Time             `json:"monthly_window_start"`
	DailyUsage   float64                `json:"daily_usage_usd"`
	WeeklyUsage  float64                `json:"weekly_usage_usd"`
	MonthlyUsage float64                `json:"monthly_usage_usd"`
	Group        *sub2SubscriptionGroup `json:"group"`
}

type sub2UserSubscription struct {
	ID              int64                  `json:"id"`
	GroupID         int64                  `json:"group_id"`
	Status          string                 `json:"status"`
	StartsAt        *time.Time             `json:"starts_at"`
	ExpiresAt       *time.Time             `json:"expires_at"`
	DailyStart      *time.Time             `json:"daily_window_start"`
	WeeklyStart     *time.Time             `json:"weekly_window_start"`
	MonthlyStart    *time.Time             `json:"monthly_window_start"`
	DailyUsageUSD   float64                `json:"daily_usage_usd"`
	WeeklyUsageUSD  float64                `json:"weekly_usage_usd"`
	MonthlyUsageUSD float64                `json:"monthly_usage_usd"`
	Group           *sub2SubscriptionGroup `json:"group"`
}

type sub2SubscriptionGroup struct {
	ID              int64   `json:"id"`
	Name            string  `json:"name"`
	DailyLimitUSD   float64 `json:"daily_limit_usd"`
	WeeklyLimitUSD  float64 `json:"weekly_limit_usd"`
	MonthlyLimitUSD float64 `json:"monthly_limit_usd"`
}

type sub2Progress struct {
	ID            int64            `json:"id"`
	GroupName     string           `json:"group_name"`
	ExpiresAt     *time.Time       `json:"expires_at"`
	ExpiresInDays int              `json:"expires_in_days"`
	Daily         *sub2UsageWindow `json:"daily"`
	Weekly        *sub2UsageWindow `json:"weekly"`
	Monthly       *sub2UsageWindow `json:"monthly"`
}

type sub2UsageWindow struct {
	LimitUSD        float64    `json:"limit_usd"`
	UsedUSD         float64    `json:"used_usd"`
	RemainingUSD    float64    `json:"remaining_usd"`
	Percentage      float64    `json:"percentage"`
	WindowStart     *time.Time `json:"window_start"`
	ResetsAt        *time.Time `json:"resets_at"`
	ResetsInSeconds int64      `json:"resets_in_seconds"`
}

func pickVisibleMethodLimit(methods map[string]sub2MethodLimit, visibleType string) (sub2MethodLimit, bool) {
	var first sub2MethodLimit
	found := false
	for _, key := range []string{visibleType, visibleType + "_direct"} {
		v, ok := methods[key]
		if !ok {
			continue
		}
		if !found {
			first = v
			found = true
		}
		if v.Available == nil || *v.Available {
			return v, true
		}
	}
	return first, found
}

func decodeSub2OrderPaymentResult(body []byte, prefix string) (*sub2OrderPaymentResult, error) {
	data, err := decodeSub2WriteData(body, prefix)
	if err != nil {
		return nil, err
	}
	var raw sub2OrderPaymentResult
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("%s data: %w", prefix, err)
	}
	if raw.ResultType == "oauth_required" || raw.ResultType == "jsapi_ready" || strings.TrimSpace(raw.ClientSecret) != "" || hasJSONValue(raw.OAuth) || hasJSONValue(raw.JSAPI) || hasJSONValue(raw.JSAPIPayload) {
		return nil, errors.New("当前支付方式需要复杂支付流程，暂不支持")
	}
	return &raw, nil
}

func (r sub2OrderPaymentResult) toLaunch(isMobile bool) (*connector.RechargeLaunch, error) {
	var expiresAt *time.Time
	if r.ExpiresAt != "" {
		if parsed, err := time.Parse(time.RFC3339, r.ExpiresAt); err == nil {
			expiresAt = &parsed
		}
	}
	payURL := strings.TrimSpace(r.PayURL)
	qrCode := strings.TrimSpace(r.QRCode)
	switch strings.ToLower(strings.TrimSpace(r.PaymentMode)) {
	case "redirect", "popup":
		if payURL != "" {
			return &connector.RechargeLaunch{
				Mode:      "redirect",
				PayURL:    payURL,
				ExpiresAt: expiresAt,
			}, nil
		}
	case "qrcode", "native":
		if qrCode != "" {
			return &connector.RechargeLaunch{
				Mode:      "qrcode",
				QRCode:    qrCode,
				ExpiresAt: expiresAt,
			}, nil
		}
	}
	if qrCode != "" && !isMobile {
		return &connector.RechargeLaunch{
			Mode:      "qrcode",
			QRCode:    qrCode,
			ExpiresAt: expiresAt,
		}, nil
	}
	if payURL != "" {
		return &connector.RechargeLaunch{
			Mode:      "redirect",
			PayURL:    payURL,
			ExpiresAt: expiresAt,
		}, nil
	}
	if qrCode != "" {
		return &connector.RechargeLaunch{
			Mode:      "qrcode",
			QRCode:    qrCode,
			ExpiresAt: expiresAt,
		}, nil
	}
	return nil, errors.New("sub2api 未返回可用的支付二维码或跳转地址")
}

func (p sub2SubscriptionProgressItem) toConnector() connector.SubscriptionUsage {
	if p.Subscription.ID == 0 && p.ID > 0 {
		return p.directSubscriptionToConnector()
	}
	sub := p.Subscription
	progress := p.Progress
	groupID := sub.GroupID
	if groupID == 0 && sub.Group != nil {
		groupID = sub.Group.ID
	}
	groupName := strings.TrimSpace(progress.GroupName)
	if groupName == "" && sub.Group != nil {
		groupName = strings.TrimSpace(sub.Group.Name)
	}
	expiresAt := progress.ExpiresAt
	if expiresAt == nil {
		expiresAt = sub.ExpiresAt
	}
	daily := sub2WindowToConnector(progress.Daily)
	weekly := sub2WindowToConnector(progress.Weekly)
	monthly := sub2WindowToConnector(progress.Monthly)
	if sub.Group != nil {
		if daily == nil {
			daily = directSubscriptionWindow(sub.Group.DailyLimitUSD, sub.DailyUsageUSD, sub.DailyStart, nil)
		}
		if weekly == nil {
			weekly = directSubscriptionWindow(sub.Group.WeeklyLimitUSD, sub.WeeklyUsageUSD, sub.WeeklyStart, nil)
		}
		if monthly == nil {
			monthly = directSubscriptionWindow(sub.Group.MonthlyLimitUSD, sub.MonthlyUsageUSD, sub.MonthlyStart, nil)
		}
	}
	return connector.SubscriptionUsage{
		ID:            firstPositiveInt64(progress.ID, sub.ID),
		GroupID:       groupID,
		GroupName:     groupName,
		Status:        strings.TrimSpace(sub.Status),
		StartsAt:      sub.StartsAt,
		ExpiresAt:     expiresAt,
		ExpiresInDays: progress.ExpiresInDays,
		Daily:         daily,
		Weekly:        weekly,
		Monthly:       monthly,
	}
}

func (p sub2SubscriptionProgressItem) directSubscriptionToConnector() connector.SubscriptionUsage {
	groupID := p.GroupID
	groupName := ""
	if p.Group != nil {
		if groupID == 0 {
			groupID = p.Group.ID
		}
		groupName = strings.TrimSpace(p.Group.Name)
	}
	var daily, weekly, monthly *connector.SubscriptionUsageWindow
	if p.Group != nil {
		daily = directSubscriptionWindow(p.Group.DailyLimitUSD, p.DailyUsage, p.DailyStart, nil)
		weekly = directSubscriptionWindow(p.Group.WeeklyLimitUSD, p.WeeklyUsage, p.WeeklyStart, nil)
		monthly = directSubscriptionWindow(p.Group.MonthlyLimitUSD, p.MonthlyUsage, p.MonthlyStart, nil)
	}
	return connector.SubscriptionUsage{
		ID:        p.ID,
		GroupID:   groupID,
		GroupName: groupName,
		Status:    strings.TrimSpace(p.Status),
		StartsAt:  p.StartsAt,
		ExpiresAt: p.ExpiresAt,
		Daily:     daily,
		Weekly:    weekly,
		Monthly:   monthly,
	}
}

func directSubscriptionWindow(limit, used float64, windowStart, resetsAt *time.Time) *connector.SubscriptionUsageWindow {
	if limit <= 0 {
		return nil
	}
	if used < 0 {
		used = 0
	}
	remaining := limit - used
	if remaining < 0 {
		remaining = 0
	}
	usedPct := 0.0
	if limit > 0 {
		usedPct = used / limit * 100
	}
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}
	return &connector.SubscriptionUsageWindow{
		LimitUSD:         limit,
		UsedUSD:          used,
		RemainingUSD:     remaining,
		RemainingPercent: 100 - usedPct,
		UsedPercent:      usedPct,
		WindowStart:      windowStart,
		ResetsAt:         resetsAt,
	}
}

func sub2WindowToConnector(w *sub2UsageWindow) *connector.SubscriptionUsageWindow {
	if w == nil || w.LimitUSD <= 0 {
		return nil
	}
	remaining := w.RemainingUSD
	if remaining < 0 {
		remaining = 0
	}
	usedPct := w.Percentage
	if usedPct < 0 {
		usedPct = 0
	}
	if usedPct > 100 {
		usedPct = 100
	}
	remainingPct := 100 - usedPct
	if remainingPct < 0 {
		remainingPct = 0
	}
	if remainingPct > 100 {
		remainingPct = 100
	}
	return &connector.SubscriptionUsageWindow{
		LimitUSD:         w.LimitUSD,
		UsedUSD:          w.UsedUSD,
		RemainingUSD:     remaining,
		RemainingPercent: remainingPct,
		UsedPercent:      usedPct,
		WindowStart:      w.WindowStart,
		ResetsAt:         w.ResetsAt,
		ResetsInSeconds:  w.ResetsInSeconds,
	}
}

func firstPositiveInt64(values ...int64) int64 {
	for _, v := range values {
		if v > 0 {
			return v
		}
	}
	return 0
}

func hasJSONValue(raw json.RawMessage) bool {
	s := strings.TrimSpace(string(raw))
	return s != "" && s != "null" && s != "{}"
}

func formatSub2SubscriptionValidity(days int, unit string) string {
	unit = strings.TrimSpace(unit)
	if days <= 0 {
		return unit
	}
	if unit == "" || unit == "day" || unit == "days" {
		return fmt.Sprintf("%d 天", days)
	}
	return fmt.Sprintf("%d %s", days, unit)
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

type sub2APIKey struct {
	ID      int64  `json:"id"`
	Key     string `json:"key"`
	Name    string `json:"name"`
	GroupID *int64 `json:"group_id"`
	Group   *struct {
		ID             int64   `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	} `json:"group"`
	Status      string     `json:"status"`
	IPWhitelist []string   `json:"ip_whitelist"`
	IPBlacklist []string   `json:"ip_blacklist"`
	LastUsedAt  *time.Time `json:"last_used_at"`
	Quota       float64    `json:"quota"`
	QuotaUsed   float64    `json:"quota_used"`
	ExpiresAt   *time.Time `json:"expires_at"`
	CreatedAt   *time.Time `json:"created_at"`
	UpdatedAt   *time.Time `json:"updated_at"`
	RateLimit5h float64    `json:"rate_limit_5h"`
	RateLimit1d float64    `json:"rate_limit_1d"`
	RateLimit7d float64    `json:"rate_limit_7d"`
	Usage5h     float64    `json:"usage_5h"`
	Usage1d     float64    `json:"usage_1d"`
	Usage7d     float64    `json:"usage_7d"`
}

func (k sub2APIKey) toConnector() connector.APIKey {
	out := connector.APIKey{
		ID:          k.ID,
		Key:         k.Key,
		Name:        k.Name,
		Status:      normalizeSub2APIKeyStatus(k.Status),
		GroupID:     k.GroupID,
		Quota:       k.Quota,
		QuotaUsed:   k.QuotaUsed,
		ExpiresAt:   k.ExpiresAt,
		CreatedAt:   k.CreatedAt,
		UpdatedAt:   k.UpdatedAt,
		LastUsedAt:  k.LastUsedAt,
		IPWhitelist: safeStringSlice(k.IPWhitelist),
		IPBlacklist: safeStringSlice(k.IPBlacklist),
		RateLimit5h: k.RateLimit5h,
		RateLimit1d: k.RateLimit1d,
		RateLimit7d: k.RateLimit7d,
		Usage5h:     k.Usage5h,
		Usage1d:     k.Usage1d,
		Usage7d:     k.Usage7d,
	}
	if k.Group != nil {
		id := k.Group.ID
		if out.GroupID == nil {
			out.GroupID = &id
		}
		out.GroupName = k.Group.Name
		out.GroupDescription = k.Group.Description
		out.GroupRatio = k.Group.RateMultiplier
	}
	return out
}

func buildSub2CreateAPIKey(req connector.APIKeyCreateRequest) map[string]any {
	body := map[string]any{
		"name":         strings.TrimSpace(req.Name),
		"ip_whitelist": safeStringSlice(req.IPWhitelist),
		"ip_blacklist": safeStringSlice(req.IPBlacklist),
	}
	if req.GroupID != nil {
		body["group_id"] = *req.GroupID
	}
	if strings.TrimSpace(req.CustomKey) != "" {
		body["custom_key"] = strings.TrimSpace(req.CustomKey)
	}
	if req.Quota != nil {
		body["quota"] = *req.Quota
	}
	if req.ExpiresInDays != nil {
		body["expires_in_days"] = *req.ExpiresInDays
	}
	if req.RateLimit5h != nil {
		body["rate_limit_5h"] = *req.RateLimit5h
	}
	if req.RateLimit1d != nil {
		body["rate_limit_1d"] = *req.RateLimit1d
	}
	if req.RateLimit7d != nil {
		body["rate_limit_7d"] = *req.RateLimit7d
	}
	return body
}

func buildSub2UpdateAPIKey(req connector.APIKeyUpdateRequest) (map[string]any, error) {
	body := map[string]any{}
	if req.Name != nil {
		body["name"] = strings.TrimSpace(*req.Name)
	}
	if req.GroupID != nil {
		body["group_id"] = *req.GroupID
	}
	if req.Status != nil {
		status := normalizeSub2APIKeyStatus(*req.Status)
		if status != "active" && status != "disabled" {
			return nil, errors.New("sub2api 编辑状态仅支持 active 或 disabled")
		}
		body["status"] = sub2APIKeyStatusForUpdate(status)
	}
	if req.Quota != nil {
		body["quota"] = *req.Quota
	}
	if req.ExpiresAt != nil {
		body["expires_at"] = strings.TrimSpace(*req.ExpiresAt)
	}
	if req.IPWhitelist != nil {
		body["ip_whitelist"] = safeStringSlice(req.IPWhitelist)
	}
	if req.IPBlacklist != nil {
		body["ip_blacklist"] = safeStringSlice(req.IPBlacklist)
	}
	if req.ResetQuota != nil {
		body["reset_quota"] = *req.ResetQuota
	}
	if req.RateLimit5h != nil {
		body["rate_limit_5h"] = *req.RateLimit5h
	}
	if req.RateLimit1d != nil {
		body["rate_limit_1d"] = *req.RateLimit1d
	}
	if req.RateLimit7d != nil {
		body["rate_limit_7d"] = *req.RateLimit7d
	}
	if req.ResetRateLimitUsage != nil {
		body["reset_rate_limit_usage"] = *req.ResetRateLimitUsage
	}
	return body, nil
}

func decodeSub2Write(body []byte, prefix string) error {
	_, err := decodeSub2WriteData(body, prefix)
	return err
}

func decodeSub2WriteData(body []byte, prefix string) (json.RawMessage, error) {
	var wrapped sub2Resp
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("%s decode: %w", prefix, err)
	}
	if wrapped.Code != 0 {
		msg := strings.TrimSpace(wrapped.Message)
		if msg == "" {
			msg = prefix + " failed"
		}
		return nil, errors.New(msg)
	}
	return wrapped.Data, nil
}

func normalizeSub2APIKeyStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "inactive":
		return "disabled"
	case "active", "disabled", "quota_exhausted", "expired":
		return strings.TrimSpace(status)
	default:
		return strings.TrimSpace(status)
	}
}

func sub2APIKeyStatusForUpdate(status string) string {
	if status == "disabled" {
		return "inactive"
	}
	return status
}

func normalizeAPIKeyPage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

func pagesFromTotal(total int64, pageSize int) int {
	if pageSize <= 0 {
		pageSize = 20
	}
	pages := int(math.Ceil(float64(total) / float64(pageSize)))
	if pages < 1 {
		return 1
	}
	return pages
}

func safeStringSlice(in []string) []string {
	if in == nil {
		return []string{}
	}
	return in
}

func parseAnnouncementTime(values ...string) *time.Time {
	for _, raw := range values {
		raw = strings.TrimSpace(raw)
		if raw == "" || raw == "0" {
			continue
		}
		if t, ok := parseFlexibleTime(raw); ok {
			return &t
		}
	}
	return nil
}

func parseFlexibleTime(raw string) (time.Time, bool) {
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, raw); err == nil {
			return t, true
		}
	}
	if n, err := strconv.ParseInt(raw, 10, 64); err == nil {
		switch {
		case n > 1e12:
			return time.UnixMilli(n), true
		case n > 1e9:
			return time.Unix(n, 0), true
		}
	}
	return time.Time{}, false
}

func hashAnnouncementKey(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(strings.TrimSpace(p)))
		h.Write([]byte{0})
	}
	return "hash:" + hex.EncodeToString(h.Sum(nil))
}

func (c *Client) sub2APIGroupMap(ctx context.Context, ch *connector.Channel, session *connector.AuthSession) (map[int64]connector.APIKeyGroup, error) {
	site := strings.TrimRight(ch.SiteURL, "/")
	body, err := c.getJSON(ctx, site+"/api/v1/groups/available", session)
	if err != nil {
		return nil, fmt.Errorf("sub2api api key groups: %w", err)
	}
	var raw []struct {
		ID             int64   `json:"id"`
		Name           string  `json:"name"`
		Description    string  `json:"description"`
		RateMultiplier float64 `json:"rate_multiplier"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("sub2api api key groups decode: %w", err)
	}
	overrides := map[string]float64{}
	if ratesBody, err := c.getJSON(ctx, site+"/api/v1/groups/rates", session); err == nil {
		_ = json.Unmarshal(ratesBody, &overrides)
	}
	out := make(map[int64]connector.APIKeyGroup, len(raw))
	for _, item := range raw {
		id := item.ID
		ratio := item.RateMultiplier
		if v, ok := overrides[strconv.FormatInt(id, 10)]; ok {
			ratio = v
		}
		out[id] = connector.APIKeyGroup{
			ID:          &id,
			Name:        item.Name,
			Description: item.Description,
			Ratio:       ratio,
		}
	}
	return out, nil
}
