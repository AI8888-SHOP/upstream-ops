package sub2api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/connector"
)

// AdminClient 只封装 Sub2API 管理员接口，使用 x-api-key 鉴权。
type AdminClient struct {
	client *Client
}

func NewAdminClient() *AdminClient {
	return &AdminClient{client: New()}
}

// SetAPIAudit forwards audit configuration to the underlying connector.
func (a *AdminClient) SetAPIAudit(opts apiaudit.Options) {
	if a != nil && a.client != nil {
		a.client.SetAPIAudit(opts)
	}
}

type AdminTarget struct {
	BaseURL string
	APIKey  string
}

type AdminGroup struct {
	ID             int64   `json:"id"`
	Name           string  `json:"name"`
	Platform       string  `json:"platform"`
	Ratio          float64 `json:"ratio"`
	RateMultiplier float64 `json:"rate_multiplier"`
	Status         string  `json:"status"`
	Sort           int     `json:"sort"`
	Description    string  `json:"description"`
}

type AdminProxy struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username,omitempty"`
	Status   string `json:"status"`
}

type AdminAccount struct {
	ID             int64          `json:"id"`
	Name           string         `json:"name"`
	Platform       string         `json:"platform"`
	Type           string         `json:"type"`
	Status         string         `json:"status"`
	Schedulable    bool           `json:"schedulable,omitempty"`
	Notes          string         `json:"notes"`
	ProxyID        *int64         `json:"proxy_id,omitempty"`
	Concurrency    int            `json:"concurrency"`
	Priority       int            `json:"priority"`
	RateMultiplier float64        `json:"rate_multiplier"`
	LoadFactor     float64        `json:"load_factor"`
	GroupIDs       []int64        `json:"group_ids"`
	Credentials    map[string]any `json:"credentials"`
}

type AdminAccountTestResult struct {
	Model        string
	ResponseText string
}

func (a *AdminClient) Ping(ctx context.Context, t AdminTarget) error {
	if _, err := a.ListGroups(ctx, t, true); err != nil {
		return err
	}
	_, err := a.ListAccounts(ctx, t, 1, 1)
	return err
}

func (a *AdminClient) ListGroups(ctx context.Context, t AdminTarget, includeInactive bool) ([]AdminGroup, error) {
	params := url.Values{}
	params.Set("include_inactive", strconv.FormatBool(includeInactive))
	body, err := a.getJSON(ctx, t, "/api/v1/admin/groups/all?"+params.Encode())
	if err != nil {
		return nil, err
	}
	var list []AdminGroup
	if err := json.Unmarshal(body, &list); err == nil {
		normalizeAdminGroupRatios(list)
		return list, nil
	}
	var wrapped struct {
		Items []AdminGroup `json:"items"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("decode admin groups: %w", err)
	}
	normalizeAdminGroupRatios(wrapped.Items)
	return wrapped.Items, nil
}

func (a *AdminClient) ListProxies(ctx context.Context, t AdminTarget) ([]AdminProxy, error) {
	body, err := a.getJSON(ctx, t, "/api/v1/admin/proxies/all")
	if err != nil {
		return nil, err
	}
	var list []AdminProxy
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, fmt.Errorf("decode admin proxies: %w", err)
	}
	return list, nil
}

func (a *AdminClient) ListAccounts(ctx context.Context, t AdminTarget, page, pageSize int) ([]AdminAccount, error) {
	body, err := a.getJSON(ctx, t, "/api/v1/admin/accounts?page="+strconv.Itoa(page)+"&page_size="+strconv.Itoa(pageSize))
	if err != nil {
		return nil, err
	}
	var raw struct {
		Items []AdminAccount `json:"items"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode admin accounts: %w", err)
	}
	return raw.Items, nil
}

func (a *AdminClient) FindGroupByName(ctx context.Context, t AdminTarget, name string) (*AdminGroup, error) {
	groups, err := a.ListGroups(ctx, t, true)
	if err != nil {
		return nil, err
	}
	for i := range groups {
		if strings.EqualFold(groups[i].Name, name) {
			return &groups[i], nil
		}
	}
	return nil, nil
}

func (a *AdminClient) FindAccountByName(ctx context.Context, t AdminTarget, name string) (*AdminAccount, error) {
	items, err := a.ListAccounts(ctx, t, 1, 100)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if strings.EqualFold(items[i].Name, name) {
			return &items[i], nil
		}
	}
	return nil, nil
}

func (a *AdminClient) CreateAccount(ctx context.Context, t AdminTarget, req AdminAccount) (*AdminAccount, error) {
	req.Status = adminAccountStatusForUpdate(req.Status)
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("x-api-key", t.APIKey).
		SetBody(body).
		Post(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("create admin account: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	out, err := decodeAdminAccount(resp.Body())
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (a *AdminClient) UpdateAccount(ctx context.Context, t AdminTarget, id int64, req AdminAccount) (*AdminAccount, error) {
	req.Status = adminAccountStatusForUpdate(req.Status)
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("x-api-key", t.APIKey).
		SetBody(body).
		Put(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts/" + strconv.FormatInt(id, 10))
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("update admin account: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	out, err := decodeAdminAccount(resp.Body())
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (a *AdminClient) SetAccountSchedulable(ctx context.Context, t AdminTarget, id int64, schedulable bool) (*AdminAccount, error) {
	body, err := json.Marshal(map[string]bool{"schedulable": schedulable})
	if err != nil {
		return nil, err
	}
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("x-api-key", t.APIKey).
		SetBody(body).
		Post(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts/" + strconv.FormatInt(id, 10) + "/schedulable")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("set admin account schedulable: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	out, err := decodeAdminAccount(resp.Body())
	if err != nil {
		return nil, err
	}
	return out, nil
}

func adminAccountStatusForUpdate(status string) string {
	if strings.TrimSpace(status) == "disabled" {
		return "inactive"
	}
	return status
}

func (a *AdminClient) DeleteAccount(ctx context.Context, t AdminTarget, id int64) error {
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("x-api-key", t.APIKey).
		Delete(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts/" + strconv.FormatInt(id, 10))
	if err != nil {
		return err
	}
	if resp.IsError() {
		return fmt.Errorf("delete admin account: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return nil
}

func (a *AdminClient) SyncAccountModelsFromUpstream(ctx context.Context, t AdminTarget, id int64) ([]string, error) {
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("x-api-key", t.APIKey).
		Post(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts/" + strconv.FormatInt(id, 10) + "/models/sync-upstream")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("sync account models from upstream: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return decodeAdminModels(resp.Body())
}

func (a *AdminClient) ListAccountModels(ctx context.Context, t AdminTarget, id int64) ([]string, error) {
	body, err := a.getJSON(ctx, t, "/api/v1/admin/accounts/"+strconv.FormatInt(id, 10)+"/models")
	if err != nil {
		return nil, err
	}
	return decodeAdminModelList(body)
}

func (a *AdminClient) TestAccount(ctx context.Context, t AdminTarget, id int64, modelID string) (*AdminAccountTestResult, error) {
	payload := map[string]string{}
	if strings.TrimSpace(modelID) != "" {
		payload["model_id"] = strings.TrimSpace(modelID)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("Content-Type", "application/json").
		SetHeader("Accept", "text/event-stream").
		SetHeader("x-api-key", t.APIKey).
		SetBody(body).
		Post(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/accounts/" + strconv.FormatInt(id, 10) + "/test")
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, fmt.Errorf("test admin account: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return decodeAdminAccountTest(resp.Body())
}

func decodeAdminAccountTest(body []byte) (*AdminAccountTestResult, error) {
	var result AdminAccountTestResult
	var completed bool
	var texts []string
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		raw := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var event struct {
			Type    string `json:"type"`
			Text    string `json:"text"`
			Model   string `json:"model"`
			Success *bool  `json:"success"`
			Error   string `json:"error"`
		}
		if err := json.Unmarshal([]byte(raw), &event); err != nil {
			continue
		}
		switch event.Type {
		case "test_start":
			result.Model = strings.TrimSpace(event.Model)
		case "content", "status":
			if event.Text != "" {
				texts = append(texts, event.Text)
			}
		case "error":
			if strings.TrimSpace(event.Error) != "" {
				result.ResponseText = strings.Join(texts, "")
				return &result, errors.New(strings.TrimSpace(event.Error))
			}
		case "test_complete":
			if event.Success != nil && !*event.Success {
				result.ResponseText = strings.Join(texts, "")
				return &result, errors.New("test failed")
			}
			completed = true
		}
	}
	result.ResponseText = strings.Join(texts, "")
	if !completed {
		return &result, errors.New("test did not complete")
	}
	return &result, nil
}

func decodeAdminModels(body []byte) ([]string, error) {
	var wrapped struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err != nil {
		return nil, fmt.Errorf("decode admin models response: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, errors.New(strings.TrimSpace(wrapped.Message))
	}
	if len(wrapped.Data) == 0 || string(wrapped.Data) == "null" {
		return []string{}, nil
	}
	var object struct {
		Models json.RawMessage `json:"models"`
	}
	if err := json.Unmarshal(wrapped.Data, &object); err == nil && len(object.Models) > 0 {
		return decodeAdminModelList(object.Models)
	}
	return decodeAdminModelList(wrapped.Data)
}

func decodeAdminModelList(raw json.RawMessage) ([]string, error) {
	var stringsList []string
	if err := json.Unmarshal(raw, &stringsList); err == nil {
		return compactUniqueStrings(stringsList), nil
	}
	var objects []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(raw, &objects); err != nil {
		return nil, fmt.Errorf("decode admin models: %w", err)
	}
	out := make([]string, 0, len(objects))
	for _, item := range objects {
		if strings.TrimSpace(item.ID) != "" {
			out = append(out, item.ID)
			continue
		}
		out = append(out, item.Name)
	}
	return compactUniqueStrings(out), nil
}

func compactUniqueStrings(list []string) []string {
	out := make([]string, 0, len(list))
	seen := map[string]struct{}{}
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func (a *AdminClient) DeleteGroup(ctx context.Context, t AdminTarget, id int64) error {
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("x-api-key", t.APIKey).
		Delete(strings.TrimRight(t.BaseURL, "/") + "/api/v1/admin/groups/" + strconv.FormatInt(id, 10))
	if err != nil {
		return err
	}
	if resp.IsError() {
		return fmt.Errorf("delete admin group: %w", connector.HTTPStatusError(resp.StatusCode(), resp.Body()))
	}
	return nil
}

func (a *AdminClient) getJSON(ctx context.Context, t AdminTarget, path string) ([]byte, error) {
	resp, err := a.client.http.R().
		SetContext(ctx).
		SetHeader("x-api-key", t.APIKey).
		Get(strings.TrimRight(t.BaseURL, "/") + path)
	if err != nil {
		return nil, err
	}
	if resp.IsError() {
		return nil, connector.HTTPStatusError(resp.StatusCode(), resp.Body())
	}
	var wrapped struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(resp.Body(), &wrapped); err != nil {
		return nil, fmt.Errorf("decode admin response: %w", err)
	}
	if wrapped.Code != 0 {
		return nil, errors.New(strings.TrimSpace(wrapped.Message))
	}
	return wrapped.Data, nil
}

func decodeAdminAccount(body []byte) (*AdminAccount, error) {
	var wrapped struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &wrapped); err == nil && wrapped.Data != nil {
		if wrapped.Code != 0 {
			return nil, errors.New(strings.TrimSpace(wrapped.Message))
		}
		var out AdminAccount
		if err := json.Unmarshal(wrapped.Data, &out); err != nil {
			return nil, err
		}
		return &out, nil
	}
	var out AdminAccount
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func normalizeAdminGroupRatios(groups []AdminGroup) {
	for i := range groups {
		if groups[i].Ratio == 0 && groups[i].RateMultiplier != 0 {
			groups[i].Ratio = groups[i].RateMultiplier
		}
	}
}
