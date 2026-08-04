// 直连渠道模型策略、模型列表规范化与目标感知的模型拉取。
package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

// NormalizeProviderModelPolicy 规范化直连渠道模型策略。空值兼容旧数据，视为 all。
func NormalizeProviderModelPolicy(raw string) (string, error) {
	policy := strings.ToLower(strings.TrimSpace(raw))
	if policy == "" {
		return storage.GatewayProviderModelPolicyAll, nil
	}
	switch policy {
	case storage.GatewayProviderModelPolicyAll, storage.GatewayProviderModelPolicyAllowlist:
		return policy, nil
	default:
		return "", fmt.Errorf("model_policy must be %q or %q",
			storage.GatewayProviderModelPolicyAll,
			storage.GatewayProviderModelPolicyAllowlist)
	}
}

// ParseProviderAllowedModelsJSON 严格解析 JSON 字符串数组，并 trim、去空、去重。
// 空数据库值兼容为 []；非数组、非字符串元素和无效 JSON 均返回错误。
func ParseProviderAllowedModelsJSON(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{}, nil
	}
	if !strings.HasPrefix(raw, "[") {
		return nil, errors.New("allowed_models_json must be a JSON string array")
	}
	var values []string
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		return nil, fmt.Errorf("allowed_models_json must be a JSON string array: %w", err)
	}
	return normalizeProviderModelIDs(values), nil
}

// NormalizeProviderAllowedModelsJSON 返回可稳定落库的紧凑 JSON。
func NormalizeProviderAllowedModelsJSON(raw string) (string, error) {
	values, err := ParseProviderAllowedModelsJSON(raw)
	if err != nil {
		return "", err
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func normalizeProviderModelIDs(values []string) []string {
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func normalizeProviderConcurrencyLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	if limit > 4096 {
		return 4096
	}
	return limit
}

// ProviderAllowsUpstreamModel 判断最终上游模型是否符合直连渠道策略。
// allowlist 使用大小写敏感的精确模型 ID；空 allowlist 明确拒绝全部模型。
func ProviderAllowsUpstreamModel(provider *storage.GatewayProvider, upstreamModel string) (bool, error) {
	if provider == nil {
		return false, errors.New("provider is nil")
	}
	policy, err := NormalizeProviderModelPolicy(provider.ModelPolicy)
	if err != nil {
		return false, err
	}
	if policy == storage.GatewayProviderModelPolicyAll {
		return true, nil
	}
	allowed, err := ParseProviderAllowedModelsJSON(provider.AllowedModelsJSON)
	if err != nil {
		return false, err
	}
	want := strings.TrimSpace(upstreamModel)
	if want == "" {
		return false, nil
	}
	for _, model := range allowed {
		if model == want {
			return true, nil
		}
	}
	return false, nil
}

// FilterProviderModels 将上游 /v1/models 结果按渠道策略过滤并稳定去重。
func FilterProviderModels(provider *storage.GatewayProvider, models []string) ([]string, error) {
	if provider == nil {
		return nil, errors.New("provider is nil")
	}
	models = normalizeProviderModelIDs(models)
	policy, err := NormalizeProviderModelPolicy(provider.ModelPolicy)
	if err != nil {
		return nil, err
	}
	if policy == storage.GatewayProviderModelPolicyAll {
		return models, nil
	}
	allowed, err := ParseProviderAllowedModelsJSON(provider.AllowedModelsJSON)
	if err != nil {
		return nil, err
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, model := range allowed {
		allowedSet[model] = struct{}{}
	}
	out := make([]string, 0, len(models))
	for _, model := range models {
		if _, ok := allowedSet[model]; ok {
			out = append(out, model)
		}
	}
	return out, nil
}

// fetchProviderModels 拉取直连渠道实时模型。HTTP 客户端、鉴权、额外 Header 和代理
// 均使用 GatewayProvider 配置，避免把 provider 降级成伪 Channel 后丢失策略。
func (s *Service) fetchProviderModels(
	ctx context.Context,
	provider *storage.GatewayProvider,
	apiKey string,
	userAgent string,
) ([]string, error) {
	if provider == nil {
		return nil, errors.New("provider is nil")
	}
	release, err := s.runtime().acquireUpstreamConcurrency(ctx, &upstreamTarget{Provider: provider})
	if err != nil {
		return nil, err
	}
	defer release()
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return nil, errors.New("provider api key is empty")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(provider.BaseURL), "/")
	if baseURL == "" {
		return nil, errors.New("provider base_url is empty")
	}
	url := baseURL + "/v1/models"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build GET %s: %w", url, err)
	}
	req.Header.Set("Accept", "application/json")
	switch s.normalizeProviderAuthStyle(provider.AuthStyle) {
	case storage.GatewayProviderAuthBearer:
		req.Header.Set("Authorization", "Bearer "+apiKey)
	case storage.GatewayProviderAuthXAPIKey:
		req.Header.Set("x-api-key", apiKey)
	default:
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("x-api-key", apiKey)
	}
	if strings.TrimSpace(provider.ExtraHeadersJSON) != "" {
		var extra map[string]string
		if json.Unmarshal([]byte(provider.ExtraHeadersJSON), &extra) == nil {
			for name, value := range extra {
				if strings.TrimSpace(name) != "" && strings.TrimSpace(value) != "" {
					req.Header.Set(name, value)
				}
			}
		}
	}
	req.Header.Set("User-Agent", withDefaultUserAgent(userAgent, s.defaultUpstreamUserAgent()))

	client := s.runtime().httpClientForTarget(nil, provider)
	client.Timeout = 30 * time.Second
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("GET %s: read body: %w", url, err)
	}
	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("GET %s: %w", url, connector.HTTPStatusError(resp.StatusCode, body))
	}
	var payload struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("GET %s: HTTP %d invalid models JSON: %w", url, resp.StatusCode, err)
	}
	models := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		models = append(models, item.ID)
	}
	return normalizeProviderModelIDs(models), nil
}

func (s *Service) invalidateAllModelsCache() {
	if s == nil {
		return
	}
	s.modelsCacheMu.Lock()
	s.modelsCache = map[uint]modelsCacheEntry{}
	s.modelsCacheMu.Unlock()
}
