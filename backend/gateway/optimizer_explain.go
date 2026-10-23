package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// 一键优化的解释层：默认输出确定性规则文案（可复现、可离线），
// 只有在运维显式配置了解释模型时才交给模型润色。
// 模型只负责措辞，不参与决策——方案本身永远来自真实流量规则。
const (
	optimizerExplainModelEnv   = "UOPS_OPTIMIZER_LLM_MODEL"
	optimizerExplainBaseURLEnv = "UOPS_OPTIMIZER_LLM_BASE_URL"
	optimizerExplainAPIKeyEnv  = "UOPS_OPTIMIZER_LLM_API_KEY"
	optimizerExplainTimeout    = 20 * time.Second
)

// ExplainOptimizerPlan 把方案翻成人话，返回文案与来源（rule | model）。
func (s *Service) ExplainOptimizerPlan(ctx context.Context, plan *OptimizerPlan, only []string) (string, string) {
	if plan == nil {
		return "没有可分析的数据。", "rule"
	}
	filter := map[string]struct{}{}
	for _, id := range only {
		if trimmed := strings.TrimSpace(id); trimmed != "" {
			filter[trimmed] = struct{}{}
		}
	}
	selected := make([]OptimizerChange, 0, len(plan.Changes))
	for _, change := range plan.Changes {
		if len(filter) == 0 {
			selected = append(selected, change)
			continue
		}
		if _, ok := filter[change.ID]; ok {
			selected = append(selected, change)
		}
	}
	rule := optimizerRuleNarrative(plan, selected)
	if text, ok := s.optimizerModelNarrative(ctx, plan, selected); ok {
		return text, "model"
	}
	return rule, "rule"
}

func optimizerRuleNarrative(plan *OptimizerPlan, changes []OptimizerChange) string {
	var b strings.Builder
	fmt.Fprintf(&b, "分析窗口 %d 小时，共 %d 条尝试。", plan.WindowHours, plan.SampleCount)
	if len(changes) == 0 {
		b.WriteString("当前配置与实测流量吻合，无需调整。")
		return b.String()
	}
	fmt.Fprintf(&b, "发现 %d 项可优化：\n", len(changes))
	for index, change := range changes {
		fmt.Fprintf(&b, "%d. 【%s】%s — %s → %s。%s\n",
			index+1, optimizerKindLabel(change.Kind),
			optimizerChangeTarget(change), change.Before, change.After, change.Reason)
	}
	b.WriteString("以上调整只涉及首字超时与路由优先级，不会增删渠道或密钥；确认后才会写入配置。")
	return b.String()
}

func optimizerKindLabel(kind string) string {
	switch kind {
	case OptimizerKindFirstTokenTimeout:
		return "首字超时"
	case OptimizerKindRouteFallbackOnly:
		return "路由降权"
	case OptimizerKindRouteDisable:
		return "路由停用"
	}
	return kind
}

func optimizerChangeTarget(change OptimizerChange) string {
	if change.RouteID > 0 {
		label := change.RouteLabel
		if label == "" {
			label = fmt.Sprintf("路由 #%d", change.RouteID)
		}
		return change.GatewayGroupName + " / " + label
	}
	return "网关组「" + change.GatewayGroupName + "」"
}

// optimizerModelNarrative 调可选的解释模型；未配置或调用失败时返回 false 由规则文案兜底。
func (s *Service) optimizerModelNarrative(ctx context.Context, plan *OptimizerPlan, changes []OptimizerChange) (string, bool) {
	baseURL := strings.TrimSpace(os.Getenv(optimizerExplainBaseURLEnv))
	model := strings.TrimSpace(os.Getenv(optimizerExplainModelEnv))
	apiKey := strings.TrimSpace(os.Getenv(optimizerExplainAPIKeyEnv))
	if baseURL == "" || model == "" {
		return "", false
	}
	payload := map[string]any{
		"model": model,
		"messages": []map[string]string{
			{"role": "system", "content": "你是 API 网关的运维助手。用简体中文、不超过 200 字，把给定的配置调整方案解释给运营同事听：先说结论，再说依据，不要编造数据，不要使用英文术语。"},
			{"role": "user", "content": optimizerRuleNarrative(plan, changes)},
		},
		"temperature": 0.2,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", false
	}
	client := &http.Client{Timeout: optimizerExplainTimeout}
	url := strings.TrimRight(baseURL, "/") + "/chat/completions"
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", false
	}
	request.Header.Set("Content-Type", "application/json")
	if apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+apiKey)
	}
	response, err := client.Do(request)
	if err != nil {
		return "", false
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", false
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", false
	}
	var parsed struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil || len(parsed.Choices) == 0 {
		return "", false
	}
	text := strings.TrimSpace(parsed.Choices[0].Message.Content)
	if text == "" {
		return "", false
	}
	return text, true
}
