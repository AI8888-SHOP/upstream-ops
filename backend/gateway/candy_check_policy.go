package gateway

import (
	"errors"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

type CandyCheckPolicyInput struct {
	CandyCheckEnabled         *bool   `json:"candy_check_enabled"`
	CandyCheckModel           *string `json:"candy_check_model"`
	CandyCheckIntervalMinutes *int    `json:"candy_check_interval_minutes"`
	CandyCheckCooldownMinutes *int    `json:"candy_check_cooldown_minutes"`
	CandyCheckReasoningEffort *string `json:"candy_check_reasoning_effort"`
}

func applyCandyCheckPolicy(p *storage.GatewayCandyCheckPolicy, in CandyCheckPolicyInput) error {
	if in.CandyCheckEnabled != nil {
		p.CandyCheckEnabled = *in.CandyCheckEnabled
	}
	if in.CandyCheckModel != nil {
		p.CandyCheckModel = strings.TrimSpace(*in.CandyCheckModel)
	}
	if in.CandyCheckIntervalMinutes != nil {
		p.CandyCheckIntervalMinutes = *in.CandyCheckIntervalMinutes
	}
	if in.CandyCheckCooldownMinutes != nil {
		p.CandyCheckCooldownMinutes = *in.CandyCheckCooldownMinutes
	}
	if in.CandyCheckReasoningEffort != nil {
		p.CandyCheckReasoningEffort = strings.TrimSpace(*in.CandyCheckReasoningEffort)
	}
	if len(p.CandyCheckModel) > 256 || strings.ContainsAny(p.CandyCheckModel, "\r\n\x00") {
		return errors.New("candy_check_model must be a model ID of at most 256 bytes")
	}
	if p.CandyCheckEnabled && p.CandyCheckModel == "" {
		return errors.New("开启不降智检测时必须填写检测模型")
	}
	if p.CandyCheckIntervalMinutes < 1 || p.CandyCheckIntervalMinutes > 1440 {
		return errors.New("检测间隔必须为 1 到 1440 分钟")
	}
	if p.CandyCheckCooldownMinutes < 1 || p.CandyCheckCooldownMinutes > 43200 {
		return errors.New("检测失败冷却时长必须为 1 到 43200 分钟")
	}
	switch p.CandyCheckReasoningEffort {
	case "default", "low", "medium", "high":
	default:
		return errors.New("检测推理强度必须为 default、low、medium 或 high")
	}
	return nil
}
