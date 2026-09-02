// 组级重试、故障转移与首字超时策略的 clamp / 生效判定。
package gateway

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
)

// normalizeCacheHealthOverride validates one group-level cache-health value.
// A nil pointer means "inherit global"; zero is retained for controls where it
// explicitly disables protection.
func normalizeCacheHealthOverride(name string, value int) (int, error) {
	max := 0
	switch name {
	case "window":
		max = 7 * 24 * 60
	case "blacklist":
		max = 30 * 24 * 60
	case "minimum":
		max = config.MaxGatewayCacheHitRateMinimumRequests
	default:
		return 0, fmt.Errorf("unknown cache health override %q", name)
	}
	if value < 0 || value > max {
		return 0, fmt.Errorf("cache health %s must be between 0 and %d", name, max)
	}
	if name == "minimum" && value < config.MinGatewayCacheHitRateMinimumRequests {
		value = config.MinGatewayCacheHitRateMinimumRequests
	}
	return value, nil
}

func normalizeCacheHealthThreshold(value float64) (float64, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
		return 0, errors.New("cache health threshold must be between 0 and 100")
	}
	return value, nil
}

func cacheHealthIntOverride(name string, value *int) (*int, error) {
	if value == nil {
		return nil, nil
	}
	normalized, err := normalizeCacheHealthOverride(name, *value)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func cacheHealthThresholdOverride(value *float64) (*float64, error) {
	if value == nil {
		return nil, nil
	}
	normalized, err := normalizeCacheHealthThreshold(*value)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func (svc *Service) clampGroupRetryPolicy(retryCount, failoverMax, cooldownSec int) (int, int, int) {
	if retryCount < 0 {
		retryCount = 0
	}
	if retryCount > 10 {
		retryCount = 10
	}
	if failoverMax < 0 {
		failoverMax = 0
	}
	if failoverMax > 32 {
		failoverMax = 32
	}
	if cooldownSec < 0 {
		cooldownSec = 0
	}
	if cooldownSec > 86400 {
		cooldownSec = 86400
	}
	return retryCount, failoverMax, cooldownSec
}

// clampResponseValidationRetryCount keeps -1 as the compatibility sentinel
// that inherits retry_count while allowing an explicit 0 to disable retries
// caused by response-rule matches.
func clampResponseValidationRetryCount(retryCount int) int {
	if retryCount < -1 {
		return -1
	}
	if retryCount > 10 {
		return 10
	}
	return retryCount
}

// clampFirstTokenTimeoutSec 0=关闭；1～300 秒有效（小于 1 且非 0 时抬到 1）。
func (svc *Service) clampFirstTokenTimeoutSec(sec int) int {
	if sec <= 0 {
		return 0
	}
	if sec < 1 {
		return 1
	}
	if sec > 300 {
		return 300
	}
	return sec
}

// clampGroupHedgePolicy 统一组级 hedge 的边界。maxParallel 包含主请求，
// maxAttempts 是整个客户端请求允许启动的 attempt 总数。
func (svc *Service) clampGroupHedgePolicy(delaySeconds float64, maxParallel, maxAttempts int) (float64, int, int) {
	cfg := (config.GatewayHedgeConfig{
		DelaySeconds: delaySeconds,
		MaxParallel:  maxParallel,
		MaxAttempts:  maxAttempts,
	}).WithDefaults()
	return cfg.DelaySeconds, cfg.MaxParallel, cfg.MaxAttempts
}

func (svc *Service) clampGroupResponseValidationPolicy(streamMode string, prefixBytes, prefixTimeoutMS int) (string, int, int) {
	cfg := (config.GatewayResponseValidationConfig{
		StreamMode:      streamMode,
		PrefixBytes:     prefixBytes,
		PrefixTimeoutMS: prefixTimeoutMS,
	}).WithDefaults()
	return cfg.StreamMode, cfg.PrefixBytes, cfg.PrefixTimeoutMS
}

// effectiveFirstTokenTimeout 决定本 attempt 实际使用的首字超时。
// 首字超时按渠道/路由独立计时，仅用于「失败后还能顺延到其它路由」时的快失败；
// 已是本请求最后一条可试路由时关闭（返回 0），让最后一枪老实等上游，避免无意义掐断。
func (svc *Service) effectiveFirstTokenTimeout(
	configured time.Duration,
	retryEnabled, failoverEnabled bool,
	failoversDone, failoverMax int,
	hasMoreRoutesAfterCurrent bool,
) time.Duration {
	if configured <= 0 {
		return 0
	}
	canFailoverToOther := retryEnabled && failoverEnabled &&
		failoversDone < failoverMax && hasMoreRoutesAfterCurrent
	if !canFailoverToOther {
		return 0
	}
	return configured
}

// errFirstTokenTimeout 首字超时，按传输类错误走重试/顺延。
var errFirstTokenTimeout = errors.New("first token timeout")

func (svc *Service) isFirstTokenTimeout(err error) bool {
	return err != nil && (errors.Is(err, errFirstTokenTimeout) || strings.Contains(err.Error(), "first token timeout"))
}
