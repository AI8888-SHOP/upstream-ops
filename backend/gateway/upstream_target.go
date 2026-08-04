// 上游转发目标（baseURL / key / channel / provider）。
package gateway

import "github.com/bejix/upstream-ops/backend/storage"

type upstreamTarget struct {
	BaseURL  string
	APIKey   string
	Channel  *storage.Channel
	Provider *storage.GatewayProvider
	// UserAgentOverride 非空时覆盖发往上游的 User-Agent（组+路由策略解析结果）。
	UserAgentOverride string
}

func (t *upstreamTarget) upstreamConcurrency() (upstreamConcurrencyKey, int, bool) {
	if t == nil {
		return upstreamConcurrencyKey{}, 0, false
	}
	if t.Provider != nil && t.Provider.ID != 0 {
		return upstreamConcurrencyKey{
			Kind: upstreamConcurrencyKindProvider,
			ID:   t.Provider.ID,
		}, t.Provider.ConcurrencyLimit, true
	}
	if t.Channel != nil && t.Channel.ID != 0 {
		return upstreamConcurrencyKey{
			Kind: upstreamConcurrencyKindMonitor,
			ID:   t.Channel.ID,
		}, t.Channel.ConcurrencyLimit, true
	}
	return upstreamConcurrencyKey{}, 0, false
}
