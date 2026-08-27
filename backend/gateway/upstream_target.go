// 上游转发目标（baseURL / key / channel / provider）。
package gateway

import (
	"sync"

	"github.com/bejix/upstream-ops/backend/storage"
)

type upstreamTarget struct {
	BaseURL  string
	APIKey   string
	Channel  *storage.Channel
	Provider *storage.GatewayProvider
	// UserAgentOverride 非空时覆盖发往上游的 User-Agent（组+路由策略解析结果）。
	UserAgentOverride string
	// onUpstreamStart is used by coordinated hedge accounting. It runs only
	// after concurrency admission and request construction, immediately before
	// the HTTP client is invoked.
	onUpstreamStart func()
}

// upstreamTargetRequestCache is deliberately request-scoped. Resolving a
// target performs a repository lookup and decrypts a credential; retries and
// hedges for the same route should reuse that immutable snapshot, while a
// process-wide cache would risk serving stale credentials after an admin edit.
type upstreamTargetRequestCache struct {
	mu       sync.Mutex
	entries  map[uint]upstreamTargetCacheEntry
	inFlight map[uint]*upstreamTargetCacheCall
}

type upstreamTargetCacheEntry struct {
	target *upstreamTarget
	err    error
}

type upstreamTargetCacheCall struct {
	done   chan struct{}
	target *upstreamTarget
	err    error
}

func (c *upstreamTargetRequestCache) resolve(rt *Runtime, route *storage.GatewayRoute) (*upstreamTarget, error) {
	if c == nil || route == nil || route.ID == 0 {
		return rt.resolveUpstreamTarget(route)
	}
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[uint]upstreamTargetCacheEntry)
	}
	if entry, ok := c.entries[route.ID]; ok {
		target := cloneUpstreamTarget(entry.target)
		err := entry.err
		c.mu.Unlock()
		return target, err
	}
	if call := c.inFlight[route.ID]; call != nil {
		c.mu.Unlock()
		<-call.done
		return cloneUpstreamTarget(call.target), call.err
	}
	if c.inFlight == nil {
		c.inFlight = make(map[uint]*upstreamTargetCacheCall)
	}
	call := &upstreamTargetCacheCall{done: make(chan struct{})}
	c.inFlight[route.ID] = call
	c.mu.Unlock()

	target, err := rt.resolveUpstreamTarget(route)
	base := cloneUpstreamTarget(target)
	c.mu.Lock()
	delete(c.inFlight, route.ID)
	call.target = base
	call.err = err
	c.entries[route.ID] = upstreamTargetCacheEntry{target: base, err: err}
	close(call.done)
	c.mu.Unlock()
	return cloneUpstreamTarget(base), err
}

func cloneUpstreamTarget(target *upstreamTarget) *upstreamTarget {
	if target == nil {
		return nil
	}
	clone := *target
	// The callback is attempt-specific and must never be shared between hedge
	// runners. Callers assign it after obtaining the request-local snapshot.
	clone.onUpstreamStart = nil
	return &clone
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
