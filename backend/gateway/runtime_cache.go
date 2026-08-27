// 数据面：源分组列表缓存。
package gateway

import (
	"context"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func (rt *Runtime) loadGroupsByChannel(ctx context.Context, routes []storage.GatewayRoute) map[uint][]connector.APIKeyGroup {
	out := make(map[uint][]connector.APIKeyGroup)
	if ctx == nil {
		ctx = context.Background()
	}
	if rt.ChannelAPI == nil {
		return out
	}
	ids := make([]uint, 0)
	seen := make(map[uint]struct{})
	for _, r := range routes {
		if r.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
			continue
		}
		if r.SourceChannelID == 0 {
			continue
		}
		if _, ok := seen[r.SourceChannelID]; ok {
			continue
		}
		seen[r.SourceChannelID] = struct{}{}
		ids = append(ids, r.SourceChannelID)
	}
	if len(ids) == 0 {
		return out
	}

	// 先吃缓存，避免保存路由 / 运行时选路重复打上游。
	ttl := rt.gatewayRuntime().ModelsCacheTTL()
	if ttl <= 0 {
		ttl = 60 * time.Second
	}
	miss := make([]uint, 0, len(ids))
	stale := make([]uint, 0, len(ids))
	now := time.Now()
	rt.channelGroupsCacheMu.Lock()
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	for _, id := range ids {
		if ent, ok := rt.channelGroupsCache[id]; ok && now.Sub(ent.at) < ttl {
			out[id] = ent.groups
			continue
		}
		if ent, ok := rt.channelGroupsCache[id]; ok {
			// A stale source-group snapshot is still more useful than blocking a
			// user request on the upstream admin API. Refresh it in the background
			// and let the next request observe the new ratios.
			out[id] = ent.groups
			stale = append(stale, id)
			continue
		}
		miss = append(miss, id)
	}
	rt.channelGroupsCacheMu.Unlock()
	for _, id := range stale {
		rt.refreshChannelGroupsAsync(id)
	}

	if len(miss) == 0 {
		return out
	}

	fetchOne := func(id uint) []connector.APIKeyGroup {
		groups, epoch, err := rt.fetchChannelGroups(ctx, id)
		if err != nil {
			// Preserve the previous cold-cache behavior: a temporarily unavailable
			// management API must not make every gateway request wait for another
			// fetch. Successful stale refreshes never replace old data with nil.
			rt.storeChannelGroupsCacheAtEpoch(id, nil, epoch)
			return nil
		}
		return groups
	}

	if len(miss) == 1 {
		groups := fetchOne(miss[0])
		out[miss[0]] = groups
		rt.storeChannelGroupsCache(miss[0], groups)
		return out
	}

	// 保存路由 / 运行时倍率排序：按渠道并发拉源分组，缩短批量等待。
	sem := make(chan struct{}, rt.gatewayRuntime().RouteBatchConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range miss {
		id := id
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				mu.Lock()
				out[id] = nil
				mu.Unlock()
				return
			}
			groups := fetchOne(id)
			mu.Lock()
			out[id] = groups
			mu.Unlock()
			rt.storeChannelGroupsCache(id, groups)
		}()
	}
	wg.Wait()
	return out
}

func (rt *Runtime) storeChannelGroupsCache(channelID uint, groups []connector.APIKeyGroup) {
	rt.channelGroupsCacheMu.Lock()
	defer rt.channelGroupsCacheMu.Unlock()
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	rt.channelGroupsCache[channelID] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
}

const (
	channelGroupsFetchTimeout = 15 * time.Second
)

// fetchChannelGroups shares one upstream management request per channel. The
// fetch uses its own bounded context so cancellation of the first gateway
// request cannot abort work that other requests are already waiting for.
func (rt *Runtime) fetchChannelGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		call := rt.startChannelGroupsFetch(channelID)
		if call == nil {
			return nil, 0, context.Canceled
		}
		select {
		case <-call.done:
			rt.channelGroupsCacheMu.Lock()
			current := call.epoch == rt.channelGroupsCacheEpoch
			rt.channelGroupsCacheMu.Unlock()
			if !current {
				continue
			}
			return call.groups, call.epoch, call.err
		case <-ctx.Done():
			return nil, call.epoch, ctx.Err()
		}
	}
}

func (rt *Runtime) startChannelGroupsFetch(channelID uint) *channelGroupsFetchCall {
	if rt == nil || rt.ChannelAPI == nil || channelID == 0 {
		return nil
	}
	rt.channelGroupsCacheMu.Lock()
	if rt.channelGroupsFetches == nil {
		rt.channelGroupsFetches = map[uint]*channelGroupsFetchCall{}
	}
	if call := rt.channelGroupsFetches[channelID]; call != nil {
		rt.channelGroupsCacheMu.Unlock()
		return call
	}
	call := &channelGroupsFetchCall{
		done:  make(chan struct{}),
		epoch: rt.channelGroupsCacheEpoch,
	}
	rt.channelGroupsFetches[channelID] = call
	rt.channelGroupsCacheMu.Unlock()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), channelGroupsFetchTimeout)
		defer cancel()
		groups, err := rt.ChannelAPI.ListAPIKeyGroups(ctx, channelID)

		rt.channelGroupsCacheMu.Lock()
		call.groups = groups
		call.err = err
		if err == nil && rt.channelGroupsCacheEpoch == call.epoch {
			if rt.channelGroupsCache == nil {
				rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
			}
			rt.channelGroupsCache[channelID] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
		}
		if rt.channelGroupsFetches[channelID] == call {
			delete(rt.channelGroupsFetches, channelID)
		}
		close(call.done)
		rt.channelGroupsCacheMu.Unlock()
	}()
	return call
}

func (rt *Runtime) storeChannelGroupsCacheAtEpoch(channelID uint, groups []connector.APIKeyGroup, epoch uint64) {
	rt.channelGroupsCacheMu.Lock()
	defer rt.channelGroupsCacheMu.Unlock()
	if rt.channelGroupsCacheEpoch != epoch {
		return
	}
	if rt.channelGroupsCache == nil {
		rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	}
	rt.channelGroupsCache[channelID] = channelGroupsCacheEntry{at: time.Now(), groups: groups}
}

// refreshChannelGroupsAsync performs stale-while-revalidate with one refresh
// per channel. It never runs on the request goroutine and therefore cannot
// turn a slow upstream management endpoint into gateway request latency.
func (rt *Runtime) refreshChannelGroupsAsync(channelID uint) {
	_ = rt.startChannelGroupsFetch(channelID)
}

// InvalidateChannelGroupsCache 清空源分组缓存（倍率扫描后调用，保证下次重排拿到新 ratio）。

func (rt *Runtime) InvalidateChannelGroupsCache() {
	rt.channelGroupsCacheMu.Lock()
	defer rt.channelGroupsCacheMu.Unlock()
	rt.channelGroupsCacheEpoch++
	rt.channelGroupsCache = map[uint]channelGroupsCacheEntry{}
	// Detach old-generation calls so the first request after an admin change
	// can fetch immediately. The old goroutines still close their own waiters,
	// but their epoch check prevents them from repopulating the cache.
	rt.channelGroupsFetches = map[uint]*channelGroupsFetchCall{}
}

// upstreamTarget 解析后的上游目标（监控渠道或直连 Provider）。
