package storage

import (
	"sync"
	"time"

	"gorm.io/gorm"
)

const storageReadCacheTTL = 2 * time.Second

type ttlReadCacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type ttlReadCacheCall[V any] struct {
	done       chan struct{}
	clearEpoch uint64
	keyEpoch   uint64
	value      V
	err        error
}

// ttlReadCache keeps immutable snapshots and coalesces concurrent cache
// misses. Exact invalidations advance only that key's generation, while a
// full/predicate clear advances the cache generation. A query that started
// before a write therefore cannot repopulate stale data after it commits.
type ttlReadCache[K comparable, V any] struct {
	mu         sync.Mutex
	ttl        time.Duration
	now        func() time.Time
	clone      func(V) V
	clearEpoch uint64
	keyEpochs  map[K]uint64
	items      map[K]ttlReadCacheEntry[V]
	inFlight   map[K]*ttlReadCacheCall[V]
}

func newTTLReadCache[K comparable, V any](ttl time.Duration, clone func(V) V) *ttlReadCache[K, V] {
	return &ttlReadCache[K, V]{
		ttl:       ttl,
		now:       time.Now,
		clone:     clone,
		keyEpochs: make(map[K]uint64),
		items:     make(map[K]ttlReadCacheEntry[V]),
		inFlight:  make(map[K]*ttlReadCacheCall[V]),
	}
}

func (c *ttlReadCache[K, V]) load(key K, load func() (V, error), cacheable func(V) bool) (V, error) {
	var zero V
	if c == nil {
		return load()
	}

	for {
		c.mu.Lock()
		now := c.now()
		if entry, ok := c.items[key]; ok {
			if now.Before(entry.expiresAt) {
				value := c.clone(entry.value)
				c.mu.Unlock()
				return value, nil
			}
			delete(c.items, key)
		}
		clearEpoch := c.clearEpoch
		keyEpoch := c.keyEpochs[key]
		if call := c.inFlight[key]; call != nil && call.clearEpoch == clearEpoch && call.keyEpoch == keyEpoch {
			c.mu.Unlock()
			<-call.done
			if call.err != nil {
				return zero, call.err
			}
			return c.clone(call.value), nil
		}

		call := &ttlReadCacheCall[V]{
			done: make(chan struct{}), clearEpoch: clearEpoch, keyEpoch: keyEpoch,
		}
		c.inFlight[key] = call
		c.mu.Unlock()

		value, err := load()
		stored := value
		if err == nil {
			stored = c.clone(value)
		}

		c.mu.Lock()
		if c.inFlight[key] == call {
			delete(c.inFlight, key)
		}
		call.value = stored
		call.err = err
		if err == nil && call.clearEpoch == c.clearEpoch && call.keyEpoch == c.keyEpochs[key] &&
			(cacheable == nil || cacheable(stored)) {
			c.items[key] = ttlReadCacheEntry[V]{
				value:     stored,
				expiresAt: c.now().Add(c.ttl),
			}
		}
		close(call.done)
		c.mu.Unlock()

		if err != nil {
			return zero, err
		}
		return c.clone(stored), nil
	}
}

func (c *ttlReadCache[K, V]) invalidate(key K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.keyEpochs[key]++
	delete(c.items, key)
	c.mu.Unlock()
}

func (c *ttlReadCache[K, V]) invalidateWhere(match func(V) bool) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.clearEpoch++
	for key, entry := range c.items {
		if match(entry.value) {
			delete(c.items, key)
		}
	}
	c.mu.Unlock()
}

func (c *ttlReadCache[K, V]) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.clearEpoch++
	clear(c.items)
	c.mu.Unlock()
}

func (c *ttlReadCache[K, V]) setClockForTest(now func() time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

type storageReadCaches struct {
	channels         *ttlReadCache[uint, Channel]
	gatewayProviders *ttlReadCache[uint, GatewayProvider]
	gatewayGroups    *ttlReadCache[uint, GatewayGroup]
	gatewayKeys      *ttlReadCache[string, GatewayKey]
	gatewayRoutes    *ttlReadCache[uint, []GatewayRoute]

	routeGroupsMu sync.RWMutex
	routeGroups   map[uint]uint
	keyHashesMu   sync.RWMutex
	keyHashes     map[uint]string
}

var storageReadCachesByDB sync.Map

func readCachesForDB(db *gorm.DB) *storageReadCaches {
	created := newStorageReadCaches()
	if db == nil {
		return created
	}
	key := any(db)
	if sqlDB, err := db.DB(); err == nil {
		key = sqlDB
	}
	actual, _ := storageReadCachesByDB.LoadOrStore(key, created)
	return actual.(*storageReadCaches)
}

func newStorageReadCaches() *storageReadCaches {
	return &storageReadCaches{
		channels:         newTTLReadCache[uint, Channel](storageReadCacheTTL, cloneChannel),
		gatewayProviders: newTTLReadCache[uint, GatewayProvider](storageReadCacheTTL, func(item GatewayProvider) GatewayProvider { return item }),
		gatewayGroups:    newTTLReadCache[uint, GatewayGroup](storageReadCacheTTL, func(item GatewayGroup) GatewayGroup { return item }),
		gatewayKeys:      newTTLReadCache[string, GatewayKey](storageReadCacheTTL, cloneGatewayKey),
		gatewayRoutes:    newTTLReadCache[uint, []GatewayRoute](storageReadCacheTTL, cloneGatewayRoutes),
		routeGroups:      make(map[uint]uint),
		keyHashes:        make(map[uint]string),
	}
}

func (c *storageReadCaches) rememberGatewayRoutes(groupID uint, routes []GatewayRoute) {
	if c == nil || groupID == 0 {
		return
	}
	c.routeGroupsMu.Lock()
	for i := range routes {
		if routes[i].ID > 0 {
			c.routeGroups[routes[i].ID] = groupID
		}
	}
	c.routeGroupsMu.Unlock()
}

func (c *storageReadCaches) invalidateGatewayRoute(id uint) {
	if c == nil || id == 0 {
		return
	}
	c.routeGroupsMu.RLock()
	groupID, known := c.routeGroups[id]
	c.routeGroupsMu.RUnlock()
	if known && groupID > 0 {
		c.gatewayRoutes.invalidate(groupID)
		return
	}
	// This fallback is limited to administrative writes before a route has
	// ever been read. Runtime routes are registered by ListByGroupID, so their
	// cooldown updates remain exact and do not disturb other groups.
	c.gatewayRoutes.invalidateWhere(func(routes []GatewayRoute) bool {
		for i := range routes {
			if routes[i].ID == id {
				return true
			}
		}
		return false
	})
}

func (c *storageReadCaches) rememberGatewayKey(item GatewayKey) {
	if c == nil || item.ID == 0 || item.KeyHash == "" {
		return
	}
	c.keyHashesMu.Lock()
	c.keyHashes[item.ID] = item.KeyHash
	c.keyHashesMu.Unlock()
}

func (c *storageReadCaches) invalidateGatewayKey(id uint, currentHash string) {
	if c == nil || id == 0 {
		return
	}
	c.keyHashesMu.Lock()
	previousHash := c.keyHashes[id]
	if currentHash != "" {
		c.keyHashes[id] = currentHash
	}
	c.keyHashesMu.Unlock()
	if previousHash != "" {
		c.gatewayKeys.invalidate(previousHash)
	}
	if currentHash != "" && currentHash != previousHash {
		c.gatewayKeys.invalidate(currentHash)
	}
	if previousHash == "" && currentHash == "" {
		c.gatewayKeys.invalidateWhere(func(item GatewayKey) bool { return item.ID == id })
	}
}

func (c *storageReadCaches) clearGatewayKeys() {
	if c == nil {
		return
	}
	c.gatewayKeys.clear()
	c.keyHashesMu.Lock()
	clear(c.keyHashes)
	c.keyHashesMu.Unlock()
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneChannel(item Channel) Channel {
	item.CaptchaConfigID = clonePointer(item.CaptchaConfigID)
	item.RechargeMultiplier = clonePointer(item.RechargeMultiplier)
	item.GroupMultiplier = clonePointer(item.GroupMultiplier)
	item.LastBalance = clonePointer(item.LastBalance)
	item.LastBalanceAt = clonePointer(item.LastBalanceAt)
	item.TodayCost = clonePointer(item.TodayCost)
	item.TotalCost = clonePointer(item.TotalCost)
	return item
}

func cloneGatewayKey(item GatewayKey) GatewayKey {
	item.LastUsedAt = clonePointer(item.LastUsedAt)
	return item
}

func cloneGatewayRoutes(items []GatewayRoute) []GatewayRoute {
	if items == nil {
		return nil
	}
	cloned := make([]GatewayRoute, len(items))
	for i := range items {
		cloned[i] = cloneGatewayRoute(items[i])
	}
	return cloned
}

func cloneGatewayRoute(item GatewayRoute) GatewayRoute {
	item.SourceGroupID = clonePointer(item.SourceGroupID)
	item.TempUnschedulableUntil = clonePointer(item.TempUnschedulableUntil)
	item.TempUnschedulableAt = clonePointer(item.TempUnschedulableAt)
	item.CacheHealthEvaluatedAt = clonePointer(item.CacheHealthEvaluatedAt)
	item.CacheHealthBlacklistedUntil = clonePointer(item.CacheHealthBlacklistedUntil)
	item.CacheHealthManualClearUntil = clonePointer(item.CacheHealthManualClearUntil)
	if item.ModelCooldowns == nil {
		return item
	}
	cooldowns := make(map[string]GatewayRouteModelCooldown, len(item.ModelCooldowns))
	for model, cooldown := range item.ModelCooldowns {
		cooldown.TempUnschedulableUntil = clonePointer(cooldown.TempUnschedulableUntil)
		cooldown.TempUnschedulableAt = clonePointer(cooldown.TempUnschedulableAt)
		cooldown.NextProbeAt = clonePointer(cooldown.NextProbeAt)
		cooldown.LastProbeAt = clonePointer(cooldown.LastProbeAt)
		cooldown.ProbeLeaseUntil = clonePointer(cooldown.ProbeLeaseUntil)
		cooldowns[model] = cooldown
	}
	item.ModelCooldowns = cooldowns
	return item
}
