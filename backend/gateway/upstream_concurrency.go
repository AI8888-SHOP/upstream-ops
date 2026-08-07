package gateway

import (
	"context"
	"sync"
)

const (
	upstreamConcurrencyKindMonitor  = "monitor"
	upstreamConcurrencyKindProvider = "provider"
)

// upstreamConcurrencyKey keeps monitored channels and direct providers in
// separate namespaces even when their database IDs are equal.
type upstreamConcurrencyKey struct {
	Kind string
	ID   uint
}

type upstreamConcurrencyEntry struct {
	active  int
	limit   int
	waiters []*upstreamConcurrencyWaiter
}

func newUpstreamConcurrencyEntry(limit int) *upstreamConcurrencyEntry {
	return &upstreamConcurrencyEntry{
		limit: normalizeUpstreamConcurrencyLimit(limit),
	}
}

type upstreamConcurrencyWaiter struct {
	ready   chan struct{}
	granted bool
}

// upstreamConcurrencyRegistry tracks attempts across every route, group,
// model, and gateway key that resolves to the same upstream.
type upstreamConcurrencyRegistry struct {
	mu      sync.Mutex
	entries map[upstreamConcurrencyKey]*upstreamConcurrencyEntry
}

func newUpstreamConcurrencyRegistry() *upstreamConcurrencyRegistry {
	return &upstreamConcurrencyRegistry{
		entries: make(map[upstreamConcurrencyKey]*upstreamConcurrencyEntry),
	}
}

func normalizeUpstreamConcurrencyLimit(limit int) int {
	if limit < 0 {
		return 0
	}
	return limit
}

// acquire waits for one attempt slot. The limit supplied by the newest
// acquisition becomes effective immediately: shrinking never cancels active
// attempts, while growing or disabling the limit wakes queued attempts.
func (r *upstreamConcurrencyRegistry) acquire(
	ctx context.Context,
	key upstreamConcurrencyKey,
	limit int,
) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key.ID == 0 {
		return func() {}, nil
	}
	if r == nil {
		return func() {}, nil
	}

	limit = normalizeUpstreamConcurrencyLimit(limit)
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[upstreamConcurrencyKey]*upstreamConcurrencyEntry)
	}
	entry := r.entries[key]
	limitChanged := false
	if entry == nil {
		entry = newUpstreamConcurrencyEntry(limit)
		r.entries[key] = entry
	} else if entry.limit != limit {
		entry.limit = limit
		limitChanged = true
	}

	// Existing waiters always keep their place. Reserving the active slot while
	// holding the registry lock prevents a release from waking every blocked
	// goroutine only to have all but one contend and go back to sleep. A caller
	// that changes the limit keeps the previous immediate-update semantics: it
	// claims newly-created capacity, then any additional slots go to the queue.
	if (limitChanged || len(entry.waiters) == 0) && (entry.limit == 0 || entry.active < entry.limit) {
		entry.active++
		r.dispatchLocked(entry)
		r.mu.Unlock()
		return r.releaseFunc(entry), nil
	}
	waiter := &upstreamConcurrencyWaiter{ready: make(chan struct{})}
	entry.waiters = append(entry.waiters, waiter)
	r.dispatchLocked(entry)
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		r.mu.Lock()
		if waiter.granted {
			if entry.active > 0 {
				entry.active--
			}
		} else {
			removeUpstreamConcurrencyWaiter(entry, waiter)
		}
		r.dispatchLocked(entry)
		r.mu.Unlock()
		return nil, ctx.Err()
	case <-waiter.ready:
		if err := ctx.Err(); err != nil {
			r.mu.Lock()
			if entry.active > 0 {
				entry.active--
			}
			r.dispatchLocked(entry)
			r.mu.Unlock()
			return nil, err
		}
	}
	return r.releaseFunc(entry), nil
}

func (r *upstreamConcurrencyRegistry) releaseFunc(entry *upstreamConcurrencyEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if entry.active > 0 {
				entry.active--
			}
			r.dispatchLocked(entry)
			r.mu.Unlock()
		})
	}
}

// dispatchLocked grants only the number of slots that are currently
// available. A normal release therefore wakes one FIFO waiter instead of the
// whole queue. Switching a limit to unlimited intentionally drains the queue.
func (r *upstreamConcurrencyRegistry) dispatchLocked(entry *upstreamConcurrencyEntry) {
	if entry == nil {
		return
	}
	for len(entry.waiters) > 0 && (entry.limit == 0 || entry.active < entry.limit) {
		waiter := entry.waiters[0]
		entry.waiters[0] = nil
		entry.waiters = entry.waiters[1:]
		if waiter == nil || waiter.granted {
			continue
		}
		waiter.granted = true
		entry.active++
		close(waiter.ready)
	}
}

func removeUpstreamConcurrencyWaiter(entry *upstreamConcurrencyEntry, target *upstreamConcurrencyWaiter) {
	if entry == nil || target == nil {
		return
	}
	for index, waiter := range entry.waiters {
		if waiter != target {
			continue
		}
		copy(entry.waiters[index:], entry.waiters[index+1:])
		entry.waiters[len(entry.waiters)-1] = nil
		entry.waiters = entry.waiters[:len(entry.waiters)-1]
		return
	}
}

func (s *Service) upstreamConcurrencyRegistry() *upstreamConcurrencyRegistry {
	if s == nil {
		return nil
	}
	s.upstreamConcurrencyMu.Lock()
	defer s.upstreamConcurrencyMu.Unlock()
	if s.upstreamConcurrency == nil {
		s.upstreamConcurrency = newUpstreamConcurrencyRegistry()
	}
	return s.upstreamConcurrency
}

func (rt *Runtime) acquireUpstreamConcurrency(ctx context.Context, target *upstreamTarget) (func(), error) {
	if rt == nil || rt.Service == nil {
		return func() {}, nil
	}
	key, limit, ok := target.upstreamConcurrency()
	if !ok {
		return func() {}, nil
	}
	return rt.Service.upstreamConcurrencyRegistry().acquire(ctx, key, limit)
}
