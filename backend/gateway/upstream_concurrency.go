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
	changed chan struct{}
}

func newUpstreamConcurrencyEntry(limit int) *upstreamConcurrencyEntry {
	return &upstreamConcurrencyEntry{
		limit:   normalizeUpstreamConcurrencyLimit(limit),
		changed: make(chan struct{}),
	}
}

func (e *upstreamConcurrencyEntry) notify() {
	close(e.changed)
	e.changed = make(chan struct{})
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
	if entry == nil {
		entry = newUpstreamConcurrencyEntry(limit)
		r.entries[key] = entry
	} else if entry.limit != limit {
		entry.limit = limit
		entry.notify()
	}

	for entry.limit > 0 && entry.active >= entry.limit {
		changed := entry.changed
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
		r.mu.Lock()
		if err := ctx.Err(); err != nil {
			r.mu.Unlock()
			return nil, err
		}
	}
	entry.active++
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if entry.active > 0 {
				entry.active--
				entry.notify()
			}
			r.mu.Unlock()
		})
	}, nil
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
