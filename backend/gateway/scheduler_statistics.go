package gateway

import (
	"container/list"
	"context"
	"crypto/sha256"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const schedulerMaxKeys = 4096
const schedulerHistoryMinutes = 60

// No plaintext credential, client API key or request content is retained.
type schedulerStatsKey struct {
	GroupID                 uint
	Source                  routeSelectionKey
	Credential              [32]byte
	Model, Effort, Endpoint string
	Size                    uint8
}

// Fixed logarithmic histogram: bounded memory and constant-time updates.
var schedulerLatencyBounds = [...]float64{250, 500, 1000, 2000, 4000, 8000, 12000, 16000, 24000, 32000, 60000, 120000, 300000, 600000, 1800000}

type schedulerBucket struct {
	minute                     int64
	completed, failures, first uint32
	sumMS                      float64
	failureWaitMS              float64
	hist                       [len(schedulerLatencyBounds)]uint32
}
type schedulerSeries struct {
	key     schedulerStatsKey
	buckets [schedulerHistoryMinutes]schedulerBucket
}
type schedulerStatistics struct {
	mu   sync.Mutex
	keys map[schedulerStatsKey]*list.Element
	lru  list.List
}
type schedulerEstimate struct {
	Samples                    int
	FirstSamples               int
	Window                     int
	MeanMS, P90MS, FailureRate float64
	FailureWindow              int
	FailureWaitMS              float64
}

func (s *Service) schedulingStatistics() *schedulerStatistics {
	s.schedulerStatsMu.Lock()
	defer s.schedulerStatsMu.Unlock()
	if s.schedulerStats == nil {
		s.schedulerStats = &schedulerStatistics{keys: make(map[schedulerStatsKey]*list.Element)}
	}
	return s.schedulerStats
}

func (s *schedulerStatistics) record(key schedulerStatsKey, now time.Time, firstMS *float64, failed *bool, failureWaitMS ...float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.keys[key]
	if element == nil {
		if len(s.keys) >= schedulerMaxKeys {
			old := s.lru.Back()
			delete(s.keys, old.Value.(*schedulerSeries).key)
			s.lru.Remove(old)
		}
		element = s.lru.PushFront(&schedulerSeries{key: key})
		s.keys[key] = element
	} else {
		s.lru.MoveToFront(element)
	}
	minute := now.Unix() / 60
	b := &element.Value.(*schedulerSeries).buckets[minute%schedulerHistoryMinutes]
	if b.minute != minute {
		*b = schedulerBucket{minute: minute}
	}
	if failed != nil {
		b.completed++
		if *failed {
			b.failures++
			if len(failureWaitMS) > 0 && !math.IsNaN(failureWaitMS[0]) && !math.IsInf(failureWaitMS[0], 0) {
				b.failureWaitMS += math.Max(0, math.Min(failureWaitMS[0], schedulerLatencyBounds[len(schedulerLatencyBounds)-1]))
			}
		}
	}
	if firstMS != nil {
		ms := math.Max(0, math.Min(*firstMS, schedulerLatencyBounds[len(schedulerLatencyBounds)-1]))
		b.first++
		b.sumMS += ms
		for i, bound := range schedulerLatencyBounds {
			if ms <= bound {
				b.hist[i]++
				break
			}
		}
	}
}

func (s *schedulerStatistics) estimate(key schedulerStatsKey, now time.Time, window, minimum int) schedulerEstimate {
	s.mu.Lock()
	defer s.mu.Unlock()
	element := s.keys[key]
	if element == nil {
		return schedulerEstimate{Window: window, FailureWindow: window}
	}
	series := element.Value.(*schedulerSeries)
	result := series.estimate(now, window)
	if result.FirstSamples < minimum || result.Samples < minimum {
		history := series.estimate(now, schedulerHistoryMinutes)
		// No first token is exactly what an outage looks like. Borrow latency
		// history without erasing a sufficiently sampled recent failure window.
		if result.FirstSamples < minimum {
			result.FirstSamples, result.MeanMS, result.P90MS, result.Window = history.FirstSamples, history.MeanMS, history.P90MS, history.Window
		}
		if result.Samples < minimum {
			result.Samples, result.FailureRate, result.FailureWaitMS, result.FailureWindow = history.Samples, history.FailureRate, history.FailureWaitMS, history.Window
		}
	}
	return result
}

func (s *schedulerSeries) estimate(now time.Time, window int) schedulerEstimate {
	result := schedulerEstimate{Window: window, FailureWindow: window}
	minute := now.Unix() / 60
	var first, sum, completed, failures, failureWait float64
	var histogram [len(schedulerLatencyBounds)]float64
	for i := range s.buckets {
		b := &s.buckets[i]
		age := minute - b.minute
		if age < 0 || age >= int64(window) {
			continue
		}
		weight := math.Exp2(-float64(age) / math.Max(1, float64(window)/2))
		result.Samples += int(b.completed)
		result.FirstSamples += int(b.first)
		first += float64(b.first) * weight
		sum += b.sumMS * weight
		completed += float64(b.completed) * weight
		failures += float64(b.failures) * weight
		failureWait += b.failureWaitMS * weight
		for index, count := range b.hist {
			histogram[index] += float64(count) * weight
		}
	}
	if first > 0 {
		result.MeanMS = sum / first
		var cumulative float64
		for i, count := range histogram {
			cumulative += count
			if cumulative >= first*0.9 {
				result.P90MS = schedulerLatencyBounds[i]
				break
			}
		}
	}
	// A small prior avoids declaring one successful call perfectly reliable.
	result.FailureRate = (failures + 0.5) / (completed + 10)
	if failures > 0 {
		result.FailureWaitMS = failureWait / failures
	}
	return result
}

// first and finish have separate once guards: streams contribute TTFT as soon
// as visible output reaches the client, while reliability is recorded at end.
type schedulerObservation struct {
	stats                 *schedulerStatistics
	key                   schedulerStatsKey
	started               time.Time
	upstreamStarted       atomic.Bool
	hasFirst              atomic.Bool
	firstOnce, finishOnce sync.Once
}

func (rt *Runtime) newSchedulerObservation(candidate ScoredRoute, started time.Time, targets ...*upstreamTarget) *schedulerObservation {
	if candidate.Decision == nil {
		return nil
	}
	key := candidate.StatisticsKey
	if len(targets) > 0 && targets[0] != nil && targets[0].Provider != nil {
		provider := targets[0].Provider
		key.Credential = schedulerCredential(provider.APIKeyCipher, provider.BaseURL)
	}
	return &schedulerObservation{stats: rt.schedulingStatistics(), key: key, started: started}
}
func (o *schedulerObservation) markUpstreamStarted() {
	if o != nil {
		o.upstreamStarted.Store(true)
	}
}
func (o *schedulerObservation) first() {
	if o == nil || !o.upstreamStarted.Load() {
		return
	}
	o.firstOnce.Do(func() {
		o.hasFirst.Store(true)
		ms := float64(time.Since(o.started).Microseconds()) / 1000
		o.stats.record(o.key, time.Now(), &ms, nil)
	})
}
func (o *schedulerObservation) finish(ctx context.Context, success bool, errorType, attemptStatus string, statuses ...int) {
	if o == nil || !o.upstreamStarted.Load() {
		return
	}
	// A client leaving, a local queue wait, the shared request budget and a
	// canceled hedge reveal no independent upstream reliability outcome.
	if ctx.Err() != nil || attemptStatus == "canceled" {
		return
	}
	switch errorType {
	case "client", "canceled", "queue_timeout", "request_timeout", "config":
		return
	}
	// Invalid payload/size errors primarily describe the caller's request.
	if len(statuses) > 0 && errorType != "upstream_error" && (statuses[0] == 400 || statuses[0] == 413 || statuses[0] == 422) {
		return
	}
	o.finishOnce.Do(func() {
		failed := !success || !o.hasFirst.Load()
		o.stats.record(o.key, time.Now(), nil, &failed, float64(time.Since(o.started).Milliseconds()))
	})
}

func schedulerCredential(cipher, baseURL string) [32]byte {
	return sha256.Sum256([]byte(cipher + "\x00" + baseURL))
}
