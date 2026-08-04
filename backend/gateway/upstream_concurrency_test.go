package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestUpstreamConcurrencyUnlimitedDoesNotBlock(t *testing.T) {
	r := newUpstreamConcurrencyRegistry()
	key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 1}
	releases := make([]func(), 0, 32)
	for i := 0; i < 32; i++ {
		release, err := r.acquire(context.Background(), key, 0)
		if err != nil {
			t.Fatalf("acquire %d: %v", i, err)
		}
		releases = append(releases, release)
	}
	if active := upstreamConcurrencyActive(r, key); active != len(releases) {
		t.Fatalf("active=%d, want %d", active, len(releases))
	}
	for _, release := range releases {
		release()
	}
}

func TestUpstreamConcurrencySharesChannelAcrossTargets(t *testing.T) {
	svc := &Service{}
	rt := svc.runtime()
	targetA := &upstreamTarget{Channel: &storage.Channel{ID: 7, ConcurrencyLimit: 1}}
	targetB := &upstreamTarget{Channel: &storage.Channel{ID: 7, ConcurrencyLimit: 1}}

	releaseA, err := rt.acquireUpstreamConcurrency(context.Background(), targetA)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseA()

	acquired := make(chan func(), 1)
	errs := make(chan error, 1)
	go func() {
		release, acquireErr := rt.acquireUpstreamConcurrency(context.Background(), targetB)
		if acquireErr != nil {
			errs <- acquireErr
			return
		}
		acquired <- release
	}()

	select {
	case release := <-acquired:
		release()
		t.Fatal("same channel target acquired while its shared slot was held")
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(40 * time.Millisecond):
	}
	releaseA()

	select {
	case release := <-acquired:
		release()
	case err := <-errs:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("shared channel slot was not released")
	}
}

func TestUpstreamConcurrencySharesProviderAcrossTargets(t *testing.T) {
	svc := &Service{}
	rt := svc.runtime()
	targetA := &upstreamTarget{Provider: &storage.GatewayProvider{ID: 9, ConcurrencyLimit: 1}}
	targetB := &upstreamTarget{Provider: &storage.GatewayProvider{ID: 9, ConcurrencyLimit: 1}}

	releaseA, err := rt.acquireUpstreamConcurrency(context.Background(), targetA)
	if err != nil {
		t.Fatal(err)
	}
	acquired := make(chan func(), 1)
	go func() {
		release, acquireErr := rt.acquireUpstreamConcurrency(context.Background(), targetB)
		if acquireErr == nil {
			acquired <- release
		}
	}()
	select {
	case release := <-acquired:
		release()
		releaseA()
		t.Fatal("same provider acquired while its shared slot was held")
	case <-time.After(40 * time.Millisecond):
	}
	releaseA()
	select {
	case release := <-acquired:
		release()
	case <-time.After(time.Second):
		t.Fatal("shared provider slot was not released")
	}
}

func TestUpstreamConcurrencyNamespacesAndChannelsAreIndependent(t *testing.T) {
	r := newUpstreamConcurrencyRegistry()
	monitor := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 1}
	provider := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindProvider, ID: 1}
	otherMonitor := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 2}

	releaseMonitor, err := r.acquire(context.Background(), monitor, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseMonitor()
	releaseProvider, err := r.acquire(context.Background(), provider, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseProvider()
	releaseOther, err := r.acquire(context.Background(), otherMonitor, 1)
	if err != nil {
		t.Fatal(err)
	}
	releaseOther()
}

func TestUpstreamConcurrencyWaitHonorsContextCancellation(t *testing.T) {
	r := newUpstreamConcurrencyRegistry()
	key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 3}
	release, err := r.acquire(context.Background(), key, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	if _, err := r.acquire(ctx, key, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
}

func TestUpstreamConcurrencyDynamicLimitChanges(t *testing.T) {
	t.Run("increase", func(t *testing.T) {
		r := newUpstreamConcurrencyRegistry()
		key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 4}
		first, err := r.acquire(context.Background(), key, 1)
		if err != nil {
			t.Fatal(err)
		}
		waiter := make(chan func(), 1)
		go func() {
			release, acquireErr := r.acquire(context.Background(), key, 1)
			if acquireErr == nil {
				waiter <- release
			}
		}()
		assertUpstreamConcurrencyBlocked(t, waiter)

		second, err := r.acquire(context.Background(), key, 2)
		if err != nil {
			t.Fatal(err)
		}
		first()
		select {
		case release := <-waiter:
			release()
		case <-time.After(time.Second):
			t.Fatal("increased limit did not wake the queued attempt")
		}
		second()
	})

	t.Run("decrease", func(t *testing.T) {
		r := newUpstreamConcurrencyRegistry()
		key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindProvider, ID: 5}
		first, _ := r.acquire(context.Background(), key, 3)
		second, _ := r.acquire(context.Background(), key, 3)
		third, _ := r.acquire(context.Background(), key, 3)
		waiter := make(chan func(), 1)
		go func() {
			release, acquireErr := r.acquire(context.Background(), key, 1)
			if acquireErr == nil {
				waiter <- release
			}
		}()
		time.Sleep(10 * time.Millisecond)
		first()
		second()
		assertUpstreamConcurrencyBlocked(t, waiter)
		third()
		select {
		case release := <-waiter:
			release()
		case <-time.After(time.Second):
			t.Fatal("decreased limit waiter did not proceed after active slots drained")
		}
	})

	t.Run("unlimited", func(t *testing.T) {
		r := newUpstreamConcurrencyRegistry()
		key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 6}
		first, _ := r.acquire(context.Background(), key, 1)
		waiter := make(chan func(), 1)
		go func() {
			release, acquireErr := r.acquire(context.Background(), key, 1)
			if acquireErr == nil {
				waiter <- release
			}
		}()
		assertUpstreamConcurrencyBlocked(t, waiter)
		second, err := r.acquire(context.Background(), key, 0)
		if err != nil {
			t.Fatal(err)
		}
		select {
		case release := <-waiter:
			release()
		case <-time.After(time.Second):
			t.Fatal("unlimited update did not wake queued attempt")
		}
		first()
		second()
	})
}

func TestUpstreamConcurrencyReleaseIsIdempotent(t *testing.T) {
	r := newUpstreamConcurrencyRegistry()
	key := upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 8}
	release, err := r.acquire(context.Background(), key, 1)
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
	if active := upstreamConcurrencyActive(r, key); active != 0 {
		t.Fatalf("active=%d after idempotent release", active)
	}
	second, err := r.acquire(context.Background(), key, 1)
	if err != nil {
		t.Fatal(err)
	}
	second()
}

func TestForwardOnceConcurrencyPeak(t *testing.T) {
	var current, peak int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := atomic.AddInt32(&current, 1)
		for {
			old := atomic.LoadInt32(&peak)
			if now <= old || atomic.CompareAndSwapInt32(&peak, old, now) {
				break
			}
		}
		time.Sleep(35 * time.Millisecond)
		atomic.AddInt32(&current, -1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	svc := &Service{}
	rt := svc.runtime()
	target := &upstreamTarget{
		BaseURL: upstream.URL,
		APIKey:  "k",
		Channel: &storage.Channel{ID: 10, ConcurrencyLimit: 2},
	}
	const requests = 8
	var wg sync.WaitGroup
	errs := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, _, _, _, err := rt.forwardOnce(
				context.Background(), nil, target, "/v1/chat/completions", http.MethodPost,
				http.Header{"Content-Type": []string{"application/json"}}, []byte(`{"model":"m"}`), false,
				protocol.KindOpenAIChat, 0,
			)
			if err != nil || status != http.StatusOK {
				errs <- errors.Join(err, errors.New("unexpected upstream status"))
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if got := atomic.LoadInt32(&peak); got > 2 {
		t.Fatalf("peak concurrency=%d, want <=2", got)
	}
}

func TestForwardStreamHoldsConcurrencyThroughClientDisconnectDrain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	firstSent := make(chan struct{})
	finishStream := make(chan struct{})
	fastStarted := make(chan struct{}, 1)
	var firstOnce sync.Once
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/stream" {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\n")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			firstOnce.Do(func() { close(firstSent) })
			<-finishStream
			_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			return
		}
		select {
		case fastStarted <- struct{}{}:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()

	svc := &Service{}
	rt := svc.runtime()
	target := &upstreamTarget{
		BaseURL: upstream.URL,
		APIKey:  "k",
		Channel: &storage.Channel{ID: 11, ConcurrencyLimit: 1},
	}
	clientCtx, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	rec := &concurrencySignalRecorder{
		flushRecorder: &flushRecorder{ResponseRecorder: httptest.NewRecorder()},
		committed:     make(chan struct{}),
	}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/stream", nil).WithContext(clientCtx)
	streamDone := make(chan streamAttemptResult, 1)
	go func() {
		streamDone <- rt.forwardStream(clientCtx, c, target, "/stream", http.MethodPost, nil,
			[]byte(`{"model":"m","stream":true}`), protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0)
	}()
	select {
	case <-firstSent:
	case <-time.After(time.Second):
		t.Fatal("stream did not start")
	}
	// Ensure the first frame has been committed before simulating disconnect.
	select {
	case <-rec.committed:
	case <-time.After(time.Second):
		t.Fatal("stream did not commit its first frame")
	}
	cancelClient()

	fastDone := make(chan error, 1)
	go func() {
		status, _, _, _, err := rt.forwardOnce(context.Background(), nil, target, "/fast", http.MethodPost, nil,
			[]byte(`{"model":"m"}`), false, protocol.KindOpenAIChat, 0)
		if err != nil {
			fastDone <- err
			return
		}
		if status != http.StatusOK {
			fastDone <- errors.New("unexpected fast status")
			return
		}
		fastDone <- nil
	}()
	select {
	case <-fastStarted:
		t.Fatal("second attempt acquired while first stream was still draining")
	case <-time.After(50 * time.Millisecond):
	}
	close(finishStream)
	select {
	case result := <-streamDone:
		if !result.ClientDisconnected {
			t.Fatalf("stream result=%+v, want client disconnect", result)
		}
	case <-time.After(time.Second):
		t.Fatal("stream did not finish draining")
	}
	select {
	case err := <-fastDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second attempt did not acquire after stream drain")
	}
}

func TestCanceledAttemptReleasesConcurrency(t *testing.T) {
	started := make(chan struct{})
	stopHandler := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-stopHandler:
		}
	}))
	defer upstream.Close()
	defer close(stopHandler)

	svc := &Service{}
	rt := svc.runtime()
	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k", Channel: &storage.Channel{ID: 12, ConcurrencyLimit: 1}}
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, _, _, _, err := rt.forwardOnce(ctx, nil, target, "/slow", http.MethodPost, nil, []byte(`{}`), false, protocol.KindOpenAIChat, 0)
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("slow attempt did not start")
	}
	secondStarted := make(chan struct{}, 1)
	fast := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondStarted <- struct{}{}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer fast.Close()
	targetFast := *target
	targetFast.BaseURL = fast.URL
	fastDone := make(chan error, 1)
	go func() {
		_, _, _, _, err := rt.forwardOnce(context.Background(), nil, &targetFast, "/fast", http.MethodPost, nil, []byte(`{}`), false, protocol.KindOpenAIChat, 0)
		fastDone <- err
	}()
	select {
	case <-secondStarted:
		t.Fatal("second attempt acquired before loser cancellation")
	case <-time.After(40 * time.Millisecond):
	}
	cancel()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("canceled attempt did not return")
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("canceled attempt did not release its slot")
	}
	if err := <-fastDone; err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamTargetConcurrencyIdentity(t *testing.T) {
	channelTarget := &upstreamTarget{Channel: &storage.Channel{ID: 4, ConcurrencyLimit: 3}}
	key, limit, ok := channelTarget.upstreamConcurrency()
	if !ok || key != (upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: 4}) || limit != 3 {
		t.Fatalf("channel spec=(%+v,%d,%v)", key, limit, ok)
	}
	providerTarget := &upstreamTarget{Provider: &storage.GatewayProvider{ID: 4, ConcurrencyLimit: 5}}
	key, limit, ok = providerTarget.upstreamConcurrency()
	if !ok || key != (upstreamConcurrencyKey{Kind: upstreamConcurrencyKindProvider, ID: 4}) || limit != 5 {
		t.Fatalf("provider spec=(%+v,%d,%v)", key, limit, ok)
	}
}

func assertUpstreamConcurrencyBlocked(t *testing.T, acquired <-chan func()) {
	t.Helper()
	select {
	case release := <-acquired:
		release()
		t.Fatal("acquire completed while the limit was full")
	case <-time.After(30 * time.Millisecond):
	}
}

func upstreamConcurrencyActive(r *upstreamConcurrencyRegistry, key upstreamConcurrencyKey) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry := r.entries[key]; entry != nil {
		return entry.active
	}
	return 0
}

type concurrencySignalRecorder struct {
	*flushRecorder
	committed chan struct{}
	once      sync.Once
}

func (r *concurrencySignalRecorder) Write(p []byte) (int, error) {
	n, err := r.ResponseRecorder.Write(p)
	if n > 0 {
		r.once.Do(func() { close(r.committed) })
	}
	return n, err
}
