package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestRunHedgeTerminalFailureStopsWithoutWinner(t *testing.T) {
	want := errors.New("transport failover disabled")
	result, err := runHedge(
		context.Background(),
		false,
		hedgePolicy{MaxAttempts: 4},
		func(context.Context, hedgeAttemptInfo) (int, error) {
			return 0, stopHedgeAttempts(want)
		},
		func(int) (bool, error) { return true, nil },
		hedgeHooks[int]{},
	)
	if !errors.Is(err, want) {
		t.Fatalf("err=%v, want %v", err, want)
	}
	if result.Winner != nil || len(result.Attempts) != 1 {
		t.Fatalf("winner=%+v attempts=%d, want no winner and one attempt", result.Winner, len(result.Attempts))
	}
}

func TestRunHedgeSequentialPlanPreservesExplicitAttemptBudget(t *testing.T) {
	var calls atomic.Int32
	maxAttempts := maxHedgeAttempts + 3
	result, err := runHedge(
		context.Background(),
		false,
		hedgePolicy{MaxAttempts: maxAttempts},
		func(context.Context, hedgeAttemptInfo) (int, error) {
			calls.Add(1)
			return 0, errors.New("retryable failure")
		},
		nil,
		hedgeHooks[int]{},
	)
	if !errors.Is(err, errHedgeExhausted) {
		t.Fatalf("err=%v, want exhausted", err)
	}
	if got := int(calls.Load()); got != maxAttempts || len(result.Attempts) != maxAttempts {
		t.Fatalf("calls=%d attempts=%d, want %d", got, len(result.Attempts), maxAttempts)
	}
}

func TestRunHedgeDelayedFallbackWinsAndCancelsPrimary(t *testing.T) {
	var calls atomic.Int32
	primaryCanceled := make(chan struct{})
	result, err := runHedge(
		context.Background(),
		true,
		hedgePolicy{Enabled: true, Delay: 10 * time.Millisecond, MaxParallel: 2, MaxAttempts: 3},
		func(ctx context.Context, info hedgeAttemptInfo) (int, error) {
			calls.Add(1)
			if info.Number == 1 {
				<-ctx.Done()
				close(primaryCanceled)
				return 0, ctx.Err()
			}
			return info.Number, nil
		},
		func(value int) (bool, error) { return value > 0, nil },
		hedgeHooks[int]{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Winner == nil || result.Winner.Info.Number != 2 || result.Value != 2 {
		t.Fatalf("winner=%+v value=%d, want attempt 2", result.Winner, result.Value)
	}
	if len(result.Attempts) != 2 || !result.Attempts[1].Info.Concurrent {
		t.Fatalf("attempts=%+v, want an overlapping auxiliary hedge", result.Attempts)
	}
	select {
	case <-primaryCanceled:
	case <-time.After(time.Second):
		t.Fatal("primary did not observe loser cancellation")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d, want 2", calls.Load())
	}
}

func TestRunHedgeDoesNotMarkFastSequentialFallbackAsConcurrent(t *testing.T) {
	result, err := runHedge(
		context.Background(),
		true,
		hedgePolicy{Enabled: true, Delay: time.Hour, MaxParallel: 2, MaxAttempts: 2},
		func(context.Context, hedgeAttemptInfo) (int, error) {
			return 0, errors.New("fast failure")
		},
		nil,
		hedgeHooks[int]{},
	)
	if !errors.Is(err, errHedgeExhausted) {
		t.Fatalf("err=%v, want exhausted", err)
	}
	if len(result.Attempts) != 2 {
		t.Fatalf("attempts=%d, want 2", len(result.Attempts))
	}
	if result.Attempts[1].Info.Kind != attemptKindHedge || result.Attempts[1].Info.Concurrent {
		t.Fatalf("fallback info=%+v, want hedge kind without overlap", result.Attempts[1].Info)
	}
}

func TestRunHedgeValidationRejectionRefillsImmediately(t *testing.T) {
	started := time.Now()
	result, err := runHedge(
		context.Background(),
		true,
		hedgePolicy{Enabled: true, Delay: time.Hour, MaxParallel: 2, MaxAttempts: 3},
		func(_ context.Context, info hedgeAttemptInfo) (int, error) { return info.Number, nil },
		func(value int) (bool, error) { return value == 2, nil },
		hedgeHooks[int]{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Winner == nil || result.Winner.Info.Number != 2 {
		t.Fatalf("winner=%+v, want attempt 2", result.Winner)
	}
	if time.Since(started) > time.Second {
		t.Fatal("rejected attempt did not refill the available slot immediately")
	}
	if len(result.Attempts) != 2 || result.Attempts[0].Outcome != hedgeOutcomeRejected {
		t.Fatalf("attempts=%+v", result.Attempts)
	}
}

func TestRunHedgeNeverExceedsMaxParallel(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	_, err := runHedge(
		context.Background(),
		true,
		hedgePolicy{Enabled: true, Delay: time.Millisecond, MaxParallel: 2, MaxAttempts: 4},
		func(ctx context.Context, info hedgeAttemptInfo) (int, error) {
			current := active.Add(1)
			defer active.Add(-1)
			for {
				previous := maxActive.Load()
				if current <= previous || maxActive.CompareAndSwap(previous, current) {
					break
				}
			}
			if info.Number == 2 {
				once.Do(func() { close(release) })
				return 2, nil
			}
			select {
			case <-release:
				<-ctx.Done()
				return 0, ctx.Err()
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		},
		func(value int) (bool, error) { return value == 2, nil },
		hedgeHooks[int]{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if maxActive.Load() > 2 {
		t.Fatalf("max active=%d, want <=2", maxActive.Load())
	}
}

func TestRunHedgeWaitsForCanceledLoserCleanup(t *testing.T) {
	originalCleanup := hedgeCleanupTimeout
	hedgeCleanupTimeout = time.Second
	t.Cleanup(func() { hedgeCleanupTimeout = originalCleanup })
	loserFinished := make(chan struct{})
	result, err := runHedge(
		context.Background(),
		true,
		hedgePolicy{Enabled: true, Delay: time.Millisecond, MaxParallel: 2, MaxAttempts: 2},
		func(ctx context.Context, info hedgeAttemptInfo) (int, error) {
			if info.Number == 2 {
				return 2, nil
			}
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			close(loserFinished)
			return 41, ctx.Err()
		},
		func(value int) (bool, error) { return value == 2, nil },
		hedgeHooks[int]{},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-loserFinished:
	default:
		t.Fatal("runHedge returned before the canceled loser finished cleanup")
	}
	if len(result.Attempts) != 2 || result.Attempts[0].Value != 41 || result.Attempts[0].Outcome != hedgeOutcomeCanceled {
		t.Fatalf("attempts=%+v, want real canceled loser result", result.Attempts)
	}
}

func TestHedgeEligibleExcludesGeneratedMediaAndRealtime(t *testing.T) {
	cases := []hedgeRequest{
		{Path: "/v1/images/generations", Model: "gpt-4o"},
		{Path: "/v1/chat/completions", Model: "gpt-image-1"},
		{Path: "/v1/responses", Body: []byte(`{"model":"gemini","modalities":["text","image"]}`)},
		{Path: "/v1/realtime", Model: "gpt-4o-realtime"},
		{Path: "/v1/chat/completions", Header: http.Header{"Upgrade": []string{"websocket"}}},
	}
	for _, request := range cases {
		if hedgeEligible(request) {
			t.Fatalf("request unexpectedly eligible: %+v", request)
		}
	}
	ordinaryMultimodal := hedgeRequest{
		Path:  "/v1/chat/completions",
		Model: "gpt-4o",
		Body:  []byte(`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example/a.png"}}]}]}`),
	}
	if !hedgeEligible(ordinaryMultimodal) {
		t.Fatal("ordinary image-input text request should remain hedge eligible")
	}
}

func TestMappedRouteContainsMediaModel(t *testing.T) {
	routes := []storage.GatewayRoute{
		{ModelMappingJSON: `{"friendly-text-model":"gpt-image-1"}`},
	}
	if !mappedRouteContainsMediaModel(routes, "friendly-text-model", nil) {
		t.Fatal("mapped image-generation model must disable concurrent hedging")
	}
	routes[0].ModelMappingJSON = `{"friendly-text-model":"sora-2"}`
	if !mappedRouteContainsMediaModel(routes, "friendly-text-model", nil) {
		t.Fatal("mapped video-generation model must disable concurrent hedging")
	}
	routes[0].ModelMappingJSON = `{"friendly-text-model":"gpt-5.6-terra"}`
	if mappedRouteContainsMediaModel(routes, "friendly-text-model", nil) {
		t.Fatal("mapped text model unexpectedly disabled concurrent hedging")
	}
}
