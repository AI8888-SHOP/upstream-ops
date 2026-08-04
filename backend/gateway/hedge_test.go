package gateway

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
	select {
	case <-primaryCanceled:
	case <-time.After(time.Second):
		t.Fatal("primary did not observe loser cancellation")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d, want 2", calls.Load())
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
