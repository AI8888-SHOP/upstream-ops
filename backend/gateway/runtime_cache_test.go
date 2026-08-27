package gateway

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

type blockingChannelGroupsAPI struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
	groups  []connector.APIKeyGroup
}

func (f *blockingChannelGroupsAPI) ListAPIKeys(context.Context, uint, connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	return &connector.APIKeyPage{}, nil
}

func (f *blockingChannelGroupsAPI) ListAPIKeyGroups(ctx context.Context, _ uint) ([]connector.APIKeyGroup, error) {
	f.calls.Add(1)
	f.once.Do(func() { close(f.started) })
	select {
	case <-f.release:
		return f.groups, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (f *blockingChannelGroupsAPI) CreateAPIKey(context.Context, uint, connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	return nil, nil
}

func (f *blockingChannelGroupsAPI) UpdateAPIKey(context.Context, uint, int64, connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	return nil, nil
}

func (f *blockingChannelGroupsAPI) RevealAPIKey(context.Context, uint, int64) (string, error) {
	return "", nil
}

func TestLoadGroupsByChannelCoalescesConcurrentColdMisses(t *testing.T) {
	groupID := int64(7)
	api := &blockingChannelGroupsAPI{
		started: make(chan struct{}),
		release: make(chan struct{}),
		groups:  []connector.APIKeyGroup{{ID: &groupID, Name: "shared"}},
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, api, nil, nil)
	route := storage.GatewayRoute{
		SourceKind:      storage.GatewayRouteSourceMonitor,
		SourceChannelID: 42,
	}

	const callers = 32
	ready := sync.WaitGroup{}
	ready.Add(callers)
	start := make(chan struct{})
	done := sync.WaitGroup{}
	done.Add(callers)
	results := make(chan map[uint][]connector.APIKeyGroup, callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			results <- svc.Runtime.loadGroupsByChannel(context.Background(), []storage.GatewayRoute{route})
		}()
	}
	ready.Wait()
	close(start)

	select {
	case <-api.started:
	case <-time.After(time.Second):
		t.Fatal("source-group fetch did not start")
	}
	// Keep the first fetch blocked long enough for all callers to reach the
	// shared in-flight entry. Without coalescing, the call count rises here.
	time.Sleep(100 * time.Millisecond)
	if got := api.calls.Load(); got != 1 {
		t.Fatalf("ListAPIKeyGroups calls while fetch blocked = %d, want 1", got)
	}
	close(api.release)
	done.Wait()
	close(results)

	for result := range results {
		groups := result[42]
		if len(groups) != 1 || groups[0].ID == nil || *groups[0].ID != groupID {
			t.Fatalf("groups = %#v, want shared fetched result", groups)
		}
	}
	if got := api.calls.Load(); got != 1 {
		t.Fatalf("ListAPIKeyGroups total calls = %d, want 1", got)
	}
}
