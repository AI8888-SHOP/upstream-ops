package gateway

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

type ensureKeyChannelAPI struct {
	mu          sync.Mutex
	keys        []connector.APIKey
	createCount int
	searchMiss  bool
}

func (f *ensureKeyChannelAPI) ListAPIKeys(_ context.Context, _ uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if strings.TrimSpace(query.Search) != "" && f.searchMiss {
		return &connector.APIKeyPage{Page: 1, PageSize: query.PageSize}, nil
	}
	items := append([]connector.APIKey(nil), f.keys...)
	if query.Search != "" {
		filtered := items[:0]
		for _, key := range items {
			if strings.Contains(key.Name, query.Search) {
				filtered = append(filtered, key)
			}
		}
		items = filtered
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	pageSize := query.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	page := query.Page
	if page <= 0 {
		page = 1
	}
	start := (page - 1) * pageSize
	if start >= len(items) {
		return &connector.APIKeyPage{Total: int64(len(items)), Page: page, PageSize: pageSize, Pages: (len(items) + pageSize - 1) / pageSize}, nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	return &connector.APIKeyPage{
		Items: items[start:end], Total: int64(len(items)), Page: page, PageSize: pageSize,
		Pages: (len(items) + pageSize - 1) / pageSize,
	}, nil
}

func (f *ensureKeyChannelAPI) ListAPIKeyGroups(context.Context, uint) ([]connector.APIKeyGroup, error) {
	return nil, nil
}

func (f *ensureKeyChannelAPI) CreateAPIKey(_ context.Context, _ uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createCount++
	key := connector.APIKey{ID: int64(len(f.keys) + 1), Name: req.Name, Key: fmt.Sprintf("secret-%d", len(f.keys)+1), GroupID: req.GroupID}
	f.keys = append(f.keys, key)
	return &key, nil
}

func (f *ensureKeyChannelAPI) UpdateAPIKey(_ context.Context, _ uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i := range f.keys {
		if f.keys[i].ID != keyID {
			continue
		}
		if req.Name != nil {
			f.keys[i].Name = *req.Name
		}
		f.keys[i].GroupID = req.GroupID
		return &f.keys[i], nil
	}
	return nil, fmt.Errorf("key %d not found", keyID)
}

func (f *ensureKeyChannelAPI) RevealAPIKey(_ context.Context, _ uint, keyID int64) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, key := range f.keys {
		if key.ID == keyID {
			return key.Key, nil
		}
	}
	return "", fmt.Errorf("key %d not found", keyID)
}

func TestEnsureRouteKeysReusesOneManagedKeyAcrossGatewayGroups(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "ensure-key-source", Type: storage.ChannelTypeNewAPI, SiteURL: "https://source.invalid", Username: "owner"}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	remoteGroupID := int64(42)
	input := []storage.GatewayRoute{{SourceChannelID: channel.ID, SourceGroupID: &remoteGroupID, SourceGroupName: "premium", Enabled: true}}
	if err := routes.SaveForGroup(601, input); err != nil {
		t.Fatalf("save first group route: %v", err)
	}
	if err := routes.SaveForGroup(602, input); err != nil {
		t.Fatalf("save second group route: %v", err)
	}
	cipher, err := crypto.NewCipher("test-secret")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	upstream := &ensureKeyChannelAPI{}
	svc := NewService(nil, nil, routes, nil, nil, channels, upstream, cipher, nil)

	var wg sync.WaitGroup
	for _, groupID := range []uint{601, 602} {
		groupID := groupID
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, ensureErr := svc.EnsureRouteKeys(context.Background(), groupID)
			if ensureErr != nil {
				t.Errorf("ensure group %d: %v", groupID, ensureErr)
				return
			}
			if result.OKCount != 1 || result.FailCount != 0 {
				t.Errorf("ensure group %d result: %+v", groupID, result)
			}
		}()
	}
	wg.Wait()

	first, err := routes.ListByGroupID(601)
	if err != nil || len(first) != 1 {
		t.Fatalf("list first route: err=%v count=%d", err, len(first))
	}
	second, err := routes.ListByGroupID(602)
	if err != nil || len(second) != 1 {
		t.Fatalf("list second route: err=%v count=%d", err, len(second))
	}
	if upstream.createCount != 1 {
		t.Fatalf("managed key created %d times, want one", upstream.createCount)
	}
	if first[0].SourceAPIKeyID == 0 || first[0].SourceAPIKeyID != second[0].SourceAPIKeyID {
		t.Fatalf("routes did not bind the same upstream key: first=%d second=%d", first[0].SourceAPIKeyID, second[0].SourceAPIKeyID)
	}
}

func TestEnsureRouteKeysFindsManagedKeyBeyondFirstPage(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	channels := storage.NewChannels(db)
	channel := &storage.Channel{Name: "ensure-key-pagination", Type: storage.ChannelTypeSub2API, SiteURL: "https://source.invalid", Username: "owner"}
	if err := channels.Create(channel); err != nil {
		t.Fatalf("create source channel: %v", err)
	}
	remoteGroupID := int64(88)
	if err := routes.SaveForGroup(603, []storage.GatewayRoute{{SourceChannelID: channel.ID, SourceGroupID: &remoteGroupID, SourceGroupName: "bulk", Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	upstream := &ensureKeyChannelAPI{searchMiss: true}
	for i := 0; i < 100; i++ {
		upstream.keys = append(upstream.keys, connector.APIKey{ID: int64(i + 1), Name: fmt.Sprintf("unmanaged-%03d", i), Key: "x"})
	}
	upstream.keys = append(upstream.keys, connector.APIKey{ID: 101, Name: fmt.Sprintf("uops-ch%d-sg88", channel.ID), Key: "existing"})
	cipher, _ := crypto.NewCipher("test-secret")
	svc := NewService(nil, nil, routes, nil, nil, channels, upstream, cipher, nil)
	result, err := svc.EnsureRouteKeys(context.Background(), 603)
	if err != nil {
		t.Fatalf("ensure route key: %v", err)
	}
	if result.OKCount != 1 || upstream.createCount != 0 {
		t.Fatalf("pagination lookup created a duplicate: result=%+v creates=%d", result, upstream.createCount)
	}
	items, err := routes.ListByGroupID(603)
	if err != nil || len(items) != 1 || items[0].SourceAPIKeyID != 101 {
		t.Fatalf("existing key beyond first page was not reused: err=%v items=%+v", err, items)
	}
}
