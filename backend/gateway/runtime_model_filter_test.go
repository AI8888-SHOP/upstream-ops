package gateway

import (
	"path/filepath"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestFilterRoutesForRequestedModelSkipsDisabledDirectProvider(t *testing.T) {
	db, err := storage.Open(storage.DBConfig{Driver: storage.DBDriverSQLite, Path: filepath.Join(t.TempDir(), "test.db")})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("runtime-model-filter-test")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	key, err := cipher.Encrypt("provider-key")
	if err != nil {
		t.Fatalf("encrypt provider key: %v", err)
	}
	disabled := &storage.GatewayProvider{
		Name: "disabled-provider-filter", BaseURL: "https://disabled.example",
		APIKeyCipher: key, Enabled: false, ModelPolicy: storage.GatewayProviderModelPolicyAll,
		AllowedModelsJSON: "[]",
	}
	enabled := &storage.GatewayProvider{
		Name: "enabled-provider-filter", BaseURL: "https://enabled.example",
		APIKeyCipher: key, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll,
		AllowedModelsJSON: "[]",
	}
	if err := providers.Create(disabled); err != nil {
		t.Fatalf("create disabled provider: %v", err)
	}
	// GatewayProvider.Enabled has a database default of true, so persist the
	// explicit disabled state instead of relying on a zero-value insert.
	if err := db.Model(&storage.GatewayProvider{}).Where("id = ?", disabled.ID).Update("enabled", false).Error; err != nil {
		t.Fatalf("disable provider: %v", err)
	}
	if err := providers.Create(enabled); err != nil {
		t.Fatalf("create enabled provider: %v", err)
	}

	rt := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil).runtime()
	rt.Providers = providers
	routes := []storage.GatewayRoute{
		{ID: 1, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: disabled.ID, Enabled: true},
		{ID: 2, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: enabled.ID, Enabled: true},
		{ID: 3, SourceChannelID: 99, Enabled: true, SourceAPIKeyCipher: "monitor-key"},
	}
	filtered, err := rt.filterRoutesForRequestedModel(routes, "", nil)
	if err != nil {
		t.Fatalf("filter routes: %v", err)
	}
	if len(filtered) != 2 || filtered[0].ID != 2 || filtered[1].ID != 3 {
		t.Fatalf("filtered routes = %+v, want enabled provider and monitor routes only", filtered)
	}
}
