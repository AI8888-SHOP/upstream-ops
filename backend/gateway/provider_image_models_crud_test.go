package gateway

import (
	"encoding/json"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestProviderImageGenerationModelsCRUDPersistence(t *testing.T) {
	db := openGatewayTestDB(t)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("test-provider-image-model-crud")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)

	created, err := svc.CreateProvider(CreateProviderInput{
		Name:                      "image-model-provider",
		BaseURL:                   "https://example.test",
		APIKey:                    "sk-image-model-provider",
		ImageGenerationModelsJSON: `[" Image-A ","image-a","Image-A","","custom-image"]`,
	})
	if err != nil {
		t.Fatalf("CreateProvider: %v", err)
	}

	// Read the database directly so a cached or returned object cannot hide a
	// missing column or a repository update that omitted this field.
	assertPersisted := func(want string) {
		t.Helper()
		var persisted storage.GatewayProvider
		if err := db.First(&persisted, created.ID).Error; err != nil {
			t.Fatalf("read persisted provider: %v", err)
		}
		if persisted.ImageGenerationModelsJSON != want {
			t.Fatalf("persisted image models=%q, want %q", persisted.ImageGenerationModelsJSON, want)
		}
		cached, err := providers.FindByID(created.ID)
		if err != nil {
			t.Fatalf("FindByID: %v", err)
		}
		if cached.ImageGenerationModelsJSON != want {
			t.Fatalf("cached image models=%q, want %q", cached.ImageGenerationModelsJSON, want)
		}
		page, err := svc.ListProviders(storage.GatewayProviderQuery{Q: "image-model-provider"})
		if err != nil {
			t.Fatalf("ListProviders: %v", err)
		}
		if len(page.Items) != 1 || page.Items[0].ImageGenerationModelsJSON != want {
			t.Fatalf("listed providers=%+v, want image models=%q", page.Items, want)
		}
		encoded, err := json.Marshal(page.Items[0])
		if err != nil {
			t.Fatalf("marshal provider response: %v", err)
		}
		var response map[string]any
		if err := json.Unmarshal(encoded, &response); err != nil {
			t.Fatalf("decode provider response: %v", err)
		}
		if response["image_generation_models_json"] != want {
			t.Fatalf("response image models=%v, want %q", response["image_generation_models_json"], want)
		}
	}
	assertPersisted(`["Image-A","image-a","custom-image"]`)

	notes := "unrelated update preserves image models"
	if _, err := svc.UpdateProvider(created.ID, UpdateProviderInput{Notes: &notes}); err != nil {
		t.Fatalf("update without image models: %v", err)
	}
	assertPersisted(`["Image-A","image-a","custom-image"]`)

	models := `[" mapped-model ","mapped-model","second-model"]`
	if _, err := svc.UpdateProvider(created.ID, UpdateProviderInput{ImageGenerationModelsJSON: &models}); err != nil {
		t.Fatalf("update image models: %v", err)
	}
	assertPersisted(`["mapped-model","second-model"]`)

	for _, bad := range []string{`{}`, `[1]`, `"model"`, `["unterminated"`} {
		if _, err := svc.UpdateProvider(created.ID, UpdateProviderInput{ImageGenerationModelsJSON: &bad}); err == nil {
			t.Fatalf("invalid image models %q should fail", bad)
		}
		assertPersisted(`["mapped-model","second-model"]`)
	}

	for _, empty := range []string{"", "   ", "[]"} {
		if _, err := svc.UpdateProvider(created.ID, UpdateProviderInput{ImageGenerationModelsJSON: &empty}); err != nil {
			t.Fatalf("clear image models with %q: %v", empty, err)
		}
		assertPersisted(`[]`)
	}

	if err := svc.DeleteProvider(created.ID); err != nil {
		t.Fatalf("DeleteProvider: %v", err)
	}
	if _, err := providers.FindByID(created.ID); err == nil {
		t.Fatal("deleted provider remains readable")
	}
}

func TestProviderImageGenerationModelsCreateDefaultsAndValidation(t *testing.T) {
	db := openGatewayTestDB(t)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("test-provider-image-model-default")
	if err != nil {
		t.Fatalf("cipher: %v", err)
	}
	svc := NewService(nil, nil, nil, nil, nil, nil, nil, cipher, nil)
	svc.SetProviders(providers)
	input := CreateProviderInput{
		Name:    "default-image-model-provider",
		BaseURL: "https://example.test",
		APIKey:  "sk-image-model-provider",
	}
	created, err := svc.CreateProvider(input)
	if err != nil {
		t.Fatalf("CreateProvider: %v", err)
	}
	var persisted storage.GatewayProvider
	if err := db.First(&persisted, created.ID).Error; err != nil {
		t.Fatalf("read default provider: %v", err)
	}
	if persisted.ImageGenerationModelsJSON != "[]" {
		t.Fatalf("default image models=%q, want []", persisted.ImageGenerationModelsJSON)
	}

	input.Name = "invalid-image-model-provider"
	input.ImageGenerationModelsJSON = `{"model":"image-a"}`
	if _, err := svc.CreateProvider(input); err == nil {
		t.Fatal("create with invalid image models should fail")
	}
	if _, err := providers.FindByName(input.Name); err == nil {
		t.Fatal("invalid provider was persisted")
	}
}
