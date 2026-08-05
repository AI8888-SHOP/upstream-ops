package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	appcrypto "github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestGatewayGroupCloneAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	db := openTestDB(t)
	groups := storage.NewGatewayGroups(db)
	keys := storage.NewGatewayKeys(db)
	routes := storage.NewGatewayRoutes(db)
	rules := storage.NewGatewayResponseRules(db)
	source := &storage.GatewayGroup{
		Name:              "api-clone-source",
		Description:       "source",
		ModelMappingJSON:  `{"alias":"upstream"}`,
		ModelsJSON:        `["model-a"]`,
		ModelsMode:        storage.GatewayModelsModeManual,
		HedgeMaxParallel:  4,
		RetryEnabled:      true,
		ResponseValidationEnabled: true,
	}
	if err := groups.Create(source); err != nil {
		t.Fatalf("create source group: %v", err)
	}
	cipher, err := appcrypto.NewCipher("clone-api-test-secret")
	if err != nil {
		t.Fatalf("new cipher: %v", err)
	}
	originalSecret := "sk-original-client-secret"
	originalCipher, err := cipher.Encrypt(originalSecret)
	if err != nil {
		t.Fatalf("encrypt source key: %v", err)
	}
	originalHash := sha256.Sum256([]byte(originalSecret))
	originalKey := &storage.GatewayKey{
		GroupID:         source.ID,
		Name:            "api-client",
		KeyHash:         hex.EncodeToString(originalHash[:]),
		KeyPrefix:       gateway.KeyPrefix(originalSecret),
		KeyCipher:       originalCipher,
		Status:          storage.GatewayKeyStatusActive,
		Quota:           100,
		QuotaUsed:       77,
		IPWhitelistJSON: `["127.0.0.1"]`,
	}
	if err := keys.Create(originalKey); err != nil {
		t.Fatalf("create source key: %v", err)
	}
	if err := routes.SaveForGroup(source.ID, []storage.GatewayRoute{{
		SourceKind:                  storage.GatewayRouteSourceProvider,
		GatewayProviderID:           8,
		ModelMappingJSON:            `{"model-a":"provider-a"}`,
		RateLimitAutoDisabled:       true,
		RateLimitAutoDisabledReason: "source cooldown",
		TempUnschedulableReason:     "capacity",
		RecoverSuccessStreak:        1,
	}}); err != nil {
		t.Fatalf("create source route: %v", err)
	}
	if err := rules.Create(&storage.GatewayResponseRule{
		GatewayGroupID: source.ID,
		Name:           "capacity",
		Enabled:        true,
		Priority:       2,
		Pattern:        `(?i)selected model is at capacity`,
		Target:         storage.GatewayResponseRuleTargetAssistantText,
		ModelsJSON:     `[]`,
		ProtocolsJSON:  `["openai"]`,
	}); err != nil {
		t.Fatalf("create source rule: %v", err)
	}

	svc := gateway.NewService(
		groups,
		keys,
		routes,
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		cipher,
		nil,
	)
	r := gin.New()
	registerGatewayAdmin(r.Group("/api"), &Deps{DB: db, Gateway: svc, GatewayResponseRules: rules})

	created := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/clone", source.ID),
		strings.NewReader(`{"name":"api-clone"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(created, request)
	if created.Code != http.StatusCreated {
		t.Fatalf("clone status = %d, body=%s", created.Code, created.Body.String())
	}
	var response struct {
		Group storage.GatewayGroup `json:"group"`
		Keys  []struct {
			Key    storage.GatewayKey `json:"key"`
			Secret string             `json:"secret"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(created.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode clone response: %v", err)
	}
	if response.Group.Name != "api-clone" || response.Group.ID == source.ID || len(response.Keys) != 1 {
		t.Fatalf("clone response = %#v", response)
	}
	secret := response.Keys[0].Secret
	if secret == "" || response.Keys[0].Key.QuotaUsed != 0 || response.Keys[0].Key.Name != "api-client (copy)" {
		t.Fatalf("clone key response = %#v", response.Keys[0])
	}
	if strings.Contains(created.Body.String(), "key_hash") || strings.Contains(created.Body.String(), "key_cipher") {
		t.Fatalf("clone response leaked sensitive field: %s", created.Body.String())
	}
	clonedKeys, err := keys.ListByGroupID(response.Group.ID)
	if err != nil || len(clonedKeys) != 1 {
		t.Fatalf("list cloned keys: %v %#v", err, clonedKeys)
	}
	if gateway.HashAPIKey(secret) == originalKey.KeyHash || gateway.HashAPIKey(secret) != clonedKeys[0].KeyHash {
		t.Fatalf("clone secret hash = %q, key = %#v", gateway.HashAPIKey(secret), clonedKeys[0])
	}
	decrypted, err := cipher.Decrypt(clonedKeys[0].KeyCipher)
	if err != nil || decrypted != secret {
		t.Fatalf("clone key ciphertext does not decrypt to returned secret: %q %v", decrypted, err)
	}

	clonedRoutes, err := routes.ListByGroupID(response.Group.ID)
	if err != nil || len(clonedRoutes) != 1 {
		t.Fatalf("list cloned routes: %v %#v", err, clonedRoutes)
	}
	if clonedRoutes[0].ModelMappingJSON != `{"model-a":"provider-a"}` || clonedRoutes[0].RateLimitAutoDisabled ||
		clonedRoutes[0].TempUnschedulableReason != "" || clonedRoutes[0].RecoverSuccessStreak != 0 {
		t.Fatalf("cloned route runtime/config = %#v", clonedRoutes[0])
	}
	clonedRules, err := rules.ListByGroupID(response.Group.ID)
	if err != nil || len(clonedRules) != 1 || clonedRules[0].Pattern != `(?i)selected model is at capacity` {
		t.Fatalf("cloned rules: %v %#v", err, clonedRules)
	}

	second := httptest.NewRecorder()
	request = httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/api/gateway/groups/%d/clone", source.ID),
		strings.NewReader(`{"name":"api-clone"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(second, request)
	if second.Code != http.StatusCreated || !strings.Contains(second.Body.String(), `"name":"api-clone (2)"`) ||
		!strings.Contains(second.Body.String(), `"name":"api-client (copy) (2)"`) {
		t.Fatalf("second clone status = %d, body=%s", second.Code, second.Body.String())
	}

	notFound := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/gateway/groups/999999/clone", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(notFound, request)
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("missing source clone status = %d, body=%s", notFound.Code, notFound.Body.String())
	}
}
