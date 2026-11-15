// 数据面：网关密钥鉴权。
package gateway

import (
	"errors"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Authenticate 鉴权并返回 AuthResult。
func (rt *Runtime) Authenticate(c *gin.Context) (*AuthResult, error) {
	raw := strings.TrimSpace(c.GetHeader("Authorization"))
	if strings.HasPrefix(strings.ToLower(raw), "bearer ") {
		raw = strings.TrimSpace(raw[7:])
	} else {
		raw = strings.TrimSpace(c.GetHeader("x-api-key"))
	}
	if raw == "" {
		return nil, errors.New("missing api key")
	}
	key, err := rt.Keys.FindByHash(HashAPIKey(raw))
	if err != nil {
		return nil, errors.New("invalid api key")
	}
	if key.Status != storage.GatewayKeyStatusActive {
		return nil, errors.New("api key disabled")
	}
	if err := rt.checkIPAccess(c.ClientIP(), key.IPWhitelistJSON, key.IPBlacklistJSON); err != nil {
		return nil, err
	}
	// Reading an already accepted video must remain possible after its
	// creation uses the last credit. Ownership is checked against the task
	// binding before any upstream call; disabled keys/groups remain blocked.
	videoLookup := c.Request.Method == http.MethodGet && videoTaskLookupID(c.Request.URL.Path) != ""
	if key.Quota > 0 && key.QuotaUsed >= key.Quota && !videoLookup {
		return nil, errors.New("api key quota exceeded")
	}
	group, err := rt.Groups.FindByID(key.GroupID)
	if err != nil {
		return nil, errors.New("gateway group not found")
	}
	if group.Status != storage.GatewayGroupStatusActive {
		return nil, errors.New("gateway group disabled")
	}
	return &AuthResult{Key: key, Group: group}, nil
}
