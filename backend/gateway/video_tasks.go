package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const videoTaskBindingTTL = 7 * 24 * time.Hour

var errVideoTaskBinding = errors.New("accepted video task could not be bound")

// Only dedicated video endpoints get these exceptions. A chat model whose
// name contains "video" must retain all ordinary text response validation.
func isVideoEndpoint(path string) bool {
	return path == "/v1/videos" || path == "/videos" ||
		strings.HasPrefix(path, "/v1/videos/") || strings.HasPrefix(path, "/videos/")
}

func isVideoTaskCreate(path string) bool {
	switch strings.TrimPrefix(path, "/v1") {
	case "/videos", "/videos/generations", "/videos/edits", "/videos/extensions":
		return true
	}
	return false
}

func videoTaskLookupID(path string) string {
	if !strings.HasPrefix(path, "/v1/videos/") && !strings.HasPrefix(path, "/videos/") {
		return ""
	}
	id := strings.TrimPrefix(strings.TrimPrefix(path, "/v1"), "/videos/")
	id = strings.TrimSuffix(id, "/content")
	if !validVideoTaskID(id) {
		return ""
	}
	return id
}

func validVideoTaskID(id string) bool {
	if id == "" || len(id) > 256 || id == "." || id == ".." {
		return false
	}
	for _, c := range id {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' ||
			c == '-' || c == '_' || c == '.' || c == ':' {
			continue
		}
		return false
	}
	return true
}

func videoTaskIDFromResponse(body []byte) (string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(body, &object); err != nil || object == nil {
		return "", errors.New("video creation returned invalid JSON")
	}
	objects := []map[string]json.RawMessage{object}
	var data map[string]json.RawMessage
	if json.Unmarshal(object["data"], &data) == nil && data != nil {
		objects = append(objects, data)
	}
	for _, candidate := range objects {
		for _, name := range []string{"request_id", "task_id", "id"} {
			raw, exists := candidate[name]
			if !exists {
				continue
			}
			var id string
			if json.Unmarshal(raw, &id) != nil || !validVideoTaskID(id) {
				return "", errors.New("video creation returned an invalid task ID")
			}
			return id, nil
		}
	}
	for _, candidate := range objects {
		var status string
		_ = json.Unmarshal(candidate["status"], &status)
		switch strings.ToLower(strings.TrimSpace(status)) {
		case "pending", "queued", "running", "processing", "in_progress":
			return "", errors.New("asynchronous video creation returned no task ID")
		}
	}
	// Some compatible providers return a synchronous video result. They do
	// not have a task to bind; preserve the existing passthrough behavior.
	return "", nil
}

type videoTaskSnapshot struct {
	RouteID           uint
	GatewayGroupID    uint
	SourceChannelID   uint
	GatewayProviderID uint
	SourceGroupID     *int64
	SourceAPIKeyID    int64
	SourceAPIKeyName  string
	BaseURL           string
	APIKey            string
	AuthStyle         string
	ExtraHeadersJSON  string
	UserAgentOverride string
	UpstreamKind      protocolKind
	RequestedModel    string
	ResponseFormat    string
	DurationSeconds   float64
}

// Persist before publishing the accepted task ID. A storage failure must
// stop this submission, not create another paid task on a different route.
func (rt *Runtime) bindVideoTask(ctx context.Context, path string, status int, body []byte, key *storage.GatewayKey,
	group *storage.GatewayGroup, route *storage.GatewayRoute, target *upstreamTarget, kind protocolKind, model string, requestBody []byte) error {
	if !isVideoTaskCreate(path) || status < 200 || status >= 300 {
		return nil
	}
	taskID, err := videoTaskIDFromResponse(body)
	if err != nil || taskID == "" {
		return err
	}
	if rt.Routes == nil || rt.Cipher == nil || target == nil || key == nil || group == nil || route == nil {
		return errors.New("video task binding is not configured")
	}
	snapshot := videoTaskSnapshot{
		RouteID: route.ID, GatewayGroupID: group.ID, SourceGroupID: route.SourceGroupID,
		SourceAPIKeyID: route.SourceAPIKeyID, SourceAPIKeyName: route.SourceAPIKeyName,
		BaseURL: target.BaseURL, APIKey: target.APIKey, UserAgentOverride: target.UserAgentOverride,
		UpstreamKind: kind, RequestedModel: model,
	}
	if usesVideosAdapter(target, path) {
		snapshot.ResponseFormat = videoResponseGrok
		snapshot.DurationSeconds = videoRequestDuration(requestBody)
	}
	if target.Provider != nil {
		snapshot.GatewayProviderID = target.Provider.ID
		snapshot.AuthStyle = target.Provider.AuthStyle
		snapshot.ExtraHeadersJSON = target.Provider.ExtraHeadersJSON
	} else if target.Channel != nil {
		snapshot.SourceChannelID = target.Channel.ID
	}
	plain, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	ciphertext, err := rt.Cipher.Encrypt(string(plain))
	if err != nil {
		return err
	}
	// Ignore the client alias/route for idempotent submissions to the exact
	// same upstream account, but include auth headers that may select it.
	identity, _ := json.Marshal([]any{snapshot.SourceChannelID, snapshot.GatewayProviderID,
		snapshot.BaseURL, snapshot.APIKey, snapshot.AuthStyle, snapshot.ExtraHeadersJSON, snapshot.ResponseFormat})
	digest := sha256.Sum256(identity)
	now := time.Now()
	// Persist even if the client disconnected after the upstream accepted.
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := rt.Routes.SaveVideoTaskBinding(persistCtx, &storage.GatewayVideoTaskBinding{
		GatewayGroupID: group.ID, GatewayKeyID: key.ID, TaskID: taskID,
		TargetFingerprint: hex.EncodeToString(digest[:]), SnapshotCipher: ciphertext,
		CreatedAt: now, ExpiresAt: now.Add(videoTaskBindingTTL),
	}); err != nil {
		return err
	}
	if err := rt.Routes.PruneVideoTaskBindings(persistCtx, now); err != nil && rt.Log != nil {
		rt.Log.Warn("prune expired video task bindings failed", "err", err)
	}
	return nil
}

func (rt *Runtime) boundVideoTarget(snapshot videoTaskSnapshot) (*upstreamTarget, error) {
	target := &upstreamTarget{BaseURL: snapshot.BaseURL, APIKey: snapshot.APIKey, UserAgentOverride: snapshot.UserAgentOverride}
	route := &storage.GatewayRoute{ID: snapshot.RouteID, GatewayGroupID: snapshot.GatewayGroupID,
		SourceChannelID: snapshot.SourceChannelID, GatewayProviderID: snapshot.GatewayProviderID,
		SourceGroupID: snapshot.SourceGroupID, SourceAPIKeyID: snapshot.SourceAPIKeyID, SourceAPIKeyName: snapshot.SourceAPIKeyName}
	if snapshot.GatewayProviderID != 0 {
		if rt.Providers == nil {
			return nil, errors.New("video provider is unavailable")
		}
		provider, err := rt.Providers.FindByID(snapshot.GatewayProviderID)
		if err != nil || !provider.Enabled {
			return nil, errors.New("video provider is unavailable")
		}
		// Keep current concurrency/proxy controls, but never replace the
		// original credential/auth headers or address after an admin edit.
		p := *provider
		p.AuthStyle, p.ExtraHeadersJSON = snapshot.AuthStyle, snapshot.ExtraHeadersJSON
		target.Provider = &p
		route.SourceKind = storage.GatewayRouteSourceProvider
	} else {
		if rt.Channels == nil || snapshot.SourceChannelID == 0 {
			return nil, errors.New("video channel is unavailable")
		}
		channel, err := rt.Channels.FindByID(snapshot.SourceChannelID)
		if err != nil {
			return nil, errors.New("video channel is unavailable")
		}
		target.Channel = channel
	}
	target.AuditMetadata = routeAPIAuditMetadata(route)
	return target, nil
}

func (rt *Runtime) handleVideoTaskLookup(c *gin.Context, path string, key *storage.GatewayKey, group *storage.GatewayGroup) {
	taskID := videoTaskLookupID(path)
	if taskID == "" {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusNotFound, "not_found_error", "video task not found or expired")
		return
	}
	binding, err := rt.Routes.FindVideoTaskBinding(c.Request.Context(), group.ID, key.ID, taskID, time.Now())
	if errors.Is(err, gorm.ErrRecordNotFound) {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusNotFound, "not_found_error", "video task not found or expired")
		return
	}
	if err != nil || rt.Cipher == nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video task binding is unavailable")
		return
	}
	plain, err := rt.Cipher.Decrypt(binding.SnapshotCipher)
	var snapshot videoTaskSnapshot
	if err != nil || json.Unmarshal([]byte(plain), &snapshot) != nil || snapshot.BaseURL == "" || snapshot.APIKey == "" {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video task binding is unavailable")
		return
	}
	contentRequest := strings.HasSuffix(path, "/"+taskID+"/content")
	target, err := rt.boundVideoTarget(snapshot)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "original video upstream is unavailable")
		return
	}
	if contentRequest && snapshot.ResponseFormat != videoResponseGrok {
		_ = rt.Keys.TouchLastUsed(key.ID, time.Now())
		rt.serveNativeVideoContent(c, target, taskID, snapshot.UpstreamKind)
		return
	}
	validator, err := rt.responseValidatorForGroup(group)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video response validation is unavailable")
		return
	}
	validator = validator.forEndpoint(path)
	_ = rt.Keys.TouchLastUsed(key.ID, time.Now())
	status, headers, body, _, err := rt.forwardOnce(c.Request.Context(), c, target, "/v1/videos/"+taskID,
		http.MethodGet, c.Request.Header, nil, false, snapshot.UpstreamKind, 0)
	if err != nil || status < 200 || status >= 300 {
		code := http.StatusBadGateway
		if status == http.StatusNotFound {
			code = http.StatusNotFound
		}
		rt.writeGatewayError(c, protocol.KindOpenAIChat, code, "upstream_error", "original video upstream could not return the task")
		return
	}
	// A failed/expired video is a legitimate task status, not a text request
	// to retry. Explicit error envelopes without a task status remain blocked.
	var task struct {
		Status string `json:"status"`
		Data   struct {
			Status string `json:"status"`
		} `json:"data"`
	}
	_ = json.Unmarshal(body, &task)
	if task.Status == "" {
		task.Status = task.Data.Status
	}
	if (hasExplicitUpstreamError(body) && !isVideoTaskFailureStatus(task.Status)) ||
		validator.Validate(body, headers, string(protocol.KindOpenAIChat), snapshot.RequestedModel).IsRejected() {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video task response rejected")
		return
	}
	body, err = videoLookupClientBody(body, taskID, snapshot)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "invalid video task response from original upstream")
		return
	}
	if snapshot.ResponseFormat == videoResponseGrok {
		if contentRequest {
			rt.serveVideoTaskContent(c, target, body)
			return
		}
		body, err = videoContentProxyBody(body, taskID)
		if err != nil {
			rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video task response conversion failed")
			return
		}
		headers = adaptedVideoHeaders(headers, "")
	}
	rt.copyResponseHeaders(c.Writer.Header(), headers)
	c.Status(status)
	_, _ = c.Writer.Write(body)
	// Polls are reads of an existing task: no generation usage row, repeat
	// per-request charge, cache-health sample, or model cooldown recovery.
}

func isVideoTaskFailureStatus(status string) bool {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed", "expired", "canceled", "cancelled", "error":
		return true
	}
	return false
}

func videoBindingErrorInfo(err error) usageErrorInfo {
	return usageErrorInfo{Type: "config", Summary: "accepted video task could not be bound",
		Detail: fmt.Sprintf("video task binding failed; submission was not retried: %v", err)}
}
