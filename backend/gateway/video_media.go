package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Uploads have no model and no generation charge. A single enabled route is
// unambiguous; groups with multiple routes must explicitly name their route.
// Never upload a user's file speculatively to multiple upstream accounts.
func (rt *Runtime) videoMediaTarget(group *storage.GatewayGroup, routeID string) (*upstreamTarget, error) {
	if rt.Routes == nil || rt.Cipher == nil {
		return nil, errors.New("video upload is not configured")
	}
	var selected uint64
	if routeID != "" {
		id, err := strconv.ParseUint(routeID, 10, 64)
		if err != nil || id == 0 {
			return nil, errors.New("invalid upload route_id")
		}
		selected = id
	}
	routes, err := rt.Routes.ListByGroupID(group.ID)
	if err != nil {
		return nil, errors.New("video upload routes are unavailable")
	}
	var target *upstreamTarget
	for _, route := range routes {
		if !route.Enabled || (selected != 0 && uint64(route.ID) != selected) {
			continue
		}
		candidate, err := rt.resolveUpstreamTarget(&route)
		if err != nil {
			continue
		}
		if target != nil {
			return nil, errors.New("multiple upload routes; specify route_id from this gateway group")
		}
		rt.applyRouteUserAgent(candidate, group, &route)
		target = candidate
	}
	if target == nil {
		return nil, errors.New("no available upload route in this gateway group")
	}
	return target, nil
}

func (rt *Runtime) handleVideoMediaUpload(c *gin.Context, key *storage.GatewayKey, group *storage.GatewayGroup) {
	if c.Request.Method != http.MethodPost {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusMethodNotAllowed, "invalid_request_error", "media upload requires POST")
		return
	}
	mediaType, params, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadRequest, "invalid_request_error", "media upload requires multipart/form-data with one file field")
		return
	}
	target, err := rt.videoMediaTarget(group, c.Query("route_id"))
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadRequest, "invalid_request_error", err.Error())
		return
	}
	validator, err := rt.responseValidatorForGroup(group)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video upload validation is unavailable")
		return
	}
	validator = validator.forEndpoint("/v1/videos")
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Minute)
	defer cancel()
	release, err := rt.acquireUpstreamConcurrencyForAttempt(ctx, target, time.Now(), 0)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "upstream_error", "video upload is unavailable")
		return
	}
	defer release()
	// Large MP4/audio references are spooled to a private temporary file.
	// Retain the exact multipart bytes and remove the file on every exit.
	file, err := os.CreateTemp("", "upstream-ops-media-*")
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video upload storage is unavailable")
		return
	}
	defer func() { _ = file.Close(); _ = os.Remove(file.Name()) }()
	size, err := io.Copy(file, http.MaxBytesReader(c.Writer, c.Request.Body, videoRequestBodyLimit))
	if err != nil {
		status := http.StatusBadRequest
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
		}
		rt.writeGatewayError(c, protocol.KindOpenAIChat, status, "invalid_request_error", "invalid media upload or body exceeds 256 MiB")
		return
	}
	if _, err = file.Seek(0, io.SeekStart); err == nil {
		err = validateVideoMediaFile(file, params["boundary"])
	}
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadRequest, "invalid_request_error", "media upload requires exactly one non-empty file field")
		return
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusServiceUnavailable, "api_error", "video upload storage is unavailable")
		return
	}
	req, err := rt.buildUpstreamHTTPRequest(gatewayAPICallContext(ctx, c), target, "/v1/media", http.MethodPost,
		c.Request.Header, nil, protocol.KindOpenAIChat, false)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video upload is unavailable")
		return
	}
	req.Body, req.ContentLength, req.GetBody = io.NopCloser(file), size, nil
	req.Header.Set("Accept", "application/json")
	_ = rt.Keys.TouchLastUsed(key.ID, time.Now())
	resp, err := rt.videoUpstreamClient(target).Do(req)
	if err != nil {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video upload failed")
		return
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, (1<<20)+1))
	if resp.StatusCode == http.StatusRequestEntityTooLarge {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusRequestEntityTooLarge, "invalid_request_error", "media exceeds upstream upload limit")
		return
	}
	if err != nil || len(body) > 1<<20 || resp.StatusCode < 200 || resp.StatusCode >= 300 ||
		hasExplicitUpstreamError(body) || validator.Validate(body, resp.Header, string(protocol.KindOpenAIChat), "").IsRejected() {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video upload failed or response rejected")
		return
	}
	var result struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(body, &result) != nil || !validVideoMediaURL(result.URL) {
		rt.writeGatewayError(c, protocol.KindOpenAIChat, http.StatusBadGateway, "upstream_error", "video upload returned no valid reference URL")
		return
	}
	c.Header("Cache-Control", "private, no-store")
	c.Data(resp.StatusCode, "application/json", body)
	// No generation usage row, key debit, model cooldown or automatic retry.
}

func validVideoMediaURL(raw string) bool {
	u, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && u.Hostname() != "" && u.User == nil && u.Fragment == "" && (u.Scheme == "https" || u.Scheme == "http")
}

func validateVideoMediaFile(src io.Reader, boundary string) error {
	reader := multipart.NewReader(src, boundary)
	part, err := reader.NextRawPart()
	if err != nil {
		return err
	}
	if part.FormName() != "file" || part.FileName() == "" {
		return errors.New("missing media file")
	}
	size, err := io.Copy(io.Discard, part)
	if err != nil || size == 0 {
		return errors.New("empty or invalid media file")
	}
	if _, err := reader.NextRawPart(); err != io.EOF {
		return errors.New("media upload must contain exactly one file")
	}
	return nil
}
