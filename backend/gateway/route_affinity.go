package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// Route affinity is deliberately short lived. It protects prompt-cache
	// continuity without permanently pinning a key to one upstream.
	routeAffinityTTL             = 45 * time.Minute
	routeAffinityProbeHold       = 15 * time.Minute
	routeAffinityCleanupInterval = time.Minute
	maxRouteAffinityEntries      = 4096
	maxRouteAffinityPrefixes     = 64

	requestRouteFailureTTL     = 5 * time.Minute
	maxRequestRouteFailureKeys = 4096
)

type routeAffinityKey struct {
	GatewayKeyID uint
	GroupID      uint
	Protocol     string
	Model        string
	Fingerprint  string
}

type routeAffinityEntry struct {
	RouteID              uint
	ExpiresAt            time.Time
	ProbeUntil           time.Time
	RecoveryBlockedUntil time.Time
}

// routeAffinityContext is request-local. The service map stores only hashed
// identifiers and the last route; request bodies are never retained.
type routeAffinityContext struct {
	Keys                  []routeAffinityKey
	LookupKey             routeAffinityKey
	RetryKeys             []routeAffinityKey
	PreferredRouteID      uint
	RecoveryRouteID       uint
	Recovery              bool
	PreservePreferred     bool
	RecoveryCooldownUntil time.Time
}

func (a routeAffinityContext) enabled() bool {
	return len(a.Keys) > 0 && a.PreferredRouteID > 0
}

func (rt *Runtime) routeAffinityForRequest(c *gin.Context, keyID, groupID uint, protocolName, model string, body []byte) routeAffinityContext {
	return rt.routeAffinityForAnalyzedRequest(c, keyID, groupID, protocolName, model, body, "")
}

func (rt *Runtime) routeAffinityForAnalyzedRequest(c *gin.Context, keyID, groupID uint, protocolName, model string, body []byte, affinityID string) routeAffinityContext {
	if rt == nil || rt.Service == nil || keyID == 0 || groupID == 0 {
		return routeAffinityContext{}
	}
	ctx := routeAffinityContext{
		RetryKeys: requestRouteFailureKeys(c, body, keyID, groupID, protocolName, model),
	}
	fingerprints := requestRouteAffinityFingerprintsWithID(c, body, model, affinityID)
	if len(fingerprints) == 0 {
		return ctx
	}
	keys := make([]routeAffinityKey, 0, len(fingerprints))
	for _, fingerprint := range fingerprints {
		keys = append(keys, routeAffinityKey{
			GatewayKeyID: keyID,
			GroupID:      groupID,
			Protocol:     strings.ToLower(strings.TrimSpace(protocolName)),
			Model:        strings.TrimSpace(model),
			Fingerprint:  fingerprint,
		})
	}
	ctx.Keys = keys
	ctx.PreferredRouteID, ctx.LookupKey = rt.lookupRouteAffinity(keys, time.Now())
	return ctx
}

func requestRouteFailureKeys(c *gin.Context, body []byte, keyID, groupID uint, protocolName, model string) []routeAffinityKey {
	if keyID == 0 || groupID == 0 || len(body) == 0 {
		return nil
	}
	fingerprints := make([]string, 0, 4)
	scope := strings.ToLower(strings.TrimSpace(protocolName))
	var stableIDs []string
	if c == nil || c.Request == nil {
		return nil
	}
	if c.Request.URL != nil {
		scope += " " + strings.ToUpper(c.Request.Method) + " " + c.Request.URL.Path
	}
	for _, header := range []string{"Idempotency-Key", "X-Request-Id", "Request-Id", "X-Client-Request-Id", "X-Openai-Request-Id", "X-Correlation-Id"} {
		value := strings.TrimSpace(c.GetHeader(header))
		if value != "" && len(value) <= 512 {
			stableIDs = append(stableIDs, "header:"+strings.ToLower(header)+"\x00"+value)
		}
	}
	if len(stableIDs) == 0 {
		return nil
	}
	bodyHash := sha256.New()
	_, _ = bodyHash.Write([]byte(scope + "\x00" + strings.TrimSpace(model) + "\x00"))
	_, _ = bodyHash.Write(body)
	bodyFingerprint := hex.EncodeToString(bodyHash.Sum(nil))
	for _, stableID := range stableIDs {
		fingerprints = appendUniqueString(fingerprints, routeAffinityDigest(stableID+"\x00"+bodyFingerprint))
	}
	keys := make([]routeAffinityKey, 0, len(fingerprints))
	for _, fingerprint := range fingerprints {
		keys = append(keys, routeAffinityKey{
			GatewayKeyID: keyID,
			GroupID:      groupID,
			Protocol:     scope,
			Model:        strings.TrimSpace(model),
			Fingerprint:  fingerprint,
		})
	}
	return keys
}

func (s *Service) lookupRequestRouteFailures(keys []routeAffinityKey, now time.Time) map[uint]struct{} {
	if s == nil || len(keys) == 0 {
		return nil
	}
	s.requestRouteFailureMu.Lock()
	defer s.requestRouteFailureMu.Unlock()
	if s.requestRouteFailures == nil {
		s.requestRouteFailures = make(map[routeAffinityKey]map[uint]time.Time)
	}
	failed := make(map[uint]struct{})
	for _, key := range keys {
		routes := s.requestRouteFailures[key]
		for routeID, until := range routes {
			if !until.After(now) {
				delete(routes, routeID)
				continue
			}
			failed[routeID] = struct{}{}
		}
		if len(routes) == 0 {
			delete(s.requestRouteFailures, key)
		}
	}
	return failed
}

func (s *Service) rememberRequestRouteFailure(keys []routeAffinityKey, routeID uint, now time.Time) {
	if s == nil || len(keys) == 0 || routeID == 0 {
		return
	}
	s.requestRouteFailureMu.Lock()
	defer s.requestRouteFailureMu.Unlock()
	if s.requestRouteFailures == nil {
		s.requestRouteFailures = make(map[routeAffinityKey]map[uint]time.Time)
	}
	for key, routes := range s.requestRouteFailures {
		for id, until := range routes {
			if !until.After(now) {
				delete(routes, id)
			}
		}
		if len(routes) == 0 {
			delete(s.requestRouteFailures, key)
		}
	}
	for _, key := range keys {
		routes := s.requestRouteFailures[key]
		if routes == nil {
			routes = make(map[uint]time.Time)
			s.requestRouteFailures[key] = routes
		}
		routes[routeID] = now.Add(requestRouteFailureTTL)
	}
	for len(s.requestRouteFailures) > maxRequestRouteFailureKeys {
		for key := range s.requestRouteFailures {
			delete(s.requestRouteFailures, key)
			break
		}
	}
}

func (rt *Runtime) filterPreviouslyFailedRequestRoutes(candidates []ScoredRoute, affinity *routeAffinityContext) []ScoredRoute {
	if rt == nil || rt.Service == nil || affinity == nil || len(affinity.RetryKeys) == 0 {
		return candidates
	}
	failedRoutes := rt.Service.lookupRequestRouteFailures(affinity.RetryKeys, time.Now())
	if len(failedRoutes) == 0 {
		return candidates
	}
	filtered := make([]ScoredRoute, 0, len(candidates))
	fallback := make([]ScoredRoute, 0, len(candidates))
	for _, candidate := range candidates {
		if _, failed := failedRoutes[candidate.Route.ID]; failed {
			fallback = append(fallback, candidate)
		} else {
			filtered = append(filtered, candidate)
		}
	}
	if len(filtered) == 0 {
		return fallback
	}
	return append(filtered, fallback...)
}

func (rt *Runtime) rememberRequestRouteFailure(affinity *routeAffinityContext, routeID uint) {
	if rt == nil || rt.Service == nil || affinity == nil || routeID == 0 || len(affinity.RetryKeys) == 0 {
		return
	}
	rt.Service.rememberRequestRouteFailure(affinity.RetryKeys, routeID, time.Now())
}

func requestRouteAffinityFingerprints(c *gin.Context, body []byte, model string) []string {
	return requestRouteAffinityFingerprintsWithID(c, body, model, "")
}

func requestRouteAffinityFingerprintsWithID(c *gin.Context, body []byte, model, affinityID string) []string {
	var fingerprints []string
	add := func(kind string, value any) {
		raw := strings.TrimSpace(stringValue(value))
		if raw == "" {
			return
		}
		fingerprints = appendUniqueString(fingerprints, routeAffinityDigest(kind+"\x00"+raw))
	}

	if c != nil && c.Request != nil {
		for _, header := range []string{
			"X-Upstream-Ops-Session-ID",
			"X-Session-ID",
			"X-Conversation-ID",
			"X-Thread-ID",
			"X-Client-Session-ID",
			"OpenAI-Conversation-ID",
			"Anthropic-Conversation-ID",
		} {
			if value := strings.TrimSpace(c.GetHeader(header)); value != "" {
				add("header:"+strings.ToLower(header), value)
			}
		}
		// An explicit session/conversation identifier is the strongest affinity
		// signal. Do not also decode and hash the full conversation body: clients
		// that provide this header already expect it to define the session, and
		// skipping the body scan keeps long-context requests off the hot path.
		if len(fingerprints) > 0 {
			return fingerprints
		}
	}
	if affinityID = strings.TrimSpace(affinityID); affinityID != "" {
		add("body:id", affinityID)
		return fingerprints
	}

	var payload map[string]any
	if len(body) == 0 || json.Unmarshal(body, &payload) != nil {
		return fingerprints
	}
	if id := findRouteAffinityID(payload); id != "" {
		add("body:id", id)
		return fingerprints
	}

	model = strings.TrimSpace(model)
	if items, ok := payload["messages"].([]any); ok {
		appendConversationFingerprints(&fingerprints, "messages", payload, items, model)
	}
	if items, ok := payload["input"].([]any); ok {
		appendConversationFingerprints(&fingerprints, "input", payload, items, model)
	}
	return fingerprints
}

func appendConversationFingerprints(out *[]string, field string, payload map[string]any, items []any, model string) {
	if len(items) == 0 || !conversationHasTurn(items) {
		return
	}
	static := map[string]any{"model": model}
	for _, key := range []string{"system", "instructions", "tools"} {
		if value, ok := payload[key]; ok {
			static[key] = value
		}
	}
	staticJSON, err := json.Marshal(static)
	if err != nil {
		return
	}

	// Hash every recent conversation prefix incrementally. The next request
	// commonly appends an assistant turn and a new user turn, so removing only
	// the final item cannot match the previous request's history. Keeping a
	// bounded tail protects long conversations from creating excessive keys.
	digest := sha256.New()
	_, _ = digest.Write([]byte("body:" + field + "\x00"))
	_, _ = digest.Write(staticJSON)
	conversationSeen := false
	prefixes := make([]string, 0, minInt(len(items), maxRouteAffinityPrefixes))
	for _, item := range items {
		itemJSON, marshalErr := json.Marshal(item)
		if marshalErr != nil {
			return
		}
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(itemJSON)
		conversationSeen = conversationSeen || conversationItemHasTurn(item)
		if !conversationSeen {
			continue
		}
		fingerprint := hex.EncodeToString(digest.Sum(nil))
		if len(prefixes) == maxRouteAffinityPrefixes {
			copy(prefixes, prefixes[1:])
			prefixes[len(prefixes)-1] = fingerprint
		} else {
			prefixes = append(prefixes, fingerprint)
		}
	}
	for index := len(prefixes) - 1; index >= 0; index-- {
		fingerprint := prefixes[index]
		*out = appendUniqueString(*out, fingerprint)
	}
}

func conversationHasTurn(items []any) bool {
	for _, item := range items {
		if conversationItemHasTurn(item) {
			return true
		}
	}
	return false
}

func conversationItemHasTurn(item any) bool {
	object, ok := item.(map[string]any)
	if !ok {
		return true
	}
	role, _ := object["role"].(string)
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "", "system", "developer", "tool":
		return false
	default:
		return true
	}
}

func findRouteAffinityID(payload map[string]any) string {
	for _, key := range []string{
		"session_id", "sessionId", "conversation_id", "conversationId",
		"thread_id", "threadId", "prompt_cache_key", "promptCacheKey",
		"previous_response_id", "previousResponseId",
	} {
		if value, ok := payload[key]; ok {
			if text := strings.TrimSpace(stringValue(value)); text != "" {
				return text
			}
		}
	}
	for _, key := range []string{"metadata", "conversation", "thread"} {
		if nested, ok := payload[key].(map[string]any); ok {
			if id := findRouteAffinityID(nested); id != "" {
				return id
			}
		}
	}
	return ""
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64)
	default:
		return ""
	}
}

func appendUniqueString(items []string, value string) []string {
	for _, item := range items {
		if item == value {
			return items
		}
	}
	return append(items, value)
}

func routeAffinityDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (s *Service) lookupRouteAffinity(keys []routeAffinityKey, now time.Time) (uint, routeAffinityKey) {
	if s == nil || len(keys) == 0 {
		return 0, routeAffinityKey{}
	}
	s.routeAffinityMu.Lock()
	defer s.routeAffinityMu.Unlock()
	if s.routeAffinities == nil {
		s.routeAffinities = make(map[routeAffinityKey]routeAffinityEntry)
	}
	s.cleanupRouteAffinitiesLocked(now)
	for _, key := range keys {
		if entry, ok := s.routeAffinities[key]; ok && entry.RouteID > 0 && entry.ExpiresAt.After(now) {
			return entry.RouteID, key
		}
		if entry, ok := s.routeAffinities[key]; ok && !entry.ExpiresAt.After(now) {
			delete(s.routeAffinities, key)
		}
	}
	return 0, routeAffinityKey{}
}

func (s *Service) rememberRouteAffinity(keys []routeAffinityKey, routeID uint, now time.Time) {
	if s == nil || routeID == 0 || len(keys) == 0 {
		return
	}
	s.routeAffinityMu.Lock()
	defer s.routeAffinityMu.Unlock()
	if s.routeAffinities == nil {
		s.routeAffinities = make(map[routeAffinityKey]routeAffinityEntry)
	}
	s.cleanupRouteAffinitiesLocked(now)
	for _, key := range keys {
		entry := s.routeAffinities[key]
		if entry.RouteID != routeID {
			entry = routeAffinityEntry{}
		}
		entry.RouteID = routeID
		entry.ExpiresAt = now.Add(routeAffinityTTL)
		s.routeAffinities[key] = entry
	}
	if len(s.routeAffinities) <= maxRouteAffinityEntries {
		return
	}
	for key := range s.routeAffinities {
		delete(s.routeAffinities, key)
		if len(s.routeAffinities) <= maxRouteAffinityEntries {
			break
		}
	}
}

func (s *Service) claimRouteAffinityProbe(key routeAffinityKey, routeID uint, now time.Time) bool {
	if s == nil || routeID == 0 || key.Fingerprint == "" {
		return false
	}
	s.routeAffinityMu.Lock()
	defer s.routeAffinityMu.Unlock()
	entry, ok := s.routeAffinities[key]
	if !ok || entry.RouteID != routeID || !entry.ExpiresAt.After(now) {
		if ok && !entry.ExpiresAt.After(now) {
			delete(s.routeAffinities, key)
		}
		return false
	}
	if entry.ProbeUntil.After(now) || entry.RecoveryBlockedUntil.After(now) {
		return false
	}
	entry.ProbeUntil = now.Add(routeAffinityProbeHold)
	s.routeAffinities[key] = entry
	return true
}

func (s *Service) cleanupRouteAffinitiesLocked(now time.Time) {
	if s == nil || !s.routeAffinityLastCleanup.Add(routeAffinityCleanupInterval).Before(now) {
		return
	}
	for key, entry := range s.routeAffinities {
		if !entry.ExpiresAt.After(now) {
			delete(s.routeAffinities, key)
		}
	}
	s.routeAffinityLastCleanup = now
}

func (s *Service) finishRouteAffinityProbe(ctx *routeAffinityContext, routeID uint, success bool, blockedUntil *time.Time, now time.Time) {
	if s == nil || ctx == nil || routeID == 0 {
		return
	}
	s.routeAffinityMu.Lock()
	defer s.routeAffinityMu.Unlock()
	for _, key := range ctx.Keys {
		entry, ok := s.routeAffinities[key]
		if !ok || entry.RouteID != routeID {
			continue
		}
		entry.ProbeUntil = time.Time{}
		if success {
			entry.RecoveryBlockedUntil = time.Time{}
		} else {
			if blockedUntil == nil && ctx.RecoveryCooldownUntil.After(now) {
				blockedUntil = &ctx.RecoveryCooldownUntil
			}
			if blockedUntil != nil && blockedUntil.After(now) {
				entry.RecoveryBlockedUntil = *blockedUntil
			}
		}
		entry.ExpiresAt = now.Add(routeAffinityTTL)
		s.routeAffinities[key] = entry
	}
}

func (a *routeAffinityContext) recoveryBlockedUntil(current *time.Time, now time.Time) *time.Time {
	if a == nil {
		return current
	}
	var until time.Time
	if current != nil {
		until = *current
	}
	if a.RecoveryCooldownUntil.After(until) {
		until = a.RecoveryCooldownUntil
	}
	if !until.After(now) {
		return nil
	}
	return &until
}

func (a *routeAffinityContext) preservePreferredOnCooldown(routeID uint) {
	if a == nil || routeID == 0 || routeID != a.PreferredRouteID {
		return
	}
	a.PreservePreferred = true
}

func (a routeAffinityContext) shouldRememberRoute(routeID uint) bool {
	if !a.PreservePreferred || a.PreferredRouteID == 0 {
		return true
	}
	return routeID == a.PreferredRouteID
}
