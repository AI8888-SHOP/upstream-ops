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
	routeAffinityTTL       = 45 * time.Minute
	routeAffinityProbeHold = 15 * time.Minute
	routeAffinityCleanupInterval = time.Minute
	maxRouteAffinityEntries  = 4096
	maxRouteAffinityPrefixes = 64
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
	Keys             []routeAffinityKey
	LookupKey        routeAffinityKey
	PreferredRouteID uint
	RecoveryRouteID  uint
	Recovery         bool
	PreservePreferred bool
	RecoveryCooldownUntil time.Time
}

func (a routeAffinityContext) enabled() bool {
	return len(a.Keys) > 0 && a.PreferredRouteID > 0
}

func (rt *Runtime) routeAffinityForRequest(c *gin.Context, keyID, groupID uint, protocolName, model string, body []byte) routeAffinityContext {
	if rt == nil || rt.Service == nil || keyID == 0 || groupID == 0 {
		return routeAffinityContext{}
	}
	fingerprints := requestRouteAffinityFingerprints(c, body, model)
	if len(fingerprints) == 0 {
		return routeAffinityContext{}
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
	ctx := routeAffinityContext{Keys: keys}
	ctx.PreferredRouteID, ctx.LookupKey = rt.lookupRouteAffinity(keys, time.Now())
	return ctx
}

func requestRouteAffinityFingerprints(c *gin.Context, body []byte, model string) []string {
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
	hashInput := make([]byte, 0, len(staticJSON)+len(field)+len(items)*32+16)
	hashInput = append(hashInput, ("body:"+field+"\x00")...)
	hashInput = append(hashInput, staticJSON...)
	conversationSeen := false
	prefixes := make([]string, 0, minInt(len(items), maxRouteAffinityPrefixes))
	for _, item := range items {
		itemJSON, marshalErr := json.Marshal(item)
		if marshalErr != nil {
			return
		}
		hashInput = append(hashInput, 0)
		hashInput = append(hashInput, itemJSON...)
		conversationSeen = conversationSeen || conversationItemHasTurn(item)
		if !conversationSeen {
			continue
		}
		sum := sha256.Sum256(hashInput)
		digest := hex.EncodeToString(sum[:])
		if len(prefixes) == maxRouteAffinityPrefixes {
			copy(prefixes, prefixes[1:])
			prefixes[len(prefixes)-1] = digest
		} else {
			prefixes = append(prefixes, digest)
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
		"thread_id", "threadId", "previous_response_id", "previousResponseId",
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
		} else if blockedUntil != nil && blockedUntil.After(now) {
			entry.RecoveryBlockedUntil = *blockedUntil
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

func (a routeAffinityContext) shouldRememberRoute(routeID uint) bool {
	if !a.PreservePreferred || a.PreferredRouteID == 0 {
		return true
	}
	return routeID == a.PreferredRouteID
}
