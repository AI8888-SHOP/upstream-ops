package gateway

import (
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// responseValidatorCacheEntry keeps the normalized group policy alongside the
// compiled validator. Rule changes are handled by explicit invalidation; the
// policy signature also protects against callers that update a group directly
// without going through AdminService.
type responseValidatorCacheEntry struct {
	validator *responseValidator
	config    responseValidationConfig
}

func responseValidationConfigForGroup(group *storage.GatewayGroup) responseValidationConfig {
	if group == nil {
		return responseValidationConfig{}
	}
	return normalizeResponseValidationConfig(responseValidationConfig{
		Enabled:       group.ResponseValidationEnabled,
		StreamMode:    group.ResponseValidationStreamMode,
		PrefixBytes:   group.ResponseValidationPrefixBytes,
		PrefixTimeout: time.Duration(group.ResponseValidationPrefixTimeoutMS) * time.Millisecond,
	})
}

func normalizeResponseValidationConfig(cfg responseValidationConfig) responseValidationConfig {
	cfg.StreamMode = strings.ToLower(strings.TrimSpace(cfg.StreamMode))
	if cfg.StreamMode == "" {
		cfg.StreamMode = "prefix"
	}
	if cfg.PrefixBytes <= 0 {
		cfg.PrefixBytes = defaultValidationPrefixBytes
	}
	if cfg.PrefixBytes > maxValidationPrefixBytes {
		cfg.PrefixBytes = maxValidationPrefixBytes
	}
	if cfg.PrefixTimeout <= 0 {
		cfg.PrefixTimeout = defaultValidationPrefixTimeout
	}
	return cfg
}

func responseValidatorConfigOf(validator *responseValidator) responseValidationConfig {
	if validator == nil {
		return responseValidationConfig{}
	}
	return responseValidationConfig{
		Enabled:       validator.enabled,
		StreamMode:    validator.streamMode,
		PrefixBytes:   validator.prefixBytes,
		PrefixTimeout: validator.prefixTimeout,
	}
}

func (s *Service) cachedResponseValidator(group *storage.GatewayGroup) *responseValidator {
	if s == nil || group == nil || group.ID == 0 {
		return nil
	}
	want := responseValidationConfigForGroup(group)
	s.responseValidatorCacheMu.RLock()
	entry, ok := s.responseValidatorCache[group.ID]
	s.responseValidatorCacheMu.RUnlock()
	if !ok || entry.validator == nil || entry.config != want {
		return nil
	}
	return entry.validator
}

func (s *Service) cacheResponseValidator(group *storage.GatewayGroup, validator *responseValidator) {
	if s == nil || group == nil || group.ID == 0 || validator == nil {
		return
	}
	s.responseValidatorCacheMu.Lock()
	if s.responseValidatorCache == nil {
		s.responseValidatorCache = make(map[uint]responseValidatorCacheEntry)
	}
	s.responseValidatorCache[group.ID] = responseValidatorCacheEntry{
		validator: validator,
		config:    responseValidatorConfigOf(validator),
	}
	s.responseValidatorCacheMu.Unlock()
}

// InvalidateResponseValidator drops the compiled response rules for one group.
// It is safe to call after a successful rule or policy mutation while existing
// requests continue using the immutable validator they already acquired.
func (s *Service) InvalidateResponseValidator(groupID uint) {
	if s == nil || groupID == 0 {
		return
	}
	s.responseValidatorCacheMu.Lock()
	delete(s.responseValidatorCache, groupID)
	s.responseValidatorCacheMu.Unlock()
}

// InvalidateAllResponseValidators drops every compiled response-rule cache
// entry, for example when the backing repository is replaced at startup.
func (s *Service) InvalidateAllResponseValidators() {
	if s == nil {
		return
	}
	s.responseValidatorCacheMu.Lock()
	s.responseValidatorCache = make(map[uint]responseValidatorCacheEntry)
	s.responseValidatorCacheMu.Unlock()
}
