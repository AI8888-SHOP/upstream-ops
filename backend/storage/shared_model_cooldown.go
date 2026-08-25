package storage

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func isGatewayGroupScopedModelCooldown(cooldown GatewayRouteModelCooldown) bool {
	return strings.EqualFold(strings.TrimSpace(cooldown.CooldownScope), GatewayModelCooldownScopeGroup)
}

// GatewaySharedModelCooldownScope returns the stable identity used to share a
// model health state. A monitored route must have a real upstream API-key ID;
// source channel IDs alone are insufficient because different keys can have
// independent quota, permissions, and failure behavior.
//
// Direct-provider routes use the provider record as their credential owner.
// The configured upstream protocol remains part of the identity because the
// same model can expose materially different capabilities through protocols.
func GatewaySharedModelCooldownScope(route *GatewayRoute) string {
	if route == nil {
		return ""
	}
	protocol := normalizeSharedCooldownProtocol(route.UpstreamProtocol)
	if route.NormalizeSourceKind() == GatewayRouteSourceProvider {
		if route.GatewayProviderID == 0 {
			return ""
		}
		return fmt.Sprintf("provider:%d:protocol:%s", route.GatewayProviderID, protocol)
	}
	if route.SourceChannelID == 0 || route.SourceAPIKeyID <= 0 {
		return ""
	}
	group := "default"
	if route.SourceGroupID != nil && *route.SourceGroupID > 0 {
		group = fmt.Sprintf("id:%d", *route.SourceGroupID)
	} else if name := strings.TrimSpace(route.SourceGroupName); name != "" {
		// A hash keeps the durable key short and delimiter-safe even when an
		// older upstream exposes a very long or non-ASCII group name.
		digest := sha256.Sum256([]byte(strings.ToLower(name)))
		group = fmt.Sprintf("name:%x", digest[:])
	}
	return fmt.Sprintf(
		"monitor:channel:%d:group:%s:key:%d:protocol:%s",
		route.SourceChannelID, group, route.SourceAPIKeyID, protocol,
	)
}

func normalizeSharedCooldownProtocol(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case GatewayUpstreamProtocolOpenAI, GatewayUpstreamProtocolOpenAIChat, "chat", "chat_completions":
		return GatewayUpstreamProtocolOpenAIChat
	case GatewayUpstreamProtocolOpenAIResponses, "responses":
		return GatewayUpstreamProtocolOpenAIResponses
	case GatewayUpstreamProtocolAnthropic:
		return GatewayUpstreamProtocolAnthropic
	default:
		return GatewayUpstreamProtocolAuto
	}
}

func sharedCooldownAsRouteCooldown(shared GatewaySharedModelCooldown, routeID uint) GatewayRouteModelCooldown {
	return GatewayRouteModelCooldown{
		ID:                         shared.ID,
		RouteID:                    routeID,
		Model:                      NormalizeGatewayModel(shared.Model),
		SharedCooldownID:           shared.ID,
		SharedScopeKey:             shared.ScopeKey,
		CooldownScope:              GatewayModelCooldownScopeShared,
		TempUnschedulableUntil:     clonePointer(shared.TempUnschedulableUntil),
		TempUnschedulableReason:    shared.TempUnschedulableReason,
		TempUnschedulableAt:        clonePointer(shared.TempUnschedulableAt),
		TempUnschedulableRequestID: shared.TempUnschedulableRequestID,
		RecoverSuccessStreak:       shared.RecoverSuccessStreak,
		NextProbeAt:                clonePointer(shared.NextProbeAt),
		LastProbeAt:                clonePointer(shared.LastProbeAt),
		ProbeLeaseUntil:            clonePointer(shared.ProbeLeaseUntil),
		ProbeStatus:                shared.ProbeStatus,
		ProbeFailureCount:          shared.ProbeFailureCount,
		ProbeRequestID:             shared.ProbeRequestID,
		ProbeInboundProtocol:       shared.ProbeInboundProtocol,
		ProbeLastStatusCode:        shared.ProbeLastStatusCode,
		ProbeLastError:             shared.ProbeLastError,
		CreatedAt:                  shared.CreatedAt,
		UpdatedAt:                  shared.UpdatedAt,
	}
}

func sharedCooldownFromRouteCooldown(scope string, cooldown GatewayRouteModelCooldown) GatewaySharedModelCooldown {
	return GatewaySharedModelCooldown{
		ScopeKey:                   scope,
		Model:                      NormalizeGatewayModel(cooldown.Model),
		PreferredRouteID:           cooldown.RouteID,
		TempUnschedulableUntil:     clonePointer(cooldown.TempUnschedulableUntil),
		TempUnschedulableReason:    cooldown.TempUnschedulableReason,
		TempUnschedulableAt:        clonePointer(cooldown.TempUnschedulableAt),
		TempUnschedulableRequestID: cooldown.TempUnschedulableRequestID,
		RecoverSuccessStreak:       cooldown.RecoverSuccessStreak,
		NextProbeAt:                clonePointer(cooldown.NextProbeAt),
		LastProbeAt:                clonePointer(cooldown.LastProbeAt),
		ProbeLeaseUntil:            clonePointer(cooldown.ProbeLeaseUntil),
		ProbeStatus:                cooldown.ProbeStatus,
		ProbeFailureCount:          cooldown.ProbeFailureCount,
		ProbeRequestID:             cooldown.ProbeRequestID,
		ProbeInboundProtocol:       cooldown.ProbeInboundProtocol,
		ProbeLastStatusCode:        cooldown.ProbeLastStatusCode,
		ProbeLastError:             cooldown.ProbeLastError,
	}
}

// migrateLegacyModelCooldownsToShared runs after AutoMigrate. It promotes
// only rows whose source identity proves that they use a real shared upstream
// credential. Remaining rows intentionally keep their historic route-local
// behavior until they acquire one through "ensure upstream key".
func migrateLegacyModelCooldownsToShared(db *gorm.DB) error {
	if db == nil {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		var cooldowns []GatewayRouteModelCooldown
		if err := tx.Order("updated_at ASC, id ASC").Find(&cooldowns).Error; err != nil {
			return err
		}
		if len(cooldowns) == 0 {
			return nil
		}
		routes := make(map[uint]GatewayRoute, len(cooldowns))
		for _, cooldown := range cooldowns {
			if cooldown.RouteID == 0 {
				continue
			}
			if _, ok := routes[cooldown.RouteID]; ok {
				continue
			}
			var route GatewayRoute
			if err := tx.First(&route, cooldown.RouteID).Error; err != nil {
				if err == gorm.ErrRecordNotFound {
					continue
				}
				return err
			}
			routes[cooldown.RouteID] = route
		}

		for _, cooldown := range cooldowns {
			// Explicit group-scoped rows (currently used for first-token timeout
			// protection) must never be promoted into the shared credential state.
			if isGatewayGroupScopedModelCooldown(cooldown) {
				continue
			}
			route, ok := routes[cooldown.RouteID]
			if !ok {
				continue
			}
			scope := GatewaySharedModelCooldownScope(&route)
			model := NormalizeGatewayModel(cooldown.Model)
			if scope == "" || model == "" {
				continue
			}
			var shared GatewaySharedModelCooldown
			err := tx.Where("scope_key = ? AND model = ?", scope, model).First(&shared).Error
			if err == gorm.ErrRecordNotFound {
				shared = sharedCooldownFromRouteCooldown(scope, cooldown)
				if err := tx.Create(&shared).Error; err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else if shared.UpdatedAt.Before(cooldown.UpdatedAt) {
				// When an older deployment created one route-local row per group,
				// retain the newest observed failure/probe generation.
				replacement := sharedCooldownFromRouteCooldown(scope, cooldown)
				replacement.ID = shared.ID
				replacement.CreatedAt = shared.CreatedAt
				if err := tx.Save(&replacement).Error; err != nil {
					return err
				}
			}
			if err := tx.Delete(&GatewayRouteModelCooldown{}, cooldown.ID).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (r *GatewayRoutes) promoteRouteLocalModelCooldownsToShared(routeID uint) error {
	if r == nil || r.db == nil || routeID == 0 {
		return nil
	}
	var sharedScope string
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var route GatewayRoute
		if err := tx.First(&route, routeID).Error; err != nil {
			return err
		}
		sharedScope = GatewaySharedModelCooldownScope(&route)
		if sharedScope == "" {
			return nil
		}
		var cooldowns []GatewayRouteModelCooldown
		if err := tx.Where("route_id = ?", routeID).Order("updated_at ASC, id ASC").Find(&cooldowns).Error; err != nil {
			return err
		}
		for _, cooldown := range cooldowns {
			if isGatewayGroupScopedModelCooldown(cooldown) {
				continue
			}
			model := NormalizeGatewayModel(cooldown.Model)
			if model == "" {
				continue
			}
			var shared GatewaySharedModelCooldown
			err := tx.Where("scope_key = ? AND model = ?", sharedScope, model).First(&shared).Error
			if err == gorm.ErrRecordNotFound {
				shared = sharedCooldownFromRouteCooldown(sharedScope, cooldown)
				if err := tx.Create(&shared).Error; err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else if shared.UpdatedAt.Before(cooldown.UpdatedAt) {
				replacement := sharedCooldownFromRouteCooldown(sharedScope, cooldown)
				replacement.ID = shared.ID
				replacement.CreatedAt = shared.CreatedAt
				if err := tx.Save(&replacement).Error; err != nil {
					return err
				}
			}
			if err := tx.Delete(&GatewayRouteModelCooldown{}, cooldown.ID).Error; err != nil {
				return err
			}
		}
		return nil
	})
	if err == nil && sharedScope != "" {
		r.invalidateSharedModelCooldownScope(sharedScope)
	}
	return err
}

func (r *GatewayRoutes) invalidateSharedModelCooldownScope(scope string) {
	if r == nil || r.readCaches == nil || strings.TrimSpace(scope) == "" {
		return
	}
	r.readCaches.gatewayRoutes.invalidateWhere(func(routes []GatewayRoute) bool {
		for i := range routes {
			if GatewaySharedModelCooldownScope(&routes[i]) == scope {
				return true
			}
		}
		return false
	})
}

func (r *GatewayRoutes) routeAndSharedCooldownScope(id uint) (*GatewayRoute, string, error) {
	if r == nil || r.db == nil || id == 0 {
		return nil, "", nil
	}
	var route GatewayRoute
	if err := r.db.First(&route, id).Error; err != nil {
		return nil, "", err
	}
	return &route, GatewaySharedModelCooldownScope(&route), nil
}

// FindActiveRouteForSharedModelCooldown picks a currently usable route that
// still resolves to the claimed shared credential. It lets a probe continue
// through another gateway group when the route that originally failed was
// later disabled or removed.
func (r *GatewayRoutes) FindActiveRouteForSharedModelCooldown(scope string, preferredRouteID uint) (*GatewayRoute, error) {
	if r == nil || r.db == nil || strings.TrimSpace(scope) == "" {
		return nil, nil
	}
	if preferredRouteID > 0 {
		var preferred GatewayRoute
		if err := r.db.First(&preferred, preferredRouteID).Error; err == nil &&
			preferred.Enabled && !preferred.RateLimitAutoDisabled &&
			GatewaySharedModelCooldownScope(&preferred) == scope {
			active, groupErr := r.gatewayGroupIsActiveForSharedCooldown(preferred.GatewayGroupID)
			if groupErr != nil {
				return nil, groupErr
			}
			if active {
				return &preferred, nil
			}
		} else if err != nil && err != gorm.ErrRecordNotFound {
			return nil, err
		}
	}
	var candidates []GatewayRoute
	if err := r.db.Where("enabled = ? AND rate_limit_auto_disabled = ?", true, false).
		Order("gateway_group_id ASC, position ASC, id ASC").Find(&candidates).Error; err != nil {
		return nil, err
	}
	groupIDs := make([]uint, 0, len(candidates))
	seenGroups := make(map[uint]struct{}, len(candidates))
	for i := range candidates {
		groupID := candidates[i].GatewayGroupID
		if groupID == 0 {
			continue
		}
		if _, exists := seenGroups[groupID]; exists {
			continue
		}
		seenGroups[groupID] = struct{}{}
		groupIDs = append(groupIDs, groupID)
	}
	if len(groupIDs) == 0 {
		return nil, nil
	}
	var groups []GatewayGroup
	if err := r.db.Select("id", "status").Where("id IN ? AND status = ?", groupIDs, GatewayGroupStatusActive).Find(&groups).Error; err != nil {
		return nil, err
	}
	activeGroups := make(map[uint]struct{}, len(groups))
	for _, group := range groups {
		activeGroups[group.ID] = struct{}{}
	}
	for i := range candidates {
		if _, active := activeGroups[candidates[i].GatewayGroupID]; !active {
			continue
		}
		if GatewaySharedModelCooldownScope(&candidates[i]) == scope {
			return &candidates[i], nil
		}
	}
	return nil, nil
}

func (r *GatewayRoutes) gatewayGroupIsActiveForSharedCooldown(groupID uint) (bool, error) {
	if groupID == 0 {
		return false, nil
	}
	var group GatewayGroup
	if err := r.db.Select("id", "status").First(&group, groupID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return false, err
	}
	return group.Status == GatewayGroupStatusActive, nil
}

func sharedCooldownLeaseRequestID(id uint, now time.Time, manual bool) string {
	prefix := "probe"
	if manual {
		prefix = "probe-manual"
	}
	return fmt.Sprintf("%s-shared-%d-%d", prefix, id, now.UnixNano())
}

func sharedCooldownHoldUntil(cooldown *time.Time, leaseUntil time.Time) time.Time {
	if cooldown != nil && cooldown.After(leaseUntil) {
		return *cooldown
	}
	return leaseUntil
}

func normalizeSharedCooldownNow(now time.Time) time.Time {
	if now.IsZero() {
		return time.Now()
	}
	return now
}

func (r *GatewayRoutes) setSharedModelTempUnschedulable(
	id uint,
	model string,
	until time.Time,
	reason string,
	failedAt time.Time,
	requestID string,
	probeEnabled bool,
	inboundProtocol string,
) (bool, error) {
	route, scope, err := r.routeAndSharedCooldownScope(id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return false, nil
		}
		return true, err
	}
	if route == nil || scope == "" {
		return false, nil
	}
	model = NormalizeGatewayModel(model)
	if model == "" {
		return true, nil
	}
	failedAt = normalizeSharedCooldownNow(failedAt)
	requestID = strings.TrimSpace(requestID)
	inboundProtocol = normalizeGatewayProbeInboundProtocol(inboundProtocol)
	now := time.Now()
	var nextProbeAt *time.Time
	probeStatus := GatewayModelProbeStatusManual
	if probeEnabled {
		next := modelCooldownInitialProbeAt(until, now)
		nextProbeAt = &next
		probeStatus = GatewayModelProbeStatusPending
	}
	cooldown := GatewaySharedModelCooldown{
		ScopeKey:                   scope,
		Model:                      model,
		PreferredRouteID:           id,
		TempUnschedulableUntil:     &until,
		TempUnschedulableReason:    reason,
		TempUnschedulableAt:        &failedAt,
		TempUnschedulableRequestID: requestID,
		NextProbeAt:                nextProbeAt,
		ProbeStatus:                probeStatus,
		ProbeInboundProtocol:       inboundProtocol,
	}
	err = r.db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "scope_key"}, {Name: "model"}},
		DoUpdates: clause.Assignments(map[string]any{
			"preferred_route_id":            id,
			"temp_unschedulable_until":      until,
			"temp_unschedulable_reason":     reason,
			"temp_unschedulable_at":         failedAt,
			"temp_unschedulable_request_id": requestID,
			"recover_success_streak":        0,
			"next_probe_at":                 nextProbeAt,
			"last_probe_at":                 nil,
			"probe_lease_until":             nil,
			"probe_status":                  probeStatus,
			"probe_failure_count":           0,
			"probe_request_id":              "",
			"probe_inbound_protocol":        inboundProtocol,
			"probe_last_status_code":        0,
			"probe_last_error":              "",
			"updated_at":                    now,
		}),
	}).Create(&cooldown).Error
	if err == nil {
		r.invalidateSharedModelCooldownScope(scope)
	}
	return true, err
}

func (r *GatewayRoutes) claimDueSharedModelCooldownProbes(now time.Time, limit int, lease time.Duration) ([]GatewayRouteModelCooldown, error) {
	if r == nil || r.db == nil || limit <= 0 {
		return nil, nil
	}
	now = normalizeSharedCooldownNow(now)
	if lease <= 0 {
		lease = 30 * time.Second
	}
	probeDueBefore := now.Add(10 * time.Second)
	var candidates []GatewaySharedModelCooldown
	err := r.db.Where("temp_unschedulable_until IS NOT NULL").
		Where("(next_probe_at <= ? OR (next_probe_at IS NULL AND temp_unschedulable_until <= ?))", now, probeDueBefore).
		Where("(probe_status IS NULL OR probe_status <> ?)", GatewayModelProbeStatusManual).
		Where("(probe_status IS NULL OR probe_status <> ? OR probe_lease_until IS NULL OR probe_lease_until <= ?)", GatewayModelProbeStatusProbing, now).
		Order("next_probe_at ASC, updated_at ASC, id ASC").
		Limit(limit * 4).Find(&candidates).Error
	if err != nil {
		return nil, err
	}
	claimed := make([]GatewayRouteModelCooldown, 0, limit)
	for _, candidate := range candidates {
		if len(claimed) >= limit {
			break
		}
		leaseUntil := now.Add(lease)
		holdUntil := sharedCooldownHoldUntil(candidate.TempUnschedulableUntil, leaseUntil)
		requestID := sharedCooldownLeaseRequestID(candidate.ID, now, false)
		query := r.db.Model(&GatewaySharedModelCooldown{}).
			Where("id = ?", candidate.ID).
			Where("(probe_status IS NULL OR probe_status <> ? OR probe_lease_until IS NULL OR probe_lease_until <= ?)", GatewayModelProbeStatusProbing, now).
			Where("(probe_status IS NULL OR probe_status <> ?)", GatewayModelProbeStatusManual).
			Where("(next_probe_at <= ? OR (next_probe_at IS NULL AND temp_unschedulable_until <= ?))", now, probeDueBefore)
		if candidate.TempUnschedulableUntil != nil {
			query = query.Where("temp_unschedulable_until = ?", *candidate.TempUnschedulableUntil)
		}
		if candidate.TempUnschedulableAt == nil {
			query = query.Where("temp_unschedulable_at IS NULL")
		} else {
			query = query.Where("temp_unschedulable_at = ?", *candidate.TempUnschedulableAt)
		}
		query = query.Where("temp_unschedulable_request_id = ?", candidate.TempUnschedulableRequestID)
		result := query.Updates(map[string]any{
			"probe_status":             GatewayModelProbeStatusProbing,
			"probe_lease_until":        leaseUntil,
			"probe_request_id":         requestID,
			"temp_unschedulable_until": holdUntil,
			"updated_at":               now,
		})
		if result.Error != nil {
			return claimed, result.Error
		}
		if result.RowsAffected == 0 {
			continue
		}
		candidate.ProbeStatus = GatewayModelProbeStatusProbing
		candidate.ProbeLeaseUntil = &leaseUntil
		candidate.ProbeRequestID = requestID
		candidate.TempUnschedulableUntil = &holdUntil
		claimed = append(claimed, sharedCooldownAsRouteCooldown(candidate, candidate.PreferredRouteID))
		r.invalidateSharedModelCooldownScope(candidate.ScopeKey)
	}
	return claimed, nil
}

func (r *GatewayRoutes) claimSharedModelCooldownProbe(id uint, model string, now time.Time, lease time.Duration) (*GatewayRouteModelCooldown, bool, error) {
	route, scope, err := r.routeAndSharedCooldownScope(id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, false, nil
		}
		return nil, true, err
	}
	if route == nil || scope == "" {
		return nil, false, nil
	}
	model = NormalizeGatewayModel(model)
	if model == "" {
		return nil, true, nil
	}
	var candidate GatewaySharedModelCooldown
	if err := r.db.Where("scope_key = ? AND model = ?", scope, model).First(&candidate).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, false, nil
		}
		return nil, true, err
	}
	now = normalizeSharedCooldownNow(now)
	if lease <= 0 {
		lease = 30 * time.Second
	}
	leaseUntil := now.Add(lease)
	holdUntil := sharedCooldownHoldUntil(candidate.TempUnschedulableUntil, leaseUntil)
	requestID := sharedCooldownLeaseRequestID(candidate.ID, now, true)
	result := r.db.Model(&GatewaySharedModelCooldown{}).
		Where("id = ?", candidate.ID).
		Where("(probe_status IS NULL OR probe_status <> ? OR probe_lease_until IS NULL OR probe_lease_until <= ?)", GatewayModelProbeStatusProbing, now).
		Updates(map[string]any{
			"preferred_route_id":       id,
			"probe_status":             GatewayModelProbeStatusProbing,
			"probe_lease_until":        leaseUntil,
			"probe_request_id":         requestID,
			"temp_unschedulable_until": holdUntil,
			"next_probe_at":            now,
			"updated_at":               now,
		})
	if result.Error != nil {
		return nil, true, result.Error
	}
	if result.RowsAffected == 0 {
		return nil, true, nil
	}
	candidate.PreferredRouteID = id
	candidate.ProbeStatus = GatewayModelProbeStatusProbing
	candidate.ProbeLeaseUntil = &leaseUntil
	candidate.ProbeRequestID = requestID
	candidate.TempUnschedulableUntil = &holdUntil
	candidate.NextProbeAt = &now
	r.invalidateSharedModelCooldownScope(scope)
	claim := sharedCooldownAsRouteCooldown(candidate, id)
	return &claim, true, nil
}

func (r *GatewayRoutes) configureSharedModelCooldownProbes(enabled bool, now time.Time) error {
	if r == nil || r.db == nil {
		return nil
	}
	now = normalizeSharedCooldownNow(now)
	var rows []GatewaySharedModelCooldown
	if err := r.db.Where(
		"temp_unschedulable_until IS NOT NULL OR next_probe_at IS NOT NULL OR probe_lease_until IS NOT NULL OR probe_status = ?",
		GatewayModelProbeStatusProbing,
	).Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		updates := map[string]any{}
		if !enabled {
			if row.ProbeStatus == GatewayModelProbeStatusProbing &&
				row.ProbeLeaseUntil != nil && row.ProbeLeaseUntil.After(now) {
				continue
			}
			if row.ProbeStatus == GatewayModelProbeStatusManual && row.NextProbeAt == nil &&
				row.ProbeLeaseUntil == nil && row.ProbeRequestID == "" {
				continue
			}
			updates["next_probe_at"] = nil
			updates["probe_lease_until"] = nil
			updates["probe_request_id"] = ""
			updates["probe_status"] = GatewayModelProbeStatusManual
		} else if row.ProbeStatus != GatewayModelProbeStatusProbing &&
			(row.ProbeStatus == "" ||
				row.ProbeStatus == GatewayModelProbeStatusManual ||
				row.ProbeStatus == GatewayModelProbeStatusHealthy ||
				row.NextProbeAt == nil) {
			next := now
			if row.NextProbeAt != nil && !row.NextProbeAt.IsZero() {
				next = *row.NextProbeAt
			} else if row.TempUnschedulableUntil != nil {
				next = modelCooldownInitialProbeAt(*row.TempUnschedulableUntil, now)
			}
			updates["next_probe_at"] = next
			updates["probe_status"] = GatewayModelProbeStatusPending
		}
		if len(updates) == 0 {
			continue
		}
		updates["updated_at"] = now
		query := r.db.Model(&GatewaySharedModelCooldown{}).Where("id = ?", row.ID)
		if !enabled {
			query = query.Where("probe_status <> ? OR probe_lease_until IS NULL OR probe_lease_until <= ?", GatewayModelProbeStatusProbing, now)
		} else {
			query = query.Where("probe_status <> ?", GatewayModelProbeStatusProbing)
		}
		result := query.Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			r.invalidateSharedModelCooldownScope(row.ScopeKey)
		}
	}
	return nil
}

func isSharedModelCooldownClaim(claim GatewayRouteModelCooldown) bool {
	return claim.SharedCooldownID > 0 || strings.TrimSpace(claim.SharedScopeKey) != ""
}

func (r *GatewayRoutes) sharedModelCooldownClaimQuery(claim GatewayRouteModelCooldown) (*gorm.DB, uint, bool) {
	if r == nil || r.db == nil || !isSharedModelCooldownClaim(claim) {
		return nil, 0, false
	}
	id := claim.SharedCooldownID
	if id == 0 {
		id = claim.ID
	}
	if id == 0 {
		return nil, 0, false
	}
	query := r.db.Model(&GatewaySharedModelCooldown{}).
		Where("id = ? AND probe_status = ? AND probe_request_id = ?", id, GatewayModelProbeStatusProbing, claim.ProbeRequestID)
	if claim.TempUnschedulableAt == nil {
		query = query.Where("temp_unschedulable_at IS NULL")
	} else {
		query = query.Where("temp_unschedulable_at = ?", *claim.TempUnschedulableAt)
	}
	query = query.Where("temp_unschedulable_request_id = ?", claim.TempUnschedulableRequestID)
	return query, id, true
}

func (r *GatewayRoutes) markSharedModelProbeSuccess(claim GatewayRouteModelCooldown, now time.Time, statusCode int) (bool, bool, error) {
	query, _, handled := r.sharedModelCooldownClaimQuery(claim)
	if !handled {
		return false, false, nil
	}
	now = normalizeSharedCooldownNow(now)
	result := query.Updates(map[string]any{
		"temp_unschedulable_until": nil,
		"next_probe_at":            nil,
		"last_probe_at":            now,
		"probe_lease_until":        nil,
		"probe_status":             GatewayModelProbeStatusHealthy,
		"probe_failure_count":      0,
		"probe_request_id":         "",
		"probe_last_status_code":   statusCode,
		"probe_last_error":         "",
		"updated_at":               now,
	})
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(claim.SharedScopeKey)
	}
	return true, result.RowsAffected > 0, result.Error
}

func (r *GatewayRoutes) markSharedModelProbeFailure(claim GatewayRouteModelCooldown, now, next time.Time, statusCode int, message string, permanent bool) (bool, bool, error) {
	query, _, handled := r.sharedModelCooldownClaimQuery(claim)
	if !handled {
		return false, false, nil
	}
	now = normalizeSharedCooldownNow(now)
	if next.IsZero() || !next.After(now) {
		next = now.Add(time.Minute)
	}
	status := GatewayModelProbeStatusTransient
	if permanent {
		status = GatewayModelProbeStatusPermanent
	}
	result := query.Updates(map[string]any{
		"temp_unschedulable_until": next,
		"next_probe_at":            next,
		"last_probe_at":            now,
		"probe_lease_until":        nil,
		"probe_status":             status,
		"probe_failure_count":      gorm.Expr("probe_failure_count + 1"),
		"probe_request_id":         "",
		"probe_last_status_code":   statusCode,
		"probe_last_error":         strings.TrimSpace(message),
		"updated_at":               now,
	})
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(claim.SharedScopeKey)
	}
	return true, result.RowsAffected > 0, result.Error
}

func (r *GatewayRoutes) markSharedManualModelProbeFailure(claim GatewayRouteModelCooldown, now time.Time, statusCode int, message string) (bool, bool, error) {
	query, _, handled := r.sharedModelCooldownClaimQuery(claim)
	if !handled {
		return false, false, nil
	}
	now = normalizeSharedCooldownNow(now)
	result := query.Updates(map[string]any{
		"next_probe_at":          nil,
		"last_probe_at":          now,
		"probe_lease_until":      nil,
		"probe_status":           GatewayModelProbeStatusManual,
		"probe_failure_count":    gorm.Expr("probe_failure_count + 1"),
		"probe_request_id":       "",
		"probe_last_status_code": statusCode,
		"probe_last_error":       strings.TrimSpace(message),
		"updated_at":             now,
	})
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(claim.SharedScopeKey)
	}
	return true, result.RowsAffected > 0, result.Error
}

func (r *GatewayRoutes) sharedCooldownForRouteModel(routeID uint, model string) (GatewaySharedModelCooldown, bool, error) {
	_, scope, err := r.routeAndSharedCooldownScope(routeID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return GatewaySharedModelCooldown{}, false, nil
		}
		return GatewaySharedModelCooldown{}, false, err
	}
	model = NormalizeGatewayModel(model)
	if scope == "" || model == "" {
		return GatewaySharedModelCooldown{}, false, nil
	}
	var shared GatewaySharedModelCooldown
	if err := r.db.Where("scope_key = ? AND model = ?", scope, model).First(&shared).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return GatewaySharedModelCooldown{}, false, nil
		}
		return GatewaySharedModelCooldown{}, false, err
	}
	return shared, true, nil
}

func (r *GatewayRoutes) clearSharedModelTempUnschedulable(routeID uint, model string, diagnostics bool) (bool, error) {
	shared, found, err := r.sharedCooldownForRouteModel(routeID, model)
	if err != nil || !found {
		return found, err
	}
	now := time.Now()
	updates := map[string]any{
		"temp_unschedulable_until": nil,
		"next_probe_at":            nil,
		"probe_lease_until":        nil,
		"probe_status":             GatewayModelProbeStatusManual,
		"probe_request_id":         "",
		"updated_at":               now,
	}
	if !diagnostics {
		updates["temp_unschedulable_reason"] = ""
		updates["temp_unschedulable_at"] = nil
		updates["temp_unschedulable_request_id"] = ""
		updates["recover_success_streak"] = 0
	}
	result := r.db.Model(&GatewaySharedModelCooldown{}).Where("id = ?", shared.ID).Updates(updates)
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(shared.ScopeKey)
	}
	return true, result.Error
}

func (r *GatewayRoutes) clearSharedModelTempUnschedulableUntilIfMatch(
	routeID uint,
	model string,
	until time.Time,
	failedAt *time.Time,
	requestID string,
) (bool, bool, error) {
	shared, found, err := r.sharedCooldownForRouteModel(routeID, model)
	if err != nil || !found {
		return found, false, err
	}
	query := r.db.Model(&GatewaySharedModelCooldown{}).
		Where("id = ? AND temp_unschedulable_until = ?", shared.ID, until)
	if failedAt == nil || failedAt.IsZero() {
		query = query.Where("temp_unschedulable_at IS NULL")
	} else {
		query = query.Where("temp_unschedulable_at = ?", *failedAt)
	}
	query = query.Where("temp_unschedulable_request_id = ?", requestID)
	result := query.Updates(map[string]any{
		"temp_unschedulable_until": nil,
		"next_probe_at":            nil,
		"probe_lease_until":        nil,
		"probe_status":             GatewayModelProbeStatusManual,
		"probe_request_id":         "",
		"updated_at":               time.Now(),
	})
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(shared.ScopeKey)
	}
	return true, result.RowsAffected > 0, result.Error
}

func (r *GatewayRoutes) noteSharedSuccessForModelPauseError(routeID uint, model string, failedAt *time.Time, requestID string) (bool, error) {
	shared, found, err := r.sharedCooldownForRouteModel(routeID, model)
	if err != nil || !found {
		return found, err
	}
	requestID = strings.TrimSpace(requestID)
	query := r.db.Model(&GatewaySharedModelCooldown{}).Where("id = ?", shared.ID)
	if failedAt == nil || failedAt.IsZero() {
		query = query.Where("temp_unschedulable_at IS NULL")
	} else {
		query = query.Where("temp_unschedulable_at = ?", *failedAt)
	}
	query = query.Where("temp_unschedulable_request_id = ?", requestID).
		Where(`(
             (temp_unschedulable_reason IS NOT NULL AND temp_unschedulable_reason != '')
             OR temp_unschedulable_until IS NOT NULL
             OR (temp_unschedulable_request_id IS NOT NULL AND temp_unschedulable_request_id != '')
             OR temp_unschedulable_at IS NOT NULL
           )`)
	now := time.Now()
	result := query.Updates(map[string]any{
		"recover_success_streak":   gorm.Expr("recover_success_streak + 1"),
		"temp_unschedulable_until": nil,
		"next_probe_at":            nil,
		"probe_lease_until":        nil,
		"probe_status":             GatewayModelProbeStatusHealthy,
		"probe_request_id":         "",
		"updated_at":               now,
	})
	if result.Error != nil || result.RowsAffected == 0 {
		return true, result.Error
	}
	clearResult := r.db.Model(&GatewaySharedModelCooldown{}).
		Where("id = ? AND recover_success_streak >= ?", shared.ID, RouteRecoverSuccessClearStreak).
		Updates(map[string]any{
			"temp_unschedulable_until":      nil,
			"temp_unschedulable_reason":     "",
			"temp_unschedulable_at":         nil,
			"temp_unschedulable_request_id": "",
			"recover_success_streak":        0,
			"next_probe_at":                 nil,
			"last_probe_at":                 nil,
			"probe_lease_until":             nil,
			"probe_status":                  "",
			"probe_failure_count":           0,
			"probe_request_id":              "",
			"probe_inbound_protocol":        "openai_chat",
			"probe_last_status_code":        0,
			"probe_last_error":              "",
			"updated_at":                    now,
		})
	if clearResult.Error == nil {
		r.invalidateSharedModelCooldownScope(shared.ScopeKey)
	}
	return true, clearResult.Error
}

func (r *GatewayRoutes) clearSharedCooldownsForRoute(routeID uint, full bool) error {
	_, scope, err := r.routeAndSharedCooldownScope(routeID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil
		}
		return err
	}
	if scope == "" {
		return nil
	}
	updates := map[string]any{
		"temp_unschedulable_until": nil,
		"updated_at":               time.Now(),
	}
	if full {
		updates["temp_unschedulable_reason"] = ""
		updates["temp_unschedulable_at"] = nil
		updates["temp_unschedulable_request_id"] = ""
		updates["recover_success_streak"] = 0
		updates["next_probe_at"] = nil
		updates["probe_lease_until"] = nil
		updates["probe_status"] = GatewayModelProbeStatusManual
		updates["probe_request_id"] = ""
	}
	result := r.db.Model(&GatewaySharedModelCooldown{}).Where("scope_key = ?", scope).Updates(updates)
	if result.Error == nil && result.RowsAffected > 0 {
		r.invalidateSharedModelCooldownScope(scope)
	}
	return result.Error
}
