package storage

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Candy checks are opt-in and scoped to a single gateway route. They never
// share transport/model cooldown state or contribute to customer usage.
type GatewayCandyCheckPolicy struct {
	CandyCheckEnabled         bool   `gorm:"not null;default:false" json:"candy_check_enabled"`
	CandyCheckModel           string `gorm:"size:256;not null;default:''" json:"candy_check_model"`
	CandyCheckIntervalMinutes int    `gorm:"not null;default:5" json:"candy_check_interval_minutes"`
	CandyCheckCooldownMinutes int    `gorm:"not null;default:10" json:"candy_check_cooldown_minutes"`
	CandyCheckReasoningEffort string `gorm:"size:16;not null;default:'medium'" json:"candy_check_reasoning_effort"`
}

type GatewayRouteCandyCheck struct {
	RouteID       uint       `gorm:"primaryKey;autoIncrement:false" json:"route_id"`
	ConfigKey     string     `gorm:"size:64;not null" json:"-"`
	Model         string     `gorm:"size:256;not null;default:''" json:"model"`
	Status        string     `gorm:"size:16;not null;default:'pending'" json:"status"`
	AnswerPreview string     `gorm:"type:text" json:"answer_preview,omitempty"`
	Reason        string     `gorm:"type:text" json:"reason,omitempty"`
	StatusCode    int        `gorm:"not null;default:0" json:"status_code"`
	LatencyMS     int64      `gorm:"not null;default:0" json:"latency_ms"`
	CheckedAt     *time.Time `json:"checked_at,omitempty"`
	NextCheckAt   *time.Time `gorm:"index" json:"next_check_at,omitempty"`
	CooldownUntil *time.Time `json:"cooldown_until,omitempty"`
	LeaseUntil    *time.Time `json:"lease_until,omitempty"`
	LeaseToken    string     `gorm:"size:64;not null;default:''" json:"-"`
	Active        bool       `gorm:"-" json:"active"`
}

func (GatewayRouteCandyCheck) TableName() string { return "gateway_route_candy_checks" }

func (c *GatewayRouteCandyCheck) Blocks(now time.Time) bool {
	return c != nil && c.Active && c.CooldownUntil != nil && c.CooldownUntil.After(now)
}

// A changed policy, credential, model mapping or source must invalidate an old
// result, including a probe that finishes after the operator saves new settings.
func GatewayCandyCheckConfigKey(group *GatewayGroup, route *GatewayRoute, provider *GatewayProvider) string {
	if group == nil || route == nil {
		return ""
	}
	values := []any{group.ID, group.Status, group.GatewayCandyCheckPolicy, group.ModelMappingJSON, group.UserAgent,
		route.ID, route.Enabled, route.RateLimitAutoDisabled, route.NormalizeSourceKind(), route.SourceChannelID,
		route.GatewayProviderID, route.SourceGroupID, route.SourceGroupName, route.SourceAPIKeyCipher,
		route.ModelMappingJSON, route.ModelPolicy, route.AllowedModelsJSON, route.UpstreamProtocol,
		route.UserAgentMode, route.UserAgentCustom}
	if provider != nil {
		values = append(values, provider.Enabled, provider.BaseURL, provider.APIKeyCipher, provider.UpstreamProtocol, provider.ModelPolicy, provider.AllowedModelsJSON)
	}
	body, _ := json.Marshal(values)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (r *GatewayRoutes) loadCandyChecks(routes []GatewayRoute) error {
	if len(routes) == 0 {
		return nil
	}
	ids := make([]uint, 0, len(routes))
	for _, route := range routes {
		ids = append(ids, route.ID)
	}
	var rows []GatewayRouteCandyCheck
	if err := r.db.Where("route_id IN ?", ids).Find(&rows).Error; err != nil {
		return err
	}
	if len(rows) == 0 {
		return nil
	}
	states := make(map[uint]GatewayRouteCandyCheck, len(rows))
	for _, row := range rows {
		states[row.RouteID] = row
	}
	groups := make(map[uint]*GatewayGroup)
	providers := make(map[uint]*GatewayProvider)
	groupIDs, providerIDs := make(map[uint]struct{}), make(map[uint]struct{})
	for _, route := range routes {
		if _, exists := states[route.ID]; !exists {
			continue
		}
		groupIDs[route.GatewayGroupID] = struct{}{}
		if route.NormalizeSourceKind() == GatewayRouteSourceProvider {
			providerIDs[route.GatewayProviderID] = struct{}{}
		}
	}
	ids = ids[:0]
	for id := range groupIDs {
		ids = append(ids, id)
	}
	var groupRows []GatewayGroup
	if len(ids) > 0 {
		if err := r.db.Where("id IN ?", ids).Find(&groupRows).Error; err != nil {
			return err
		}
	}
	for i := range groupRows {
		groups[groupRows[i].ID] = &groupRows[i]
	}
	ids = ids[:0]
	for id := range providerIDs {
		ids = append(ids, id)
	}
	var providerRows []GatewayProvider
	if len(ids) > 0 {
		if err := r.db.Where("id IN ?", ids).Find(&providerRows).Error; err != nil {
			return err
		}
	}
	for i := range providerRows {
		providers[providerRows[i].ID] = &providerRows[i]
	}
	for i := range routes {
		route := &routes[i]
		state, ok := states[route.ID]
		if !ok {
			continue
		}
		group := groups[route.GatewayGroupID]
		if group == nil {
			continue
		}
		var provider *GatewayProvider
		if route.NormalizeSourceKind() == GatewayRouteSourceProvider {
			provider = providers[route.GatewayProviderID]
			if provider == nil {
				continue
			}
		}
		state.Active = group.CandyCheckEnabled && group.Status == GatewayGroupStatusActive && state.ConfigKey == GatewayCandyCheckConfigKey(group, route, provider)
		route.CandyCheck = &state
	}
	return nil
}

// ListCandyCheckRoutes reads configured routes in due-time order. No usage-log
// scans and no remote model-list fetches are required by the periodic worker.
func (r *GatewayRoutes) ListCandyCheckRoutes() ([]GatewayRoute, error) {
	var routes []GatewayRoute
	err := r.db.Model(&GatewayRoute{}).
		Select("gateway_routes.*").
		Joins("JOIN gateway_groups g ON g.id = gateway_routes.gateway_group_id").
		Joins("LEFT JOIN gateway_route_candy_checks c ON c.route_id = gateway_routes.id").
		Where("g.candy_check_enabled = ? AND g.status = ? AND gateway_routes.enabled = ? AND gateway_routes.rate_limit_auto_disabled = ?", true, GatewayGroupStatusActive, true, false).
		Order("COALESCE(c.next_check_at, gateway_routes.created_at) ASC, gateway_routes.id ASC").Find(&routes).Error
	if err == nil {
		err = r.loadCandyChecks(routes)
	}
	return routes, err
}

func lockCandyContext(tx *gorm.DB, routeID uint) (*GatewayGroup, *GatewayRoute, *GatewayProvider, error) {
	var route GatewayRoute
	if err := tx.First(&route, routeID).Error; err != nil {
		return nil, nil, nil, err
	}
	var group GatewayGroup
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&group, route.GatewayGroupID).Error; err != nil {
		return nil, nil, nil, err
	}
	if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&route, routeID).Error; err != nil {
		return nil, nil, nil, err
	}
	var provider *GatewayProvider
	if route.NormalizeSourceKind() == GatewayRouteSourceProvider {
		provider = &GatewayProvider{}
		if err := tx.Clauses(clause.Locking{Strength: "SHARE"}).First(provider, route.GatewayProviderID).Error; err != nil {
			return nil, nil, nil, err
		}
	}
	return &group, &route, provider, nil
}

func (r *GatewayRoutes) ClaimCandyCheck(routeID uint, configKey, model string, now time.Time, lease time.Duration) (*GatewayRouteCandyCheck, error) {
	var claimed *GatewayRouteCandyCheck
	err := r.db.Transaction(func(tx *gorm.DB) error {
		group, route, provider, err := lockCandyContext(tx, routeID)
		if err != nil {
			return err
		}
		if !group.CandyCheckEnabled || group.Status != GatewayGroupStatusActive || !route.Enabled || route.RateLimitAutoDisabled || configKey != GatewayCandyCheckConfigKey(group, route, provider) {
			return nil
		}
		if provider != nil && !provider.Enabled {
			return nil
		}
		var state GatewayRouteCandyCheck
		err = tx.First(&state, "route_id = ?", routeID).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if state.LeaseUntil != nil && state.LeaseUntil.After(now) {
			return nil
		}
		if state.ConfigKey == configKey && state.NextCheckAt != nil && state.NextCheckAt.After(now) {
			return nil
		}
		if state.ConfigKey != configKey {
			state = GatewayRouteCandyCheck{RouteID: routeID, ConfigKey: configKey, Status: "pending"}
		}
		token := make([]byte, 16)
		if _, err := rand.Read(token); err != nil {
			return err
		}
		until := now.Add(lease)
		state.Model, state.LeaseUntil, state.LeaseToken = model, &until, hex.EncodeToString(token)
		if err := tx.Save(&state).Error; err != nil {
			return err
		}
		claimed = &state
		return nil
	})
	if err == nil && claimed != nil {
		r.readCaches.invalidateGatewayRoute(routeID)
	}
	return claimed, err
}

func (r *GatewayRoutes) FinishCandyCheck(claim GatewayRouteCandyCheck, result GatewayRouteCandyCheck, now time.Time) (bool, error) {
	updated := false
	err := r.db.Transaction(func(tx *gorm.DB) error {
		group, route, provider, err := lockCandyContext(tx, claim.RouteID)
		if err != nil {
			return err
		}
		if !group.CandyCheckEnabled || group.Status != GatewayGroupStatusActive || claim.ConfigKey != GatewayCandyCheckConfigKey(group, route, provider) {
			return nil
		}
		next := now.Add(time.Duration(group.CandyCheckIntervalMinutes) * time.Minute)
		var until *time.Time
		if result.Status == "incorrect" || result.Status == "error" {
			end := now.Add(time.Duration(group.CandyCheckCooldownMinutes) * time.Minute)
			until = &end
			if end.After(next) {
				next = end
			}
		}
		// Busy or locally cancelled probes do not alter the previous verdict or
		// clear an existing cooldown; retry later without penalizing the upstream.
		updates := map[string]any{"lease_until": nil, "lease_token": "", "next_check_at": next}
		if result.Status != "deferred" {
			updates["checked_at"], updates["cooldown_until"] = now, until
			updates["status"], updates["answer_preview"], updates["reason"] = result.Status, result.AnswerPreview, result.Reason
			updates["status_code"], updates["latency_ms"] = result.StatusCode, result.LatencyMS
		}
		q := tx.Model(&GatewayRouteCandyCheck{}).Where("route_id = ? AND config_key = ? AND lease_token = ?", claim.RouteID, claim.ConfigKey, claim.LeaseToken).Updates(updates)
		updated = q.RowsAffected > 0
		return q.Error
	})
	if err == nil && updated {
		r.readCaches.invalidateGatewayRoute(claim.RouteID)
	}
	return updated, err
}

func (r *GatewayRoutes) ClearCandyCheck(routeID uint, now time.Time) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		group, _, _, err := lockCandyContext(tx, routeID)
		if err != nil {
			return err
		}
		interval := group.CandyCheckIntervalMinutes
		if interval < 1 {
			interval = 5
		}
		return tx.Model(&GatewayRouteCandyCheck{}).Where("route_id = ?", routeID).Updates(map[string]any{
			"cooldown_until": nil, "lease_until": nil, "lease_token": "", "status": "manual",
			"next_check_at": now.Add(time.Duration(interval) * time.Minute),
		}).Error
	})
	if err == nil {
		r.readCaches.invalidateGatewayRoute(routeID)
	}
	return err
}
