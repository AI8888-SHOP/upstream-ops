// 管理面：分组 CRUD 与排序。
package gateway

import (
	"errors"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

// CreateGroup 创建网关分组。
func (a *AdminService) CreateGroup(in CreateGroupInput) (*storage.GatewayGroup, error) {
	name := strings.TrimSpace(in.Name)
	if name == "" {
		return nil, errors.New("name is required")
	}
	if _, err := a.Groups.FindByName(name); err == nil {
		return nil, errors.New("name already exists")
	}
	dir := strings.ToLower(strings.TrimSpace(in.RateSortDirection))
	if dir != "desc" {
		dir = "asc"
	}
	mode := strings.ToLower(strings.TrimSpace(in.ModelsMode))
	if mode != storage.GatewayModelsModeManual && mode != storage.GatewayModelsModeHybrid {
		mode = storage.GatewayModelsModeAuto
	}
	retryEnabled := true
	if in.RetryEnabled != nil {
		retryEnabled = *in.RetryEnabled
	}
	failoverEnabled := true
	if in.FailoverEnabled != nil {
		failoverEnabled = *in.FailoverEnabled
	}
	failoverOn4xx := false
	if in.FailoverOn4xx != nil {
		failoverOn4xx = *in.FailoverOn4xx
	}
	gwDefaults := a.gatewayRuntime()
	retryCount, failoverMax, cooldown := 0, gwDefaults.MaxFailoverSwitches, gwDefaults.TempPauseSeconds
	if in.RetryCount != nil {
		retryCount = *in.RetryCount
	}
	if in.FailoverMax != nil {
		failoverMax = *in.FailoverMax
	}
	if in.CooldownSeconds != nil {
		cooldown = *in.CooldownSeconds
	}
	retryCount, failoverMax, cooldown = a.clampGroupRetryPolicy(retryCount, failoverMax, cooldown)
	ftTimeout := 0
	if in.FirstTokenTimeoutSec != nil {
		ftTimeout = a.clampFirstTokenTimeoutSec(*in.FirstTokenTimeoutSec)
	}
	hedgeEnabled := gwDefaults.Hedge.Enabled
	if in.HedgeEnabled != nil {
		hedgeEnabled = *in.HedgeEnabled
	}
	hedgeDelay, hedgeParallel, hedgeAttempts := gwDefaults.Hedge.DelaySeconds, gwDefaults.Hedge.MaxParallel, gwDefaults.Hedge.MaxAttempts
	if in.HedgeDelaySeconds != nil {
		hedgeDelay = *in.HedgeDelaySeconds
	}
	if in.HedgeMaxParallel != nil {
		hedgeParallel = *in.HedgeMaxParallel
	}
	if in.HedgeMaxAttempts != nil {
		hedgeAttempts = *in.HedgeMaxAttempts
	}
	hedgeDelay, hedgeParallel, hedgeAttempts = a.clampGroupHedgePolicy(hedgeDelay, hedgeParallel, hedgeAttempts)
	hedgeVirtualCache := false
	if in.HedgeVirtualCacheEnabled != nil {
		hedgeVirtualCache = *in.HedgeVirtualCacheEnabled
	}
	validationEnabled := gwDefaults.ResponseValidation.Enabled
	if in.ResponseValidationEnabled != nil {
		validationEnabled = *in.ResponseValidationEnabled
	}
	validationMode := gwDefaults.ResponseValidation.StreamMode
	if in.ResponseValidationStreamMode != nil {
		validationMode = *in.ResponseValidationStreamMode
	}
	prefixBytes, prefixTimeoutMS := gwDefaults.ResponseValidation.PrefixBytes, gwDefaults.ResponseValidation.PrefixTimeoutMS
	if in.ResponseValidationPrefixBytes != nil {
		prefixBytes = *in.ResponseValidationPrefixBytes
	}
	if in.ResponseValidationPrefixTimeoutMS != nil {
		prefixTimeoutMS = *in.ResponseValidationPrefixTimeoutMS
	}
	validationMode, prefixBytes, prefixTimeoutMS = a.clampGroupResponseValidationPolicy(validationMode, prefixBytes, prefixTimeoutMS)
	rateResort := false
	if in.RateResortEnabled != nil {
		rateResort = *in.RateResortEnabled
	}
	maxBillingRate := 0.0
	if in.MaxBillingRateMultiplier != nil {
		maxBillingRate = normalizeMaxBillingRateMultiplier(*in.MaxBillingRateMultiplier)
	}
	loadBalanceRouteCount := 1
	if in.LoadBalanceRouteCount != nil {
		loadBalanceRouteCount = normalizeLoadBalanceRouteCount(*in.LoadBalanceRouteCount)
	}
	pos, err := a.Groups.NextPosition()
	if err != nil {
		return nil, err
	}
	item := &storage.GatewayGroup{
		Name:                              name,
		Description:                       strings.TrimSpace(in.Description),
		Position:                          pos,
		Status:                            storage.GatewayGroupStatusActive,
		RateSortDirection:                 dir,
		RateResortEnabled:                 rateResort,
		MaxBillingRateMultiplier:          maxBillingRate,
		LoadBalanceRouteCount:             loadBalanceRouteCount,
		ModelMappingJSON:                  strings.TrimSpace(in.ModelMappingJSON),
		ModelsJSON:                        strings.TrimSpace(in.ModelsJSON),
		ModelsMode:                        mode,
		RetryEnabled:                      retryEnabled,
		RetryCount:                        retryCount,
		FailoverEnabled:                   failoverEnabled,
		FailoverMax:                       failoverMax,
		FailoverOn4xx:                     failoverOn4xx,
		CooldownSeconds:                   cooldown,
		FirstTokenTimeoutSec:              ftTimeout,
		HedgeEnabled:                      hedgeEnabled,
		HedgeDelaySeconds:                 hedgeDelay,
		HedgeMaxParallel:                  hedgeParallel,
		HedgeMaxAttempts:                  hedgeAttempts,
		HedgeVirtualCacheEnabled:          hedgeVirtualCache,
		ResponseValidationEnabled:         validationEnabled,
		ResponseValidationStreamMode:      validationMode,
		ResponseValidationPrefixBytes:     prefixBytes,
		ResponseValidationPrefixTimeoutMS: prefixTimeoutMS,
		UserAgent:                         strings.TrimSpace(in.UserAgent),
	}
	if err := a.Groups.Create(item); err != nil {
		return nil, err
	}
	return item, nil
}

// UpdateGroup 更新网关分组。
// CloneGroup creates an independent copy of a gateway group and generates a
// fresh client secret for every gateway key. The storage clone runs as one
// transaction; plaintext secrets remain in memory only long enough to attach
// them to the one-time response.
func (a *AdminService) CloneGroup(id uint, in CloneGroupInput) (*CloneGroupResult, error) {
	if a == nil || a.Service == nil || a.Groups == nil || a.Keys == nil {
		return nil, errors.New("gateway group service unavailable")
	}
	if _, err := a.Groups.FindByID(id); err != nil {
		return nil, err
	}
	sourceKeys, err := a.Keys.ListByGroupID(id)
	if err != nil {
		return nil, err
	}
	if len(sourceKeys) > 0 && a.Cipher == nil {
		return nil, errors.New("gateway key cipher unavailable")
	}

	templates := make([]storage.GatewayGroupCloneKey, 0, len(sourceKeys))
	secretsBySourceID := make(map[uint]string, len(sourceKeys))
	for _, sourceKey := range sourceKeys {
		secret, err := GenerateAPIKey(0)
		if err != nil {
			return nil, err
		}
		cipherText, err := a.Cipher.Encrypt(secret)
		if err != nil {
			return nil, err
		}
		templates = append(templates, storage.GatewayGroupCloneKey{
			SourceID:  sourceKey.ID,
			KeyHash:   HashAPIKey(secret),
			KeyPrefix: KeyPrefix(secret),
			KeyCipher: cipherText,
		})
		secretsBySourceID[sourceKey.ID] = secret
	}

	cloned, err := a.Groups.Clone(id, in.Name, templates)
	if err != nil {
		return nil, err
	}
	result := &CloneGroupResult{
		Group: &cloned.Group,
		Keys:  make([]CloneGroupKeyResult, 0, len(cloned.Keys)),
	}
	for i, key := range cloned.Keys {
		var secret string
		if i < len(cloned.KeySourceIDs) {
			secret = secretsBySourceID[cloned.KeySourceIDs[i]]
		}
		result.Keys = append(result.Keys, CloneGroupKeyResult{Key: key, Secret: secret})
	}
	// The destination has no runtime state yet, but invalidate any stale cache
	// entries in case a caller reuses a group ID after a failed/deleted clone.
	a.invalidateModelsCache(cloned.Group.ID)
	a.InvalidateResponseValidator(cloned.Group.ID)
	a.resetLoadBalanceGroup(cloned.Group.ID)
	return result, nil
}

func (a *AdminService) UpdateGroup(id uint, in UpdateGroupInput) (*storage.GatewayGroup, error) {
	item, err := a.Groups.FindByID(id)
	if err != nil {
		return nil, err
	}
	if in.Name != nil {
		name := strings.TrimSpace(*in.Name)
		if name == "" {
			return nil, errors.New("name is required")
		}
		if other, err := a.Groups.FindByName(name); err == nil && other.ID != id {
			return nil, errors.New("name already exists")
		}
		item.Name = name
	}
	if in.Description != nil {
		item.Description = strings.TrimSpace(*in.Description)
	}
	if in.Status != nil {
		st := strings.TrimSpace(*in.Status)
		if st != storage.GatewayGroupStatusActive && st != storage.GatewayGroupStatusDisabled {
			return nil, errors.New("invalid status")
		}
		item.Status = st
	}
	if in.RateSortDirection != nil {
		dir := strings.ToLower(strings.TrimSpace(*in.RateSortDirection))
		if dir != "desc" {
			dir = "asc"
		}
		item.RateSortDirection = dir
	}
	rateResortTurnedOn := false
	if in.RateResortEnabled != nil {
		rateResortTurnedOn = *in.RateResortEnabled && !item.RateResortEnabled
		item.RateResortEnabled = *in.RateResortEnabled
	}
	rateLimitChanged := false
	if in.MaxBillingRateMultiplier != nil {
		next := normalizeMaxBillingRateMultiplier(*in.MaxBillingRateMultiplier)
		rateLimitChanged = next != normalizeMaxBillingRateMultiplier(item.MaxBillingRateMultiplier)
		item.MaxBillingRateMultiplier = next
	}
	if in.LoadBalanceRouteCount != nil {
		item.LoadBalanceRouteCount = normalizeLoadBalanceRouteCount(*in.LoadBalanceRouteCount)
	}
	if in.ModelMappingJSON != nil {
		item.ModelMappingJSON = strings.TrimSpace(*in.ModelMappingJSON)
	}
	if in.ModelsJSON != nil {
		item.ModelsJSON = strings.TrimSpace(*in.ModelsJSON)
	}
	if in.ModelsMode != nil {
		mode := strings.ToLower(strings.TrimSpace(*in.ModelsMode))
		if mode != storage.GatewayModelsModeManual && mode != storage.GatewayModelsModeHybrid {
			mode = storage.GatewayModelsModeAuto
		}
		item.ModelsMode = mode
	}
	if in.RetryEnabled != nil {
		item.RetryEnabled = *in.RetryEnabled
	}
	if in.FailoverEnabled != nil {
		item.FailoverEnabled = *in.FailoverEnabled
	}
	if in.FailoverOn4xx != nil {
		item.FailoverOn4xx = *in.FailoverOn4xx
	}
	rc, fm, cd := item.RetryCount, item.FailoverMax, item.CooldownSeconds
	if in.RetryCount != nil {
		rc = *in.RetryCount
	}
	if in.FailoverMax != nil {
		fm = *in.FailoverMax
	}
	if in.CooldownSeconds != nil {
		cd = *in.CooldownSeconds
	}
	item.RetryCount, item.FailoverMax, item.CooldownSeconds = a.clampGroupRetryPolicy(rc, fm, cd)
	if in.FirstTokenTimeoutSec != nil {
		item.FirstTokenTimeoutSec = a.clampFirstTokenTimeoutSec(*in.FirstTokenTimeoutSec)
	}
	if in.HedgeEnabled != nil {
		item.HedgeEnabled = *in.HedgeEnabled
	}
	if in.HedgeVirtualCacheEnabled != nil {
		item.HedgeVirtualCacheEnabled = *in.HedgeVirtualCacheEnabled
	}
	hd, hp, ha := item.HedgeDelaySeconds, item.HedgeMaxParallel, item.HedgeMaxAttempts
	if in.HedgeDelaySeconds != nil {
		hd = *in.HedgeDelaySeconds
	}
	if in.HedgeMaxParallel != nil {
		hp = *in.HedgeMaxParallel
	}
	if in.HedgeMaxAttempts != nil {
		ha = *in.HedgeMaxAttempts
	}
	item.HedgeDelaySeconds, item.HedgeMaxParallel, item.HedgeMaxAttempts = a.clampGroupHedgePolicy(hd, hp, ha)
	if in.ResponseValidationEnabled != nil {
		item.ResponseValidationEnabled = *in.ResponseValidationEnabled
	}
	validationMode := item.ResponseValidationStreamMode
	if in.ResponseValidationStreamMode != nil {
		validationMode = *in.ResponseValidationStreamMode
	}
	prefixBytes, prefixTimeoutMS := item.ResponseValidationPrefixBytes, item.ResponseValidationPrefixTimeoutMS
	if in.ResponseValidationPrefixBytes != nil {
		prefixBytes = *in.ResponseValidationPrefixBytes
	}
	if in.ResponseValidationPrefixTimeoutMS != nil {
		prefixTimeoutMS = *in.ResponseValidationPrefixTimeoutMS
	}
	item.ResponseValidationStreamMode, item.ResponseValidationPrefixBytes, item.ResponseValidationPrefixTimeoutMS =
		a.clampGroupResponseValidationPolicy(validationMode, prefixBytes, prefixTimeoutMS)
	if in.UserAgent != nil {
		item.UserAgent = strings.TrimSpace(*in.UserAgent)
	}
	rateSortChanged := in.RateSortDirection != nil
	if err := a.Groups.Update(item); err != nil {
		return nil, err
	}
	// 排序方向变更，或刚打开「渠道分组价格倍率重排」时，立即按实时倍率落库顺序
	if rateSortChanged || rateResortTurnedOn {
		if err := a.reorderRoutesPersisted(id); err != nil && a.Log != nil {
			a.Log.Warn("reorder routes after group update", "group_id", id, "err", err)
		}
	}
	if rateLimitChanged {
		if err := a.applyRateLimitForGroup(id); err != nil && a.Log != nil {
			a.Log.Warn("apply gateway group multiplier limit", "group_id", id, "err", err)
		}
	}
	a.invalidateModelsCache(id)
	a.InvalidateResponseValidator(id)
	a.resetLoadBalanceGroup(id)
	return item, nil
}

// DeleteGroup 删除分组。
func (a *AdminService) DeleteGroup(id uint) error {
	a.invalidateModelsCache(id)
	a.resetLoadBalanceGroup(id)
	if err := a.Groups.Delete(id); err != nil {
		return err
	}
	a.InvalidateResponseValidator(id)
	return nil
}

// ListGroups 列出分组。
func (a *AdminService) ListGroups() ([]storage.GatewayGroup, error) {
	return a.Groups.List()
}

// ReorderGroups 按 ids 顺序重写网关组侧栏排序。

func (a *AdminService) ReorderGroups(ids []uint) error {
	if len(ids) == 0 {
		return errors.New("ids is required")
	}
	return a.Groups.Reorder(ids)
}

// GetGroup 获取分组。
func (a *AdminService) GetGroup(id uint) (*storage.GatewayGroup, error) {
	return a.Groups.FindByID(id)
}

// ---------- admin: keys ----------
