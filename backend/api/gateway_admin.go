// 管理端网关 HTTP 适配层：路由注册与 handler 委托 gateway.Service / storage。
// 风格与 channels.go 等一致：func(c *gin.Context, d *Deps)，不引入 handler struct。
package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// registerGatewayAdmin 在管理 API 下注册 /gateway/* 路由。
func registerGatewayAdmin(g *gin.RouterGroup, d *Deps) {
	if d.Gateway == nil {
		return
	}
	gp := g.Group("/gateway")
	{
		// groups（reorder 须在 :id 之前注册，避免被当成 id）
		gp.GET("/groups", func(c *gin.Context) { listGatewayGroups(c, d) })
		gp.POST("/groups", func(c *gin.Context) { createGatewayGroup(c, d) })
		gp.POST("/groups/:id/clone", func(c *gin.Context) { cloneGatewayGroup(c, d) })
		gp.PUT("/groups/reorder", func(c *gin.Context) { reorderGatewayGroups(c, d) })
		gp.GET("/groups/:id", func(c *gin.Context) { getGatewayGroup(c, d) })
		gp.PUT("/groups/:id", func(c *gin.Context) { updateGatewayGroup(c, d) })
		gp.DELETE("/groups/:id", func(c *gin.Context) { deleteGatewayGroup(c, d) })

		// keys under group
		gp.GET("/groups/:id/keys", func(c *gin.Context) { listGatewayGroupKeys(c, d) })
		gp.POST("/groups/:id/keys", func(c *gin.Context) { createGatewayGroupKey(c, d) })

		// routes under group
		gp.GET("/groups/:id/routes", func(c *gin.Context) { listGatewayGroupRoutes(c, d) })
		gp.PUT("/groups/:id/routes", func(c *gin.Context) { saveGatewayGroupRoutes(c, d) })
		gp.POST("/groups/:id/routes/ensure-keys", func(c *gin.Context) { ensureGatewayGroupRouteKeys(c, d) })

		// models
		gp.GET("/groups/:id/models/preview", func(c *gin.Context) { previewGatewayGroupModels(c, d) })
		gp.POST("/groups/:id/models/sync", func(c *gin.Context) { syncGatewayGroupModels(c, d) })
		gp.POST("/groups/:id/models/test", func(c *gin.Context) { testGatewayGroupModel(c, d) })

		// response validation rules
		gp.GET("/groups/:id/response-rules", func(c *gin.Context) { listGatewayGroupResponseRules(c, d) })
		gp.POST("/groups/:id/response-rules", func(c *gin.Context) { createGatewayGroupResponseRule(c, d) })
		gp.GET("/groups/:id/response-rules/export", func(c *gin.Context) { exportGatewayGroupResponseRules(c, d) })
		gp.POST("/groups/:id/response-rules/import", func(c *gin.Context) { importGatewayGroupResponseRules(c, d) })
		gp.GET("/response-rules/:id", func(c *gin.Context) { getGatewayResponseRule(c, d) })
		gp.PUT("/response-rules/:id", func(c *gin.Context) { updateGatewayResponseRule(c, d) })
		gp.DELETE("/response-rules/:id", func(c *gin.Context) { deleteGatewayResponseRule(c, d) })

		// key ops
		gp.PUT("/keys/:id", func(c *gin.Context) { updateGatewayKey(c, d) })
		gp.DELETE("/keys/:id", func(c *gin.Context) { deleteGatewayKey(c, d) })
		gp.POST("/keys/:id/reveal", func(c *gin.Context) { revealGatewayKey(c, d) })

		// route ops
		gp.POST("/routes/:id/clear-pause", func(c *gin.Context) { clearGatewayRoutePause(c, d) })

		// providers（直连渠道）— options 须在 :id 之前注册
		gp.GET("/providers/options", func(c *gin.Context) { listGatewayProviderOptions(c, d) })
		gp.GET("/providers/:id/models/preview", func(c *gin.Context) { previewGatewayProviderModels(c, d) })
		gp.GET("/providers/:id/cache-health", func(c *gin.Context) { gatewayProviderCacheHealth(c, d) })
		gp.POST("/providers/:id/cache-health/clear", func(c *gin.Context) { clearGatewayProviderCacheHealth(c, d) })
		gp.GET("/providers", func(c *gin.Context) { listGatewayProviders(c, d) })
		gp.POST("/providers", func(c *gin.Context) { createGatewayProvider(c, d) })
		gp.PUT("/providers/:id", func(c *gin.Context) { updateGatewayProvider(c, d) })
		gp.DELETE("/providers/:id", func(c *gin.Context) { deleteGatewayProvider(c, d) })
		gp.POST("/providers/:id/reveal", func(c *gin.Context) { revealGatewayProvider(c, d) })

		// usage
		gp.GET("/usage", func(c *gin.Context) { listGatewayUsage(c, d) })
		gp.GET("/usage/stats", func(c *gin.Context) { statsGatewayUsage(c, d) })
		gp.GET("/usage/models", func(c *gin.Context) { listGatewayUsageModels(c, d) })
		gp.POST("/usage/cleanup", func(c *gin.Context) { cleanupGatewayUsage(c, d) })
		gp.GET("/cache-health", func(c *gin.Context) { listGatewayCacheHealth(c, d) })
		gp.POST("/cache-health/clear", func(c *gin.Context) { clearGatewayCacheHealth(c, d) })

		// prices
		gp.GET("/prices", func(c *gin.Context) { listGatewayPrices(c, d) })
		gp.GET("/prices/defaults", func(c *gin.Context) { listGatewayDefaultPrices(c, d) })
		gp.PUT("/prices", func(c *gin.Context) { upsertGatewayPrice(c, d) })
		gp.DELETE("/prices/:id", func(c *gin.Context) { deleteGatewayPrice(c, d) })
	}
}

type gatewayResponseRuleCreateInput struct {
	Name          string   `json:"name"`
	Enabled       *bool    `json:"enabled"`
	Priority      int      `json:"priority"`
	Pattern       string   `json:"pattern"`
	Target        string   `json:"target"`
	ModelsJSON    string   `json:"models_json"`
	ProtocolsJSON string   `json:"protocols_json"`
	Models        []string `json:"models"`
	Protocols     []string `json:"protocols"`
}

type gatewayResponseRuleUpdateInput struct {
	Name          *string   `json:"name"`
	Enabled       *bool     `json:"enabled"`
	Priority      *int      `json:"priority"`
	Pattern       *string   `json:"pattern"`
	Target        *string   `json:"target"`
	ModelsJSON    *string   `json:"models_json"`
	ProtocolsJSON *string   `json:"protocols_json"`
	Models        *[]string `json:"models"`
	Protocols     *[]string `json:"protocols"`
}

type gatewayResponseRuleImportInput struct {
	Kind              string                                  `json:"kind"`
	Version           int                                     `json:"version"`
	ExportedAt        time.Time                               `json:"exported_at"`
	Rules             []storage.GatewayResponseRuleBundleRule `json:"rules"`
	Strategy          string                                  `json:"strategy"`
	DuplicateStrategy string                                  `json:"duplicate_strategy"`
	Bundle            *storage.GatewayResponseRuleBundle      `json:"bundle"`
}

type gatewayResponseRuleView struct {
	storage.GatewayResponseRule
	Models    []string `json:"models"`
	Protocols []string `json:"protocols"`
}

func responseRuleView(item storage.GatewayResponseRule) gatewayResponseRuleView {
	view := gatewayResponseRuleView{GatewayResponseRule: item, Models: []string{}, Protocols: []string{}}
	_ = json.Unmarshal([]byte(item.ModelsJSON), &view.Models)
	_ = json.Unmarshal([]byte(item.ProtocolsJSON), &view.Protocols)
	return view
}

func responseRuleViews(items []storage.GatewayResponseRule) []gatewayResponseRuleView {
	views := make([]gatewayResponseRuleView, 0, len(items))
	for _, item := range items {
		views = append(views, responseRuleView(item))
	}
	return views
}

func encodeResponseRuleList(values []string) (string, error) {
	encoded, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func gatewayResponseRuleRepo(d *Deps) *storage.GatewayResponseRules {
	if d == nil {
		return nil
	}
	if d.GatewayResponseRules != nil {
		return d.GatewayResponseRules
	}
	if d.DB == nil {
		return nil
	}
	return storage.NewGatewayResponseRules(d.DB)
}

func listGatewayGroupResponseRules(c *gin.Context, d *Deps) {
	groupID, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group id"})
		return
	}
	if _, err := d.Gateway.GetGroup(groupID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "gateway group not found"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	items, err := repo.ListByGroupID(groupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": responseRuleViews(items)})
}

func exportGatewayGroupResponseRules(c *gin.Context, d *Deps) {
	groupID, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group id"})
		return
	}
	if _, err := d.Gateway.GetGroup(groupID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "gateway group not found"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	bundle, err := repo.Export(groupID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Keep the response usable both as a browser download and as an apiFetch
	// JSON response. The filename contains only the numeric group ID.
	c.Header("Content-Disposition", "attachment; filename=\"upstream-ops-response-rules-"+strconv.FormatUint(uint64(groupID), 10)+".json\"")
	c.Header("Cache-Control", "no-store")
	c.JSON(http.StatusOK, bundle)
}

func importGatewayGroupResponseRules(c *gin.Context, d *Deps) {
	groupID, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group id"})
		return
	}
	if _, err := d.Gateway.GetGroup(groupID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "gateway group not found"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	// A rule pattern is capped at 16 KiB and each bundle at 512 rules. Keep a
	// request-level cap as an additional guard against oversized JSON uploads.
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 4<<20)
	var in gatewayResponseRuleImportInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	bundle := storage.GatewayResponseRuleBundle{
		Kind: in.Kind, Version: in.Version, ExportedAt: in.ExportedAt, Rules: in.Rules,
	}
	if in.Bundle != nil {
		// A wrapper payload ({"bundle": {...}, "strategy": "replace"}) is
		// accepted in addition to posting the exported bundle directly.
		bundle = *in.Bundle
	}
	strategy := strings.TrimSpace(in.Strategy)
	if strategy == "" {
		strategy = strings.TrimSpace(in.DuplicateStrategy)
	}
	if strategy == "" {
		strategy = strings.TrimSpace(c.Query("strategy"))
	}
	if strategy == "" {
		strategy = string(storage.GatewayResponseRuleImportSkip)
	}
	result, err := repo.Import(groupID, bundle, storage.GatewayResponseRuleImportStrategy(strategy))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if result.Created > 0 || result.Replaced > 0 {
		d.Gateway.InvalidateResponseValidator(groupID)
	}
	c.JSON(http.StatusOK, gin.H{
		"strategy": result.Strategy,
		"created":  result.Created,
		"replaced": result.Replaced,
		"renamed":  result.Renamed,
		"skipped":  result.Skipped,
		"items":    responseRuleViews(result.Items),
	})
}

func createGatewayGroupResponseRule(c *gin.Context, d *Deps) {
	groupID, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group id"})
		return
	}
	if _, err := d.Gateway.GetGroup(groupID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "gateway group not found"})
		return
	}
	var in gatewayResponseRuleCreateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	enabled := true
	if in.Enabled != nil {
		enabled = *in.Enabled
	}
	modelsJSON, protocolsJSON := in.ModelsJSON, in.ProtocolsJSON
	if in.Models != nil {
		modelsJSON, err = encodeResponseRuleList(in.Models)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	if in.Protocols != nil {
		protocolsJSON, err = encodeResponseRuleList(in.Protocols)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	item := &storage.GatewayResponseRule{
		GatewayGroupID: groupID,
		Name:           in.Name,
		Enabled:        enabled,
		Priority:       in.Priority,
		Pattern:        in.Pattern,
		Target:         in.Target,
		ModelsJSON:     modelsJSON,
		ProtocolsJSON:  protocolsJSON,
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	if err := repo.Create(item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	d.Gateway.InvalidateResponseValidator(groupID)
	c.JSON(http.StatusCreated, responseRuleView(*item))
}

func getGatewayResponseRule(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid response rule id"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	item, err := repo.FindByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "response rule not found"})
		return
	}
	c.JSON(http.StatusOK, responseRuleView(*item))
}

func updateGatewayResponseRule(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid response rule id"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	item, err := repo.FindByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "response rule not found"})
		return
	}
	var in gatewayResponseRuleUpdateInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.Name != nil {
		item.Name = *in.Name
	}
	if in.Enabled != nil {
		item.Enabled = *in.Enabled
	}
	if in.Priority != nil {
		item.Priority = *in.Priority
	}
	if in.Pattern != nil {
		item.Pattern = *in.Pattern
	}
	if in.Target != nil {
		item.Target = *in.Target
	}
	if in.ModelsJSON != nil {
		item.ModelsJSON = *in.ModelsJSON
	}
	if in.ProtocolsJSON != nil {
		item.ProtocolsJSON = *in.ProtocolsJSON
	}
	if in.Models != nil {
		encoded, err := encodeResponseRuleList(*in.Models)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		item.ModelsJSON = encoded
	}
	if in.Protocols != nil {
		encoded, err := encodeResponseRuleList(*in.Protocols)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		item.ProtocolsJSON = encoded
	}
	if err := repo.Update(item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	d.Gateway.InvalidateResponseValidator(item.GatewayGroupID)
	c.JSON(http.StatusOK, responseRuleView(*item))
}

func deleteGatewayResponseRule(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid response rule id"})
		return
	}
	repo := gatewayResponseRuleRepo(d)
	if repo == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "response rule repository unavailable"})
		return
	}
	item, err := repo.FindByID(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "response rule not found"})
		return
	}
	if err := repo.Delete(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	d.Gateway.InvalidateResponseValidator(item.GatewayGroupID)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func listGatewayProviders(c *gin.Context, d *Deps) {
	q := storage.GatewayProviderQuery{
		Q:        strings.TrimSpace(c.Query("q")),
		Page:     queryInt(c, "page", 1),
		PageSize: queryInt(c, "page_size", 20),
	}
	page, err := d.Gateway.ListProviders(q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// Enrich the direct-channel list with the same rolling cache statistics
	// exposed by /cache-health. Failure to read optional stats must not hide the
	// provider list itself.
	if d.Gateway != nil && d.Gateway.Usage != nil && len(page.Items) > 0 {
		ids := make([]uint, 0, len(page.Items))
		for _, item := range page.Items {
			ids = append(ids, item.ID)
		}
		if stats, statErr := d.Gateway.CacheHealthStats(storage.GatewayRouteSourceProvider, ids); statErr == nil {
			byID := make(map[uint]gateway.CacheHealthStat, len(stats))
			for _, stat := range stats {
				byID[stat.SourceID] = stat
			}
			for i := range page.Items {
				if stat, ok := byID[page.Items[i].ID]; ok {
					page.Items[i].CacheHitRate = stat.HitRate
					page.Items[i].CacheHealthRequestCount = stat.RequestCount
					page.Items[i].CacheHealthInputTokens = stat.InputTokens
					page.Items[i].CacheHealthReadTokens = stat.CacheReadTokens
					page.Items[i].CacheHealthCreationTokens = stat.CacheCreationTokens
					page.Items[i].CacheHealthEvaluatedAt = stat.EvaluatedAt
					page.Items[i].CacheHealthBlacklistedUntil = stat.BlacklistedUntil
					page.Items[i].CacheHealthBlacklistReason = stat.BlacklistReason
				}
			}
		}
	}
	c.JSON(http.StatusOK, page)
}

func listGatewayCacheHealth(c *gin.Context, d *Deps) {
	kind := strings.TrimSpace(c.Query("source_kind"))
	if kind == "" {
		kind = storage.GatewayRouteSourceProvider
	}
	if !strings.EqualFold(kind, storage.GatewayRouteSourceProvider) &&
		!strings.EqualFold(kind, storage.GatewayRouteSourceMonitor) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source_kind"})
		return
	}
	var ids []uint
	if raw := strings.TrimSpace(c.Query("source_id")); raw != "" {
		if id, err := strconv.ParseUint(raw, 10, 64); err == nil && id > 0 {
			ids = append(ids, uint(id))
		}
	}
	if raw := strings.TrimSpace(c.Query("source_ids")); raw != "" {
		for _, part := range strings.Split(raw, ",") {
			if id, err := strconv.ParseUint(strings.TrimSpace(part), 10, 64); err == nil && id > 0 {
				ids = append(ids, uint(id))
			}
		}
	}
	stats, err := d.Gateway.CacheHealthStats(kind, uniqueUintIDs(ids))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"items":   stats,
		"enabled": d.Gateway.CacheHealthEnabled(),
	})
}

func gatewayProviderCacheHealth(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid provider id"})
		return
	}
	stats, err := d.Gateway.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{id})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	var item *gateway.CacheHealthStat
	if len(stats) > 0 {
		item = &stats[0]
	}
	c.JSON(http.StatusOK, gin.H{"item": item, "enabled": d.Gateway.CacheHealthEnabled()})
}

func clearGatewayProviderCacheHealth(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil || id == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid provider id"})
		return
	}
	if err := d.Gateway.ClearCacheHealthBlacklist(storage.GatewayRouteSourceProvider, id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

type clearGatewayCacheHealthInput struct {
	SourceKind string `json:"source_kind"`
	SourceID   uint   `json:"source_id"`
}

func clearGatewayCacheHealth(c *gin.Context, d *Deps) {
	var input clearGatewayCacheHealthInput
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	kind := strings.ToLower(strings.TrimSpace(input.SourceKind))
	if kind != storage.GatewayRouteSourceProvider && kind != storage.GatewayRouteSourceMonitor {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid source_kind"})
		return
	}
	if input.SourceID == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source_id is required"})
		return
	}
	if err := d.Gateway.ClearCacheHealthBlacklist(kind, input.SourceID); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func uniqueUintIDs(ids []uint) []uint {
	if len(ids) < 2 {
		return ids
	}
	seen := make(map[uint]struct{}, len(ids))
	out := make([]uint, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func listGatewayProviderOptions(c *gin.Context, d *Deps) {
	list, err := d.Gateway.ListProviderOptions(strings.TrimSpace(c.Query("q")))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	// 轻量字段：不返回 cipher
	type opt struct {
		ID                 uint    `json:"id"`
		Name               string  `json:"name"`
		BaseURL            string  `json:"base_url"`
		APIKeyHint         string  `json:"api_key_hint"`
		UpstreamProtocol   string  `json:"upstream_protocol"`
		ConcurrencyLimit   int     `json:"concurrency_limit"`
		DefaultBillingRate float64 `json:"default_billing_rate"`
		Enabled            bool    `json:"enabled"`
	}
	items := make([]opt, 0, len(list))
	for _, p := range list {
		items = append(items, opt{
			ID: p.ID, Name: p.Name, BaseURL: p.BaseURL, APIKeyHint: p.APIKeyHint,
			UpstreamProtocol: p.UpstreamProtocol, ConcurrencyLimit: p.ConcurrencyLimit,
			DefaultBillingRate: p.DefaultBillingRate, Enabled: p.Enabled,
		})
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func createGatewayProvider(c *gin.Context, d *Deps) {
	var in gateway.CreateProviderInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := d.Gateway.CreateProvider(in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, item)
}

func updateGatewayProvider(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var in gateway.UpdateProviderInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := d.Gateway.UpdateProvider(id, in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, item)
}

func previewGatewayProviderModels(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	preview, err := d.Gateway.PreviewProviderModels(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, preview)
}

func deleteGatewayProvider(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := d.Gateway.DeleteProvider(id); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func revealGatewayProvider(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	secret, err := d.Gateway.RevealProviderKey(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"secret": secret})
}

func listGatewayGroups(c *gin.Context, d *Deps) {
	list, err := d.Gateway.ListGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func reorderGatewayGroups(c *gin.Context, d *Deps) {
	var body struct {
		IDs []uint `json:"ids"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := d.Gateway.ReorderGroups(body.IDs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	list, err := d.Gateway.ListGroups()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func createGatewayGroup(c *gin.Context, d *Deps) {
	var in gateway.CreateGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := d.Gateway.CreateGroup(in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, item)
}

func cloneGatewayGroup(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid group id"})
		return
	}
	var in gateway.CloneGroupInput
	if err := c.ShouldBindJSON(&in); err != nil && !errors.Is(err, io.EOF) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	result, err := d.Gateway.CloneGroup(id, in)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "gateway group not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, result)
}

func getGatewayGroup(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	item, err := d.Gateway.GetGroup(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusOK, item)
}

func updateGatewayGroup(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var in gateway.UpdateGroupInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := d.Gateway.UpdateGroup(id, in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, item)
}

func deleteGatewayGroup(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := d.Gateway.DeleteGroup(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func listGatewayGroupKeys(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	list, err := d.Gateway.ListKeysByGroup(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func createGatewayGroupKey(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var in gateway.CreateKeyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	in.GroupID = id
	res, err := d.Gateway.CreateKey(in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusCreated, res)
}

func listGatewayGroupRoutes(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	list, err := d.Gateway.ListRoutes(id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func saveGatewayGroupRoutes(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var body struct {
		Routes []gateway.RouteInput `json:"routes"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	list, err := d.Gateway.SaveRoutes(id, body.Routes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func ensureGatewayGroupRouteKeys(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	// 单条失败跳过，结果在 routes 明细中；不因个别路由失败而整体 4xx
	res, err := d.Gateway.EnsureRouteKeys(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)
}

func previewGatewayGroupModels(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	list, err := d.Gateway.PreviewGroupModels(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func syncGatewayGroupModels(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	// body 可选；空 body / 非法 JSON 时仅保留库内已保存的 custom
	var in gateway.SyncGroupModelsInput
	_ = c.ShouldBindJSON(&in)
	// 单渠道失败会跳过并在 routes 明细中体现，不因个别渠道失败而整体 4xx
	res, err := d.Gateway.SyncGroupModels(c.Request.Context(), id, in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)
}

func testGatewayGroupModel(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var in gateway.TestModelInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	results, err := d.Gateway.TestGroupModel(c.Request.Context(), id, in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	okCount := 0
	for _, r := range results {
		if r.OK {
			okCount++
		}
	}
	c.JSON(http.StatusOK, gin.H{
		"items":    results,
		"ok_count": okCount,
		"total":    len(results),
		"all_ok":   okCount == len(results) && len(results) > 0,
	})
}

func updateGatewayKey(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	var in gateway.UpdateKeyInput
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	item, err := d.Gateway.UpdateKey(id, in)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, item)
}

func deleteGatewayKey(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := d.Gateway.DeleteKey(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func revealGatewayKey(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	secret, err := d.Gateway.RevealKey(id)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"secret": secret})
}

func clearGatewayRoutePause(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := d.Gateway.ClearRoutePause(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func parseGatewayUsageQuery(c *gin.Context) storage.GatewayUsageQuery {
	q := storage.GatewayUsageQuery{
		Page:     queryInt(c, "page", 1),
		PageSize: queryInt(c, "page_size", 20),
		// Keep the legacy response shape for external API callers. The bundled
		// frontend opts out explicitly because neither aggregate is displayed.
		IncludeSum:       true,
		IncludeEndpoints: true,
	}
	if v, ok := c.GetQuery("include_sum"); ok {
		q.IncludeSum = parseGatewayUsageBool(v, q.IncludeSum)
	}
	if v, ok := c.GetQuery("include_endpoints"); ok {
		q.IncludeEndpoints = parseGatewayUsageBool(v, q.IncludeEndpoints)
	}
	if v := c.Query("group_id"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			q.GatewayGroupID = uint(n)
		}
	}
	if v := c.Query("gateway_key_id"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			q.GatewayKeyID = uint(n)
		}
	}
	if v := c.Query("channel_id"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			q.ChannelID = uint(n)
		}
	}
	if v := c.Query("provider_id"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			q.GatewayProviderID = uint(n)
		}
	}
	q.Model = strings.TrimSpace(c.Query("model"))
	q.RequestID = strings.TrimSpace(c.Query("request_id"))
	if v := c.Query("request_type"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			q.RequestType = &n
		}
	}
	// result 优先：success | fail | client | multi | multi_success | multi_fail
	if v := strings.TrimSpace(c.Query("result")); v != "" {
		q.ResultMode = strings.ToLower(v)
	} else if v := c.Query("success"); v == "true" || v == "1" {
		q.ResultMode = "success"
		t := true
		q.SuccessOnly = &t
	} else if v == "false" || v == "0" {
		q.ResultMode = "fail"
		f := false
		q.SuccessOnly = &f
	}
	if v := c.Query("from"); v != "" {
		if t, ok := parseUsageTime(v); ok {
			q.From = &t
		}
	}
	if v := c.Query("to"); v != "" {
		if t, ok := parseUsageTime(v); ok {
			q.To = &t
		}
	}
	return q
}

func parseGatewayUsageBool(value string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return fallback
	}
}

// parseUsageTime 兼容前端 toISOString（含毫秒）与 datetime-local（无时区=本地）。
func parseUsageTime(v string) (time.Time, bool) {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}, false
	}
	// JS Date.toISOString() → 带毫秒的 RFC3339，必须用 Nano
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, true
	}
	if t, err := time.Parse(time.RFC3339, v); err == nil {
		return t, true
	}
	// datetime-local: 无时区，按服务器本地时区
	for _, layout := range []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04:05",
		"2006-01-02",
	} {
		if t, err := time.ParseInLocation(layout, v, time.Local); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func listGatewayUsage(c *gin.Context, d *Deps) {
	q := parseGatewayUsageQuery(c)
	page, err := d.GatewayUsage.List(q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, page)
}

func statsGatewayUsage(c *gin.Context, d *Deps) {
	q := parseGatewayUsageQuery(c)
	stats, err := d.GatewayUsage.Stats(q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, stats)
}

// listGatewayUsageModels 使用记录中出现过的模型聚合（下拉选项）。
// 支持 group_id / gateway_key_id / from / to；忽略 model / result。
func listGatewayUsageModels(c *gin.Context, d *Deps) {
	if d.GatewayUsage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "usage storage unavailable"})
		return
	}
	q := parseGatewayUsageQuery(c)
	items, err := d.GatewayUsage.ListModels(q)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": items})
}

// cleanupGatewayUsageReq 清理访问日志。
// - all=true：删除全部
// - all=false：删除 created_at < before 的记录（before 为 RFC3339 / datetime-local）
// confirm 必须为 true，防止误触。
type cleanupGatewayUsageReq struct {
	All     bool   `json:"all"`
	Before  string `json:"before"`
	Confirm bool   `json:"confirm"`
	// DryRun 仅统计将删除的条数，不实际删除
	DryRun bool `json:"dry_run"`
}

func cleanupGatewayUsage(c *gin.Context, d *Deps) {
	if d.GatewayUsage == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "usage storage unavailable"})
		return
	}
	var req cleanupGatewayUsageReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if !req.Confirm && !req.DryRun {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请确认清理操作（confirm=true）"})
		return
	}

	var (
		count int64
		err   error
	)
	if req.All {
		if req.DryRun {
			count, err = d.GatewayUsage.CountAll()
		} else {
			count, err = d.GatewayUsage.DeleteAll()
		}
	} else {
		before, ok := parseUsageTime(req.Before)
		if !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "请提供有效的截止时间 before"})
			return
		}
		if req.DryRun {
			count, err = d.GatewayUsage.CountBefore(before)
		} else {
			count, err = d.GatewayUsage.DeleteBefore(before)
		}
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if !req.DryRun && count > 0 && d.Gateway != nil && d.Gateway.Routes != nil {
		d.Gateway.Routes.InvalidateAllCacheHealth()
	}
	if req.DryRun {
		c.JSON(http.StatusOK, gin.H{"dry_run": true, "matched": count})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": count, "all": req.All})
}

func listGatewayPrices(c *gin.Context, d *Deps) {
	list, err := d.ModelPrices.List()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"items": list})
}

func listGatewayDefaultPrices(c *gin.Context, d *Deps) {
	if d.Gateway == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "gateway unavailable"})
		return
	}
	items := d.Gateway.ListDefaultPrices(c.Query("q"))
	c.JSON(http.StatusOK, gin.H{"items": items})
}

func upsertGatewayPrice(c *gin.Context, d *Deps) {
	var item storage.ModelPriceOverride
	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if strings.TrimSpace(item.ModelName) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "model_name is required"})
		return
	}
	if err := d.ModelPrices.Upsert(&item); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, item)
}

func deleteGatewayPrice(c *gin.Context, d *Deps) {
	id, err := parseUintParam(c, "id")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}
	if err := d.ModelPrices.Delete(id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func parseUintParam(c *gin.Context, name string) (uint, error) {
	n, err := strconv.ParseUint(c.Param(name), 10, 64)
	return uint(n), err
}

func queryInt(c *gin.Context, name string, def int) int {
	v := c.Query(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
