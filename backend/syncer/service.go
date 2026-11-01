// Package syncer 上游同步服务（公告/余额等相关任务）。
package syncer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/connector/sub2api"
	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/notify"
	"github.com/bejix/upstream-ops/backend/pkg/rateconvert"
	"github.com/bejix/upstream-ops/backend/storage"
)

type channelSvc interface {
	RevealAPIKey(ctx context.Context, channelID uint, keyID int64) (string, error)
	CreateAPIKey(ctx context.Context, channelID uint, req connector.APIKeyCreateRequest) (*connector.APIKey, error)
	UpdateAPIKey(ctx context.Context, channelID uint, keyID int64, req connector.APIKeyUpdateRequest) (*connector.APIKey, error)
	DeleteAPIKey(ctx context.Context, channelID uint, keyID int64) error
	ListAPIKeys(ctx context.Context, channelID uint, query connector.APIKeyQuery) (*connector.APIKeyPage, error)
	ListAPIKeyGroups(ctx context.Context, channelID uint) ([]connector.APIKeyGroup, error)
}

type Service struct {
	channels   *storage.Channels
	rates      *storage.Rates
	cipher     *crypto.Cipher
	channelSvc channelSvc
	log        *slog.Logger
	dispatcher *notify.Dispatcher

	targets         *storage.UpstreamSyncTargets
	groups          *storage.UpstreamSyncTargetGroups
	syncGroups      *storage.UpstreamSyncGroups
	syncAccounts    *storage.UpstreamSyncAccounts
	managedAccounts *storage.UpstreamSyncManagedAccounts
	logs            *storage.UpstreamSyncLogs
	// APICalls records real upstream sync management HTTP attempts.
	APICalls *storage.GatewayAPICallLogs
}

func New(
	channels *storage.Channels,
	rates *storage.Rates,
	cipher *crypto.Cipher,
	channelSvc channelSvc,
	log *slog.Logger,
	targets *storage.UpstreamSyncTargets,
	groups *storage.UpstreamSyncTargetGroups,
	syncGroups *storage.UpstreamSyncGroups,
	syncAccounts *storage.UpstreamSyncAccounts,
	managedAccounts *storage.UpstreamSyncManagedAccounts,
	logs *storage.UpstreamSyncLogs,
) *Service {
	return &Service{
		channels:        channels,
		rates:           rates,
		cipher:          cipher,
		channelSvc:      channelSvc,
		log:             log,
		targets:         targets,
		groups:          groups,
		syncGroups:      syncGroups,
		syncAccounts:    syncAccounts,
		managedAccounts: managedAccounts,
		logs:            logs,
	}
}

func (s *Service) SetDispatcher(dispatcher *notify.Dispatcher) {
	s.dispatcher = dispatcher
}

func (s *Service) newAdminClient() *sub2api.AdminClient {
	client := sub2api.NewAdminClient()
	if s.APICalls != nil {
		client.SetAPIAudit(apiaudit.Options{
			Store:   s.APICalls,
			Purpose: storage.GatewayAPICallPurposeSyncAPI,
			Log:     s.log,
			Metadata: storage.GatewayAPICallLog{
				Purpose: storage.GatewayAPICallPurposeSyncAPI,
			},
		})
	}
	return client
}

type TargetDTO struct {
	ID              uint       `json:"id"`
	Name            string     `json:"name"`
	BaseURL         string     `json:"base_url"`
	Enabled         bool       `json:"enabled"`
	LastCheckStatus string     `json:"last_check_status,omitempty"`
	LastCheckAt     *time.Time `json:"last_check_at,omitempty"`
	LastCheckError  string     `json:"last_check_error,omitempty"`
}

type TargetGroupDTO struct {
	ID            uint       `json:"id"`
	TargetID      uint       `json:"target_id"`
	RemoteGroupID int64      `json:"remote_group_id"`
	Name          string     `json:"name"`
	Platform      string     `json:"platform,omitempty"`
	Ratio         float64    `json:"ratio"`
	Status        string     `json:"status"`
	Sort          int        `json:"sort"`
	Description   string     `json:"description,omitempty"`
	LastSyncAt    *time.Time `json:"last_sync_at,omitempty"`
}

type TargetProxyDTO struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Status   string `json:"status"`
}

type SyncGroupDTO struct {
	ID                       uint             `json:"id"`
	DisplayName              string           `json:"display_name"`
	NameTemplate             string           `json:"name_template"`
	Name                     string           `json:"name"`
	TargetID                 uint             `json:"target_id"`
	TargetGroupIDs           []uint           `json:"target_group_ids"`
	Platform                 string           `json:"platform"`
	ModelLimitsMode          string           `json:"model_limits_mode"`
	ModelLimits              string           `json:"model_limits,omitempty"`
	PoolModeEnabled          bool             `json:"pool_mode_enabled"`
	PoolModeRetryCount       int              `json:"pool_mode_retry_count"`
	PoolModeRetryStatusCodes string           `json:"pool_mode_retry_status_codes,omitempty"`
	CustomErrorCodesEnabled  bool             `json:"custom_error_codes_enabled"`
	CustomErrorCodes         string           `json:"custom_error_codes,omitempty"`
	RateSortDirection        string           `json:"rate_sort_direction"`
	Accounts                 []SyncAccountDTO `json:"accounts"`
	Enabled                  *bool            `json:"enabled"`
	ApplyStatus              string           `json:"apply_status,omitempty"`
	ApplyError               string           `json:"apply_error,omitempty"`
	LastAppliedAt            *time.Time       `json:"last_applied_at,omitempty"`
}

type SyncAccountDTO struct {
	ID               uint    `json:"id,omitempty"`
	SourceChannelID  uint    `json:"source_channel_id"`
	SourceGroupID    *int64  `json:"source_group_id,omitempty"`
	SourceGroupName  string  `json:"source_group_name,omitempty"`
	ProxyID          *int64  `json:"proxy_id,omitempty"`
	Concurrency      int     `json:"concurrency"`
	Weight           int     `json:"weight"`
	RateConvertMode  string  `json:"rate_convert_mode"`
	RateConvertValue float64 `json:"rate_convert_value"`
	Enabled          bool    `json:"enabled"`
	TestEnabled      bool    `json:"test_enabled"`
	TestModel        string  `json:"test_model,omitempty"`
}

type SourceModelsInput struct {
	ChannelID       uint
	SyncAccountID   uint
	SourceGroupID   *int64
	SourceGroupName string
	Platform        string
}

type ManagedAccountDTO struct {
	ID                uint       `json:"id"`
	SyncGroupID       uint       `json:"sync_group_id"`
	SyncAccountID     uint       `json:"sync_account_id"`
	SourceAPIKeyID    int64      `json:"source_api_key_id"`
	SourceAPIKeyName  string     `json:"source_api_key_name"`
	TargetAccountID   int64      `json:"target_account_id"`
	TargetAccountName string     `json:"target_account_name"`
	TargetGroupIDs    []uint     `json:"target_group_ids"`
	LastAppliedAt     *time.Time `json:"last_applied_at,omitempty"`
}

type LogDTO struct {
	ID          uint      `json:"id"`
	SyncGroupID uint      `json:"sync_group_id"`
	TargetID    uint      `json:"target_id"`
	Action      string    `json:"action"`
	Success     bool      `json:"success"`
	Message     string    `json:"message,omitempty"`
	Detail      string    `json:"detail,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

type accountApplyResult struct {
	SyncedModels []string
	Message      string
	Changes      []string
}

const applyAccountWorkerLimit = 5
const sourceModelsBodyLimit int64 = 8 << 20

type syncAccountApplyOutcome struct {
	Index        int
	Applied      bool
	Failure      string
	Success      string
	Changes      []string
	SyncedModels []string
}

type TargetInput struct {
	Name        string `json:"name"`
	BaseURL     string `json:"base_url"`
	AdminAPIKey string `json:"admin_api_key"`
	Enabled     bool   `json:"enabled"`
}

func (s *Service) ListTargets() ([]TargetDTO, error) {
	list, err := s.targets.List()
	if err != nil {
		return nil, err
	}
	out := make([]TargetDTO, 0, len(list))
	for _, item := range list {
		out = append(out, TargetDTO{
			ID:              item.ID,
			Name:            item.Name,
			BaseURL:         item.BaseURL,
			Enabled:         item.Enabled,
			LastCheckStatus: item.LastCheckStatus,
			LastCheckAt:     item.LastCheckAt,
			LastCheckError:  item.LastCheckError,
		})
	}
	return out, nil
}

func (s *Service) CreateTarget(ctx context.Context, in TargetInput) (*TargetDTO, error) {
	enc, err := s.cipher.Encrypt(strings.TrimSpace(in.AdminAPIKey))
	if err != nil {
		return nil, err
	}
	item := &storage.UpstreamSyncTarget{
		Name:              strings.TrimSpace(in.Name),
		BaseURL:           strings.TrimSpace(in.BaseURL),
		AdminAPIKeyCipher: enc,
		Enabled:           in.Enabled,
	}
	if err := s.targets.Create(item); err != nil {
		return nil, err
	}
	return s.toTargetDTO(item), nil
}

func (s *Service) UpdateTarget(ctx context.Context, id uint, in TargetInput) (*TargetDTO, error) {
	item, err := s.targets.FindByID(id)
	if err != nil {
		return nil, err
	}
	item.Name = strings.TrimSpace(in.Name)
	item.BaseURL = strings.TrimSpace(in.BaseURL)
	if strings.TrimSpace(in.AdminAPIKey) != "" {
		enc, err := s.cipher.Encrypt(strings.TrimSpace(in.AdminAPIKey))
		if err != nil {
			return nil, err
		}
		item.AdminAPIKeyCipher = enc
	}
	item.Enabled = in.Enabled
	if err := s.targets.Update(item); err != nil {
		return nil, err
	}
	return s.toTargetDTO(item), nil
}

func (s *Service) DeleteTarget(id uint) error {
	return s.targets.Delete(id)
}

func (s *Service) CheckTarget(ctx context.Context, id uint) error {
	item, err := s.targets.FindByID(id)
	if err != nil {
		return err
	}
	client := s.newAdminClient()
	plain, err := s.cipher.Decrypt(item.AdminAPIKeyCipher)
	if err != nil {
		_ = s.targets.UpdateCheck(id, "failed", ptrTime(time.Now()), err.Error())
		return err
	}
	err = client.Ping(ctx, sub2api.AdminTarget{BaseURL: item.BaseURL, APIKey: plain})
	status := "ok"
	errText := ""
	if err != nil {
		status = "failed"
		errText = err.Error()
	}
	now := time.Now()
	_ = s.targets.UpdateCheck(id, status, &now, errText)
	return err
}

func (s *Service) SyncTargetGroups(ctx context.Context, targetID uint) ([]TargetGroupDTO, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, err
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	client := s.newAdminClient()
	groups, err := client.ListGroups(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}, true)
	if err != nil {
		_ = s.targets.UpdateCheck(targetID, "failed", ptrTime(time.Now()), err.Error())
		return nil, err
	}
	seen := make([]int64, 0, len(groups))
	now := time.Now()
	out := make([]TargetGroupDTO, 0, len(groups))
	for _, g := range groups {
		seen = append(seen, g.ID)
		item, err := s.groups.FindByTargetAndRemote(targetID, g.ID)
		if err != nil {
			item = &storage.UpstreamSyncTargetGroup{TargetID: targetID, RemoteGroupID: g.ID}
		}
		item.TargetID = targetID
		item.RemoteGroupID = g.ID
		item.Name = g.Name
		item.Platform = g.Platform
		item.Ratio = g.Ratio
		item.Sort = g.Sort
		item.Description = g.Description
		item.Status = strings.TrimSpace(g.Status)
		if item.Status == "" {
			item.Status = "active"
		}
		item.LastSyncAt = &now
		if err := s.groups.Upsert(item); err != nil {
			return nil, err
		}
		out = append(out, s.toGroupDTO(item))
	}
	_ = s.groups.DeleteMissing(targetID, seen)
	return out, nil
}

func (s *Service) ListTargetGroups(targetID uint, includeMissing bool) ([]TargetGroupDTO, error) {
	list, err := s.groups.ListByTarget(targetID, includeMissing)
	if err != nil {
		return nil, err
	}
	out := make([]TargetGroupDTO, 0, len(list))
	for _, item := range list {
		out = append(out, s.toGroupDTO(&item))
	}
	return out, nil
}

func (s *Service) ListTargetProxies(ctx context.Context, targetID uint) ([]TargetProxyDTO, error) {
	target, err := s.targets.FindByID(targetID)
	if err != nil {
		return nil, err
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	client := s.newAdminClient()
	proxies, err := client.ListProxies(ctx, sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain})
	if err != nil {
		_ = s.targets.UpdateCheck(targetID, "failed", ptrTime(time.Now()), err.Error())
		return nil, err
	}
	out := make([]TargetProxyDTO, 0, len(proxies))
	for _, proxy := range proxies {
		out = append(out, TargetProxyDTO{
			ID:       proxy.ID,
			Name:     proxy.Name,
			Protocol: proxy.Protocol,
			Host:     proxy.Host,
			Port:     proxy.Port,
			Status:   strings.TrimSpace(proxy.Status),
		})
	}
	return out, nil
}

func (s *Service) ListSourceModels(ctx context.Context, in SourceModelsInput) ([]string, error) {
	if in.ChannelID == 0 {
		return nil, errors.New("source channel is required")
	}
	ch, err := s.channels.FindByID(in.ChannelID)
	if err != nil {
		return nil, err
	}
	page, err := s.channelSvc.ListAPIKeys(ctx, in.ChannelID, connector.APIKeyQuery{Page: 1, PageSize: 100})
	if err != nil {
		return nil, err
	}
	var managedKeyID int64
	if in.SyncAccountID > 0 {
		if managed, findErr := s.managedAccounts.FindByAccountID(in.SyncAccountID); findErr == nil && managed != nil {
			managedKeyID = managed.SourceAPIKeyID
		}
	}
	key := selectSourceModelKey(page.Items, managedKeyID, in.SourceGroupID, in.SourceGroupName)
	if key == nil {
		if sourceModelGroupSpecified(in.SourceGroupID, in.SourceGroupName) {
			return nil, errors.New("当前源分组没有可用 API Key，请先创建或应用同步账号")
		}
		return nil, errors.New("该渠道没有可用于获取模型的 API Key")
	}
	secret, err := s.channelSvc.RevealAPIKey(ctx, in.ChannelID, key.ID)
	if err != nil {
		return nil, err
	}
	return fetchGatewayModels(ctx, ch.SiteURL, in.Platform, secret, apiaudit.Options{
		Store:   s.APICalls,
		Purpose: storage.GatewayAPICallPurposeSyncAPI,
		Log:     s.log,
		Metadata: storage.GatewayAPICallLog{
			Purpose:        storage.GatewayAPICallPurposeSyncAPI,
			ChannelID:      ch.ID,
			SourceGroupID:  in.SourceGroupID,
			SourceAPIKeyID: key.ID,
		},
	})
}

func (s *Service) ListSyncGroups() ([]SyncGroupDTO, error) {
	list, err := s.syncGroups.List()
	if err != nil {
		return nil, err
	}
	out := make([]SyncGroupDTO, 0, len(list))
	for _, item := range list {
		ids, _ := s.syncGroups.ParseTargetGroupIDs(&item)
		accounts, err := s.syncAccounts.ListBySyncGroupID(item.ID)
		if err != nil {
			return nil, err
		}
		out = append(out, s.toSyncGroupDTO(&item, ids, accounts))
	}
	return out, nil
}

func (s *Service) CreateSyncGroup(in SyncGroupDTO) (*SyncGroupDTO, error) {
	accounts := accountItems(in.Accounts)
	sourceGroupID := int64(0)
	sourceChannelID := uint(0)
	if len(accounts) > 0 {
		sourceChannelID = accounts[0].SourceChannelID
		if accounts[0].SourceGroupID != nil {
			sourceGroupID = *accounts[0].SourceGroupID
		}
	}
	name := renderSyncGroupName(in.NameTemplate, 0, sourceChannelID, sourceGroupID)
	displayName := strings.TrimSpace(in.DisplayName)
	if displayName == "" {
		displayName = name
	}
	item := &storage.UpstreamSyncGroup{
		DisplayName:              displayName,
		NameTemplate:             strings.TrimSpace(in.NameTemplate),
		Name:                     name,
		TargetID:                 in.TargetID,
		TargetGroupIDsJSON:       marshalUintArray(in.TargetGroupIDs),
		Platform:                 strings.TrimSpace(in.Platform),
		ModelLimitsMode:          normalizeModelLimitsMode(in.ModelLimitsMode),
		ModelLimitsText:          strings.TrimSpace(in.ModelLimits),
		PoolModeEnabled:          in.PoolModeEnabled,
		PoolModeRetryCount:       in.PoolModeRetryCount,
		PoolModeRetryStatusCodes: strings.TrimSpace(in.PoolModeRetryStatusCodes),
		CustomErrorCodesEnabled:  in.CustomErrorCodesEnabled,
		CustomErrorCodes:         strings.TrimSpace(in.CustomErrorCodes),
		RateSortDirection:        strings.TrimSpace(in.RateSortDirection),
		Enabled:                  boolValue(in.Enabled, true),
	}
	if item.RateSortDirection == "" {
		item.RateSortDirection = "asc"
	}
	if item.PoolModeRetryCount == 0 {
		item.PoolModeRetryCount = 10
	}
	if err := s.syncGroups.Create(item); err != nil {
		return nil, err
	}
	// 同步分组 ID 只有入库后才确定；这里立刻回写最终名称，保证后续远端对象命名稳定。
	item.Name = renderSyncGroupName(item.NameTemplate, item.ID, sourceChannelID, sourceGroupID)
	if strings.TrimSpace(in.DisplayName) == "" {
		item.DisplayName = item.Name
	}
	item.Enabled = boolValue(in.Enabled, true)
	if err := s.syncGroups.Update(item); err != nil {
		return nil, err
	}
	if err := s.syncAccounts.SaveForGroup(item.ID, accounts); err != nil {
		return nil, err
	}
	s.notifySyncGroupChanged("新增", item, accounts)
	return s.syncGroupDTOByItem(item), nil
}

func (s *Service) UpdateSyncGroup(id uint, in SyncGroupDTO) (*SyncGroupDTO, error) {
	item, err := s.syncGroups.FindByID(id)
	if err != nil {
		return nil, err
	}
	item.TargetID = in.TargetID
	item.DisplayName = strings.TrimSpace(in.DisplayName)
	item.TargetGroupIDsJSON = marshalUintArray(in.TargetGroupIDs)
	item.ModelLimitsMode = normalizeModelLimitsMode(in.ModelLimitsMode)
	item.ModelLimitsText = strings.TrimSpace(in.ModelLimits)
	item.PoolModeEnabled = in.PoolModeEnabled
	item.PoolModeRetryCount = in.PoolModeRetryCount
	item.PoolModeRetryStatusCodes = strings.TrimSpace(in.PoolModeRetryStatusCodes)
	item.CustomErrorCodesEnabled = in.CustomErrorCodesEnabled
	item.CustomErrorCodes = strings.TrimSpace(in.CustomErrorCodes)
	item.RateSortDirection = strings.TrimSpace(in.RateSortDirection)
	item.Enabled = boolValue(in.Enabled, item.Enabled)
	if item.DisplayName == "" {
		item.DisplayName = item.Name
	}
	if item.RateSortDirection == "" {
		item.RateSortDirection = "asc"
	}
	if err := s.syncGroups.Update(item); err != nil {
		return nil, err
	}
	if err := s.syncAccounts.SaveForGroup(item.ID, accountItems(in.Accounts)); err != nil {
		return nil, err
	}
	ids, _ := s.syncGroups.ParseTargetGroupIDs(item)
	accounts, err := s.syncAccounts.ListBySyncGroupID(item.ID)
	if err != nil {
		return nil, err
	}
	s.notifySyncGroupChanged("更新", item, accounts)
	dto := s.toSyncGroupDTO(item, ids, accounts)
	return &dto, nil
}

func (s *Service) DeleteSyncGroup(id uint) error {
	item, err := s.syncGroups.FindByID(id)
	if err != nil {
		return err
	}
	accounts, _ := s.syncAccounts.ListBySyncGroupID(id)
	if err := s.syncGroups.Delete(id); err != nil {
		return err
	}
	s.notifySyncGroupChanged("删除", item, accounts)
	return nil
}

func (s *Service) notifySyncGroupChanged(action string, item *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) {
	if s.dispatcher == nil || item == nil {
		return
	}
	targetName := fmt.Sprintf("目标 ID %d", item.TargetID)
	if target, err := s.targets.FindByID(item.TargetID); err == nil {
		targetName = target.Name
	}
	targetGroupIDs, _ := s.syncGroups.ParseTargetGroupIDs(item)
	displayName := strings.TrimSpace(item.DisplayName)
	if displayName == "" {
		displayName = item.Name
	}
	body := fmt.Sprintf(
		"动作：%s\n同步分组：%s\n同步名称：%s\n目标上游：%s\n平台：%s\n目标分组数：%d\n同步账号数：%d\n时间：%s",
		action,
		displayName,
		item.Name,
		targetName,
		item.Platform,
		len(targetGroupIDs),
		len(accounts),
		time.Now().Format("2006-01-02 15:04:05"),
	)
	if err := s.dispatcher.Dispatch(context.Background(), notify.Message{
		Event:   storage.EventUpstreamSyncGroupChanged,
		Subject: fmt.Sprintf("[同步分组变动] %s · %s", action, displayName),
		Body:    body,
		Extra: map[string]any{
			"sync_group_id": item.ID,
			"action":        action,
		},
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch sync group change notification", "err", err)
	}
}

func (s *Service) notifySyncGroupApplyChanged(ctx context.Context, item *storage.UpstreamSyncGroup, target *storage.UpstreamSyncTarget, applied int, failures []string, changes []string) {
	if s.dispatcher == nil || item == nil || target == nil || (len(changes) == 0 && len(failures) == 0) {
		return
	}
	displayName := strings.TrimSpace(item.DisplayName)
	if displayName == "" {
		displayName = item.Name
	}
	details := make([]string, 0, 2)
	if len(changes) > 0 {
		details = append(details, "变动账号：\n"+strings.Join(prefixLines(changes, "- "), "\n"))
	}
	if len(failures) > 0 {
		details = append(details, "失败账号：\n"+strings.Join(prefixLines(failures, "- "), "\n"))
	}
	body := fmt.Sprintf(
		"动作：应用同步\n同步分组：%s\n同步名称：%s\n目标上游：%s\n应用账号数：%d\n变动账号数：%d\n失败账号数：%d\n时间：%s\n\n%s",
		displayName,
		item.Name,
		target.Name,
		applied,
		len(changes),
		len(failures),
		time.Now().Format("2006-01-02 15:04:05"),
		strings.Join(details, "\n\n"),
	)
	subject := fmt.Sprintf("[同步账号变动] %s · %d 项", displayName, len(changes))
	if len(failures) > 0 {
		subject = fmt.Sprintf("[同步账号异常] %s · 失败 %d / 变动 %d", displayName, len(failures), len(changes))
	}
	if err := s.dispatcher.Dispatch(ctx, notify.Message{
		Event:   storage.EventUpstreamSyncGroupChanged,
		Subject: subject,
		Body:    body,
		Extra: map[string]any{
			"sync_group_id": item.ID,
			"action":        "apply",
		},
	}); err != nil && s.log != nil {
		s.log.Warn("dispatch sync apply change notification", "err", err)
	}
}

func (s *Service) ApplySyncGroup(ctx context.Context, syncGroupID uint) (*LogDTO, error) {
	syncGroup, err := s.syncGroups.FindByID(syncGroupID)
	if err != nil {
		return nil, err
	}
	if normalized := normalizeModelLimitsMode(syncGroup.ModelLimitsMode); syncGroup.ModelLimitsMode != normalized {
		syncGroup.ModelLimitsMode = normalized
		if err := s.syncGroups.Update(syncGroup); err != nil {
			return nil, err
		}
	}
	target, err := s.targets.FindByID(syncGroup.TargetID)
	if err != nil {
		return nil, err
	}
	if !target.Enabled {
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "target disabled")
	}
	accounts, err := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return nil, err
	}
	if len(accounts) == 0 {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", "no sync accounts", nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, "no sync accounts")
	}
	accounts = s.sortAccountsForApply(ctx, syncGroup, accounts)
	if err := s.syncAccounts.SaveForGroup(syncGroup.ID, accounts); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	targetGroups, selectedGroups, remoteGroupIDs, err := s.selectedTargetGroups(syncGroup)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "blocked_missing_group", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	plain, err := s.cipher.Decrypt(target.AdminAPIKeyCipher)
	if err != nil {
		return nil, err
	}
	client := s.newAdminClient()
	adminTarget := sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
	if err := client.Ping(ctx, adminTarget); err != nil {
		_ = s.targets.UpdateCheck(target.ID, "failed", ptrTime(time.Now()), err.Error())
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	deletedManaged, err := s.cleanupDeletedManagedAccounts(ctx, syncGroup, accounts, adminTarget, client)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	deletedUnmanaged, err := s.cleanupUnmanagedRemoteAccounts(ctx, syncGroup, adminTarget, client)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	remoteAccounts, err := client.ListAccounts(ctx, adminTarget, 1, 1000)
	if err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), nil)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	remoteBeforeByID := make(map[int64]sub2api.AdminAccount, len(remoteAccounts))
	for _, account := range remoteAccounts {
		remoteBeforeByID[account.ID] = account
	}
	applied := 0
	failures := make([]string, 0)
	successes := make([]string, 0)
	changes := make([]string, 0)
	if deletedManaged+deletedUnmanaged > 0 {
		changes = append(changes, fmt.Sprintf("清理：删除失效托管账号 %d 个，重复远端账号 %d 个", deletedManaged, deletedUnmanaged))
	}
	syncedModels := make([]string, 0)
	now := time.Now()
	outcomes := s.applyAccountsConcurrently(ctx, syncGroup, accounts, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
	for _, outcome := range outcomes {
		if outcome.Applied {
			applied++
		}
		if outcome.Failure != "" {
			failures = append(failures, outcome.Failure)
		}
		if outcome.Success != "" {
			successes = append(successes, outcome.Success)
		}
		syncedModels = append(syncedModels, outcome.SyncedModels...)
		changes = append(changes, outcome.Changes...)
	}
	if applied == 0 {
		msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
		status := "failed"
		if strings.Contains(msg, "missing") {
			status = "blocked_missing_group"
		}
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, status, msg, nil)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, msg)
	}
	if len(failures) > 0 {
		if err := s.cacheSyncedModels(syncGroup, syncedModels); err != nil {
			failures = append(failures, err.Error())
		}
		msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", msg, &now)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, msg)
	}
	if err := s.cacheSyncedModels(syncGroup, syncedModels); err != nil {
		_ = s.syncGroups.UpdateStatus(syncGroup.ID, "failed", err.Error(), &now)
		s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, []string{err.Error()}, changes)
		return s.appendLog(syncGroup.ID, target.ID, "apply", false, err.Error())
	}
	_ = s.syncGroups.UpdateStatus(syncGroup.ID, "applied", "", &now)
	msg := buildApplyLogMessage(syncGroup, target, selectedGroups, applied, failures, successes, changes, deletedManaged, deletedUnmanaged)
	if len(changes) == 0 {
		return &LogDTO{
			SyncGroupID: syncGroup.ID,
			TargetID:    target.ID,
			Action:      "apply",
			Success:     true,
			Message:     msg,
			CreatedAt:   now,
		}, nil
	}
	s.notifySyncGroupApplyChanged(ctx, syncGroup, target, applied, failures, changes)
	return s.appendLog(
		syncGroup.ID,
		target.ID,
		"apply",
		true,
		msg,
	)
}

func (s *Service) sortAccountsForApply(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, accounts []storage.UpstreamSyncAccount) []storage.UpstreamSyncAccount {
	groupsByChannel := make(map[uint][]connector.APIKeyGroup)
	for _, account := range accounts {
		if account.SourceChannelID == 0 {
			continue
		}
		if _, ok := groupsByChannel[account.SourceChannelID]; ok {
			continue
		}
		groups, err := s.channelSvc.ListAPIKeyGroups(ctx, account.SourceChannelID)
		if err != nil {
			groups = nil
		}
		groupsByChannel[account.SourceChannelID] = groups
	}
	fixed := make(map[int]storage.UpstreamSyncAccount)
	sortable := make([]storage.UpstreamSyncAccount, 0, len(accounts))
	for _, account := range accounts {
		if !account.Enabled || sourceGroupMissingForSort(&account, groupsByChannel[account.SourceChannelID]) {
			pos := account.Position
			if mapped, err := s.managedAccounts.FindByAccountID(account.ID); err == nil && mapped != nil {
				if mappedPos, ok := managedAccountPosition(syncGroup, mapped.TargetAccountName); ok && mappedPos >= 0 && mappedPos < len(accounts) {
					pos = mappedPos
				}
			}
			for {
				if _, exists := fixed[pos]; !exists {
					break
				}
				pos++
			}
			fixed[pos] = account
			continue
		}
		sortable = append(sortable, account)
	}
	direction := 1.0
	if strings.EqualFold(syncGroup.RateSortDirection, "desc") {
		direction = -1
	}
	sort.SliceStable(sortable, func(i, j int) bool {
		leftRate := rateMultiplierForAccount(&sortable[i], groupsByChannel[sortable[i].SourceChannelID])
		rightRate := rateMultiplierForAccount(&sortable[j], groupsByChannel[sortable[j].SourceChannelID])
		if leftRate != rightRate {
			return (leftRate-rightRate)*direction < 0
		}
		if sortable[i].Weight != sortable[j].Weight {
			return sortable[i].Weight > sortable[j].Weight
		}
		return sortable[i].Position < sortable[j].Position
	})
	out := make([]storage.UpstreamSyncAccount, 0, len(accounts))
	sortableIndex := 0
	for pos := 0; len(out) < len(accounts); pos++ {
		if account, ok := fixed[pos]; ok {
			account.Position = len(out)
			out = append(out, account)
			continue
		}
		if sortableIndex >= len(sortable) {
			break
		}
		account := sortable[sortableIndex]
		sortableIndex++
		account.Position = len(out)
		out = append(out, account)
	}
	for i := range out {
		out[i].Position = i
	}
	return out
}

func sourceGroupMissingForSort(account *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) bool {
	sourceGroupName := strings.TrimSpace(account.SourceGroupName)
	if account.SourceGroupID == nil && sourceGroupName == "" {
		return true
	}
	for _, group := range groups {
		if account.SourceGroupID != nil && group.ID != nil && *group.ID == *account.SourceGroupID {
			return false
		}
		if sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName) {
			return false
		}
	}
	return true
}

func (s *Service) applyAccountsConcurrently(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	accounts []storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) []syncAccountApplyOutcome {
	// 同一源渠道会复用同一个源 Key，必须按账号顺序处理；不同源渠道才并发。
	indexesBySourceChannel := make(map[uint][]int)
	sourceChannelIDs := make([]uint, 0)
	for i, account := range accounts {
		if _, ok := indexesBySourceChannel[account.SourceChannelID]; !ok {
			sourceChannelIDs = append(sourceChannelIDs, account.SourceChannelID)
		}
		indexesBySourceChannel[account.SourceChannelID] = append(indexesBySourceChannel[account.SourceChannelID], i)
	}
	workerCount := len(sourceChannelIDs)
	if workerCount > applyAccountWorkerLimit {
		workerCount = applyAccountWorkerLimit
	}
	if workerCount <= 0 {
		return nil
	}
	jobs := make(chan uint)
	results := make(chan syncAccountApplyOutcome, len(accounts))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for sourceChannelID := range jobs {
				for _, index := range indexesBySourceChannel[sourceChannelID] {
					account := accounts[index]
					outcome := s.applyAccountWithCleanup(ctx, syncGroup, &account, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
					outcome.Index = index
					results <- outcome
				}
			}
		}()
	}
	for _, sourceChannelID := range sourceChannelIDs {
		jobs <- sourceChannelID
	}
	close(jobs)
	wg.Wait()
	close(results)

	ordered := make([]syncAccountApplyOutcome, len(accounts))
	for outcome := range results {
		ordered[outcome.Index] = outcome
	}
	return ordered
}

func (s *Service) applyAccountWithCleanup(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	account *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) syncAccountApplyOutcome {
	if !account.Enabled {
		change, err := s.disableManagedTargetForSkippedAccount(ctx, syncGroup, account, adminTarget, client, remoteBeforeByID, now, "同步账号已禁用")
		if err != nil {
			msg := fmt.Sprintf("同步账号%d: disable managed target: %s", account.Position+1, err.Error())
			return syncAccountApplyOutcome{Failure: msg}
		}
		return syncAccountApplyOutcome{Changes: singleChange(change)}
	}
	if account.SourceGroupID == nil && strings.TrimSpace(account.SourceGroupName) == "" {
		msg := fmt.Sprintf("同步账号%d: source group not bound", account.Position+1)
		change, err := s.ensureDisabledPlaceholderTargetForAccount(ctx, syncGroup, account, adminTarget, client, selectedGroups, remoteGroupIDs, remoteBeforeByID, now, "源分组未绑定")
		if err != nil {
			msg = msg + "; create disabled placeholder: " + err.Error()
		}
		return syncAccountApplyOutcome{Failure: msg, Changes: singleChange(change)}
	}
	result, err := s.applyAccount(ctx, syncGroup, account, adminTarget, client, targetGroups, selectedGroups, remoteGroupIDs, remoteBeforeByID, now)
	if err != nil {
		msg := fmt.Sprintf("同步账号%d: %s", account.Position+1, err.Error())
		changes := changesFromApplyError(err)
		if shouldCreateDisabledPlaceholderOnApplyError(err) {
			change, placeholderErr := s.ensureDisabledPlaceholderTargetForAccount(ctx, syncGroup, account, adminTarget, client, selectedGroups, remoteGroupIDs, remoteBeforeByID, now, err.Error())
			if placeholderErr != nil {
				msg = msg + "; create disabled placeholder: " + placeholderErr.Error()
			}
			changes = append(changes, singleChange(change)...)
		} else if shouldDisableManagedTargetOnApplyError(err) {
			change, disableErr := s.disableManagedTargetForSkippedAccount(ctx, syncGroup, account, adminTarget, client, remoteBeforeByID, now, err.Error())
			if disableErr != nil {
				msg = msg + "; disable managed target: " + disableErr.Error()
			}
			changes = append(changes, singleChange(change)...)
		}
		return syncAccountApplyOutcome{Failure: msg, Changes: changes}
	}
	return syncAccountApplyOutcome{
		Applied:      true,
		Success:      result.Message,
		Changes:      result.Changes,
		SyncedModels: result.SyncedModels,
	}
}

func (s *Service) applyAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	targetGroups []storage.UpstreamSyncTargetGroup,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
) (*accountApplyResult, error) {
	ch, err := s.channels.FindByID(syncAccount.SourceChannelID)
	if err != nil {
		return nil, fmt.Errorf("source channel missing: %d", syncAccount.SourceChannelID)
	}
	sourceGroups, err := s.checkSourceGroup(ctx, syncAccount)
	if err != nil {
		return nil, err
	}
	keyName := sourceAPIKeyName(syncGroup)
	key, secret, err := s.ensureSourceAPIKey(ctx, syncGroup, syncAccount, keyName)
	if err != nil {
		return nil, err
	}
	accountBaseName := managedObjectBaseName(syncGroup, syncAccount)
	accountName := managedObjectName(syncGroup, syncAccount, ch)
	accountReq := s.buildAdminAccount(
		syncGroup,
		syncAccount,
		ch,
		secret,
		remoteGroupIDs,
		syncAccount.Position+1,
		rateMultiplierForAccount(syncAccount, sourceGroups),
	)
	accountReq.Name = accountName
	accountReq.Notes = syncedAccountNotes(now)
	var account *sub2api.AdminAccount
	var mapped *storage.UpstreamSyncManagedAccount
	var previous *sub2api.AdminAccount
	action := "创建"
	if found, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && found != nil {
		mapped = found
		action = "更新"
		if before, ok := remoteBeforeByID[mapped.TargetAccountID]; ok {
			previous = &before
		}
		account, err = client.UpdateAccount(ctx, adminTarget, mapped.TargetAccountID, accountReq)
		if err != nil && !isHTTPNotFound(err) {
			return nil, err
		}
		if err != nil && isHTTPNotFound(err) {
			action = "重建"
		}
	}
	if account == nil {
		existing, err := client.FindAccountByName(ctx, adminTarget, accountName)
		if err != nil {
			return nil, err
		}
		if existing == nil && accountName != accountBaseName {
			existing, err = client.FindAccountByName(ctx, adminTarget, accountBaseName)
			if err != nil {
				return nil, err
			}
		}
		if existing != nil {
			before := *existing
			previous = &before
			if action == "创建" {
				action = "复用更新"
			} else {
				action = "重建更新"
			}
			account, err = client.UpdateAccount(ctx, adminTarget, existing.ID, accountReq)
		} else {
			account, err = client.CreateAccount(ctx, adminTarget, accountReq)
		}
		if err != nil {
			return nil, err
		}
	}
	syncedModels := []string(nil)
	if syncGroup.ModelLimitsMode == "sync_upstream" {
		models, err := client.SyncAccountModelsFromUpstream(ctx, adminTarget, account.ID)
		if err != nil {
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
		if len(models) == 0 {
			err := errors.New("synced upstream models is empty")
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
		syncedModels = models
		accountReq.Credentials["model_mapping"] = modelMappingFromModels(models)
		account, err = client.UpdateAccount(ctx, adminTarget, account.ID, accountReq)
		if err != nil {
			change, _ := s.disableManagedTargetAfterApplyFailure(ctx, syncGroup, syncAccount, adminTarget, client, account, accountName, selectedGroups, key, keyName, now, err.Error())
			return nil, errorWithChanges(err, change)
		}
	}
	if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
		return nil, err
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     key.ID,
		SourceAPIKeyName:   keyName,
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return nil, err
	}
	testMessage, testChange, err := s.testManagedTargetAccount(ctx, adminTarget, client, syncAccount, account)
	if err != nil {
		return nil, err
	}
	msg := fmt.Sprintf(
		"账号%d：%s远端账号 %s(ID %d)，源渠道 %s(ID %d)，源分组 %s，倍率 %s，权重 %d，并发 %d",
		syncAccount.Position+1,
		action,
		accountName,
		account.ID,
		ch.Name,
		ch.ID,
		sourceGroupLabel(syncAccount.SourceGroupID, syncAccount.SourceGroupName, sourceGroups),
		formatNumber(accountReq.RateMultiplier),
		syncAccount.Weight,
		positiveOrDefault(syncAccount.Concurrency, 10),
	)
	if syncAccount.ProxyID != nil {
		msg += fmt.Sprintf("，代理 ID %d", *syncAccount.ProxyID)
	}
	if syncGroup.ModelLimitsMode == "sync_upstream" {
		msg += fmt.Sprintf("，同步模型 %d 个", len(syncedModels))
	}
	if testMessage != "" {
		msg += "，" + testMessage
	}
	return &accountApplyResult{
		SyncedModels: syncedModels,
		Message:      msg,
		Changes:      append(accountChangeDetails(syncAccount, previous, accountReq, account.ID), singleChange(testChange)...),
	}, nil
}

func (s *Service) testManagedTargetAccount(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	syncAccount *storage.UpstreamSyncAccount,
	account *sub2api.AdminAccount,
) (string, string, error) {
	if syncAccount == nil || account == nil || !syncAccount.TestEnabled {
		return "", "", nil
	}
	model := strings.TrimSpace(syncAccount.TestModel)
	if model == "" {
		models, err := client.ListAccountModels(ctx, adminTarget, account.ID)
		if err != nil {
			change := testRemoteAccountChange(syncAccount, account.Name, account.ID, "", false, err.Error())
			if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
				return "", "", setErr
			}
			return fmt.Sprintf("测试失败，调度已禁用：%s", err.Error()), change, nil
		}
		if len(models) == 0 {
			err := errors.New("account models is empty")
			change := testRemoteAccountChange(syncAccount, account.Name, account.ID, "", false, err.Error())
			if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
				return "", "", setErr
			}
			return fmt.Sprintf("测试失败，调度已禁用：%s", err.Error()), change, nil
		}
		model = models[0]
	}
	if _, err := client.TestAccount(ctx, adminTarget, account.ID, model); err != nil {
		change := testRemoteAccountChange(syncAccount, account.Name, account.ID, model, false, err.Error())
		if _, setErr := client.SetAccountSchedulable(ctx, adminTarget, account.ID, false); setErr != nil {
			return "", "", setErr
		}
		return fmt.Sprintf("测试模型 %s 失败，调度已禁用：%s", model, err.Error()), change, nil
	}
	if _, err := client.SetAccountSchedulable(ctx, adminTarget, account.ID, true); err != nil {
		return "", "", err
	}
	change := ""
	if !account.Schedulable {
		change = testRemoteAccountChange(syncAccount, account.Name, account.ID, model, true, "")
	}
	return fmt.Sprintf("测试模型 %s 通过，调度已启用", model), change, nil
}

func (s *Service) ensureDisabledPlaceholderTargetForAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	remoteGroupIDs []int64,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
	reason string,
) (string, error) {
	ch, err := s.channels.FindByID(syncAccount.SourceChannelID)
	if err != nil {
		return "", err
	}
	accountName := managedObjectName(syncGroup, syncAccount, ch)
	accountBaseName := managedObjectBaseName(syncGroup, syncAccount)
	accountReq := s.buildAdminAccount(
		syncGroup,
		syncAccount,
		ch,
		"1234",
		remoteGroupIDs,
		syncAccount.Position+1,
		rateMultiplierForAccount(syncAccount, nil),
	)
	accountReq.Name = accountName
	accountReq.Status = "inactive"
	disabledDescription := disabledManagedAccountDescription(reason, now)
	accountReq.Notes = disabledDescription

	var account *sub2api.AdminAccount
	if mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && mapped != nil {
		if before, ok := remoteBeforeByID[mapped.TargetAccountID]; ok {
			account, err = client.UpdateAccount(ctx, adminTarget, before.ID, accountReq)
			if err != nil && !isHTTPNotFound(err) {
				return "", err
			}
			if err == nil {
				if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
					return "", err
				}
				return s.upsertDisabledPlaceholderMapping(syncGroup, syncAccount, selectedGroups, account, accountName, now, reason)
			}
		}
	}
	existing, err := client.FindAccountByName(ctx, adminTarget, accountName)
	if err != nil {
		return "", err
	}
	if existing == nil && accountName != accountBaseName {
		existing, err = client.FindAccountByName(ctx, adminTarget, accountBaseName)
		if err != nil {
			return "", err
		}
	}
	if existing != nil {
		account, err = client.UpdateAccount(ctx, adminTarget, existing.ID, accountReq)
	} else {
		account, err = client.CreateAccount(ctx, adminTarget, accountReq)
	}
	if err != nil {
		return "", err
	}
	if strings.EqualFold(strings.TrimSpace(account.Status), "active") {
		if err := s.disableRemoteAccount(ctx, adminTarget, client, *account, now, reason); err != nil {
			return "", err
		}
		account.Status = "inactive"
	} else if err := s.syncRemoteAccountSchedulable(ctx, adminTarget, client, account); err != nil {
		return "", err
	}
	return s.upsertDisabledPlaceholderMapping(syncGroup, syncAccount, selectedGroups, account, accountName, now, reason)
}

func (s *Service) upsertDisabledPlaceholderMapping(
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	account *sub2api.AdminAccount,
	accountName string,
	now time.Time,
	reason string,
) (string, error) {
	if account == nil {
		return "", nil
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     0,
		SourceAPIKeyName:   "",
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, accountName, account.ID, reason), nil
}

func (s *Service) disableManagedTargetForSkippedAccount(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	remoteBeforeByID map[int64]sub2api.AdminAccount,
	now time.Time,
	reason string,
) (string, error) {
	mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID)
	if err != nil {
		return "", nil
	}
	if !isManagedAccountName(syncGroup, mapped.TargetAccountName) {
		return "", nil
	}
	account, ok := remoteBeforeByID[mapped.TargetAccountID]
	if !ok {
		return "", nil
	}
	if isRemoteAccountDisabledForReason(account, reason) {
		return "", nil
	}
	if err := s.disableRemoteAccount(ctx, adminTarget, client, account, now, reason); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, account.Name, account.ID, reason), nil
}

func (s *Service) disableManagedTargetAfterApplyFailure(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	syncAccount *storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account *sub2api.AdminAccount,
	accountName string,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	key *connector.APIKey,
	keyName string,
	now time.Time,
	reason string,
) (string, error) {
	if account == nil {
		return "", nil
	}
	if err := s.disableRemoteAccount(ctx, adminTarget, client, *account, now, reason); err != nil {
		return "", err
	}
	if err := s.managedAccounts.Upsert(&storage.UpstreamSyncManagedAccount{
		SyncGroupID:        syncGroup.ID,
		SyncAccountID:      syncAccount.ID,
		SourceAPIKeyID:     key.ID,
		SourceAPIKeyName:   keyName,
		TargetAccountID:    account.ID,
		TargetAccountName:  accountName,
		TargetGroupIDsJSON: marshalUintArray(groupIDs(selectedGroups)),
		LastAppliedAt:      &now,
	}); err != nil {
		return "", err
	}
	return disabledRemoteAccountChange(syncAccount, accountName, account.ID, reason), nil
}

func (s *Service) disableRemoteAccount(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account sub2api.AdminAccount,
	now time.Time,
	reason string,
) error {
	account.Status = "inactive"
	disabledDescription := disabledManagedAccountDescription(reason, now)
	account.Notes = disabledDescription
	updated, err := client.UpdateAccount(ctx, adminTarget, account.ID, account)
	if err != nil && isHTTPNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if updated == nil {
		updated = &account
	}
	return s.syncRemoteAccountSchedulable(ctx, adminTarget, client, updated)
}

func (s *Service) syncRemoteAccountSchedulable(
	ctx context.Context,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
	account *sub2api.AdminAccount,
) error {
	if account == nil {
		return nil
	}
	schedulable := strings.EqualFold(strings.TrimSpace(account.Status), "active")
	_, err := client.SetAccountSchedulable(ctx, adminTarget, account.ID, schedulable)
	return err
}

func disabledManagedAccountDescription(reason string, at time.Time) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	return "Upstream Ops 自动禁用：" + reason + "\n同步时间：" + formatSyncNoteTime(at)
}

func isRemoteAccountDisabledForReason(account sub2api.AdminAccount, reason string) bool {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	if !strings.EqualFold(strings.TrimSpace(account.Status), "inactive") || account.Schedulable {
		return false
	}
	return strings.Contains(account.Notes, "Upstream Ops 自动禁用："+reason) ||
		strings.Contains(account.Notes, "Upstream Hub 自动禁用："+reason)
}

func disabledRemoteAccountChange(syncAccount *storage.UpstreamSyncAccount, accountName string, accountID int64, reason string) string {
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "同步失败"
	}
	return fmt.Sprintf("账号%d：%s(ID %d) 已自动禁用，原因：%s", syncAccount.Position+1, accountName, accountID, reason)
}

func testRemoteAccountChange(syncAccount *storage.UpstreamSyncAccount, accountName string, accountID int64, model string, success bool, reason string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		model = "未获取"
	}
	if success {
		return fmt.Sprintf("账号%d：%s(ID %d) 测试模型 %s 通过，调度已启用", syncAccount.Position+1, accountName, accountID, model)
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "测试失败"
	}
	return fmt.Sprintf("账号%d：%s(ID %d) 测试模型 %s 失败，调度已禁用，原因：%s", syncAccount.Position+1, accountName, accountID, model, reason)
}

func syncedAccountNotes(at time.Time) string {
	return "Upstream Ops 同步\n同步时间：" + formatSyncNoteTime(at)
}

func formatSyncNoteTime(at time.Time) string {
	if at.IsZero() {
		at = time.Now()
	}
	return at.Format("2006-01-02 15:04:05")
}

func singleChange(change string) []string {
	if strings.TrimSpace(change) == "" {
		return nil
	}
	return []string{change}
}

type applyErrorWithChanges struct {
	err     error
	changes []string
}

func (e applyErrorWithChanges) Error() string { return e.err.Error() }
func (e applyErrorWithChanges) Unwrap() error { return e.err }

func errorWithChanges(err error, change string) error {
	changes := singleChange(change)
	if len(changes) == 0 {
		return err
	}
	return applyErrorWithChanges{err: err, changes: changes}
}

func changesFromApplyError(err error) []string {
	var wrapped applyErrorWithChanges
	if errors.As(err, &wrapped) {
		return wrapped.changes
	}
	return nil
}

func shouldDisableManagedTargetOnApplyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "source group missing") || strings.Contains(msg, "source channel missing")
}

func shouldCreateDisabledPlaceholderOnApplyError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "source group missing")
}

func buildApplyLogMessage(
	syncGroup *storage.UpstreamSyncGroup,
	target *storage.UpstreamSyncTarget,
	selectedGroups []storage.UpstreamSyncTargetGroup,
	applied int,
	failures []string,
	successes []string,
	changes []string,
	deletedManaged int,
	deletedUnmanaged int,
) string {
	displayName := strings.TrimSpace(syncGroup.DisplayName)
	if displayName == "" {
		displayName = syncGroup.Name
	}
	var b strings.Builder
	if len(failures) == 0 {
		fmt.Fprintf(&b, "applied %d accounts", applied)
	} else {
		fmt.Fprintf(&b, "applied %d, failed %d", applied, len(failures))
	}
	fmt.Fprintf(&b, "\n同步分组：%s (%s)", displayName, syncGroup.Name)
	fmt.Fprintf(&b, "\n目标上游：%s", target.Name)
	fmt.Fprintf(&b, "\n目标分组：%s", targetGroupNames(selectedGroups))
	fmt.Fprintf(&b, "\n排序方向：%s", rateSortDirectionLabel(syncGroup.RateSortDirection))
	if deletedManaged+deletedUnmanaged > 0 {
		fmt.Fprintf(&b, "\n清理：已删除失效托管账号 %d 个，重复远端账号 %d 个", deletedManaged, deletedUnmanaged)
	}
	if len(changes) > 0 {
		b.WriteString("\n\n变动账号：")
		for _, item := range changes {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	if len(successes) > 0 {
		b.WriteString("\n\n成功账号：")
		for _, item := range successes {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	if len(failures) > 0 {
		b.WriteString("\n\n失败账号：")
		for _, item := range failures {
			b.WriteString("\n- ")
			b.WriteString(item)
		}
	}
	return b.String()
}

func targetGroupNames(groups []storage.UpstreamSyncTargetGroup) string {
	if len(groups) == 0 {
		return "未选择"
	}
	out := make([]string, 0, len(groups))
	for _, group := range groups {
		out = append(out, fmt.Sprintf("%s(ID %d，倍率 %s)", group.Name, group.RemoteGroupID, formatNumber(group.Ratio)))
	}
	return strings.Join(out, "、")
}

func sourceGroupLabel(sourceGroupID *int64, sourceGroupName string, groups []connector.APIKeyGroup) string {
	sourceGroupName = strings.TrimSpace(sourceGroupName)
	if sourceGroupID == nil && sourceGroupName == "" {
		return "未绑定"
	}
	for _, group := range groups {
		if sourceGroupID != nil && group.ID != nil && *group.ID == *sourceGroupID {
			return fmt.Sprintf("%s(ID %d，倍率 %s)", group.Name, *group.ID, formatNumber(group.Ratio))
		}
		if sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName) {
			return fmt.Sprintf("%s，倍率 %s", group.Name, formatNumber(group.Ratio))
		}
	}
	if sourceGroupName != "" {
		return sourceGroupName
	}
	return fmt.Sprintf("ID %d", *sourceGroupID)
}

func accountChangeDetails(syncAccount *storage.UpstreamSyncAccount, previous *sub2api.AdminAccount, next sub2api.AdminAccount, nextID int64) []string {
	prefix := fmt.Sprintf("账号%d：%s(ID %d)", syncAccount.Position+1, next.Name, nextID)
	if previous == nil {
		return []string{prefix + " 新增"}
	}
	parts := make([]string, 0)
	if previous.Name != next.Name {
		parts = append(parts, fmt.Sprintf("名称 %s -> %s", previous.Name, next.Name))
	}
	if previous.Priority != next.Priority {
		parts = append(parts, fmt.Sprintf("优先级 %d -> %d", previous.Priority, next.Priority))
	}
	if previous.RateMultiplier != next.RateMultiplier {
		parts = append(parts, fmt.Sprintf("倍率 %s -> %s", formatNumber(previous.RateMultiplier), formatNumber(next.RateMultiplier)))
	}
	if previous.LoadFactor != next.LoadFactor {
		parts = append(parts, fmt.Sprintf("权重 %s -> %s", formatNumber(previous.LoadFactor), formatNumber(next.LoadFactor)))
	}
	if previous.Concurrency != next.Concurrency {
		parts = append(parts, fmt.Sprintf("并发 %d -> %d", previous.Concurrency, next.Concurrency))
	}
	if !sameInt64Slice(previous.GroupIDs, next.GroupIDs) {
		parts = append(parts, fmt.Sprintf("目标分组 %s -> %s", int64SliceLabel(previous.GroupIDs), int64SliceLabel(next.GroupIDs)))
	}
	if !sameInt64Ptr(previous.ProxyID, next.ProxyID) {
		parts = append(parts, fmt.Sprintf("代理 %s -> %s", int64PtrLabel(previous.ProxyID), int64PtrLabel(next.ProxyID)))
	}
	if len(parts) == 0 {
		return nil
	}
	if len(parts) > 0 {
		return []string{prefix + " " + strings.Join(parts, "，")}
	}
	return nil
}

func sameInt64Slice(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameInt64Ptr(a, b *int64) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func int64SliceLabel(list []int64) string {
	if len(list) == 0 {
		return "空"
	}
	parts := make([]string, 0, len(list))
	for _, v := range list {
		parts = append(parts, strconv.FormatInt(v, 10))
	}
	return strings.Join(parts, ",")
}

func int64PtrLabel(v *int64) string {
	if v == nil {
		return "不使用"
	}
	return strconv.FormatInt(*v, 10)
}

func prefixLines(list []string, prefix string) []string {
	out := make([]string, 0, len(list))
	for _, item := range list {
		out = append(out, prefix+item)
	}
	return out
}

func rateSortDirectionLabel(direction string) string {
	if strings.EqualFold(direction, "desc") {
		return "倍率降序，倍率相同按权重从大到小"
	}
	return "倍率升序，倍率相同按权重从大到小"
}

func formatNumber(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func (s *Service) cacheSyncedModels(syncGroup *storage.UpstreamSyncGroup, models []string) error {
	if syncGroup.ModelLimitsMode != "sync_upstream" || len(models) == 0 {
		return nil
	}
	syncGroup.ModelLimitsText = strings.Join(uniqueStrings(models), ",")
	return s.syncGroups.Update(syncGroup)
}

func (s *Service) cleanupDeletedManagedAccounts(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	accounts []storage.UpstreamSyncAccount,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
) (int, error) {
	current := make(map[uint]struct{}, len(accounts))
	for _, account := range accounts {
		if account.ID != 0 {
			current[account.ID] = struct{}{}
		}
	}
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, account := range managedAccounts {
		if _, ok := current[account.SyncAccountID]; ok {
			continue
		}
		if isManagedAccountName(syncGroup, account.TargetAccountName) {
			if err := client.DeleteAccount(ctx, adminTarget, account.TargetAccountID); err != nil {
				return deleted, err
			}
		}
		if err := s.managedAccounts.Delete(account.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) cleanupUnmanagedRemoteAccounts(
	ctx context.Context,
	syncGroup *storage.UpstreamSyncGroup,
	adminTarget sub2api.AdminTarget,
	client *sub2api.AdminClient,
) (int, error) {
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return 0, err
	}
	managedTargetIDs := make(map[int64]struct{}, len(managedAccounts))
	for _, account := range managedAccounts {
		managedTargetIDs[account.TargetAccountID] = struct{}{}
	}
	remoteAccounts, err := client.ListAccounts(ctx, adminTarget, 1, 1000)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, account := range remoteAccounts {
		if _, ok := managedTargetIDs[account.ID]; ok {
			continue
		}
		if !isManagedAccountName(syncGroup, account.Name) {
			continue
		}
		if err := client.DeleteAccount(ctx, adminTarget, account.ID); err != nil {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

func (s *Service) DeleteManaged(ctx context.Context, syncGroupID uint) (*LogDTO, error) {
	syncGroup, err := s.syncGroups.FindByID(syncGroupID)
	if err != nil {
		return nil, err
	}
	managedAccounts, err := s.managedAccounts.ListBySyncGroupID(syncGroup.ID)
	if err != nil {
		return nil, err
	}
	if len(managedAccounts) > 0 {
		target, err := s.targets.FindByID(syncGroup.TargetID)
		if err == nil {
			if plain, decErr := s.cipher.Decrypt(target.AdminAPIKeyCipher); decErr == nil {
				client := s.newAdminClient()
				adminTarget := sub2api.AdminTarget{BaseURL: target.BaseURL, APIKey: plain}
				for _, account := range managedAccounts {
					if isManagedAccountName(syncGroup, account.TargetAccountName) {
						_ = client.DeleteAccount(ctx, adminTarget, account.TargetAccountID)
					}
				}
			}
		}
		syncAccounts, _ := s.syncAccounts.ListBySyncGroupID(syncGroup.ID)
		channelByAccount := make(map[uint]uint, len(syncAccounts))
		for _, account := range syncAccounts {
			channelByAccount[account.ID] = account.SourceChannelID
		}
		for _, account := range managedAccounts {
			if account.SourceAPIKeyName == sourceAPIKeyName(syncGroup) || strings.HasPrefix(account.SourceAPIKeyName, syncGroup.Name+"-账号") {
				if channelID := channelByAccount[account.SyncAccountID]; channelID != 0 {
					_ = s.channelSvc.DeleteAPIKey(ctx, channelID, account.SourceAPIKeyID)
				}
			}
		}
		_ = s.managedAccounts.DeleteBySyncGroupID(syncGroup.ID)
	}
	return s.appendLog(syncGroup.ID, syncGroup.TargetID, "delete_managed", true, "deleted")
}

func (s *Service) ListSyncGroupLogs(syncGroupID uint, page, pageSize int) ([]LogDTO, int64, error) {
	list, total, err := s.logs.ListPageBySyncGroupID(syncGroupID, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	out := make([]LogDTO, 0, len(list))
	for _, item := range list {
		out = append(out, LogDTO{
			ID:          item.ID,
			SyncGroupID: item.SyncGroupID,
			TargetID:    item.TargetID,
			Action:      item.Action,
			Success:     item.Success,
			Message:     item.Message,
			CreatedAt:   item.CreatedAt,
		})
	}
	return out, total, nil
}

func (s *Service) SyncAllOnRateScan(ctx context.Context) {
	syncGroups, err := s.syncGroups.List()
	if err != nil {
		if s.log != nil {
			s.log.Warn("list sync groups for rate scan", "err", err)
		}
		return
	}
	for _, syncGroup := range syncGroups {
		if !syncGroup.Enabled {
			continue
		}
		target, err := s.targets.FindByID(syncGroup.TargetID)
		if err != nil {
			if s.log != nil {
				s.log.Warn("find sync target for rate scan", "syncGroupID", syncGroup.ID, "targetID", syncGroup.TargetID, "err", err)
			}
			continue
		}
		if !target.Enabled {
			continue
		}
		if _, err := s.ApplySyncGroup(ctx, syncGroup.ID); err != nil && s.log != nil {
			s.log.Warn("apply sync group after rate scan", "syncGroupID", syncGroup.ID, "err", err)
		}
	}
}

func (s *Service) checkSourceGroup(ctx context.Context, syncAccount *storage.UpstreamSyncAccount) ([]connector.APIKeyGroup, error) {
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	if syncAccount.SourceGroupID == nil && sourceGroupName == "" {
		return nil, nil
	}
	groups, err := s.channelSvc.ListAPIKeyGroups(ctx, syncAccount.SourceChannelID)
	if err != nil {
		return nil, err
	}
	for _, g := range groups {
		if syncAccount.SourceGroupID != nil && g.ID != nil && *g.ID == *syncAccount.SourceGroupID {
			return groups, nil
		}
		if sourceGroupName != "" && strings.EqualFold(g.Name, sourceGroupName) {
			return groups, nil
		}
	}
	if sourceGroupName != "" {
		return groups, fmt.Errorf("source group missing: %s", sourceGroupName)
	}
	return groups, fmt.Errorf("source group missing: %d", *syncAccount.SourceGroupID)
}

func (s *Service) selectedTargetGroups(syncGroup *storage.UpstreamSyncGroup) ([]storage.UpstreamSyncTargetGroup, []storage.UpstreamSyncTargetGroup, []int64, error) {
	all, err := s.groups.ListByTarget(syncGroup.TargetID, true)
	if err != nil {
		return nil, nil, nil, err
	}
	ids, err := s.syncGroups.ParseTargetGroupIDs(syncGroup)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(ids) == 0 {
		return nil, nil, nil, errors.New("target group missing")
	}
	byID := make(map[uint]storage.UpstreamSyncTargetGroup, len(all))
	for _, g := range all {
		byID[g.ID] = g
	}
	selected := make([]storage.UpstreamSyncTargetGroup, 0, len(ids))
	remoteIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		g, ok := byID[id]
		if !ok || g.Status == "missing" {
			return all, selected, remoteIDs, fmt.Errorf("target group missing: %d", id)
		}
		selected = append(selected, g)
		remoteIDs = append(remoteIDs, g.RemoteGroupID)
	}
	return all, selected, remoteIDs, nil
}

func (s *Service) ensureSourceAPIKey(ctx context.Context, syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, keyName string) (*connector.APIKey, string, error) {
	sourceChannel, err := s.channels.FindByID(syncAccount.SourceChannelID)
	if err != nil {
		return nil, "", err
	}
	unlimitedQuota := boolPtrIf(sourceChannel.Type == storage.ChannelTypeNewAPI)
	neverExpire := int64PtrIf(sourceChannel.Type == storage.ChannelTypeNewAPI, -1)
	var managedKeyID int64
	if mapped, err := s.managedAccounts.FindByAccountID(syncAccount.ID); err == nil && mapped != nil && mapped.SourceAPIKeyID > 0 {
		managedKeyID = mapped.SourceAPIKeyID
	}
	page, err := s.channelSvc.ListAPIKeys(ctx, syncAccount.SourceChannelID, connector.APIKeyQuery{
		Page:     1,
		PageSize: 100,
		Search:   keyName,
	})
	if err != nil {
		return nil, "", err
	}
	var key *connector.APIKey
	if managedKeyID > 0 {
		key = findAPIKeyByID(page.Items, managedKeyID)
	}
	if key == nil {
		key = findAPIKeyByName(page.Items, keyName)
	}
	if key == nil {
		page, err = s.channelSvc.ListAPIKeys(ctx, syncAccount.SourceChannelID, connector.APIKeyQuery{
			Page:     1,
			PageSize: 100,
		})
		if err != nil {
			return nil, "", err
		}
		if managedKeyID > 0 {
			key = findAPIKeyByID(page.Items, managedKeyID)
		}
		if key == nil {
			key = findAPIKeyByName(page.Items, keyName)
		}
	}
	if key != nil {
		name := keyName
		groupName := strings.TrimSpace(syncAccount.SourceGroupName)
		updated, err := s.channelSvc.UpdateAPIKey(ctx, syncAccount.SourceChannelID, key.ID, connector.APIKeyUpdateRequest{
			Name:           &name,
			Group:          stringPtrOrNil(groupName),
			GroupID:        syncAccount.SourceGroupID,
			UnlimitedQuota: unlimitedQuota,
			ExpiredTime:    neverExpire,
		})
		if err != nil {
			return nil, "", err
		}
		key = updated
	} else {
		groupName := strings.TrimSpace(syncAccount.SourceGroupName)
		key, err = s.channelSvc.CreateAPIKey(ctx, syncAccount.SourceChannelID, connector.APIKeyCreateRequest{
			Name:           keyName,
			Group:          groupName,
			GroupID:        syncAccount.SourceGroupID,
			UnlimitedQuota: unlimitedQuota,
			ExpiredTime:    neverExpire,
		})
		if err != nil {
			return nil, "", err
		}
	}
	secret, err := s.channelSvc.RevealAPIKey(ctx, syncAccount.SourceChannelID, key.ID)
	if err != nil {
		return nil, "", err
	}
	return key, secret, nil
}

func findAPIKeyByName(items []connector.APIKey, name string) *connector.APIKey {
	for i := range items {
		if strings.TrimSpace(items[i].Name) == name {
			return &items[i]
		}
	}
	return nil
}

func findAPIKeyByID(items []connector.APIKey, id int64) *connector.APIKey {
	for i := range items {
		if items[i].ID == id {
			return &items[i]
		}
	}
	return nil
}

func (s *Service) buildAdminAccount(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, ch *storage.Channel, apiKey string, remoteGroupIDs []int64, priority int, rateMultiplier float64) sub2api.AdminAccount {
	credentials := map[string]any{
		"api_key":  apiKey,
		"base_url": ch.SiteURL,
	}
	if syncGroup.PoolModeEnabled {
		credentials["pool_mode"] = true
		credentials["pool_mode_retry_count"] = syncGroup.PoolModeRetryCount
		credentials["pool_mode_retry_status_codes"] = parseIntList(syncGroup.PoolModeRetryStatusCodes)
	}
	if syncGroup.CustomErrorCodesEnabled {
		credentials["custom_error_codes_enabled"] = true
		credentials["custom_error_codes"] = parseIntList(syncGroup.CustomErrorCodes)
	}
	if syncGroup.ModelLimitsMode == "custom" {
		if mapping := modelMappingFromModels(splitList(syncGroup.ModelLimitsText)); len(mapping) > 0 {
			credentials["model_mapping"] = mapping
		}
	}
	return sub2api.AdminAccount{
		Platform:       syncGroup.Platform,
		Type:           "apikey",
		Status:         "active",
		Notes:          "",
		Credentials:    credentials,
		ProxyID:        syncAccount.ProxyID,
		Concurrency:    positiveOrDefault(syncAccount.Concurrency, 10),
		Priority:       priority,
		RateMultiplier: rateMultiplier,
		LoadFactor:     float64(syncAccount.Weight),
		GroupIDs:       remoteGroupIDs,
	}
}

func priorityForSourceGroup(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) int {
	ratios := make([]float64, 0, len(groups))
	seen := map[string]struct{}{}
	selectedRatio := 0.0
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	for _, g := range groups {
		ratio := convertRate(g.Ratio, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
		if (syncAccount.SourceGroupID != nil && g.ID != nil && *g.ID == *syncAccount.SourceGroupID) ||
			(sourceGroupName != "" && strings.EqualFold(g.Name, sourceGroupName)) {
			selectedRatio = ratio
		}
		key := strconv.FormatFloat(ratio, 'f', 8, 64)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		ratios = append(ratios, ratio)
	}
	sort.Float64s(ratios)
	if strings.EqualFold(syncGroup.RateSortDirection, "desc") {
		sort.Sort(sort.Reverse(sort.Float64Slice(ratios)))
	}
	rank := 0
	for i, ratio := range ratios {
		if ratio == selectedRatio {
			rank = i
			break
		}
	}
	return rank*1000 - syncAccount.Weight
}

func convertRate(v float64, mode string, customValue float64) float64 {
	return rateconvert.Convert(v, mode, customValue)
}

func rateMultiplierForAccount(syncAccount *storage.UpstreamSyncAccount, groups []connector.APIKeyGroup) float64 {
	if strings.TrimSpace(syncAccount.RateConvertMode) == "custom" {
		return syncAccount.RateConvertValue
	}
	sourceGroupName := strings.TrimSpace(syncAccount.SourceGroupName)
	if syncAccount.SourceGroupID == nil && sourceGroupName == "" {
		return convertRate(1, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
	}
	for _, group := range groups {
		if (syncAccount.SourceGroupID != nil && group.ID != nil && *group.ID == *syncAccount.SourceGroupID) ||
			(sourceGroupName != "" && strings.EqualFold(group.Name, sourceGroupName)) {
			return convertRate(group.Ratio, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
		}
	}
	return convertRate(1, syncAccount.RateConvertMode, syncAccount.RateConvertValue)
}

func managedObjectBaseName(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount) string {
	return fmt.Sprintf("%s-账号%d", syncGroup.Name, syncAccount.Position+1)
}

func managedObjectName(syncGroup *storage.UpstreamSyncGroup, syncAccount *storage.UpstreamSyncAccount, ch *storage.Channel) string {
	base := managedObjectBaseName(syncGroup, syncAccount)
	channelName := strings.TrimSpace(ch.Name)
	if channelName == "" {
		return base
	}
	return fmt.Sprintf("%s [%s]", base, channelName)
}

func managedObjectMatchName(name string) string {
	if idx := strings.Index(name, " ["); idx >= 0 {
		return name[:idx]
	}
	return name
}

func isManagedAccountName(syncGroup *storage.UpstreamSyncGroup, name string) bool {
	return strings.HasPrefix(managedObjectMatchName(name), syncGroup.Name+"-账号")
}

func managedAccountPosition(syncGroup *storage.UpstreamSyncGroup, name string) (int, bool) {
	base := managedObjectMatchName(name)
	prefix := syncGroup.Name + "-账号"
	if !strings.HasPrefix(base, prefix) {
		return 0, false
	}
	rest := strings.TrimPrefix(base, prefix)
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0, false
	}
	n, err := strconv.Atoi(rest[:end])
	if err != nil || n <= 0 {
		return 0, false
	}
	return n - 1, true
}

func sourceAPIKeyName(syncGroup *storage.UpstreamSyncGroup) string {
	return syncGroup.Name
}

func normalizeModelLimits(raw string) string {
	parts := splitList(raw)
	return strings.Join(parts, ",")
}

func normalizeModelLimitsMode(raw string) string {
	if strings.EqualFold(strings.TrimSpace(raw), "custom") {
		return "custom"
	}
	return "sync_upstream"
}

func selectSourceModelKey(items []connector.APIKey, managedKeyID int64, groupID *int64, groupName string) *connector.APIKey {
	if managedKeyID > 0 {
		for i := range items {
			if items[i].ID == managedKeyID && apiKeyUsableForModels(items[i]) && sourceAPIKeyMatchesGroup(items[i], groupID, groupName) {
				return &items[i]
			}
		}
	}
	for i := range items {
		if !apiKeyUsableForModels(items[i]) {
			continue
		}
		if sourceAPIKeyMatchesGroup(items[i], groupID, groupName) {
			return &items[i]
		}
	}
	if sourceModelGroupSpecified(groupID, groupName) {
		return nil
	}
	for i := range items {
		if apiKeyUsableForModels(items[i]) {
			return &items[i]
		}
	}
	return nil
}

func sourceModelGroupSpecified(groupID *int64, groupName string) bool {
	return groupID != nil || strings.TrimSpace(groupName) != ""
}

func sourceAPIKeyMatchesGroup(key connector.APIKey, groupID *int64, groupName string) bool {
	if !sourceModelGroupSpecified(groupID, groupName) {
		return true
	}
	if groupID != nil && key.GroupID != nil && *key.GroupID == *groupID {
		return true
	}
	name := strings.TrimSpace(groupName)
	return name != "" && (strings.EqualFold(strings.TrimSpace(key.GroupName), name) || strings.EqualFold(strings.TrimSpace(key.Group), name))
}

func apiKeyUsableForModels(key connector.APIKey) bool {
	switch strings.ToLower(strings.TrimSpace(key.Status)) {
	case "disabled", "inactive", "expired", "quota_exhausted":
		return false
	default:
		return true
	}
}

func fetchGatewayModels(ctx context.Context, baseURL, platform, apiKey string, audit apiaudit.Options) ([]string, error) {
	endpoint := buildGatewayModelsURL(baseURL, platform)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	apiKey = strings.TrimSpace(apiKey)
	req.Header.Set("Accept", "application/json")
	setGatewayModelAuthHeaders(req, platform, apiKey)
	client := &http.Client{
		Timeout:   20 * time.Second,
		Transport: apiaudit.WrapTransport(http.DefaultTransport, audit),
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, sourceModelsBodyLimit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > sourceModelsBodyLimit {
		return nil, errors.New("model list response is too large")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("model list request failed with HTTP %d", resp.StatusCode)
	}
	models, err := decodeGatewayModels(body)
	if err != nil {
		return nil, err
	}
	if len(models) == 0 {
		return nil, errors.New("model list is empty")
	}
	return models, nil
}

func buildGatewayModelsURL(base, platform string) string {
	normalized := strings.TrimRight(strings.TrimSpace(base), "/")
	if strings.EqualFold(strings.TrimSpace(platform), "gemini") {
		if strings.HasSuffix(normalized, "/v1beta/models") {
			return normalized
		}
		if strings.HasSuffix(normalized, "/v1beta") {
			return normalized + "/models"
		}
		return normalized + "/v1beta/models"
	}
	if strings.HasSuffix(normalized, "/v1/models") {
		return normalized
	}
	if strings.HasSuffix(normalized, "/v1") {
		return normalized + "/models"
	}
	return normalized + "/v1/models"
}

func setGatewayModelAuthHeaders(req *http.Request, platform, apiKey string) {
	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "gemini":
		req.Header.Set("x-goog-api-key", apiKey)
	case "anthropic", "antigravity":
		req.Header.Set("x-api-key", apiKey)
		req.Header.Set("anthropic-version", "2023-06-01")
		req.Header.Set("anthropic-beta", "oauth-2025-04-20")
	default:
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
}

func decodeGatewayModels(body []byte) ([]string, error) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}
	return uniqueStrings(collectGatewayModelIDs(raw)), nil
}

func collectGatewayModelIDs(raw any) []string {
	switch value := raw.(type) {
	case []any:
		out := make([]string, 0, len(value))
		for _, item := range value {
			out = append(out, collectGatewayModelIDs(item)...)
		}
		return out
	case map[string]any:
		out := []string(nil)
		if models, ok := value["models"]; ok {
			if modelMap, ok := models.(map[string]any); ok {
				for id := range modelMap {
					out = append(out, id)
				}
			} else {
				out = append(out, collectGatewayModelIDs(models)...)
			}
		}
		if data, ok := value["data"]; ok {
			out = append(out, collectGatewayModelIDs(data)...)
		}
		for _, key := range []string{"id", "name", "model"} {
			if text, ok := value[key].(string); ok {
				out = append(out, text)
				break
			}
		}
		return out
	case string:
		return []string{value}
	default:
		return nil
	}
}

func parseIntList(raw string) []int {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return []int{}
	}
	var list []int
	if err := json.Unmarshal([]byte(trimmed), &list); err == nil {
		return list
	}
	parts := splitList(raw)
	list = make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err == nil {
			list = append(list, n)
		}
	}
	return list
}

func modelMappingFromModels(models []string) map[string]string {
	out := make(map[string]string, len(models))
	for _, model := range models {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		out[model] = model
	}
	return out
}

func uniqueStrings(list []string) []string {
	out := make([]string, 0, len(list))
	seen := map[string]struct{}{}
	for _, item := range list {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func isHTTPNotFound(err error) bool {
	return err != nil && strings.Contains(err.Error(), "status 404")
}

func splitList(raw string) []string {
	fields := strings.FieldsFunc(raw, func(r rune) bool {
		return r == ',' || r == '\n' || r == '\r' || r == '\t'
	})
	out := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, field := range fields {
		item := strings.TrimSpace(field)
		if item == "" {
			continue
		}
		if _, ok := seen[item]; ok {
			continue
		}
		seen[item] = struct{}{}
		out = append(out, item)
	}
	return out
}

func accountItems(list []SyncAccountDTO) []storage.UpstreamSyncAccount {
	out := make([]storage.UpstreamSyncAccount, 0, len(list))
	for i, item := range list {
		mode := strings.TrimSpace(item.RateConvertMode)
		if mode == "" {
			mode = "raw"
		}
		value := item.RateConvertValue
		if mode != "custom" && value == 0 {
			value = 1
		}
		concurrency := item.Concurrency
		if concurrency <= 0 {
			concurrency = 10
		}
		weight := item.Weight
		if weight <= 0 {
			weight = 1
		}
		out = append(out, storage.UpstreamSyncAccount{
			ID:               item.ID,
			Position:         i,
			SourceChannelID:  item.SourceChannelID,
			SourceGroupID:    item.SourceGroupID,
			SourceGroupName:  strings.TrimSpace(item.SourceGroupName),
			ProxyID:          item.ProxyID,
			Concurrency:      concurrency,
			Weight:           weight,
			RateConvertMode:  mode,
			RateConvertValue: value,
			Enabled:          item.Enabled,
			TestEnabled:      item.TestEnabled,
			TestModel:        strings.TrimSpace(item.TestModel),
		})
	}
	return out
}

func marshalUintArray(list []uint) string {
	if len(list) == 0 {
		return "[]"
	}
	body, _ := json.Marshal(list)
	return string(body)
}

func parseJSONUintArray(raw string) []uint {
	var list []uint
	_ = json.Unmarshal([]byte(raw), &list)
	return list
}

func groupIDs(groups []storage.UpstreamSyncTargetGroup) []uint {
	out := make([]uint, 0, len(groups))
	for _, g := range groups {
		out = append(out, g.ID)
	}
	return out
}

func boolValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

func ptrBool(value bool) *bool {
	return &value
}

func positiveOrDefault(v, fallback int) int {
	if v <= 0 {
		return fallback
	}
	return v
}

func positiveFloatOrDefault(v, fallback float64) float64 {
	if v <= 0 {
		return fallback
	}
	return v
}

func (s *Service) appendLog(syncGroupID, targetID uint, action string, success bool, msg string) (*LogDTO, error) {
	item := &storage.UpstreamSyncLog{
		SyncGroupID: syncGroupID,
		TargetID:    targetID,
		Action:      action,
		Success:     success,
		Message:     msg,
	}
	if err := s.logs.Append(item); err != nil {
		return nil, err
	}
	return &LogDTO{
		ID:          item.ID,
		SyncGroupID: item.SyncGroupID,
		TargetID:    item.TargetID,
		Action:      item.Action,
		Success:     item.Success,
		Message:     item.Message,
		CreatedAt:   item.CreatedAt,
	}, nil
}

func (s *Service) toTargetDTO(item *storage.UpstreamSyncTarget) *TargetDTO {
	return &TargetDTO{
		ID:              item.ID,
		Name:            item.Name,
		BaseURL:         item.BaseURL,
		Enabled:         item.Enabled,
		LastCheckStatus: item.LastCheckStatus,
		LastCheckAt:     item.LastCheckAt,
		LastCheckError:  item.LastCheckError,
	}
}

func (s *Service) toGroupDTO(item *storage.UpstreamSyncTargetGroup) TargetGroupDTO {
	return TargetGroupDTO{
		ID:            item.ID,
		TargetID:      item.TargetID,
		RemoteGroupID: item.RemoteGroupID,
		Name:          item.Name,
		Platform:      item.Platform,
		Ratio:         item.Ratio,
		Status:        item.Status,
		Sort:          item.Sort,
		Description:   item.Description,
		LastSyncAt:    item.LastSyncAt,
	}
}

func (s *Service) toSyncGroupDTO(item *storage.UpstreamSyncGroup, ids []uint, accounts []storage.UpstreamSyncAccount) SyncGroupDTO {
	return SyncGroupDTO{
		ID:                       item.ID,
		DisplayName:              item.DisplayName,
		NameTemplate:             item.NameTemplate,
		Name:                     item.Name,
		TargetID:                 item.TargetID,
		TargetGroupIDs:           ids,
		Platform:                 item.Platform,
		ModelLimitsMode:          normalizeModelLimitsMode(item.ModelLimitsMode),
		ModelLimits:              item.ModelLimitsText,
		PoolModeEnabled:          item.PoolModeEnabled,
		PoolModeRetryCount:       item.PoolModeRetryCount,
		PoolModeRetryStatusCodes: item.PoolModeRetryStatusCodes,
		CustomErrorCodesEnabled:  item.CustomErrorCodesEnabled,
		CustomErrorCodes:         item.CustomErrorCodes,
		RateSortDirection:        item.RateSortDirection,
		Accounts:                 accountDTOs(accounts),
		Enabled:                  ptrBool(item.Enabled),
		ApplyStatus:              item.ApplyStatus,
		ApplyError:               item.ApplyError,
		LastAppliedAt:            item.LastAppliedAt,
	}
}

func (s *Service) syncGroupDTOByItem(item *storage.UpstreamSyncGroup) *SyncGroupDTO {
	ids, _ := s.syncGroups.ParseTargetGroupIDs(item)
	accounts, _ := s.syncAccounts.ListBySyncGroupID(item.ID)
	dto := s.toSyncGroupDTO(item, ids, accounts)
	return &dto
}

func accountDTOs(list []storage.UpstreamSyncAccount) []SyncAccountDTO {
	out := make([]SyncAccountDTO, 0, len(list))
	for _, item := range list {
		out = append(out, SyncAccountDTO{
			ID:               item.ID,
			SourceChannelID:  item.SourceChannelID,
			SourceGroupID:    item.SourceGroupID,
			SourceGroupName:  item.SourceGroupName,
			ProxyID:          item.ProxyID,
			Concurrency:      item.Concurrency,
			Weight:           item.Weight,
			RateConvertMode:  item.RateConvertMode,
			RateConvertValue: item.RateConvertValue,
			Enabled:          item.Enabled,
			TestEnabled:      item.TestEnabled,
			TestModel:        item.TestModel,
		})
	}
	return out
}

func renderSyncGroupName(tpl string, syncGroupID uint, channelID uint, sourceGroupID int64) string {
	out := strings.ReplaceAll(tpl, "{同步分组ID}", strconv.FormatUint(uint64(syncGroupID), 10))
	out = strings.ReplaceAll(out, "{渠道ID}", strconv.FormatUint(uint64(channelID), 10))
	out = strings.ReplaceAll(out, "{源分组ID}", strconv.FormatInt(sourceGroupID, 10))
	return strings.TrimSpace(out)
}

func stringPtrOrNil(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func boolPtrIf(value bool) *bool {
	if !value {
		return nil
	}
	return &value
}

func int64PtrIf(ok bool, value int64) *int64 {
	if !ok {
		return nil
	}
	return &value
}

func ptrTime(t time.Time) *time.Time { return &t }
