package storage

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm/clause"
)

var ErrVideoTaskBindingConflict = errors.New("video task ID already belongs to another upstream")

// GatewayVideoTaskBinding is independent of usage retention and route edits.
// SnapshotCipher contains the original transport credentials, encrypted by
// the application cipher. Task IDs are scoped to the creating gateway key.
type GatewayVideoTaskBinding struct {
	ID                uint   `gorm:"primaryKey"`
	GatewayGroupID    uint   `gorm:"not null;uniqueIndex:idx_video_task_owner"`
	GatewayKeyID      uint   `gorm:"not null;uniqueIndex:idx_video_task_owner"`
	TaskID            string `gorm:"size:256;not null;uniqueIndex:idx_video_task_owner"`
	TargetFingerprint string `gorm:"size:64;not null" json:"-"`
	SnapshotCipher    string `gorm:"type:text;not null" json:"-"`
	CreatedAt         time.Time
	ExpiresAt         time.Time `gorm:"not null;index"`
}

func (GatewayVideoTaskBinding) TableName() string { return "gateway_video_task_bindings" }

func (r *GatewayRoutes) SaveVideoTaskBinding(ctx context.Context, binding *GatewayVideoTaskBinding) error {
	if r == nil || r.db == nil || binding == nil || binding.GatewayGroupID == 0 || binding.GatewayKeyID == 0 ||
		binding.TaskID == "" || len(binding.TaskID) > 256 || binding.SnapshotCipher == "" || binding.TargetFingerprint == "" {
		return errors.New("invalid video task binding")
	}
	// Never overwrite a task from another upstream, even if two sites happen
	// to issue the same ID. An idempotent submission to the same target is OK.
	result := r.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "gateway_group_id"}, {Name: "gateway_key_id"}, {Name: "task_id"}},
		DoNothing: true,
	}).Create(binding)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}
	var existing GatewayVideoTaskBinding
	if err := r.db.WithContext(ctx).Where("gateway_group_id = ? AND gateway_key_id = ? AND task_id = ?",
		binding.GatewayGroupID, binding.GatewayKeyID, binding.TaskID).First(&existing).Error; err != nil {
		return err
	}
	if existing.TargetFingerprint != binding.TargetFingerprint || !existing.ExpiresAt.After(time.Now()) {
		return ErrVideoTaskBindingConflict
	}
	return nil
}

func (r *GatewayRoutes) FindVideoTaskBinding(ctx context.Context, groupID, keyID uint, taskID string, now time.Time) (*GatewayVideoTaskBinding, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("video task repository is not configured")
	}
	var binding GatewayVideoTaskBinding
	err := r.db.WithContext(ctx).Where("gateway_group_id = ? AND gateway_key_id = ? AND task_id = ? AND expires_at > ?",
		groupID, keyID, taskID, now).First(&binding).Error
	return &binding, err
}

// Bounded cleanup runs only when new videos are submitted, never on the text
// forwarding hot path. Expired bindings are already excluded from lookups.
func (r *GatewayRoutes) PruneVideoTaskBindings(ctx context.Context, now time.Time) error {
	var ids []uint
	if err := r.db.WithContext(ctx).Model(&GatewayVideoTaskBinding{}).
		Where("expires_at <= ?", now).Order("expires_at").Limit(100).Pluck("id", &ids).Error; err != nil {
		return err
	}
	if len(ids) == 0 {
		return nil
	}
	return r.db.WithContext(ctx).Where("id IN ? AND expires_at <= ?", ids, now).Delete(&GatewayVideoTaskBinding{}).Error
}
