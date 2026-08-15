package storage

import (
	"errors"
	"strings"

	"gorm.io/gorm"
)

// Channels 渠道仓库。
type Channels struct {
	db         *gorm.DB
	readCaches *storageReadCaches
}

func NewChannels(db *gorm.DB) *Channels {
	return &Channels{db: db, readCaches: readCachesForDB(db)}
}

func (r *Channels) Create(c *Channel) error {
	err := r.db.Create(c).Error
	if err == nil {
		r.readCaches.channels.invalidate(c.ID)
	}
	return err
}
func (r *Channels) Update(c *Channel) error {
	err := r.db.Save(c).Error
	if err == nil {
		r.readCaches.channels.invalidate(c.ID)
	}
	return err
}
func (r *Channels) Delete(id uint) error {
	err := r.db.Transaction(func(tx *gorm.DB) error {
		var channel Channel
		if err := tx.Select("id", "name").First(&channel, id).Error; err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err := tx.Where("channel_id = ?", id).Delete(&AuthSession{}).Error; err != nil {
			return err
		}
		for _, model := range []any{
			&RateSnapshot{},
			&RateChangeLog{},
			&BalanceSnapshot{},
			&CostSnapshot{},
			&MonitorLog{},
			&NotificationCooldown{},
			&UpstreamAnnouncement{},
		} {
			if err := tx.Where("channel_id = ?", id).Delete(model).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("upstream_channel_id = ?", id).Delete(&NotificationLog{}).Error; err != nil {
			return err
		}
		if channel.Name != "" {
			pattern := "%" + strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(channel.Name) + "%"
			if err := tx.Where("upstream_channel_id = 0 AND (subject LIKE ? ESCAPE '!' OR body LIKE ? ESCAPE '!')", pattern, pattern).
				Delete(&NotificationLog{}).Error; err != nil {
				return err
			}
		}
		if err := tx.Where("source_kind = ? AND source_id = ?", GatewayRouteSourceMonitor, id).
			Delete(&GatewayChannelCacheHealth{}).Error; err != nil {
			return err
		}
		return tx.Delete(&Channel{}, id).Error
	})
	if err == nil {
		r.readCaches.channels.invalidate(id)
	}
	return err
}
func (r *Channels) FindByID(id uint) (*Channel, error) {
	item, err := r.readCaches.channels.load(id, func() (Channel, error) {
		var c Channel
		err := r.db.First(&c, id).Error
		return c, err
	}, nil)
	if err != nil {
		return nil, err
	}
	return &item, nil
}
func (r *Channels) List() ([]Channel, error) {
	var list []Channel
	if err := r.db.Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
func (r *Channels) ListPage(page, pageSize int) ([]Channel, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 && pageSize != -1 {
		pageSize = 20
	}
	var total int64
	if err := r.db.Model(&Channel{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []Channel
	q := r.db.Order("sort_order DESC").Order("id ASC")
	if pageSize != -1 {
		q = q.Offset((page - 1) * pageSize).Limit(pageSize)
	}
	if err := q.Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}
func (r *Channels) ListMonitorEnabled() ([]Channel, error) {
	var list []Channel
	if err := r.db.Where("monitor_enabled = ?", true).Order("sort_order DESC").Order("id ASC").Find(&list).Error; err != nil {
		return nil, err
	}
	return list, nil
}
func (r *Channels) UpdateBalance(id uint, balance float64, at any, lastErr string) error {
	err := r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"last_balance":    balance,
		"last_balance_at": at,
		"last_error":      lastErr,
	}).Error
	if err == nil {
		r.readCaches.channels.invalidate(id)
	}
	return err
}

func (r *Channels) UpdateCosts(id uint, todayCost float64, totalCost float64) error {
	err := r.db.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{
		"today_cost": todayCost,
		"total_cost": totalCost,
	}).Error
	if err == nil {
		r.readCaches.channels.invalidate(id)
	}
	return err
}
func (r *Channels) SetLastError(id uint, msg string) error {
	err := r.db.Model(&Channel{}).Where("id = ?", id).Update("last_error", msg).Error
	if err == nil {
		r.readCaches.channels.invalidate(id)
	}
	return err
}
