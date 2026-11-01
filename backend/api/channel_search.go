package api

import (
	"context"
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

// Search all saved groups before pagination. Channels configured to show only
// groups with keys retain the same visibility rule as their inline group list.
func channelSearchPage(ctx context.Context, d *Deps, page, pageSize int, search string) ([]storage.Channel, int64, error) {
	term := strings.ToLower(strings.TrimSpace(search))
	if term == "" {
		return d.Channels.ListPage(page, pageSize)
	}
	candidates, _, err := d.Channels.ListPage(1, -1, term)
	if err != nil {
		return nil, 0, err
	}
	visible := make([]storage.Channel, 0, len(candidates))
	for _, item := range candidates {
		if item.OnlyCreatedKeyGroupsEnabled {
			if d.ChannelSvc == nil || d.Rates == nil {
				return nil, 0, fmt.Errorf("分组可见性服务未就绪")
			}
			groups, err := d.Rates.ListByChannel(item.ID)
			if err != nil {
				return nil, 0, err
			}
			keys, err := d.ChannelSvc.ListAPIKeyGroupSet(ctx, item.ID)
			if err != nil {
				return nil, 0, err
			}
			matched := false
			for _, group := range groups {
				if keys.ContainsSnapshot(group) && (strings.Contains(strings.ToLower(group.ModelName), term) || strings.Contains(strings.ToLower(group.Description), term)) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}
		visible = append(visible, item)
	}
	total := int64(len(visible))
	if pageSize == -1 {
		return visible, total, nil
	}
	// Compare before multiplying, so an excessive client page cannot overflow.
	if len(visible) == 0 || page > (len(visible)-1)/pageSize+1 {
		return []storage.Channel{}, total, nil
	}
	start := (page - 1) * pageSize
	end := min(start+pageSize, len(visible))
	return visible[start:end], total, nil
}
