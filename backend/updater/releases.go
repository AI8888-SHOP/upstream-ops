// Package updater implements a separate, durable update agent. The web
// process never owns the task that stops/replaces that same web process.
package updater

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const Repository = "AI8888-SHOP/upstream-ops"
const ImageRepository = "ghcr.io/ai8888-shop/upstream-ops"
const releasesURL = "https://api.github.com/repos/" + Repository + "/releases?per_page=50"

var stableTag = regexp.MustCompile(`^v?(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type Asset struct {
	Name   string `json:"name"`
	URL    string `json:"browser_download_url"`
	Digest string `json:"digest"`
}
type Release struct {
	Tag         string    `json:"tag_name"`
	URL         string    `json:"html_url"`
	PublishedAt time.Time `json:"published_at"`
	Draft       bool      `json:"draft"`
	Prerelease  bool      `json:"prerelease"`
	Assets      []Asset   `json:"assets,omitempty"`
}
type Catalog struct {
	Current         string    `json:"current_version"`
	UpdateAvailable bool      `json:"update_available"`
	Latest          *Release  `json:"latest"`
	Rollback        []Release `json:"rollback"`
}
type releaseSource struct {
	mu     sync.Mutex
	client *http.Client
	url    string
	at     time.Time
	items  []Release
}

func compareVersions(a, b string) int {
	aParts, bParts := strings.Split(strings.TrimPrefix(a, "v"), "."), strings.Split(strings.TrimPrefix(b, "v"), ".")
	for i := 0; i < 3; i++ {
		// Decimal length then lexical comparison also handles oversized input
		// without integer overflow. Stable tags reject leading zeros.
		if len(aParts[i]) < len(bParts[i]) {
			return -1
		}
		if len(aParts[i]) > len(bParts[i]) {
			return 1
		}
		if aParts[i] < bParts[i] {
			return -1
		}
		if aParts[i] > bParts[i] {
			return 1
		}
	}
	return 0
}

func (s *releaseSource) list(ctx context.Context, force bool) ([]Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !force && len(s.items) > 0 && time.Since(s.at) < 5*time.Minute {
		return s.items, nil
	}
	endpoint := s.url
	if endpoint == "" {
		endpoint = releasesURL
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "upstream-ops-updater")
	client := s.client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("检查 GitHub 发版失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub 发版接口返回 %d", resp.StatusCode)
	}
	var releases []Release
	if err := json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(&releases); err != nil {
		return nil, err
	}
	items := make([]Release, 0, len(releases))
	seen := map[string]bool{}
	for _, release := range releases {
		key := strings.TrimPrefix(release.Tag, "v")
		if release.Draft || release.Prerelease || !stableTag.MatchString(release.Tag) || seen[key] {
			continue
		}
		seen[key] = true
		items = append(items, release)
	}
	sort.Slice(items, func(i, j int) bool { return compareVersions(items[i].Tag, items[j].Tag) > 0 })
	if len(items) == 0 {
		return nil, errors.New("仓库没有正式发行版")
	}
	s.items, s.at = items, time.Now()
	return items, nil
}

func catalogFor(current string, items []Release) (Catalog, error) {
	if !stableTag.MatchString(current) {
		return Catalog{}, errors.New("当前不是正式版本，不能自动升级或回退")
	}
	result := Catalog{Current: current, Rollback: []Release{}}
	if len(items) > 0 {
		latest := items[0]
		latest.Assets = nil
		result.Latest = &latest
		result.UpdateAvailable = compareVersions(latest.Tag, current) > 0
	}
	for _, release := range items {
		if compareVersions(release.Tag, current) < 0 && len(result.Rollback) < 3 {
			release.Assets = nil
			result.Rollback = append(result.Rollback, release)
		}
	}
	return result, nil
}

func selectRelease(current, target, action string, items []Release) (Release, error) {
	catalog, err := catalogFor(current, items)
	if err != nil {
		return Release{}, err
	}
	allowed := false
	if action == "upgrade" && catalog.Latest != nil {
		allowed = target == catalog.Latest.Tag && compareVersions(target, current) > 0
	}
	if action == "rollback" {
		for _, release := range catalog.Rollback {
			if target == release.Tag {
				allowed = true
			}
		}
	}
	if !allowed {
		return Release{}, errors.New("只能升级到最新正式版，或回退到当前版本之前最近三个正式发版")
	}
	for _, release := range items {
		if release.Tag == target {
			return release, nil
		}
	}
	return Release{}, errors.New("发行版不存在")
}

func assetFor(release Release, name string) (Asset, error) {
	for _, asset := range release.Assets {
		if asset.Name == name {
			prefix := "https://github.com/" + Repository + "/releases/download/" + release.Tag + "/"
			if !strings.HasPrefix(asset.URL, prefix) {
				return Asset{}, errors.New("发行版下载地址不属于本仓库")
			}
			return asset, nil
		}
	}
	return Asset{}, fmt.Errorf("发行版缺少文件 %s", strconv.Quote(name))
}
