package updater

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

var jobIDPattern = regexp.MustCompile(`^[0-9a-f]{24}$`)

// Keep the current recovery point plus the preceding three local jobs. The
// rollback picker is the release catalog, not the set of cached artifacts.
func (d *localDeployment) cleanup(current Job) {
	if current.Phase == "rollback_failed" {
		return
	}
	root := filepath.Join(d.cfg.StateDir, "jobs")
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	type backup struct {
		name string
		time time.Time
	}
	older := []backup{}
	for _, entry := range entries {
		if entry.Name() == current.ID || !entry.IsDir() || !jobIDPattern.MatchString(entry.Name()) {
			continue
		}
		if info, err := entry.Info(); err == nil {
			older = append(older, backup{entry.Name(), info.ModTime()})
		}
	}
	sort.Slice(older, func(i, j int) bool { return older[i].time.After(older[j].time) })
	for i := 3; i < len(older); i++ {
		if d.cfg.Mode == "docker" {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			_, err := d.commands.Run(ctx, "docker", "image", "rm", "upstream-ops-rollback:"+older[i].name)
			cancel()
			if err != nil {
				continue
			}
		}
		_ = os.RemoveAll(filepath.Join(root, older[i].name))
	}
}
