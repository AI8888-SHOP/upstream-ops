package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const imageOverride = "docker-compose.web-update.yml"

var imageDigest = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

func dotenvValue(data []byte, key string) string {
	value := ""
	for _, line := range strings.Split(string(data), "\n") {
		name, item, found := strings.Cut(strings.TrimSpace(line), "=")
		if found && strings.TrimSpace(name) == key {
			value = strings.TrimSpace(item)
			if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
				value = value[1 : len(value)-1]
			}
		}
	}
	return value
}

func (d *localDeployment) composeFiles() ([]string, error) {
	raw := d.cfg.ComposeFiles
	if raw == "" {
		data, err := os.ReadFile(filepath.Join(d.cfg.ProjectDir, ".env"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		raw = dotenvValue(data, "COMPOSE_FILE")
	}
	if raw == "" {
		raw = "docker-compose.yml"
		if _, err := os.Stat(filepath.Join(d.cfg.ProjectDir, "docker-compose.updater.yml")); err == nil {
			raw += ":docker-compose.updater.yml"
		}
	}
	files := []string{}
	for _, name := range strings.Split(raw, ":") {
		name = strings.TrimSpace(name)
		if name == "" || strings.ContainsAny(name, "\r\n'\"") {
			return nil, errors.New("COMPOSE_FILE 中包含非法文件名")
		}
		path := name
		if !filepath.IsAbs(path) {
			path = filepath.Join(d.cfg.ProjectDir, path)
		}
		if filepath.Clean(path) == filepath.Join(d.cfg.ProjectDir, imageOverride) {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
		files = append(files, path)
	}
	if len(files) == 0 {
		return nil, errors.New("没有 Compose 配置文件")
	}
	return files, nil
}

func (d *localDeployment) compose(ctx context.Context, args ...string) ([]byte, error) {
	files, err := d.composeFiles()
	if err != nil {
		return nil, err
	}
	command := []string{"compose", "--project-directory", d.cfg.ProjectDir}
	if _, err := os.Stat(filepath.Join(d.cfg.ProjectDir, ".env")); err == nil {
		command = append(command, "--env-file", filepath.Join(d.cfg.ProjectDir, ".env"))
	}
	for _, file := range files {
		command = append(command, "-f", file)
	}
	if _, err := os.Stat(filepath.Join(d.cfg.ProjectDir, imageOverride)); err == nil {
		command = append(command, "-f", filepath.Join(d.cfg.ProjectDir, imageOverride))
	}
	command = append(command, args...)
	return d.commands.Run(ctx, "docker", command...)
}

func (d *localDeployment) container(ctx context.Context) (string, error) {
	output, err := d.compose(ctx, "ps", "--all", "-q", d.cfg.Service)
	if err != nil {
		return "", err
	}
	ids := strings.Fields(string(output))
	if len(ids) != 1 {
		return "", errors.New("自动更新要求目标服务恰好有一个容器，不支持多副本同时切换")
	}
	return ids[0], nil
}

func (d *localDeployment) prepareDocker(ctx context.Context, job *Job, release Release) error {
	if _, err := d.compose(ctx, "config", "--quiet"); err != nil {
		return err
	}
	container, err := d.container(ctx)
	if err != nil {
		return err
	}
	label, err := d.commands.Run(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "io.upstream-ops.update-agent"}}`, container)
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(label)) == "true" {
		return errors.New("目标容器不能是更新服务自身")
	}
	image, err := d.commands.Run(ctx, "docker", "inspect", "--format", "{{.Image}}", container)
	if err != nil {
		return err
	}
	job.PreviousArtifact = strings.TrimSpace(string(image))
	if !imageDigest.MatchString(job.PreviousArtifact) {
		return errors.New("当前容器没有有效镜像 ID")
	}
	if err := d.Healthy(ctx, job, true); err != nil {
		return fmt.Errorf("当前容器尚不健康: %w", err)
	}
	// A local immutable backup tag keeps the old image reachable even when
	// its mutable :latest tag moves. It is never removed during this update.
	if _, err := d.commands.Run(ctx, "docker", "image", "tag", job.PreviousArtifact, "upstream-ops-rollback:"+job.ID); err != nil {
		return err
	}
	ref := ImageRepository + ":" + release.Tag
	if _, err := d.commands.Run(ctx, "docker", "image", "pull", ref); err != nil {
		return err
	}
	image, err = d.commands.Run(ctx, "docker", "image", "inspect", "--format", "{{.Id}}", ref)
	if err != nil {
		return err
	}
	job.TargetArtifact = strings.TrimSpace(string(image))
	if !imageDigest.MatchString(job.TargetArtifact) {
		return errors.New("下载的镜像没有有效 ID")
	}
	return nil
}

func replaceDotenv(data []byte, key, value string) []byte {
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	out := make([]string, 0, len(lines)+1)
	for _, line := range lines {
		name, _, found := strings.Cut(strings.TrimSpace(line), "=")
		if found && strings.TrimSpace(name) == key {
			continue
		}
		out = append(out, line)
	}
	out = append(out, key+"='"+value+"'")
	return []byte(strings.Join(out, "\n") + "\n")
}

func (d *localDeployment) switchDocker(ctx context.Context, image string) error {
	if !imageDigest.MatchString(image) {
		return errors.New("拒绝使用未固定的镜像切换")
	}
	files, err := d.composeFiles()
	if err != nil {
		return err
	}
	overridePath := filepath.Join(d.cfg.ProjectDir, imageOverride)
	content := fmt.Sprintf("# Managed by UpstreamOps update agent.\nservices:\n  %q:\n    image: %s\n", d.cfg.Service, image)
	if err := atomicWrite(overridePath, []byte(content), 0644); err != nil {
		return err
	}
	// Persist selection for subsequent ordinary `docker compose up`, not
	// only the one command the agent is about to execute. Preserve all other
	// dotenv keys and all existing PostgreSQL/network overrides.
	files = append(files, overridePath)
	envPath := filepath.Join(d.cfg.ProjectDir, ".env")
	env, err := os.ReadFile(envPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := atomicWrite(envPath, replaceDotenv(env, "COMPOSE_FILE", strings.Join(files, ":")), 0600); err != nil {
		return err
	}
	_, err = d.compose(ctx, "up", "-d", "--no-deps", "--pull", "never", "--force-recreate", d.cfg.Service)
	return err
}
