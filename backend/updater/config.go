package updater

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func (c Config) identity() string {
	digest := sha256.Sum256([]byte(strings.Join([]string{c.Mode, c.ProjectDir, c.ComposeFiles, c.Service, c.Binary, c.Unit, c.HealthURL}, "\x00")))
	return hex.EncodeToString(digest[:])
}

type Config struct {
	Mode, StateDir, Socket            string
	SocketGroup                       string
	ProjectDir, ComposeFiles, Service string
	Binary, Unit, HealthURL           string
	HealthTimeout                     time.Duration
}

var safeService = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

func ConfigFromEnv() (Config, error) {
	c := Config{Mode: os.Getenv("UPDATER_MODE"), StateDir: os.Getenv("UPDATER_STATE_DIR"), Socket: os.Getenv("UPDATER_SOCKET"), SocketGroup: os.Getenv("UPDATER_SOCKET_GROUP"),
		ProjectDir: os.Getenv("UPDATER_PROJECT_DIR"), ComposeFiles: os.Getenv("UPDATER_COMPOSE_FILES"), Service: os.Getenv("UPDATER_SERVICE"),
		Binary: os.Getenv("UPDATER_BINARY"), Unit: os.Getenv("UPDATER_SYSTEMD_UNIT"), HealthURL: os.Getenv("UPDATER_HEALTH_URL"), HealthTimeout: 180 * time.Second}
	if c.StateDir == "" {
		c.StateDir = "/app/data/.updater"
	}
	if c.Socket == "" {
		c.Socket = filepath.Join(c.StateDir, "control.sock")
	}
	if c.Service == "" {
		c.Service = "app"
	}
	if c.HealthURL == "" {
		c.HealthURL = "http://127.0.0.1:8418/healthz"
	}
	if value := os.Getenv("UPDATER_HEALTH_TIMEOUT_SECONDS"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 30 || n > 1800 {
			return c, errors.New("UPDATER_HEALTH_TIMEOUT_SECONDS must be 30–1800")
		}
		c.HealthTimeout = time.Duration(n) * time.Second
	}
	return c, c.validate()
}

func (c Config) validate() error {
	if runtime.GOOS != "linux" {
		return errors.New("自动更新进程目前支持 Linux systemd 与 Linux Docker 容器")
	}
	if !filepath.IsAbs(c.StateDir) || !filepath.IsAbs(c.Socket) {
		return errors.New("更新状态和 socket 必须使用绝对路径")
	}
	health, err := url.Parse(c.HealthURL)
	if err != nil || health.Scheme != "http" || health.Hostname() != "127.0.0.1" || health.User != nil || health.Path != "/healthz" || health.RawQuery != "" || health.Fragment != "" {
		return errors.New("更新健康检查必须指向本地 127.0.0.1 端口")
	}
	if port, err := strconv.Atoi(health.Port()); err != nil || port < 1 || port > 65535 {
		return errors.New("更新健康检查端口不合法")
	}
	switch c.Mode {
	case "docker":
		if !filepath.IsAbs(c.ProjectDir) || !safeService.MatchString(c.Service) {
			return errors.New("Docker 更新需要绝对路径 UPDATER_PROJECT_DIR 和合法服务名")
		}
	case "systemd":
		if !filepath.IsAbs(c.Binary) || !safeService.MatchString(c.Unit) || !strings.HasSuffix(c.Unit, ".service") {
			return errors.New("原生更新需要 UPDATER_BINARY 绝对路径和 UPDATER_SYSTEMD_UNIT 服务名")
		}
	default:
		return errors.New("UPDATER_MODE 必须为 docker 或 systemd")
	}
	return nil
}
