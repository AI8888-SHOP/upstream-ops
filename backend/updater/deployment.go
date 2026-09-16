package updater

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type commandRunner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

var errDeploymentChanged = errors.New("部署在准备期间被外部修改，取消更新")

// A failed preflight must not restore an older artifact over a deployment
// that another operator may have just changed.
var errNotApplied = errors.New("尚未切换应用")

type execCommands struct{}
type limitedOutput struct{ bytes.Buffer }

func (w *limitedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if available := (64 << 10) - w.Len(); available > 0 {
		if len(p) > available {
			p = p[:available]
		}
		_, _ = w.Buffer.Write(p)
	}
	return n, nil
}
func (execCommands) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var output, diagnostics limitedOutput
	cmd.Stdout, cmd.Stderr = &output, &diagnostics
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s 执行失败: %w: %s", name, err, strings.TrimSpace(diagnostics.String()+"\n"+output.String()))
	}
	return output.Bytes(), nil
}

type localDeployment struct {
	cfg       Config
	commands  commandRunner
	downloads *http.Client
}

func (d *localDeployment) jobDir(job *Job) string {
	return filepath.Join(d.cfg.StateDir, "jobs", job.ID)
}

func fileSHA(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (d *localDeployment) Prepare(ctx context.Context, job *Job, release Release) error {
	if err := os.MkdirAll(d.jobDir(job), 0700); err != nil {
		return err
	}
	if err := syncDir(filepath.Dir(d.jobDir(job))); err != nil {
		return err
	}
	if d.cfg.Mode == "docker" {
		return d.prepareDocker(ctx, job, release)
	}
	info, err := os.Lstat(d.cfg.Binary)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 {
		return errors.New("原生程序必须是可执行的普通文件，不能直接替换符号链接或目录")
	}
	job.BinaryMode = uint32(info.Mode().Perm())
	if job.PreviousArtifact, err = fileSHA(d.cfg.Binary); err != nil {
		return err
	}
	if err := d.Healthy(ctx, job, true); err != nil {
		return fmt.Errorf("当前服务与配置不匹配: %w", err)
	}
	if err := copyAtomic(d.cfg.Binary, filepath.Join(d.jobDir(job), "previous"), 0700); err != nil {
		return err
	}
	if digest, err := fileSHA(filepath.Join(d.jobDir(job), "previous")); err != nil || digest != job.PreviousArtifact {
		return errors.New("备份程序与当前版本不一致，取消更新")
	}
	staged := filepath.Join(d.jobDir(job), "target")
	if err := d.downloadBinary(ctx, release, staged); err != nil {
		return err
	}
	job.TargetArtifact, err = fileSHA(staged)
	if err != nil {
		return err
	}
	return syncDir(d.jobDir(job))
}

func (d *localDeployment) Apply(ctx context.Context, job *Job) error {
	if d.cfg.Mode == "docker" {
		container, err := d.container(ctx)
		if err != nil {
			return fmt.Errorf("%w: %w", errNotApplied, err)
		}
		current, err := d.commands.Run(ctx, "docker", "inspect", "--format", "{{.Image}}", container)
		if err != nil {
			return fmt.Errorf("%w: %w", errNotApplied, err)
		}
		if strings.TrimSpace(string(current)) != job.PreviousArtifact {
			return fmt.Errorf("%w: %w", errNotApplied, errDeploymentChanged)
		}
		return d.switchDocker(ctx, job.TargetArtifact)
	}
	// Refuse a stale plan if an operator replaced the executable meanwhile.
	current, err := fileSHA(d.cfg.Binary)
	if err != nil {
		return fmt.Errorf("%w: %w", errNotApplied, err)
	}
	if current != job.PreviousArtifact {
		return fmt.Errorf("%w: %w", errNotApplied, errDeploymentChanged)
	}
	staged := filepath.Join(d.jobDir(job), "target")
	if actual, err := fileSHA(staged); err != nil || actual != job.TargetArtifact {
		return fmt.Errorf("%w: 目标程序校验失败", errNotApplied)
	}
	if err := copyAtomic(staged, d.cfg.Binary, os.FileMode(job.BinaryMode)); err != nil {
		return err
	}
	_, err = d.commands.Run(ctx, "systemctl", "restart", d.cfg.Unit)
	return err
}

func (d *localDeployment) Rollback(ctx context.Context, job *Job) error {
	if job.PreviousArtifact == "" {
		return errors.New("缺少可恢复的原版本记录")
	}
	if d.cfg.Mode == "docker" {
		return d.switchDocker(ctx, job.PreviousArtifact)
	}
	backup := filepath.Join(d.jobDir(job), "previous")
	actual, err := fileSHA(backup)
	if err != nil {
		return err
	}
	if actual != job.PreviousArtifact {
		return errors.New("原版本备份校验失败")
	}
	if err := copyAtomic(backup, d.cfg.Binary, os.FileMode(job.BinaryMode)); err != nil {
		return err
	}
	// A broken version may have exhausted systemd's restart burst limit.
	// Clear it for this unit so the healthy old executable can start again.
	if _, err := d.commands.Run(ctx, "systemctl", "reset-failed", d.cfg.Unit); err != nil {
		return err
	}
	_, err = d.commands.Run(ctx, "systemctl", "restart", d.cfg.Unit)
	return err
}

func (d *localDeployment) Healthy(ctx context.Context, job *Job, rollback bool) error {
	wantArtifact, wantVersion := job.TargetArtifact, job.Target
	if rollback {
		wantArtifact, wantVersion = job.PreviousArtifact, job.Previous
	}
	var body []byte
	if d.cfg.Mode == "docker" {
		container, err := d.container(ctx)
		if err != nil {
			return err
		}
		actual, err := d.commands.Run(ctx, "docker", "inspect", "--format", "{{.Image}}", container)
		if err != nil {
			return err
		}
		if strings.TrimSpace(string(actual)) != wantArtifact {
			return errors.New("正在运行的容器不是本次选定镜像")
		}
		body, err = d.commands.Run(ctx, "docker", "exec", container, "env", "http_proxy=", "https_proxy=", "HTTP_PROXY=", "HTTPS_PROXY=", "wget", "-q", "-T", "4", "-O", "-", d.cfg.HealthURL)
		if err != nil {
			return err
		}
	} else {
		pidBytes, err := d.commands.Run(ctx, "systemctl", "show", "--property=MainPID", "--value", d.cfg.Unit)
		if err != nil {
			return err
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(pidBytes)))
		if err != nil || pid <= 1 {
			return errors.New("systemd 服务没有有效主进程")
		}
		if pid == os.Getpid() {
			return errors.New("目标服务不能是更新进程自身")
		}
		actual, err := fileSHA(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			return err
		}
		if actual != wantArtifact {
			return errors.New("systemd 主进程尚未运行选定程序")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.HealthURL, nil)
		if err != nil {
			return err
		}
		client := &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second}
		defer client.CloseIdleConnections()
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("健康接口返回 %d", resp.StatusCode)
		}
		body, err = io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return err
		}
	}
	var health struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	if err := json.Unmarshal(body, &health); err != nil {
		return err
	}
	if health.Status != "ok" {
		return errors.New("应用或数据库尚未就绪")
	}
	// Older releases have no version in /healthz. Executable SHA or exact
	// Docker image ID still proves that the selected artifact is running.
	if health.Version != "" && strings.TrimPrefix(health.Version, "v") != strings.TrimPrefix(wantVersion, "v") {
		return errors.New("健康接口版本与选定版本不一致")
	}
	return nil
}
