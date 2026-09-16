package updater

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"
)

type Job struct {
	DeploymentID     string    `json:"deployment_id,omitempty"`
	ID               string    `json:"id"`
	Action           string    `json:"action"`
	Phase            string    `json:"phase"`
	Target           string    `json:"target_version"`
	Previous         string    `json:"previous_version"`
	Message          string    `json:"message"`
	Error            string    `json:"error,omitempty"`
	StartedAt        time.Time `json:"started_at"`
	UpdatedAt        time.Time `json:"updated_at"`
	PreviousArtifact string    `json:"previous_artifact,omitempty"`
	TargetArtifact   string    `json:"target_artifact,omitempty"`
	BinaryMode       uint32    `json:"binary_mode,omitempty"`
}

func (j Job) active() bool {
	switch j.Phase {
	case "queued", "preparing", "applying", "validating", "rolling_back":
		return true
	}
	return false
}

type Status struct {
	Available bool   `json:"available"`
	Mode      string `json:"mode"`
	Reason    string `json:"reason,omitempty"`
	Busy      bool   `json:"busy"`
	Job       *Job   `json:"job,omitempty"`
}
type StartRequest struct {
	Current string `json:"current_version"`
	Version string `json:"version"`
	Action  string `json:"action"`
}
type deployment interface {
	Prepare(context.Context, *Job, Release) error
	Apply(context.Context, *Job) error
	Rollback(context.Context, *Job) error
	Healthy(context.Context, *Job, bool) error
}
type Agent struct {
	cfg          Config
	mu           sync.Mutex
	job          Job
	source       *releaseSource
	deployment   deployment
	pollInterval time.Duration
}

func (a *Agent) statePath() string { return filepath.Join(a.cfg.StateDir, "status.json") }
func (a *Agent) saveLocked(job Job) error {
	job.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	if err := atomicWrite(a.statePath(), data, 0600); err != nil {
		return err
	}
	a.job = job
	return nil
}
func (a *Agent) save(job Job) error { a.mu.Lock(); defer a.mu.Unlock(); return a.saveLocked(job) }
func (a *Agent) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := Status{Available: true, Mode: a.cfg.Mode, Busy: a.job.active()}
	if a.job.ID != "" {
		job := a.job
		job.DeploymentID = ""
		job.PreviousArtifact = ""
		job.TargetArtifact = ""
		job.BinaryMode = 0
		s.Job = &job
	}
	return s
}

func (a *Agent) start(ctx context.Context, request StartRequest) (Status, error) {
	items, err := a.source.list(ctx, false)
	if err != nil {
		return Status{}, err
	}
	release, err := selectRelease(request.Current, request.Version, request.Action, items)
	if err != nil {
		return Status{}, err
	}
	a.mu.Lock()
	if a.job.active() {
		a.mu.Unlock()
		return Status{}, errors.New("已有升级或回退任务正在执行")
	}
	if a.job.Phase == "rollback_failed" {
		a.mu.Unlock()
		return Status{}, errors.New("上次恢复未通过健康检查，请先检查部署并重启更新进程")
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		a.mu.Unlock()
		return Status{}, err
	}
	job := Job{ID: hex.EncodeToString(random[:]), DeploymentID: a.cfg.identity(), Action: request.Action, Phase: "queued", Target: release.Tag, Previous: request.Current, Message: "等待准备更新", StartedAt: time.Now().UTC()}
	err = a.saveLocked(job)
	a.mu.Unlock()
	if err != nil {
		return Status{}, err
	}
	go a.run(job, release)
	return a.Status(), nil
}

func (a *Agent) run(job Job, release Release) {
	defer func() {
		if value := recover(); value != nil {
			a.fail(job, fmt.Errorf("更新进程异常: %v", value))
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	job.Phase, job.Message = "preparing", "下载并校验目标版本，保留当前版本用于恢复"
	if err := a.save(job); err != nil {
		a.fail(job, err)
		return
	}
	if err := a.deployment.Prepare(ctx, &job, release); err != nil {
		a.fail(job, err)
		return
	}
	job.Phase, job.Message = "applying", "切换版本并重启应用"
	// Persist rollback information BEFORE the first mutation of the app.
	if err := a.save(job); err != nil {
		job.Phase = "preparing"
		a.fail(job, err)
		return
	}
	if err := a.deployment.Apply(ctx, &job); err != nil {
		if errors.Is(err, errNotApplied) {
			job.Phase = "preparing"
		}
		a.fail(job, err)
		return
	}
	job.Phase, job.Message = "validating", "等待应用与数据库健康检查"
	if err := a.save(job); err != nil {
		a.fail(job, err)
		return
	}
	if err := a.waitHealthy(&job, false); err != nil {
		a.fail(job, err)
		return
	}
	job.Phase, job.Message = "succeeded", "目标版本已连续通过健康检查"
	if err := a.save(job); err != nil {
		job.Phase = "validating"
		a.fail(job, err)
		return
	}
	if local, ok := a.deployment.(*localDeployment); ok {
		local.cleanup(job)
	}
}

func (a *Agent) fail(job Job, cause error) {
	job.Error = cause.Error()
	if job.Phase == "queued" || job.Phase == "preparing" {
		job.Phase, job.Message = "failed", "更新准备失败，当前版本未切换"
	} else {
		job.Phase, job.Message = "rolling_back", "更新未通过检查，正在自动恢复原版本"
		if err := a.save(job); err != nil {
			log.Printf("persist rollback state: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		err := a.deployment.Rollback(ctx, &job)
		cancel()
		if err == nil {
			err = a.waitHealthy(&job, true)
		}
		if err != nil {
			job.Phase, job.Message = "rollback_failed", "自动恢复未通过健康检查，需要检查部署"
			job.Error += "; 恢复失败: " + err.Error()
		} else {
			job.Phase, job.Message = "rolled_back", "已自动恢复升级前的版本，并通过健康检查"
		}
	}
	if err := a.save(job); err != nil {
		log.Printf("persist update outcome: %v", err)
	}
	if local, ok := a.deployment.(*localDeployment); ok {
		local.cleanup(job)
	}
}

func (a *Agent) waitHealthy(job *Job, rollback bool) error {
	timeout := a.cfg.HealthTimeout
	if timeout <= 0 {
		timeout = 180 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	interval := a.pollInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	consecutive := 0
	var last error
	for {
		probeCtx, stop := context.WithTimeout(ctx, 6*time.Second)
		last = a.deployment.Healthy(probeCtx, job, rollback)
		stop()
		if last == nil {
			consecutive++
			if consecutive >= 3 {
				return nil
			}
		} else {
			consecutive = 0
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("健康检查超时: %v", last)
		case <-ticker.C:
		}
	}
}

func (a *Agent) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) { respond(w, http.StatusOK, a.Status()) })
	mux.HandleFunc("GET /releases", func(w http.ResponseWriter, r *http.Request) {
		items, err := a.source.list(r.Context(), r.URL.Query().Get("force") == "1")
		if err != nil {
			respondError(w, err)
			return
		}
		catalog, err := catalogFor(r.URL.Query().Get("current"), items)
		if err != nil {
			respondError(w, err)
			return
		}
		respond(w, http.StatusOK, catalog)
	})
	mux.HandleFunc("POST /start", func(w http.ResponseWriter, r *http.Request) {
		var request StartRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			respondError(w, err)
			return
		}
		status, err := a.start(r.Context(), request)
		if err != nil {
			respondError(w, err)
			return
		}
		respond(w, http.StatusAccepted, status)
	})
	return mux
}

func respond(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}
func respondError(w http.ResponseWriter, err error) {
	respond(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
}

func RunAgent() error {
	cfg, err := ConfigFromEnv()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.StateDir, 0700); err != nil {
		return err
	}
	lock, err := lockAgent(filepath.Join(cfg.StateDir, "agent.lock"))
	if err != nil {
		return err
	}
	defer lock.Close()
	a := &Agent{cfg: cfg, source: &releaseSource{}}
	a.deployment = &localDeployment{cfg: cfg, commands: execCommands{}, downloads: &http.Client{Timeout: 10 * time.Minute}}
	if err := readJSON(a.statePath(), &a.job); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot read update recovery state: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(cfg.Socket), 0700); err != nil {
		return err
	}
	if info, err := os.Lstat(cfg.Socket); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return errors.New("update socket path is not a socket")
		}
		if err := os.Remove(cfg.Socket); err != nil {
			return err
		}
	}
	listener, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	socketMode := os.FileMode(0600)
	if cfg.SocketGroup != "" {
		group, err := user.LookupGroup(cfg.SocketGroup)
		if err != nil {
			return fmt.Errorf("lookup update socket group: %w", err)
		}
		gid, err := strconv.Atoi(group.Gid)
		if err != nil {
			return err
		}
		if err := os.Chown(cfg.Socket, -1, gid); err != nil {
			return err
		}
		socketMode = 0660
	}
	if err := os.Chmod(cfg.Socket, socketMode); err != nil {
		return err
	}
	server := &http.Server{Handler: a.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second, WriteTimeout: 45 * time.Second}
	if a.job.active() || a.job.Phase == "rollback_failed" {
		if a.job.DeploymentID != cfg.identity() || !jobIDPattern.MatchString(a.job.ID) {
			return errors.New("更新恢复记录与当前部署配置不一致，拒绝操作其他部署")
		}
		job := a.job
		if job.Phase == "rollback_failed" {
			job.Phase = "rolling_back"
			if err := a.save(job); err != nil {
				return err
			}
		}
		go a.fail(job, errors.New("上次更新中断，执行恢复检查"))
	}
	stop, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() { <-stop.Done(); _ = server.Close() }()
	log.Printf("update agent listening on %s (%s)", cfg.Socket, cfg.Mode)
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
