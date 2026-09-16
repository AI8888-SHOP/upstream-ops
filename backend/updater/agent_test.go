package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeDeployment struct {
	mu                                           sync.Mutex
	prepareErr, applyErr, healthErr, rollbackErr error
	prepared, applied, rolledBack, healthChecks  int
	beforeApply                                  func(*Job)
	block                                        chan struct{}
}

func (d *fakeDeployment) Prepare(ctx context.Context, job *Job, _ Release) error {
	if d.block != nil {
		select {
		case <-d.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prepared++
	job.PreviousArtifact, job.TargetArtifact = "old", "new"
	return d.prepareErr
}
func (d *fakeDeployment) Apply(_ context.Context, job *Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.applied++
	if d.beforeApply != nil {
		d.beforeApply(job)
	}
	return d.applyErr
}
func (d *fakeDeployment) Rollback(context.Context, *Job) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.rolledBack++
	return d.rollbackErr
}
func (d *fakeDeployment) Healthy(_ context.Context, _ *Job, rollback bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.healthChecks++
	if !rollback {
		return d.healthErr
	}
	return d.rollbackErr
}

func testAgent(t *testing.T, deployment *fakeDeployment) *Agent {
	t.Helper()
	return &Agent{cfg: Config{StateDir: t.TempDir(), Mode: "systemd", HealthTimeout: 250 * time.Millisecond}, deployment: deployment, pollInterval: time.Millisecond,
		source: &releaseSource{at: time.Now(), items: []Release{{Tag: "v1.2.0"}, {Tag: "v1.1.0"}, {Tag: "v1.0.0"}}}}
}

func TestUpdateTransactionOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name                             string
		prepare, apply, health, rollback bool
		want                             string
		restores                         int
	}{
		{"success", false, false, false, false, "succeeded", 0},
		{"download fails", true, false, false, false, "failed", 0},
		{"restart fails", false, true, false, false, "rolled_back", 1},
		{"unhealthy new version", false, false, true, false, "rolled_back", 1},
		{"recovery also fails", false, true, false, true, "rollback_failed", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.New("simulated failure")
			d := &fakeDeployment{}
			if tc.prepare {
				d.prepareErr = failure
			}
			if tc.apply {
				d.applyErr = failure
			}
			if tc.health {
				d.healthErr = failure
			}
			if tc.rollback {
				d.rollbackErr = failure
			}
			a := testAgent(t, d)
			d.beforeApply = func(job *Job) {
				var persisted Job
				if err := readJSON(a.statePath(), &persisted); err != nil {
					t.Error(err)
				}
				if persisted.Phase != "applying" || persisted.PreviousArtifact != "old" {
					t.Error("rollback point was not durable before mutation")
				}
			}
			a.run(Job{ID: "abc", Target: "v1.2.0", Previous: "v1.1.0", Phase: "queued"}, Release{Tag: "v1.2.0"})
			status := a.Status()
			if status.Busy || status.Job == nil || status.Job.Phase != tc.want || d.rolledBack != tc.restores {
				t.Fatalf("status=%+v restores=%d", status.Job, d.rolledBack)
			}
			if tc.prepare && d.applied != 0 {
				t.Fatal("download failure restarted the app")
			}
			if status.Job.PreviousArtifact != "" || status.Job.TargetArtifact != "" {
				t.Fatal("public status exposed internal recovery fields")
			}
		})
	}
}

func TestOnlyOneUpdateCanRunAndRecoverySurvivesAgentRestart(t *testing.T) {
	d := &fakeDeployment{block: make(chan struct{})}
	a := testAgent(t, d)
	request := StartRequest{Current: "v1.1.0", Version: "v1.2.0", Action: "upgrade"}
	if _, err := a.start(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := a.start(context.Background(), request); err == nil {
		t.Fatal("concurrent updates were accepted")
	}
	close(d.block)
	deadline := time.After(time.Second)
	for a.Status().Busy {
		select {
		case <-deadline:
			t.Fatal("update did not finish")
		case <-time.After(time.Millisecond):
		}
	}
	interrupted := Job{ID: "interrupted", Phase: "validating", PreviousArtifact: "old", TargetArtifact: "new"}
	if err := a.save(interrupted); err != nil {
		t.Fatal(err)
	}
	restarted := testAgent(t, &fakeDeployment{})
	restarted.cfg.StateDir = a.cfg.StateDir
	var persisted Job
	if err := readJSON(restarted.statePath(), &persisted); err != nil {
		t.Fatal(err)
	}
	restarted.fail(persisted, errors.New("process interrupted"))
	if got := restarted.Status(); got.Job.Phase != "rolled_back" {
		t.Fatalf("recovery=%+v", got.Job)
	}
}

func TestCannotStartWhenDurableStateCannotBeWritten(t *testing.T) {
	d := &fakeDeployment{}
	a := testAgent(t, d)
	// A regular file in place of the state directory works even when tests
	// run as root (chmod-based permission tests do not).
	path := filepath.Join(a.cfg.StateDir, "not-a-directory")
	if err := os.WriteFile(path, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	a.cfg.StateDir = path
	if _, err := a.start(context.Background(), StartRequest{Current: "v1.1.0", Version: "v1.2.0", Action: "upgrade"}); err == nil {
		t.Fatal("accepted a non-durable update")
	}
	if d.prepared != 0 || d.applied != 0 {
		t.Fatal("update started without a durable job")
	}
}

func TestExternalDeploymentChangeDoesNotTriggerRollback(t *testing.T) {
	d := &fakeDeployment{applyErr: fmt.Errorf("%w: %w", errNotApplied, errDeploymentChanged)}
	a := testAgent(t, d)
	a.run(Job{ID: "external-change", Phase: "queued"}, Release{Tag: "v1.2.0"})
	if got := a.Status(); got.Job.Phase != "failed" || d.rolledBack != 0 {
		t.Fatalf("external change was overwritten: job=%+v restores=%d", got.Job, d.rolledBack)
	}
}
