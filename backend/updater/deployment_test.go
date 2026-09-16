package updater

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func stagedNativeDeployment(t *testing.T) (*localDeployment, *Job, *fakeCommands) {
	t.Helper()
	root := t.TempDir()
	commands := &fakeCommands{}
	d := &localDeployment{cfg: Config{Mode: "systemd", StateDir: root, Binary: filepath.Join(root, "app"), Unit: "app.service"}, commands: commands}
	job := &Job{ID: "0123456789abcdef01234567", BinaryMode: 0755}
	if err := os.MkdirAll(d.jobDir(job), 0700); err != nil {
		t.Fatal(err)
	}
	for path, content := range map[string]string{
		d.cfg.Binary: "old executable", filepath.Join(d.jobDir(job), "previous"): "old executable", filepath.Join(d.jobDir(job), "target"): "new executable",
	} {
		if err := os.WriteFile(path, []byte(content), 0755); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	job.PreviousArtifact, err = fileSHA(d.cfg.Binary)
	if err != nil {
		t.Fatal(err)
	}
	job.TargetArtifact, err = fileSHA(filepath.Join(d.jobDir(job), "target"))
	if err != nil {
		t.Fatal(err)
	}
	return d, job, commands
}

func TestNativeFailedRestartRestoresExactPreviousExecutable(t *testing.T) {
	d, job, commands := stagedNativeDeployment(t)
	attempts := 0
	commands.run = func(name string, args []string) ([]byte, error) {
		if name == "systemctl" && strings.Join(args, " ") == "reset-failed app.service" {
			return nil, nil
		}
		if name != "systemctl" || strings.Join(args, " ") != "restart app.service" {
			t.Fatalf("unexpected command %s %v", name, args)
		}
		attempts++
		if attempts == 1 {
			return nil, errors.New("new app cannot start")
		}
		return nil, nil
	}
	if err := d.Apply(context.Background(), job); err == nil {
		t.Fatal("expected restart failure")
	}
	if err := d.Rollback(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(d.cfg.Binary)
	if err != nil || string(contents) != "old executable" || attempts != 2 {
		t.Fatalf("restore content=%q attempts=%d err=%v", contents, attempts, err)
	}
	if len(commands.calls) != 3 || commands.calls[1] != "systemctl reset-failed app.service" {
		t.Fatal("systemd restart limit was not cleared before recovery")
	}
}

func TestNativeRejectsDamagedBackupAndStaleOrDamagedTarget(t *testing.T) {
	for _, name := range []string{"backup", "external replacement", "target"} {
		t.Run(name, func(t *testing.T) {
			d, job, commands := stagedNativeDeployment(t)
			path := d.cfg.Binary
			if name == "backup" {
				path = filepath.Join(d.jobDir(job), "previous")
			} else if name == "target" {
				path = filepath.Join(d.jobDir(job), "target")
			}
			if err := os.WriteFile(path, []byte("changed"), 0755); err != nil {
				t.Fatal(err)
			}
			before, _ := os.ReadFile(d.cfg.Binary)
			var err error
			if name == "backup" {
				err = d.Rollback(context.Background(), job)
			} else {
				err = d.Apply(context.Background(), job)
				if !errors.Is(err, errNotApplied) {
					t.Fatal("preflight failure must not trigger a stale rollback")
				}
			}
			after, _ := os.ReadFile(d.cfg.Binary)
			if err == nil || len(commands.calls) != 0 || string(before) != string(after) {
				t.Fatalf("unsafe change: err=%v calls=%v before=%q after=%q", err, commands.calls, before, after)
			}
		})
	}
}

func TestCommandWarningsDoNotPolluteStructuredOutput(t *testing.T) {
	// Spawn the test binary itself, without requiring a shell or Docker.
	if os.Args[len(os.Args)-1] == "--updater-output-helper" {
		fmt.Fprintln(os.Stdout, "container-id")
		fmt.Fprintln(os.Stderr, "compose warning")
		os.Exit(0)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output, err := (execCommands{}).Run(context.Background(), binary, "-test.run=^TestCommandWarningsDoNotPolluteStructuredOutput$", "--", "--updater-output-helper")
	if err != nil || strings.TrimSpace(string(output)) != "container-id" {
		t.Fatalf("output=%q err=%v", output, err)
	}
}
