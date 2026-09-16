package updater

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type fakeCommands struct {
	calls []string
	run   func(string, []string) ([]byte, error)
}

func (c *fakeCommands) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	c.calls = append(c.calls, name+" "+strings.Join(args, " "))
	if c.run != nil {
		return c.run(name, args)
	}
	return nil, nil
}

func TestDockerSwitchPersistsImageAndPreservesDatabaseOverrides(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"docker-compose.yml", "database.yml"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("services: {}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	env := "DATABASE_PASSWORD='untouched-secret'\nCOMPOSE_FILE='docker-compose.yml:database.yml'\n"
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte(env), 0600); err != nil {
		t.Fatal(err)
	}
	commands := &fakeCommands{}
	d := &localDeployment{cfg: Config{Mode: "docker", ProjectDir: root, Service: "app"}, commands: commands}
	digest := "sha256:" + strings.Repeat("a", 64)
	if err := d.switchDocker(context.Background(), digest); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(root, imageOverride))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(contents), "image: "+digest) {
		t.Fatal("image was not pinned")
	}
	saved, err := os.ReadFile(filepath.Join(root, ".env"))
	if err != nil {
		t.Fatal(err)
	}
	if dotenvValue(saved, "DATABASE_PASSWORD") != "untouched-secret" || !strings.Contains(dotenvValue(saved, "COMPOSE_FILE"), "database.yml") || !strings.HasSuffix(dotenvValue(saved, "COMPOSE_FILE"), imageOverride) {
		t.Fatalf("configuration was not preserved: %q", dotenvValue(saved, "COMPOSE_FILE"))
	}
	if len(commands.calls) != 1 || !strings.HasSuffix(commands.calls[0], "up -d --no-deps --pull never --force-recreate app") {
		t.Fatalf("unexpected commands: %v", commands.calls)
	}
	if err := d.switchDocker(context.Background(), "latest"); err == nil {
		t.Fatal("mutable image reference accepted")
	}
}

func TestDockerPullFailureLeavesRunningServiceUntouched(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "docker-compose.yml"), []byte("services: {}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	old := "sha256:" + strings.Repeat("a", 64)
	commands := &fakeCommands{run: func(_ string, args []string) ([]byte, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "ps --all -q"):
			return []byte("container-id\n"), nil
		case strings.HasPrefix(joined, "inspect --format {{.Image}}"):
			return []byte(old), nil
		case strings.HasPrefix(joined, "exec "):
			return []byte(`{"status":"ok","version":"1.0.0"}`), nil
		case strings.HasPrefix(joined, "image pull "):
			return nil, errors.New("pull failed")
		}
		return nil, nil
	}}
	d := &localDeployment{cfg: Config{Mode: "docker", ProjectDir: root, StateDir: filepath.Join(root, "state"), Service: "app"}, commands: commands}
	job := &Job{ID: "test", Previous: "v1.0.0", Target: "v1.1.0"}
	if err := d.Prepare(context.Background(), job, Release{Tag: "v1.1.0"}); err == nil {
		t.Fatal("expected pull failure")
	}
	for _, call := range commands.calls {
		if strings.Contains(call, "force-recreate") || strings.Contains(call, " stop ") {
			t.Fatal("pull failure stopped the old service")
		}
	}
	if _, err := os.Stat(filepath.Join(root, imageOverride)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("pull failure changed the persistent image")
	}
}

func TestDockerFailedRecreateRestoresOriginalImageAndKeepsOverrides(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"docker-compose.yml", "postgres.yml"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("services: {}\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, ".env"), []byte("COMPOSE_FILE=docker-compose.yml:postgres.yml\n"), 0600); err != nil {
		t.Fatal(err)
	}
	old, target := "sha256:"+strings.Repeat("a", 64), "sha256:"+strings.Repeat("b", 64)
	recreates := 0
	commands := &fakeCommands{run: func(_ string, args []string) ([]byte, error) {
		joined := strings.Join(args, " ")
		switch {
		case strings.Contains(joined, "ps --all -q"):
			return []byte("original-container\n"), nil
		case strings.HasPrefix(joined, "inspect --format {{.Image}}"):
			return []byte(old), nil
		case strings.Contains(joined, "--force-recreate app"):
			recreates++
			if recreates == 1 {
				return nil, errors.New("target container failed to start")
			}
		}
		return nil, nil
	}}
	d := &localDeployment{cfg: Config{Mode: "docker", ProjectDir: root, Service: "app"}, commands: commands}
	job := &Job{PreviousArtifact: old, TargetArtifact: target}
	if err := d.Apply(context.Background(), job); err == nil || errors.Is(err, errNotApplied) {
		t.Fatalf("expected failure after switching, got %v", err)
	}
	if err := d.Rollback(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	override, _ := os.ReadFile(filepath.Join(root, imageOverride))
	env, _ := os.ReadFile(filepath.Join(root, ".env"))
	if recreates != 2 || !strings.Contains(string(override), old) || !strings.Contains(dotenvValue(env, "COMPOSE_FILE"), "postgres.yml") {
		t.Fatalf("restore failed: recreates=%d override=%s", recreates, override)
	}
}
