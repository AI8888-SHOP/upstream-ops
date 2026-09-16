package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testArchive(t *testing.T, name string, kind byte) []byte {
	t.Helper()
	var archive bytes.Buffer
	zipped := gzip.NewWriter(&archive)
	tarred := tar.NewWriter(zipped)
	contents := []byte("checked application binary")
	header := &tar.Header{Name: name, Mode: 0755, Typeflag: kind}
	if kind == tar.TypeReg {
		header.Size = int64(len(contents))
	} else {
		header.Linkname = "../../outside"
	}
	if err := tarred.WriteHeader(header); err != nil {
		t.Fatal(err)
	}
	if kind == tar.TypeReg {
		if _, err := tarred.Write(contents); err != nil {
			t.Fatal(err)
		}
	}
	if err := tarred.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zipped.Close(); err != nil {
		t.Fatal(err)
	}
	return archive.Bytes()
}

func TestDownloadRequiresChecksumAndNeverExtractsArchivePaths(t *testing.T) {
	for _, tc := range []struct {
		name, member     string
		kind             byte
		corrupt, success bool
	}{
		{"valid", "./upstream-ops", tar.TypeReg, false, true},
		{"wrong digest", "./upstream-ops", tar.TypeReg, true, false},
		{"path traversal", "../upstream-ops", tar.TypeReg, false, false},
		{"symlink", "./upstream-ops", tar.TypeSymlink, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			archive := testArchive(t, tc.member, tc.kind)
			digest := fmt.Sprintf("sha256:%x", sha256.Sum256(archive))
			if tc.corrupt {
				digest = "sha256:" + strings.Repeat("0", 64)
			}
			name := fmt.Sprintf("upstream-ops-linux-%s.tar.gz", runtime.GOARCH)
			release := Release{Tag: "v1.0.0", Assets: []Asset{{Name: name, URL: "https://github.com/" + Repository + "/releases/download/v1.0.0/" + name, Digest: digest}}}
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(archive)), Header: make(http.Header)}, nil
			})}
			d := &localDeployment{downloads: client}
			root := t.TempDir()
			stage := filepath.Join(root, "stage")
			if err := os.Mkdir(stage, 0700); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(stage, "target")
			err := d.downloadBinary(context.Background(), release, target)
			if (err == nil) != tc.success {
				t.Fatalf("download err=%v, success=%v", err, tc.success)
			}
			if tc.success {
				contents, err := os.ReadFile(target)
				if err != nil || string(contents) != "checked application binary" {
					t.Fatal("wrong extracted content")
				}
			}
			if _, err := os.Stat(filepath.Join(root, "upstream-ops")); !os.IsNotExist(err) {
				t.Fatal("archive escaped staging directory")
			}
		})
	}
}
