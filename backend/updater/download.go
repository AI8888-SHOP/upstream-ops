package updater

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const maxArchiveBytes int64 = 256 << 20
const maxBinaryBytes int64 = 192 << 20

func (d *localDeployment) fetchFile(ctx context.Context, asset Asset, path string, maximum int64) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, asset.URL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "upstream-ops-updater")
	response, err := d.downloads.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("下载 %s 失败: HTTP %d", asset.Name, response.StatusCode)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	n, err := io.Copy(io.MultiWriter(file, hash), io.LimitReader(response.Body, maximum+1))
	if err == nil && n > maximum {
		err = errors.New("下载文件超过大小上限")
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return "", err
	}
	if closeErr != nil {
		return "", closeErr
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func (d *localDeployment) downloadBinary(ctx context.Context, release Release, target string) error {
	name := fmt.Sprintf("upstream-ops-linux-%s.tar.gz", runtime.GOARCH)
	asset, err := assetFor(release, name)
	if err != nil {
		return err
	}
	want := strings.TrimPrefix(asset.Digest, "sha256:")
	if !strings.HasPrefix(asset.Digest, "sha256:") || len(want) != 64 {
		checksums, err := assetFor(release, "SHA256SUMS")
		if err != nil {
			return errors.New("该发版没有可验证的 SHA256 摘要，无法自动安装")
		}
		checksumPath := filepath.Join(filepath.Dir(target), "SHA256SUMS")
		if _, err := d.fetchFile(ctx, checksums, checksumPath, 1<<20); err != nil {
			return err
		}
		file, err := os.Open(checksumPath)
		if err != nil {
			return err
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) == 2 && strings.TrimPrefix(fields[1], "*") == name {
				want = fields[0]
				break
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
	}
	if bytes, err := hex.DecodeString(want); err != nil || len(bytes) != 32 {
		return errors.New("发行版校验摘要不合法")
	}
	archive := filepath.Join(filepath.Dir(target), name)
	actual, err := d.fetchFile(ctx, asset, archive, maxArchiveBytes)
	if err != nil {
		return err
	}
	if !strings.EqualFold(actual, want) {
		return errors.New("发行版 SHA256 校验失败，已取消更新")
	}
	defer os.Remove(archive)
	file, err := os.Open(archive)
	if err != nil {
		return err
	}
	defer file.Close()
	zipped, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer zipped.Close()
	reader := tar.NewReader(io.LimitReader(zipped, 512<<20))
	found := false
	for {
		header, err := reader.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		// Never extract archive-supplied paths, symlinks or permissions.
		if header.Name != "upstream-ops" && header.Name != "./upstream-ops" {
			continue
		}
		if found || header.Typeflag != tar.TypeReg || header.Size <= 0 || header.Size > maxBinaryBytes {
			return errors.New("发行版程序文件格式不合法")
		}
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
		if err != nil {
			return err
		}
		_, err = io.CopyN(output, reader, header.Size)
		if err == nil {
			err = output.Sync()
		}
		closeErr := output.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		found = true
	}
	if !found {
		return errors.New("发行版压缩包中没有程序文件")
	}
	return nil
}
