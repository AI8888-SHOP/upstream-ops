//go:build linux

package updater

import (
	"errors"
	"os"
	"syscall"
)

// The privileged agent must not turn an app-owned 0700 executable or an
// operator-owned 0600 .env into a root-owned file during atomic replacement.
func preserveOwner(replacement *os.File, target string) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("更新目标必须为普通文件")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("无法读取原文件属主")
	}
	return replacement.Chown(int(stat.Uid), int(stat.Gid))
}
