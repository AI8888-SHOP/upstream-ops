//go:build linux

package updater

import (
	"fmt"
	"os"
	"syscall"
)

func lockAgent(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		return nil, fmt.Errorf("已有更新进程持有锁: %w", err)
	}
	return file, nil
}
