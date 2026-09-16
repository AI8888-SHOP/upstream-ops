//go:build !linux

package updater

import (
	"errors"
	"os"
)

func lockAgent(string) (*os.File, error) { return nil, errors.New("更新进程仅支持 Linux") }
