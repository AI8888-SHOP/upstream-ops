package updater

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".updater-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if err = preserveOwner(file, path); err == nil {
		err = file.Chmod(mode)
	}
	if err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func copyAtomic(source, target string, mode os.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.CreateTemp(filepath.Dir(target), ".updater-*")
	if err != nil {
		return err
	}
	defer os.Remove(output.Name())
	if err = preserveOwner(output, target); err == nil {
		err = output.Chmod(mode)
	}
	if err == nil {
		_, err = io.Copy(output, input)
	}
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
	if err := os.Rename(output.Name(), target); err != nil {
		return err
	}
	return syncDir(filepath.Dir(target))
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func readJSON(path string, dest any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return json.NewDecoder(io.LimitReader(file, 1<<20)).Decode(dest)
}
