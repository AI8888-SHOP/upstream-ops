//go:build !linux

package updater

import "os"

// The application builds on other platforms; running the update agent on
// them is rejected by Config.validate before it can modify any deployment.
func preserveOwner(*os.File, string) error { return nil }
