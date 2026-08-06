// Command migrate copies an existing SQLite data file into PostgreSQL. It is
// intentionally separate from the HTTP server so an upgrade can be run while
// the application is stopped and verified before switching DATABASE_DRIVER.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bejix/upstream-ops/backend/storage"
)

func main() {
	sourcePath := flag.String("source", "./data/upstream-ops.db", "source SQLite database path")
	targetHost := flag.String("target-host", "localhost", "PostgreSQL host")
	targetPort := flag.Int("target-port", 5432, "PostgreSQL port")
	targetUser := flag.String("target-user", "upstreamops", "PostgreSQL user")
	targetPassword := flag.String("target-password", "", "PostgreSQL password (or DATABASE_PASSWORD)")
	targetName := flag.String("target-name", "upstreamops", "PostgreSQL database name")
	targetSSLMode := flag.String("target-ssl-mode", "disable", "PostgreSQL sslmode")
	targetConnectTimeout := flag.Int("target-connect-timeout", 15, "PostgreSQL connection timeout in seconds")
	batchSize := flag.Int("batch-size", 500, "rows inserted per batch")
	allowExisting := flag.Bool("allow-existing", false, "allow appending to a non-empty target (not recommended)")
	skipMissing := flag.Bool("skip-missing", true, "skip tables absent from older SQLite versions")
	flag.Parse()

	if *targetPassword == "" {
		*targetPassword = os.Getenv("DATABASE_PASSWORD")
	}
	if _, err := os.Stat(*sourcePath); err != nil {
		fatal("source database %q: %v", *sourcePath, err)
	}
	source, err := storage.Open(storage.DBConfig{
		Driver:   storage.DBDriverSQLite,
		Path:     filepath.Clean(*sourcePath),
		ReadOnly: true,
	})
	if err != nil {
		fatal("open source database: %v", err)
	}
	sourceSQL, err := source.DB()
	if err != nil {
		fatal("open source sql database: %v", err)
	}
	defer sourceSQL.Close()
	target, err := storage.Open(storage.DBConfig{
		Driver:                storage.DBDriverPostgres,
		Host:                  *targetHost,
		Port:                  *targetPort,
		User:                  *targetUser,
		Password:              *targetPassword,
		Name:                  *targetName,
		SSLMode:               *targetSSLMode,
		ConnectTimeoutSeconds: *targetConnectTimeout,
	})
	if err != nil {
		fatal("open target database: %v", err)
	}
	targetSQL, err := target.DB()
	if err != nil {
		fatal("open target sql database: %v", err)
	}
	defer targetSQL.Close()
	report, err := storage.MigrateDatabase(source, target, storage.MigrationOptions{
		BatchSize: *batchSize, AllowExisting: *allowExisting, SkipMissingTbl: *skipMissing,
	})
	if err != nil {
		fatal("migration failed: %v", err)
	}
	body, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(body))
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
