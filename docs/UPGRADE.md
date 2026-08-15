# UpstreamOps Upgrade Guide

This guide is for an existing Docker installation. It keeps the existing
`data/` directory and creates a recoverable backup before changing the image.

## 1. Get the upgrade kit

Download the `upstream-ops-upgrade-kit-<version>.zip` (Windows) or
`upstream-ops-upgrade-kit-<version>.tar.gz` (Linux/macOS) asset from the
GitHub Release for the version you want to install. Extract it to a temporary
directory. Do not overwrite the live deployment directory yet.

The kit contains the upgrade helpers, the current Compose templates, and the
database migration helper. It does not contain `.env`, `data/`, API keys, or
any other runtime secret.

## 2. Normal image upgrade (keep SQLite/MySQL/PostgreSQL)

Run the helper with paths to the live `.env`, Compose file, and `data/`
directory. The kit itself intentionally has no runtime `.env` or `data/`.

Linux/macOS:

```bash
COMPOSE_FILE=/srv/upstream-ops/docker-compose.yml \
ENV_FILE=/srv/upstream-ops/.env \
DATA_DIR=/srv/upstream-ops/data \
TARGET_TAG=v0.0.28 \
/tmp/upstream-ops-upgrade-kit-v0.0.28/scripts/upgrade.sh
```

Windows PowerShell:

```powershell
.\scripts\upgrade.ps1 `
  -ComposeFile 'D:\upstream-ops\docker-compose.yml' `
  -EnvFile 'D:\upstream-ops\.env' `
  -DataDir 'D:\upstream-ops\data' `
  -TargetTag 'v0.0.28'
```

The helper performs these checks and actions:

1. It verifies Docker Compose, the service, and the data directory.
2. It stops the old container before copying the SQLite file.
3. It stores `data/`, `.env`, the old image reference, and an immutable local
   rollback tag under `backups/upstream-ops-<UTC timestamp>/`.
4. It generates a temporary Compose image override, pulls and starts the
   requested image (even when an old Compose file hard-codes `image:`), and
   waits for `/healthz`.
5. It writes the new `IMAGE_TAG` to `.env` only after the health check passes.
6. A pull, startup, health, or persistence failure automatically restores the
   old image. Keep the backup and rollback image until request and billing
   checks have passed.

The helper never deletes the old image or backup. Remove them only after a
manual verification window.

For a MySQL deployment, include the second Compose file in the same stack:

```bash
COMPOSE_EXTRA_FILES=docker-compose.mysql.yml \
COMPOSE_FILE=docker-compose.yml \
ENV_FILE=/srv/upstream-ops/.env DATA_DIR=/srv/upstream-ops/data \
TARGET_TAG=v0.0.28 /tmp/upstream-ops-upgrade-kit-v0.0.28/scripts/upgrade.sh
```

PowerShell uses `-ComposeExtraFile 'docker-compose.mysql.yml'`.

## 3. SQLite to PostgreSQL migration

Create a new, empty PostgreSQL database and a dedicated user first. Put the
connection values in the live `.env` (do not put the password in the command
line), then run the PostgreSQL helper from the kit with explicit deployment
paths:

```bash
chmod +x /tmp/upstream-ops-upgrade-kit-v0.0.28/scripts/upgrade-to-postgres.sh
ENV_FILE=/srv/upstream-ops/.env \
DATA_DIR=/srv/upstream-ops/data \
COMPOSE_FILE=/srv/upstream-ops/docker-compose.yml \
POSTGRES_COMPOSE_FILE=/tmp/upstream-ops-upgrade-kit-v0.0.28/docker-compose.postgres.yml \
TARGET_TAG=v0.0.28 \
MIGRATION_IMAGE_TAG=v0.0.28 \
/tmp/upstream-ops-upgrade-kit-v0.0.28/scripts/upgrade-to-postgres.sh
```

PowerShell:

```powershell
.\scripts\upgrade-to-postgres.ps1 `
  -ComposeFile 'D:\upstream-ops\docker-compose.yml' `
  -PostgresComposeFile 'C:\Temp\upstream-ops-upgrade-kit-v0.0.28\docker-compose.postgres.yml' `
  -EnvFile 'D:\upstream-ops\.env' `
  -DataDir 'D:\upstream-ops\data' `
  -TargetTag 'v0.0.28' `
  -MigrationImageTag 'v0.0.28'
```

The password is passed to the migration container through
`DATABASE_PASSWORD`; it is not put in Docker command arguments. The target
database must be empty. The helper refuses to run when `.env` already says
`DATABASE_DRIVER=postgres`.

The migration runs in a target transaction, copies only application tables,
converts SQLite boolean values, preserves compatible columns from older
releases, and verifies row counts. A failed migration leaves SQLite as the
active database. After a healthy PostgreSQL start, the helper persists
`DATABASE_DRIVER=postgres` and the connection settings in `.env`.
Before invoking the container it creates a writable temporary SQLite snapshot,
including any `-wal`, `-shm`, or journal sidecars; the snapshot is removed after
success or rollback. Set `MIGRATION_TIMEOUT_SECONDS` (PowerShell:
`-MigrationTimeoutSeconds`) when the migration needs more than the 30-minute
default.

For PostgreSQL in an existing Docker network, set:

```env
DATABASE_NETWORK_NAME=sub2api-localtest_default
DATABASE_NETWORK_EXTERNAL=true
```

The helper checks that the network exists, joins both the app and migration
container to it, and persists `DATABASE_NETWORK_NAME` plus
`DATABASE_NETWORK_EXTERNAL=true` after success. A custom SQLite location is
read from `.env DATABASE_PATH`; use `SOURCE_DB=/app/data/other.db` (or
`-SourceDb '/app/data/other.db'`) when the container path is different.

## 4. Rollback

If the helper reports a failed health check, it restores the previous image
automatically. To restore manually, stop the app and run the old image from
the backup's `rollback-image.txt` using the original `.env.before` file. Do
not delete the SQLite backup until the new database has been audited.

## 5. Non-Docker installations

Release archives also contain `upstream-ops-migrate` for each supported
platform. Stop the old process, copy the SQLite file, run the migration binary
against the empty PostgreSQL database, update the service environment to
`DATABASE_DRIVER=postgres`, and start the new server. Keep the same `APP_SECRET`
so encrypted credentials remain readable.

When `TARGET_TAG` is provided, it is the tag used by both the migration binary
and the new application container. If it is omitted, `MIGRATION_IMAGE_TAG`
(or `latest`) supplies both values. The PostgreSQL helper creates
`docker-compose.upstream-ops-postgres.yml` beside
the live Compose file and records it in `.env COMPOSE_FILE`. This prevents an
older Compose file with a hard-coded SQLite driver from silently switching back
on a later plain `docker compose up`. Existing files are backed up before they
are replaced. For a MySQL installation, pass every Compose file with
`-ComposeExtraFile` (PowerShell) or `COMPOSE_EXTRA_FILES=file1:file2` (Bash).
