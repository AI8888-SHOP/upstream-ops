#!/usr/bin/env bash
set -Eeuo pipefail

# Recoverable SQLite -> PostgreSQL upgrade for Docker installations.
#
# The target image is pulled before the old service is stopped, and the old
# container image is kept by immutable ID for rollback.  The migration target
# must be empty.  No application secret is rewritten by this script.

ENV_FILE="${ENV_FILE:-.env}"
DATA_DIR="${DATA_DIR:-./data}"
SERVICE="${SERVICE:-app}"
COMPOSE_FILE="${COMPOSE_FILE:-}"
POSTGRES_COMPOSE_FILE="${POSTGRES_COMPOSE_FILE:-docker-compose.postgres.yml}"
IMAGE="${IMAGE:-}"
MIGRATION_IMAGE_TAG="${MIGRATION_IMAGE_TAG:-}"
TARGET_TAG="${TARGET_TAG:-}"
MIGRATION_NETWORK="${MIGRATION_NETWORK:-}"
SOURCE_DB="${SOURCE_DB:-}"
BACKUP_ROOT="${BACKUP_ROOT:-./backups}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-120}"
MIGRATION_TIMEOUT_SECONDS="${MIGRATION_TIMEOUT_SECONDS:-1800}"

die() {
  echo "upstream-ops upgrade: $*" >&2
  exit 2
}

command -v docker >/dev/null 2>&1 || die "docker is required"
[[ -f "$ENV_FILE" ]] || die "missing env file: $ENV_FILE"
[[ -d "$DATA_DIR" ]] || die "missing data directory: $DATA_DIR"
[[ -f "$POSTGRES_COMPOSE_FILE" ]] || die "missing PostgreSQL compose file: $POSTGRES_COMPOSE_FILE"
[[ "$SERVICE" =~ ^[A-Za-z0-9_.-]+$ ]] || die "SERVICE contains unsupported characters"

read_dotenv_value() {
  local key="$1" value
  value="$(sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*//p" "$ENV_FILE" | tail -n 1)"
  value="${value%$'\r'}"
  value="${value#\"}"; value="${value%\"}"
  value="${value#\'}"; value="${value%\'}"
  printf '%s' "$value"
}

if [[ -z "$COMPOSE_FILE" ]]; then
  COMPOSE_FILE="$(read_dotenv_value COMPOSE_FILE)"
  COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
fi
if [[ -z "$TARGET_TAG" ]]; then
  if [[ -z "$MIGRATION_IMAGE_TAG" ]]; then
    MIGRATION_IMAGE_TAG="$(read_dotenv_value MIGRATION_IMAGE_TAG)"
  fi
  MIGRATION_IMAGE_TAG="${MIGRATION_IMAGE_TAG:-latest}"
  TARGET_TAG="$MIGRATION_IMAGE_TAG"
elif [[ -z "$MIGRATION_IMAGE_TAG" ]]; then
  # TARGET_TAG is the runtime image tag.  Keep the migration and application
  # on the same image unless the caller supplies a complete IMAGE reference.
  MIGRATION_IMAGE_TAG="$TARGET_TAG"
fi

[[ "$MIGRATION_IMAGE_TAG" =~ ^[A-Za-z0-9._-]+$ ]] || die "MIGRATION_IMAGE_TAG contains unsupported characters"
[[ "$TARGET_TAG" =~ ^[A-Za-z0-9._-]+$ ]] || die "TARGET_TAG contains unsupported characters"
[[ "$MIGRATION_TIMEOUT_SECONDS" =~ ^[1-9][0-9]*$ ]] || die "MIGRATION_TIMEOUT_SECONDS must be a positive integer"

# Compose uses ':' on Unix and ';' on Windows.  Supporting both separators
# also lets an old COMPOSE_FILE value survive a migration on Git Bash.
compose_files=()
compose_raw="$COMPOSE_FILE"
if [[ "$compose_raw" == *';'* ]]; then
  IFS=';' read -r -a compose_files <<< "$compose_raw"
elif [[ "$compose_raw" =~ ^[A-Za-z]: ]]; then
  # A single native Windows path contains a drive-letter colon, not a
  # Unix-style Compose file separator.
  compose_files=("$compose_raw")
else
  IFS=':' read -r -a compose_files <<< "$compose_raw"
fi
(( ${#compose_files[@]} > 0 )) || die "COMPOSE_FILE is empty"
for compose_file in "${compose_files[@]}"; do
  [[ -n "$compose_file" && -f "$compose_file" ]] || die "compose file not found: $compose_file"
done

compose_base=(docker compose --env-file "$ENV_FILE")
for compose_file in "${compose_files[@]}"; do compose_base+=(-f "$compose_file"); done

compose_dir="$(dirname "${compose_files[0]}")"
if [[ "$compose_dir" == "." ]]; then
  compose_dir="$(pwd -P)"
else
  compose_dir="$(cd "$compose_dir" && pwd -P)"
fi

for key in DATABASE_HOST DATABASE_USER DATABASE_PASSWORD DATABASE_NAME; do
  value="$(read_dotenv_value "$key")"
  [[ -n "$value" ]] || die "$key must be set in $ENV_FILE"
  export "$key=$value"
done
# Do not let an inherited process variable override the live .env.  Compose
# gives shell variables precedence over --env-file values, so clear optional
# database settings that are absent from the file before validating overlays.
for key in DATABASE_DRIVER DATABASE_PATH DATABASE_PORT DATABASE_SSL_MODE IMAGE_REPOSITORY DATABASE_NETWORK_NAME DATABASE_NETWORK_EXTERNAL DATABASE_MAX_OPEN_CONNS DATABASE_MAX_IDLE_CONNS; do
  value="$(read_dotenv_value "$key")"
  if [[ -n "$value" ]]; then
    export "$key=$value"
  else
    unset "$key"
  fi
done

if [[ -z "$SOURCE_DB" ]]; then
  SOURCE_DB="$(read_dotenv_value DATABASE_PATH)"
  SOURCE_DB="${SOURCE_DB:-/app/data/upstream-ops.db}"
fi
case "$SOURCE_DB" in
  /app/data/*) ;;
  *) die "SOURCE_DB must stay under /app/data inside the migration container" ;;
esac

# Refuse a second migration run after the PostgreSQL settings were already
# persisted.  The target is intentionally never deleted by this helper.
source_driver="$(read_dotenv_value DATABASE_DRIVER)"
source_driver_lower="$(printf '%s' "$source_driver" | tr '[:upper:]' '[:lower:]')"
if [[ "$source_driver_lower" == "postgres" || "$source_driver_lower" == "postgresql" ]]; then
  die "DATABASE_DRIVER is already postgres; use the normal image upgrade helper"
fi

if [[ -z "$IMAGE" ]]; then
  image_repository="$(read_dotenv_value IMAGE_REPOSITORY)"
  IMAGE="${image_repository:-ghcr.io/ai8888-shop/upstream-ops}:$TARGET_TAG"
fi
[[ "$IMAGE" != *$'\n'* && "$IMAGE" != *$'\r'* && "$IMAGE" != *'"'* ]] || die "IMAGE contains unsupported characters"

data_resolved="$(cd "$DATA_DIR" && pwd -P)"
workspace_resolved="$(pwd -P)"
[[ "$data_resolved" != "/" && "$data_resolved" != "$workspace_resolved" ]] || die "refusing to use a broad data path: $DATA_DIR"
source_relative="${SOURCE_DB#/app/data/}"
[[ "$source_relative" != /* && "$source_relative" != */../* && "$source_relative" != ../* ]] || die "SOURCE_DB must name a file below /app/data"
source_local="$data_resolved/$source_relative"
[[ -f "$source_local" ]] || die "SQLite database not found: $source_local"

network_external="$(read_dotenv_value DATABASE_NETWORK_EXTERNAL)"
network_external="${network_external:-false}"
network_external="$(printf '%s' "$network_external" | tr '[:upper:]' '[:lower:]')"
if [[ -z "$MIGRATION_NETWORK" && "$network_external" == "true" ]]; then
  MIGRATION_NETWORK="${DATABASE_NETWORK_NAME:-}"
fi
if [[ "$network_external" == "true" && -z "${DATABASE_NETWORK_NAME:-}" ]]; then
  die "DATABASE_NETWORK_NAME is required when DATABASE_NETWORK_EXTERNAL=true"
fi
if [[ -n "$MIGRATION_NETWORK" ]]; then
  docker network inspect "$MIGRATION_NETWORK" >/dev/null 2>&1 || die "Docker network not found: $MIGRATION_NETWORK"
  export DATABASE_NETWORK_NAME="$MIGRATION_NETWORK"
  export DATABASE_NETWORK_EXTERNAL="true"
fi

compose_postgres=("${compose_base[@]}" -f "$POSTGRES_COMPOSE_FILE")
"${compose_base[@]}" config --quiet >/dev/null || die "base Compose configuration is invalid"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_ROOT"
backup_root_resolved="$(cd "$BACKUP_ROOT" && pwd -P)"
case "$backup_root_resolved/" in
  "$data_resolved"/*) die "backup path must not be inside DATA_DIR: $backup_root_resolved" ;;
esac
backup_dir="$backup_root_resolved/upstream-ops-$timestamp"
mkdir -p "$backup_dir/data"

container_ids="$("${compose_base[@]}" ps -q "$SERVICE" 2>/dev/null || true)"
container_id="$(printf '%s\n' "$container_ids" | sed -n '1p')"
[[ -n "$container_id" ]] || die "service $SERVICE is not running; start it once before upgrading"
[[ -z "$(printf '%s\n' "$container_ids" | sed -n '2p')" ]] || die "service $SERVICE has multiple containers; scale it to one before upgrading"
old_image_ref="$(docker inspect -f '{{.Config.Image}}' "$container_id")"
old_image_id="$(docker inspect -f '{{.Image}}' "$container_id")"
[[ -n "$old_image_ref" && -n "$old_image_id" ]] || die "cannot determine current image"
rollback_image="upstream-ops-rollback:$timestamp"
docker tag "$old_image_id" "$rollback_image" || die "could not tag the old image for rollback"

target_override="$(mktemp "${TMPDIR:-/tmp}/upstream-ops-postgres-target.XXXXXX.yml")"
rollback_override="$(mktemp "${TMPDIR:-/tmp}/upstream-ops-postgres-rollback.XXXXXX.yml")"
snapshot_dir=""
snapshot_db=""
cleanup() {
  rm -f "$target_override" "$rollback_override" || true
  if [[ -n "$snapshot_dir" && -d "$snapshot_dir" ]]; then
    rm -rf -- "$snapshot_dir" || true
  fi
}
trap cleanup EXIT

write_override() {
  local file="$1" image="$2" include_image="$3"
  {
    echo "services:"
    echo "  $SERVICE:"
    if [[ "$include_image" == "true" ]]; then
      printf '    image: "%s"\n' "$image"
    fi
    cat <<'EOF'
    environment:
      DATABASE_DRIVER: "postgres"
      DATABASE_HOST: "${DATABASE_HOST:?DATABASE_HOST is required}"
      DATABASE_PORT: "${DATABASE_PORT:-5432}"
      DATABASE_USER: "${DATABASE_USER:?DATABASE_USER is required}"
      DATABASE_PASSWORD: "${DATABASE_PASSWORD:?DATABASE_PASSWORD is required}"
      DATABASE_NAME: "${DATABASE_NAME:-upstreamops}"
      DATABASE_SSL_MODE: "${DATABASE_SSL_MODE:-disable}"
      DATABASE_MAX_OPEN_CONNS: "${DATABASE_MAX_OPEN_CONNS:-20}"
      DATABASE_MAX_IDLE_CONNS: "${DATABASE_MAX_IDLE_CONNS:-5}"
EOF
    if [[ -n "$MIGRATION_NETWORK" ]]; then
      cat <<'EOF'
    networks:
      - default
      - database

networks:
  database:
    name: "${DATABASE_NETWORK_NAME:?DATABASE_NETWORK_NAME is required}"
    external: true
EOF
    fi
  } > "$file"
}

write_override "$target_override" "$IMAGE" true
cat > "$rollback_override" <<EOF
services:
  $SERVICE:
    image: "$rollback_image"
EOF

# Validate the complete target stack before taking the service offline.
compose_target=("${compose_postgres[@]}" -f "$target_override")
"${compose_target[@]}" config --quiet >/dev/null || die "PostgreSQL Compose configuration is invalid"

# Pull the exact image used by both the migration binary and the application.
# The old image ID has already been captured, so latest-tag replacement is
# still recoverable.
docker pull "$IMAGE" >/dev/null || die "could not pull target image: $IMAGE"

APP_STOPPED=0
POSTGRES_STARTED=0
ROLLBACK_DONE=0
COMPLETED=0
PERSISTENT_OVERRIDE_CHANGED=0
PERSISTENT_OVERRIDE=""
PERSISTENT_OVERRIDE_BACKUP=""

wait_for_healthy() {
  local service="$1"
  shift
  local deadline=$((SECONDS + HEALTH_TIMEOUT_SECONDS))
  local container status
  while (( SECONDS < deadline )); do
    container="$("$@" ps -q "$service" 2>/dev/null | head -n 1 || true)"
    if [[ -n "$container" ]]; then
      status="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-health{{end}}' "$container" 2>/dev/null || true)"
      if [[ "$status" == "healthy" ]]; then return 0; fi
      if [[ "$status" == "no-health" ]] && docker exec "$container" wget -q -O- http://localhost:8418/healthz >/dev/null 2>&1; then return 0; fi
      if [[ "$status" == "unhealthy" || "$status" == "exited" || "$status" == "dead" ]]; then break; fi
    fi
    sleep 2
  done
  echo "health check failed for $service" >&2
  "$@" logs --tail=100 "$service" >&2 || true
  return 1
}

persist_env_value_in_file() {
  local file="$1" key="$2" value="$3" tmp="${file}.next" mode
  [[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || return 1
  mode="$(stat -c '%a' "$file" 2>/dev/null || stat -f '%Lp' "$file" 2>/dev/null || printf '600')"
  local updated=0 line
  : > "$tmp" || return 1
  while IFS= read -r line || [[ -n "$line" ]]; do
    if [[ "$line" =~ ^[[:space:]]*${key}[[:space:]]*= ]]; then
      if (( !updated )); then printf '%s=%s\n' "$key" "$value" >> "$tmp"; updated=1; fi
      continue
    fi
    printf '%s\n' "$line" >> "$tmp"
  done < "$file"
  if (( !updated )); then printf '%s=%s\n' "$key" "$value" >> "$tmp"; fi
  chmod "$mode" "$tmp" 2>/dev/null || true
  mv "$tmp" "$file"
}

compose_file_separator() {
  if [[ "$COMPOSE_FILE" == *';'* || "${compose_files[0]}" =~ ^[A-Za-z]: ]]; then printf ';'; else printf ':'; fi
}

persist_postgres_env() {
  local tmp="${ENV_FILE}.upgrade.$$" separator compose_value persistent_basename has_persistent=0
  separator="$(compose_file_separator)"
  persistent_basename="$(basename "$PERSISTENT_OVERRIDE")"
  for compose_file in "${compose_files[@]}"; do
    case "$compose_file" in
      "$PERSISTENT_OVERRIDE"|"$persistent_basename"|"./$persistent_basename") has_persistent=1 ;;
    esac
  done
  compose_value="$COMPOSE_FILE"
  if (( !has_persistent )); then compose_value+="${separator}${PERSISTENT_OVERRIDE}"; fi
  cp -p "$ENV_FILE" "$tmp" || return 1
  persist_env_value_in_file "$tmp" DATABASE_DRIVER postgres || { rm -f "$tmp"; return 1; }
  persist_env_value_in_file "$tmp" IMAGE_TAG "$TARGET_TAG" || { rm -f "$tmp"; return 1; }
  persist_env_value_in_file "$tmp" COMPOSE_FILE "$compose_value" || { rm -f "$tmp"; return 1; }
  if [[ -n "$MIGRATION_NETWORK" ]]; then
    persist_env_value_in_file "$tmp" DATABASE_NETWORK_NAME "$MIGRATION_NETWORK" || { rm -f "$tmp"; return 1; }
    persist_env_value_in_file "$tmp" DATABASE_NETWORK_EXTERNAL true || { rm -f "$tmp"; return 1; }
  fi
  mv "$tmp" "$ENV_FILE"
}

restore_persistent_override() {
  [[ "$PERSISTENT_OVERRIDE_CHANGED" == "1" ]] || return 0
  if [[ -n "$PERSISTENT_OVERRIDE_BACKUP" && -f "$PERSISTENT_OVERRIDE_BACKUP" ]]; then
    cp "$PERSISTENT_OVERRIDE_BACKUP" "$PERSISTENT_OVERRIDE" || true
  else
    rm -f "$PERSISTENT_OVERRIDE" || true
  fi
}

rollback() {
  local rc="${1:-1}"
  if (( ROLLBACK_DONE )); then return "$rc"; fi
  ROLLBACK_DONE=1
  set +e
  if (( POSTGRES_STARTED )); then "${compose_target[@]}" stop "$SERVICE" >/dev/null 2>&1; fi
  restore_persistent_override
  if (( APP_STOPPED )); then
    echo "Restoring the previous SQLite container..." >&2
    "${compose_base[@]}" -f "$rollback_override" up -d "$SERVICE" >/dev/null 2>&1
    if ! wait_for_healthy "$SERVICE" "${compose_base[@]}" -f "$rollback_override"; then
      echo "Previous container could not be confirmed healthy; backup: $backup_dir" >&2
    fi
  fi
  return "$rc"
}

on_exit() {
  local rc=$?
  if (( rc != 0 && APP_STOPPED && !COMPLETED )); then rollback "$rc" || true; fi
  cleanup
  exit "$rc"
}
trap on_exit EXIT

echo "Stopping upstream-ops before taking the SQLite snapshot..."
if ! "${compose_base[@]}" stop "$SERVICE"; then
  APP_STOPPED=1
  rollback "could not stop the current container cleanly"
  exit 1
fi
APP_STOPPED=1
cp -a "$data_resolved"/. "$backup_dir/data"/
cp -a "$ENV_FILE" "$backup_dir/.env.before"
printf '%s\n' "$old_image_ref" > "$backup_dir/old-image.txt"
printf '%s\n' "$old_image_id" > "$backup_dir/old-image-id.txt"
printf '%s\n' "$rollback_image" > "$backup_dir/rollback-image.txt"
printf '%s\n' "$IMAGE" > "$backup_dir/target-image.txt"

snapshot_dir="$(mktemp -d "${TMPDIR:-/tmp}/upstream-ops-sqlite-snapshot.XXXXXX")"
snapshot_db="$snapshot_dir/$source_relative"
mkdir -p "$(dirname "$snapshot_db")"
cp -a "$source_local" "$snapshot_db"
for suffix in -wal -shm -journal; do
  if [[ -e "$source_local$suffix" ]]; then
    cp -a "$source_local$suffix" "$snapshot_db$suffix"
  fi
done
if command -v sqlite3 >/dev/null 2>&1; then
  # A checkpoint folds any copied WAL pages into the writable snapshot.  The
  # migration still receives the sidecars when sqlite3 is unavailable.
  sqlite3 "$snapshot_db" 'PRAGMA wal_checkpoint(TRUNCATE);' >/dev/null || die "could not checkpoint SQLite snapshot"
else
  echo "sqlite3 not found; retaining copied SQLite WAL/SHM sidecars" >&2
fi

echo "Copying SQLite data into PostgreSQL (target must be empty)..."
network_args=()
if [[ -n "$MIGRATION_NETWORK" ]]; then network_args+=(--network "$MIGRATION_NETWORK"); fi
docker run --pull=never --rm \
  "${network_args[@]}" \
  --entrypoint /bin/busybox \
  -v "$snapshot_dir:/app/data:ro" \
  -e DATABASE_PASSWORD \
  "$IMAGE" \
  timeout "$MIGRATION_TIMEOUT_SECONDS" /usr/local/bin/upstream-ops-migrate \
  -source "$SOURCE_DB" \
  -target-host "$DATABASE_HOST" \
  -target-port "${DATABASE_PORT:-5432}" \
  -target-user "$DATABASE_USER" \
  -target-name "$DATABASE_NAME" \
  -target-ssl-mode "${DATABASE_SSL_MODE:-disable}" \
  -skip-missing=true

echo "Starting upstream-ops with PostgreSQL and target image..."
POSTGRES_STARTED=1
"${compose_target[@]}" up -d "$SERVICE"
wait_for_healthy "$SERVICE" "${compose_target[@]}"

# Keep an override beside the deployment so an old hard-coded SQLite Compose
# file cannot silently switch back after a later plain `docker compose up`.
PERSISTENT_OVERRIDE="$compose_dir/docker-compose.upstream-ops-postgres.yml"
if [[ -e "$PERSISTENT_OVERRIDE" ]]; then
  PERSISTENT_OVERRIDE_BACKUP="$backup_dir/previous-postgres-compose.override.yml"
  cp "$PERSISTENT_OVERRIDE" "$PERSISTENT_OVERRIDE_BACKUP"
fi
PERSISTENT_OVERRIDE_CHANGED=1
write_override "$PERSISTENT_OVERRIDE" "$IMAGE" true
persist_postgres_env || { echo "Could not persist PostgreSQL settings to $ENV_FILE" >&2; exit 1; }

COMPLETED=1
APP_STOPPED=0
echo "Upgrade completed. Backup: $backup_dir"
echo "Future plain Compose starts use $PERSISTENT_OVERRIDE via COMPOSE_FILE."
