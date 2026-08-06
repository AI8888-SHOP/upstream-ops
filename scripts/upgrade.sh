#!/usr/bin/env bash
set -Eeuo pipefail

# Guided, recoverable Docker image upgrade for older upstream-ops installs.
# The script never deletes the old image or the backup.  It keeps a tagged
# rollback image until the operator removes it after a successful soak period.

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
COMPOSE_EXTRA_FILES="${COMPOSE_EXTRA_FILES:-}"
ENV_FILE="${ENV_FILE:-.env}"
SERVICE="${SERVICE:-app}"
DATA_DIR="${DATA_DIR:-./data}"
BACKUP_ROOT="${BACKUP_ROOT:-./backups}"
TARGET_TAG="${TARGET_TAG:-}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8418/healthz}"
HEALTH_TIMEOUT_SECONDS="${HEALTH_TIMEOUT_SECONDS:-180}"

die() { echo "[upgrade] $*" >&2; exit 2; }
command -v docker >/dev/null 2>&1 || die "docker is required"
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 is required"
[[ -f "$ENV_FILE" ]] || die "env file not found: $ENV_FILE"
[[ -d "$DATA_DIR" ]] || die "data directory not found: $DATA_DIR"

read_dotenv_value() {
	local key="$1" value
	value="$(sed -n "s/^[[:space:]]*${key}[[:space:]]*=[[:space:]]*//p" "$ENV_FILE" | tail -n 1)"
	value="${value%$'\r'}"
	value="${value#\"}"; value="${value%\"}"
	value="${value#\'}"; value="${value%\'}"
	printf '%s' "$value"
}

# A PostgreSQL migration persists COMPOSE_FILE in .env so that a later plain
# `docker compose up` keeps loading its generated database override.  Honor
# that value when the caller did not explicitly select another file.
if [[ "$COMPOSE_FILE" == "docker-compose.yml" ]]; then
	from_env="$(read_dotenv_value COMPOSE_FILE)"
	[[ -z "$from_env" ]] || COMPOSE_FILE="$from_env"
fi

compose_files=()
compose_raw="$COMPOSE_FILE"
if [[ "$compose_raw" == *';'* ]]; then
	IFS=';' read -r -a compose_files <<< "$compose_raw"
elif [[ "$compose_raw" =~ ^[A-Za-z]: ]]; then
	# A single native Windows path contains a colon after the drive letter;
	# it is not a Unix-style Compose file separator.
	compose_files=("$compose_raw")
else
	IFS=':' read -r -a compose_files <<< "$compose_raw"
fi
if [[ -n "$COMPOSE_EXTRA_FILES" ]]; then
	if [[ "$COMPOSE_EXTRA_FILES" == *';'* ]]; then
		IFS=';' read -r -a extra_files <<< "$COMPOSE_EXTRA_FILES"
	elif [[ "$COMPOSE_EXTRA_FILES" =~ ^[A-Za-z]: ]]; then
		extra_files=("$COMPOSE_EXTRA_FILES")
	else
		IFS=':' read -r -a extra_files <<< "$COMPOSE_EXTRA_FILES"
	fi
	compose_files+=("${extra_files[@]}")
fi
(( ${#compose_files[@]} > 0 )) || die "COMPOSE_FILE is empty"
compose_args=(docker compose --env-file "$ENV_FILE")
for compose_file in "${compose_files[@]}"; do
	[[ -n "$compose_file" && -f "$compose_file" ]] || die "compose file not found: $compose_file"
	compose_args+=(-f "$compose_file")
done
compose_dir="$(cd "$(dirname "${compose_files[0]}")" && pwd -P)"

if [[ -z "$TARGET_TAG" ]]; then
	# Read only the IMAGE_TAG assignment; do not source .env because it is
	# operator-controlled input and may contain shell code.
	TARGET_TAG="$(read_dotenv_value IMAGE_TAG)"
	TARGET_TAG="${TARGET_TAG:-latest}"
fi
[[ "$TARGET_TAG" =~ ^[A-Za-z0-9._-]+$ ]] || die "TARGET_TAG contains unsupported characters: $TARGET_TAG"
[[ "$SERVICE" =~ ^[A-Za-z0-9_.-]+$ ]] || die "SERVICE contains unsupported characters: $SERVICE"

data_abs="$(cd "$DATA_DIR" && pwd -P)"
cwd_abs="$(pwd -P)"
[[ "$data_abs" != "/" && "$data_abs" != "$cwd_abs" ]] || die "refusing broad data path: $data_abs"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$BACKUP_ROOT"
backup_root_abs="$(cd "$BACKUP_ROOT" && pwd -P)"
case "$backup_root_abs/" in
	"$data_abs"/*) die "backup path must not be inside DATA_DIR: $backup_root_abs" ;;
esac
backup_dir="$backup_root_abs/upstream-ops-$timestamp"
mkdir -p "$backup_dir/data"

container_ids="$("${compose_args[@]}" ps -q "$SERVICE")"
container_id="$(printf '%s\n' "$container_ids" | sed -n '1p')"
[[ -n "$container_id" ]] || die "service $SERVICE is not running; start it once before upgrading"
[[ -z "$(printf '%s\n' "$container_ids" | sed -n '2p')" ]] || die "service $SERVICE has multiple containers; scale it to one before upgrading"
old_image_ref="$(docker inspect -f '{{.Config.Image}}' "$container_id")"
old_image_id="$(docker inspect -f '{{.Image}}' "$container_id")"
[[ -n "$old_image_ref" && -n "$old_image_id" ]] || die "cannot determine current image"

# A mutable tag such as `latest` is replaced by `pull`.  Keep an immutable
# local tag so a failed health check can really restore the old image.
rollback_image="upstream-ops-rollback:$timestamp"
docker tag "$old_image_id" "$rollback_image"

image_repository="$(read_dotenv_value IMAGE_REPOSITORY)"
image_repository="${image_repository:-ghcr.io/ai8888-shop/upstream-ops}"
target_image="${image_repository}:$TARGET_TAG"
[[ "$target_image" != *$'\n'* && "$target_image" != *$'\r'* && "$target_image" != *'"'* ]] || die "target image contains unsupported characters"

rollback_compose="$(mktemp "${TMPDIR:-/tmp}/upstream-ops-rollback.XXXXXX.yml")"
target_compose="$(mktemp "${TMPDIR:-/tmp}/upstream-ops-target.XXXXXX.yml")"
persistent_override="$compose_dir/docker-compose.upstream-ops-image.yml"
persistent_override_backup="$backup_dir/previous-image-compose.override.yml"
persistent_override_changed=0
cleanup() { rm -f "$rollback_compose" "$target_compose"; }
trap cleanup EXIT
cat > "$rollback_compose" <<EOF
services:
  $SERVICE:
    image: "$rollback_image"
EOF
cat > "$target_compose" <<EOF
services:
  $SERVICE:
    image: "$target_image"
EOF
target_compose_args=("${compose_args[@]}" -f "$target_compose")

rollback() {
	local reason="${1:-upgrade failed}"
	echo "[upgrade] $reason; restoring $rollback_image (was $old_image_ref)" >&2
	if (( persistent_override_changed )); then
		if [[ -f "$persistent_override_backup" ]]; then
			cp "$persistent_override_backup" "$persistent_override" || true
		else
			rm -f "$persistent_override" || true
		fi
	fi
	"${compose_args[@]}" -f "$rollback_compose" up -d "$SERVICE" >/dev/null 2>&1 || true
	if ! wait_for_health 60 "${compose_args[@]}" -f "$rollback_compose"; then
		echo "[upgrade] rollback health check failed; backup: $backup_dir" >&2
	else
		echo "[upgrade] rollback restored; backup: $backup_dir" >&2
	fi
}

wait_for_health() {
	local timeout="${1:-$HEALTH_TIMEOUT_SECONDS}" start now cid status
	shift
	local -a health_compose=("$@")
	start="$(date +%s)"
	while :; do
		cid="$("${health_compose[@]}" ps -q "$SERVICE" 2>/dev/null || true)"
		if [[ -n "$cid" ]]; then
			status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' "$cid" 2>/dev/null || true)"
			if [[ "$status" == "healthy" ]]; then
				return 0
			fi
			if [[ "$status" == "running" ]]; then
				if docker exec "$cid" wget -q -O- http://127.0.0.1:8418/healthz >/dev/null 2>&1; then return 0; fi
				if command -v curl >/dev/null 2>&1 && curl -fsS --max-time 5 "$HEALTH_URL" >/dev/null 2>&1; then return 0; fi
			fi
			if [[ "$status" == "unhealthy" || "$status" == "exited" || "$status" == "dead" ]]; then
				"${health_compose[@]}" logs --tail=80 "$SERVICE" >&2 || true
				return 1
			fi
		fi
		now="$(date +%s)"
		if (( now - start >= timeout )); then
			"${health_compose[@]}" logs --tail=80 "$SERVICE" >&2 || true
			return 1
		fi
		sleep 2
	done
}

compose_file_separator() {
	if [[ "$COMPOSE_FILE" == *';'* || "${compose_files[0]}" =~ ^[A-Za-z]: ]]; then
		printf ';'
	else
		printf ':'
	fi
}

persist_env_value_in_file() {
	local file="$1" key="$2" value="$3" tmp="${file}.next" line updated=0 mode
	[[ "$value" != *$'\n'* && "$value" != *$'\r'* ]] || return 1
	mode="$(stat -c '%a' "$file" 2>/dev/null || stat -f '%Lp' "$file" 2>/dev/null || printf '600')"
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

persist_target_env() {
	local tmp="${ENV_FILE}.upgrade.$$" compose_value separator has_persistent=0 compose_file
	separator="$(compose_file_separator)"
	for compose_file in "${compose_files[@]}"; do
		case "$compose_file" in
			"$persistent_override"|"$(basename "$persistent_override")"|"./$(basename "$persistent_override")") has_persistent=1 ;;
		esac
	done
	compose_value="$COMPOSE_FILE"
	if (( !has_persistent )); then compose_value+="${separator}${persistent_override}"; fi
	cp -p "$ENV_FILE" "$tmp" || return 1
	persist_env_value_in_file "$tmp" IMAGE_TAG "$TARGET_TAG" || { rm -f "$tmp"; return 1; }
	persist_env_value_in_file "$tmp" COMPOSE_FILE "$compose_value" || { rm -f "$tmp"; return 1; }
	mv "$tmp" "$ENV_FILE"
}

echo "[upgrade] backup: $backup_dir"
echo "[upgrade] current image: $old_image_ref"
echo "[upgrade] target tag: $TARGET_TAG"
# Pull before stopping the old service.  If the pull fails there is no
# downtime, and the immutable old image tag is already available for rollback.
if ! IMAGE_TAG="$TARGET_TAG" "${target_compose_args[@]}" pull "$SERVICE"; then
	die "image pull failed"
fi
if ! "${compose_args[@]}" stop "$SERVICE"; then
	rollback "could not stop the current container cleanly"
	exit 1
fi
if ! cp -a "$DATA_DIR"/. "$backup_dir/data"/ ||
	! cp -a "$ENV_FILE" "$backup_dir/.env.before" ||
	! printf '%s\n' "$old_image_ref" > "$backup_dir/old-image.txt" ||
	! printf '%s\n' "$rollback_image" > "$backup_dir/rollback-image.txt" ||
	! printf '%s\n' "$target_image" > "$backup_dir/target-image.txt"; then
	rollback "backup failed"
	exit 1
fi

if ! IMAGE_TAG="$TARGET_TAG" "${target_compose_args[@]}" up -d "$SERVICE"; then
	rollback "container start failed"
	exit 1
fi
if ! wait_for_health "$HEALTH_TIMEOUT_SECONDS" "${target_compose_args[@]}"; then
	rollback "health check failed"
	exit 1
fi

if [[ -f "$persistent_override" ]]; then
	cp "$persistent_override" "$persistent_override_backup" || { rollback "could not back up existing image override"; exit 1; }
fi
if ! cp "$target_compose" "$persistent_override"; then
	rollback "could not persist image override in $compose_dir"
	exit 1
fi
persistent_override_changed=1
if ! persist_target_env; then
	rollback "could not persist IMAGE_TAG/COMPOSE_FILE in $ENV_FILE"
	exit 1
fi

echo "[upgrade] completed successfully"
echo "[upgrade] keep $backup_dir and $rollback_image until the new version is verified"
