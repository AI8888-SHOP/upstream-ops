[CmdletBinding()]
param(
    [string]$ComposeFile = "",
    [string[]]$ComposeExtraFile = @(),
    [string]$PostgresComposeFile = "docker-compose.postgres.yml",
    [string]$EnvFile = ".env",
    [string]$DataDir = "./data",
    [string]$Service = "app",
    [string]$Image = "",
    [string]$MigrationImageTag = "",
    [string]$TargetTag = "",
    [string]$MigrationNetwork = "",
    [string]$SourceDb = "",
    [string]$BackupRoot = "./backups",
    [int]$HealthTimeoutSeconds = 120,
    [int]$MigrationTimeoutSeconds = 1800
)

$ErrorActionPreference = "Stop"

function Read-DotEnv([string]$Path) {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^\s*#' -or $line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { continue }
        $values[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
    }
    return $values
}

function Invoke-DockerChecked([string[]]$Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker command failed (exit $LASTEXITCODE): docker $($Arguments -join ' ')"
    }
}

function New-ComposeArgs([string[]]$Files, [string]$EnvironmentFile) {
    $args = @("compose", "--env-file", $EnvironmentFile)
    foreach ($file in $Files) { $args += @("-f", $file) }
    return $args
}

function Wait-Healthy([string[]]$ComposeArgs, [string]$TargetService, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $containers = @(& docker @ComposeArgs ps -q $TargetService 2>$null | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($containers.Count -gt 0) {
            $container = ([string]$containers[0]).Trim()
            $health = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}no-health{{end}}' $container 2>$null)
            $health = if ($null -eq $health) { "" } else { ([string]$health).Trim() }
            if ($health -eq "healthy") { return }
            if ($health -eq "no-health") {
                & docker exec $container wget -q -O- http://localhost:8418/healthz *> $null
                if ($LASTEXITCODE -eq 0) { return }
            }
            if ($health -in @("unhealthy", "exited", "dead")) { break }
        }
        Start-Sleep -Seconds 2
    }
    Write-Host "health check failed for $TargetService" -ForegroundColor Red
    & docker @ComposeArgs logs --tail=100 $TargetService
    throw "upstream-ops did not become healthy within $TimeoutSeconds seconds"
}

function Write-PostgresOverride([string]$Path, [string]$OverrideImage, [bool]$IncludeImage) {
    $lines = @("services:", "  ${Service}:")
    if ($IncludeImage) {
        if ($OverrideImage -match '[\r\n"]') { throw "image contains unsupported characters" }
        $lines += "    image: `"$OverrideImage`""
    }
    $lines += @'
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
'@ -split "`r?`n"
    if (-not [string]::IsNullOrWhiteSpace($MigrationNetwork)) {
        $lines += @'
    networks:
      - default
      - database

networks:
  database:
    name: "${DATABASE_NETWORK_NAME:?DATABASE_NETWORK_NAME is required}"
    external: true
'@ -split "`r?`n"
    }
    $lines | Set-Content -LiteralPath $Path -Encoding utf8
}

function Persist-EnvValues([string]$Path, [hashtable]$Values) {
    $lines = [System.Collections.Generic.List[string]](Get-Content -LiteralPath $Path)
    foreach ($entry in $Values.GetEnumerator()) {
        if ([string]$entry.Value -match '[\r\n]') { throw "$($entry.Key) contains a newline" }
        $pattern = '^\s*' + [regex]::Escape($entry.Key) + '\s*='
        $replaced = $false
        for ($i = $lines.Count - 1; $i -ge 0; $i--) {
            if ($lines[$i] -match $pattern) {
                if (-not $replaced) {
                    $lines[$i] = "$($entry.Key)=$($entry.Value)"
                    $replaced = $true
                } else {
                    $lines.RemoveAt($i)
                }
            }
        }
        if (-not $replaced) { $lines.Add("$($entry.Key)=$($entry.Value)") }
    }
    $envFullPath = (Resolve-Path -LiteralPath $Path).Path
    $tempEnv = "$envFullPath.upgrade.$PID.tmp"
    try {
        [System.IO.File]::WriteAllLines($tempEnv, $lines, (New-Object System.Text.UTF8Encoding($false)))
        Move-Item -LiteralPath $tempEnv -Destination $envFullPath -Force
    } finally {
        if (Test-Path -LiteralPath $tempEnv) { Remove-Item -LiteralPath $tempEnv -Force }
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw "docker is required" }
if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) { throw "Missing env file: $EnvFile" }
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { throw "Missing data directory: $DataDir" }
if (-not (Test-Path -LiteralPath $PostgresComposeFile -PathType Leaf)) { throw "Missing PostgreSQL compose file: $PostgresComposeFile" }
if ($Service -notmatch '^[A-Za-z0-9_.-]+$') { throw "Service contains unsupported characters" }
if ($MigrationTimeoutSeconds -lt 1) { throw "MigrationTimeoutSeconds must be a positive integer" }

$envValues = Read-DotEnv $EnvFile
if ([string]::IsNullOrWhiteSpace($ComposeFile)) {
    $ComposeFile = if ($envValues.ContainsKey("COMPOSE_FILE")) { [string]$envValues["COMPOSE_FILE"] } else { "docker-compose.yml" }
}
$composeFiles = @()
if ($ComposeFile -match ';') {
    $composeFiles += $ComposeFile -split ';' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
} elseif ($ComposeFile -match '^[A-Za-z]:[\\/]') {
    # A single native Windows path contains a drive-letter colon, not a
    # Unix-style Compose file separator.
    $composeFiles += $ComposeFile
} else {
    $composeFiles += $ComposeFile -split ':' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }
}
$composeFiles += $ComposeExtraFile
if ($composeFiles.Count -eq 0) { throw "ComposeFile is empty" }
foreach ($file in $composeFiles) {
    if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw "Compose file not found: $file" }
}

foreach ($name in @("DATABASE_HOST", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME")) {
    if (-not $envValues.ContainsKey($name) -or [string]::IsNullOrWhiteSpace($envValues[$name])) { throw "$name must be set in $EnvFile" }
}
$sourceDriver = if ($envValues.ContainsKey("DATABASE_DRIVER")) { ([string]$envValues["DATABASE_DRIVER"]).Trim().ToLowerInvariant() } else { "sqlite" }
if ($sourceDriver -in @("postgres", "postgresql")) { throw "DATABASE_DRIVER is already postgres; use the normal image upgrade helper" }

if ([string]::IsNullOrWhiteSpace($TargetTag)) {
    if ([string]::IsNullOrWhiteSpace($MigrationImageTag)) {
        $MigrationImageTag = if ($envValues.ContainsKey("MIGRATION_IMAGE_TAG") -and -not [string]::IsNullOrWhiteSpace($envValues["MIGRATION_IMAGE_TAG"])) { [string]$envValues["MIGRATION_IMAGE_TAG"] } else { "latest" }
    }
    $TargetTag = $MigrationImageTag
} elseif ([string]::IsNullOrWhiteSpace($MigrationImageTag)) {
    # TARGET_TAG is the runtime image tag.  Keep the migration and application
    # on the same image unless the caller supplies a complete IMAGE reference.
    $MigrationImageTag = $TargetTag
}
if ($MigrationImageTag -notmatch '^[A-Za-z0-9._-]+$') { throw "MigrationImageTag contains unsupported characters" }
if ($TargetTag -notmatch '^[A-Za-z0-9._-]+$') { throw "TargetTag contains unsupported characters" }
if ([string]::IsNullOrWhiteSpace($Image)) {
    $repo = if ($envValues.ContainsKey("IMAGE_REPOSITORY")) { [string]$envValues["IMAGE_REPOSITORY"] } else { "ghcr.io/ai8888-shop/upstream-ops" }
    $Image = "$repo`:$TargetTag"
}

if ([string]::IsNullOrWhiteSpace($SourceDb)) {
    $SourceDb = if ($envValues.ContainsKey("DATABASE_PATH") -and -not [string]::IsNullOrWhiteSpace($envValues["DATABASE_PATH"])) { [string]$envValues["DATABASE_PATH"] } else { "/app/data/upstream-ops.db" }
}
if ($SourceDb -notmatch '^/app/data/') { throw "SourceDb must stay under /app/data inside the migration container" }

$dataResolved = (Resolve-Path -LiteralPath $DataDir).Path
$workspaceResolved = (Get-Location).Path
if ($dataResolved -eq [System.IO.Path]::GetPathRoot($dataResolved) -or $dataResolved -eq $workspaceResolved) { throw "Refusing to use a broad data path: $dataResolved" }
$sourceRelative = $SourceDb.Substring("/app/data/".Length)
if ([string]::IsNullOrWhiteSpace($sourceRelative) -or $sourceRelative.StartsWith("/") -or $sourceRelative -match '(^|[\/])\.\.([\/]|$)') { throw "SourceDb must name a file below /app/data" }
$sqlitePath = Join-Path $dataResolved ($sourceRelative -replace '/', [System.IO.Path]::DirectorySeparatorChar)
if (-not (Test-Path -LiteralPath $sqlitePath -PathType Leaf)) { throw "SQLite database not found: $sqlitePath" }

if ([string]::IsNullOrWhiteSpace($MigrationNetwork) -and $envValues.ContainsKey("DATABASE_NETWORK_EXTERNAL") -and ([string]$envValues["DATABASE_NETWORK_EXTERNAL"]).Trim().ToLowerInvariant() -eq "true") {
    if (-not $envValues.ContainsKey("DATABASE_NETWORK_NAME") -or [string]::IsNullOrWhiteSpace($envValues["DATABASE_NETWORK_NAME"])) { throw "DATABASE_NETWORK_NAME is required when DATABASE_NETWORK_EXTERNAL=true" }
    $MigrationNetwork = [string]$envValues["DATABASE_NETWORK_NAME"]
}
if (-not [string]::IsNullOrWhiteSpace($MigrationNetwork)) {
    & docker network inspect $MigrationNetwork *> $null
    if ($LASTEXITCODE -ne 0) { throw "Docker network not found: $MigrationNetwork" }
    $envValues["DATABASE_NETWORK_NAME"] = $MigrationNetwork
    $envValues["DATABASE_NETWORK_EXTERNAL"] = "true"
}

$composeBase = New-ComposeArgs $composeFiles $EnvFile
$composePostgres = $composeBase + @("-f", $PostgresComposeFile)
Invoke-DockerChecked ($composeBase + @("config", "--quiet"))

$composeDir = Split-Path -Parent ((Resolve-Path -LiteralPath $composeFiles[0]).Path)
$timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$backupRootResolved = if (Test-Path -LiteralPath $BackupRoot -PathType Container) { (Resolve-Path -LiteralPath $BackupRoot).Path } else { (New-Item -ItemType Directory -Path $BackupRoot -Force).FullName }
if ($backupRootResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) -eq $dataResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) -or $backupRootResolved.StartsWith($dataResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) { throw "Backup path must not be inside DATA_DIR: $backupRootResolved" }
$backupDir = Join-Path $backupRootResolved "upstream-ops-$timestamp"
New-Item -ItemType Directory -Path (Join-Path $backupDir "data") -Force | Out-Null

$containerLines = @(& docker @composeBase ps -q $Service 2>$null | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($containerLines.Count -ne 1) { throw "service $Service must have exactly one running container before upgrading" }
$containerId = ([string]$containerLines[0]).Trim()
$oldImageRef = (& docker inspect -f '{{.Config.Image}}' $containerId).Trim()
$oldImageId = (& docker inspect -f '{{.Image}}' $containerId).Trim()
if ([string]::IsNullOrWhiteSpace($oldImageRef) -or [string]::IsNullOrWhiteSpace($oldImageId)) { throw "cannot determine current image" }
$rollbackImage = "upstream-ops-rollback:$timestamp"
Invoke-DockerChecked @("tag", $oldImageId, $rollbackImage)

$targetOverride = [System.IO.Path]::GetTempFileName()
$rollbackOverride = [System.IO.Path]::GetTempFileName()
$persistentOverride = Join-Path $composeDir "docker-compose.upstream-ops-postgres.yml"
$persistentOverrideBackup = Join-Path $backupDir "previous-postgres-compose.override.yml"
$persistentOverrideChanged = $false
$appStopped = $false
$postgresStarted = $false
$completed = $false
$snapshotDir = ""
$snapshotDb = ""
$environmentNames = @("DATABASE_DRIVER", "DATABASE_PATH", "DATABASE_HOST", "DATABASE_PORT", "DATABASE_USER", "DATABASE_PASSWORD", "DATABASE_NAME", "DATABASE_SSL_MODE", "DATABASE_MAX_OPEN_CONNS", "DATABASE_MAX_IDLE_CONNS", "DATABASE_NETWORK_NAME", "DATABASE_NETWORK_EXTERNAL", "IMAGE_REPOSITORY")
$oldEnvironment = @{}
foreach ($name in $environmentNames) { $oldEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process") }

try {
    Write-PostgresOverride $targetOverride $Image $true
    @"
services:
  ${Service}:
    image: "$rollbackImage"
"@ | Set-Content -LiteralPath $rollbackOverride -Encoding utf8
    $composeTarget = $composePostgres + @("-f", $targetOverride)
    foreach ($name in $environmentNames) {
        if ($envValues.ContainsKey($name)) {
            [Environment]::SetEnvironmentVariable($name, [string]$envValues[$name], "Process")
        } else {
            # Compose gives process variables precedence over --env-file.
            # Clear absent settings so a parent shell cannot silently alter
            # the migration target or its network.
            [Environment]::SetEnvironmentVariable($name, $null, "Process")
        }
    }
    if (-not $envValues.ContainsKey("DATABASE_DRIVER")) { [Environment]::SetEnvironmentVariable("DATABASE_DRIVER", "sqlite", "Process") }
    $env:DATABASE_PASSWORD = [string]$envValues["DATABASE_PASSWORD"]

    Invoke-DockerChecked ($composeTarget + @("config", "--quiet"))

    # Pull before stopping the service.  The old image ID was captured above,
    # so replacing a mutable tag cannot make rollback point at the new image.
    Invoke-DockerChecked @("pull", $Image)

    Write-Host "Stopping upstream-ops before taking the SQLite snapshot..."
    try {
        Invoke-DockerChecked ($composeBase + @("stop", $Service))
    } catch {
        $appStopped = $true
        throw
    }
    $appStopped = $true
    $backupDataDir = Join-Path $backupDir "data"
    Get-ChildItem -LiteralPath $dataResolved -Force | ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination $backupDataDir -Recurse -Force }
    Copy-Item -LiteralPath $EnvFile -Destination (Join-Path $backupDir ".env.before") -Force
    Set-Content -LiteralPath (Join-Path $backupDir "old-image.txt") -Value $oldImageRef -Encoding utf8
    Set-Content -LiteralPath (Join-Path $backupDir "old-image-id.txt") -Value $oldImageId -Encoding utf8
    Set-Content -LiteralPath (Join-Path $backupDir "rollback-image.txt") -Value $rollbackImage -Encoding utf8
    Set-Content -LiteralPath (Join-Path $backupDir "target-image.txt") -Value $Image -Encoding utf8

    $snapshotDir = Join-Path ([System.IO.Path]::GetTempPath()) ("upstream-ops-sqlite-snapshot-" + [guid]::NewGuid().ToString("N"))
    New-Item -ItemType Directory -Path $snapshotDir -Force | Out-Null
    $snapshotDb = Join-Path $snapshotDir ($sourceRelative -replace '/', [System.IO.Path]::DirectorySeparatorChar)
    $snapshotParent = Split-Path -Parent $snapshotDb
    New-Item -ItemType Directory -Path $snapshotParent -Force | Out-Null
    Copy-Item -LiteralPath $sqlitePath -Destination $snapshotDb -Force
    foreach ($suffix in @("-wal", "-shm", "-journal")) {
        $sidecar = "$sqlitePath$suffix"
        if (Test-Path -LiteralPath $sidecar -PathType Leaf) { Copy-Item -LiteralPath $sidecar -Destination "$snapshotDb$suffix" -Force }
    }
    $sqlite3 = Get-Command sqlite3 -ErrorAction SilentlyContinue
    if ($null -ne $sqlite3) {
        & $sqlite3.Source $snapshotDb "PRAGMA wal_checkpoint(TRUNCATE);" *> $null
        if ($LASTEXITCODE -ne 0) { throw "could not checkpoint SQLite snapshot" }
    } else {
        Write-Warning "sqlite3 was not found; retaining copied SQLite WAL/SHM sidecars"
    }

    $targetArgs = @("run", "--pull", "never", "--rm")
    if (-not [string]::IsNullOrWhiteSpace($MigrationNetwork)) { $targetArgs += @("--network", $MigrationNetwork) }
    $targetArgs += @(
        "--entrypoint", "/bin/busybox",
        "-v", "${snapshotDir}:/app/data:ro",
        "-e", "DATABASE_PASSWORD",
        $Image,
        "timeout", [string]$MigrationTimeoutSeconds, "/usr/local/bin/upstream-ops-migrate",
        "-source", $SourceDb,
        "-target-host", [string]$envValues["DATABASE_HOST"],
        "-target-port", $(if ($envValues.ContainsKey("DATABASE_PORT")) { [string]$envValues["DATABASE_PORT"] } else { "5432" }),
        "-target-user", [string]$envValues["DATABASE_USER"],
        "-target-name", [string]$envValues["DATABASE_NAME"],
        "-target-ssl-mode", $(if ($envValues.ContainsKey("DATABASE_SSL_MODE")) { [string]$envValues["DATABASE_SSL_MODE"] } else { "disable" }),
        "-skip-missing=true"
    )
    Write-Host "Copying SQLite data into PostgreSQL (target must be empty)..."
    Invoke-DockerChecked $targetArgs

    Write-Host "Starting upstream-ops with PostgreSQL and target image..."
    $postgresStarted = $true
    Invoke-DockerChecked ($composeTarget + @("up", "-d", $Service))
    Wait-Healthy $composeTarget $Service $HealthTimeoutSeconds

    if (Test-Path -LiteralPath $persistentOverride -PathType Leaf) { Copy-Item -LiteralPath $persistentOverride -Destination $persistentOverrideBackup -Force }
    $persistentOverrideChanged = $true
    Write-PostgresOverride $persistentOverride $Image $true
    $persistentComposeFiles = @($composeFiles)
    $persistentFullPath = [System.IO.Path]::GetFullPath($persistentOverride)
    $hasPersistentOverride = $false
    foreach ($composeFile in $persistentComposeFiles) {
        if ([System.IO.Path]::GetFullPath($composeFile) -eq $persistentFullPath) { $hasPersistentOverride = $true; break }
    }
    if (-not $hasPersistentOverride) { $persistentComposeFiles += $persistentOverride }
    $composeValue = ($persistentComposeFiles -join [System.IO.Path]::PathSeparator)
    $persistValues = [ordered]@{ DATABASE_DRIVER = "postgres"; IMAGE_TAG = $TargetTag; COMPOSE_FILE = $composeValue }
    if (-not [string]::IsNullOrWhiteSpace($MigrationNetwork)) { $persistValues["DATABASE_NETWORK_NAME"] = $MigrationNetwork; $persistValues["DATABASE_NETWORK_EXTERNAL"] = "true" }
    Persist-EnvValues $EnvFile $persistValues

    $completed = $true
    $appStopped = $false
    Write-Host "Upgrade completed. Backup: $backupDir"
    Write-Host "Future plain Compose starts use $persistentOverride via COMPOSE_FILE."
} finally {
    if (-not $completed -and $appStopped) {
        Write-Host "Upgrade failed; restoring the previous SQLite container..." -ForegroundColor Yellow
        try {
            if ($postgresStarted) { & docker @composeTarget stop $Service *> $null }
            if ($persistentOverrideChanged) {
                if (Test-Path -LiteralPath $persistentOverrideBackup -PathType Leaf) { Copy-Item -LiteralPath $persistentOverrideBackup -Destination $persistentOverride -Force } else { Remove-Item -LiteralPath $persistentOverride -Force -ErrorAction SilentlyContinue }
            }
            & docker @composeBase -f $rollbackOverride up -d $Service *> $null
            if ($LASTEXITCODE -eq 0) { Wait-Healthy ($composeBase + @("-f", $rollbackOverride)) $Service $HealthTimeoutSeconds }
        } catch { Write-Host "Previous container could not be confirmed healthy; backup: $backupDir" -ForegroundColor Red }
    }
    foreach ($name in $environmentNames) { [Environment]::SetEnvironmentVariable($name, $oldEnvironment[$name], "Process") }
    Remove-Item -LiteralPath $targetOverride, $rollbackOverride -Force -ErrorAction SilentlyContinue
    if (-not [string]::IsNullOrWhiteSpace($snapshotDir) -and (Test-Path -LiteralPath $snapshotDir -PathType Container)) { Remove-Item -LiteralPath $snapshotDir -Recurse -Force -ErrorAction SilentlyContinue }
}
