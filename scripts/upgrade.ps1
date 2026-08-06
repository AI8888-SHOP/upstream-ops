[CmdletBinding()]
param(
    [string]$ComposeFile = "",
    [string[]]$ComposeExtraFile = @(),
    [string]$EnvFile = ".env",
    [string]$Service = "app",
    [string]$DataDir = "./data",
    [string]$BackupRoot = "./backups",
    [string]$TargetTag = "",
    [string]$HealthUrl = "http://127.0.0.1:8418/healthz",
    [int]$HealthTimeoutSeconds = 180
)

$ErrorActionPreference = "Stop"

function Fail([string]$Message) { throw "[upgrade] $Message" }
function Read-DotEnv([string]$Path) {
    $values = @{}
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match '^\s*#' -or $line -notmatch '^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$') { continue }
        $values[$Matches[1]] = $Matches[2].Trim().Trim('"').Trim("'")
    }
    return $values
}
function New-ComposeArgs([string[]]$Files, [string]$EnvironmentFile) {
    $args = @("compose", "--env-file", $EnvironmentFile)
    foreach ($file in $Files) { $args += @("-f", $file) }
    return $args
}
function Invoke-DockerChecked([string[]]$Arguments) {
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker command failed (exit $LASTEXITCODE): docker $($Arguments -join ' ')" }
}
function Wait-ForHealth([string[]]$ComposeArgs, [int]$TimeoutSeconds) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $ids = @(& docker @ComposeArgs ps -q $Service 2>$null | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        if ($ids.Count -gt 0) {
            $cid = ([string]$ids[0]).Trim()
            $status = (& docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}' $cid 2>$null)
            $status = if ($null -eq $status) { "" } else { ([string]$status).Trim() }
            if ($status -eq "healthy") { return $true }
            if ($status -eq "running") {
                & docker exec $cid wget -q -O- http://127.0.0.1:8418/healthz *> $null
                if ($LASTEXITCODE -eq 0) { return $true }
                if (Get-Command curl.exe -ErrorAction SilentlyContinue) {
                    & curl.exe -fsS --max-time 5 $HealthUrl *> $null
                    if ($LASTEXITCODE -eq 0) { return $true }
                }
            }
            if ($status -in @("unhealthy", "exited", "dead")) { break }
        }
        Start-Sleep -Seconds 2
    }
    & docker @ComposeArgs logs --tail=80 $Service
    return $false
}
function Persist-TargetTag([string]$Path, [string]$Tag) {
    $content = Get-Content -LiteralPath $Path -Raw
    $replacement = "IMAGE_TAG=$Tag"
    if ($content -match '(?m)^\s*IMAGE_TAG\s*=') {
        $content = [regex]::Replace($content, '(?m)^\s*IMAGE_TAG\s*=.*$', $replacement)
    } else {
        if (-not $content.EndsWith("`n")) { $content += "`r`n" }
        $content += "$replacement`r`n"
    }
    $envFullPath = (Resolve-Path -LiteralPath $Path).Path
    $tempEnv = "$envFullPath.upgrade.$PID.tmp"
    try {
        [System.IO.File]::WriteAllText($tempEnv, $content, (New-Object System.Text.UTF8Encoding($false)))
        Move-Item -LiteralPath $tempEnv -Destination $envFullPath -Force
    } finally {
        if (Test-Path -LiteralPath $tempEnv) { Remove-Item -LiteralPath $tempEnv -Force }
    }
}
function Persist-TargetEnvironment([string]$Path, [string]$Tag, [string]$ComposeValue) {
    $content = Get-Content -LiteralPath $Path -Raw
    foreach ($entry in ([ordered]@{ IMAGE_TAG = $Tag; COMPOSE_FILE = $ComposeValue }).GetEnumerator()) {
        if ([string]$entry.Value -match '[\r\n]') { throw "$($entry.Key) contains a newline" }
        $replacement = "$($entry.Key)=$($entry.Value)"
        $pattern = '(?m)^\s*' + [regex]::Escape($entry.Key) + '\s*=.*$'
        if ($content -match $pattern) {
            $content = [regex]::Replace($content, $pattern, $replacement)
        } else {
            if (-not $content.EndsWith("`n")) { $content += "`r`n" }
            $content += "$replacement`r`n"
        }
    }
    $envFullPath = (Resolve-Path -LiteralPath $Path).Path
    $tempEnv = "$envFullPath.upgrade.$PID.env.tmp"
    try {
        [System.IO.File]::WriteAllText($tempEnv, $content, (New-Object System.Text.UTF8Encoding($false)))
        Move-Item -LiteralPath $tempEnv -Destination $envFullPath -Force
    } finally {
        if (Test-Path -LiteralPath $tempEnv) { Remove-Item -LiteralPath $tempEnv -Force }
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail "docker is required" }
& docker compose version *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Compose v2 is required" }
if (-not (Test-Path -LiteralPath $EnvFile -PathType Leaf)) { Fail "env file not found: $EnvFile" }
if (-not (Test-Path -LiteralPath $DataDir -PathType Container)) { Fail "data directory not found: $DataDir" }
if ($Service -notmatch '^[A-Za-z0-9_.-]+$') { Fail "service contains unsupported characters" }

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
if ($composeFiles.Count -eq 0) { Fail "ComposeFile is empty" }
foreach ($file in $composeFiles) { if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { Fail "compose file not found: $file" } }
$composeDir = Split-Path -Parent ((Resolve-Path -LiteralPath $composeFiles[0]).Path)

if ([string]::IsNullOrWhiteSpace($TargetTag)) {
    $envLine = Get-Content -LiteralPath $EnvFile | Where-Object { $_ -match '^\s*IMAGE_TAG\s*=' } | Select-Object -Last 1
    if ($envLine -match '^\s*IMAGE_TAG\s*=\s*(.*)$') { $TargetTag = $Matches[1].Trim().Trim('"').Trim("'") }
    if ([string]::IsNullOrWhiteSpace($TargetTag)) { $TargetTag = "latest" }
}
if ($TargetTag -notmatch '^[A-Za-z0-9._-]+$') { Fail "TargetTag contains unsupported characters: $TargetTag" }

$dataResolved = (Resolve-Path -LiteralPath $DataDir).Path
$cwdResolved = (Get-Location).Path
$rootResolved = [System.IO.Path]::GetPathRoot($dataResolved)
if ($dataResolved -eq $rootResolved -or $dataResolved -eq $cwdResolved) { Fail "refusing broad data path: $dataResolved" }

$composeBase = New-ComposeArgs $composeFiles $EnvFile
Invoke-DockerChecked ($composeBase + @("config", "--quiet"))
$timestamp = (Get-Date).ToUniversalTime().ToString("yyyyMMddTHHmmssZ")
$backupRootResolved = if (Test-Path -LiteralPath $BackupRoot -PathType Container) { (Resolve-Path -LiteralPath $BackupRoot).Path } else { (New-Item -ItemType Directory -Path $BackupRoot -Force).FullName }
if ($backupRootResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) -eq $dataResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) -or $backupRootResolved.StartsWith($dataResolved.TrimEnd([System.IO.Path]::DirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) { Fail "backup path must not be inside DATA_DIR: $backupRootResolved" }
$backupDir = Join-Path $backupRootResolved "upstream-ops-$timestamp"
New-Item -ItemType Directory -Path (Join-Path $backupDir "data") -Force | Out-Null

$containerLines = @(& docker @composeBase ps -q $Service 2>$null | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
if ($containerLines.Count -ne 1) { Fail "service $Service must have exactly one running container before upgrading" }
$containerId = ([string]$containerLines[0]).Trim()
$oldImageRef = (& docker inspect -f '{{.Config.Image}}' $containerId).Trim()
$oldImageId = (& docker inspect -f '{{.Image}}' $containerId).Trim()
if ([string]::IsNullOrWhiteSpace($oldImageRef) -or [string]::IsNullOrWhiteSpace($oldImageId)) { Fail "cannot determine current image" }
$rollbackImage = "upstream-ops-rollback:$timestamp"
Invoke-DockerChecked @("tag", $oldImageId, $rollbackImage)

$imageRepository = if ($envValues.ContainsKey("IMAGE_REPOSITORY") -and -not [string]::IsNullOrWhiteSpace($envValues["IMAGE_REPOSITORY"])) { [string]$envValues["IMAGE_REPOSITORY"] } else { "" }
if ([string]::IsNullOrWhiteSpace($imageRepository)) {
    $imageRepository = "ghcr.io/ai8888-shop/upstream-ops"
}
$targetImage = "$imageRepository`:$TargetTag"
if ($targetImage -match '[\r\n"]') { Fail "target image contains unsupported characters" }

$rollbackCompose = Join-Path $backupDir "rollback.compose.yml"
@"
services:
  ${Service}:
    image: "$rollbackImage"
"@ | Set-Content -LiteralPath $rollbackCompose -Encoding utf8
$rollbackArgs = $composeBase + @("-f", $rollbackCompose)

$targetEnvFile = "$((Resolve-Path -LiteralPath $EnvFile).Path).upgrade.$PID.target.tmp"
$targetContent = Get-Content -LiteralPath $EnvFile -Raw
if ($targetContent -match '(?m)^\s*IMAGE_TAG\s*=') { $targetContent = [regex]::Replace($targetContent, '(?m)^\s*IMAGE_TAG\s*=.*$', "IMAGE_TAG=$TargetTag") } else { if (-not $targetContent.EndsWith("`n")) { $targetContent += "`r`n" }; $targetContent += "IMAGE_TAG=$TargetTag`r`n" }
[System.IO.File]::WriteAllText($targetEnvFile, $targetContent, (New-Object System.Text.UTF8Encoding($false)))
$targetComposeOverride = [System.IO.Path]::GetTempFileName()
@"
services:
  ${Service}:
    image: "$targetImage"
"@ | Set-Content -LiteralPath $targetComposeOverride -Encoding utf8
$composeTarget = (New-ComposeArgs $composeFiles $targetEnvFile) + @("-f", $targetComposeOverride)
$persistentOverride = Join-Path $composeDir "docker-compose.upstream-ops-image.yml"
$persistentOverrideBackup = Join-Path $backupDir "previous-image-compose.override.yml"
$persistentOverrideChanged = $false

$appStopped = $false
$completed = $false
$oldImageTag = [Environment]::GetEnvironmentVariable("IMAGE_TAG", "Process")
try {
    # Shell variables have precedence over --env-file in Compose.  Make the
    # requested tag win even when the operator's process already exports one,
    # then restore it in finally.
    [Environment]::SetEnvironmentVariable("IMAGE_TAG", $TargetTag, "Process")
    # Pull while the old container is still available.  A failed pull leaves
    # the service and its data untouched.
    Invoke-DockerChecked ($composeTarget + @("pull", $Service))

    Write-Host "[upgrade] backup: $backupDir"
    Write-Host "[upgrade] current image: $oldImageRef"
    Write-Host "[upgrade] target tag: $TargetTag"
    $appStopped = $true
    Invoke-DockerChecked ($composeBase + @("stop", $Service))
    try {
        $backupDataDir = Join-Path $backupDir "data"
        Get-ChildItem -LiteralPath $dataResolved -Force | ForEach-Object { Copy-Item -LiteralPath $_.FullName -Destination $backupDataDir -Recurse -Force }
        Copy-Item -LiteralPath $EnvFile -Destination (Join-Path $backupDir ".env.before") -Force
        Set-Content -LiteralPath (Join-Path $backupDir "old-image.txt") -Value $oldImageRef -Encoding utf8
        Set-Content -LiteralPath (Join-Path $backupDir "old-image-id.txt") -Value $oldImageId -Encoding utf8
        Set-Content -LiteralPath (Join-Path $backupDir "rollback-image.txt") -Value $rollbackImage -Encoding utf8
        Set-Content -LiteralPath (Join-Path $backupDir "target-image.txt") -Value $targetImage -Encoding utf8
    } catch { throw "backup failed: $($_.Exception.Message)" }

    Invoke-DockerChecked ($composeTarget + @("up", "-d", $Service))
    if (-not (Wait-ForHealth $composeTarget $HealthTimeoutSeconds)) { throw "health check failed" }
    if (Test-Path -LiteralPath $persistentOverride -PathType Leaf) { Copy-Item -LiteralPath $persistentOverride -Destination $persistentOverrideBackup -Force }
    Copy-Item -LiteralPath $targetComposeOverride -Destination $persistentOverride -Force
    $persistentOverrideChanged = $true
    $persistentComposeFiles = @($composeFiles)
    $persistentFullPath = [System.IO.Path]::GetFullPath($persistentOverride)
    if (-not ($persistentComposeFiles | Where-Object { [System.IO.Path]::GetFullPath($_) -eq $persistentFullPath })) { $persistentComposeFiles += $persistentOverride }
    $composeValue = $persistentComposeFiles -join [System.IO.Path]::PathSeparator
    Persist-TargetEnvironment $EnvFile $TargetTag $composeValue
    $completed = $true
    $appStopped = $false
    Write-Host "[upgrade] completed successfully"
    Write-Host "[upgrade] keep $backupDir and $rollbackImage until the new version is verified"
} catch {
    if ($appStopped) {
        Write-Warning "[upgrade] $($_.Exception.Message); restoring $rollbackImage (was $oldImageRef)"
        try {
            if ($persistentOverrideChanged) {
                if (Test-Path -LiteralPath $persistentOverrideBackup -PathType Leaf) { Copy-Item -LiteralPath $persistentOverrideBackup -Destination $persistentOverride -Force } else { Remove-Item -LiteralPath $persistentOverride -Force -ErrorAction SilentlyContinue }
            }
            & docker @rollbackArgs up -d $Service *> $null
            if (-not (Wait-ForHealth $rollbackArgs 60)) { Write-Warning "[upgrade] rollback health check failed; backup: $backupDir" } else { Write-Host "[upgrade] rollback restored; backup: $backupDir" }
        } catch { Write-Warning "[upgrade] rollback failed; backup: $backupDir" }
    } else {
        Write-Warning "[upgrade] $($_.Exception.Message); the running service was left unchanged"
    }
    throw
} finally {
    if (Test-Path -LiteralPath $targetEnvFile) { Remove-Item -LiteralPath $targetEnvFile -Force -ErrorAction SilentlyContinue }
    if (Test-Path -LiteralPath $targetComposeOverride) { Remove-Item -LiteralPath $targetComposeOverride -Force -ErrorAction SilentlyContinue }
    [Environment]::SetEnvironmentVariable("IMAGE_TAG", $oldImageTag, "Process")
}
