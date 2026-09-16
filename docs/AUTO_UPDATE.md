# 网页一键升级与最近三个发版回退

在 **系统设置 → 应用信息** 点击“检查更新”，即可点击“升级到版本号”。“最近三个旧版本”提供当前运行版本之前、版本号最高的三个正式发行版；草稿和预发布版不参与。选中版本后点击“回退到所选版本”。顶部新版本提示也会进入此页面。

更新来源固定为 `AI8888-SHOP/upstream-ops` 的 GitHub Releases；Docker 镜像固定为 `ghcr.io/ai8888-shop/upstream-ops`。不接受网页传入的下载 URL、镜像仓库或任意命令。

## 先配置一次独立更新服务

应用停止后，需要另一个进程继续执行健康检查和恢复，所以更新服务必须独立于应用运行。后台登录鉴权必须开启；普通管理 API 的免登录模式不能执行程序安装。未配置更新服务时仍可检查版本，升级/回退按钮会说明缺少的条件。

### Docker Compose

先使用包含本功能的正式镜像更新现有应用一次，并将对应发版升级包中的 `docker-compose.updater.yml` 放到现有部署目录。保留原有 `.env`、数据库设置、数据卷和 Compose 叠加文件。

在现有 `.env` 中设置：

```dotenv
AUTH_ENABLED=true
ADMIN_USERNAME=admin
ADMIN_PASSWORD=你已设置的后台密码
UPDATER_PROJECT_DIR=/srv/upstream-ops
COMPOSE_FILE=docker-compose.yml:docker-compose.updater.yml
```

`UPDATER_PROJECT_DIR` 必须是宿主机部署目录的真实绝对路径，包含 `.env`、Compose 文件和 `data/`。如果已有 `COMPOSE_FILE`（例如 PostgreSQL 网络、迁移或自定义镜像叠加文件），**在原列表末尾追加 `:docker-compose.updater.yml`，不要替换原列表**。如果使用自定义 Compose 文件名，可设置 `UPDATER_COMPOSE_FILES` 为完整文件列表，用冒号分隔。

在该目录执行一次：

```bash
docker compose up -d app updater
```

更新服务通过共享的 `/app/data/.updater/control.sock` 接收应用请求，不开放额外网络端口。只有 updater 容器挂载 Docker socket；app 不需要 Docker socket。updater 同时挂载宿主机部署目录，并保留相同绝对路径，确保 Compose 不会创建错误的数据卷路径。应用和 updater 默认都以镜像的 root 用户运行；自定义 UID 部署需确保二者都可访问状态目录和 0600 socket。

updater 会读取现有 Compose 配置，仅重建 `app` 服务（`--no-deps`），不重启 PostgreSQL 或 updater 自身；目标服务名可通过 `UPDATER_SERVICE` 指定。它记录当前运行镜像的不可变 ID，并保留一个本地恢复标签。新镜像下载成功后，将所选镜像 ID 保存到 `docker-compose.web-update.yml`，并追加到 `.env` 的 `COMPOSE_FILE`。此后普通 `docker compose up` 会继续使用网页选择的版本；不要使用会忽略该叠加文件的手工 `-f` 列表。

支持单个应用容器。多副本、Swarm、Kubernetes 不通过此入口滚动更新。rootless Docker 可自行调整 updater 的 Docker socket 挂载。Docker 镜像拉取使用宿主机 Docker daemon 的网络配置；GitHub 发版查询可通过 `.env` 的 `UPDATER_HTTPS_PROXY` / `UPDATER_HTTP_PROXY` 配置 updater 的代理，独立于应用里的版本检查代理。

如果平时使用 `docker compose -p 自定义项目名`，应将同一个名称保存在 `.env` 的 `COMPOSE_PROJECT_NAME`，确保更新器定位同一套容器。自定义应用服务名时，还需同步调整模板中的 `app` 键和共享状态目录挂载。

### Linux 原生二进制 + systemd

原生网页更新目前支持 Linux amd64/arm64 的 systemd 部署。先安装包含本功能的应用版本，并从同一发版的 upgrade-kit 包取得 `scripts/upstream-ops-updater.service`。再复制一个固定的更新器程序，避免回退应用时把更新器也回退掉：

```bash
sudo install -d -m 0755 /usr/local/lib/upstream-ops
sudo install -m 0755 /usr/local/bin/upstream-ops /usr/local/lib/upstream-ops/updater
sudo install -m 0644 scripts/upstream-ops-updater.service /etc/systemd/system/upstream-ops-updater.service
```

创建 `/etc/upstream-ops-updater.env`，权限设为 0600，按实际部署填写：

```dotenv
UPDATER_MODE=systemd
UPDATER_STATE_DIR=/srv/upstream-ops/data/.updater
UPDATER_SOCKET=/srv/upstream-ops/data/.updater/control.sock
UPDATER_BINARY=/usr/local/bin/upstream-ops
UPDATER_SYSTEMD_UNIT=upstream-ops.service
UPDATER_HEALTH_URL=http://127.0.0.1:8418/healthz
UPDATER_HEALTH_TIMEOUT_SECONDS=180
```

`UPDATER_BINARY` 必须是 app 服务实际执行的普通二进制文件，不是符号链接。更新器服务需有替换该文件和管理对应 systemd 服务的权限。默认示例以 root 运行，适合 app 同样以 root 运行的部署；使用其他 OS 用户时，应配置同用户的受控权限和 socket 访问，不能把 socket 改成全员可写。

应用默认从配置文件旁的 `.updater/control.sock` 连接更新器。例如配置文件 `/srv/upstream-ops/data/config.yaml` 对应上面的 socket；其他位置可在 app 服务环境中设置同一个 `UPDATER_SOCKET`。两边状态目录必须持久化。

应用以独立用户运行、更新器以 root 运行时，可以给更新器配置 `UPDATER_SOCKET_GROUP=应用的专用用户组`，socket 使用 0660，仅该组可访问。建议把 socket 放在独立的 `/run/upstream-ops-updater/control.sock`，在更新器 systemd 单元中设置 `Group=应用的专用用户组`、`RuntimeDirectory=upstream-ops-updater` 和 `RuntimeDirectoryMode=0750`；在应用和更新器环境中设置同一个 `UPDATER_SOCKET`。持久状态目录仍由 root 以 0700 管理，无需向应用开放备份文件。

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now upstream-ops-updater.service
```

原生更新下载与主机架构匹配的 GitHub Release 压缩包，并校验 GitHub asset SHA256（旧资产没有 digest 时使用发版内 `SHA256SUMS`）。缺少校验信息、下载失败、摘要不一致或压缩包异常时，停止更新，保持原服务。程序文件使用同目录临时文件 + 原子重命名替换；原启动参数、环境和 systemd 单元保持不变。

替换程序和现有 `.env` 时保留文件属主。原生自动恢复还会清除目标 systemd 单元的失败启动限流，避免坏版本反复启动后，正确的旧程序也无法重启。

## 失败恢复与可观察状态

1. 记录持久任务并加独占锁；重复点击不会启动第二个升级。
2. 下载/校验目标版本，保留原程序或原镜像。准备失败不停止当前应用。
3. 将恢复点写入磁盘并同步，再切换应用。
4. 验证运行的程序 SHA256 / Docker 镜像 ID，检查 `/healthz` 的应用与数据库状态；新版本还验证返回的版本号。连续三次成功才完成。
5. 切换或健康检查失败，自动恢复原程序/镜像并重新检查。升级中 updater 中断，重启后读取持久记录执行恢复；恢复也失败时显示“恢复异常”，不宣称成功，也不继续接受新升级。

健康检查默认等待 180 秒，可用 `UPDATER_HEALTH_TIMEOUT_SECONDS` 调整为 30–1800 秒。应用重启期间网页会重连，后台任务不依赖浏览器保持打开。状态保存在 `.updater/status.json`；原生备份位于 `.updater/jobs/`，Docker 恢复镜像标签为 `upstream-ops-rollback:<任务ID>`。完成后保留当前恢复点和之前三个本地任务，其余由更新器清理。

回退到首次引入此功能之前的版本时，该版本自身没有网页更新 API。独立更新器仍会完成健康检查和失败恢复；旧页面恢复后如需再次升级，使用既有 `scripts/upgrade.sh`，或者先安装带更新入口的版本。更新器本身保持独立，不能把它的 systemd 单元或 Docker 服务设为应用更新目标。

查询任务和更新器日志：

```bash
# Docker 部署，在部署目录内
docker compose logs --tail=100 updater
docker compose exec updater cat /app/data/.updater/status.json

# 原生部署
sudo journalctl -u upstream-ops-updater.service -n 100 --no-pager
```

## 数据库与验证范围

自动回退恢复的是应用程序/镜像和 Compose 的版本选择，**不会还原 PostgreSQL 旧快照，也不会删除升级后的新记录**。它不能撤销不兼容的数据库迁移；发版应保持最近三个版本的读写兼容，包含破坏性 schema 变化的升级需单独规划数据迁移与备份。健康检查能发现启动/数据库连接问题，不能保证发现所有业务逻辑回归。

回归用例覆盖：正式版本筛选与三版范围、任意版本/下载地址拒绝、校验和与压缩包路径、下载失败不重启、恢复点先于切换落盘、升级并发互斥、健康失败回退、更新进程中断恢复、恢复失败状态，以及 Docker 配置持久化与数据库叠加文件保留。Go 测试和构建在 GitHub Actions 执行，本机不执行编译。
