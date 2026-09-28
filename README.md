# UpstreamOps

**多上游运维面板 · AI 请求网关 · 倍率与首字调度**

把 NewAPI / Sub2API 上游的账号、余额、倍率、API Key 和告警集中到一个面板，再通过统一网关向客户端提供服务。支持接入监控渠道或直连上游，在配置的费用与尝试预算内选择线路、处理故障切换，并记录每次请求的用量和调度依据。

[快速部署](#快速部署) · [网关接入](#网关接入) · [调度与故障处理](#调度与故障处理) · [详细手册](README.zh.md) · [下载发行版](https://github.com/AI8888-SHOP/upstream-ops/releases) · [反馈问题](https://github.com/AI8888-SHOP/upstream-ops/issues)

## 赞助商

<table>
<tr>
<td width="200" align="center">
<a href="https://www.ai8888.shop"><img src="docs/images/ai8888-shop-logo.png" alt="ai8888.shop Logo" width="180"></a>
</td>
<td>
<strong><a href="https://www.ai8888.shop">ai8888.shop（低价稳定token）</a></strong><br>
感谢 ai8888.shop 对本项目的支持。点击名称或 Logo 访问网站。
</td>
</tr>
</table>

## 可以做什么

| 场景 | 能力 |
| --- | --- |
| 集中管理上游 | 接入 NewAPI / Sub2API，查看余额、消费、分组倍率、公告与订阅状态，管理上游 API Key、充值和兑换。 |
| 统一请求入口 | 创建网关分组和密钥，接入监控渠道或直连 Provider，配置模型映射、模型列表、代理与鉴权方式。 |
| 控制成本与等待 | 提供倍率优先、均衡、首字优先策略，结合近期首字、失败情况与负载选路，遵守配置的倍率上限。 |
| 处理上游故障 | 支持重试、故障顺延、冷却、首字等待预算，以及可选的响应校验和并发兜底。 |
| 回看请求过程 | 记录请求与尝试耗时、Token、缓存、费用、响应模型、错误详情、胜出线路和调度依据。 |
| 同步与告警 | 向 Sub2API 同步上游账号与分组；通过 Telegram、Webhook、邮件、企业微信、钉钉、飞书、ServerChan3 推送通知。 |

上游账号操作取决于对应站点开放的 API；网关兼容性也取决于上游模型、协议和功能支持。

## 界面预览

![UpstreamOps 界面预览](docs/images/demo1.png)

<details>
<summary>展开更多截图</summary>

![UpstreamOps 界面预览 2](docs/images/demo2.png)
![UpstreamOps 界面预览 3](docs/images/demo3.png)
![UpstreamOps 界面预览 4](docs/images/demo4.png)
![UpstreamOps 界面预览 5](docs/images/demo5.png)
![UpstreamOps 界面预览 6](docs/images/demo6.png)
![UpstreamOps 界面预览 7](docs/images/demo7.png)

</details>

## 快速部署

### 准备环境

- Docker Engine 与 Docker Compose v2；也可使用下文的预编译二进制。
- 一个可连接的 **PostgreSQL 数据库**和专用账号。默认 Compose 只启动应用，不会创建数据库。
- 一份妥善保存的 `APP_SECRET`，用于加密上游凭据等敏感字段。

**当前服务端只支持 PostgreSQL。** SQLite / MySQL 旧部署应先完成数据迁移；仅修改数据库驱动或替换镜像不会迁移数据。

### 使用 Docker Compose

获取仓库并复制配置模板：

```bash
git clone https://github.com/AI8888-SHOP/upstream-ops.git
cd upstream-ops
cp .env.example .env
```

生成主密钥，将输出填入 `.env` 的 `APP_SECRET`：

```bash
openssl rand -hex 32
```

编辑 `.env`，替换以下占位值：

```dotenv
HTTP_PORT=8418
IMAGE_REPOSITORY=ghcr.io/ai8888-shop/upstream-ops
IMAGE_TAG=latest

APP_SECRET=替换为上一步生成的随机字符串

DATABASE_DRIVER=postgres
DATABASE_HOST=替换为应用容器可访问的数据库地址
DATABASE_PORT=5432
DATABASE_USER=upstreamops
DATABASE_PASSWORD=替换为数据库密码
DATABASE_NAME=upstreamops
DATABASE_SSL_MODE=require

AUTH_ENABLED=true
ADMIN_USERNAME=admin
ADMIN_PASSWORD=替换为强密码
```

`DATABASE_SSL_MODE` 应与数据库的 TLS 配置一致。对未启用 TLS 的可信内网数据库，可设为 `disable`。容器内的 `localhost` 指向应用容器自身；连接已有 PostgreSQL 容器时，应用需要加入它所在的 Docker 网络，并使用该网络内可解析的数据库服务名：

```dotenv
DATABASE_NETWORK_NAME=替换为已有数据库网络名
DATABASE_NETWORK_EXTERNAL=true
```

启动并检查：

```bash
docker compose pull
docker compose up -d
docker compose ps
curl -fsS http://localhost:8418/healthz
```

浏览器打开 `http://localhost:8418`，使用上面设置的后台账号登录。排查启动问题时运行：

```bash
docker compose logs --tail=100 app
```

默认镜像来自 GHCR，包含前端页面，无需本机编译。生产环境可将 `IMAGE_TAG` 固定为 [Releases](https://github.com/AI8888-SHOP/upstream-ops/releases) 中已发布的具体版本。

### 使用预编译二进制

从 [Releases](https://github.com/AI8888-SHOP/upstream-ops/releases) 下载对应系统与架构的安装包，并按同一发行版的 `SHA256SUMS` 校验。发行包覆盖 Linux、Windows、macOS 和 FreeBSD 的支持架构，包含主程序和迁移工具；可用文件以该发行版附件为准。

解压后，通过进程环境变量或 `config.yaml` 设置前面的 PostgreSQL、主密钥与后台登录参数，再启动：

```bash
./upstream-ops -config ./data/config.yaml
```

Windows 使用 `upstream-ops.exe`。原生程序不会自动加载 Docker Compose 的 `.env`；请由 shell 或服务管理器注入环境变量，或使用 YAML 配置。配置字段见 [详细手册](README.zh.md)。

### 数据与配置

| 位置或变量 | 用途 |
| --- | --- |
| PostgreSQL | 保存业务数据、网关使用记录、配额和结算信息。 |
| `data/config.yaml` | Docker 部署中持久化到宿主机的应用配置文件。 |
| `.env` | Compose 的部署参数，包含数据库连接、主密钥和后台登录设置。 |
| `APP_SECRET` | 解密已保存的敏感字段；升级和迁移时必须保持一致。 |
| `AUTH_ENABLED` | 控制后台登录；模板默认关闭，公网部署应按上面的示例开启。 |

备份时同时保留数据库、`.env` 和 `data/`。只备份 `data/` 不包含 PostgreSQL 业务数据；更换 `APP_SECRET` 会导致既有加密凭据无法解密。公网访问建议通过 HTTPS 反向代理接入。

## 网关接入

### 在后台完成配置

1. **添加来源**：在上游管理中添加 NewAPI / Sub2API 渠道，或在请求网关中添加直连 Provider。
2. **创建网关分组**：添加路由，选择源分组、设置有效计费倍率、模型映射和模型列表。
3. **检查上游密钥与模型**：监控渠道可创建或复用上游专用密钥；使用模型同步、预览和探测确认路线可用。
4. **设置调度与预算**：按业务选择策略，设置倍率上限、重试、顺延、总尝试次数和首字等待时间。
5. **创建网关 API Key**：绑定对应分组，将网关地址和密钥填入客户端。

后台登录凭据、上游 API Key、客户端使用的网关 API Key 各有用途。客户端应使用**网关 API Key**，不能用后台登录 Token 代替。

### 常用接口

| 接口 | 用途 |
| --- | --- |
| `GET /v1/models` | 获取当前网关密钥可见的模型列表。 |
| `POST /v1/chat/completions` | OpenAI Chat 兼容请求。 |
| `POST /v1/responses` | OpenAI Responses 兼容请求，支持流式处理。 |
| `POST /v1/messages` | Anthropic Messages 兼容请求。 |
| `POST /v1/messages/count_tokens` | Anthropic Token 计数接口。 |
| `GET /v1/usage` | 查询当前网关密钥的用量信息。 |

常用鉴权方式为 `Authorization: Bearer <网关密钥>` 或 `x-api-key: <网关密钥>`。其他兼容路径和管理接口见 [网关详细说明](README.zh.md#请求网关使用说明)。

以下示例假设已在当前 shell 设置 `GATEWAY_API_KEY` 和 `MODEL_NAME`；模型名应取自当前分组的模型列表：

```bash
curl http://localhost:8418/v1/models \
  -H "Authorization: Bearer ${GATEWAY_API_KEY}"

curl http://localhost:8418/v1/chat/completions \
  -H "Authorization: Bearer ${GATEWAY_API_KEY}" \
  -H 'Content-Type: application/json' \
  -d "{\"model\":\"${MODEL_NAME}\",\"messages\":[{\"role\":\"user\",\"content\":\"你好\"}],\"stream\":false}"
```

网关支持 OpenAI Chat、OpenAI Responses、Anthropic Messages 之间的 JSON 与 SSE 协议转换。图片、视频、Embedding 等接口按相应兼容路径转发，具体功能仍需上游支持；协议转换不意味着所有模型能力完全等价。

## 调度与故障处理

### 三种选路策略

| 策略 | 适用方式 |
| --- | --- |
| 倍率优先 `cost` | 按既有倍率顺序、权重与会话亲和规则调度。 |
| 均衡 `balanced` | 优先选择达到首字目标、失败率较低的便宜来源；没有达标来源时按综合等待评分选择。 |
| 首字优先 `latency` | 在允许的倍率范围内，结合近期首字均值、近似 P90、失败率、失败等待与当前负载选择线路。 |

动态策略目前用于**流式文本请求**；非流式、图片、视频和实时连接沿用原调度方式。策略受分组最大计费倍率和动态策略的允许溢价约束，候选失败后不会自动突破本次倍率预算。

速度统计在进程内维护，重启后重新学习，多实例间不共享。使用记录中的“调度依据”可以查看当时的样本数、统计窗口、倍率、失败情况和负载。配置与统计口径见 [首字调度说明](docs/scheduler-ttft.md)。

### 故障切换与次数预算

- 网络错误、429、5xx，以及可识别的模型不存在、上游凭据失效或余额不足错误，可按配置重试或切换来源。普通参数错误默认不会向所有上游重复发送。
- 启用普通顺延且存在其他候选时，优先尝试未使用的来源，减少反复请求同一坏源。
- **总尝试次数包含首次请求、重试、顺延和并发兜底。** 总上限设为 2 时，即使允许顺延 8 次，也最多发起 2 次尝试。
- 单次首字超时用于快速换源；整个请求的首字总预算还包含入口准备、排队、失败尝试与校验等待。最后一个候选同样受总预算约束。
- 所有候选不可用、额度不足或预算耗尽时，网关会返回失败。路由只能利用已有可用来源，不能保证所有请求成功。

### 响应校验与并发兜底

按需启用响应内容规则、响应模型核查和零用量校验，识别不符合分组要求的返回。流式内容校验使用提交前的前缀缓冲；有效内容已经交付后，不能再无缝替换成另一条回答。

并发兜底（Hedging）默认关闭：主请求超过配置等待时间后启动备选，首个通过校验的结果胜出，取消仍在运行的其他尝试。图片、视频和实时请求不参与这类竞速。**取消请求不保证上游不计费**，开启前应结合总尝试次数、并发上限和使用记录评估额外成本。

可选的并发虚拟缓存折扣属于网关计费策略，不代表上游发生了真实缓存命中；使用记录保留各次尝试的原始费用和额外成本，便于核对。

## 使用记录与排障

遇到慢请求或 502 / 503 时，先按请求 ID 查看完整尝试链：哪些来源被尝试、是否切换、何时失败、哪个结果最终交付，再结合分组的次数、倍率和时间预算判断原因。

- **用户等待**：优先查看请求级首字时间，它包含前面失败与换源消耗的时间；单次尝试首字只衡量该次尝试。
- **失败原因**：查看上游状态码、错误详情、响应模型、校验规则与冷却状态。
- **实际费用**：区分最终交付费用、各次上游尝试费用、真实缓存与虚拟缓存折扣。
- **历史记录**：没有请求级耗时字段的旧记录仍使用尝试口径，不宜直接混算为用户首字体验。

健康检查使用 `/healthz`，会同时检查数据库连接；版本信息使用 `/api/version`，启用后台登录时需携带管理端凭据。健康检查通过不代表每条上游线路或每个模型都可用。

## 升级与回退

升级前备份 PostgreSQL、`.env` 和 `data/`，保留原 `APP_SECRET`，并核对目标版本的发行说明。旧 SQLite 部署可使用仓库的迁移工具迁往空 PostgreSQL 数据库；旧 MySQL 部署也需先完成到 PostgreSQL 的数据迁移，不能直接套用 SQLite 迁移脚本。

- **已有 PostgreSQL 的 Docker 部署**：使用 [升级脚本说明](docs/UPGRADE.md)，或按现有 Compose 配置升级镜像。文档中的旧版本号是示例，目标应选用实际发布的版本。
- **网页升级与回退**：先启用后台登录并配置独立更新服务，支持 Docker Compose 与 Linux systemd 部署，见 [自动升级说明](docs/AUTO_UPDATE.md)。
- **旧 SQLite 部署**：参阅 [迁移步骤](docs/UPGRADE.md#3-sqlite-to-postgresql-migration)，迁移后验证健康检查、网关请求、用量与结算。

升级工具的 `data/` 备份和旧镜像恢复不能替代 PostgreSQL 数据库备份。回退程序也不等于回退数据库结构与数据，回退前应核对版本兼容性。

## 文档与项目结构

| 入口 | 内容 |
| --- | --- |
| [中文详细手册](README.zh.md) | 完整配置、通知渠道、订阅、上游同步、管理 API 和常见问题。 |
| [首字调度说明](docs/scheduler-ttft.md) | 动态选路、请求预算、统计口径与边界。 |
| [升级指南](docs/UPGRADE.md) | Docker 升级、SQLite 迁移、备份与恢复。 |
| [网页升级与回退](docs/AUTO_UPDATE.md) | 独立更新服务的 Docker / systemd 配置。 |
| [backend/](backend) | 网关转发、上游连接、监控、通知与数据存储。 |
| [frontend/](frontend) | 管理界面。 |
| [cmd/](cmd) | 服务和迁移工具入口。 |
| [scripts/](scripts) | 升级、迁移与运维脚本。 |

## 致谢与许可

本项目基于 [worryzyy/upstream-hub](https://github.com/worryzyy/upstream-hub) 二次开发，感谢原作者 [@worryzyy](https://github.com/worryzyy) 的开源工作。

许可证：MIT。
