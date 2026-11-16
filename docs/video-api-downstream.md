# upstream-ops 视频生成 API：下游接入说明

本文约定：下游程序自行上传参考素材并取得可访问的文件直链；使用 upstream-ops 网关密钥，向网关提交 JSON 视频任务；网关转发给提供原生 `/v1/videos` 接口的 v6k 渠道。

**适用版本：v0.0.80 及以后。支持 JSON 创建、素材 URL 透传、任务绑定及查询，以及原生任务鉴权下载和 HEAD。完整流程需在服务端部署对应版本后验收；尚未完成 v6k 实际付费生成测试。**

## 1. 接入信息与流程

服务提供方需要交付以下信息：

| 配置 | 说明 |
| --- | --- |
| `GATEWAY_URL` | upstream-ops 对外地址，如 `https://gateway.example.com`，本文示例不含 `/v1` |
| `GATEWAY_KEY` | upstream-ops 签发的网关密钥，绑定已配置 v6k 渠道的网关组 |
| `VIDEO_MODEL` | 服务提供方确认可用的视频模型 ID 或网关公开模型别名 |
| 模型能力 | 允许的时长、分辨率、比例、参考素材类型及数量 |

所有接口都访问 `GATEWAY_URL`，使用网关密钥。下游不需要持有 v6k 的上游密钥。

```text
下游上传参考文件（可选）→ 得到素材直链
  → POST /v1/videos → 保存任务 id
  → GET /v1/videos/{id} → 等待 completed
  → GET /v1/videos/{id}/content → 保存视频文件
```

生成是异步任务。创建接口返回表示任务已受理，不表示视频已经生成成功。此流程使用原生 `id` 和 `status: "completed"`，不使用其他视频适配协议的 `request_id`、`status: "done"` 或 `video.url` 成功判据。

## 2. 鉴权与模型

请求头：

```http
Authorization: Bearer YOUR_GATEWAY_KEY
Content-Type: application/json
```

`Content-Type` 用于创建请求；GET、HEAD 无须请求体。密钥放在下游服务端保存，用户浏览器通过下游后端访问生成和下载接口。

可使用以下接口查看网关公开的模型列表：

```bash
curl --fail-with-body "$GATEWAY_URL/v1/models" \
  -H "Authorization: Bearer $GATEWAY_KEY"
```

该列表可能混合文本、图片、视频模型，不能仅凭出现在列表中就认定支持视频。视频模型及能力应由服务提供方确认；存在模型映射时，提交网关公开的模型别名。

## 3. 创建视频任务

```http
POST /v1/videos
```

请求参数：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `model` | string | 是 | 可用的视频模型 ID 或公开别名 |
| `prompt` | string | 是 | 视频生成提示词 |
| `seconds` | integer / 数字字符串 | 否 | 时长，单位秒；允许值、默认值按模型确认 |
| `duration` | integer / 数字字符串 | 否 | v6k 的 `seconds` 别名；建议只传一种，同时传必须相同 |
| `aspect_ratio` | string | 否 | 如 `16:9`、`9:16`；支持值按模型确认 |
| `ratio` | string | 否 | 比例字段的另一种写法，建议与 `aspect_ratio` 二选一 |
| `resolution` | string | 否 | 如 `720p`、`1080p`；部分模型支持 `480p`、`768p`，不代表所有模型都支持 |
| `size` | string | 否 | 模型支持的尺寸字符串；只在该模型明确支持时使用 |
| `images` | array | 否 | 参考图片直链数组，或含 `url` 的对象数组 |
| `videos` | array | 否 | 参考视频直链数组，或含 `url` 的对象数组 |
| `audios` | array | 否 | 参考音频直链数组，或含 `url` 的对象数组 |
| `input_reference` | string / object | 否 | 单张输入图、首帧的 URL 或 URL 对象，模型须支持首帧能力 |
| `omni_reference_task_type` | string | 否 | 特定模型支持 `reference`、`edit`、`extend`；普通生成无需传 |

不要设置 `stream: true`，这是异步 JSON 接口。本文统一使用 JSON 与素材直链，下游无需调用网关素材上传接口。

模型可能不支持某些可选参数或组合，不能把所有字段默认全部填上。例如，不应假设任意模型同时支持图片、视频和音频参考。`seconds: -1` 仅用于明确支持自动时长的模型；编辑用途需按模型契约提交一段参考视频，并避免同时提交固定比例、尺寸等冲突字段。没有确认能力时使用正数时长和基础参数。

### 文生视频

以下为可直接替换参数的请求示例。`YOUR_VIDEO_MODEL` 必须替换成服务提供方给出的模型名。

```bash
curl --fail-with-body --request POST "$GATEWAY_URL/v1/videos" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Content-Type: application/json' \
  --data-raw '{
    "model": "YOUR_VIDEO_MODEL",
    "prompt": "一只小猫在阳光下的草地上奔跑，镜头缓慢跟随",
    "seconds": 5,
    "aspect_ratio": "16:9",
    "resolution": "720p"
  }'
```

### 使用参考素材

对于支持参考图的模型，在上述请求中加入：

```json
{
  "images": ["https://files.example.com/reference.png"]
}
```

对于明确支持多种参考素材的模型，可使用以下字段组合；这些片段须与 `model`、`prompt` 等基础字段合并：

```json
{
  "images": ["https://files.example.com/reference.png"],
  "videos": ["https://files.example.com/reference.mp4"],
  "audios": ["https://files.example.com/reference.mp3"]
}
```

只有一个素材也使用数组。单张首帧按模型约定使用 `input_reference`，不要自动认为它与 `images` 含义相同。URL 对象及扩展字段会原样转发，但接收这些字段不代表模型支持相应能力。

### 创建响应

成功响应按上游返回，客户端应接受 2xx，并解析 JSON。典型示例：

```json
{
  "id": "task_example",
  "object": "video",
  "model": "YOUR_VIDEO_MODEL",
  "status": "queued",
  "progress": 0,
  "created_at": 1790467200
}
```

立刻持久化 `id`，同时保存本地业务单号、请求参数、创建时间以及所用网关密钥的内部标识。后续查询和下载使用创建时同一把网关密钥。

`id` 是视频任务 ID。响应头 `X-Upstream-Ops-Request-Id` 是网关诊断 ID，两者用途不同；建议都记录，不能拿诊断 ID 查询视频任务。

HTTP 成功但缺少任务 ID 时，不要直接标记生成成功或无条件重新提交；记录完整响应并交给服务提供方核查是否已受理。

## 4. 查询任务

```http
GET /v1/videos/{id}
```

```bash
curl --fail-with-body "$GATEWAY_URL/v1/videos/$VIDEO_TASK_ID" \
  -H "Authorization: Bearer $GATEWAY_KEY"
```

建议从每 5 秒轮询一次开始，长任务可逐步放缓至每 10–15 秒一次并加入少量随机间隔；这是客户端建议，不是接口承诺的限流阈值。若响应提供 `poll_after_ms` 或 `poll_interval_ms`，优先遵守该提示。

状态处理：

| `status` | 含义 | 下游处理 |
| --- | --- | --- |
| `queued` | 排队 | 保留任务，稍后查询 |
| `in_progress` | 生成中 | 继续查询，可展示 `progress` |
| `completed` | 已完成 | 停止生成状态轮询，下载结果 |
| `failed` | 生成失败 | 停止轮询，记录任务错误，不自动重复创建付费任务 |
| `unknown` | 状态暂不明确 | 不当作成功或直接重建；有限次数延后查询，持续异常转人工处理 |

状态查询 HTTP 200 不等于生成成功，必须读取 `status`。对于未知状态值，保留原响应并按状态未知处理，避免程序崩溃或无限高频轮询。

完成响应示例：

```json
{
  "id": "task_example",
  "object": "video",
  "model": "YOUR_VIDEO_MODEL",
  "status": "completed",
  "progress": 100,
  "created_at": 1790467200,
  "completed_at": 1790467800
}
```

**完成响应可以没有 `url`、`video` 或实际时长字段。** 使用固定下载接口，不要因为缺少 `video.url` 把已完成任务判断为失败。若包含根级 `url`，也不能假设该地址公开可读。

查询绑定有效期为创建后 7 天，超过有效期返回 404。这是网关的任务定位期限，不保证上游视频文件也保存 7 天；完成后应及时下载到下游自己的存储。

## 5. 下载视频（需要部署本次原生下载修复）

```http
GET /v1/videos/{id}/content
Authorization: Bearer YOUR_GATEWAY_KEY
```

```bash
curl --fail \
  "$GATEWAY_URL/v1/videos/$VIDEO_TASK_ID/content" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  --output result.mp4
```

成功返回视频二进制，通常为 `Content-Type: video/mp4`，也可能为 `application/octet-stream`。不要按 JSON 解析，不要把失败响应保存为成功视频。下游后端可将响应流式写入文件或对象存储，再提供自己的播放地址给浏览器。

查询响应头：

```bash
curl --fail --head "$GATEWAY_URL/v1/videos/$VIDEO_TASK_ID/content" \
  -H "Authorization: Bearer $GATEWAY_KEY"
```

读取部分字节：

```bash
curl --fail "$GATEWAY_URL/v1/videos/$VIDEO_TASK_ID/content" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -H 'Range: bytes=0-1048575' \
  --output first-part.bin
```

支持分段时返回 `206 Partial Content` 与 `Content-Range`。也可能因上游不支持而返回 200 全文件，下游必须检查状态码后再决定覆盖还是拼接；416 表示范围不可用。需要条件续传时可携带 `If-Range` 与之前得到的 ETag 或 Last-Modified。

## 6. 素材直链要求

下游自行上传到对象存储、CDN 或其他素材服务即可，upstream-ops 只转发这些 URL，不下载或重新上传参考素材。

- 使用直接返回文件的 HTTP(S) URL，优先 HTTPS。
- URL 应能被 v6k 及其实际生成服务访问，不能依赖浏览器登录、Cookie 或额外请求头。
- 带签名参数的临时直链可以使用，需完整保留签名参数，并让有效期覆盖排队、素材读取和生成所需时间。
- 不使用本地路径、内网地址、浏览器 `blob:` 地址或网页分享页。
- 本接入方式不使用裸 Base64 视频或音频；先在下游上传并转换成文件直链。
- 素材格式、数量、文件大小、时长和内容要求由具体模型决定，传入 URL 本身不保证素材被模型接受。

下游自己的存储有效期由下游管理，不受 v6k 素材上传服务的 24 小时有效期约束。

## 7. 错误、超时与重试

网关错误通常是 JSON 错误信封，示例：

```json
{
  "error": {
    "type": "upstream_error",
    "message": "original video upstream could not return the task"
  },
  "upstream_ops_request_id": "example_diagnostic_id"
}
```

下游应结合 HTTP 状态码、`error`、任务 `status` 和诊断 ID 判断。错误消息可变化，不建议写死字符串作为业务判断条件。主要处理规则：

| 场景 | 处理 |
| --- | --- |
| 创建成功且有任务 ID | 保存 ID，进入轮询；不再次提交同一任务 |
| 创建请求超时、断连或 5xx 且没有拿到 ID | 任务可能已被上游受理；保存诊断信息核查，不自动无条件重发 POST |
| 查询或下载短暂超时、502/503/504 | 保留原 ID，退避重试 GET，不能转成重新创建 |
| 鉴权失败、网关密钥或组禁用 | 检查密钥与服务配置，停止高频重试 |
| 查询/下载 404 | 检查任务 ID、是否使用原密钥、绑定是否过期及上游是否仍保留任务 |
| 下载 409 | 结果尚不可获取；检查任务状态后再试 |
| 下载 410 | 结果已不可用，联系服务提供方确认；不要自动生成第二个任务 |
| 状态 `failed` | 本次任务失败；再次生成是新的业务操作，应明确展示给用户 |

可选发送 `Idempotency-Key`，同一业务的重试使用相同值。upstream-ops 会透传该请求头，但是否真正去重取决于上游，而且不同上游账户之间不共享幂等记录；这不是跨源“绝不重复扣费”的保证。

接口请求超时与视频生成超时是两件事：创建接口只等待任务受理；视频生成靠轮询等待。下游可以设置总等待上限，到达上限后标记“仍在处理/需稍后查询”，保存任务 ID，避免因页面超时重建任务。

## 8. 计费与验收

视频响应通常不包含文本 Token 用量。不得因为输入、输出 Token 为 0 就认定视频失败。查询、HEAD 和下载不会新增网关生成用量或再次按生成请求扣费。

本次原生接入不新增网关按秒、分辨率定价、完成后结算或自动退款。下游若需要计费，应使用双方确认的价格规则；实际视频时长未返回时，不要将申请时长冒充实测时长。

上线前至少验收以下场景：

1. 文生视频：创建返回 ID，查询到 completed，下载文件可以播放。
2. 参考素材：分别测试该模型支持的图片、视频、音频 URL；无需为了验收而把不支持的类型一起传入。
3. 完成响应不带 URL：仍能通过固定 `/content` 下载。
4. 失败任务与查询超时：停止或退避轮询，不重复创建。
5. 原密钥可以查询下载，另一把密钥访问相同任务返回 404。
6. 下游进程重启后恢复任务轮询；已完成视频及时转存到下游自己的存储。
7. 如播放器需要分段加载，测试 HEAD、Range 以及上游返回全文件 200 时的处理。

排查问题时请提供：本地业务单号、视频任务 ID、`X-Upstream-Ops-Request-Id`、请求时间、模型、HTTP 状态码和脱敏响应。不要把 API Key 或完整带签名的素材链接写入公开日志。
