# 视频任务绑定

支持的异步提交路径为 `POST /v1/videos/generations`、`POST /v1/videos/edits`、`POST /v1/videos/extensions`、`POST /v1/videos`，以及不带 `/v1` 的对应别名。默认透传相同的视频协议；直连渠道可单独启用下述 Image2Pro 兼容转换。

## upstream-ops → v6k 原生接口

此流程使用 v6k 的原生协议，入站直接调用 `POST /v1/videos`。直连渠道 Base URL 填 `https://www.v6k.top`（不加 `/v1`），填写该站 API Key，鉴权选 Bearer，「上游视频接口」保持「原协议透传」。把渠道加入网关组，按需设置支持模型和路由模型映射；模型 ID 必须是该上游密钥可用的模型。

| 操作 | 调用本程序的接口 | 行为 |
| --- | --- | --- |
| 上传参考素材（可选） | `POST /v1/media` | 单个原始文件使用 multipart 字段 `file`，返回顶层 `url`、`expires_at` |
| 创建 | `POST /v1/videos` | JSON、URL-encoded 表单和 multipart 表单；保留原生任务响应并保存绑定 |
| 查询 | `GET /v1/videos/{id}` | 固定原渠道及创建时的密钥，原样返回 `queued / in_progress / completed / failed / unknown` 等任务状态 |
| 下载 | `GET /v1/videos/{id}/content` | 使用绑定密钥访问原上游的同一路径，流式返回文件，支持 Range、If-Range |
| 文件响应头 | `HEAD /v1/videos/{id}/content` | 返回上游文件响应头、状态码，不下载响应正文 |

上述所有请求都使用本程序的网关密钥；查询和下载必须使用创建任务的同一把网关密钥。任务完成响应允许没有 `url` 或时长，此时直接调用固定的 `/content` 接口获取文件，无须重复创建。HEAD 和 GET 下载都不消耗第二次生成额度，创建后额度耗尽仍可读取自己的任务。任务等待、失败、过期或下载失败时不会换渠道重新生成。

JSON 直接使用 v6k 字段，例如：

```json
{
  "model": "上游支持的模型ID",
  "prompt": "一只小猫在草地上奔跑",
  "seconds": 5,
  "aspect_ratio": "16:9",
  "resolution": "720p",
  "images": ["https://example.com/reference.png"]
}
```

`duration` 是该站 `seconds` 的别名，两者同时填写需相同。`images`、`videos`、`audios`、`input_reference`、编辑/续写用途等字段按其模型文档填写并原样传递；不根据模型名猜测能力。原生 multipart 文件和字段保留，模型映射同时支持 JSON、URL-encoded 和 multipart，表单模型也遵守渠道支持模型限制。创建请求不能开启 `stream`。网关视频创建和素材上传请求体上限为 256 MiB，上游或前置反向代理可有更低限制；大文件推荐先走素材上传再使用 URL。

参考素材上传示例（请求本程序，非直接请求 v6k）：

```bash
curl "$GATEWAY_URL/v1/media" \
  -H "Authorization: Bearer $GATEWAY_KEY" \
  -F 'file=@reference.mp4'
```

网关组只有一个可用渠道时自动使用它；存在多个可用渠道时，上传 URL 必须加 `?route_id=该网关组内的路由ID`，防止无模型的上传被发到不确定的站点。该 ID 是网关组的路由 ID，不是直连渠道 ID。上传不跨渠道重试、不产生视频生成用量和按次扣费记录，HTTP 200 错误信封及原始报文/错误消息拒绝规则仍有效。文件暂存到权限为 0600 的临时文件，结束即删除，不在内存中缓存整个上传文件。

使用上传返回的顶层 `url` 填入 `images`、`videos` 或 `audios` 数组；v6k 文档标明素材链接公开可读、保留 24 小时，过期前应提交任务。已有公开素材直链可跳过上传。此功能接收原始文件，不会把 JSON 里的 Base64 视频自动上传或转换成链接。

原生文件下载使用创建时绑定的地址和鉴权，并遵守当前渠道的代理与并发设置；任务归属校验通过前不会访问上游。若下载接口重定向到 CDN，则用不携带任何密钥、Cookie、额外鉴权头的独立客户端访问公网 HTTP(S) 地址；这部分 CDN 下载不继承渠道代理，禁止内网地址和非 80/443 端口。原上游的错误正文不作为视频透传。

本节覆盖 upstream-ops 与 v6k 的原生接口链路，不包含 sub2api `/v1/videos/generations` 到 v6k 的字段及响应转换。现有 Image2Pro 选项仍按下一节的协议工作，不应用于 v6k 的 sub2api 转换。也不新增视频按秒、分辨率计价或完成结算。

## sub2api → Image2Pro 视频适配

在 **直连渠道 → 编辑 → 上游视频接口** 选择 **POST /v1/videos（Image2Pro 兼容）**。管理 API 字段为 `video_upstream_mode: "videos"`；默认 `passthrough`。Base URL 填 `https://api.image2pro.top`，无需加 `/v1`。在路由模型映射中，将下游模型映射为该站模型列表的实际 ID。

| 步骤 | sub2api / 下游 | 上游 |
| --- | --- | --- |
| 创建 | `POST /v1/videos/generations` | `POST /v1/videos` |
| 创建响应 | `{"request_id":"任务ID"}` | `{"id":"任务ID","status":"queued",...}` |
| 查询 | `GET /v1/videos/{request_id}` | 固定原上游 `GET /v1/videos/{id}` |
| 等待 | `status: "pending"` | `queued / processing / in_progress / running / pending` |
| 完成 | `status: "done"` 和受保护的 `video.url: "/v1/videos/{id}/content"` | `completed / succeeded / success / done`，原视频地址取 `video.url`、`url`、`metadata.url` 或 `output[0].url` |
| 失败 | `failed / expired`，保留错误信息 | `failed / error / canceled / cancelled / expired` |
| 下载 | `GET /v1/videos/{id}/content`，携带创建时的网关密钥 | 查询原任务取得最新视频地址，再流式下载文件，支持 Range |

只有明确完成并且存在有效 HTTP(S) 视频地址才返回 `done`，以满足 sub2api 的完成及计费判断。无地址的完成响应返回查询错误，不会重新创建视频。返回模型使用创建时的下游模型名，避免上游模型别名改变下游计价；时长优先使用完成响应，其次使用提交时保存的 `duration / seconds`，两者都缺失时不虚构时长或审核结果。`respect_moderation` 只在上游明确提供时保留。

适配保留映射后的 `model`、提示词、时长、比例、分辨率和扩展字段，将 xAI 的 `image: {url: ...}` 转为 `input_image`，将 `images` 中的 URL 对象转为 URL 字符串。Veo / Omni 等模型特有的 `reference_images`（包括首尾帧角色）等字段原样保留，需要调用方按上游模型能力填写；此选项不自动推断各模型能力，也不把视频编辑或续写接口改成视频创建。

视频请求透传 `Idempotency-Key` 和 `X-Xiaojuzi-Media-Contract` 请求头。幂等编号应在客户端的同一业务重试中保持不变；不同上游账户之间的幂等记录不共享，不能保证跨源提交绝不重复生成。已有任务 ID 后始终只查原任务。

响应格式和请求时长随任务一起持久化，后续修改渠道地址、密钥、视频接口选项，甚至删除路由、重启程序，都不会改变已有任务的查询目标及响应格式。直接入站 `POST /v1/videos` 的客户端仍得到原生响应；仅 `/v1/videos/generations` 在选中适配的渠道上转换。所有视频入口均禁止并行抢跑创建。

sub2api 的视频下载处理会拒绝非 xAI 的任意 CDN 地址，因此适配后的 `video.url` 返回本程序的受保护下载路径。下载与查询执行同样的任务归属校验，不重复计费；任务未完成时返回 409。文件流式转发，不把整个视频读入内存，不向 CDN 传递上游 API Key 或客户端凭据。文件下载直连公网的 HTTP(S) 80/443 端口，不继承渠道代理；DNS 解析和重定向均限制为公网地址。原任务 API 查询仍使用渠道配置的代理。视频链接需要上游提供可直接下载的地址，依赖额外鉴权的 CDN 不在本次适配范围内。

此适配覆盖视频创建、状态查询及响应转换，不包含异步图片、参考音视频文件上传、视频编辑/续写协议转换，也不新增视频定价或退款结算。sub2api 可按其自身配置识别并结算完成的视频；本程序的账单仍按下文规则处理。

## 提交与查询

1. 提交请求正常选路，成功响应中的根级 `request_id`、`task_id`、`id`（或 `data` 对象中的对应字段）保存为任务绑定。
2. 绑定保存成功后才向客户端返回任务 ID。数据库写入失败时终止本次提交，不因保存失败自动重试创建另一个付费任务。
3. 使用同一把网关密钥调用 `GET /v1/videos/{request_id}`。查询使用创建时的地址、API Key、鉴权方式和额外请求头，不重新选路，不跨站查询。创建请求无需设置 `stream`；设置为 `true` 会在发送上游前拒绝。

绑定存放在 `gateway_video_task_bindings`，凭据快照通过 `APP_SECRET` 加密，所有实例共享数据库即可查询。有效期为创建后 7 天；新任务提交时分批清理过期记录。使用记录清理不影响绑定。修改模型映射、路由排序、自动冷却、删除路由或轮换渠道密钥均不会把查询切到其他上游。当前直连渠道的禁用和并发限制仍然生效；原渠道被删除时查询返回不可用。

同一网关组、同一密钥下，如果不同上游意外生成相同任务 ID，拒绝覆盖原绑定并终止提交；同一目标的幂等响应保留最初绑定。任务 ID 仅支持最长 256 字节的字母、数字、`-`、`_`、`.`、`:`，不接受路径或查询参数。

未知任务、升级前创建的任务、过期任务及其他密钥的任务统一返回 404。不能通过尝试所有渠道来找回任务；此类任务应向原上游查询。

## 校验与记录

视频提交及查询跳过文本专用的 `zero_usage`、`response_model`、`assistant_text` 校验。`raw_body` 与 `error_message` 正则仍按原来的模型、协议选择器生效；视频请求不会修改其他请求共用的规则缓存。HTTP 错误和 HTTP 200 错误信封仍被检查。

查询到任务的 `failed`、`expired`、`canceled` 等状态时会返回任务结果，供客户端停止轮询；若管理员的原始报文／错误消息规则拒绝该结果，则返回网关错误，但不会换源重建任务。查询失败也不会清除或新增模型冷却。

任务查询只读取已有任务，不生成新的用量结算记录，避免反复查询重复按次收费。创建任务耗尽网关密钥额度后仍能查询自己的已有任务；密钥或网关组被禁用时仍会拒绝。提交仍沿用现有计价逻辑；此次改动不增加视频按秒、按分辨率定价或完成后结算。同步直接返回视频 URL、没有任务 ID 的 JSON 结果保留透传行为。

`/v1/videos/{id}/content` 对 Image2Pro 兼容任务使用结果中的公网视频 URL，对原生透传任务使用绑定上游的同名鉴权下载接口；原生查询结果中的视频 URL 保持原样。原上游需要支持所选方式。本功能不新增 Seedance 的 `/api/v3/contents/generations/tasks` 接口。
