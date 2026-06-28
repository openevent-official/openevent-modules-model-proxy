# LLM Protocol llm.v1

[English version](LLM_PROTOCOL.md)

> 状态：当前有效规格
> 适用范围：OpenEvent channel `protocol="llm.v1"` 的模型代理 payload 与 channel description

## 0. 设计前提

本协议默认参与方遵守 `llm.v1` 规则与 OpenEvent ACL：

- 调用方通过协议定义的 `infer.request` 语义写入请求
- `model-proxy` 通过协议定义的 `infer.result` 语义写入结果
- 订阅方按协议字段解释消息

绕过 SDK、伪造字段、错误设置 `principal` / `recipients`、向非目标 proxy 发送消息、或构造其他违反本协议的输入，不属于 `llm.v1` 需要兼容的正常行为。

## 1. Channel 约定

所有 LLM channel MUST 设置：

```text
protocol = "llm.v1"
```

`description` MUST 是 JSON 字符串：

```json
{
  "version": "v1",
  "updated_at_ms": 1710000000000,
  "metadata": {}
}
```

字段约束：

- `version`：当前固定为 `v1`
- `updated_at_ms`：毫秒时间戳
- `metadata`：可选 object，用于承载部署或业务域需要的静态扩展信息

同一个 `channel_id` 对应一个模型代理会话域。协议层不强制 description 保存 provider 凭据、base URL、模型列表或成员列表；provider 凭据与路由配置由 `model-proxy` 配置文件管理，channel ACL 与成员由 OpenEvent 管理。

LLM channel 约束：

- `visibility` MUST NOT 是 `VISIBILITY_PUBLIC`；只能使用 `VISIBILITY_PROTECTED` 或 `VISIBILITY_PRIVATE`
- 每个 `llm.v1` channel MUST 且只能由一个 `model-proxy` principal/进程负责消费；该唯一性由部署或 bootstrap 配置保证
- 业务调用方与该 `model-proxy` principal 都必须是 channel 成员，否则 OpenEvent 的写入或定向结果投递可能失败

`infer.request` 不使用 OpenEvent `recipients` 定向到 proxy；目标 proxy 由部署和启动配置中绑定该 channel 的唯一 `model-proxy` 决定。OpenEvent channel 成员列表不表达成员角色，具体 provider 由 `model-proxy` 根据自身配置选择。

## 2. 公共规则

所有 `llm.v1` payload 都是 UTF-8 JSON object：

```json
{
  "kind": "infer.request",
  "request_id": "req_xxx",
  "ts_ms": 1710000000000,
  "body": {}
}
```

公共字段：

- `kind`：必填，取值为 `infer.request` / `infer.result`
- `request_id`：必填，在同一个 `channel_id` 内唯一，长度为 `1..128`，只能包含
  ASCII 字母、数字、`.`、`_`、`:`、`-`
- `ts_ms`：必填，Unix 毫秒时间戳（UTC）
- `body`：必填，JSON 可表示的业务载荷 object、array、string、number、boolean 或 null

协议字段严格校验：未知字段、缺失必填字段、字段类型不匹配均视为非法 payload。

### 2.1 OpenEvent 顶层字段

`principal` 是 OpenEvent EventMessage 的顶层字段，不放入 `llm.v1` payload。协议内所有来源身份判断都以 OpenEvent EventMessage 的 `principal` 为准：

- `infer.request`：OpenEvent `principal` 必须使用提交推理请求的业务调用方 principal
- `infer.result`：OpenEvent `principal` 必须使用 `model-proxy` principal

payload 中不得包含 `source_principal`、`provider_api_key`、`api_key` 等身份或密钥字段。Provider 鉴权信息只能由 `model-proxy` 配置提供。

## 3. infer.request

`infer.request` 表示业务模块请求 `model-proxy` 调用模型服务。

```json
{
  "kind": "infer.request",
  "request_id": "req_xxx",
  "prev_seq": 12344,
  "method": "POST",
  "path": "/v1/chat/completions",
  "ts_ms": 1710000000000,
  "body": {
    "model": "gpt-4o-mini",
    "messages": []
  }
}
```

规则：

- 业务模块 SHOULD 通过 `openevent.model_proxy_sdk.publish_infer_request(...)` 写入
- OpenEvent 顶层字段：`principal` 使用业务调用方 principal，`recipients` MUST 为空
- `kind` 必填，固定为 `infer.request`
- `request_id` 必填且在同一个 `channel_id` 内唯一，长度和字符集必须满足公共规则
- `method` 必填，取值为 `GET` / `POST` / `PUT` / `PATCH` / `DELETE`
- `path` 必填，必须以 `/` 开头，且不能包含 scheme、host 或 query；例如 `/v1/chat/completions`
- `ts_ms` 必填，Unix 毫秒时间戳（UTC）
- `body` 必填，按目标模型服务该接口的请求协议构造；模型名等 provider 业务字段由后端自行检查
- `prev_seq` 可选；协议不限制其业务语义，填写时必须为正整数

`request_id` 生成规则：

- OpenAI-like Agent SDK 在调用方未显式传入 `request_id` 时，负责生成 `req_<uuid4 hex>` 格式、碰撞概率可忽略的 ID
- 底层协议 SDK 的 `publish_infer_request(...)` 不生成 `request_id`，只校验调用方传入的 `request_id` 满足长度和字符集约束；直接使用协议 SDK 的业务方必须自行保证同一 `channel_id` 内唯一

同一 `channel_id` 内 `request_id` 重复时，`model-proxy` MUST 不再调用模型 API，并写入 `status_code=60005` 的拒绝 `infer.result`。OpenEvent 日志中的每条 `infer.request` 都由自己的 `seq` 标识；原始请求的最终 `infer.result.prev_seq` 指向原始 request seq，重复请求的拒绝 `infer.result.prev_seq` 指向该重复 request 自己的 seq。因此，同一个 `request_id` 可以出现多个 result 日志事件，但每个 result 都只对应一条具体的 request 日志事件。

## 4. infer.result

`infer.result` 表示 `model-proxy` 对某条 `infer.request` 的最终响应。

```json
{
  "kind": "infer.result",
  "request_id": "req_xxx",
  "prev_seq": 12345,
  "ts_ms": 1710000001234,
  "status_code": 200,
  "headers": [
    {"name": "content-type", "value": "application/json"},
    {"name": "set-cookie", "value": "a=1"},
    {"name": "set-cookie", "value": "b=2"}
  ],
  "body": {}
}
```

规则：

- `model-proxy` MUST 通过 `openevent.model_proxy_sdk.publish_infer_result(...)` 写入
- OpenEvent 顶层字段：`principal` 使用 `model-proxy` principal，`recipients` 固定为对应 `infer.request` 的发送方
- `kind` 必填，固定为 `infer.result`
- `request_id` 必填，值与对应 `infer.request.request_id` 相同
- `prev_seq` 必填，值为对应 `infer.request.seq`
- `ts_ms` 必填，Unix 毫秒时间戳（UTC）
- 一个由 OpenEvent `seq` 唯一标识的 `infer.request` 只允许对应一个最终 `infer.result`
- `status_code`、`body` 按 HTTP 语义透传；无 HTTP 响应时由代理写扩展 `status_code`
- 收到上游 HTTP 响应时，代理不改写上游状态码；默认会在写入 OpenEvent 前过滤不重要的响应头，
  也可通过 model-proxy 配置关闭过滤。JSON 响应体按 JSON 值写入
  `body`，非 JSON 响应体按本文非 JSON 响应规则写入 `body`

`headers` 规则：

- 类型：`[{ "name": string, "value": string }]`
- 每个元素对应一个 HTTP 头字段行，可重复出现同名字段，例如多个 `set-cookie`
- header 名按 HTTP 标准大小写不敏感，建议落盘统一为小写
- 收到 HTTP 响应时 SHOULD 包含过滤后的 `headers`；代理自身生成扩展错误时 MUST 不包含 `headers`

## 5. status_code

收到上游 HTTP 响应时，`status_code` 透传标准 HTTP 状态码（`100~599`）。

未收到 HTTP 响应或代理本地拒绝时，使用扩展码：

- `60000`：代理调用模型 API 超时
- `60001`：DNS 解析失败
- `60002`：TLS 握手失败
- `60003`：连接失败/连接重置
- `60004`：请求被本地取消
- `60005`：重复 `request_id` 被拒绝
- `60006`：保留，不再用于上游非 JSON 响应
- `60007`：代理内部错误
- `60008`：payload 超过 OpenEvent 部署上限
- `60009`：请求 payload 非法，无法按有效推理请求处理

代理生成扩展 `status_code` 时，`body` 使用统一错误结构：

```json
{
  "error": {
    "code": "MODEL_API_TIMEOUT",
    "message": "model API request timed out",
    "type": "model_proxy_error"
  }
}
```

## 6. 透传边界

- `infer.request.body` 按目标模型服务接口协议透传
- `infer.result.status_code` 和 `infer.result.body` 按 HTTP 语义透传
- `infer.result.headers` 默认只写入过滤后的上游 HTTP 响应头；可通过 model-proxy 配置关闭过滤并保留全部响应头
- 连接异常、超时、重复请求拒绝等无上游 HTTP 响应场景，统一使用扩展 `status_code` 与标准错误 `body`
- 首版支持上游非 JSON 响应；此时 `status_code` 仍按 HTTP 响应透传，`headers` 按配置过滤后写入，
  `body` 写为 JSON object：

```json
{
  "non_json_body": {
    "encoding": "base64",
    "content_type": "text/plain; charset=utf-8",
    "data": "..."
  }
}
```

`content_type` 来自上游 `content-type` 响应头；如果不存在则为空字符串。`data`
是上游原始响应 body bytes 的 base64 编码。代理不得截断该 JSON payload；若编码后超过
OpenEvent payload 上限，必须写入 `status_code=60008` 的代理扩展错误 result。
- 业务不得通过 payload 传 provider 凭据、base URL 或 provider 选择字段；这些由 `model-proxy` 配置管理

### 6.1 Payload 大小约束

OpenEvent `payload` 大小建议首版部署上限为 **16 MiB**。

约束：

- `infer.request` 与 `infer.result` 都必须能在 OpenEvent 服务端配置的 payload 上限内完整写入
- 16 MiB 是首版非流式文本、工具调用和 JSON 响应的部署建议值；超过该上限的内容应通过未来流式协议或外部对象存储引用处理
- proxy 不得写入截断后的 JSON payload；当 provider 响应超过 OpenEvent payload 上限时，应写入 `status_code=60008` 的代理扩展错误 result
- 调用方不应依赖超过 16 MiB 的单条非流式 payload 能被稳定传输

## 7. 超时

正常情况下，一个 `infer.request` 对应一个 `infer.result`。若超过调用方设置的请求等待时间仍未观察到 `infer.result`，业务模块可判定该请求超时。

超时后的重发策略由业务模块决定。`model-proxy` 对重复 `request_id` 执行幂等拒绝；业务模块若需要重新发起一次模型调用，应使用新的 `request_id`。

## 8. 版本策略

- `llm.v1` 只做向后兼容增强
- 破坏性变更使用新的 channel protocol，如 `llm.v2`
- SDK 主版本应与协议主版本保持一致
- 首版不支持 `stream=True`；未来流式模式通过新增 `kind` 扩展，不复用当前最终响应语义的 `infer.result`
