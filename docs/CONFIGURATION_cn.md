# Configuration

[English version](CONFIGURATION.md)

`model-proxy` 通过启动时传入的 YAML 配置文件运行：

```bash
model-proxy --config model-proxy.yaml
```

## 示例

```yaml
protocol: llm.v1

open_event:
  addr: 127.0.0.1:9527
  rpc_timeout_ms: 30000

worker:
  max_concurrency: 8
  max_retries: 3
  retry_interval_ms: 1000

principal: 20001
token: token-xxx
channels: [1001, 1002]
max_payload_bytes: 16777216

default_provider: openai_main

providers:
  openai_main:
    type: openai_compatible
    base_url: https://api.openai.com
    api_key: sk-xxx
    timeout:
      response_header_ms: 65000
      idle_ms: 30000
```

## 字段

| 字段 | 必填 | 说明 |
|------|------|------|
| `protocol` | 是 | 当前必须为 `llm.v1` |
| `open_event.addr` | 是 | OpenEvent 0.8.0 服务地址 |
| `open_event.rpc_timeout_ms` | 否 | 每次非流式 OpenEvent RPC 的全局 deadline，以及等待新的 Subscribe 被服务端接受的本地最长时间；默认 `30000`，必须为正整数毫秒 |
| `worker.max_concurrency` | 否 | 同时调用 provider 的任务槽位数，默认 `8`，必须为正整数 |
| `worker.max_retries` | 否 | Worker 各 OpenEvent 操作共用的额外尝试次数配置，默认 `3`，必须为非负整数；设为 `0` 表示不做额外尝试 |
| `worker.retry_interval_ms` | 否 | 每次 OpenEvent 额外尝试前的固定等待间隔，默认 `1000`，必须为正整数毫秒 |
| `principal` | 是 | model-proxy 使用的 OpenEvent principal |
| `token` | 是 | model-proxy 使用的 OpenEvent token |
| `channels` | 是 | 当前 worker 负责的 Channel ID；必须为非空、无重复的正整数列表 |
| `max_payload_bytes` | 否 | 收到的 request payload、编码后的输出 payload 和保留的 provider 响应数据的最大字节数；默认 `16777216`，必须为不小于 `4096` 的整数 |
| `default_provider` | 是 | 默认 provider 名称，必须引用 `providers` 中的一项 |
| `providers.<name>.type` | 是 | 当前支持 `openai_compatible`，必须满足 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 第 4.1 节定义的最小兼容契约 |
| `providers.<name>.base_url` | 是 | Provider 的绝对 HTTP(S) 基础 URL；可包含网关路径前缀，具体拼接规则见下文 |
| `providers.<name>.api_key` | 是 | Provider 原始 API token；必须是非空字符串，不包含首尾空白或控制字符 |
| `providers.<name>.timeout.response_header_ms` | 是 | 从发起 provider 请求到收到 HTTP 响应头最多等待多久；正整数毫秒 |
| `providers.<name>.timeout.idle_ms` | 否 | 读取 provider 响应时连续多久没有进展就超时，默认 `30000`；正整数毫秒 |

配置文件顶层必须是 YAML mapping，并且只能包含表中列出的顶层字段。`open_event`、`worker`、`providers` 和每个
provider 的 `timeout` 都必须是 mapping；这些 mapping 也只能包含本文列出的字段。未知字段、字段拼错、重复 YAML key、
缺少必填字段或字段类型错误都会使 Worker 在启动时退出，不会忽略错误后使用默认值。YAML 的 boolean 不算整数。

- `open_event`、`providers` 和每个 provider 的 `timeout` 都是必填 mapping。
- `worker` 是可选 mapping；整体省略等价于空 mapping，`max_concurrency`、`max_retries` 和 `retry_interval_ms` 分别使用表中的默认值。
- `open_event.addr` 必须是非空字符串。
- `principal` 必须是正整数；`token` 必须是非空字符串。
- `channels` 必须是非空列表，其中每项都是互不重复的正整数。
- `providers` 必须是非空 mapping。每个 provider 名称必须是非空字符串，且不能含首尾空白或控制字符。
- `default_provider` 必须与 `providers` 中的一个名称完全一致。

`open_event.rpc_timeout_ms` 是每次非流式 OpenEvent RPC 的 deadline，也是每次建立 Subscribe 时等待服务端接受的最长时间。
已经建立的 Subscribe 没有整个流的 deadline。连接断开后，Worker 从已经处理的位置继续订阅。
GetStatus、Fetch、UUID 分配与查询以及 Subscribe 建连按
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)执行；`PublishAutoSeq` 的提交判断和重试按
[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)执行。两类操作共用 `worker.max_retries` 和
`worker.retry_interval_ms` 的配置值，各操作分别计数，错误分类不同。这两个配置不用于 Provider HTTP 请求。

部署必须把 `max_payload_bytes` 配置为不大于 OpenEvent 服务端 payload 上限。如果两个上限没有对齐，OpenEvent 会用
`RESOURCE_EXHAUSTED` 拒绝实际的超限发布；worker 会把最终输出发布失败作为致命错误并退出。

同一个 `max_payload_bytes` 分别限制下面每一个对象，不把多个对象累加成一次调用的总量：

1. OpenEvent `infer.request` 的原始 payload 字节数，即 `EventMessage.payload` 的长度。
2. Provider 的全部响应头字段名和值。Worker 在过滤响应头之前计数；名称和值分别按 UTF-8 编码后的字节数相加，
   不计冒号、空格和换行等 HTTP 分隔符，重复字段分别计数。
3. HTTP content decoding 之后的普通响应 body，或流式请求收到的非 SSE 响应 body。这里的 content decoding
   包括下文支持范围内的 HTTP 解压，计数发生在 UTF-8 和 JSON 解析之前。
4. HTTP content decoding 之后的一条完整 SSE 事件。Worker 先把 CRLF 和 CR 换行统一成 LF，再对整条事件的
   UTF-8 字节计数；字段行、注释行、每行结尾的 LF 和结束事件的空行都计入，不只计算 `data:` 内容。
5. Worker 准备发布的每一条 result、append 或 end 的完整 UTF-8 JSON payload。

Worker 在调用 Provider 前拒绝超限 request。输入本身未超限时，生成的输出 payload 仍需单独检查，因为 JSON
外层和字符串转义会增加字节数。触发上限后的
`60008` 结果只在 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#81-max_payload_bytes) 中定义。

当前只支持 `POST /v1/chat/completions` 和 `POST /v1/responses`，endpoint 集合固定不可配置。request 校验、
流事件和终态规则见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。

Provider HTTP 响应支持不带 `Content-Encoding`、`identity`，或单层 `gzip`（含别名 `x-gzip`）、`deflate`。
Worker 发送 `Accept-Encoding: gzip, deflate`。其他编码和多个编码叠加不在支持范围内，按 Provider 解析失败处理；
普通请求和流式请求分别按 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 的错误规则生成输出。这是本模块的传输支持范围，
不是 OpenAI API 对响应压缩方式的承诺。

`base_url` 必须是带 authority 的绝对 `http` 或 `https` URL，不得包含 userinfo、query 或 fragment；允许
包含网关路径前缀。构造 Provider 请求 URL 时，先删除 `base_url` 末尾全部 `/`，再直接追加协议中以 `/`
开头的固定 endpoint path，不能使用会把路径前缀替换掉的 URL join 语义。例如
`https://host/gateway/` 与 `/v1/responses` 得到 `https://host/gateway/v1/responses`。

`api_key` 保存原始 token，不包含 `Bearer ` 前缀。它不得包含首尾空白或控制字符；Worker 按 HTTP 规则把它放入
`Authorization` 请求头。Worker 对每个 Provider 请求只注入一条
`Authorization: Bearer <api_key>` 和一条 `Content-Type: application/json`；不会识别或保留配置值中已有的
鉴权 scheme，也不从 payload 接受额外凭据或覆盖这两个字段。

`response_header_ms` 是从发起 provider 请求到收到 HTTP 响应头的总预算，包括 DNS、建连、TLS、发送请求和等待响应头。
收到响应头后，`idle_ms` 限制连续没有有效读取进展的时间；普通响应收到新的 body 字节算进展，SSE 只有收到完整且可解析的
`data:` 事件才算进展。Worker 因背压暂停读取以及向 OpenEvent 发布输出的时间不计入空闲时间。具体错误分类、状态码和
流式终态只由 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 定义。

部署必须保证每个 `llm.v1` Channel 只分配给一个运行中的 model-proxy Worker。设置 `body.stream=true` 的请求在终态前
持续占用一个 `worker.max_concurrency` 槽位。

provider、默认 provider 和 timeout 配置只影响 Worker 启动后新收到的 request，不用于重新执行历史 request。
历史未终态 request 的结果形态以
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#82-worker-重启后的结果) 为准。

`worker.max_concurrency` 只限制正在调用 provider 的请求数量。

## 运行前准备

运行前需要准备一个 OpenEvent channel：

- channel protocol 设置为 `llm.v1`
- channel description 必须符合 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 定义的 `llm.v1` 格式：它是包含必填
  `version`、`updated_at_ms` 和 `metadata` 字段的 JSON object
- 业务调用方和 model-proxy principal 都是 channel 成员
- channel 必须使用 protected 或 private visibility，不得使用 public visibility
- Provider 凭据只写在 model-proxy 配置文件中，不放入 `llm.v1` payload
- 使用任意 `openai_compatible` 服务前，必须确认它满足 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 第 4.1 节定义的
  endpoint、JSON 响应和 SSE 最小兼容契约

每个 Worker 实例使用配置中的 OpenEvent token。Worker 重启时按
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#82-worker-重启后的结果) 处理历史未终态 request。

`llm.v1` channel 和 payload 的详细约束见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。
