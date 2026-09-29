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
| `open_event.addr` | 是 | OpenEvent 服务地址，非空字符串 |
| `open_event.rpc_timeout_ms` | 否 | 每次非流式 OpenEvent RPC 的全局 deadline，以及等待新的 Subscribe 被服务端接受的本地最长时间；默认 `30000`，必须为正整数毫秒 |
| `worker.max_concurrency` | 否 | 同时调用 provider 的任务槽位数，默认 `8`，必须为正整数 |
| `worker.max_retries` | 否 | Worker 各 OpenEvent 操作共用的额外尝试次数配置，默认 `3`，必须为非负整数；设为 `0` 表示不做额外尝试 |
| `worker.retry_interval_ms` | 否 | 每次 OpenEvent 额外尝试前的固定等待间隔，默认 `1000`，必须为正整数毫秒 |
| `principal` | 是 | model-proxy 使用的 OpenEvent principal，正整数 |
| `token` | 是 | model-proxy 使用的 OpenEvent token，非空字符串 |
| `channels` | 是 | 当前 worker 负责的 Channel ID；必须为非空、无重复的正整数列表 |
| `max_payload_bytes` | 否 | 收到的 request payload、编码后的输出 payload 和保留的 provider 响应数据的最大字节数；默认 `16777216`，必须为不小于 `4096` 的整数 |
| `default_provider` | 是 | 默认 provider 名称，必须与 `providers` 中的一个名称完全一致 |
| `providers.<name>.type` | 是 | 当前支持 `openai_compatible`，必须满足 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 第 4.1 节定义的最小兼容契约 |
| `providers.<name>.base_url` | 是 | Provider 的绝对 HTTP(S) 基础 URL；可包含网关路径前缀，具体拼接规则见下文 |
| `providers.<name>.api_key` | 是 | Provider 原始 API token；必须是非空字符串，不包含首尾空白或控制字符 |
| `providers.<name>.timeout.response_header_ms` | 是 | 收到 HTTP 响应头前的总时间预算，正整数毫秒；DNS 等待边界见下文 |
| `providers.<name>.timeout.idle_ms` | 否 | 读取 provider 响应时连续多久没有进展就超时，默认 `30000`；正整数毫秒 |

配置文件顶层必须是 YAML mapping，并且只能包含表中列出的顶层字段。`open_event`、`worker`、`providers` 和每个
provider 的 `timeout` 都必须是 mapping；这些 mapping 也只能包含本文列出的字段。未知字段、字段拼错、重复 YAML key、
缺少必填字段或字段类型错误都会使 Worker 在启动时退出。YAML 的 boolean 不算整数。

- `open_event`、`providers` 和每个 provider 的 `timeout` 都是必填 mapping。
- `worker` 是可选 mapping；整体省略等价于空 mapping，`max_concurrency`、`max_retries` 和 `retry_interval_ms` 分别使用表中的默认值。
- `providers` 必须是非空 mapping。每个 provider 名称必须是非空字符串，且不能含首尾空白或控制字符。

已经建立的 Subscribe 没有整个流的 deadline。连接断开后，Worker 从已经处理的位置继续订阅。
GetStatus、Fetch、UUID 分配与查询以及 Subscribe 建连按
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)执行；`PublishAutoSeq` 的提交判断和重试按
[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)执行。两类操作共用 `worker.max_retries` 和
`worker.retry_interval_ms` 的配置值，各操作分别计数，错误分类不同。这两个配置不用于 Provider HTTP 请求。

部署必须把 `max_payload_bytes` 配置为不大于 OpenEvent 服务端 payload 上限；输出发布被服务端拒绝时按
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#81-max_payload_bytes) 处理。

同一个 `max_payload_bytes` 分别限制下面每一个对象，不把多个对象累加成一次调用的总量：

1. OpenEvent `infer.request` 的原始 payload 字节数，即 `EventMessage.payload` 的长度。
2. Provider 原始响应头区块的总字节数。Worker 在解析和过滤之前计数，包含状态行、全部响应头字段、HTTP 分隔符
   和结束空行；临时 `1xx` 响应也计入同一总量。响应还受 `http.client` 自带的行长和响应头数量限制。
3. HTTP content decoding 之后的普通响应 body，或流式请求收到的非 SSE 响应 body。这里的 content decoding
   包括下文支持范围内的 HTTP 解压，计数发生在 UTF-8 和 JSON 解析之前。
4. HTTP content decoding 之后的一条完整 SSE 事件。Worker 先把 CRLF 和 CR 换行统一成 LF，再对整条事件的
   UTF-8 字节计数；字段行、注释行、每行结尾的 LF 和结束事件的空行都计入，不只计算 `data:` 内容。
5. Worker 准备发布的每一条 result、append 或 end 的完整 UTF-8 JSON payload。

超限请求不调用 Provider；超限结果见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#81-max_payload_bytes)。

Provider HTTP 响应支持不带 `Content-Encoding`、`identity`，或单层 `gzip`（含别名 `x-gzip`）、`deflate`。
Worker 发送 `Accept-Encoding: gzip, deflate`。其他编码和多个编码叠加不在支持范围内，按 Provider 解析失败处理；
普通请求和流式请求分别按 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md) 的错误规则生成输出。

`base_url` 必须是带 authority 的绝对 `http` 或 `https` URL，不得包含 userinfo、query 或 fragment；允许
包含网关路径前缀。构造 Provider 请求 URL 时，先删除 `base_url` 末尾全部 `/`，再直接追加协议中以 `/`
开头的固定 endpoint path。例如
`https://host/gateway/` 与 `/v1/responses` 得到 `https://host/gateway/v1/responses`。

`api_key` 保存不带 `Bearer ` 前缀的原始 token。每个 Provider 请求只注入一条
`Authorization: Bearer <api_key>` 和一条 `Content-Type: application/json`；payload 不能覆盖这两个字段或提供额外凭据。

`response_header_ms` 是从发起 provider 请求到收到 HTTP 响应头的总预算，包括 DNS、建连、TLS、发送请求和等待响应头。
DNS 耗时计入预算，但系统解析不受 socket 超时控制，也不能被取消打断；解析返回后再检查 deadline 和取消标记。
收到响应头后，`idle_ms` 限制连续没有有效读取进展的时间；普通响应收到新的 body 字节算进展，SSE 只有收到完整且可解析的
`data:` 事件才算进展。Worker 因背压暂停读取以及向 OpenEvent 发布输出的时间不计入空闲时间。

`worker.max_concurrency` 只限制正在调用 Provider 的请求数量；流式请求在终态前持续占用一个槽位。

provider、默认 provider 和 timeout 配置只影响 Worker 启动后新收到的 request，不用于重新执行历史 request。
历史未终态 request 的结果形态以
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md#82-worker-重启后的结果) 为准。

## 运行前准备

Channel、成员及单 Worker 部署要求见 [LLM_PROTOCOL_cn.md 第 2 节](LLM_PROTOCOL_cn.md#2-channel-与-openevent-字段)。
Provider 必须满足该协议[第 4.1 节的兼容契约](LLM_PROTOCOL_cn.md#41-openai_compatible-的最小兼容契约)。
