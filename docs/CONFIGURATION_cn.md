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

worker:
  max_concurrency: 8

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
      total_ms: 65000
    allowlist:
      methods: ["POST"]
      paths: ["/v1/chat/completions", "/v1/responses"]
```

## 字段

| 字段 | 必填 | 说明 |
|------|------|------|
| `protocol` | 是 | 当前必须为 `llm.v1` |
| `open_event.addr` | 是 | OpenEvent 服务地址 |
| `worker.max_concurrency` | 否 | 并发 request 任务上限，默认 `8`，必须为正整数 |
| `principal` | 是 | model-proxy 使用的 OpenEvent principal |
| `token` | 是 | model-proxy 使用的 OpenEvent token |
| `channels` | 是 | 当前 worker 负责的 Channel ID；必须为非空、无重复的正整数列表 |
| `max_payload_bytes` | 否 | 单条 payload 最大字节数，默认 `16777216` |
| `default_provider` | 是 | 默认 provider 名称，必须引用 `providers` 中的一项 |
| `providers.<name>.type` | 是 | 当前支持 `openai_compatible` |
| `providers.<name>.base_url` | 是 | Provider 基础地址，不包含请求 path |
| `providers.<name>.api_key` | 是 | Provider API key |
| `providers.<name>.timeout.total_ms` | 是 | 单次 provider 调用超时时间，单位毫秒 |
| `providers.<name>.allowlist.methods` | 否 | 允许的 HTTP method 精确列表，默认 `["POST"]` |
| `providers.<name>.allowlist.paths` | 否 | 允许的请求 path 精确列表，默认 `["/v1/chat/completions", "/v1/responses"]` |

显式配置时，两个 allowlist 字段都必须为非空列表。method 区分大小写，path
使用精确匹配，不支持 query string、fragment 或通配符。请求必须同时匹配两个
列表；否则 proxy 在构造 HTTP 请求和使用 Provider API key 前以
`status_code=60010` 拒绝请求。

恢复 Fetch 只读取配置的 `channels`。Subscribe 不支持 Channel filter，因此 worker 仍接收全局可见流，
但会在调用 GetChannel 前丢弃不在此列表中的消息。部署必须保证每个 `llm.v1` Channel 只分配给一个
运行中的 model-proxy worker。

主线程负责订阅、校验和判重。每个已接受 request 作为一个独立任务在线程池中完成 provider 调用和
result 发布；所有并发槽位占满时，主线程阻塞等待空闲槽位，以此形成背压。

## 运行前准备

运行前需要准备一个 OpenEvent channel：

- channel protocol 设置为 `llm.v1`
- 业务调用方和 model-proxy principal 都是 channel 成员
- channel 不应使用 public visibility
- Provider 凭据只写在 model-proxy 配置文件中，不放入 `llm.v1` payload
- 部署前应检查每个 provider 的 allowlist，只授权业务所需的 method 和 path

`llm.v1` channel 和 payload 的详细约束见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。
