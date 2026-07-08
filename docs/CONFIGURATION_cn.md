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

principal: 20001
token: token-xxx
max_payload_bytes: 16777216
filter_response_headers: true

default_provider: openai_main

providers:
  openai_main:
    type: openai_compatible
    base_url: https://api.openai.com
    api_key: sk-xxx
    timeout:
      total_ms: 65000
```

## 字段

| 字段 | 必填 | 说明 |
|------|------|------|
| `protocol` | 是 | 当前必须为 `llm.v1` |
| `open_event.addr` | 是 | OpenEvent 服务地址 |
| `principal` | 是 | model-proxy 使用的 OpenEvent principal |
| `token` | 是 | model-proxy 使用的 OpenEvent token |
| `max_payload_bytes` | 否 | 单条 payload 最大字节数，默认 `16777216` |
| `filter_response_headers` | 否 | 是否在写入 `infer.result` 前过滤不重要的上游响应头，默认 `true`；设为 `false` 时保留全部上游响应头 |
| `default_provider` | 是 | 默认 provider 名称，必须引用 `providers` 中的一项 |
| `providers.<name>.type` | 是 | 当前支持 `openai_compatible` |
| `providers.<name>.base_url` | 是 | Provider 基础地址，不包含请求 path |
| `providers.<name>.api_key` | 是 | Provider API key |
| `providers.<name>.timeout.total_ms` | 是 | 单次 provider 调用超时时间，单位毫秒 |

## 运行前准备

运行前需要准备一个 OpenEvent channel：

- channel protocol 设置为 `llm.v1`
- 业务调用方和 model-proxy principal 都是 channel 成员
- channel 不应使用 public visibility
- Provider 凭据只写在 model-proxy 配置文件中，不放入 `llm.v1` payload

`llm.v1` channel 和 payload 的详细约束见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。
