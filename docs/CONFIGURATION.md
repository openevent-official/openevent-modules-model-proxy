# Configuration

[中文版](CONFIGURATION_cn.md)

`model-proxy` runs from a YAML configuration file passed at startup:

```bash
model-proxy --config model-proxy.yaml
```

## Example

```yaml
protocol: llm.v1

open_event:
  addr: 127.0.0.1:9527

principal: 20001
token: token-xxx
idempotency_dsn: sqlite:///model_proxy.db
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

## Fields

| Field | Required | Description |
|------|----------|-------------|
| `protocol` | yes | Must be `llm.v1` |
| `open_event.addr` | yes | OpenEvent service address |
| `principal` | yes | OpenEvent principal used by model-proxy |
| `token` | yes | OpenEvent token used by model-proxy |
| `idempotency_dsn` | no | Local state DSN, default `sqlite:///model_proxy.db` |
| `max_payload_bytes` | no | Maximum payload bytes, default `16777216` |
| `filter_response_headers` | no | Whether to filter unimportant upstream response headers before writing `infer.result`; default `true`; set `false` to keep all upstream headers |
| `default_provider` | yes | Default provider name, must reference an entry in `providers` |
| `providers.<name>.type` | yes | Currently supports `openai_compatible` |
| `providers.<name>.base_url` | yes | Provider base URL, without request path |
| `providers.<name>.api_key` | yes | Provider API key |
| `providers.<name>.timeout.total_ms` | yes | Per-provider-call total timeout in milliseconds |

## Runtime Preparation

Prepare an OpenEvent channel before running:

- Channel protocol is `llm.v1`.
- Business callers and the model-proxy principal are channel members.
- The channel should not use public visibility.
- Provider credentials are stored only in the model-proxy configuration file,
  not in `llm.v1` payloads.

Detailed `llm.v1` channel and payload constraints are documented in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md).
