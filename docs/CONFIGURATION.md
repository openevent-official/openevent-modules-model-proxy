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
  rpc_timeout_ms: 30000

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

## Fields

| Field | Required | Description |
|------|----------|-------------|
| `protocol` | yes | Must be `llm.v1` |
| `open_event.addr` | yes | OpenEvent service address |
| `open_event.rpc_timeout_ms` | no | Per-RPC timeout in milliseconds, default `30000`; Subscribe reconnects from the last cursor after this deadline |
| `worker.max_concurrency` | no | Maximum concurrent request tasks, default `8`; must be positive |
| `principal` | yes | OpenEvent principal used by model-proxy |
| `token` | yes | OpenEvent token used by model-proxy |
| `channels` | yes | Non-empty, duplicate-free list of positive Channel IDs owned by this worker |
| `max_payload_bytes` | no | Maximum payload bytes, default `16777216` |
| `default_provider` | yes | Default provider name, must reference an entry in `providers` |
| `providers.<name>.type` | yes | Currently supports `openai_compatible` |
| `providers.<name>.base_url` | yes | Provider base URL, without request path |
| `providers.<name>.api_key` | yes | Provider API key |
| `providers.<name>.timeout.total_ms` | yes | Per-provider-call total timeout in milliseconds |
| `providers.<name>.allowlist.methods` | no | Exact allowed HTTP methods; defaults to `["POST"]` |
| `providers.<name>.allowlist.paths` | no | Exact allowed request paths; defaults to `["/v1/chat/completions", "/v1/responses"]` |

Both allowlist fields must be non-empty when configured. Methods are
case-sensitive and paths use exact matching; query strings, fragments, and
wildcards are not supported. A request must match both lists. The proxy rejects
other requests with `status_code=60010` before constructing an HTTP request or
using the provider API key.

Recovery Fetches only the configured `channels`. Subscribe has no Channel filter,
so the worker receives the global visible stream but discards messages outside
this list before calling GetChannel. Deployment must assign each `llm.v1` Channel
to exactly one running model-proxy worker.

The main thread subscribes, validates, and deduplicates messages. Each accepted
request runs as one task in a fixed-size thread pool through provider execution
and result publishing. When all slots are occupied, subscription processing
blocks until a slot is available, providing backpressure.

## Runtime Preparation

Prepare an OpenEvent channel before running:

- Channel protocol is `llm.v1`.
- Business callers and the model-proxy principal are channel members.
- The channel should not use public visibility.
- Provider credentials are stored only in the model-proxy configuration file,
  not in `llm.v1` payloads.
- Review each provider allowlist before deployment and grant only the methods and
  paths required by callers.

Detailed `llm.v1` channel and payload constraints are documented in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md).
