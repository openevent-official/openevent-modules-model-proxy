# Configuration

[中文版](CONFIGURATION_cn.md)

`model-proxy` runs with a YAML configuration file supplied at startup:

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

## Fields

| Field | Required | Description |
| --- | --- | --- |
| `protocol` | Yes | Must currently be `llm.v1` |
| `open_event.addr` | Yes | OpenEvent 0.8.0 service address |
| `open_event.rpc_timeout_ms` | No | Global deadline for each non-streaming OpenEvent RPC and local maximum wait for a new Subscribe to be accepted; defaults to `30000`, a positive integer in milliseconds |
| `worker.max_concurrency` | No | Number of task slots concurrently calling providers; defaults to `8`, a positive integer |
| `worker.max_retries` | No | Extra-attempt setting shared by the Worker's OpenEvent operations; defaults to `3`, a nonnegative integer; `0` disables extra attempts |
| `worker.retry_interval_ms` | No | Fixed wait before each extra OpenEvent attempt; defaults to `1000`, a positive integer in milliseconds |
| `principal` | Yes | OpenEvent principal used by model-proxy |
| `token` | Yes | OpenEvent token used by model-proxy |
| `channels` | Yes | Channel IDs assigned to this worker; a nonempty list of distinct positive integers |
| `max_payload_bytes` | No | Maximum bytes for received request payloads, encoded output payloads, and retained provider response data; defaults to `16777216`, an integer of at least `4096` |
| `default_provider` | Yes | Default provider name; must reference an entry in `providers` |
| `providers.<name>.type` | Yes | Currently supports `openai_compatible`, which must satisfy the minimum compatibility contract in Section 4.1 of [LLM_PROTOCOL.md](LLM_PROTOCOL.md) |
| `providers.<name>.base_url` | Yes | Absolute HTTP(S) Provider base URL; may include a gateway path prefix; see the concatenation rules below |
| `providers.<name>.api_key` | Yes | Raw Provider API token; a nonempty string without surrounding whitespace or control characters |
| `providers.<name>.timeout.response_header_ms` | Yes | Maximum time from starting a provider request until receiving HTTP response headers; positive integer milliseconds |
| `providers.<name>.timeout.idle_ms` | No | Maximum continuous time without progress while reading a provider response; defaults to `30000`, positive integer milliseconds |

The configuration root must be a YAML mapping containing only the top-level fields listed above.
`open_event`, `worker`, `providers`, and each provider's `timeout` must also be mappings containing
only fields defined here. Unknown or misspelled fields, duplicate YAML keys, missing required fields,
and incorrect field types cause the Worker to exit at startup; errors are not ignored in favor of
defaults. YAML booleans do not count as integers.

- `open_event`, `providers`, and each provider's `timeout` are required mappings.
- `worker` is optional; omitting it is equivalent to an empty mapping, with the table's defaults
  for `max_concurrency`, `max_retries`, and `retry_interval_ms`.
- `open_event.addr` must be a nonempty string.
- `principal` must be a positive integer; `token` must be a nonempty string.
- `channels` must be a nonempty list of distinct positive integers.
- `providers` must be a nonempty mapping. Each provider name must be a nonempty string without
  surrounding whitespace or control characters.
- `default_provider` must exactly match a name in `providers`.

`open_event.rpc_timeout_ms` is the deadline for each non-streaming OpenEvent RPC and the maximum
wait for service acceptance when establishing each Subscribe. An established Subscribe has no
whole-stream deadline. After a disconnection, the Worker resumes from its processed position.
GetStatus, Fetch, UUID allocation and lookup, and Subscribe establishment follow the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md); `PublishAutoSeq` commit decisions and
retries follow the [single-event reliable publishing contract](RESULT_PUBLISHING.md). Both categories
share `worker.max_retries` and `worker.retry_interval_ms`, with separate counters for each operation
and different error classification. These two settings do not apply to Provider HTTP requests.

Deployments must configure `max_payload_bytes` no greater than the OpenEvent server's payload limit.
If the limits differ, OpenEvent rejects an actual oversized publication with `RESOURCE_EXHAUSTED`;
the worker treats the final output publishing failure as fatal and exits.

The same `max_payload_bytes` limit applies separately to each of the following objects;
it is not an aggregate limit across all objects in one call:

1. The raw OpenEvent `infer.request` payload: the length of `EventMessage.payload`.
2. All Provider response header names and values, counted before filtering. Add the UTF-8 byte
   lengths of names and values, excluding HTTP separators such as colons, spaces, and newlines.
   Count duplicate fields separately.
3. An ordinary response body, or a non-SSE response body received for a streaming request, after
   HTTP content decoding. Content decoding includes HTTP decompression within the supported range below; counting
   precedes UTF-8 decoding and JSON parsing.
4. One complete SSE event after HTTP content decoding. First normalize CRLF and CR to LF, then
   count UTF-8 bytes for the entire event, including field lines, comment lines, each terminating
   LF, and the blank line ending the event, rather than only `data:` content.
5. The complete UTF-8 JSON payload of each result, append, or end the Worker prepares to publish.

The Worker rejects oversized requests before calling the Provider. Even when input fits the limit,
each generated output payload must be checked separately because the JSON envelope and string
escaping add bytes. The resulting `60008` behavior is defined only in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md#81-max_payload_bytes).

Only `POST /v1/chat/completions` and `POST /v1/responses` are supported; the endpoint set is fixed
and cannot be configured. See [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for request validation, stream
events, and terminal rules.

Provider HTTP responses may omit `Content-Encoding`, use `identity`, or use a single layer of `gzip`
(including its `x-gzip` alias) or `deflate`. The Worker sends `Accept-Encoding: gzip, deflate`.
Other encodings and stacked encodings are unsupported and treated as Provider parsing failures;
ordinary and streaming requests produce output under the error rules in [LLM_PROTOCOL.md](LLM_PROTOCOL.md).
This is the module's transport support range, not an OpenAI API guarantee about response compression.

`base_url` must be an absolute `http` or `https` URL with an authority and without userinfo, query,
or fragment. A gateway path prefix is allowed. To construct the Provider request URL, remove all
trailing `/` characters from `base_url`, then directly append the protocol's fixed endpoint path
starting with `/`. Do not use URL-join behavior that replaces the path prefix. For example,
`https://host/gateway/` and `/v1/responses` produce `https://host/gateway/v1/responses`.

`api_key` stores the raw token without a `Bearer ` prefix. It must not contain surrounding whitespace
or control characters; the Worker places it in the `Authorization` request header according to
HTTP rules. Each Provider request receives exactly one `Authorization: Bearer <api_key>` and one
`Content-Type: application/json` header. The Worker neither recognizes nor preserves any existing
authentication scheme in the configured value, and accepts no payload credentials or overrides
for these two fields.

`response_header_ms` is the total budget from starting the provider request through receiving HTTP
response headers, including DNS, connection establishment, TLS, sending the request, and waiting for
headers. After headers arrive, `idle_ms` limits continuous time without effective read progress.
New body bytes count as progress for ordinary responses; SSE requires a complete, parseable `data:`
event. Time spent pausing reads for backpressure or publishing output to OpenEvent does not count
as idle time. Error classification, status codes, and streaming terminal rules are defined only in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md).

Deployments must assign each `llm.v1` Channel to exactly one running model-proxy Worker.
A request with `body.stream=true` occupies one `worker.max_concurrency` slot until it reaches a terminal state.

Provider, default-provider, and timeout settings affect only requests newly received after Worker
startup; they are not used to re-execute historical requests. Historical requests without a terminal
state follow [LLM_PROTOCOL.md](LLM_PROTOCOL.md#82-outcomes-after-worker-restart).

`worker.max_concurrency` limits only the number of requests currently calling providers.

## Preparation

Prepare an OpenEvent channel before running:

- Set the channel protocol to `llm.v1`.
- Its description must follow the `llm.v1` format in [LLM_PROTOCOL.md](LLM_PROTOCOL.md): a JSON object
  with required `version`, `updated_at_ms`, and `metadata` fields.
- Both business callers and the model-proxy principal must be channel members.
- The channel must use protected or private visibility, never public visibility.
- Provider credentials belong only in the model-proxy configuration, never in `llm.v1` payloads.
- Before using any `openai_compatible` service, confirm that it meets the minimum endpoint,
  JSON response, and SSE compatibility contract in Section 4.1 of [LLM_PROTOCOL.md](LLM_PROTOCOL.md).

Each Worker instance uses the configured OpenEvent token. On restart, historical requests without
a terminal state are handled according to [LLM_PROTOCOL.md](LLM_PROTOCOL.md#82-outcomes-after-worker-restart).

See [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for detailed `llm.v1` channel and payload constraints.
