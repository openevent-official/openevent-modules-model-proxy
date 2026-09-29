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
| `open_event.addr` | Yes | OpenEvent service address, a nonempty string |
| `open_event.rpc_timeout_ms` | No | Global deadline for each non-streaming OpenEvent RPC and local maximum wait for a new Subscribe to be accepted; defaults to `30000`, a positive integer in milliseconds |
| `worker.max_concurrency` | No | Number of task slots concurrently calling providers; defaults to `8`, a positive integer |
| `worker.max_retries` | No | Extra-attempt setting shared by the Worker's OpenEvent operations; defaults to `3`, a nonnegative integer; `0` disables extra attempts |
| `worker.retry_interval_ms` | No | Fixed wait before each extra OpenEvent attempt; defaults to `1000`, a positive integer in milliseconds |
| `principal` | Yes | OpenEvent principal used by model-proxy, a positive integer |
| `token` | Yes | OpenEvent token used by model-proxy, a nonempty string |
| `channels` | Yes | Channel IDs assigned to this worker; a nonempty list of distinct positive integers |
| `max_payload_bytes` | No | Maximum bytes for received request payloads, encoded output payloads, and retained provider response data; defaults to `16777216`, an integer of at least `4096` |
| `default_provider` | Yes | Default provider name; must exactly match a name in `providers` |
| `providers.<name>.type` | Yes | Currently supports `openai_compatible`, which must satisfy the minimum compatibility contract in Section 4.1 of [LLM_PROTOCOL.md](LLM_PROTOCOL.md) |
| `providers.<name>.base_url` | Yes | Absolute HTTP(S) Provider base URL; may include a gateway path prefix; see the concatenation rules below |
| `providers.<name>.api_key` | Yes | Raw Provider API token; a nonempty string without surrounding whitespace or control characters |
| `providers.<name>.timeout.response_header_ms` | Yes | Total time budget before HTTP response headers arrive, in positive integer milliseconds; see the DNS waiting boundary below |
| `providers.<name>.timeout.idle_ms` | No | Maximum continuous time without progress while reading a provider response; defaults to `30000`, positive integer milliseconds |

The configuration root must be a YAML mapping containing only the top-level fields listed above.
`open_event`, `worker`, `providers`, and each provider's `timeout` must also be mappings containing
only fields defined here. Unknown or misspelled fields, duplicate YAML keys, missing required fields,
and incorrect field types cause the Worker to exit at startup. YAML booleans do not count as integers.

- `open_event`, `providers`, and each provider's `timeout` are required mappings.
- `worker` is optional; omitting it is equivalent to an empty mapping, with the table's defaults
  for `max_concurrency`, `max_retries`, and `retry_interval_ms`.
- `providers` must be a nonempty mapping. Each provider name must be a nonempty string without
  surrounding whitespace or control characters.

An established Subscribe has no whole-stream deadline. After a disconnection, the Worker resumes
from its processed position.
GetStatus, Fetch, UUID allocation and lookup, and Subscribe establishment follow the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md); `PublishAutoSeq` commit decisions and
retries follow the [single-event reliable publishing contract](RESULT_PUBLISHING.md). Both categories
share `worker.max_retries` and `worker.retry_interval_ms`, with separate counters for each operation
and different error classification. These two settings do not apply to Provider HTTP requests.

Deployments must configure `max_payload_bytes` no greater than the OpenEvent server's payload limit.
Server rejection of an output publication follows
[LLM_PROTOCOL.md](LLM_PROTOCOL.md#81-max_payload_bytes).

The same `max_payload_bytes` limit applies separately to each of the following objects;
it is not an aggregate limit across all objects in one call:

1. The raw OpenEvent `infer.request` payload: the length of `EventMessage.payload`.
2. The raw Provider response header blocks, counted before parsing and filtering. Count all bytes
   received, including status lines, every header field, HTTP separators, and terminating blank
   lines. Include any interim `1xx` responses in the same total. Responses must also fit
   `http.client`'s built-in line-length and header-count limits.
3. An ordinary response body, or a non-SSE response body received for a streaming request, after
   HTTP content decoding. Content decoding includes HTTP decompression within the supported range below; counting
   precedes UTF-8 decoding and JSON parsing.
4. One complete SSE event after HTTP content decoding. First normalize CRLF and CR to LF, then
   count UTF-8 bytes for the entire event, including field lines, comment lines, each terminating
   LF, and the blank line ending the event, rather than only `data:` content.
5. The complete UTF-8 JSON payload of each result, append, or end the Worker prepares to publish.

Oversized requests do not call the Provider; overflow outcomes are defined in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md#81-max_payload_bytes).

Provider HTTP responses may omit `Content-Encoding`, use `identity`, or use a single layer of `gzip`
(including its `x-gzip` alias) or `deflate`. The Worker sends `Accept-Encoding: gzip, deflate`.
Other encodings and stacked encodings are unsupported and treated as Provider parsing failures;
ordinary and streaming requests produce output under the error rules in [LLM_PROTOCOL.md](LLM_PROTOCOL.md).

`base_url` must be an absolute `http` or `https` URL with an authority and without userinfo, query,
or fragment. A gateway path prefix is allowed. To construct the Provider request URL, remove all
trailing `/` characters from `base_url`, then directly append the protocol's fixed endpoint path
starting with `/`. For example,
`https://host/gateway/` and `/v1/responses` produce `https://host/gateway/v1/responses`.

`api_key` stores the raw token without a `Bearer ` prefix. Each Provider request receives exactly
one `Authorization: Bearer <api_key>` and one `Content-Type: application/json` header.
The payload cannot override these fields or supply additional credentials.

`response_header_ms` is the total budget from starting the provider request through receiving HTTP
response headers, including DNS, connection establishment, TLS, sending the request, and waiting for
headers. DNS time counts toward this budget, but socket timeouts and cancellation cannot interrupt
system resolution; the deadline and cancellation flag are checked when resolution returns.
After headers arrive, `idle_ms` limits continuous time without effective read progress.
New body bytes count as progress for ordinary responses; SSE requires a complete, parseable `data:`
event. Time spent pausing reads for backpressure or publishing output to OpenEvent does not count
as idle time.

`worker.max_concurrency` limits only the number of requests currently calling Providers.
A streaming request occupies one slot until it reaches a terminal state.

Provider, default-provider, and timeout settings affect only requests newly received after Worker
startup; they are not used to re-execute historical requests. Historical requests without a terminal
state follow [LLM_PROTOCOL.md](LLM_PROTOCOL.md#82-outcomes-after-worker-restart).

## Preparation

For Channel, membership, and single-Worker deployment requirements, see
[LLM_PROTOCOL.md Section 2](LLM_PROTOCOL.md#2-channels-and-openevent-fields).
Providers must satisfy its [Section 4.1 compatibility contract](LLM_PROTOCOL.md#41-minimum-openai_compatible-contract).
