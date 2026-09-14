# LLM Protocol llm.v1

[中文版](LLM_PROTOCOL_cn.md)

> Status: current effective specification
> Scope: model-proxy payloads and conventions for OpenEvent channels with `protocol="llm.v1"`

## 1. Boundaries

`llm.v1` defines messages between business callers, `model-proxy`, and result consumers. OpenEvent
stores opaque payloads, assigns global `seq` values and message `uuid` values, enforces Channel ACLs,
and provides Fetch/Subscribe. It does not interpret this protocol.

The protocol assumes callers use the SDK and follow the principal, recipients, stream_id, and
single-writer constraints. It adds no business authorization, does not guarantee that a request
calls the provider only once, and defines no business retry semantics.

## 2. Channels and OpenEvent Fields

Every model Channel MUST set `protocol="llm.v1"` and use protected or private visibility.
Its description is a JSON string with this shape:

```json
{"version":"v1","updated_at_ms":1710000000000,"metadata":{}}
```

The decoded description must be a JSON object containing exactly the three required fields
`version`, `updated_at_ms`, and `metadata`. `version` must be the string `"v1"`; `updated_at_ms`
must be a nonnegative integer, with `bool` excluded; `metadata` must be a JSON object whose
contents are application-defined. JSON field order and insignificant whitespace do not affect validity.

A Channel is one Model Proxy session domain, bound by deployment configuration to exactly one
running Worker. Both callers and the Worker must be members. A request has an empty OpenEvent
`recipients` list; Worker output messages are addressed to the principal that published the request.

Top-level OpenEvent fields retain their native meanings:

- `principal` is the publisher: the caller for requests and the Worker for output.
- `seq` is an immutable global position and the value referenced by `prev_seq`.
- `recipients` only filters delivery; it is neither an ACL nor a confidentiality boundary.
- `uuid` is a server-allocated message ID, distinct from both `stream_id` and `request_seq`. When a
  PublishAutoSeq result is uncertain, the same UUID must be reused as specified in the
  [single-event reliable publishing contract](RESULT_PUBLISHING.md).

## 3. JSON and Message Fields

Every payload must be a UTF-8 JSON object. JSON integers exclude booleans. The following table defines
the types and allowed values of all top-level fields. A top-level field is invalid for a message unless
it is listed as required or optional for that message kind.

| Field | JSON type and values |
| --- | --- |
| `kind` | string; one of `infer.request`, `infer.result`, `infer.append`, `infer.end`, or `infer.cancel` |
| `stream_id` | string; caller-generated call identifier; length `1..128`, with every character in ASCII `[A-Za-z0-9._:-]` |
| `ts_ms` | integer, `>= 0`, with no additional upper bound; Unix millisecond timestamp generated and frozen by the publisher |
| `provider` | nonempty string; field validity does not depend on whether the Worker currently configures that name |
| `method` | string; only `POST` |
| `path` | string; only `/v1/chat/completions` or `/v1/responses` |
| `prev_seq` | positive integer; references an OpenEvent message's `seq` |
| `request_seq` | positive integer; the target `infer.request` message's OpenEvent `seq` |
| `status_code` | integer; Provider HTTP status `100..599`, or a Model Proxy status code explicitly listed in Section 7 |
| `headers` | nonempty array; each element is an object containing exactly `name` and `value`, both strings; `name` must be a lowercase name allowed by Section 5 |
| `end_status` | string; only `completed`, `failed`, or `interrupted` |
| `body` | must be an object in `infer.request`; allowed JSON types and omission rules for other messages are listed below |

The five message kinds allow the following fields. Any field not listed as required or optional is invalid.

| `kind` | Required fields | Optional fields | `body` rule |
| --- | --- | --- | --- |
| `infer.request` | `kind`, `stream_id`, `ts_ms`, `method`, `path`, `body` | `provider`, `prev_seq` | Must be an object; `stream`, when present, must be boolean |
| `infer.result` | `kind`, `stream_id`, `ts_ms`, `prev_seq`, `status_code` | `headers`, `body` | Any JSON value; an absent `body` differs from `body: null` |
| `infer.append` | `kind`, `stream_id`, `ts_ms`, `request_seq`, `prev_seq`, `body` | None | Any JSON value |
| `infer.end` | `kind`, `stream_id`, `ts_ms`, `request_seq`, `status_code`, `end_status` | `body` | Optional for `completed`; required for `failed` and `interrupted`; any JSON value |
| `infer.cancel` | `kind`, `stream_id`, `ts_ms`, `request_seq` | None | No `body` |

Strict parsing checks that the payload is a UTF-8 JSON object, its field set, JSON types, and all value
ranges listed in this section. If the Worker encounters any `llm.v1` message that fails strict parsing,
whether during startup scanning or live subscription, it considers the Channel unprocessable and exits
with an error. It does not extract `kind` or `stream_id` from a payload that failed parsing, and does not
publish a `60009` or any other result for it.

The publisher must generate `ts_ms` before the first serialization and freeze it together with the full
payload. Retrying the same event must not regenerate the timestamp. The payload's `ts_ms` is independent
of the outer OpenEvent `EventMessage.ts_ms`; the two need not be equal. The outer timestamp is when the
server received the publish request, as defined by the
[OpenEvent API contract](../openevent-sdk/docs/API.md#1-basic-conventions).

Payloads contain no principal, token, provider credentials, or base URL. An `infer.request` may include
an optional top-level `provider` name to select a provider from the Worker's configuration. This name
is outside `body` and is not forwarded to the provider. `body` is provider-defined JSON; other fields
are passed through according to the rules.

## 4. infer.request

```json
{
  "kind":"infer.request",
  "stream_id":"stream_01",
  "ts_ms":1710000000000,
  "provider":"openai_main",
  "method":"POST",
  "path":"/v1/chat/completions",
  "body":{"model":"gpt-4o-mini","messages":[],"stream":true}
}
```

The current contract accepts only `POST /v1/chat/completions` and `POST /v1/responses`, each under
the minimum compatibility contract below. The endpoint set is not configurable. Omitting `provider`
selects the configured `default_provider`. If the specified provider name is absent from the Worker's
configuration, the payload still parses successfully, but the Worker publishes one ordinary terminal
`60009` result without calling a Provider. `provider` is not included in the forwarded Provider `body`.
`body` must be a JSON object. When present, `body.stream` must be boolean: `true` selects streaming;
absence or `false` selects an ordinary response. The Worker recognizes only this field. Other OpenAI
request fields pass through unchanged for validation by the selected provider. Any other method/path,
a non-object body, or a non-boolean `stream` fails strict parsing and causes the Worker to exit with
an error under Section 3.

Both ordinary and streaming calls begin with `infer.request`; there is no separate streaming request
kind. After obtaining Provider HTTP response headers that can be recorded, a streaming call first
publishes an `infer.result` without `body` to carry the HTTP status and response headers. If the call
fails before that result is written, it publishes `infer.end` directly without adding a result merely
to establish an output chain.

The OpenEvent `principal` is the caller, and `recipients` MUST be empty. The publisher must provide
`stream_id`.

A request's `prev_seq` is optional. When present, it must be a positive OpenEvent seq. Its application
meaning is outside this protocol, and it does not participate in the output chain.

A duplicate request is an `infer.request` with the same `stream_id` as an earlier strictly parsed
`infer.request` in the same Channel. The Worker evaluates messages in OpenEvent seq order. The first
request claims `(channel_id, stream_id)` and is the original request; every later request with the
same identifier is a duplicate, regardless of differences in principal, provider, method, path, or body.
To reject a duplicate, the Worker publishes an ordinary terminal `60005` result whose `prev_seq` points
to that duplicate request's own seq. If the duplicate declares `body.stream=true`, a valid cancel
committed earlier can still be its terminal event under Section 6; in that case no `60005` is published.
The original request retains its claim on `stream_id` even if it receives `60008` for exceeding
`max_payload_bytes` or `60009` for an unconfigured provider. A parsing failure is fatal to the Worker,
produces no result, and does not enter the `stream_id` deduplication state.

### 4.1 Minimum `openai_compatible` Contract

`openai_compatible` means that Model Proxy can complete an HTTP call under the following rules. It
does not require a Provider to implement all OpenAI product models, fields, or business capabilities:

1. The Provider accepts JSON object requests at `POST /v1/chat/completions` and `POST /v1/responses`.
   Model Proxy forwards the request `body` unchanged; it does not validate model names, message contents,
   or other business fields on the Provider's behalf.
2. When `body.stream` is absent or `false`, the Provider returns a complete HTTP response. Its body
   must be a complete UTF-8 JSON value, for both HTTP success and HTTP error responses.
3. When `body.stream=true`, the Provider may return UTF-8 SSE with the `Content-Type` media type
   `text/event-stream`. Chat Completions ends normally with `[DONE]`; Responses ends with
   `response.completed`, `response.failed`, or `response.incomplete`. Each Responses SSE data event must be a JSON object
   with a string `type` naming an event in the
   [official OpenAI event definitions](https://developers.openai.com/api/reference/resources/responses/streaming-events).
   Section 6 defines SSE parsing and terminal mapping.
4. A streaming request may also receive a complete non-SSE JSON response; Model Proxy records it as
   a streaming terminal event under Section 6.
5. The Provider must return a valid HTTP status code and parseable response headers. Model Proxy records
   only the response headers listed in Section 5; other headers are not observable `llm.v1` results.

See [CONFIGURATION.md](CONFIGURATION.md) for supported HTTP response compression.

This is the full compatibility scope on which Model Proxy depends. Providers own the business meaning
of request fields and returned JSON. Responses that violate these transport or terminal rules are handled
as Provider parsing failures or call interruptions.

## 5. infer.result

A request may have multiple `infer.result` messages. Consumers treat an unparseable `llm.v1` message as
a protocol error; they cannot skip it and keep looking for a later result. Before the request is terminal,
the consumer accepts the first result whose `stream_id` and `prev_seq` match that request, and ignores
all later matching results. It cannot skip the first result to choose a later, more suitable one. A first
result containing `body` is an ordinary terminal event, including rejections of duplicate or invalid
requests whose original body specified `stream=true`. A first result without `body` establishes the
streaming output chain. If the first result's shape is invalid for that call, the call reports a protocol
error. A result's `prev_seq` points directly to the request, although unrelated messages may occur
between their global OpenEvent seq values. A result containing `body` is an ordinary terminal event:

```json
{
  "kind":"infer.result",
  "stream_id":"stream_01",
  "prev_seq":12345,
  "ts_ms":1710000001234,
  "status_code":200,
  "headers":[{"name":"content-type","value":"application/json"}],
  "body":{"id":"..."}
}
```

A result without `body` is the first node of a streaming output chain, not a terminal event:

```json
{
  "kind":"infer.result",
  "stream_id":"stream_01",
  "prev_seq":12345,
  "ts_ms":1710000001234,
  "status_code":200,
  "headers":[{"name":"content-type","value":"text/event-stream"}]
}
```

A result's `prev_seq` equals the target request's `request_seq`. A bodyless streaming result must have
a Provider HTTP `status_code` in `100..599`. An ordinary terminal result may use a Provider HTTP status
or `60000`, `60001`, `60002`, `60003`, `60005`, `60007`, `60008`, or `60009`. Only cancel represents
`60004`; it must not appear in a result. `headers` follows these fixed forwarding rules:

1. Match HTTP field names case-insensitively over ASCII and write them in lowercase in the payload.
2. Retain only fields named exactly `content-type`, `retry-after`, or `x-request-id`, or whose names
   start with `x-ratelimit-`.
3. Write retained fields in provider response order. Keep duplicate fields separately without merging
   them or splitting their values on commas.
4. Use the field value strings produced by the Worker's valid HTTP parsing, without further trimming
   whitespace or interpreting the values.
5. Discard all other response fields. If none remain, omit `headers` rather than writing an empty array.

The Worker principal publishes results with only the request principal in recipients. The corresponding
request determines the result shape:

1. A valid non-streaming request uses its first matching result as the terminal response, and that result
   must contain `body`. JSON `null` is an explicitly present body, not an omission. If the first matching
   result has no `body`, the call reports a protocol error; the consumer cannot skip it for a later result.
   Regardless of `Content-Type`, the provider body must decode as one complete JSON value. A decoding
   failure after the Provider input size check produces an ordinary terminal result under Section 5.1.
2. For a streaming request that passes the duplicate, size, and provider configuration checks and enters
   the Provider call, any result published must omit `body`. Unless the Worker has already observed a
   winning cancel, it publishes this result after obtaining recordable Provider HTTP response headers
   and before consuming the first response body data or stream event. If waiting for headers, DNS, TLS,
   connection establishment, or a similar step fails, or the retained headers cannot fit in one result,
   the Worker publishes an interrupted `infer.end` directly, with no result.
3. Rejections of duplicate requests, requests exceeding `max_payload_bytes`, or strictly parsed requests
   naming an unconfigured provider always use an ordinary terminal result containing an error body,
   even when the original payload has `stream=true`. No append/end follows that result. If a valid cancel
   commits first, the terminal resolution rules in Section 6 apply.

Section 3 defines the complete result field set and types. Payload parsing itself does not read history;
the Worker or consumer state machine checks that a result's shape agrees with its request.

### 5.1 Provider Failure Mapping for Ordinary Calls

An ordinary call, whether successful or failed, produces only one `infer.result` containing `body` and
never produces `infer.end`. The mapping is:

| Provider stage | `status_code` | `body` and headers |
| --- | --- | --- |
| HTTP response received with status `100..599` | Original Provider HTTP status | Preserve the JSON-decodable response body unchanged and retain headers under the forwarding rules. HTTP 4xx/5xx is still an ordinary terminal result; callers identify failure from the status. |
| Timeout while waiting for response headers | `60000` | Standard Model Proxy error body; no provider response headers. |
| Explicit DNS resolution failure | `60001` | Standard Model Proxy error body; no provider response headers. |
| Explicit TLS handshake failure | `60002` | Standard Model Proxy error body; no provider response headers. |
| Explicit connection failure or reset before response headers | `60003` | Standard Model Proxy error body; no provider response headers. |
| No-progress timeout reading the body after response headers | `60000` | Discard the incomplete body and headers; write the standard Model Proxy error body. |
| Connection reset or premature end while reading the body after response headers | `60003` | Discard the incomplete body and headers; write the standard Model Proxy error body. |
| Response received, but body is not complete JSON or UTF-8/parsing fails | `60007` | Discard the raw body and headers; write the standard Model Proxy error body. |
| Output payload exceeds `max_payload_bytes` | `60008` | Discard the oversized provider body and headers; write a small standard error body that fits the limit. |

Section 7 maps the standard Model Proxy error body's `error.code` to status codes. These failures do not
automatically retry an ordinary Provider request. A caller must create a new `infer.request` to make
another model call.

## 6. Streaming Events

For a request already classified as a valid streaming request, normal Provider output starts with a
bodyless result followed by zero or more `infer.append` messages. Before that output chain is established,
the streaming call may instead receive an ordinary terminal result containing `body` under Section 5,
representing a duplicate request, an oversized request, or an unconfigured provider. `infer.end` and
`infer.cancel` are independent terminal control events: they are outside the output chain and carry no
`prev_seq`. A streaming call has only three kinds of candidate terminal events: its first matching result
containing `body` before any result has been accepted, an `infer.end` published by the Worker, or an
`infer.cancel` published by a Channel member. Consumers accept the earliest valid candidate in OpenEvent seq order. Later
candidate terminal events and output messages cannot change the outcome.

An `infer.append` cannot follow the request directly. Receiving an append before accepting a bodyless
result must report a protocol error only for that call; the consumer cannot use it to establish the chain.
`stream_id` is a caller-generated string identifying one model call, used by both ordinary and streaming
calls. `request_seq` is not another random identifier: it is the OpenEvent `seq` assigned to the
`infer.request` that started this call. Later duplicate requests may use the same `stream_id`, so append,
end, and cancel must also carry `request_seq` to identify exactly which request event they belong to or
terminate. `request_seq` is known when request publication succeeds, so end or cancel may be sent before
result. Result does not repeat `request_seq`; instead, its `prev_seq` equals the target request's
`request_seq`.

### 6.1 infer.append

```json
{
  "kind":"infer.append",
  "stream_id":"stream_01",
  "request_seq":12345,
  "prev_seq":12346,
  "ts_ms":1710000001240,
  "body":{"id":"...","choices":[{"delta":{"content":"Hi"}}]}
}
```

`prev_seq` equals the preceding bodyless result or append seq. When the response Content-Type media type
is `text/event-stream`, parse it under the standard Server-Sent Events rules used by OpenAI, comparing the
media type case-insensitively and ignoring parameters such as `charset`. The provider keeps the HTTP
response open and sends events made of field lines, with a blank line ending each event. The parser handles
LF, CRLF, and CR line endings, ignores comment lines, and splits each line at the first colon into a field
name and value. If the value begins with an ASCII space, remove that one space; a field without a colon
has an empty value. Append each `data` field's value and a newline in arrival order, then remove the last
appended newline when the event ends. Ignore events with no `data` field. Comments and fields such as
`event`, `id`, and `retry` do not form model events.

Events must not be split at arbitrary network read boundaries. Every completely assembled, non-terminal
`data` JSON event conforming to the event structure in Section 4.1 becomes one append body in Provider
arrival order. The protocol does not interpret
`choices`, `delta`, usage, or Provider segmentation. The Provider terminal markers listed in Section 4.1
create no append: Chat Completions `[DONE]` is not written to a body; Responses terminal events are
preserved unchanged in the end's `body`.

If a streaming request receives an ordinary HTTP response instead of `text/event-stream`, the Worker
first decodes its complete body as JSON regardless of `Content-Type`. On successful decoding, this is
the provider terminal response and becomes one completed `infer.end` containing that JSON body, without
an append. Both the bodyless result and completed end carry the HTTP status; consumers still treat
non-2xx statuses as provider HTTP errors.

The project's minimum Chat Completions and Responses compatibility contract requires JSON responses,
including error responses. A non-event body that cannot decode as JSON is therefore a Provider response
parsing failure, not a compatible response. The Worker discards the raw body and writes a
`60007 interrupted` end with the standard error body. If a bodyless result was already written, it retains the
observed HTTP status and headers while the end uses `60007`; otherwise, the Worker writes the end directly.
Every non-terminal event in an event stream must also be valid JSON. Invalid event JSON produces the
same `60007 interrupted` end. Neither path exposes the raw body to callers.

### 6.2 infer.end

```json
{
  "kind":"infer.end",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001300,
  "status_code":200,
  "end_status":"completed"
}
```

Provider terminal events become `infer.end` directly and are never first published as appends. A Responses
`response.failed` or `response.incomplete` always produces `end_status="failed"`, with the provider terminal event in the end's `body`:

```json
{
  "kind":"infer.end",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001300,
  "status_code":200,
  "end_status":"failed",
  "body":{"type":"response.failed","response":{"id":"..."}}
}
```

Normal Responses completion has the same shape, except `end_status="completed"` and
`body.type="response.completed"`.

`response.incomplete` means that the Provider finished this generation without producing a complete
answer, for example because the output token limit was reached. It uses the failed end shape above,
with `body.type` still set to `response.incomplete` and the entire original event, including
`response.incomplete_details`, preserved. `status_code` retains the actual HTTP status; even HTTP 200
is treated as terminal failure. Previously published appends remain valid.

`request_seq` equals the request seq, and `stream_id` must match that request. End does not identify
which append it follows. It identifies the stream by `request_seq` and participates in terminal resolution
using its own OpenEvent seq. `end_status` is exactly `completed`, `failed`, or `interrupted`:

- `completed`: the Worker received a complete terminating HTTP response or the endpoint's normal terminal
  event from the Provider. This means the provider response ended, not that the call necessarily succeeded.
  Callers treat it as success only when `status_code` is in `200..299`. At the message-schema level `body`
  is optional: a Chat Completions `[DONE]` end omits it; a Responses `response.completed` end contains the
  original terminal event JSON; a non-event HTTP response end contains the ordinary JSON response. Thus
  an HTTP 429 ordinary JSON response may be a `completed` end, but callers must treat it as a provider HTTP
  error.
- `failed`: the provider explicitly reported terminal failure or incomplete generation in its response. `body` is required and
  contains the original provider terminal event JSON; the Worker does not publish that event as an append.
  `status_code` equals the bodyless result's status, so even HTTP 200 may represent terminal failure.
  Consumers MUST treat this as a failed model call regardless of the HTTP status.
- `interrupted`: the Worker did not observe, or could not persist, a Provider terminal marker.
  `status_code` is a Model Proxy extension code and `body` is the standard Model Proxy error object.
  Existing appends remain valid and consumers MUST retain them.

Failed and interrupted ends must contain `body`; no end may contain `headers`. The Worker checks
endpoint-specific completed-body rules against the corresponding request. Parsing one end payload alone
cannot determine the endpoint from that payload.

Completed and failed ends must use a Provider HTTP `status_code` in `100..599`. Interrupted ends may use
only `60000`, `60001`, `60002`, `60003`, `60007`, or `60008`.

The Worker maps the Provider terminal markers in Section 4.1 to completed or failed ends. A Responses
terminal failure is a model outcome, not a transport interruption, even with HTTP status 200. A clean EOF
before the applicable terminal marker means the Provider stream is incomplete and produces an interrupted
end. Invalid stream events/JSON, Provider connection or stream-idle timeouts, connection resets, and Worker
restarts before a terminal event is persisted also produce interrupted ends.

The stream ends at the first accepted result containing `body`, end, or cancel in OpenEvent seq order.
All subsequent results, appends, ends, and cancels are ignored, including publications already in flight
when the terminal event committed but persisted afterward. End does not reference the last append, so
an in-flight append committed after end is simply ignored under the terminal rules; it does not break
an end chain. Closing a local reader or interrupting Subscribe is not a protocol terminal event.

### 6.3 infer.cancel

```json
{
  "kind":"infer.cancel",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001250
}
```

`request_seq` must be the exact OpenEvent `seq` of the target `infer.request`, and `stream_id` must match
that request. Cancel contains no `prev_seq`, body, or recipients; its OpenEvent `recipients` list must
be empty. Any member of the protected or private `llm.v1` Channel may publish cancel. Channel membership
is the protocol's only write authorization.

For a stream that is not yet terminal, cancel itself is the `60004 / STREAM_CANCELLED` terminal event;
no following `infer.end` is required. After observing a winning cancel, the Worker initiates no new output
publication. Outputs already in flight that commit later are ignored under the terminal rules. A cancel
is ignored if its target `request_seq` does not exist, its `stream_id` does not match, or the call is already
terminal. Of multiple cancels for one call, only the one with the smallest OpenEvent seq is accepted.
Results containing `body`, ends, and cancels likewise compete as candidate terminal events, with the
earliest valid candidate in seq order accepted.

## 7. Model Proxy Extension Status Codes

HTTP responses pass through `100..599`. The Worker generates:

| Status code | Meaning |
| --- | --- |
| `60000` | Timeout waiting for provider response headers or lack of progress while reading the response |
| `60001` | DNS resolution failed |
| `60002` | TLS handshake failed |
| `60003` | Connection failed or reset |
| `60004` | `infer.cancel` terminated the stream |
| `60005` | Duplicate `stream_id` rejected |
| `60007` | Model Proxy internal failure, Provider response parsing failure, or restart interruption |
| `60008` | Request, Provider input, or output exceeded the Worker's `max_payload_bytes` |
| `60009` | Request passed strict parsing but names an unconfigured provider |

During Provider connection setup, use the first definite cause observed. If the `response_header_ms`
budget expires first, use `60000` whether the operation is resolving DNS, connecting TCP, negotiating TLS,
sending the request, or waiting for response headers. Use `60001`, `60002`, or `60003` only when an explicit
DNS, TLS, or connection failure is observed before that budget expires. After response headers arrive,
read-idle timeouts use `60000` and explicit connection resets use `60003`. Do not infer an error code from
the current phase after the deadline has expired.

`infer.cancel` itself represents `60004 / STREAM_CANCELLED`. It carries no error body and requires no
`infer.end` to represent cancellation.

Error results/ends generated by the Worker using a Model Proxy extension status code use the following
standard error body. `error.code` must correspond to `status_code`:

```json
{"error":{"code":"MODEL_API_DNS_ERROR","message":"DNS resolution failed","type":"model_proxy_error"}}
```

Provider HTTP error bodies and `response.failed` / `response.incomplete` terminal events retain their original JSON under
Sections 5 and 6.

The mapping is: `60000` to `MODEL_API_TIMEOUT`, `60001` to `MODEL_API_DNS_ERROR`, `60002` to
`MODEL_API_TLS_ERROR`, `60003` to `MODEL_API_CONNECTION_ERROR`, `60005` to `DUPLICATE_REQUEST`,
`60007` to `MODEL_PROXY_INTERRUPTED`, `60008` to `PAYLOAD_TOO_LARGE`, and `60009` to `INVALID_REQUEST`.
Callers use `status_code` to identify the category; `error.code` is for display and logs. If they disagree,
`status_code` takes precedence.

If a streaming request fails during its Provider call, publish only one interrupted `infer.end` whose
`status_code` is the corresponding Model Proxy extension code. Here, failure during the call means a
timeout, disconnect, parsing failure, or payload overflow. Pre-call rejections and failure responses
returned by the Provider follow Sections 5 and 6; restart recovery follows Section 8.2.

1. If the bodyless result has not been written, do not add one.
2. If the bodyless result has already been written, retain its Provider HTTP status and all previously
   published appends. The end records the interruption reason without rewriting existing output.

For example, Provider HTTP 200 followed by a connection reset produces `result.status_code=200` and
`end.status_code=60003`. A timeout before the provider returns headers produces only
`end.status_code=60000`; oversized retained response headers produce only `end.status_code=60008`.
Callers do not need to guess whether the failure occurred before or after the output chain was established.

## 8. Payload Size and Restart Outcomes

### 8.1 `max_payload_bytes`

The Worker's `max_payload_bytes` limits the original strictly parsed request, Provider response headers
and response data, and the result, append, and end payloads the Worker prepares to publish. Byte counts
use the counting objects defined in the configuration document. No message is truncated. On overflow,
discard the Provider content that cannot be recorded and end the call with the following fixed outcome:

| Oversized input/output | Observable outcome |
| --- | --- |
| Original request | Do not call the Provider. Publish an ordinary terminal `60008` result with an error body and `prev_seq` pointing to the request, even when `body.stream=true`. |
| Ordinary response headers or body | Publish an ordinary terminal `60008` result with a small error body. |
| Streaming response headers | Do not publish a bodyless result; publish a `60008 interrupted` end. |
| Streaming event, non-SSE response body, or Provider terminal event containing a body | Do not publish the oversized content; publish a `60008 interrupted` end. Previously published results and appends remain valid. |

This section applies only to requests that pass strict parsing. Parsing failures follow Section 3; visible
partial fields do not allow a failed payload to be converted to `60008`. A strictly parsed request still
claims `stream_id` when its original payload is oversized.

`60008` means only that the Worker's configured `max_payload_bytes` was exceeded, not that OpenEvent
rejected a message. OpenEvent's own payload limit belongs to its publishing contract. If OpenEvent rejects
an output publication, the Worker treats it as a publishing failure and does not convert it to `60008`.

### 8.2 Outcomes After Worker Restart

After restart, the Worker determines state only from strictly valid messages already committed to
OpenEvent. Historical messages follow the same fatal parsing-error rule in Section 3.

Recovery scanning resolves terminal events under Sections 5 and 6. For non-streaming requests, a matching
result containing `body` is terminal. For requests with `body.stream=true`, accept the earliest matching
result containing `body`, valid end, or valid cancel in OpenEvent seq order. Original or duplicate requests
that are already terminal retain their outcomes. A strictly parsed original request with no protocol
terminal event never calls the Provider again: a non-streaming original request receives an ordinary
terminal `60007` result whose `prev_seq` points to that request; a streaming original request receives a
`60007 interrupted` end without `prev_seq`. A duplicate request with no protocol terminal event receives
an ordinary terminal `60007` result regardless of whether `body.stream` is `true`, with `prev_seq` pointing
to that duplicate request's own seq. Recovery states only that the previous processing left no terminal
event. It does not guess whether the Worker would have published `60005`, `60008`, `60009`, or a Provider
terminal event before stopping, and does not reinterpret old requests using new Provider configuration.

This document is the sole current `llm.v1` contract. It retains no compatibility rules from earlier drafts.
