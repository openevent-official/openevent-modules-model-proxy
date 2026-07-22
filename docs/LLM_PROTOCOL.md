# LLM Protocol llm.v1

[中文版](LLM_PROTOCOL_cn.md)

> Status: current specification
> Scope: model-proxy payloads and channel descriptions for OpenEvent channels
> with `protocol="llm.v1"`

## 0. Design Assumptions

This protocol assumes that participants follow the `llm.v1` rules and
OpenEvent ACLs:

- Callers write requests with the protocol-defined `infer.request` semantics.
- `model-proxy` writes results with the protocol-defined `infer.result`
  semantics.
- Subscribers interpret messages according to protocol fields.

Bypassing the SDK, forging fields, setting `principal` / `recipients`
incorrectly, sending messages to a non-target proxy, or constructing other
inputs that violate this protocol is not normal behavior that `llm.v1` must
remain compatible with.

## 1. Channel Conventions

All LLM channels MUST set:

```text
protocol = "llm.v1"
```

`description` MUST be a JSON string:

```json
{
  "version": "v1",
  "updated_at_ms": 1710000000000,
  "metadata": {}
}
```

Field constraints:

- `version`: currently fixed to `v1`.
- `updated_at_ms`: millisecond timestamp.
- `metadata`: optional object for static deployment or business-domain
  extension information.

One `channel_id` corresponds to one model-proxy session domain. The protocol
does not require the description to store provider credentials, base URLs,
model lists, or member lists. Provider credentials and routing configuration
are managed by the `model-proxy` configuration file. Channel ACLs and members
are managed by OpenEvent.

LLM channel constraints:

- `visibility` MUST NOT be `VISIBILITY_PUBLIC`; only `VISIBILITY_PROTECTED` or
  `VISIBILITY_PRIVATE` may be used.
- Each `llm.v1` channel MUST be consumed by exactly one `model-proxy`
  principal/process. Deployment or bootstrap configuration guarantees this
  uniqueness.
- Business callers and the `model-proxy` principal must both be channel members;
  otherwise OpenEvent writes or directed result delivery may fail.

`infer.request` does not use OpenEvent `recipients` to target the proxy. The
target proxy is the single `model-proxy` bound to the channel in deployment and
startup configuration. The OpenEvent channel member list does not express member
roles. The concrete provider is selected by `model-proxy` from its own
configuration.

## 2. Common Rules

All `llm.v1` payloads are UTF-8 JSON objects:

```json
{
  "kind": "infer.request",
  "request_id": "req_xxx",
  "ts_ms": 1710000000000,
  "body": {}
}
```

Common fields:

- `kind`: required; either `infer.request` or `infer.result`.
- `request_id`: required; unique within the same `channel_id`; length `1..128`;
  may contain only ASCII letters, digits, `.`, `_`, `:`, and `-`.
- `ts_ms`: required; Unix millisecond timestamp (UTC).
- `body`: required; any JSON-representable business payload value: object,
  array, string, number, boolean, or null.

Protocol fields are validated strictly. Unknown fields, missing required fields,
or mismatched field types are invalid payloads.

### 2.1 OpenEvent Top-Level Fields

`principal` is a top-level OpenEvent EventMessage field and is not stored inside the
`llm.v1` payload. All source identity checks in this protocol use the OpenEvent
EventMessage `principal`:

- `infer.request`: OpenEvent `principal` must be the business caller principal
  that submits the inference request.
- `infer.result`: OpenEvent `principal` must be the `model-proxy` principal.

Payloads MUST NOT contain identity or secret fields such as `source_principal`,
`provider_api_key`, or `api_key`. Provider authentication information can only
come from `model-proxy` configuration.

## 3. infer.request

`infer.request` means a business module is asking `model-proxy` to call a model
service.

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

Rules:

- Business modules SHOULD write through
  `openevent.model_proxy_sdk.publish_infer_request(...)`.
- OpenEvent top-level fields: `principal` is the business caller principal, and
  `recipients` MUST be empty.
- `kind` is required and fixed to `infer.request`.
- `request_id` is required and must be unique within the same `channel_id`; its
  length and character set must satisfy the common rules.
- `method` is required and must be one of `GET`, `POST`, `PUT`, `PATCH`, or
  `DELETE`.
- `path` is required; it must start with `/` and must not contain a scheme, host,
  or query string. Example: `/v1/chat/completions`.
- `ts_ms` is required; Unix millisecond timestamp (UTC).
- `body` is required and is constructed according to the target model service
  API. Provider business fields such as the model name are checked by the
  backend.
- `prev_seq` is optional. The protocol does not constrain its business
  semantics, but when present it must be a positive integer.

`request_id` generation rules:

- The OpenAI-like Agent SDK generates an ID in `req_<uuid4 hex>` format when the
  caller does not pass an explicit `request_id`. Collision probability is
  negligible.
- The low-level protocol SDK method `publish_infer_request(...)` does not
  generate `request_id`; it only validates the caller-provided value against the
  length and character-set constraints. Business code that uses the protocol SDK
  directly must guarantee uniqueness within the same `channel_id`.

If the same `request_id` appears more than once in the same `channel_id`,
`model-proxy` MUST NOT call the model API again and MUST write a rejecting
`infer.result` with `status_code=60005`. Every `infer.request` in the OpenEvent
log is identified by its own `seq`. The final `infer.result.prev_seq` for the
original request points to the original request seq; the rejecting
`infer.result.prev_seq` for a duplicate request points to that duplicate request
seq. Therefore, the same `request_id` may have multiple result log events, but
each result corresponds to exactly one concrete request log event.

## 4. infer.result

`infer.result` is the final response from `model-proxy` for one
`infer.request`.

```json
{
  "kind": "infer.result",
  "request_id": "req_xxx",
  "prev_seq": 12345,
  "ts_ms": 1710000001234,
  "status_code": 200,
  "headers": [
    {"name": "content-type", "value": "application/json"},
    {"name": "x-ratelimit-policy", "value": "requests;w=60"},
    {"name": "x-ratelimit-policy", "value": "tokens;w=60"}
  ],
  "body": {}
}
```

Rules:

- `model-proxy` MUST write through
  `openevent.model_proxy_sdk.publish_infer_result(...)`.
- OpenEvent top-level fields: `principal` is the `model-proxy` principal, and
  `recipients` is fixed to the sender of the corresponding `infer.request`.
- `kind` is required and fixed to `infer.result`.
- `request_id` is required and must equal the corresponding
  `infer.request.request_id`.
- `prev_seq` is required and must be the corresponding `infer.request.seq`.
- `ts_ms` is required; Unix millisecond timestamp (UTC).
- One `infer.request`, uniquely identified by OpenEvent `seq`, may have only one
  final `infer.result`.
- `status_code` and `body` are passed through with HTTP semantics. If there is no
  HTTP response, the proxy writes an extended `status_code`.
- When an upstream HTTP response is received, the proxy does not rewrite the
  upstream status code. It stores only the fixed safe response-header allowlist.
  JSON response bodies are written to `body` as JSON values.
  Non-JSON response bodies are written according to the non-JSON response rule
  in this document.

`headers` rules:

- Type: `[{ "name": string, "value": string }]`.
- Each element corresponds to one HTTP header field line. Repeated field names
  are allowed, for example multiple `x-ratelimit-policy` headers.
- Header names are case-insensitive by HTTP standards. Stored names should be
  normalized to lower case.
- When an HTTP response is received, filtered `headers` SHOULD be included. When
  the proxy generates an extended error itself, `headers` MUST be absent.

## 5. status_code

When an upstream HTTP response is received, `status_code` passes through the
standard HTTP status code (`100..599`).

When no HTTP response is received or the proxy rejects the request locally, use
extended codes:

- `60000`: model API request timed out.
- `60001`: DNS resolution failed.
- `60002`: TLS handshake failed.
- `60003`: connection failed or was reset.
- `60004`: request was cancelled locally.
- `60005`: duplicate `request_id` was rejected.
- `60006`: reserved; no longer used for upstream non-JSON responses.
- `60007`: proxy internal error.
- `60008`: payload exceeds the OpenEvent deployment limit.
- `60009`: request payload is invalid and cannot be processed as a valid
  inference request.
- `60010`: request method or path is not allowed by the selected provider
  configuration. The proxy does not send an HTTP request in this case.

When the proxy generates an extended `status_code`, `body` uses a standard error
shape:

```json
{
  "error": {
    "code": "MODEL_API_TIMEOUT",
    "message": "model API request timed out",
    "type": "model_proxy_error"
  }
}
```

## 6. Pass-Through Boundary

- `infer.request.body` passes through according to the target model service API.
- `infer.result.status_code` and `infer.result.body` pass through with HTTP
  semantics.
- `infer.result.headers` stores only `content-type`, `retry-after`, `x-request-id`,
  and rate-limit headers from the upstream response.
- Connection errors, timeouts, duplicate request rejections, and other cases with
  no upstream HTTP response use extended `status_code` values and the standard
  error `body`.
- The first version supports upstream non-JSON responses. In that case,
  `status_code` still passes through from the HTTP response, `headers` are
  filtered by the fixed allowlist, and `body` is written as a JSON object:

```json
{
  "non_json_body": {
    "encoding": "base64",
    "content_type": "text/plain; charset=utf-8",
    "data": "..."
  }
}
```

`content_type` comes from the upstream `content-type` response header; if absent,
it is an empty string. `data` is the base64 encoding of the raw upstream response
body bytes. The proxy MUST NOT truncate this JSON payload. If the encoded payload
exceeds the OpenEvent payload limit, the proxy must write a proxy extended error
result with `status_code=60008`.

- Business code MUST NOT pass provider credentials, base URLs, or provider
  selection fields through the payload. These are managed by `model-proxy`
  configuration.
- A syntactically valid `method` and `path` are still subject to the selected
  provider's exact method/path allowlist. Requests outside that policy receive
  `status_code=60010` without reaching the provider.

### 6.1 Payload Size Constraints

The recommended OpenEvent `payload` size limit for first-version deployments is
**16 MiB**.

Constraints:

- Both `infer.request` and `infer.result` must fit completely within the
  OpenEvent server's configured payload limit.
- 16 MiB is the first-version deployment recommendation for non-streaming text,
  tool calls, and JSON responses. Content beyond this limit should be handled by
  a future streaming protocol or external object-storage reference.
- The proxy MUST NOT write a truncated JSON payload. When a provider response
  exceeds the OpenEvent payload limit, it should write a proxy extended error
  result with `status_code=60008`.
- Callers should not depend on single non-streaming payloads larger than 16 MiB
  being transmitted reliably.

## 7. Timeouts

Normally, one `infer.request` corresponds to one `infer.result`. If no
`infer.result` is observed before the caller's request wait timeout, the
business module may treat the request as timed out.

The business module decides how to retry after a timeout. `model-proxy` rejects
duplicate `request_id` values idempotently. If the business module needs to start
a new model call, it should use a new `request_id`.

Starting a new model call applies only after the request was successfully
published and assigned a seq, but waiting for its result timed out. If
PublishAutoSeq itself ends with a broken connection, deadline, or another
uncertain outcome, the caller must first reconcile the OpenEvent log by
`channel_id + request_id`. Retry the same publish only after confirming that the
original request was not committed; do not immediately write a new request ID.

## 8. Versioning

- `llm.v1` only receives backward-compatible additions.
- Breaking changes use a new channel protocol, such as `llm.v2`.
- SDK major versions should stay aligned with protocol major versions.
- The first version does not support `stream=True`. Future streaming mode will be
  added through new `kind` values and will not reuse the current final-response
  semantics of `infer.result`.
