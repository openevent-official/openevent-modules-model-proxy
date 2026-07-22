# Python SDK Usage

[中文版](SDK_USAGE_cn.md)

`openevent.model_proxy_sdk` provides two API layers:

- OpenAI-like client: for migrating common OpenAI call shapes to
  OpenEvent + model-proxy.
- Protocol SDK: for directly publishing and parsing `llm.v1` request/result
  payloads.

## OpenAI-like Client

```python
from openevent.model_proxy_sdk import OpenAI

client = OpenAI(
    openevent_addr="127.0.0.1:9527",
    openevent_token="token-xxx",
    openevent_channel_id=1001,
    openevent_principal=10,
    request_timeout_ms=60000,
)

resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "hello"}],
)

print(resp.choices[0].message.content)
print(resp.openevent_seq)
```

`responses.create(...)` is also supported:

```python
resp = client.responses.create(
    model="gpt-4o-mini",
    input="hello",
)

print(resp.model_dump())
```

### Differences from the Official OpenAI SDK

- This is an OpenAI-like client, not a drop-in replacement for the official SDK.
- Initialization uses OpenEvent parameters and rejects provider parameters such
  as `api_key` and `base_url`.
- Provider base URL, API key, and routing are managed by `model-proxy`
  configuration.
- The worker only forwards methods and paths allowed by the selected provider
  configuration. The defaults cover `POST /v1/chat/completions` and
  `POST /v1/responses`.
- Successful response objects include an extra `openevent_seq` field.
- `stream=True` is not supported.
- Exception types are close to OpenAI-style errors but do not guarantee identical
  class hierarchy.

### Publish Failures and Retries

Publishing an `infer.request` must distinguish a confirmed non-commit from an
uncertain outcome:

- Parameter, authentication, permission, Channel, or payload errors explicitly
  returned by OpenEvent with a non-commit guarantee are confirmed failures. Retry
  with the same `request_id` and payload only after the cause is recoverable or
  has been corrected, and use bounded backoff.
- A broken connection, cancellation, `DEADLINE_EXCEEDED`, `UNKNOWN`, or
  `UNAVAILABLE` does not prove that the message was not committed. Do not
  immediately republish or generate a new `request_id`.
- For an uncertain request publish, use the pre-publish watermark, GetStatus, and
  Fetch to search by `channel_id + request_id`.
- Reuse the original seq when a match is found. Retry the same publish only after
  the complete reconciliation range proves that no match exists.

A timeout waiting for `infer.result` after the request seq was returned is a
different case. The application may start a new model attempt with a new
`request_id`; that is not a retry of the original PublishAutoSeq operation.

The OpenAI-like client's `request_timeout_ms` is one total deadline covering the
initial watermark, PublishAutoSeq, reconciliation, and result wait. `max_retries`
only retries the same frozen request after an uncertain publish is reconciled and
confirmed absent. Explicit authentication, permission, parameter, and payload
failures return immediately; result-wait failures never trigger another model call.

Worker result publishing is defined only in
[RESULT_PUBLISHING.md](RESULT_PUBLISHING.md).

## Protocol SDK

Publish `infer.request` directly:

```python
from openevent.sdk import OpenEventClient
from openevent.model_proxy_sdk import InferRequestInput, create_client, publish_infer_request

openevent = OpenEventClient("127.0.0.1:9527")
client = create_client(openevent, token="token-xxx")

seq = publish_infer_request(
    client,
    channel_id=1001,
    principal=10,
    req=InferRequestInput(
        request_id="req_001",
        method="POST",
        path="/v1/chat/completions",
        body={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": "hello"}],
        },
    ),
    timeout=30.0,
)

print(seq)
```

Parse an OpenEvent message:

```python
from openevent.model_proxy_sdk import InferResult, parse_message

parsed = parse_message(message)

if isinstance(parsed.payload, InferResult):
    print(parsed.payload.request_id)
    print(parsed.payload.status_code)
    print(parsed.payload.body)
```

Protocol fields and status codes are documented in
[LLM_PROTOCOL.md](LLM_PROTOCOL.md).
