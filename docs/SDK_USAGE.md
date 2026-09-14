# Python SDK Usage Guide

[中文版](SDK_USAGE_cn.md)

See [SDK_API.md](SDK_API.md) for complete public signatures, return fields, exceptions, and
closing semantics, and [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for protocol fields.
This document provides common usage examples only.
See the [README build and test instructions](../README.md#build-and-test) for build artifacts, installation, and test dependencies.

## 1. OpenAI-like Client

### 1.1 Ordinary Calls

```python
from openevent.model_proxy_sdk import OpenAI

with OpenAI(
    openevent_addr="127.0.0.1:9527",
    openevent_token="token-xxx",
    openevent_channel_id=1001,
    openevent_principal=10,
    rpc_timeout_ms=30000,
    max_retries=3,
    retry_interval_ms=1000,
) as client:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": "hello"}],
        provider="openai_main",
        stream_id="stream_001",
        prev_seq=42,
    )

    print(response.choices[0].message.content)
    print(response.request_seq)
    print(response.result_openevent_seq)
```

`responses.create(...)` uses the same client. `provider`, `stream_id`, and `prev_seq` are
Model Proxy control parameters; all remaining parameters form the Provider request body.

An ordinary `create()` returns the model response, so it continues waiting for that response after
request publication succeeds. The publishing phase itself does not wait for model output.
The first call still prepares a replay position, after which the subscription connects in the
background; publishing does not wait for subscription establishment or reconnection.
See [SDK_API.md Section 3.2](SDK_API.md#32-starting-a-call) for preparation, publication, and return rules.

### 1.2 Streaming Calls

```python
from openevent.model_proxy_sdk import OpenAI

with OpenAI(
    openevent_addr="127.0.0.1:9527",
    openevent_token="token-xxx",
    openevent_channel_id=1001,
    openevent_principal=10,
) as client:
    stream = client.responses.create(
        model="gpt-4o-mini",
        input="hello",
        stream=True,
    )

    for event in stream:
        print(event.model_dump())
```

Request identifiers are known when `stream` is returned. Response properties update as the SDK
accepts the corresponding messages; no iteration is required first. Even when completion information
is already visible, previously queued chunks must still be read through iteration. See
[SDK_API.md Section 3.4](SDK_API.md#34-streaming-return-object) for when each property becomes known.

Call `stream.close()` explicitly when stopping early. `client.close()` or a `with` block only closes
the underlying OpenEvent client; it neither replaces individual stream cancellation nor waits for
other calls to finish. See [SDK_API.md Section 4](SDK_API.md#4-closing).

### 1.3 Subscription Failure Notification

```python
def handle_subscription_error(error):
    print(error.reason, error.last_status)

client = OpenAI(
    openevent_addr="127.0.0.1:9527",
    openevent_token="token-xxx",
    openevent_channel_id=1001,
    openevent_principal=10,
    on_subscription_error=handle_subscription_error,
)

try:
    response = client.responses.create(model="gpt-4o-mini", input="hello")
finally:
    client.close()
```

Each client instance invokes the callback once if its shared subscription finally fails during
initialization (including the first `GetStatus`), establishment, reading, reconnection, or protocol
validation such as message parsing. Temporary disconnections still being retried do not trigger it;
final subscription failure caused by closing the underlying connection still does. See
[SDK_API.md Section 3.1](SDK_API.md#31-creating-a-client).
The subscription error describes a failure of the entire instance and contains no individual
request identifiers; see [SDK_API.md Section 5](SDK_API.md#5-exceptions) for its fields.
The prohibition on requests after final subscription failure and the two closing operations are
defined in [SDK_API.md Section 4](SDK_API.md#4-closing).
The callback may call `client.close()` on the same instance.

## 2. Protocol SDK

### 2.1 Publishing a Request

```python
from openevent.sdk import OpenEventClient
from openevent.model_proxy_sdk import InferRequestInput, create_client, publish_infer_request

with OpenEventClient("127.0.0.1:9527") as openevent:
    client = create_client(openevent, token="token-xxx", max_retries=3, retry_interval_ms=1000)
    seq = publish_infer_request(
        client,
        channel_id=1001,
        principal=10,
        req=InferRequestInput(
            stream_id="stream_001",
            method="POST",
            path="/v1/chat/completions",
            prev_seq=42,
            body={
                "model": "gpt-4o-mini",
                "messages": [{"role": "user", "content": "hello"}],
            },
        ),
    )

print(seq)
```

This function returns the request's OpenEvent seq without waiting for the model response.

Use the corresponding input models and publishing functions for result, append, end, and cancel;
the public API contract lists them all.

### 2.2 Parsing Messages

```python
from openevent.model_proxy_sdk import InferAppend, InferResult, parse_message

parsed = parse_message(message)

if isinstance(parsed.payload, InferResult):
    if parsed.payload.has_body:
        print(parsed.payload.body)
    else:
        print(parsed.payload.headers)
elif isinstance(parsed.payload, InferAppend):
    print(parsed.payload.body)

print(parsed.seq, parsed.uuid, parsed.ts_ms)
```

Catch the public `PayloadValidationError` for parsing failures and `ResultPublishError` for publishing
failures. Do not infer whether a message was committed from exception text; use the commit state
defined by [RESULT_PUBLISHING.md](RESULT_PUBLISHING.md).
