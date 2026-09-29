# Python SDK Usage Guide

[中文版](SDK_USAGE_cn.md)

See the [README](../README.md#build-and-test) for installation and [SDK_API.md](SDK_API.md)
for the complete API.

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

Ordinary `create()` waits for the model response; a streaming call returns an iterator after
request publication succeeds. See the [call rules](SDK_API.md#32-starting-a-call).

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

See [stream properties](SDK_API.md#34-streaming-return-object) for request identifiers,
response information, and the completion body.

Call `stream.close()` explicitly when stopping early. `client.close()` or a `with` block only closes
the underlying OpenEvent client; it neither replaces individual stream cancellation nor waits for
other calls to finish. See [SDK_API.md Section 4](SDK_API.md#4-closing).

### 1.3 Subscription Failure Notification

```python
from openevent.model_proxy_sdk import OpenAI


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

Each instance invokes the callback once on final subscription failure; the callback may close
the same client. See [client creation](SDK_API.md#31-creating-a-client) for trigger conditions
and [exceptions](SDK_API.md#5-exceptions) for diagnostic fields and in-flight call outcomes.

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

See the [protocol SDK API](SDK_API.md#2-protocol-sdk) for the other input models and publishing functions.

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

Parsing failures raise `PayloadValidationError`; publishing failures raise `ResultPublishError`.
Read its [`commit_state`](RESULT_PUBLISHING.md#4-resultpublisherror) to determine the commit outcome.
