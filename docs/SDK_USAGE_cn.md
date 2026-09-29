# Python SDK 使用指南

[English version](SDK_USAGE.md)

安装见 [README_cn.md](../README_cn.md#构建和测试)，完整 API 见 [SDK_API_cn.md](SDK_API_cn.md)。

## 1. OpenAI-like 客户端

### 1.1 普通调用

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

`responses.create(...)` 使用相同的客户端。`provider`、`stream_id` 和 `prev_seq` 是 Model Proxy 控制参数；
其余参数组成 Provider 请求 body。

普通 `create()` 等待模型回答；流式调用在 request 发布成功后返回迭代器，见
[调用规则](SDK_API_cn.md#32-发起调用)。

### 1.2 流式调用

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

流的请求标识、响应信息和完成正文见 [流对象属性](SDK_API_cn.md#34-流式返回对象)。

提前停止读取某个流时，应显式调用 `stream.close()`。`client.close()` 或上例的 `with` 只关闭底层 OpenEvent client，
不代替逐个流的取消，也不等待其他调用结束，见 [SDK_API_cn.md 第 4 节](SDK_API_cn.md#4-关闭)。

### 1.3 订阅失败通知

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

共享订阅最终失败时，每个实例通知一次；回调可以关闭同一客户端。触发条件见
[创建客户端](SDK_API_cn.md#31-创建客户端)，故障字段和在途调用结果见 [异常](SDK_API_cn.md#5-异常)。

## 2. 协议 SDK

### 2.1 发布 request

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

这个发布函数返回 request 在 OpenEvent 中的 seq，不等待模型回答。

其他消息的输入模型与发布函数见 [协议 SDK API](SDK_API_cn.md#2-协议-sdk)。

### 2.2 解析消息

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

解析失败抛出 `PayloadValidationError`；发布失败抛出 `ResultPublishError`，通过其
[`commit_state`](RESULT_PUBLISHING_cn.md#4-resultpublisherror) 判断提交状态。
