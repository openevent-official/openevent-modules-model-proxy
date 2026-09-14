# Python SDK 使用指南

[English version](SDK_USAGE.md)

公开 API 的完整签名、返回字段、异常和关闭语义见 [SDK_API_cn.md](SDK_API_cn.md)。协议字段见
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。本文只给出常用调用示例。
构建产物、安装和测试依赖准备见 [README_cn.md 的构建和测试说明](../README_cn.md#构建和测试)。

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

普通 `create()` 返回模型回答，因此会在 request 发布成功后继续等待回答；发布阶段本身不等待模型输出。
首次调用仍需准备消息回放起点，随后订阅连接在后台建立；发布不等待订阅建连或重连完成。
准备、发布与返回结果的规则见 [SDK_API_cn.md 第 3.2 节](SDK_API_cn.md#32-发起调用)。

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

拿到 `stream` 时，请求标识已经确定；响应属性在 SDK 确认对应消息时更新，不必先迭代。即使已经能读到完成信息，
此前排队的 chunk 仍需通过迭代读取。各属性的确定时机见 [SDK_API_cn.md 第 3.4 节](SDK_API_cn.md#34-流式返回对象)。

提前停止读取某个流时，应显式调用 `stream.close()`。`client.close()` 或上例的 `with` 只关闭底层 OpenEvent client，
不代替逐个流的取消，也不等待其他调用结束，见 [SDK_API_cn.md 第 4 节](SDK_API_cn.md#4-关闭)。

### 1.3 订阅失败通知

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

共享订阅因初始化（含首次 `GetStatus`）、建连、读取、重连或消息解析等协议错误而最终失败时，每个客户端实例通过回调通知一次；
仍在重试的临时断线不会触发；关闭底层连接导致的最终订阅失败仍会通知，详见 [SDK_API_cn.md 第 3.1 节](SDK_API_cn.md#31-创建客户端)。
回调收到的订阅异常描述整个实例的故障，不带单次请求标识；异常字段见 [SDK_API_cn.md 第 5 节](SDK_API_cn.md#5-异常)。
订阅最终失败后的禁发规则和两种关闭操作见 [SDK_API_cn.md 第 4 节](SDK_API_cn.md#4-关闭)。
回调可以调用同一实例的 `client.close()`。

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

发布 result、append、end 和 cancel 使用各自的输入模型与发布函数，完整列表见公开 API 契约。

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

解析失败时捕获公开的 `PayloadValidationError`；消息发布失败时捕获 `ResultPublishError`。不要根据异常文本猜测
消息是否已经提交，提交状态以 [RESULT_PUBLISHING_cn.md](RESULT_PUBLISHING_cn.md) 为准。
