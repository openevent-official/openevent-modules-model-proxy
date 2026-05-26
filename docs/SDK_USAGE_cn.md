# Python SDK Usage

[English version](SDK_USAGE.md)

`openevent.model_proxy_sdk` 提供两层 API：

- OpenAI-like 客户端：适合已有 OpenAI 调用形态迁移到 OpenEvent + model-proxy
- 协议 SDK：适合直接发布和解析 `llm.v1` request/result payload

## OpenAI-like 客户端

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

`responses.create(...)` 也可用：

```python
resp = client.responses.create(
    model="gpt-4o-mini",
    input="hello",
)

print(resp.model_dump())
```

### 与 OpenAI 官方 SDK 的差异

- 这是 OpenAI-like 客户端，不是 OpenAI 官方 SDK 的 drop-in replacement
- 初始化参数使用 OpenEvent 信息，不接受 `api_key`、`base_url` 等 provider 参数
- Provider 的 base URL、API key 和路由由 `model-proxy` 配置管理
- 成功返回对象包含额外字段 `openevent_seq`
- 当前不支持 `stream=True`
- 异常类型接近 OpenAI 风格，但不保证与官方 SDK 类型层级完全一致

## 协议 SDK

直接发布 `infer.request`：

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
)

print(seq)
```

解析 OpenEvent message：

```python
from openevent.model_proxy_sdk import InferResult, parse_message

parsed = parse_message(message)

if isinstance(parsed.payload, InferResult):
    print(parsed.payload.request_id)
    print(parsed.payload.status_code)
    print(parsed.payload.body)
```

协议字段和状态码见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)。
