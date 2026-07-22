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
- Worker 只转发所选 provider 配置允许的 method 和 path；默认覆盖
  `POST /v1/chat/completions` 与 `POST /v1/responses`
- 成功返回对象包含额外字段 `openevent_seq`
- 当前不支持 `stream=True`
- 异常类型接近 OpenAI 风格，但不保证与官方 SDK 类型层级完全一致

### 发布失败与重试

向 OpenEvent 发布 `infer.request` 时，必须区分明确未提交与提交结果不确定：

- 参数、认证、权限、Channel、payload 等由 OpenEvent 明确返回且保证未提交的错误，才属于明确失败。
  只有失败原因可恢复或已经修复时，才允许使用同一 `request_id` 和同一 payload 有界重试。
- 连接中断、取消、`DEADLINE_EXCEEDED`、`UNKNOWN`、`UNAVAILABLE` 等不能证明消息未提交。
  此时不得直接重发，也不得立即生成新的 `request_id`。
- request 发布结果不确定时，先用发布前记录的消息水位、GetStatus 和 Fetch，按
  `channel_id + request_id` 查找已写入的 request。
- 找到消息时继续使用原 seq；确认扫描范围内不存在后，才允许重试同一发布。

request 已成功发布并取得 seq 后，等待 `infer.result` 超时是另一个场景。业务可以决定开始一次新的
模型 attempt，并为新 attempt 生成新的 `request_id`；这不属于原 PublishAutoSeq 的重试。

OpenAI-like 客户端的 `request_timeout_ms` 是覆盖发布前水位、PublishAutoSeq、发布对账和等待结果的
统一总 deadline。`max_retries` 只在发布结果不确定、且完整对账确认 request 不存在后重试同一份
冻结 request；明确的认证、权限、参数和 payload 错误立即返回，等待 result 失败也不会触发另一次
模型调用。

Worker 的 result 发布只在 [RESULT_PUBLISHING.md](RESULT_PUBLISHING.md) 中定义。

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
    timeout=30.0,
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
