# Python SDK API 契约

[English version](SDK_API.md)

> 状态：当前公开契约

本文是 `openevent.model_proxy_sdk` 公开 Python API 的唯一权威文档。协议字段和状态机见
[LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)，单条消息发布结果见
[RESULT_PUBLISHING_cn.md](RESULT_PUBLISHING_cn.md)。[SDK_USAGE_cn.md](SDK_USAGE_cn.md) 只提供使用示例。

## 1. 两层 API

- 协议 SDK：构造、发布和解析五种 `llm.v1` 消息。
- OpenAI-like 客户端：用同步的 `chat.completions.create(...)` 和 `responses.create(...)` 发起调用并等待结果。

所有本模块公开类型都从 `openevent.model_proxy_sdk` 导入。

```python
from openevent.model_proxy_sdk import (
    InferAppend,
    InferAppendInput,
    InferCancel,
    InferCancelInput,
    InferEnd,
    InferEndInput,
    InferRequest,
    InferRequestInput,
    InferResult,
    InferResultInput,
    ModelProxyProtocolClient,
    OpenAI,
    OpenAIChunk,
    OpenAIResponse,
    OpenAIStream,
    ParsedMessage,
    UNSET,
    create_client,
    parse_message,
    parse_payload,
    publish_infer_append,
    publish_infer_cancel,
    publish_infer_end,
    publish_infer_request,
    publish_infer_result,
)
```

本 API 使用的 OpenEvent 类型从 OpenEvent SDK 的公开路径导入：

```python
from openevent.sdk import OpenEventClient, openevent_pb2

EventMessage = openevent_pb2.EventMessage
```

## 2. 协议 SDK

### 2.1 客户端

```python
create_client(
    openevent_client: OpenEventClient,
    token: str,
    max_retries: int = 3,
    retry_interval_ms: int = 1000,
) -> ModelProxyProtocolClient
```

`openevent_client` 由调用方创建并负责关闭。`token` 是发布消息时使用的 OpenEvent token。`max_retries` 是每个
OpenEvent 操作首次失败后的额外尝试次数，默认 `3`，必须是非负整数；`retry_interval_ms` 是每次额外尝试前的
固定等待间隔，默认 `1000`，必须是正整数毫秒，二者都不接受 `bool`。普通 RPC 和单条可靠发布共用这两个
配置值，各操作分别计数；次数和错误分类分别见 [普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md) 与
[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)。`token` 必须是非空字符串；这些构造参数不合法时抛出
`ConfigurationError`。

### 2.2 输入模型

输入模型全部使用关键字参数：

```python
InferRequestInput(*, stream_id, method, path, body, provider=None, prev_seq=None)
InferResultInput(*, stream_id, prev_seq, status_code, headers=None, body=UNSET)
InferAppendInput(*, stream_id, request_seq, prev_seq, body)
InferEndInput(*, stream_id, request_seq, status_code, end_status, body=UNSET)
InferCancelInput(*, stream_id, request_seq)
```

`UNSET` 表示不生成该 JSON 字段；它与 Python `None` 不同。例如 `InferResultInput(body=UNSET)` 生成不带 body 的
流式链头，`InferResultInput(body=None)` 生成带 `"body": null` 的普通终态。输入模型不接收 `kind` 和 `ts_ms`；
对应发布函数自动生成并冻结这两个字段。各字段的类型、范围和组合限制只看协议文档。
`provider=None` 和 request 的 `prev_seq=None` 表示省略对应字段；`headers=None` 表示省略 headers。

### 2.3 发布函数

```python
publish_infer_request(client, channel_id, principal, req) -> int
publish_infer_result(client, channel_id, principal, request_principal, result) -> int
publish_infer_append(client, channel_id, principal, request_principal, event) -> int
publish_infer_end(client, channel_id, principal, request_principal, event) -> int
publish_infer_cancel(client, channel_id, principal, cancel) -> int
```

返回值是本次消息提交后的 OpenEvent `seq`。构造输入模型或发布时发现参数不符合 `llm.v1`，会在调用 OpenEvent 前抛出
`PayloadValidationError`。输入校验通过后，函数只在发布相关 RPC 取得该 seq 时成功返回；发布流程最终失败时抛出
`ResultPublishError`。调用方不传 OpenEvent 消息 UUID，也不能从 Subscribe 消息替代发布函数的返回值。

result、append 和 end 的 `request_principal` 是原 request 的 OpenEvent 发布 principal，发布函数据此设置输出消息的
recipients。request 和 cancel 的 recipients 为空。`channel_id`、`principal` 和 `request_principal` 必须是正整数，
`bool` 不算整数。

### 2.4 解析函数和模型

```python
parse_payload(payload: bytes) -> InferRequest | InferResult | InferAppend | InferEnd | InferCancel
parse_message(message: EventMessage) -> ParsedMessage
```

`parse_payload` 严格解析 UTF-8 JSON payload。`parse_message` 还返回 OpenEvent 消息元数据：

```text
ParsedMessage.payload
ParsedMessage.uuid
ParsedMessage.seq
ParsedMessage.channel_id
ParsedMessage.principal
ParsedMessage.recipients
ParsedMessage.object_keys
ParsedMessage.ts_ms
```

`ParsedMessage.ts_ms` 是 OpenEvent 服务端收到发布请求的时间，语义以
[OpenEvent API 契约](../openevent-sdk/docs/API_cn.md)为准；`ParsedMessage.payload.ts_ms` 是协议发布方写入 payload 的时间。
五种只读 payload 模型分别是 `InferRequest`、`InferResult`、`InferAppend`、`InferEnd` 和 `InferCancel`。
`InferResult.has_body` 和 `InferEnd.has_body` 用于区分 body 缺失和 JSON null。

严格解析失败时抛出 `PayloadValidationError`：

```text
code: str
kind: str | None
stream_id: str | None
```

`code` 使用以下固定值：

| `code` | 含义 |
| --- | --- |
| `INVALID_JSON` | payload 不是合法 UTF-8 JSON |
| `INVALID_PAYLOAD` | JSON 顶层不是 object，或字段组合不合法 |
| `MISSING_REQUIRED_FIELD` | 缺少当前 kind 的必填字段 |
| `UNKNOWN_FIELD` | 出现当前 kind 不允许的字段 |
| `INVALID_KIND` | kind 缺失、类型错误或取值不支持 |
| `INVALID_STREAM_ID` | stream_id 不符合协议 |
| `INVALID_TS_MS` | ts_ms 不符合协议 |
| `INVALID_PROVIDER` | provider 不符合协议 |
| `INVALID_METHOD` | method 不符合协议 |
| `INVALID_PATH` | path 不符合协议 |
| `INVALID_PREV_SEQ` | prev_seq 不符合协议 |
| `INVALID_REQUEST_SEQ` | request_seq 不符合协议 |
| `INVALID_STATUS_CODE` | status_code 不符合协议 |
| `INVALID_HEADERS` | headers 或其中的名称、值不符合协议 |
| `INVALID_END_STATUS` | end_status 不符合协议 |
| `INVALID_BODY` | body 或 request body 中的 stream 不符合协议 |
| `INVALID_PUBLISH_ARGUMENT` | Channel 或 principal 等 OpenEvent 发布参数不合法 |

只有原始 payload 已经解码为 JSON object，并且其中存在可独立识别的合法字段时，`kind` 或 `stream_id` 才有值；
字段有值不表示整条消息合法。

## 3. OpenAI-like 客户端

### 3.1 创建客户端

```python
OpenAI(
    *,
    openevent_addr: str,
    openevent_token: str,
    openevent_channel_id: int,
    openevent_principal: int,
    rpc_timeout_ms: int = 30000,
    max_retries: int = 3,
    retry_interval_ms: int = 1000,
    on_subscription_error: Callable[[OpenEventSubscriptionError], None] | None = None,
)
```

客户端按地址创建并持有自己的 OpenEvent 连接，不接受外部 `OpenEventClient` 或 `grpc.Channel`。`rpc_timeout_ms`
和 `retry_interval_ms` 必须是正整数毫秒，`max_retries` 必须是非负整数，三者都不接受 `bool`。
`rpc_timeout_ms` 限制每次非流式 OpenEvent RPC 和 Subscribe 的接受确认，不是模型调用总时长；已经建立的
Subscribe 没有整个流的 deadline。`max_retries` 和 `retry_interval_ms` 与第 2.1 节含义相同，适用于本实例的
普通 OpenEvent RPC 和 request、cancel 的可靠发布。

构造参数缺失、值或类型不合法时抛出 `ConfigurationError`。签名之外的参数由 Python 按普通调用规则抛出
`TypeError`；客户端不接受 `api_key`、`base_url`、外部 OpenEvent client 或外部 gRPC channel。

`on_subscription_error` 在共享订阅的初始化、建立、读取或重连最终失败时，每个实例同步调用一次，参数是按第 5 节构造的
`OpenEventSubscriptionError`。初始化失败包括首次 `GetStatus` 准备回放起点失败，即使此时尚未创建 Subscribe 也会通知；
协议解析失败或请求一致性检查失败导致共享订阅最终失败时，同样通知。

可重试故障尚在重试或重连期间不调用回调；遇到永久错误或用尽重试次数才算最终失败。上述订阅协议错误直接导致最终失败。
回调用于及时通知实例已经停止接收新消息；各个调用何时抛出订阅异常，仍按第 5 节的已有输出和在途发布规则处理。
回调抛出的异常只记录，不替换订阅错误。`OpenAI.close()` 导致的订阅错误同样按现有重试和最终失败规则处理，
不豁免回调；回调可以关闭同一个客户端，`close()` 不等待回调退出。

### 3.2 发起调用

```python
client.chat.completions.create(**kwargs)
client.responses.create(**kwargs)
```

两个方法都接受可选控制参数 `provider`、`stream_id` 和 `prev_seq`。这些参数写入 `llm.v1` request 的顶层字段，
不会进入 Provider body。`stream_id` 省略时生成 `stream_<uuid4 hex>`；`provider` 省略时由 Worker 使用默认 provider；
`prev_seq` 省略时不生成该字段。其他关键字参数组成对应的 OpenAI 请求 body。`stream=True` 返回
`OpenAIStream`，缺失或为 `False` 时等待普通结果。

首次调用先通过一次 `GetStatus` 准备消息回放起点；并发首次调用共用这次准备结果。起点准备好后，订阅连接在后台建立，
request 发布不等待 Subscribe 接受确认；订阅断线重连期间也允许发起新调用。连接建立或恢复后，SDK 从保存的位置读回
期间提交的 request 和模型输出。订阅已经最终失败时，按第 5 节拒绝新调用；底层连接关闭后的调用按第 4 节处理。

如果本地订阅读取线程创建或启动失败且线程未启动，本次调用原样抛出该异常，不发布 request，也不触发订阅错误回调；
客户端保留首次准备前的状态，等待中的其他调用或后续调用可重新准备。

发布 request 和等待模型回答是两个阶段。发布阶段只等待可靠发布 API 返回 seq 或发布错误，不等待 Worker 的输出。
发布成功后，普通 `create()` 等待模型回答；流式 `create()` 返回 `OpenAIStream`，业务调用方通过迭代器读取输出，
不要求返回流对象前订阅已经建立或已经收到模型响应头、正文。已经形成的订阅错误仍按第 5 节处理。
公开返回对象的 `request_seq` 只取自可靠发布 API 的成功返回值。

共享订阅可以在发布 API 尚未返回时接收并暂存该调用的输出，同时继续处理其他调用的消息。这些输出供普通 `create()`
返回回答或流式迭代使用；发布流程不读取这些输出，也不依赖它们结束。该调用在发布成功前不会向业务调用方
交付模型结果或返回流对象；发布最终失败时，丢弃该调用暂存的输出并原样抛出 `ResultPublishError`，不因已经观察到
request 或模型输出而改写发布结论。

### 3.3 普通返回对象

普通成功返回 `OpenAIResponse`：

```text
body
stream_id: str
request_seq: int
result_openevent_seq: int
status_code: int
headers
model_dump()
```

`body` 保留完整 Provider JSON 值，包括 null、数组或标量；`model_dump()` 返回等价的 JSON 值副本。body 是 object
时可以递归使用只读属性访问。若 Provider 字段与 SDK 固定属性同名，SDK 固定属性优先，原始 Provider 字段仍可从
`body` 读取。

### 3.4 流式返回对象

`OpenAIStream` 是同步、可关闭的迭代器。每条 append 返回 `OpenAIChunk`；其 `body`、只读属性和
`model_dump()` 与普通返回对象规则相同，并提供 `append_openevent_seq: int`。

流对象的只读属性供业务调用方查看，SDK 能确定其值时就更新，不等待业务调用方执行 `next()` 或 `for` 迭代：

| 属性 | 含义与确定时机 |
| --- | --- |
| `stream_id` | 调用方提供或 SDK 生成的字符串调用标识，在发布 request 前确定；返回流对象时已有值。 |
| `request_seq` | request 写入 OpenEvent 后得到的正整数 seq，在可靠发布 API 成功返回时确定；返回流对象时已有值。 |
| `result_openevent_seq` | 首个被接受的 result 的 OpenEvent seq，由订阅线程校验并按协议接受该消息时确定。 |
| `response_status_code` | 与上项同一条 result 的状态码，与上项一起更新。 |
| `response_headers` | 与上项同一条 result 的响应头，与上项一起更新。 |
| `terminal_has_body` | 被接受的 `end_status="completed"` 的 end 是否包含 body，由订阅线程接受该消息时确定。 |
| `terminal_body` | 上项为 `True` 时保留该 end 的完整 JSON body，包括 JSON null；上项为 `False` 时为 `None`，与上项一起更新。 |

尚未接受对应 result 或 completed end 时，相应属性为 `None`。没有 body 的 completed end 对应
`terminal_has_body=False`、`terminal_body=None`；带有 JSON null body 的 completed end 对应
`terminal_has_body=True`、`terminal_body=None`。其他终态不设置这两个 completed end 属性。

订阅线程在发布 API 返回前接受的消息先保存在调用内部；发布成功返回流对象时，其中已经确定的属性直接可读。
读取属性只查看 SDK 已保存的信息，不等待后续消息，也不消费输出队列。流对象返回后，已经接受的响应信息不会因业务迭代、
关闭或订阅失败而清空，被忽略的重复消息和终态后的消息也不会覆盖它们。

属性可见不表示业务已经读完输出：即使已经能读到 `terminal_body`，此前排队的 chunk 仍按顺序由迭代器返回，
然后迭代器才结束或抛出对应异常。流失败时，已经返回的 chunk 仍然有效。

## 4. 关闭

`OpenAIStream.close()` 同步关闭当前流，不影响同一客户端的其他调用。实例未进入 `FAILED` 且 SDK 尚未接受本流终态时，它发布一条 cancel；发布失败时抛出
原始 `ResultPublishError`。实际发布 cancel 并成功时，只证明 cancel 已经提交，不表示 Worker 或 Provider 已经停止。
已经接受终态时，只完成本地关闭，不再发布 cancel。关闭前 SDK 已经接收的 chunk 仍可继续迭代；没有已经接受的终态或错误时，
这些 chunk 返回完后结束迭代。重复或并发关闭同一流共享同一个提交结果，不重复发布 cancel；若首次关闭发布失败，
后续关闭分别抛出新建的等价 `ResultPublishError`，保留异常类型、消息和公开字段，不共享异常对象或此前的 Python 调用栈。

实例已经进入 `FAILED` 时，`stream.close()` 只完成本流的本地关闭，不分配 UUID、不发布 cancel，
也不发起其他 OpenEvent 请求。已有输出和错误继续按第 5 节交付；关闭操作本身不再次抛出订阅错误。
已经开始的 cancel 调用按第 5 节收口，真实发布错误仍原样返回。

`OpenAI.close()` 只调用实例持有的 `OpenEventClient.close()`，由 OpenEvent SDK 关闭自己的 gRPC channel；
Model SDK 不直接操作 channel，也不维护额外的客户端关闭状态。重复关闭遵循底层 client 的幂等语义，
关闭调用本身的耗时由底层 `OpenEventClient.close()` 决定。

Model SDK 不额外等待发布取得最终结论、订阅读取线程或用户回调退出，不自动关闭各个流或发布 `infer.cancel`，
也不清空结果队列、冻结接收、停止重试或构造专门的关闭异常。request 发布、Subscribe 和结果读取继续走原有链路：
底层返回错误后，按普通 RPC 重试、可靠发布和第 5 节异常规则处理；订阅最终失败时仍进入 `FAILED`、通知回调并唤醒等待者。
`FAILED` 的禁发规则不因调用 `close()` 改变。

`close()` 返回只表示底层 client 已经关闭，不保证其他调用或后台任务已经退出，也不撤销已提交的消息或保证 Worker、Provider 停止。
已经收到的输出、终态和错误仍按原有队列及发布结论规则交付，不因关闭而改成另一种结果。关闭后再次调用 `create()` 不会重新创建
底层 client：已有 `FAILED` 时抛出保存的订阅错误，否则沿用原有调用链路处理底层错误。

客户端支持上下文管理器；调用方必须显式关闭客户端或使用 `with`，不能依赖垃圾回收决定关闭时间。

## 5. 异常

主要映射如下：

| 场景 | 异常 |
| --- | --- |
| HTTP `400` | `BadRequestError` |
| HTTP `401` | `AuthenticationError` |
| HTTP `403` | `PermissionDeniedError` |
| HTTP `404` | `NotFoundError` |
| HTTP `409` | `ConflictError` |
| HTTP `422` | `UnprocessableEntityError` |
| HTTP `429` | `RateLimitError` |
| HTTP `500..599` | `InternalServerError` |
| 其他非 2xx HTTP 状态 | `APIStatusError` |
| Model Proxy 状态 `60000` | `APITimeoutError` |
| Model Proxy 状态 `60001..60003` | `APIConnectionError` |
| Model Proxy 状态 `60004` 或获胜 cancel | `StreamCancelledError` |
| Model Proxy 状态 `60009` | `BadRequestError` |
| Model Proxy 状态 `60005/60007/60008` | `APIError` |
| `end_status="failed"` | `APIError` |
| 协议输入模型或调用控制参数不合法 | `PayloadValidationError` |
| 协议客户端或 OpenAI-like 客户端构造参数不合法 | `ConfigurationError` |
| 目标调用的消息形态或流链错误 | `ProtocolError` |
| 共享订阅初始化、建立、读取或重连最终失败，或订阅协议校验失败 | `OpenEventSubscriptionError` |
| 任一 `publish_infer_*` 发布流程失败 | `ResultPublishError` |

流式调用先按 OpenEvent seq 接受最早的带 body result、end 或 cancel。带 body result 直接按其 HTTP 或 Model Proxy
状态码映射；end 先按 `end_status` 判断：`failed` 固定抛出 `APIError`，`interrupted` 按它的 Model Proxy 状态码映射，
`completed` 再按 HTTP 状态码判断成功或异常；获胜 cancel 固定抛出 `StreamCancelledError`。

除 `ConfigurationError`、`PayloadValidationError`、`ResultPublishError`、`OpenEventSubscriptionError` 和 Python 原生 `TypeError` 外，
OpenAI-like 调用等待或迭代期间产生的上述异常都提供以下只读上下文；不适用或尚未知的字段为 `None`：

```text
stream_id
request_seq
result_openevent_seq
last_stream_openevent_seq
status_code
headers
body
end_status
```

`OpenEventSubscriptionError` 描述整个客户端实例的共享订阅故障，不提供上述单次调用上下文，
也不附加 `stream_id` 或 `request_seq`。它只提供以下只读故障字段：

```text
reason: "rpc" | "protocol"
last_status: str | None
protocol_error: ProtocolError | PayloadValidationError | None
```

`reason="rpc"` 表示 GetStatus 或 Subscribe 建立、读取、重连最终失败，`last_status` 保存可用的最后一个 gRPC status；
`reason="protocol"` 表示订阅收到的消息无法解析，或者本实例发布的 request 在订阅中出现了与冻结内容或发布 RPC
返回 seq 矛盾的消息；此时 `protocol_error` 保留具体错误的类型、消息和公开诊断字段，`last_status` 为 `None`。能够解析、但只破坏某个目标调用
的 result 形态或 append 链的消息只让该调用得到 `ProtocolError`，不让整个 Subscribe 失败。

每次向调用方或回调交付订阅故障时，都新建等价的 `OpenEventSubscriptionError`，保持上述诊断信息一致，
不共享异常对象或此前的 Python 调用栈；嵌套的 `protocol_error` 也不保留原始调用栈或异常链。

共享订阅最终失败后，实例进入不可恢复的 `FAILED`，禁止发起任何新的 OpenEvent 请求，包括 request/cancel 的
UUID 分配、Publish 首次调用或重试、UUID 查询或重试，以及新的 GetStatus/Subscribe。已经开始的单次 RPC 可以返回；
尚在等待重试间隔的调用停止等待，不再进行下一次尝试。`close()` 本身不设置 `FAILED`，关闭引起的错误按第 4 节进入原有处理链路。

`FAILED` 发生时，在途 request/cancel 按已经取得的发布证据收口，不为取得更多证据继续发送 RPC：

| 所处阶段或本次 RPC 的返回 | 结果 |
| --- | --- |
| 尚未发起首次 Publish，UUID 分配成功 | request 返回保存的订阅错误；cancel 只完成本地关闭，不再 Publish。 |
| 在途 UUID 分配失败 | 原始 `ResultPublishError(NOT_COMMITTED)`，`event_uuid=None`，不再重试。 |
| 在途 Publish 或 UUID 查询成功返回合法 seq | 保留发布成功及该 seq，不恢复订阅或发起其他请求。 |
| Publish 返回 `ALREADY_EXISTS`，尚未开始查询 | `ResultPublishError(COMMITTED)`，`committed_seq=None`，不再查询。 |
| 在途 Publish 返回失败，或失败后的重试等待被停止 | 按可靠发布契约已有证据返回 `NOT_COMMITTED` 或 `UNKNOWN`，不再重试。 |
| 已确认 `ALREADY_EXISTS` 后的查询失败，或查询重试被停止 | `ResultPublishError(COMMITTED)`，`committed_seq=None`。 |

本节只约束这个 OpenAI-like 实例自己发起的请求；独立协议 SDK 客户端和 Worker 仍按各自的发布契约执行。

共享订阅最终失败后停止接收新消息。已经接收的输出先交付；已有终态的调用按该终态结束，尚无终态的调用在已有输出
交付完后得到保存的 `OpenEventSubscriptionError`。request 仍在发布时，先按第 3.2 节确定发布结论；发布成功才采用
这些接收结果，发布失败仍返回原始发布错误。若错误表明该调用的 request 与冻结内容或发布返回 seq 矛盾，则该调用已暂存
的结果不能交付，发布成功后仍以此订阅错误结束。

若订阅最终失败时 request 的 UUID 仍在分配，尚未完成发布前的本地登记，则先等待 UUID 分配结束：分配失败仍返回原始
`ResultPublishError`；分配成功也不再发布 request，直接返回保存的订阅错误。首次 `GetStatus` 最终失败时还没有 request 进入发布，
直接返回 `OpenEventSubscriptionError`。关闭底层连接不改变这些异常选择规则。

公开异常包括：

```python
from openevent.model_proxy_sdk import (
    APIConnectionError,
    APIError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    CommitState,
    ConfigurationError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    OpenEventSubscriptionError,
    PayloadValidationError,
    PermissionDeniedError,
    ProtocolError,
    RateLimitError,
    ResultPublishError,
    StreamCancelledError,
    UnprocessableEntityError,
)
```

`ResultPublishError` 和 `CommitState` 的字段与判断规则只看单条事件可靠发布契约。
