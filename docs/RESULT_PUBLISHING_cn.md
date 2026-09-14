# 单条事件可靠发布契约

[English version](RESULT_PUBLISHING.md)

> 状态：当前契约

本文是 Model Proxy 协议 SDK 发布单条 OpenEvent 事件时的唯一权威契约，适用于
`infer.request`、`infer.result`、`infer.append`、`infer.end` 和 `infer.cancel`。
`llm.v1` 的字段和流链规则见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)；OpenEvent 的
UUID、`PublishAutoSeq` 和 `GetSeqByUuid` 基础语义见
[OpenEvent API 契约](../openevent-sdk/docs/API_cn.md)。

本文从输入和发布参数通过协议校验后开始，规定一条逻辑事件的冻结、发布、重试、UUID 对账和最终错误。
输入校验失败按 [SDK_API_cn.md](SDK_API_cn.md) 抛出 `PayloadValidationError`。本文不决定是否发布下一条协议消息，
不维护跨消息流状态，也不替应用决定业务重试。

先把名词说清楚：本文讨论的是 **OpenEvent 发布层**（`openevent-sdk` 客户端调用 OpenEvent 服务端），不是
model-proxy Worker，也不是 provider 的 HTTP 请求。一次“逻辑事件”就是调用方想写入的一条完整 OpenEvent 消息，例如一条
`infer.request`；`UUID` 是 OpenEvent 为这条消息预留的唯一编号，`seq` 是消息真正提交后由服务端分配的全局顺序号。
`PublishAutoSeq` 是“写入消息并由服务端分配 seq”的 RPC；`GetSeqByUuid` 是“按 UUID 查询已经提交的消息 seq”的 RPC。
“已提交（committed）”表示 OpenEvent 已接受并持久化该消息；它不表示 provider 已经处理，也不表示 model-proxy 已经返回结果。

## 1. 发布前冻结

每次调用高层发布 API，只为这条逻辑事件取得一个最终使用的 OpenEvent UUID。UUID 分配调用（OpenEvent 的
`AllocateUuids`，在 SDK 中由 `get_uuid()` 使用）按[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)
执行；只有成功返回的合法非零 UUID 才会成为本次事件的 UUID。
如果分配请求已经在服务端成功但响应丢失，重试可能拿到另一个 UUID；前一个 UUID 形成未使用的空洞，这是允许的，不能拿它去调用
`PublishAutoSeq`，也不需要补写或回收。取得 UUID 后，下列内容全部冻结：

- 完整的 UTF-8 payload 字节，包括首次生成的 `kind` 和 `ts_ms`；
- Channel、发布 principal 和 recipients；输出事件还包括 request principal；
- OpenEvent UUID；
- 交给 `PublishAutoSeq` 的其他请求字段。

每条 request、result、append、end 和 cancel 使用各自的 UUID。后续发布尝试必须复用同一 UUID 和同一份冻结请求，
不能修改 payload、`prev_seq`、`ts_ms`、recipients 或其他字段。

UUID 分配在重试耗尽后仍未返回合法 UUID 时，不得调用 `PublishAutoSeq`。发布 API 返回：

```text
ResultPublishError(
    commit_state=NOT_COMMITTED,
    event_uuid=None,
    committed_seq=None,
)
```

有可用 gRPC 状态时写入 `last_status`。这里可以确定事件没有发布，因为 SDK 尚未取得会交给
`PublishAutoSeq` 的事件 UUID。

## 2. PublishAutoSeq 流程

每条事件执行以下流程：

1. 使用冻结请求调用 `PublishAutoSeq`。
2. 根据本节分类结束、复用同一 UUID 重试，或进入 `GetSeqByUuid` 对账。
3. 最终返回已提交事件的 seq，或抛出 `ResultPublishError`。

每个逻辑发布先调用一次 `PublishAutoSeq`。兼容 OpenEvent 契约的成功响应必然带合法正整数 seq；遇到下文规定的
可重试失败后，最多再调用配置的 `max_retries` 次。首次调用不计入 `max_retries`，每次额外尝试前固定等待
`retry_interval_ms` 毫秒。次数和间隔使用与普通 RPC 相同的 Worker 或 SDK 客户端配置，配置入口见
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md#1-次数和等待)；发布重试单独计数。重试次数耗尽后
不启动后台发布。

### 2.1 返回分类

- `OK` 且响应带合法正整数 seq：事件已经提交，直接返回该 seq。
- `ALREADY_EXISTS`：本次尝试没有创建新消息，并且该 UUID 已被此前提交的消息消费。停止发布，按第 3 节
  调用 `GetSeqByUuid` 取得原消息 seq。
- `UNAUTHENTICATED`、`PERMISSION_DENIED`、`NOT_FOUND`、`INVALID_ARGUMENT` 或
  `RESOURCE_EXHAUSTED`：本次尝试保证没有提交。此前有过不确定尝试时，整体仍为 `UNKNOWN`；没有时为
  `NOT_COMMITTED`。收到这些确定终态后立即结束，不继续消耗发布重试次数。
- `CANCELLED`、`DEADLINE_EXCEEDED`、`UNKNOWN`、`UNAVAILABLE`、`INTERNAL`、连接断开或没有
  gRPC 状态：提交结果不确定。使用同一 UUID 和冻结请求继续有界重试。
- OpenEvent 契约没有明确标为“保证未提交”的其他非 `OK` 结果：提交结果不确定，使用同一 UUID 和冻结请求继续有界重试。

一个较晚的“保证本次尝试未提交”状态不能推翻较早的不确定尝试。发布重试耗尽时，整体结果为 `UNKNOWN`，抛出
`ResultPublishError`。只有 `PublishAutoSeq` 成功返回 seq，或 `ALREADY_EXISTS` 后由 `GetSeqByUuid` 成功返回 seq，
发布 API 才成功结束。

## 3. GetSeqByUuid 对账

只有 `ALREADY_EXISTS` 证明 UUID 已经被已提交消息消费后，发布流程才进入 UUID 对账。SDK 调用
`GetSeqByUuid(uuid)`，不扫描消息历史，也不比较 payload。

`GetSeqByUuid` 的调用次数、等待和错误分类全部按
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)执行，并与 `PublishAutoSeq` 分别计算重试次数。

- 查询成功并返回合法正整数 seq：返回该 committed seq，发布成功结束。
- 查询按普通 RPC 契约最终失败：返回 `commit_state=COMMITTED` 且 `committed_seq=None`。

进入 UUID 对账后，不得再次调用 `PublishAutoSeq`，也不得换 UUID。先前的 `ALREADY_EXISTS` 已经证明事件存在；
查询失败只表示当前拿不到它的 seq。

## 4. ResultPublishError

调用方从公开包导入错误类型和提交状态枚举：

```python
from openevent.model_proxy_sdk import CommitState, ResultPublishError
```

`commit_state` 的类型是 `CommitState`，枚举值只有 `NOT_COMMITTED`、`COMMITTED` 和 `UNKNOWN`；文档中的同名大写字样
就是这些枚举值。`event_uuid` 和 `committed_seq` 的类型都是 `int | None`，`last_status` 的类型是 `str | None`。
它们是异常对象的只读结果字段，调用方应读取字段，不自行构造错误或修改提交状态。例如：

```python
try:
    publish_one_event(...)
except ResultPublishError as exc:
    if exc.commit_state is CommitState.NOT_COMMITTED:
        # 可以确定这条逻辑事件没有写入 OpenEvent
        pass
    else:
        # 可能已经写入；不要换 UUID 重发
        record_for_manual_handling(exc.event_uuid, exc.committed_seq, exc.last_status)
```

- `commit_state`：
  - `NOT_COMMITTED`：可以证明这条逻辑事件没有发布。典型情况是 UUID 分配失败，或所有发布尝试都得到
    “本次未提交”终态且从未出现不确定尝试。
  - `COMMITTED`：可以证明事件 UUID 已被提交消息消费，但当前没有取得可返回的合法 seq。
  - `UNKNOWN`：至少一次发布尝试可能已经提交，且当前既没有取得 committed seq，也没有其他已提交证明。
- `event_uuid`：成功取得并冻结 UUID 后填写；UUID 分配失败时为 `None`。
- `committed_seq`：已经确认的 committed seq；不知道时为 `None`。取得合法 seq 时，发布 API 通常直接成功返回该 seq。
- `last_status`：最后观察到的 gRPC 或传输状态，仅用于诊断，不能单独覆盖 `commit_state` 的结论。

`COMMITTED` 只描述“消息已经写入”这个事实，不表示本次 SDK 调用成功。`ALREADY_EXISTS` 已经证明 UUID 被已提交消息使用，
但后续查询最终失败时，调用仍以 `ResultPublishError` 结束。

只有 `NOT_COMMITTED` 才允许调用方把这条逻辑事件当作未发布。`COMMITTED` 和 `UNKNOWN` 都不能通过更换 UUID
重新发布同一逻辑事件，否则可能产生重复消息或破坏 `prev_seq` 链。调用方可以明确放弃继续等待，但必须接受该事件
仍可能已经提交，或随后产生当前调用不再接收的输出。

## 5. 调用边界

上述尝试次数是上限。高层 `OpenAI` 客户端进入 `FAILED` 后，即使还有重试次数，也不得启动下一次 RPC；
已开始的单次 RPC 如何保留发布证据，以及首次发布前停止时的结果，见
[SDK_API_cn.md 第 5 节](SDK_API_cn.md#5-异常)。这不改变本契约的提交状态含义，
也不影响独立使用的协议 SDK 和 Worker。

协议 SDK 不保存本地持久化发布状态。OpenEvent 的已提交消息和 UUID 索引是持久事实。可靠发布流程只返回已提交 seq，
或抛出本契约定义的 `ResultPublishError`；它不生成恢复句柄，不在 API 返回后继续后台发布，也不决定 Worker 是否退出。

发布 API 不依赖 Subscribe 连接已经建立，取得 seq 或最终发布错误后即结束，不等待 Worker 的模型输出。高层客户端公开的 `request_seq` 只采用本发布 API
成功返回的 seq；订阅端识别消息归属和暂存输出不会改变发布结论。发布最终失败时原样抛出 `ResultPublishError`，不能用
Subscribe 读到的消息改变发布结果，也不在后台继续发布。高层客户端何时向业务调用方交付模型输出，见
[SDK_API_cn.md 第 3.2 节](SDK_API_cn.md#32-发起调用)。
