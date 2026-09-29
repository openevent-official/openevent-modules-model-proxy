# 单条事件可靠发布契约

[English version](RESULT_PUBLISHING.md)

本契约适用于 Model Proxy 协议 SDK 发布的
`infer.request`、`infer.result`、`infer.append`、`infer.end` 和 `infer.cancel`。
`llm.v1` 的字段和流链规则见 [LLM_PROTOCOL_cn.md](LLM_PROTOCOL_cn.md)；OpenEvent 的
UUID、`PublishAutoSeq` 和 `GetSeqByUuid` 基础语义见
[OpenEvent API 契约](../openevent-sdk/docs/API_cn.md)。

输入校验失败按 [SDK_API_cn.md](SDK_API_cn.md) 抛出 `PayloadValidationError`；通过校验后执行下述发布流程。
“逻辑事件”是一条完整的 OpenEvent 消息。“已提交”表示 OpenEvent 已持久化消息，不表示模型调用完成。

## 1. 发布前冻结

每条逻辑事件使用一个 UUID。UUID 分配调用（SDK 的 `get_uuid()`）按
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)执行，只使用成功返回的合法非零 UUID。
响应丢失后重试可能拿到另一个 UUID；未返回的 UUID 留作空洞，不用于发布，也不回收。取得 UUID 后冻结：

- 完整的 UTF-8 payload 字节，包括首次生成的 `kind` 和 `ts_ms`；
- Channel、发布 principal 和 recipients；输出事件还包括 request principal；
- OpenEvent UUID；
- 交给 `PublishAutoSeq` 的其他请求字段。

后续发布尝试必须复用同一 UUID 和冻结请求。
UUID 分配最终失败时，不调用 `PublishAutoSeq`，抛出
`ResultPublishError(commit_state=NOT_COMMITTED, event_uuid=None, committed_seq=None)`；
有可用 gRPC 状态时写入 `last_status`。

## 2. PublishAutoSeq 流程

使用冻结请求调用 `PublishAutoSeq`；成功返回已提交 seq，最终失败抛出 `ResultPublishError`。
遇到可重试失败后，最多再尝试 `max_retries` 次，每次额外尝试前固定等待
`retry_interval_ms` 毫秒。次数和间隔使用与普通 RPC 相同的 Worker 或 SDK 客户端配置，配置入口见
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md#1-次数和等待)；发布重试单独计数。

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

不确定错误的重试耗尽时，抛出 `ResultPublishError(commit_state=UNKNOWN)`。

## 3. GetSeqByUuid 对账

只有 `ALREADY_EXISTS` 证明 UUID 已经被已提交消息消费后，发布流程才进入 UUID 对账。SDK 调用
`GetSeqByUuid(uuid)`，不扫描消息历史，也不比较 payload。

`GetSeqByUuid` 的调用次数、等待和错误分类全部按
[OpenEvent 普通 RPC 重试规则](OPEN_EVENT_RPC_RETRY_cn.md)执行，并与 `PublishAutoSeq` 分别计算重试次数。

- 查询成功并返回合法正整数 seq：返回该 committed seq，发布成功结束。
- 查询按普通 RPC 契约最终失败：返回 `commit_state=COMMITTED` 且 `committed_seq=None`。

进入 UUID 对账后，不得再次调用 `PublishAutoSeq`，也不得换 UUID。

## 4. ResultPublishError

```python
from openevent.model_proxy_sdk import CommitState, ResultPublishError
```

异常字段只读：`commit_state` 为 `CommitState` 枚举；`event_uuid` 和 `committed_seq` 为 `int | None`；
`last_status` 为 `str | None`。

- `commit_state`：
  - `NOT_COMMITTED`：可以证明这条逻辑事件没有发布。典型情况是 UUID 分配失败，或所有发布尝试都得到
    “本次未提交”终态且从未出现不确定尝试。
  - `COMMITTED`：可以证明事件 UUID 已被提交消息消费，但当前没有取得可返回的合法 seq。
  - `UNKNOWN`：至少一次发布尝试可能已经提交，且当前既没有取得 committed seq，也没有其他已提交证明。
- `event_uuid`：成功取得并冻结 UUID 后填写；UUID 分配失败时为 `None`。
- `committed_seq`：已经确认的 committed seq；不知道时为 `None`。取得合法 seq 时，发布 API 通常直接成功返回该 seq。
- `last_status`：最后观察到的 gRPC 或传输状态，仅用于诊断，不能单独覆盖 `commit_state` 的结论。

只有 `NOT_COMMITTED` 才允许调用方把这条逻辑事件当作未发布。`COMMITTED` 和 `UNKNOWN` 都不能通过更换 UUID
重新发布同一逻辑事件，否则可能产生重复消息或破坏 `prev_seq` 链。调用方可以明确放弃继续等待，但必须接受该事件
仍可能已经提交，或随后产生当前调用不再接收的输出。

## 5. 调用边界

上述尝试次数是上限。高层 `OpenAI` 客户端进入 `FAILED` 后，即使还有重试次数，也不得启动下一次 RPC；
已开始的单次 RPC 如何保留发布证据，以及首次发布前停止时的结果，见
[SDK_API_cn.md 第 5 节](SDK_API_cn.md#5-异常)。独立使用的协议 SDK 和 Worker 不受该客户端状态影响。

发布 API 不要求 Subscribe 已建立，也不等待模型输出；取得 seq 或最终错误后结束，不在后台继续发布。
协议 SDK 不保存本地持久化发布状态或生成恢复句柄。Subscribe 读到的消息不能改变发布结果。
高层客户端的 `request_seq` 和模型输出交付规则见
[SDK_API_cn.md 第 3.2 节](SDK_API_cn.md#32-发起调用)。
