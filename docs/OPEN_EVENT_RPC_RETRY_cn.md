# OpenEvent 普通 RPC 重试规则

[English version](OPEN_EVENT_RPC_RETRY.md)

> 状态：当前契约

本文统一规定 Model Proxy Worker、协议 SDK 和 OpenAI-like 客户端调用 OpenEvent 普通 RPC 时的重试次数、
等待间隔和错误分类。普通 RPC 包括 `GetStatus`、每一页 `Fetch`、`AllocateUuids`（Python SDK 的
`get_uuid()`）、`GetSeqByUuid`，以及一次新的 `Subscribe` 建连。

`PublishAutoSeq` 可能已经提交消息却没有返回成功，因此它不属于这里所说的普通 RPC；它的重试和提交状态只看
[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)。Provider HTTP 请求也不使用本文规则。

## 1. 次数和等待

每个逻辑操作先调用一次。遇到可重试错误后，最多再调用 `max_retries` 次；每次额外尝试前固定等待
`retry_interval_ms` 毫秒。Worker 从 YAML 的 `worker` 配置读取这两个值；协议 SDK 和 OpenAI-like 客户端从
各自的构造参数读取，字段默认值与校验分别见 [Worker 配置](CONFIGURATION_cn.md) 和
[SDK API 契约](SDK_API_cn.md)。默认最多额外尝试 `3` 次、每次等待 `1000 ms`；`max_retries=0` 时只调用一次，
不等待或重试。每次调用仍使用配置的单次 `rpc_timeout_ms` deadline。

上述次数是上限。高层 `OpenAI` 客户端进入 `FAILED` 后，停止等待重试且不得启动下一次 RPC，
具体边界见 [SDK_API_cn.md 第 5 节](SDK_API_cn.md#5-异常)；独立使用的协议 SDK 和 Worker 不受该客户端状态影响。

不同逻辑操作分别计算次数。例如两页 Fetch、一次 UUID 分配和一次 UUID 查询各有自己的重试计数，互不占用预算。
UUID 分配已经在服务端成功、但响应丢失时，再次调用可能取得另一个 UUID；未返回给上层的 UUID 可以形成空洞，
不需要回收，也不能拿去发布消息。

## 2. 错误分类

下列错误可以重试：

- `CANCELLED`、`DEADLINE_EXCEEDED`、`UNKNOWN`、`UNAVAILABLE`、`INTERNAL`；
- 连接断开或重置，或没有可用的 gRPC status。

只有上面列出的错误可以重试。其他带 gRPC status 的错误全部立即结束，不再尝试，包括
`UNAUTHENTICATED`、`PERMISSION_DENIED`、`NOT_FOUND`、`INVALID_ARGUMENT`、`RESOURCE_EXHAUSTED` 和
`OUT_OF_RANGE`。例如 Subscribe 起点超过当前 `max_seq + 1` 时返回的 `OUT_OF_RANGE` 是本次建连的最终错误，
不能用同一个起点重复建连。

已有停止规则要求结束操作时，例如 Worker 退出或高层实例进入 `FAILED`，不再重试。
`OpenAI.close()` 只关闭底层 client，不设置这样的停止条件；由此返回的错误仍按上述分类处理，
订阅最终失败的通知和关闭行为见 [SDK_API_cn.md](SDK_API_cn.md)。

## 3. Subscribe

每次 Subscribe 建连先尝试一次，失败后使用同一预算创建替代连接。一次连接被服务端接受后，本轮计数清零；
连接以后断开时重新开始一轮。

等待服务端接受超时或建连失败后进入下一次尝试。Worker 和 SDK 必须保证重试期间不会留下相互竞争的
Subscribe。被接受的 Subscribe 没有整体 RPC deadline。

## 4. 最终结果

重试耗尽或遇到永久错误后，本次逻辑操作失败，不再启动后台重试：

- Worker 的 GetStatus、Fetch 或 Subscribe 失败时退出；
- OpenAI-like 客户端的 GetStatus 或 Subscribe 失败时保存 `OpenEventSubscriptionError`，当前实例不再接受新调用；
  已开始调用的发布结论、已有输出和订阅错误如何交付，按 [SDK_API_cn.md 第 5 节](SDK_API_cn.md#5-异常)处理；
- UUID 分配失败时，发布 API 返回 `ResultPublishError(commit_state=NOT_COMMITTED, event_uuid=None)`；
- `GetSeqByUuid` 失败时，最终 `commit_state` 由[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)决定。
