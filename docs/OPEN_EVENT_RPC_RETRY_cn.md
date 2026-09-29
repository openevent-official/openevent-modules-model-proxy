# OpenEvent 普通 RPC 重试规则

[English version](OPEN_EVENT_RPC_RETRY.md)

本规则适用于 Model Proxy Worker、协议 SDK 和 OpenAI-like 客户端的
`GetStatus`、每一页 `Fetch`、`AllocateUuids`（Python SDK 的
`get_uuid()`）、`GetSeqByUuid`，以及一次新的 `Subscribe` 建连。

`PublishAutoSeq` 的重试和提交状态见
[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)。Provider HTTP 请求也不使用本文规则。

## 1. 次数和等待

每个逻辑操作先调用一次。遇到可重试错误后，最多再调用 `max_retries` 次；每次额外尝试前固定等待
`retry_interval_ms` 毫秒。Worker 从 YAML 的 `worker` 配置读取这两个值；协议 SDK 和 OpenAI-like 客户端从
各自的构造参数读取，默认值与校验见 [Worker 配置](CONFIGURATION_cn.md) 和
[SDK API 契约](SDK_API_cn.md)。`max_retries=0` 时只调用一次。每次调用使用配置的单次 `rpc_timeout_ms` deadline。
不同逻辑操作分别计数；每一页 Fetch、UUID 分配和 UUID 查询都有独立预算。

## 2. 错误分类

下列错误可以重试：

- `CANCELLED`、`DEADLINE_EXCEEDED`、`UNKNOWN`、`UNAVAILABLE`、`INTERNAL`；
- 连接断开或重置，或没有可用的 gRPC status。

只有上面列出的错误可以重试。其他带 gRPC status 的错误全部立即结束，不再尝试，包括
`UNAUTHENTICATED`、`PERMISSION_DENIED`、`NOT_FOUND`、`INVALID_ARGUMENT`、`RESOURCE_EXHAUSTED` 和
`OUT_OF_RANGE`。

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
