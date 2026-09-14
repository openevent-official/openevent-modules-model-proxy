# LLM Protocol llm.v1

[English version](LLM_PROTOCOL.md)

> 状态：当前有效规格
> 适用范围：OpenEvent `protocol="llm.v1"` channel 的模型代理 payload 与约定

## 1. 边界

`llm.v1` 定义业务调用方、`model-proxy` 和结果消费者之间的消息。OpenEvent
只保存不透明 payload，分配全局 `seq` 和消息 `uuid`，执行 Channel ACL，并提供
Fetch/Subscribe；它不解析本协议。

协议假定调用方使用 SDK，并遵守 principal、recipients、stream_id 和单 writer
约束。不额外提供业务授权，不保证同一个 request 只调用 provider 一次，也不提供业务重试语义。

## 2. Channel 与 OpenEvent 字段

每个模型 Channel MUST 设置 `protocol="llm.v1"`，并使用 protected 或 private
visibility。description 是如下形状的 JSON 字符串：

```json
{"version":"v1","updated_at_ms":1710000000000,"metadata":{}}
```

description 解码后必须是 JSON object，并且只能包含 `version`、`updated_at_ms`、`metadata`
三个必填字段。`version` 必须是字符串 `"v1"`；`updated_at_ms` 必须是非负整数，`bool` 不算整数；
`metadata` 必须是 JSON object，内部内容由应用决定。JSON 字段顺序和无意义空白不影响合法性。

一个 Channel 是一个 Model Proxy 会话域，由部署配置绑定到且只能绑定到一个运行中的 Worker。
调用方和 Worker 必须都是成员。request 的 OpenEvent `recipients` 为空；Worker 输出消息定向给
request 的发布 principal。

OpenEvent 顶层字段保持原生语义：

- `principal` 是发布者（request 为调用方，输出为 Worker）。
- `seq` 是不可变全局位置，也是 `prev_seq` 引用的值。
- `recipients` 只是投递过滤字段，不是 ACL 或保密边界。
- `uuid` 是服务端分配的消息 ID，既不是 `stream_id`，也不是 `request_seq`。PublishAutoSeq
  结果不确定时必须复用同一 UUID，具体按[单条事件可靠发布契约](RESULT_PUBLISHING_cn.md)处理。

## 3. JSON 与消息字段

所有 payload 都必须是 UTF-8 JSON object。JSON integer 不包括 boolean。下表定义全部顶层字段的类型和取值；
每种消息未列为必填或可选的顶层字段都非法。

| 字段 | JSON 类型和取值 |
| --- | --- |
| `kind` | string；只能是 `infer.request`、`infer.result`、`infer.append`、`infer.end` 或 `infer.cancel` |
| `stream_id` | string；调用方生成的调用标识；长度 `1..128`，每个字符都必须属于 ASCII `[A-Za-z0-9._:-]` |
| `ts_ms` | integer，`>= 0`，不另设上限；发布方生成并冻结的 Unix 毫秒时间戳 |
| `provider` | string；非空；字段合法性不取决于 Worker 当前是否配置了该名称 |
| `method` | string；只能是 `POST` |
| `path` | string；只能是 `/v1/chat/completions` 或 `/v1/responses` |
| `prev_seq` | 正 integer；引用 OpenEvent 消息的 `seq` |
| `request_seq` | 正 integer；目标 `infer.request` 的 OpenEvent `seq` |
| `status_code` | integer；Provider HTTP 状态为 `100..599`，或第 7 节明确列出的 Model Proxy 状态码 |
| `headers` | 非空 array；每个元素必须是只含 `name`、`value` 的 object，两个值都是 string；`name` 只能是第 5 节允许的小写名称 |
| `end_status` | string；只能是 `completed`、`failed` 或 `interrupted` |
| `body` | `infer.request` 中必须是 object；其他消息中的具体 JSON 类型和是否允许省略见下表 |

五种消息允许的字段如下。标为必填或可选之外的字段一律非法。

| `kind` | 必填字段 | 可选字段 | `body` 规则 |
| --- | --- | --- | --- |
| `infer.request` | `kind`、`stream_id`、`ts_ms`、`method`、`path`、`body` | `provider`、`prev_seq` | 必须是 object；`stream` 存在时必须是 boolean |
| `infer.result` | `kind`、`stream_id`、`ts_ms`、`prev_seq`、`status_code` | `headers`、`body` | 允许任意 JSON 值；`body` 缺失与 `body: null` 含义不同 |
| `infer.append` | `kind`、`stream_id`、`ts_ms`、`request_seq`、`prev_seq`、`body` | 无 | 允许任意 JSON 值 |
| `infer.end` | `kind`、`stream_id`、`ts_ms`、`request_seq`、`status_code`、`end_status` | `body` | `completed` 可以省略；`failed` 和 `interrupted` 必须提供，允许任意 JSON 值 |
| `infer.cancel` | `kind`、`stream_id`、`ts_ms`、`request_seq` | 无 | 不包含 `body` |

严格解析同时检查 payload 是 UTF-8 JSON object、字段集合、JSON 类型和本节列出的取值范围。Worker 无论在启动扫描
还是实时订阅中遇到任何无法严格解析的 `llm.v1` 消息，都把该 Channel 视为无法继续处理并报错退出。Worker 不从
解析失败的 payload 中提取 `kind` 或 `stream_id`，也不为它写 `60009` 或其他结果。

发布方必须在第一次序列化前生成 `ts_ms`，并将它和完整 payload 一起冻结；重试同一事件时不能重新生成时间戳。
payload 中的 `ts_ms` 与 OpenEvent 外层 `EventMessage.ts_ms` 相互独立，不要求相等。外层时间表示服务端
收到发布请求的时间，语义以[OpenEvent API 契约](../openevent-sdk/docs/API_cn.md#1-基础约定)为准。

payload 不包含 principal、token、provider 凭据或 base URL。`infer.request`
可以带一个可选的顶层 `provider` 名称，用来选择 worker 配置中的 provider；
这个名称不属于 `body`，也不会转发给 provider。`body` 是 provider 定义的
JSON 值，其他字段按规则透传。

## 4. infer.request

```json
{
  "kind":"infer.request",
  "stream_id":"stream_01",
  "ts_ms":1710000000000,
  "provider":"openai_main",
  "method":"POST",
  "path":"/v1/chat/completions",
  "body":{"model":"gpt-4o-mini","messages":[],"stream":true}
}
```

当前契约只接受两个 method/path 组合：`POST /v1/chat/completions` 和 `POST /v1/responses`，分别使用
下节定义的最小兼容契约，endpoint 集合不可配置。`provider` 不传时使用配置的 `default_provider`；指定的名称
在 Worker 配置中不存在时，payload 本身仍然解析成功，但 Worker 不调用 Provider，而是写一条 `60009` 普通终态
result。`provider` 不会放进转发给 Provider 的 `body`。`body` 必须是 JSON object；存在
`body.stream` 时必须是 boolean，`true` 选择流式，缺失或 `false` 选择普通响应。Worker 只识别该字段，
其他 OpenAI 请求字段原样透传并由选中的 provider 校验。其他 method/path、非 object body 或非 boolean
`stream` 都会导致严格解析失败，Worker 按第 3 节报错退出。

普通调用和流式调用都只由 `infer.request` 发起，没有单独的流式请求 kind。流式请求取得可记录的 provider
HTTP 响应头后，先写不带 `body` 的 `infer.result`，用来承载 HTTP 状态和响应头；如果在它写入前调用已经
失败，则直接写 `infer.end`，不为了建立流链而补一条 result。

OpenEvent `principal` 是调用方，`recipients` MUST 为空。发布方必须提供 `stream_id`。

request 的 `prev_seq` 可选；存在时必须是正数 OpenEvent seq，其应用含义不属于本协议，
也不参与输出流单链。

“重复 request”是指同一个 Channel 内，已经存在一条严格解析成功的 `infer.request` 后，又出现一条具有相同
`stream_id` 的 `infer.request`。Worker 按 OpenEvent seq 顺序判断；第一条占用 `(channel_id, stream_id)`，是原始
request，后续同名 request 都是重复 request，不比较两条消息的 principal、provider、method、path 或 body 是否相同。
Worker 拒绝重复 request 时写一条 `60005` 普通终态 result，`prev_seq` 指向这条重复 request 自己的 seq。若重复
request 声明 `body.stream=true`，先提交的合法 cancel 仍可按第 6 节成为终态；这种情况下不再写 `60005`。原始 request 即使因为
超过 `max_payload_bytes` 返回 `60008`，或因为指定的 provider 不存在而返回 `60009`，仍然占用 `stream_id`。解析失败
属于 Worker 致命错误，不产生结果，也不进入 `stream_id` 去重状态。

### 4.1 `openai_compatible` 的最小兼容契约

`openai_compatible` 表示 Model Proxy 能按下面这些规则完成一次 HTTP 调用，不表示 Provider 必须实现 OpenAI
产品的所有模型、字段或业务能力：

1. Provider 接受 JSON object 形式的 `POST /v1/chat/completions` 和 `POST /v1/responses` 请求；Model Proxy
   原样转发 request 的 `body`，不会替 Provider 校验模型名、消息内容或其他业务字段。
2. `body.stream` 缺失或为 `false` 时，Provider 返回一个完整 HTTP 响应。响应 body 必须是 UTF-8 编码的完整
   JSON 值，HTTP 成功和 HTTP 错误响应都遵守这条规则。
3. `body.stream=true` 时，Provider 可以返回 `Content-Type` 媒体类型为 `text/event-stream` 的 UTF-8 SSE；
   Chat Completions 用 `[DONE]` 正常结束，Responses 用 `response.completed`、`response.failed` 或 `response.incomplete`
   结束。Responses 的 SSE 数据事件必须是含字符串 `type` 的 JSON object，`type` 使用
   [OpenAI 官方事件定义](https://developers.openai.com/api/reference/resources/responses/streaming-events)中的事件类型。
   SSE 的具体解析和终态映射见第 6 节。
4. 流式请求也可以返回非 SSE 的完整 JSON 响应；Model Proxy 按第 6 节把它记为流式终态。
5. Provider 必须返回合法 HTTP 状态码和可解析的响应头。Model Proxy 只记录第 5 节列出的响应头；其他响应头
   不属于 `llm.v1` 的可观察结果。

HTTP 响应压缩的支持范围见 [CONFIGURATION_cn.md](CONFIGURATION_cn.md)。

以上就是 Model Proxy 依赖的兼容范围。Provider 对请求字段和返回 JSON 的业务语义负责；不满足上述传输与终态
规则的响应按 Provider 解析失败或调用中断处理。

## 5. infer.result

同一个 request 可以出现多条 `infer.result`。消费者遇到无法解析的 `llm.v1` 消息时按协议错误处理，不能跳过它继续找后续
result。request 尚未终态时，消费者接受第一条 `stream_id`、`prev_seq` 与该 request 匹配的 result，后面的匹配 result
全部忽略；不能跳过第一条，再选择更合意的后续 result。第一条带 `body` 的 result
是普通终态，包括原 request body 写了 `stream=true`、但因重复或非法而返回的拒绝结果；第一条不带 `body` 的 result
建立流式输出链。如果第一条 result 的形态对该任务不合法，该任务报告协议错误。result 的 `prev_seq` 直接指向 request，
但两者的 OpenEvent 全局 seq 之间可以穿插其他消息。带 `body` 的 result 是普通终态：

```json
{
  "kind":"infer.result",
  "stream_id":"stream_01",
  "prev_seq":12345,
  "ts_ms":1710000001234,
  "status_code":200,
  "headers":[{"name":"content-type","value":"application/json"}],
  "body":{"id":"..."}
}
```

不带 `body` 的 result 是流式输出链的首节点，不是终态：

```json
{
  "kind":"infer.result",
  "stream_id":"stream_01",
  "prev_seq":12345,
  "ts_ms":1710000001234,
  "status_code":200,
  "headers":[{"name":"content-type","value":"text/event-stream"}]
}
```

result 的 `prev_seq` 等于目标 request 的 `request_seq`。流式无 body result 的 `status_code` 必须是 Provider HTTP 状态码 `100..599`。
普通终态 result 可以使用 Provider HTTP 状态码，或 `60000`、`60001`、`60002`、`60003`、`60005`、
`60007`、`60008`、`60009`；`60004` 只由 cancel 表示，不得写入 result。`headers` 使用下述固定转发规则：

1. HTTP 字段名称按 ASCII 大小写不敏感匹配，并在 payload 中统一写成小写。
2. 只保留名称严格等于 `content-type`、`retry-after`、`x-request-id` 的字段，或名称以
   `x-ratelimit-` 开头的字段。
3. 保留字段按照 provider 响应中的出现顺序写入数组；重复字段分别保留，不合并，也不按逗号拆分字段值。
4. 字段值使用 Worker 完成合法 HTTP 解析后得到的字符串，Worker 不再去除空白或解释字段含义。
5. 其他响应字段全部丢弃；没有任何保留字段时省略 `headers`，不写空数组。

result 由 Worker principal 发布，且只把 request principal 放入 recipients。具体形态由对应 request 决定：

1. 合法非流式 request 使用第一条匹配的 result 作为终态响应，并且该 result 必须带 `body`。JSON `null` 是
   一个明确存在的 body，不能当作省略。第一条匹配的 result 如果没有 `body`，任务报告协议错误；消费者不能
   跳过它再使用后续 result。provider body 不依赖 `Content-Type`，必须能解为一个完整 JSON 值。通过
   Provider 输入大小检查后仍解码失败时，Worker 按 5.1 节的失败映射写出普通终态。
2. 通过重复、大小和 provider 配置检查并进入 Provider 调用的流式 request，如果发布 result，该 result 必须省略
   `body`。除非 Worker 已经观察到获胜 cancel，否则
   Worker 在取得可记录的 Provider HTTP 响应头后、消费第一条响应 body 或流事件前发布它。如果等待响应头、
   DNS、TLS、建连等步骤失败，或者保留的响应头无法放入一个 result，Worker 直接发布 interrupted `infer.end`，
   不发布 result。
3. Worker 为重复 request、超过 `max_payload_bytes` 的 request，或严格解析成功但指定的 provider 不存在的 request
   生成拒绝结果时，固定使用带错误 body 的普通终态 result，即使原 payload 中 `stream=true`；这种结果之后不再发布
   append/end。若合法 cancel 先提交，则按第 6 节的终态裁决处理。

result 的完整字段集合和类型见第 3 节。payload 解析本身不读取历史；result 形态是否与 request 一致由
Worker 或消费者状态机校验。

### 5.1 普通调用的 Provider 失败映射

普通调用无论成功还是失败，都只产生一条带 `body` 的 `infer.result`，不会产生 `infer.end`。映射规则如下：

| Provider 阶段 | `status_code` | `body` 和 headers |
| --- | --- | --- |
| 收到 HTTP 响应，状态码为 `100..599` | Provider 原始 HTTP 状态码 | 能解为 JSON 的响应 body 原样保留；按响应头转发规则保留 headers。HTTP 4xx/5xx 仍是普通终态，调用方依据状态码判断失败。 |
| 等待响应头期间超时 | `60000` | 标准 Model Proxy 错误 body；不提供 provider 响应头。 |
| 明确 DNS 解析失败 | `60001` | 标准 Model Proxy 错误 body；不提供 provider 响应头。 |
| 明确 TLS 握手失败 | `60002` | 标准 Model Proxy 错误 body；不提供 provider 响应头。 |
| 明确连接失败或连接重置，且尚未得到响应头 | `60003` | 标准 Model Proxy 错误 body；不提供 provider 响应头。 |
| 已收到响应头，读取 body 时无进展超时 | `60000` | 丢弃未完成的 body 和 headers，写标准 Model Proxy 错误 body。 |
| 已收到响应头，读取 body 时连接重置或提前结束 | `60003` | 丢弃未完成的 body 和 headers，写标准 Model Proxy 错误 body。 |
| 已收到响应，但 body 不是完整 JSON，或 UTF-8/解析失败 | `60007` | 丢弃原始 body 和 headers，写标准 Model Proxy 错误 body。 |
| 输出 payload 超过 `max_payload_bytes` | `60008` | 丢弃过大的 provider body 和 headers，写可放入上限的小型标准错误 body。 |

标准 Model Proxy 错误 body 的 `error.code` 与状态码对应关系见第 7 节。普通调用的 provider 请求不因这些失败自动重试；
调用方若要再次发起模型调用，必须创建新的 `infer.request`。

## 6. 流式事件

对于已经判定为合法流式请求的 request，正常 Provider 输出链由无 body result 开始，后接零或多条 `infer.append`。
流式调用也可能在输出链建立前收到第 5 节定义的带 body 普通终态 result，用来表示重复 request、request 超限或
指定的 provider 未配置。`infer.end` 和 `infer.cancel` 是独立的终态控制事件，不属于输出链，也不携带 `prev_seq`。
流式调用的候选终态只有三类：尚未接受任何 result 时出现的第一条匹配带 body result、Worker 发布的 `infer.end`、
Channel 成员发布的 `infer.cancel`。消费者按 OpenEvent seq 顺序接受最早的合法候选终态，后续候选终态和输出消息
都不能改变结果。
`infer.append` 不能直接接在 request 后；消费者在尚未接受无 body result 时收到 append，必须只向该调用报告协议错误，
不能用它建立流式链。
`stream_id` 是调用方生成的字符串，用来表示一次模型调用；普通和流式调用都使用它。`request_seq` 不是另一套随机
ID，它直接使用发起这次调用的 `infer.request` 写入 OpenEvent 后得到的 `seq`。后续可能出现相同 `stream_id` 的重复
request，所以 append、end 和 cancel 还必须带 `request_seq`，精确说明自己属于或要终止哪一条 request 事件。
request 发布成功后 `request_seq` 已经确定，因此 end 或 cancel 可以在 result 之前发送。result 不重复携带
`request_seq`，而是让 `prev_seq` 等于目标 request 的 `request_seq`。

### 6.1 infer.append

```json
{
  "kind":"infer.append",
  "stream_id":"stream_01",
  "request_seq":12345,
  "prev_seq":12346,
  "ts_ms":1710000001240,
  "body":{"id":"...","choices":[{"delta":{"content":"Hi"}}]}
}
```

`prev_seq` 等于前一条无 body result 或 append 的 seq。响应 Content-Type 的媒体类型为 `text/event-stream`
时按 OpenAI 使用的标准 Server-Sent Events 规则处理，比较媒体类型时忽略大小写和 `charset` 等参数。provider 保持
HTTP 响应打开并发送事件；事件由字段行组成，空行表示事件结束。解析器按标准处理 LF、CRLF 和 CR 换行，忽略注释行，
每行按第一个冒号分成字段名和值，值开头如果恰好有一个 ASCII 空格就去掉该空格；没有冒号的字段值为空。
同一事件中的每条 `data` 字段值按出现顺序追加一个换行符，事件结束时删除最后一个追加的换行符；没有 `data` 字段的事件忽略。
注释以及 `event`、`id`、`retry` 等字段不形成模型事件。
事件不能按任意网络读取边界切分。每个完整组装出且符合第 4.1 节事件结构的非终态 `data` JSON 事件按 Provider 到达顺序作为一条
append body；协议不解释 `choices`、`delta`、usage 或 Provider 分段边界。第 4.1 节列出的 Provider 终态
标记不创建 append：Chat Completions 的 `[DONE]` 不写入 body；Responses 的终态事件原样放入 end 的 `body`。

如果流式 request 收到普通 HTTP 响应而不是 `text/event-stream`，Worker 不依赖 `Content-Type`，先把完整 body
按 JSON 解码。解码成功时，该响应就是 provider 终态，直接写一条包含该 JSON body 的 completed `infer.end`，
不创建 append。HTTP 状态码同时写入无 body result 和 completed end；非 2xx 仍由消费者作为 provider HTTP 错误处理。

本项目对 Chat Completions 和 Responses 的最小兼容契约规定，响应（包括错误响应）必须是 JSON。非事件流 body 无法解为 JSON
就是 Provider 响应解析失败，不是兼容响应：Worker 丢弃原始 body，写一条带标准错误 body 的 `60007 interrupted`
end。此前已写无 body result 时，该 result 保留已观察到的 HTTP 状态和 headers，而 end 使用 `60007`；此前还没写
result 时，直接写 end。事件流的每个非终态事件同样必须是合法 JSON；事件 JSON 解析失败也按同样的
`60007 interrupted` end 处理。两种路径都不把原始 body 交给调用方。

### 6.2 infer.end

```json
{
  "kind":"infer.end",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001300,
  "status_code":200,
  "end_status":"completed"
}
```

Provider 终态都直接写成 `infer.end`，不会先把终态事件写成 append。Responses 的 `response.failed` 和 `response.incomplete` 固定生成
`end_status="failed"`，end 的 `body` 保存 provider 终态事件：

```json
{
  "kind":"infer.end",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001300,
  "status_code":200,
  "end_status":"failed",
  "body":{"type":"response.failed","response":{"id":"..."}}
}
```

Responses 正常完成时结构相同，但 `end_status="completed"`，且 `body.type="response.completed"`。

`response.incomplete` 表示 Provider 已结束本次生成，但内容没有生成完整，例如达到输出 token 上限。它使用上述
failed end 结构，`body.type` 仍是 `response.incomplete`，完整保留原始事件及 `response.incomplete_details`；
`status_code` 保留实际 HTTP 状态码，即使为 200 也按失败终态处理。此前已发布的 append 保留。

`request_seq` 等于 request 的 seq，`stream_id` 必须与该 request 一致。end 不声明自己接在哪一条 append 后面；
它只按 `request_seq` 定位流，并以自己的 OpenEvent seq 参与终态裁决。
`end_status` 只能是 `completed`、`failed` 或 `interrupted`：

- `completed`：Worker 已经从 Provider 收到一个完整的 HTTP 终止响应，或收到了该 endpoint 规定的正常终态
  事件。它只表示 provider 这次响应已经结束，不单独表示调用成功；只有 `status_code` 在 `200..299`
  时，调用方才把它当作成功。消息结构层面 `body` 可选：Chat Completions 的 `[DONE]` end 省略它；
  Responses 的 `response.completed` end 包含终态事件的原始 JSON 结构；非事件流 HTTP 响应的 end 包含普通 JSON
  响应。因此，HTTP 429 的普通 JSON 也可以是 `completed` end，但调用方必须把它当作
  provider HTTP 错误。
- `failed`：provider 在响应中明确报告了失败或生成未完成的终态，必须包含 `body`，其值是 provider 终态事件的原始 JSON 结构；
  worker 不把该事件发布为 append。`status_code` 等于无 body result 的状态码，因此 HTTP 200 也可能是失败终态。
  消费者无论 HTTP 状态码是多少，都 MUST 把它当作模型调用失败。
- `interrupted`：Worker 没有观察到，或无法持久化 Provider 终止标记，`status_code` 为 Model Proxy 扩展码，
  `body` 是标准 Model Proxy 错误对象。
  已有 append 仍然有效，消费者 MUST 保留。

`failed` 和 `interrupted` end 必须包含 `body`，所有 end 都不得包含 `headers`。Worker 根据对应 request
校验 completed 的 endpoint-specific body 规则；单独解析一条 end
payload 无法从 payload 本身推断 endpoint。

completed 和 failed end 的 `status_code` 必须是 Provider HTTP 状态码 `100..599`。interrupted end 的
`status_code` 只能是 `60000`、`60001`、`60002`、`60003`、`60007` 或 `60008`。

Worker 按第 4.1 节的 Provider 终态标记生成 completed 或 failed end。Responses 的失败终态表示模型结果，
不是传输中断，即使 HTTP 状态为 200 也一样。对应终止标记前遇到干净 EOF 视为 Provider 流不完整，写
`interrupted` end。流事件/JSON 解析失败、Provider 连接或流式空闲超时、连接重置，以及 Worker 在终态事件
持久化前重启，也均写 `interrupted` end。
流在 OpenEvent seq 顺序中第一个被接受的带 body result、end 或 cancel 后结束；终态之后到达的 result、append、end、
cancel 全部忽略，包括终态提交时已经在途、但随后才落库的发布。end 不引用最后一条 append，因此即使一条
在途 append 晚于 end 提交，它也只会被终态规则忽略，不会造成 end 断链。关闭本地读取或 Subscribe 中断
不是协议终态。

### 6.3 infer.cancel

```json
{
  "kind":"infer.cancel",
  "stream_id":"stream_01",
  "request_seq":12345,
  "ts_ms":1710000001250
}
```

`request_seq` 必须是目标 `infer.request` 的准确 OpenEvent `seq`，`stream_id` 必须与该 request
一致。cancel 不包含 `prev_seq`、body 或 recipients；其 OpenEvent `recipients` 必须为空。受保护或
私有 `llm.v1` Channel 的任意成员都可以发布 cancel，Channel 成员资格是协议层唯一的写权限。

对于尚未进入终态的 stream，cancel 自身就是 `60004 / STREAM_CANCELLED` 终态，不要求后续 `infer.end`。
Worker 观察到获胜 cancel 后不再发起新的输出发布；当时已经在途、后来才提交的输出由终态规则忽略。
目标 `request_seq` 不存在、`stream_id` 不匹配或调用已经终态时，cancel 忽略。同一调用的多个 cancel 只接受
OpenEvent seq 最小的一条；带 body result、end 与 cancel 同样按 seq 接受最早的合法候选终态。

## 7. Model Proxy 扩展状态码

HTTP 响应透传 `100..599`。Worker 生成：

| 状态码 | 含义 |
| --- | --- |
| `60000` | provider 响应头等待或响应读取无进展超时 |
| `60001` | DNS 解析失败 |
| `60002` | TLS 握手失败 |
| `60003` | 连接失败或重置 |
| `60004` | `infer.cancel` 终止流 |
| `60005` | 重复 `stream_id` 被拒绝 |
| `60007` | Model Proxy 内部、Provider 响应解析或重启中断 |
| `60008` | request 或 Provider 输入、输出超过 Worker 的 `max_payload_bytes` |
| `60009` | request 已严格解析成功，但它指定的 provider 未配置 |

Provider 建连阶段按最先观察到的确定原因分类：`response_header_ms` 预算先耗尽时，无论当时正在
DNS、TCP 建连、TLS、发送请求还是等待响应头，都使用 `60000`；只有在预算耗尽前已经得到明确的
DNS、TLS 或连接失败，才分别使用 `60001`、`60002` 或 `60003`。收到响应头后的读取空闲超时使用
`60000`，明确的连接重置使用 `60003`。不得在 deadline 到达后仅按当前阶段猜测错误码。

`infer.cancel` 本身表示 `60004 / STREAM_CANCELLED`，不携带错误 body，也不要求用
`infer.end` 表示取消。

Worker 自己生成、使用 Model Proxy 扩展状态码的错误 result/end 统一使用下面的错误 body，`error.code` 必须和
`status_code` 对应：

```json
{"error":{"code":"MODEL_API_DNS_ERROR","message":"DNS resolution failed","type":"model_proxy_error"}}
```

Provider 返回的 HTTP 错误正文和 `response.failed`、`response.incomplete` 终态仍按第 5、6 节保留原始 JSON。

对应关系如下：`60000` 为 `MODEL_API_TIMEOUT`，`60001` 为 `MODEL_API_DNS_ERROR`，`60002` 为
`MODEL_API_TLS_ERROR`，`60003` 为 `MODEL_API_CONNECTION_ERROR`，`60005` 为 `DUPLICATE_REQUEST`，
`60007` 为 `MODEL_PROXY_INTERRUPTED`，`60008` 为 `PAYLOAD_TOO_LARGE`，`60009` 为 `INVALID_REQUEST`。
调用方先看 `status_code` 判断类别，`error.code` 只用于展示和日志；二者不一致时以 `status_code` 为准。

流式请求在调用 Provider 的中途出错时，只写一条 interrupted `infer.end`，`status_code` 使用对应的 Model Proxy 扩展码。
这里的中途出错指超时、断连、解析失败或 payload 超限；调用前拒绝和 Provider 自身返回的失败响应按第 5、6 节处理，
重启恢复按第 8.2 节处理。

1. 无 body result 尚未写入时，不补写 result。
2. 无 body result 已写入时，保留它的 Provider HTTP 状态码和此前已写出的 append；end 记录中断原因，不改写已有输出。

例如，provider 返回 HTTP 200 后连接被重置，结果是 `result.status_code=200`、`end.status_code=60003`；
provider 一直没有返回响应头而超时，则只写 `end.status_code=60000`；保留的响应头过大时只写
`end.status_code=60008`。这样调用方不需要猜测失败发生在流链建立之前还是之后。

## 8. Payload 大小与重启结果

### 8.1 `max_payload_bytes`

Worker 的 `max_payload_bytes` 同时限制严格解析成功的原始 request、Provider 响应头和响应数据，以及 Worker
准备发布的 result、append 和 end。字节数按配置文档定义的计数对象计算。任何消息都不截断；超限时丢弃不能记录的
Provider 内容，并按下面的固定结果结束调用：

| 超限位置 | 对外结果 |
| --- | --- |
| 原始 request | 不调用 Provider；写带错误 body 的 `60008` 普通终态 result，`prev_seq` 指向 request；即使 `body.stream=true` 也一样 |
| 普通响应头或 body | 写带小型错误 body 的 `60008` 普通终态 result |
| 流式响应头 | 不写无 body result，写 `60008 interrupted` end |
| 流式事件、非 SSE 响应 body 或带 body 的 Provider 终态 | 不写超限内容，写 `60008 interrupted` end；此前已经写出的 result 和 append 保留 |

本节只适用于严格解析成功的 request；解析失败按第 3 节处理，不会因为能看见部分字段而改写成 `60008`。
严格解析成功但原始 request 超限时，该 request 仍然占用 `stream_id`。

`60008` 只表示触发了 Worker 配置的 `max_payload_bytes`，不表示 OpenEvent 拒绝了消息。OpenEvent 自身的 payload
限制属于 OpenEvent 发布契约；一旦输出发布被 OpenEvent 拒绝，Worker 按发布失败处理，不把它改写成 `60008`。

### 8.2 Worker 重启后的结果

Worker 重启时只依据 OpenEvent 中已经提交的严格合法消息判断状态。历史消息同样遵守第 3 节的致命解析错误规则。

恢复扫描仍按第 5、6 节判断终态：非流式 request 的匹配带 body result 是终态；`body.stream=true` 的 request 按
OpenEvent seq 接受最早的匹配带 body result、合法 end 或合法 cancel。已经终态的原始或重复 request 保持原结果。
严格解析成功但尚无协议终态的原始 request 不会再次调用 Provider：非流式原始 request 补写 `60007` 普通终态 result，
`prev_seq` 指向该 request；流式原始 request 补写不含 `prev_seq` 的 `60007 interrupted` end。尚无协议终态的重复
request 无论 `body.stream` 是否为 `true`，都补写 `60007` 普通终态 result，`prev_seq` 指向这条重复 request 自己的 seq。
恢复只表达“上一次处理没有留下终态”，不猜测停止前原本要写 `60005`、`60008`、`60009` 还是 Provider 终态，也不读取
新 Provider 配置重新解释旧 request。

本文是当前唯一有效的 `llm.v1` 契约，不保留旧草案的兼容规则。
