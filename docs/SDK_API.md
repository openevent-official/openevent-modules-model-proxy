# Python SDK API Contract

[中文版](SDK_API_cn.md)

This document defines the public API of `openevent.model_proxy_sdk`.
See [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for protocol fields and state machines,
[RESULT_PUBLISHING.md](RESULT_PUBLISHING.md) for single-message publishing outcomes,
and [SDK_USAGE.md](SDK_USAGE.md) for examples.

## 1. Two API Layers

- Protocol SDK: constructs, publishes, and parses the five `llm.v1` message kinds.
- OpenAI-like client: starts calls and waits for results through synchronous
  `chat.completions.create(...)` and `responses.create(...)` methods.

Import the classes, functions, constants, and exceptions below from `openevent.model_proxy_sdk`.
Import OpenEvent types from the SDK:

```python
from openevent.sdk import OpenEventClient, openevent_pb2

EventMessage = openevent_pb2.EventMessage
```

## 2. Protocol SDK

### 2.1 Client

```python
create_client(
    openevent_client: OpenEventClient,
    token: str,
    max_retries: int = 3,
    retry_interval_ms: int = 1000,
) -> ModelProxyProtocolClient
```

The caller creates and closes `openevent_client`. `token` is the OpenEvent token used for publishing.
`max_retries` is the number of extra attempts after an OpenEvent operation's first failure: it defaults
to `3` and must be a nonnegative integer. `retry_interval_ms` is the fixed wait before each extra
attempt: it defaults to `1000` and must be a positive integer in milliseconds. Neither accepts `bool`.
Ordinary RPCs and reliable single-event publishing share these settings but count attempts separately
for each operation. See the [ordinary RPC retry rules](OPEN_EVENT_RPC_RETRY.md) and
[single-event reliable publishing contract](RESULT_PUBLISHING.md) for attempt counts and error
classification. `token` must be a nonempty string. Invalid constructor arguments raise `ConfigurationError`.

### 2.2 Input Models

All input models use keyword-only arguments:

```python
InferRequestInput(*, stream_id, method, path, body, provider=None, prev_seq=None)
InferResultInput(*, stream_id, prev_seq, status_code, headers=None, body=UNSET)
InferAppendInput(*, stream_id, request_seq, prev_seq, body)
InferEndInput(*, stream_id, request_seq, status_code, end_status, body=UNSET)
InferCancelInput(*, stream_id, request_seq)
```

`UNSET` omits the JSON field. For example, `InferResultInput(body=UNSET)` creates a bodyless streaming
chain head. Supply a Python `dict` conforming to the protocol when a body is present; `None` is not a valid body.
Input models do not accept `kind` or `ts_ms`; the SDK generates and freezes them when preparing publication.
Field types, ranges, and allowed combinations are defined only by the protocol.
`provider=None` and a request's `prev_seq=None` omit those fields; `headers=None` omits headers.

To inspect the actual bytes before sending, prepare the payload once:

```python
prepare_payload(event) -> PreparedPayload
```

`event` must be one of the five input models above. The factory generates the current Unix millisecond timestamp and freezes the complete payload,
returning a read-only object with `kind: str`, `stream_id: str`, `ts_ms: int`, and `payload: bytes`.
Use only factory-returned objects; do not construct or modify them manually. The object retains neither the original body nor an OpenEvent UUID;
preparation does not modify the input model, allocate a UUID, or issue RPCs. `payload` contains complete UTF-8 JSON bytes conforming to `llm.v1`,
preserving the contents of valid field values in the input model. After inspecting `prepared.payload`, pass that same `PreparedPayload` to the corresponding publishing function,
which sends the bytes that were checked.

### 2.3 Publishing Functions

```python
publish_infer_request(client, channel_id, principal, req) -> int
publish_infer_result(client, channel_id, principal, request_principal, result) -> int
publish_infer_append(client, channel_id, principal, request_principal, event) -> int
publish_infer_end(client, channel_id, principal, request_principal, event) -> int
publish_infer_cancel(client, channel_id, principal, cancel) -> int
```

Each function returns the message's committed OpenEvent `seq`. Arguments that violate `llm.v1`
during input-model construction, payload preparation, or publication raise `PayloadValidationError` before any OpenEvent
call. After validation succeeds, the function returns successfully only when publishing-related RPCs
obtain that seq; final publishing failure raises `ResultPublishError`. The caller supplies no OpenEvent
message UUID and cannot replace the function's result with a message observed through Subscribe.

Each event argument accepts either its corresponding input model or a `PreparedPayload`; a prepared object's `kind` must match the publishing function.
Input models automatically pass through `prepare_payload` inside the function. Prepared objects reuse their `ts_ms` and `payload` without re-encoding.
Each publishing-function call still allocates its own OpenEvent UUID. Retries within that call reuse this UUID and the same bytes under the
[reliable publishing contract](RESULT_PUBLISHING.md). `PreparedPayload` is an encoded in-memory input, not a retry handle across calls or restarts.
Passing the same object to a publishing function again starts another publication; it cannot resume an earlier call with an uncertain commit outcome.

For result, append, and end, `request_principal` is the OpenEvent principal that published the original
request; publishing functions use it to set output recipients. Request and cancel recipients are empty.
`channel_id`, `principal`, and `request_principal` must be positive integers; `bool` is excluded.

### 2.4 Parsing Functions and Models

```python
parse_payload(payload: bytes) -> InferRequest | InferResult | InferAppend | InferEnd | InferCancel
parse_message(message: EventMessage) -> ParsedMessage
```

Both functions parse only `llm.v1` business messages. When consuming Fetch/Subscribe directly, identify
system messages according to the [OpenEvent system protocol](../openevent-sdk/docs/SYSTEM_PROTOCOL.md#1-protocol-boundary).
`parse_payload` strictly parses a UTF-8 JSON payload. `parse_message` also returns OpenEvent metadata:

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

`ParsedMessage.ts_ms` is when the OpenEvent server received the publishing request, as defined by the
[OpenEvent API contract](../openevent-sdk/docs/API.md). `ParsedMessage.payload.ts_ms` is the timestamp
written into the payload by the protocol publisher. The five read-only payload models are
`InferRequest`, `InferResult`, `InferAppend`, `InferEnd`, and `InferCancel`.
`InferResult.has_body` and `InferEnd.has_body` indicate whether the message contains a body.
Parsing expands only the fields needed by the protocol. Reading a model's `body` or calling `to_dict()`
converts the body to Python objects and returns an independent copy.

Strict parsing failures raise `PayloadValidationError`:

```text
code: str
kind: str | None
stream_id: str | None
```

`code` uses the following fixed values:

| `code` | Meaning |
| --- | --- |
| `INVALID_JSON` | Payload is not valid UTF-8 JSON |
| `INVALID_PAYLOAD` | JSON root is not an object, or the field combination is invalid |
| `MISSING_REQUIRED_FIELD` | A required field for the current kind is missing |
| `UNKNOWN_FIELD` | A field not allowed for the current kind is present |
| `INVALID_KIND` | Kind is missing, has the wrong type, or is unsupported |
| `INVALID_STREAM_ID` | stream_id violates the protocol |
| `INVALID_TS_MS` | ts_ms violates the protocol |
| `INVALID_PROVIDER` | provider violates the protocol |
| `INVALID_METHOD` | method violates the protocol |
| `INVALID_PATH` | path violates the protocol |
| `INVALID_PREV_SEQ` | prev_seq violates the protocol |
| `INVALID_REQUEST_SEQ` | request_seq violates the protocol |
| `INVALID_STATUS_CODE` | status_code violates the protocol |
| `INVALID_HEADERS` | headers, or a header name or value, violates the protocol |
| `INVALID_END_STATUS` | end_status violates the protocol |
| `INVALID_BODY` | body, or stream in a request body, violates the protocol |
| `INVALID_PUBLISH_ARGUMENT` | An OpenEvent publishing argument, such as Channel or principal, is invalid |

`kind` or `stream_id` has a value only when the original payload has decoded to a JSON object
containing an independently identifiable valid field. A populated field does not mean the entire
message is valid.

## 3. OpenAI-like Client

### 3.1 Creating a Client

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

The client creates and owns its OpenEvent connection from the address. It does not accept an external
`OpenEventClient` or `grpc.Channel`. `rpc_timeout_ms` and `retry_interval_ms` must be positive integer
milliseconds, and `max_retries` must be a nonnegative integer; all three exclude `bool`.
`rpc_timeout_ms` limits each non-streaming OpenEvent RPC and Subscribe acceptance confirmation,
not the total model call duration. An established Subscribe has no whole-stream deadline.
`max_retries` and `retry_interval_ms` have the same meaning as in Section 2.1 and apply to this
instance's ordinary OpenEvent RPCs and reliable request/cancel publishing.

Missing, incorrectly typed, or invalid constructor arguments raise `ConfigurationError`;
arguments outside the signature raise `TypeError`.

`on_subscription_error` is invoked synchronously once per instance on final subscription failure,
with the `OpenEventSubscriptionError` defined in Section 5. Triggers include a permanent error or
exhausted retries during the first `GetStatus`, Subscribe establishment, reading, or reconnection,
and message-parsing failures. Temporary faults still being retried do not
notify; faults caused by closing the underlying connection follow the same rules.
The callback reports that the instance has stopped receiving new messages; individual calls receive
errors according to Section 5. Callback exceptions are logged without replacing the subscription
error. The callback may close the same client; `close()` does not wait for it to exit.

### 3.2 Starting a Call

```python
client.chat.completions.create(**kwargs)
client.responses.create(**kwargs)
```

Both methods accept optional control parameters `provider`, `stream_id`, and `prev_seq`.
These become top-level `llm.v1` request fields and are not included in the Provider body.
An omitted `stream_id` is generated as `stream_<uuid4 hex>`; an omitted `provider` lets the Worker
use its default provider; an omitted `prev_seq` produces no such field. All remaining keyword
arguments form the corresponding OpenAI request body. `stream=True` returns `OpenAIStream`;
an absent or `False` stream parameter waits for an ordinary result.

The first call prepares a replay position with one logical `GetStatus`; concurrent first calls
share it. The subscription then connects in the background. Request publishing need not wait for
Subscribe acceptance or reconnection; a recovered connection replays from the saved position.
Calls after final subscription failure or closure follow Sections 5 and 4, respectively.

If creating or starting the local subscription reader fails before the thread starts, the current
call raises the original exception, publishes no request, and invokes no subscription-error callback.
The client retains its state before initial preparation; other waiting or later calls may prepare again.

Publishing waits only for the reliable publishing API's seq or error; `request_seq` uses only its
successful return value. After publication succeeds, ordinary `create()` waits for the model response;
streaming `create()` returns `OpenAIStream` without waiting for subscription establishment or a model
response. Existing subscription errors still follow Section 5.

While publishing, the SDK may buffer early output and continue receiving other calls' messages.
Results or stream objects are delivered only after publication succeeds. Final publishing failure
discards that call's buffered output and raises the original `ResultPublishError`; subscribed
messages do not change the publishing conclusion.

### 3.3 Ordinary Return Object

An ordinary successful call returns `OpenAIResponse`:

```text
body
stream_id: str
request_seq: int
result_openevent_seq: int
status_code: int
headers
model_dump()
```

`body` and `model_dump()` return Python `dict` copies of the corresponding OpenAI response object,
which also supports recursive read-only attribute access. The SDK converts the body to Python objects
only on the first read of the body or a Provider field. If a Provider field conflicts with a fixed SDK attribute, the SDK attribute takes precedence;
the original Provider field remains accessible through `body`.

### 3.4 Streaming Return Object

`OpenAIStream` is a synchronous, closeable iterator. Each append produces an `OpenAIChunk`, whose
`body`, read-only attributes, and `model_dump()` follow the ordinary return-object rules, with the
additional `append_openevent_seq: int` property.

The stream's read-only properties are available to the business caller and update as soon as the SDK
can determine their values, without waiting for the caller to execute `next()` or a `for` loop:

| Property | Meaning and when it becomes known |
| --- | --- |
| `stream_id` | String call identifier supplied by the caller or generated by the SDK; determined before request publication and populated when the stream is returned. |
| `request_seq` | Positive integer seq assigned when the request is written to OpenEvent; determined by the reliable publishing API's successful return and populated when the stream is returned. |
| `result_openevent_seq` | OpenEvent seq of the first accepted result; determined when the subscription thread validates and accepts that message under the protocol. |
| `response_status_code` | Status code of that same result; updated together with the property above. |
| `response_headers` | Response headers of that same result; updated together with the properties above. |
| `terminal_has_body` | Whether the accepted end with `end_status="completed"` contains a body; determined when the subscription thread accepts that message. |
| `terminal_body` | A Python `dict` copy of the end's body when the preceding property is `True`; `None` when it is `False`; readable together with that property. The body is converted when read. |

Corresponding properties are `None` until the relevant result or completed end has been accepted.
A completed end without a body gives `terminal_has_body=False` and `terminal_body=None`.
A completed end with a body gives `terminal_has_body=True`.
Other terminal outcomes do not set these two completed-end properties.

Information received before publication completes is readable as soon as the stream is returned.
Property reads neither wait for messages nor consume the queue. Determined values are not cleared
or overwritten by iteration, closure, subscription failure, duplicates, or messages after a terminal
event. Even when `terminal_body` is readable, the iterator yields queued chunks in order before
ending or raising an error. Chunks already returned remain valid if the stream fails.

## 4. Closing

`OpenAIStream.close()` synchronously closes the current stream without affecting other calls on the
same client. If the instance has not entered `FAILED` and the SDK has not accepted this stream's terminal event, it publishes a cancel;
publishing failure raises the original `ResultPublishError`. Successfully publishing a cancel
proves only that the cancel was committed, not that the Worker or Provider has stopped.
If a terminal event was already accepted, closing is local and no cancel is published. Chunks received
by the SDK before closing remain iterable; if no terminal event or error was already accepted,
iteration ends after those chunks. Repeated or concurrent closes of the same stream share one
commit outcome and do not publish duplicate cancels. If the first close fails to publish, subsequent
closes each raise a fresh equivalent `ResultPublishError` with the same type, message, and public
fields, without sharing the exception object or earlier Python tracebacks.

After the instance enters `FAILED`, `stream.close()` only closes that stream locally: it allocates
no UUID, publishes no cancel, and starts no other OpenEvent request. Existing output and errors
continue to follow Section 5; closing itself does not raise the subscription error again. A cancel
already in progress finishes under Section 5, preserving actual publishing errors.

`OpenAI.close()` calls the instance's owned `OpenEventClient.close()`; repeated calls follow its
idempotent behavior, and it determines the call's duration. Closing adds no wait for publication, the subscription thread,
or callbacks to finish, and does not automatically cancel individual streams. Other calls and
background tasks may still be running when it returns; committed messages remain and the Worker
and Provider may continue running.

Closing preserves existing queues, output, terminal events, and errors. It sets no receipt boundary
or special closing exception and does not directly stop retries. Underlying errors still follow
ordinary RPC retry rules, reliable publishing, and Section 5. Final subscription failure enters
`FAILED`, invokes the callback, and wakes waiters; the prohibition on later requests remains.
A later `create()` uses the closed underlying client: it raises the saved subscription error if
already `FAILED`, or processes the underlying error otherwise.

The client supports context management. Callers must explicitly close it or use `with`; do not depend
on garbage collection to determine when it closes.

## 5. Exceptions

The main mappings are:

| Scenario | Exception |
| --- | --- |
| HTTP `400` | `BadRequestError` |
| HTTP `401` | `AuthenticationError` |
| HTTP `403` | `PermissionDeniedError` |
| HTTP `404` | `NotFoundError` |
| HTTP `409` | `ConflictError` |
| HTTP `422` | `UnprocessableEntityError` |
| HTTP `429` | `RateLimitError` |
| HTTP `500..599` | `InternalServerError` |
| Other non-2xx HTTP status | `APIStatusError` |
| Model Proxy status `60000` | `APITimeoutError` |
| Model Proxy status `60001..60003` | `APIConnectionError` |
| Model Proxy status `60004` or a winning cancel | `StreamCancelledError` |
| Model Proxy status `60009` | `BadRequestError` |
| Model Proxy status `60005/60007/60008` | `APIError` |
| `end_status="failed"` | `APIError` |
| Invalid protocol input model or call control parameters | `PayloadValidationError` |
| Invalid protocol-client or OpenAI-like-client constructor arguments | `ConfigurationError` |
| Invalid message shape or stream chain for the target call | `ProtocolError` |
| Final shared-subscription initialization, establishment, read, or reconnection failure, or subscription protocol validation failure | `OpenEventSubscriptionError` |
| Failure of any `publish_infer_*` publishing flow | `ResultPublishError` |

Streaming calls first accept the earliest result containing a body, end, or cancel in OpenEvent seq
order. A result containing a body maps directly through its HTTP or Model Proxy status code.
An end is first classified by `end_status`: `failed` always raises `APIError`, `interrupted` maps
through its Model Proxy status, and `completed` uses its HTTP status to determine success or failure.
A winning cancel always raises `StreamCancelledError`.

Except for `ConfigurationError`, `PayloadValidationError`, `ResultPublishError`,
`OpenEventSubscriptionError`, and Python's native `TypeError`, the exceptions above raised while
waiting or iterating in OpenAI-like calls expose the following read-only context. Fields that are
inapplicable or not yet known are `None`:

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

`OpenEventSubscriptionError` describes the entire client instance's shared subscription failure.
It exposes none of the per-call context above and adds neither `stream_id` nor `request_seq`.
It provides only these read-only failure fields:

```text
reason: "rpc" | "protocol"
last_status: str | None
protocol_error: ProtocolError | PayloadValidationError | None
```

`reason="rpc"` means GetStatus or Subscribe establishment, reading, or reconnection finally failed;
`last_status` holds the last available gRPC status. `reason="protocol"` means a subscribed message
cannot be parsed. Here, `protocol_error` retains the specific error's type, message, and public diagnostic fields, and `last_status`
is `None`. A parseable message that only breaks a target call's result shape or append chain gives
that call a `ProtocolError` without failing the whole Subscribe.

Each delivery of a subscription failure to a caller or callback constructs a fresh equivalent
`OpenEventSubscriptionError` with the same diagnostics, without sharing an exception object or earlier
Python tracebacks. Its nested `protocol_error` also retains no original traceback or exception chain.

Final shared-subscription failure puts the instance into irreversible `FAILED`. It must start no new
OpenEvent requests, including UUID allocation for request/cancel, initial or retried Publish calls,
UUID lookups or retries, and new GetStatus/Subscribe calls. A single RPC already started may return;
operations waiting between retries stop waiting and make no next attempt. `close()` itself does not
set `FAILED`; errors caused by closure enter the existing processing paths under Section 4.

When `FAILED` occurs, an in-progress request/cancel finishes using publishing evidence already obtained,
without sending further RPCs to obtain more evidence:

| Stage or return from the current RPC | Outcome |
| --- | --- |
| First Publish has not started and UUID allocation succeeds | The request returns the saved subscription error; the cancel closes locally without Publish. |
| In-flight UUID allocation fails | Original `ResultPublishError(NOT_COMMITTED)`, with `event_uuid=None`, without retrying. |
| In-flight Publish or UUID lookup succeeds and returns a seq | Preserve successful publication and that seq without restoring the subscription or starting other requests. |
| Publish returns `ALREADY_EXISTS` before lookup starts | `ResultPublishError(COMMITTED)`, without lookup. |
| In-flight Publish fails, or its retry wait is stopped | Return `NOT_COMMITTED` or `UNKNOWN` using existing evidence under the publishing contract, without retrying. |
| Lookup after confirmed `ALREADY_EXISTS` fails, or its retry is stopped | `ResultPublishError(COMMITTED)`. |

This section applies only to requests initiated by this OpenAI-like instance. Independent protocol SDK
clients and Workers continue to follow their own publishing contracts.

Final shared-subscription failure stops receipt of new messages. Already received output is delivered
first. Calls with an existing terminal event end according to it; calls without one receive the saved
`OpenEventSubscriptionError` after existing output has been delivered. If the request is still
publishing, first determine the publishing conclusion under Section 3.2: only successful publication
uses these received results; publishing failure still returns the original publishing error.

Final failure of the first `GetStatus` occurs before request publication and directly returns
`OpenEventSubscriptionError`.

The fields and decision rules for `ResultPublishError` and `CommitState` are defined only in the
single-event reliable publishing contract.
