# Single-Event Reliable Publishing Contract

[中文版](RESULT_PUBLISHING_cn.md)

> Status: current contract

This is the sole authoritative contract for publishing one OpenEvent event through the Model Proxy
protocol SDK. It applies to `infer.request`, `infer.result`, `infer.append`, `infer.end`, and
`infer.cancel`. See [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for `llm.v1` fields and stream chains, and the
[OpenEvent API contract](../openevent-sdk/docs/API.md) for the underlying UUID, `PublishAutoSeq`,
and `GetSeqByUuid` semantics.

This contract begins after inputs and publishing arguments pass protocol validation. It defines
freezing, publication, retries, UUID reconciliation, and final errors for one logical event.
Invalid input raises `PayloadValidationError` as defined in [SDK_API.md](SDK_API.md). This contract
does not decide whether to publish the next protocol message, maintain cross-message stream state,
or choose application-level business retries.

The terminology refers to the **OpenEvent publishing layer**: the `openevent-sdk` client calling the
OpenEvent server, rather than the model-proxy Worker or a provider HTTP request. A logical event is
one complete OpenEvent message the caller wants to write, such as an `infer.request`. Its `UUID` is
the unique identifier reserved by OpenEvent; its `seq` is the global sequence number assigned by the
server when the message is committed. `PublishAutoSeq` writes the message and assigns a seq;
`GetSeqByUuid` looks up a committed message's seq by UUID. Committed means OpenEvent has accepted
and durably stored the message; it does not mean the provider has processed it or model-proxy has
returned a result.

## 1. Freezing Before Publication

Each high-level publishing API call obtains one final OpenEvent UUID for its logical event.
UUID allocation (`AllocateUuids` in OpenEvent, used by the SDK's `get_uuid()`) follows the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md). Only a successfully returned, valid
nonzero UUID becomes this event's UUID. If allocation succeeds at the server but its response is lost,
a retry may obtain another UUID. The previous UUID remains an unused gap: this is allowed, it must
not be used for `PublishAutoSeq`, and it needs no compensating write or reclamation. Once the UUID
has been obtained, freeze all of the following:

- Complete UTF-8 payload bytes, including the initially generated `kind` and `ts_ms`.
- Channel, publishing principal, and recipients; output events also include the request principal.
- OpenEvent UUID.
- All other request fields passed to `PublishAutoSeq`.

Each request, result, append, end, and cancel has its own UUID. All subsequent publishing attempts
must reuse that UUID and the same frozen request. They cannot change the payload, `prev_seq`,
`ts_ms`, recipients, or other fields.

If UUID allocation still has not returned a valid UUID after exhausting retries, do not call
`PublishAutoSeq`. The publishing API returns:

```text
ResultPublishError(
    commit_state=NOT_COMMITTED,
    event_uuid=None,
    committed_seq=None,
)
```

Populate `last_status` when a gRPC status is available. The event is definitely unpublished in this
case because the SDK has not obtained an event UUID to pass to `PublishAutoSeq`.

## 2. PublishAutoSeq Flow

Each event follows this flow:

1. Call `PublishAutoSeq` with the frozen request.
2. Finish, retry with the same UUID, or proceed to `GetSeqByUuid` reconciliation according to this section.
3. Return the committed event's seq or raise `ResultPublishError`.

A logical publication first calls `PublishAutoSeq` once. A successful response conforming to the
OpenEvent contract always contains a valid positive integer seq. After a retryable failure described
below, make at most `max_retries` extra attempts. The first attempt does not count toward
`max_retries`; wait the fixed `retry_interval_ms` before every extra attempt. Counts and intervals
come from the same Worker or SDK client configuration used by ordinary RPCs; see the configuration
entry points in the [ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md#1-attempts-and-waiting).
Publishing retries have their own counter. Exhausting retries does not start background publishing.

### 2.1 Response Classification

- `OK` with a valid positive integer seq: the event is committed; return that seq directly.
- `ALREADY_EXISTS`: this attempt created no new message, and a previously committed message already
  consumed the UUID. Stop publishing and call `GetSeqByUuid` under Section 3 to obtain its seq.
- `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`, or `RESOURCE_EXHAUSTED`:
  this attempt definitely did not commit. The overall state remains `UNKNOWN` if an earlier attempt
  was uncertain; otherwise it is `NOT_COMMITTED`. Finish immediately on these definite terminal
  outcomes without consuming further publishing retries.
- `CANCELLED`, `DEADLINE_EXCEEDED`, `UNKNOWN`, `UNAVAILABLE`, `INTERNAL`, a disconnection, or no
  gRPC status: the commit outcome is uncertain. Continue bounded retries with the same UUID and
  frozen request.
- Other non-`OK` outcomes not explicitly guaranteed by the OpenEvent contract to be uncommitted:
  the commit outcome is uncertain; continue bounded retries with the same UUID and frozen request.

A later guarantee that this particular attempt did not commit cannot overturn an earlier uncertain
attempt. Exhausted publishing retries leave the overall state `UNKNOWN` and raise `ResultPublishError`.
The publishing API succeeds only when `PublishAutoSeq` returns a seq, or when `GetSeqByUuid`
returns a seq after `ALREADY_EXISTS`.

## 3. GetSeqByUuid Reconciliation

UUID reconciliation begins only after `ALREADY_EXISTS` proves that a committed message consumed
the UUID. The SDK calls `GetSeqByUuid(uuid)` without scanning message history or comparing payloads.

All `GetSeqByUuid` attempt counts, waits, and error classifications follow the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md), counted separately from `PublishAutoSeq`.

- A successful lookup with a valid positive integer seq returns that committed seq and completes publication.
- A lookup that finally fails under the ordinary RPC contract returns `commit_state=COMMITTED`
  with `committed_seq=None`.

Once UUID reconciliation starts, do not call `PublishAutoSeq` again or change UUIDs. The earlier
`ALREADY_EXISTS` already proves that the event exists; lookup failure only means its seq is currently
unavailable.

## 4. ResultPublishError

Callers import the error type and commit-state enum from the public package:

```python
from openevent.model_proxy_sdk import CommitState, ResultPublishError
```

`commit_state` is a `CommitState` enum whose only values are `NOT_COMMITTED`, `COMMITTED`, and
`UNKNOWN`. The uppercase names in this document refer to those enum values. `event_uuid` and
`committed_seq` both have type `int | None`; `last_status` has type `str | None`. They are read-only
result fields on the exception. Callers should read them rather than constructing errors or changing
commit states. For example:

```python
try:
    publish_one_event(...)
except ResultPublishError as exc:
    if exc.commit_state is CommitState.NOT_COMMITTED:
        # This logical event definitely was not written to OpenEvent.
        pass
    else:
        # It may already exist; do not republish with a different UUID.
        record_for_manual_handling(exc.event_uuid, exc.committed_seq, exc.last_status)
```

- `commit_state`:
  - `NOT_COMMITTED`: the logical event is provably unpublished. Typical cases are failed UUID
    allocation, or publishing attempts that all returned definite uncommitted terminal outcomes
    without any uncertain attempt.
  - `COMMITTED`: a committed message provably consumed the UUID, but no valid seq is currently
    available to return.
  - `UNKNOWN`: at least one publishing attempt may have committed, and there is neither an obtained
    committed seq nor other proof of commitment.
- `event_uuid`: populated after a UUID is successfully obtained and frozen; `None` on allocation failure.
- `committed_seq`: the confirmed committed seq, or `None` when unknown. Normally, obtaining a valid
  seq causes the publishing API to return it successfully.
- `last_status`: the last observed gRPC or transport status, for diagnostics only; it cannot independently
  override the `commit_state` conclusion.

`COMMITTED` describes only the fact that the message has been written, not that this SDK call
succeeded. If `ALREADY_EXISTS` proves UUID consumption but the subsequent lookup finally fails,
the call still ends with `ResultPublishError`.

Only `NOT_COMMITTED` allows the caller to treat the logical event as unpublished. Neither
`COMMITTED` nor `UNKNOWN` permits republishing the same logical event under a new UUID,
which could duplicate messages or break the `prev_seq` chain. The caller may explicitly stop waiting,
but must accept that the event may already be committed or may later produce output no longer
received by the current call.

## 5. Call Boundaries

The attempt counts above are upper bounds. Once the high-level `OpenAI` client enters `FAILED`,
it must not start another RPC even if attempts remain. For preserving publication evidence from
an already started single RPC and stopping before the first Publish, see
[SDK_API.md Section 5](SDK_API.md#5-exceptions). This does not change the meaning of commit states
in this contract or affect the independently used protocol SDK and Worker.

The protocol SDK stores no local durable publishing state. OpenEvent's committed messages and UUID
index are the durable facts. Reliable publishing returns a committed seq or raises the
`ResultPublishError` defined here. It creates no recovery handle, performs no background publishing
after the API returns, and does not decide whether the Worker exits.

The publishing API does not depend on an established Subscribe. It finishes with a seq or final
publishing error without waiting for Worker model output. The high-level client's public
`request_seq` uses only a seq successfully returned by this API. Identifying subscribed message
ownership and buffering output do not change the publishing conclusion. Final publishing failure
raises the original `ResultPublishError`; messages read through Subscribe cannot change that outcome,
and publishing does not continue in the background. For when model output is delivered to the
business caller, see [SDK_API.md Section 3.2](SDK_API.md#32-starting-a-call).
