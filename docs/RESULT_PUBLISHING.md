# Single-Event Reliable Publishing Contract

[中文版](RESULT_PUBLISHING_cn.md)

This contract applies to Model Proxy protocol SDK publication of
`infer.request`, `infer.result`, `infer.append`, `infer.end`, and
`infer.cancel`. See [LLM_PROTOCOL.md](LLM_PROTOCOL.md) for `llm.v1` fields and stream chains, and the
[OpenEvent API contract](../openevent-sdk/docs/API.md) for the underlying UUID, `PublishAutoSeq`,
and `GetSeqByUuid` semantics.

Invalid input raises `PayloadValidationError` as defined in [SDK_API.md](SDK_API.md); valid input
follows the publication flow below. A logical event is one complete OpenEvent message.
Committed means OpenEvent has durably stored the message, not that the model call has completed.

## 1. Freezing Before Publication

Each logical event uses one UUID. UUID allocation through the SDK's `get_uuid()` follows the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md). Use only a successfully returned,
valid nonzero UUID. A retry after a lost response may obtain another UUID; unreturned UUIDs remain
unused gaps and are neither used for publishing nor reclaimed. Once the UUID is obtained, freeze:

- Complete UTF-8 payload bytes, including the initially generated `kind` and `ts_ms`.
- Channel, publishing principal, and recipients; output events also include the request principal.
- OpenEvent UUID.
- All other request fields passed to `PublishAutoSeq`.

Subsequent publishing attempts must reuse the same UUID and frozen request.
If UUID allocation finally fails, do not call `PublishAutoSeq`; raise
`ResultPublishError(commit_state=NOT_COMMITTED, event_uuid=None, committed_seq=None)`.
Populate `last_status` when a gRPC status is available.

## 2. PublishAutoSeq Flow

Call `PublishAutoSeq` with the frozen request; return the committed seq on success or raise
`ResultPublishError` on final failure. After a retryable failure, make at most `max_retries`
extra attempts, waiting the fixed `retry_interval_ms` before each. Counts and intervals
come from the same Worker or SDK client configuration used by ordinary RPCs; see the configuration
entry points in the [ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md#1-attempts-and-waiting).
Publishing retries have their own counter.

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

Exhausting retries for uncertain errors raises `ResultPublishError(commit_state=UNKNOWN)`.

## 3. GetSeqByUuid Reconciliation

UUID reconciliation begins only after `ALREADY_EXISTS` proves that a committed message consumed
the UUID. The SDK calls `GetSeqByUuid(uuid)` without scanning message history or comparing payloads.

All `GetSeqByUuid` attempt counts, waits, and error classifications follow the
[ordinary OpenEvent RPC retry rules](OPEN_EVENT_RPC_RETRY.md), counted separately from `PublishAutoSeq`.

- A successful lookup with a valid positive integer seq returns that committed seq and completes publication.
- A lookup that finally fails under the ordinary RPC contract returns `commit_state=COMMITTED`
  with `committed_seq=None`.

Once UUID reconciliation starts, do not call `PublishAutoSeq` again or change UUIDs.

## 4. ResultPublishError

```python
from openevent.model_proxy_sdk import CommitState, ResultPublishError
```

Exception fields are read-only: `commit_state` is a `CommitState` enum, `event_uuid` and
`committed_seq` have type `int | None`, and `last_status` has type `str | None`.

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

Only `NOT_COMMITTED` allows the caller to treat the logical event as unpublished. Neither
`COMMITTED` nor `UNKNOWN` permits republishing the same logical event under a new UUID,
which could duplicate messages or break the `prev_seq` chain. The caller may explicitly stop waiting,
but must accept that the event may already be committed or may later produce output no longer
received by the current call.

## 5. Call Boundaries

The attempt counts above are upper bounds. Once the high-level `OpenAI` client enters `FAILED`,
it must not start another RPC even if attempts remain. For preserving publication evidence from
an already started single RPC and stopping before the first Publish, see
[SDK_API.md Section 5](SDK_API.md#5-exceptions). The independently used protocol SDK and Worker
are not affected by that client's state.

The publishing API requires no established Subscribe and does not wait for model output.
It finishes with a seq or final error, with no background publishing afterward. The protocol SDK
stores no local durable publishing state and creates no recovery handle. Messages read through
Subscribe cannot change the publishing result. For the high-level client's `request_seq` and model
output delivery rules, see [SDK_API.md Section 3.2](SDK_API.md#32-starting-a-call).
