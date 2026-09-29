# Ordinary OpenEvent RPC Retry Rules

[中文版](OPEN_EVENT_RPC_RETRY_cn.md)

These rules apply to the Model Proxy Worker, protocol SDK, and OpenAI-like client's
`GetStatus`, each `Fetch` page, `AllocateUuids` (the Python SDK's `get_uuid()`),
`GetSeqByUuid`, and establishment of one new `Subscribe`.

For `PublishAutoSeq` retries and commit states, see the
[single-event reliable publishing contract](RESULT_PUBLISHING.md). Provider HTTP requests also
do not use these rules.

## 1. Attempts and Waiting

Each logical operation runs once initially. After a retryable failure, it makes at most `max_retries`
extra attempts, waiting the fixed `retry_interval_ms` before each. The Worker reads both values from
its YAML `worker` configuration; the protocol SDK and OpenAI-like client receive them as constructor
arguments. Defaults and validation are defined by [Worker configuration](CONFIGURATION.md) and
the [SDK API contract](SDK_API.md). `max_retries=0` runs once. Each attempt uses the configured
per-call `rpc_timeout_ms` deadline. Different logical operations have separate counters;
each Fetch page, UUID allocation, and UUID lookup has its own budget.

## 2. Error Classification

The following errors are retryable:

- `CANCELLED`, `DEADLINE_EXCEEDED`, `UNKNOWN`, `UNAVAILABLE`, and `INTERNAL`.
- Connection loss or reset, or no available gRPC status.

Only these errors may be retried. Every other error carrying a gRPC status finishes immediately,
including `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`,
`RESOURCE_EXHAUSTED`, and `OUT_OF_RANGE`.

Existing stop rules, such as Worker shutdown or a high-level instance entering `FAILED`, stop
further retries. `OpenAI.close()` only closes the underlying client and does not set such a stop
condition; resulting errors still follow the classification above. Subscription failure notification
and closing behavior are defined in [SDK_API.md](SDK_API.md).

## 3. Subscribe

Each Subscribe establishment first makes one attempt. Failures use the same budget to create
replacement connections. Once the server accepts a connection, that round's counter resets;
a later disconnection starts a new round.

An acceptance timeout or establishment failure proceeds to the next attempt. The Worker and SDK
must ensure that retries leave no competing Subscribe connections. An accepted Subscribe has no
whole-RPC deadline.

## 4. Final Outcomes

Exhausting retries or encountering a permanent error fails the logical operation, without starting
background retries:

- The Worker exits on a final GetStatus, Fetch, or Subscribe failure.
- The OpenAI-like client saves `OpenEventSubscriptionError` on a final GetStatus or Subscribe failure
  and rejects new calls on that instance. Publishing conclusions, existing output, and subscription
  error delivery for calls already started follow [SDK_API.md Section 5](SDK_API.md#5-exceptions).
- UUID allocation failure makes the publishing API return
  `ResultPublishError(commit_state=NOT_COMMITTED, event_uuid=None)`.
- Final `commit_state` after a `GetSeqByUuid` failure follows the
  [single-event reliable publishing contract](RESULT_PUBLISHING.md).
