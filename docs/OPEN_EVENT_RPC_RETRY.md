# Ordinary OpenEvent RPC Retry Rules

[中文版](OPEN_EVENT_RPC_RETRY_cn.md)

> Status: current contract

This document defines common attempt counts, waiting intervals, and error classification for ordinary
OpenEvent RPCs used by the Model Proxy Worker, protocol SDK, and OpenAI-like client. These RPCs
include `GetStatus`, each `Fetch` page, `AllocateUuids` (the Python SDK's `get_uuid()`),
`GetSeqByUuid`, and establishment of one new `Subscribe`.

`PublishAutoSeq` may commit a message without returning success, so it is not an ordinary RPC
in this document. Its retries and commit state follow only the
[single-event reliable publishing contract](RESULT_PUBLISHING.md). Provider HTTP requests also
do not use these rules.

## 1. Attempts and Waiting

Each logical operation runs once initially. After a retryable failure, it makes at most `max_retries`
extra attempts, waiting the fixed `retry_interval_ms` before each. The Worker reads both values from
its YAML `worker` configuration; the protocol SDK and OpenAI-like client receive them as constructor
arguments. Defaults and validation are defined by [Worker configuration](CONFIGURATION.md) and
the [SDK API contract](SDK_API.md). Defaults allow at most `3` extra attempts with `1000 ms`
between them. `max_retries=0` runs once without waiting or retrying. Each attempt still uses the
configured per-call `rpc_timeout_ms` deadline.

These counts are upper bounds. Once the high-level `OpenAI` client enters `FAILED`, it stops waiting
to retry and must not start another RPC; the exact boundary is defined in
[SDK_API.md Section 5](SDK_API.md#5-exceptions). The independently used protocol SDK and Worker
are not affected by that client's state.

Different logical operations have separate counters. For example, two Fetch pages, one UUID
allocation, and one UUID lookup each have independent retry budgets. If UUID allocation succeeds
at the server but its response is lost, another call may obtain a different UUID. UUIDs never returned
to the upper layer may remain unused gaps; they need no reclamation and cannot be used to publish messages.

## 2. Error Classification

The following errors are retryable:

- `CANCELLED`, `DEADLINE_EXCEEDED`, `UNKNOWN`, `UNAVAILABLE`, and `INTERNAL`.
- Connection loss or reset, or no available gRPC status.

Only these errors may be retried. Every other error carrying a gRPC status finishes immediately,
including `UNAUTHENTICATED`, `PERMISSION_DENIED`, `NOT_FOUND`, `INVALID_ARGUMENT`,
`RESOURCE_EXHAUSTED`, and `OUT_OF_RANGE`. For example, `OUT_OF_RANGE` from a Subscribe
position beyond the current `max_seq + 1` is a final establishment error; do not reconnect repeatedly
with the same position.

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
