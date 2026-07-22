# Result Publishing Reliability

This document is the authoritative design for publishing one `infer.result`.

`openevent.model_proxy_sdk.publish_infer_result()` owns the complete state
machine. Before the first PublishAutoSeq call, the protocol SDK freezes the encoded payload,
recipient, `request_id`, `prev_seq`, and `ts_ms`. Every retry uses those exact
values.

The publisher has three states:

1. `PUBLISH`: PublishAutoSeq success completes the result. An error that OpenEvent
   guarantees was not committed is fatal because the SDK cannot repair
   authentication, permission, Channel, argument, or payload failures.
2. `RECONCILE`: every other publish error has an uncertain outcome. The SDK
   obtains one post-failure GetStatus watermark and Fetches the target Channel
   from `prev_seq + 1` through that watermark with `only_my_recipient=false`.
   A result matches only when Channel, proxy principal, request recipient,
   `request_id`, and `prev_seq` all match.
3. `DONE`: a successful publish or a matching reconciled message returns its seq.

If a complete reconciliation finds no match, the publisher returns to `PUBLISH`.
It never republishes while the outcome remains uncertain. Publish and
reconciliation failures share a limit of three; retries use bounded exponential
backoff. A permanent error or exhaustion raises `ResultPublishError`. The worker
turns that into a fatal process exit; after restart, normal history recovery
determines whether the request still needs a result.
