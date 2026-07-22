from __future__ import annotations

import time

from .client import ModelProxyProtocolClient
from .codec import dict_to_model, dumps_payload, loads_payload, result_input_to_dict
from .errors import ModelProxySDKError, ResultPublishError
from .model import InferResult, InferResultInput


MAX_FAILURES = 3
INITIAL_BACKOFF_S = 0.2
MAX_BACKOFF_S = 2.0
_GUARANTEED_NOT_COMMITTED = frozenset(
    {"UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND", "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED", "ABORTED"}
)


def publish_result(
    client: ModelProxyProtocolClient,
    channel_id: int,
    principal: int,
    request_principal: int,
    req: InferResultInput,
) -> int:
    ts_ms = req.ts_ms or int(time.time() * 1000)
    payload = dumps_payload(result_input_to_dict(req, ts_ms=ts_ms))
    failures = 0
    state = "PUBLISH"
    reconcile_max_seq: int | None = None
    cursor = req.prev_seq + 1

    while True:
        try:
            if state == "PUBLISH":
                response = client.openevent_client.publish_auto_seq(
                    principal=principal,
                    token=client.token,
                    channel_id=channel_id,
                    payload=payload,
                    recipients=(request_principal,),
                )
                return int(response.seq)

            if reconcile_max_seq is None:
                reconcile_max_seq = int(
                    client.openevent_client.get_status(principal, client.token).max_seq
                )
            matched_seq, cursor = _scan_for_result(
                client,
                channel_id,
                principal,
                request_principal,
                req,
                cursor,
                reconcile_max_seq,
            )
            if matched_seq is not None:
                return matched_seq
            state = "PUBLISH"
            reconcile_max_seq = None
            cursor = req.prev_seq + 1
        except Exception as exc:
            failures += 1
            if state == "PUBLISH":
                if _grpc_code_name(exc) in _GUARANTEED_NOT_COMMITTED:
                    raise ResultPublishError("infer.result was not committed") from exc
                state = "RECONCILE"
            if failures >= MAX_FAILURES:
                raise ResultPublishError("unable to determine whether infer.result was committed") from exc
            time.sleep(min(INITIAL_BACKOFF_S * (2 ** (failures - 1)), MAX_BACKOFF_S))


def _scan_for_result(
    client: ModelProxyProtocolClient,
    channel_id: int,
    principal: int,
    request_principal: int,
    req: InferResultInput,
    cursor: int,
    reconcile_max_seq: int,
) -> tuple[int | None, int]:
    while cursor <= reconcile_max_seq:
        response = client.openevent_client.fetch(
            principal=principal,
            token=client.token,
            from_seq=cursor,
            limit=1000,
            only_my_recipient=False,
            channels=(channel_id,),
        )
        for message in response.messages:
            if int(message.seq) > reconcile_max_seq:
                continue
            if int(message.channel_id) != channel_id or int(message.principal) != principal:
                continue
            if {int(value) for value in message.recipients} != {request_principal}:
                continue
            try:
                parsed = dict_to_model(loads_payload(message.payload))
            except ModelProxySDKError:
                continue
            if isinstance(parsed, InferResult) and parsed.request_id == req.request_id and parsed.prev_seq == req.prev_seq:
                return int(message.seq), cursor
        next_seq = int(response.next_seq)
        if next_seq <= cursor:
            raise RuntimeError("Fetch did not advance during result reconciliation")
        cursor = next_seq
    return None, cursor


def _grpc_code_name(exc: Exception) -> str | None:
    code = getattr(exc, "code", None)
    if not callable(code):
        return None
    try:
        value = code()
    except Exception:
        return None
    return getattr(value, "name", None) or str(value).rsplit(".", 1)[-1]
