from __future__ import annotations

import time

from .client import ModelProxyProtocolClient
from .codec import dict_to_model, dumps_payload, loads_payload, request_input_to_dict
from .model import InferRequest, InferRequestInput, InferResult, InferResultInput, ParsedMessage
from .result_publishing import publish_result


def parse_payload(payload: bytes) -> InferRequest | InferResult:
    return dict_to_model(loads_payload(payload))


def parse_message(message) -> ParsedMessage:
    parsed = parse_payload(message.payload)
    return ParsedMessage(
        seq=int(message.seq),
        channel_id=int(message.channel_id),
        principal=int(message.principal),
        recipients=tuple(int(v) for v in message.recipients),
        payload=parsed,
    )


def publish_infer_request(
    client: ModelProxyProtocolClient,
    channel_id: int,
    principal: int,
    req: InferRequestInput,
    prev_seq: int | None = None,
) -> int:
    ts_ms = req.ts_ms or int(time.time() * 1000)
    payload = dumps_payload(request_input_to_dict(req, ts_ms=ts_ms, prev_seq=prev_seq))
    resp = client.openevent_client.publish_auto_seq(
        principal=principal,
        token=client.token,
        channel_id=channel_id,
        payload=payload,
        recipients=(),
    )
    return int(resp.seq)


def publish_infer_result(
    client: ModelProxyProtocolClient,
    channel_id: int,
    principal: int,
    request_principal: int,
    req: InferResultInput,
) -> int:
    return publish_result(
        client=client,
        channel_id=channel_id,
        principal=principal,
        request_principal=request_principal,
        req=req,
    )
