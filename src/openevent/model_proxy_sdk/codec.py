from __future__ import annotations

import json
from typing import Any

from .errors import ModelProxySDKError
from .model import Header, InferRequest, InferRequestInput, InferResult, InferResultInput
from .validator import validate_payload_dict


def dumps_payload(data: dict[str, Any]) -> bytes:
    validate_payload_dict(data)
    try:
        return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ModelProxySDKError("INVALID_JSON", "payload cannot be encoded as JSON") from exc


def loads_payload(payload: bytes) -> dict[str, Any]:
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ModelProxySDKError("INVALID_JSON", "payload is not UTF-8 JSON") from exc
    return validate_payload_dict(data)


def request_input_to_dict(req: InferRequestInput, ts_ms: int, prev_seq: int | None = None) -> dict[str, Any]:
    data = {
        "kind": "infer.request",
        "request_id": req.request_id,
        "method": req.method,
        "path": req.path,
        "ts_ms": ts_ms,
        "body": req.body,
    }
    if prev_seq is not None:
        data["prev_seq"] = prev_seq
    return data


def result_input_to_dict(req: InferResultInput, ts_ms: int) -> dict[str, Any]:
    data = {
        "kind": "infer.result",
        "request_id": req.request_id,
        "prev_seq": req.prev_seq,
        "ts_ms": ts_ms,
        "status_code": req.status_code,
        "body": req.body,
    }
    if req.headers or 100 <= req.status_code <= 599:
        data["headers"] = [{"name": h.name, "value": h.value} for h in req.headers]
    return data


def dict_to_model(data: dict[str, Any]) -> InferRequest | InferResult:
    validate_payload_dict(data)
    if data["kind"] == "infer.request":
        return InferRequest(
            request_id=data["request_id"],
            method=data["method"],
            path=data["path"],
            ts_ms=data["ts_ms"],
            body=data["body"],
            prev_seq=data.get("prev_seq"),
        )
    headers = [Header(name=h["name"], value=h["value"]) for h in data.get("headers", [])]
    return InferResult(
        request_id=data["request_id"],
        prev_seq=data["prev_seq"],
        ts_ms=data["ts_ms"],
        status_code=data["status_code"],
        headers=headers,
        body=data["body"],
    )
