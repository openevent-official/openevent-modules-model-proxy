from __future__ import annotations

import time
from typing import Any

from .model import InferResultInput

ERROR_CODES = {
    60000: "MODEL_API_TIMEOUT",
    60001: "DNS_RESOLUTION_FAILED",
    60002: "TLS_HANDSHAKE_FAILED",
    60003: "CONNECTION_FAILED",
    60004: "REQUEST_CANCELLED",
    60005: "DUPLICATE_REQUEST_ID",
    60007: "MODEL_PROXY_INTERNAL_ERROR",
    60008: "PAYLOAD_TOO_LARGE",
    60009: "INVALID_REQUEST_PAYLOAD",
}


def proxy_error_body(status_code: int, message: str, context: dict[str, Any] | None = None) -> dict:
    code = ERROR_CODES.get(status_code, "MODEL_PROXY_ERROR")
    error = {"code": code, "message": message, "type": "model_proxy_error"}
    if context:
        error["context"] = context
    return {"error": error}


def proxy_error_result(
    request_id: str,
    prev_seq: int,
    status_code: int,
    message: str,
    context: dict[str, Any] | None = None,
    ts_ms: int | None = None,
) -> InferResultInput:
    return InferResultInput(
        request_id=request_id,
        prev_seq=prev_seq,
        ts_ms=ts_ms or int(time.time() * 1000),
        status_code=status_code,
        body=proxy_error_body(status_code, message, context),
    )
