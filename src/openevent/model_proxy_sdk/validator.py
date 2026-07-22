from __future__ import annotations

import re
from typing import Any

from .errors import ModelProxySDKError
from .model import ALLOWED_METHODS, KIND_INFER_REQUEST, KIND_INFER_RESULT

_REQUEST_FIELDS = frozenset({"kind", "request_id", "prev_seq", "method", "path", "ts_ms", "body"})
_RESULT_FIELDS = frozenset({"kind", "request_id", "prev_seq", "ts_ms", "status_code", "headers", "body"})
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


def validate_request_id(value: Any) -> str:
    if not isinstance(value, str) or not _REQUEST_ID_RE.fullmatch(value):
        raise ModelProxySDKError(
            "INVALID_REQUEST_ID",
            "request_id must be 1..128 chars of [A-Za-z0-9._:-]",
            {"request_id": value},
        )
    return value


def validate_ts_ms(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ModelProxySDKError("INVALID_PAYLOAD", "ts_ms must be a positive integer", {"ts_ms": value})
    return value


def validate_prev_seq(value: Any, *, required: bool) -> int | None:
    if value is None and not required:
        return None
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ModelProxySDKError("INVALID_PREV_SEQ", "prev_seq must be a positive integer", {"prev_seq": value})
    return value


def validate_json_value(value: Any, field: str = "body") -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        for item in value:
            validate_json_value(item, field)
        return value
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ModelProxySDKError("INVALID_PAYLOAD", f"{field} object keys must be strings", {"key": key})
            validate_json_value(item, field)
        return value
    raise ModelProxySDKError("INVALID_PAYLOAD", f"{field} must be JSON representable", {field: type(value).__name__})


def validate_method(value: Any) -> str:
    if not isinstance(value, str) or value not in ALLOWED_METHODS:
        raise ModelProxySDKError("INVALID_METHOD", "method is not allowed", {"method": value})
    return value


def validate_path(value: Any) -> str:
    if not isinstance(value, str):
        raise ModelProxySDKError("INVALID_PATH", "path must be a string", {"path": value})
    if not value.startswith("/"):
        raise ModelProxySDKError("INVALID_PATH", "path must start with /", {"path": value})
    if "://" in value or "?" in value or "#" in value or any(ord(ch) < 32 or ord(ch) == 127 for ch in value):
        raise ModelProxySDKError("INVALID_PATH", "path must not contain scheme, query, fragment, or controls", {"path": value})
    return value


def validate_headers(value: Any) -> list[dict[str, str]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ModelProxySDKError("INVALID_PAYLOAD", "headers must be a list", {"headers": value})
    headers = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"name", "value"}:
            raise ModelProxySDKError("INVALID_PAYLOAD", "header must contain name and value", {"header": item})
        name = item["name"]
        header_value = item["value"]
        if not isinstance(name, str) or not isinstance(header_value, str):
            raise ModelProxySDKError("INVALID_PAYLOAD", "header name and value must be strings", {"header": item})
        headers.append({"name": name, "value": header_value})
    return headers


def validate_status_code(value: Any) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ModelProxySDKError("INVALID_PAYLOAD", "status_code must be an integer", {"status_code": value})
    if not ((100 <= value <= 599) or (60000 <= value <= 60010)):
        raise ModelProxySDKError("INVALID_PAYLOAD", "status_code is out of range", {"status_code": value})
    return value


def validate_payload_dict(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ModelProxySDKError("INVALID_PAYLOAD", "payload must be a JSON object", {"payload_type": type(data).__name__})
    kind = data.get("kind")
    if kind == KIND_INFER_REQUEST:
        _validate_keys(data, _REQUEST_FIELDS)
        validate_request_id(data.get("request_id"))
        validate_ts_ms(data.get("ts_ms"))
        validate_prev_seq(data.get("prev_seq"), required=False)
        validate_method(data.get("method"))
        validate_path(data.get("path"))
        if "body" not in data:
            raise ModelProxySDKError("MISSING_REQUIRED_FIELD", "body is required", {"field": "body"})
        validate_json_value(data["body"])
        return data
    if kind == KIND_INFER_RESULT:
        _validate_keys(data, _RESULT_FIELDS)
        validate_request_id(data.get("request_id"))
        validate_ts_ms(data.get("ts_ms"))
        validate_prev_seq(data.get("prev_seq"), required=True)
        validate_status_code(data.get("status_code"))
        if "body" not in data:
            raise ModelProxySDKError("MISSING_REQUIRED_FIELD", "body is required", {"field": "body"})
        validate_json_value(data["body"])
        validate_headers(data.get("headers", []))
        return data
    raise ModelProxySDKError("INVALID_KIND", "kind must be infer.request or infer.result", {"kind": kind})


def _validate_keys(data: dict[str, Any], allowed: frozenset[str]) -> None:
    unknown = sorted(set(data) - allowed)
    if unknown:
        raise ModelProxySDKError("UNKNOWN_FIELD", "payload contains unknown fields", {"fields": unknown})
    for key in allowed:
        if key in {"prev_seq", "headers"}:
            continue
        if key not in data:
            raise ModelProxySDKError("MISSING_REQUIRED_FIELD", f"{key} is required", {"field": key})
