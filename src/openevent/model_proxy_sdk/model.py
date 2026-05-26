from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


KIND_INFER_REQUEST = "infer.request"
KIND_INFER_RESULT = "infer.result"

ALLOWED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


@dataclass(frozen=True)
class Header:
    name: str
    value: str


@dataclass(frozen=True)
class InferRequestInput:
    request_id: str
    method: str
    path: str
    body: Any
    ts_ms: int | None = None


@dataclass(frozen=True)
class InferRequest:
    request_id: str
    method: str
    path: str
    ts_ms: int
    body: Any
    prev_seq: int | None = None
    kind: str = KIND_INFER_REQUEST


@dataclass(frozen=True)
class InferResultInput:
    request_id: str
    prev_seq: int
    status_code: int
    body: Any
    ts_ms: int | None = None
    headers: list[Header] = field(default_factory=list)


@dataclass(frozen=True)
class InferResult:
    request_id: str
    prev_seq: int
    ts_ms: int
    status_code: int
    body: Any
    headers: list[Header] = field(default_factory=list)
    kind: str = KIND_INFER_RESULT


@dataclass(frozen=True)
class ParsedMessage:
    seq: int
    channel_id: int
    principal: int
    recipients: tuple[int, ...]
    payload: InferRequest | InferResult
