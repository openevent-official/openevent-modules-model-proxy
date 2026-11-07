"""Single-message llm.v1 models and strict JSON validation."""

from copy import deepcopy
from dataclasses import dataclass
import math
import re

import msgspec

from . import _json
from .errors import PayloadValidationError


class _Unset:
    __slots__ = ()

    def __repr__(self):
        return "UNSET"

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self


UNSET = _Unset()

_COMMON = {"kind", "stream_id", "ts_ms"}
_REQUIRED = {
    "infer.request": _COMMON | {"method", "path", "body"},
    "infer.result": _COMMON | {"prev_seq", "status_code"},
    "infer.append": _COMMON | {"request_seq", "prev_seq", "body"},
    "infer.end": _COMMON | {"request_seq", "status_code", "end_status"},
    "infer.cancel": _COMMON | {"request_seq"},
}
_OPTIONAL = {
    "infer.request": {"provider", "prev_seq"},
    "infer.result": {"headers", "body"},
    "infer.append": set(),
    "infer.end": {"body"},
    "infer.cancel": set(),
}
_STREAM_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)
_RESULT_CODES = {60000, 60001, 60002, 60003, 60005, 60007, 60008, 60009}
_INTERRUPTED_CODES = {60000, 60001, 60002, 60003, 60007, 60008}


def positive_int(value):
    return type(value) is int and value > 0


def _valid_stream_id(value):
    return isinstance(value, str) and _STREAM_ID.fullmatch(value) is not None


def _valid_json(value, active=None):
    if value is None or type(value) in (str, bool, int):
        return True
    if type(value) is float:
        return math.isfinite(value)
    if type(value) not in (dict, list):
        return False
    active = set() if active is None else active
    if id(value) in active:
        return False
    active.add(id(value))
    try:
        if type(value) is dict:
            return all(type(key) is str and _valid_json(item, active)
                       for key, item in value.items())
        return all(_valid_json(item, active) for item in value)
    finally:
        active.remove(id(value))


def _validate(data):
    """Validate one message and return its request mode, if applicable."""
    if type(data) is not dict:
        raise PayloadValidationError("INVALID_PAYLOAD", "Payload must be a JSON object")
    kind = data.get("kind")
    valid_kind = kind if isinstance(kind, str) and kind in _REQUIRED else None
    stream_id = data.get("stream_id")
    valid_stream_id = stream_id if _valid_stream_id(stream_id) else None

    def fail(code, message):
        raise PayloadValidationError(code, message, kind=valid_kind, stream_id=valid_stream_id)

    if valid_kind is None:
        fail("INVALID_KIND", "Unsupported or missing kind")
    required, optional = _REQUIRED[kind], _OPTIONAL[kind]
    unknown = data.keys() - required - optional
    if unknown:
        fail("UNKNOWN_FIELD", f"Unexpected fields for {kind}: {sorted(unknown)}")
    missing = required - data.keys()
    if missing:
        fail("MISSING_REQUIRED_FIELD", f"Missing fields for {kind}: {sorted(missing)}")
    if valid_stream_id is None:
        fail("INVALID_STREAM_ID", "stream_id must use 1..128 ASCII letters, digits or ._:-")
    if type(data["ts_ms"]) is not int or data["ts_ms"] < 0:
        fail("INVALID_TS_MS", "ts_ms must be a non-negative integer")
    for field in ("prev_seq", "request_seq"):
        if field in data and not positive_int(data[field]):
            fail(f"INVALID_{field.upper()}", f"{field} must be a positive integer")
    if "provider" in data and (not isinstance(data["provider"], str) or not data["provider"]):
        fail("INVALID_PROVIDER", "provider must be a non-empty string")
    if "method" in data and data["method"] != "POST":
        fail("INVALID_METHOD", "method must be POST")
    if "path" in data and data["path"] not in ("/v1/chat/completions", "/v1/responses"):
        fail("INVALID_PATH", "Unsupported request path")
    streaming = None
    if "body" in data:
        body = data["body"]
        try:
            if isinstance(body, msgspec.Raw):
                if kind == "infer.request":
                    streaming = _json.request_stream(body)
                else:
                    _json.validate_body(body)
            elif type(body) is not dict or not _valid_json(body):
                fail("INVALID_BODY", "body must be a JSON object")
            elif kind == "infer.request":
                streaming = body.get("stream", False)
                if type(streaming) is not bool:
                    fail("INVALID_BODY", "request stream must be boolean")
        except (msgspec.DecodeError, RecursionError) as exc:
            fail("INVALID_BODY", str(exc))
    if "headers" in data:
        headers = data["headers"]
        if type(headers) is not list or not headers:
            fail("INVALID_HEADERS", "headers must be a non-empty array")
        for header in headers:
            if (type(header) is not dict or header.keys() != {"name", "value"}
                    or type(header["name"]) is not str or type(header["value"]) is not str):
                fail("INVALID_HEADERS", "Each header must contain string name and value")
            name = header["name"]
            if (not name.isascii() or name != name.lower()
                    or not (name in {"content-type", "retry-after", "x-request-id"}
                            or name.startswith("x-ratelimit-"))):
                fail("INVALID_HEADERS", "Header name is not an allowed lowercase name")
    if kind == "infer.end":
        if data["end_status"] not in ("completed", "failed", "interrupted"):
            fail("INVALID_END_STATUS", "Unsupported end_status")
        if data["end_status"] != "completed" and "body" not in data:
            fail("MISSING_REQUIRED_FIELD", "failed and interrupted end require body")
    if "status_code" in data:
        status = data["status_code"]
        http = type(status) is int and 100 <= status <= 599
        valid = http
        if kind == "infer.result" and "body" in data:
            valid = http or (type(status) is int and status in _RESULT_CODES)
        if kind == "infer.end" and data["end_status"] == "interrupted":
            valid = type(status) is int and status in _INTERRUPTED_CODES
        if not valid:
            fail("INVALID_STATUS_CODE", "status_code is not allowed for this message shape")
    return streaming


class _Model:
    __slots__ = ("_data", "_streaming")
    KIND = None

    def __init__(self, data):
        streaming = _validate(data)
        if data["kind"] != self.KIND:
            raise PayloadValidationError("INVALID_KIND", "Wrong model kind")
        object.__setattr__(self, "_data", deepcopy(data))
        object.__setattr__(self, "_streaming", streaming)

    def __setattr__(self, name, value):
        raise AttributeError(f"{type(self).__name__} is read-only")

    def __getattr__(self, name):
        if name == "has_body" and self.KIND in ("infer.result", "infer.end"):
            return "body" in self._data
        if name in _REQUIRED[self.KIND] | _OPTIONAL[self.KIND]:
            if name == "body":
                return _json.body_copy(self._data.get(name))
            return deepcopy(self._data.get(name))
        raise AttributeError(name)

    def to_dict(self):
        return {key: _json.body_copy(value) if key == "body" else deepcopy(value)
                for key, value in self._data.items()}

    def __repr__(self):
        return f"{type(self).__name__}({self._data!r})"


class InferRequest(_Model):
    __slots__ = ()
    KIND = "infer.request"


class InferResult(_Model):
    __slots__ = ()
    KIND = "infer.result"


class InferAppend(_Model):
    __slots__ = ()
    KIND = "infer.append"


class InferEnd(_Model):
    __slots__ = ()
    KIND = "infer.end"


class InferCancel(_Model):
    __slots__ = ()
    KIND = "infer.cancel"


class _Input(_Model):
    __slots__ = ()

    def _initialize(self, fields):
        data = {"kind": self.KIND, "ts_ms": 0, **fields}
        _Model.__init__(self, data)


class InferRequestInput(_Input):
    __slots__ = ()
    KIND = "infer.request"

    def __init__(self, *, stream_id, method, path, body, provider=None, prev_seq=None):
        fields = dict(stream_id=stream_id, method=method, path=path, body=body)
        if provider is not None:
            fields["provider"] = provider
        if prev_seq is not None:
            fields["prev_seq"] = prev_seq
        self._initialize(fields)


class InferResultInput(_Input):
    __slots__ = ()
    KIND = "infer.result"

    def __init__(self, *, stream_id, prev_seq, status_code, headers=None, body=UNSET):
        fields = dict(stream_id=stream_id, prev_seq=prev_seq, status_code=status_code)
        if headers is not None:
            fields["headers"] = headers
        if body is not UNSET:
            fields["body"] = body
        self._initialize(fields)


class InferAppendInput(_Input):
    __slots__ = ()
    KIND = "infer.append"

    def __init__(self, *, stream_id, request_seq, prev_seq, body):
        self._initialize(dict(stream_id=stream_id, request_seq=request_seq, prev_seq=prev_seq, body=body))


class InferEndInput(_Input):
    __slots__ = ()
    KIND = "infer.end"

    def __init__(self, *, stream_id, request_seq, status_code, end_status, body=UNSET):
        fields = dict(stream_id=stream_id, request_seq=request_seq,
                      status_code=status_code, end_status=end_status)
        if body is not UNSET:
            fields["body"] = body
        self._initialize(fields)


class InferCancelInput(_Input):
    __slots__ = ()
    KIND = "infer.cancel"

    def __init__(self, *, stream_id, request_seq):
        self._initialize(dict(stream_id=stream_id, request_seq=request_seq))


def parse_payload(payload):
    try:
        if not isinstance(payload, bytes):
            raise ValueError("payload must be bytes")
        payload.decode("utf-8")
        fields = msgspec.json.decode(payload, type=dict[str, msgspec.Raw])
        data = {key: value.copy() if key == "body" else msgspec.json.decode(value)
                for key, value in fields.items()}
    except msgspec.ValidationError as exc:
        raise PayloadValidationError("INVALID_PAYLOAD", "Payload must be a JSON object") from exc
    except (msgspec.DecodeError, UnicodeError, ValueError, TypeError, RecursionError) as exc:
        raise PayloadValidationError("INVALID_JSON", "payload must be valid UTF-8 JSON") from exc
    models = {"infer.request": InferRequest, "infer.result": InferResult,
              "infer.append": InferAppend, "infer.end": InferEnd, "infer.cancel": InferCancel}
    streaming = _validate(data)
    # Only the envelope is decoded. The body owns its independent byte slice.
    model = object.__new__(models[data["kind"]])
    object.__setattr__(model, "_data", data)
    object.__setattr__(model, "_streaming", streaming)
    return model


@dataclass(frozen=True)
class ParsedMessage:
    payload: _Model
    uuid: int
    seq: int
    channel_id: int
    principal: int
    recipients: tuple
    object_keys: tuple
    ts_ms: int


def parse_message(message):
    return ParsedMessage(payload=parse_payload(message.payload), uuid=message.uuid,
                         seq=message.seq, channel_id=message.channel_id,
                         principal=message.principal, recipients=tuple(message.recipients),
                         object_keys=tuple(deepcopy(list(message.object_keys))), ts_ms=message.ts_ms)
