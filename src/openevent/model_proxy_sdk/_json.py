"""JSON bodies stay encoded until a caller needs their Python representation."""

from copy import deepcopy

import msgspec


class _Object(msgspec.Struct):
    pass


class _Request(msgspec.Struct):
    stream: bool = False


class _ResponseEvent(msgspec.Struct):
    type: str


def dump_bytes(value):
    """Encode an envelope, embedding Raw bodies without decoding them."""
    return msgspec.json.encode(value)


def raw_body(data):
    """Validate an HTTP body without building a tree for its contents."""
    data = bytes(data)
    # Raw/unknown fields are skipped by the parser; check their UTF-8 too.
    data.decode("utf-8")
    validate_body(msgspec.Raw(data))
    return msgspec.Raw(data)


def validate_body(body):
    msgspec.json.decode(body, type=_Object)


def request_stream(body):
    return msgspec.json.decode(body, type=_Request).stream


def event_type(body):
    return msgspec.json.decode(body, type=_ResponseEvent).type


def body_copy(body):
    return msgspec.json.decode(body) if isinstance(body, msgspec.Raw) else deepcopy(body)
