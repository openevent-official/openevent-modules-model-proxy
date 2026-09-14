"""Public errors; subscription failures deliberately carry no call context."""

from copy import deepcopy
from enum import Enum


class ConfigurationError(ValueError):
    pass


def _field(name, *, copy=False):
    def get(self):
        value = self._values[name]
        return deepcopy(value) if copy else value
    return property(get)


class PayloadValidationError(ValueError):
    code = _field("code")
    kind = _field("kind")
    stream_id = _field("stream_id")

    def __init__(self, code, message="", *, kind=None, stream_id=None):
        super().__init__(message or code)
        self._values = dict(code=code, kind=kind, stream_id=stream_id)


class CommitState(Enum):
    NOT_COMMITTED = "NOT_COMMITTED"
    COMMITTED = "COMMITTED"
    UNKNOWN = "UNKNOWN"


class ResultPublishError(Exception):
    commit_state = _field("commit_state")
    event_uuid = _field("event_uuid")
    committed_seq = _field("committed_seq")
    last_status = _field("last_status")

    def __init__(self, message="", *, commit_state, event_uuid=None,
                 committed_seq=None, last_status=None):
        super().__init__(message or f"Event publication failed: {commit_state.value}")
        self._values = dict(commit_state=commit_state, event_uuid=event_uuid,
                            committed_seq=committed_seq, last_status=last_status)


class APIError(Exception):
    stream_id = _field("stream_id")
    request_seq = _field("request_seq")
    result_openevent_seq = _field("result_openevent_seq")
    last_stream_openevent_seq = _field("last_stream_openevent_seq")
    status_code = _field("status_code")
    headers = _field("headers", copy=True)
    body = _field("body", copy=True)
    end_status = _field("end_status")

    def __init__(self, message="", **context):
        fields = ("stream_id", "request_seq", "result_openevent_seq",
                  "last_stream_openevent_seq", "status_code", "headers", "body", "end_status")
        unknown = context.keys() - fields
        if unknown:
            raise TypeError(f"Unknown error context: {sorted(unknown)}")
        super().__init__(message or type(self).__name__)
        self._values = {name: deepcopy(context.get(name)) for name in fields}


class APIStatusError(APIError):
    pass


class BadRequestError(APIStatusError):
    pass


class AuthenticationError(APIStatusError):
    pass


class PermissionDeniedError(APIStatusError):
    pass


class NotFoundError(APIStatusError):
    pass


class ConflictError(APIStatusError):
    pass


class UnprocessableEntityError(APIStatusError):
    pass


class RateLimitError(APIStatusError):
    pass


class InternalServerError(APIStatusError):
    pass


class APITimeoutError(APIError):
    pass


class APIConnectionError(APIError):
    pass


class StreamCancelledError(APIError):
    pass


class ProtocolError(APIError):
    pass


class OpenEventSubscriptionError(Exception):
    reason = _field("reason")
    last_status = _field("last_status")
    protocol_error = _field("protocol_error")

    def __init__(self, message="", *, reason, last_status=None, protocol_error=None):
        super().__init__(message or f"OpenEvent subscription failed: {reason}")
        self._values = dict(reason=reason, last_status=last_status, protocol_error=protocol_error)


def _copy_error(error):
    """Copy error diagnostics without retaining traceback or exception chains."""
    values = getattr(error, "_values", {}).copy()
    if values.get("protocol_error") is not None:
        values["protocol_error"] = _copy_error(values["protocol_error"])
    if isinstance(error, PayloadValidationError):
        return type(error)(values.pop("code"), *error.args, **values)
    return type(error)(*error.args, **values)


def make_api_error(status_code, *, end_status=None, **context):
    """Map an accepted protocol outcome; None means a successful HTTP outcome."""
    if end_status == "failed":
        error_type = APIError
    elif status_code == 60000:
        error_type = APITimeoutError
    elif status_code in (60001, 60002, 60003):
        error_type = APIConnectionError
    elif status_code == 60004:
        error_type = StreamCancelledError
    elif status_code == 60009:
        error_type = BadRequestError
    elif status_code in (60005, 60007, 60008):
        error_type = APIError
    elif 200 <= status_code <= 299:
        return None
    elif 500 <= status_code <= 599:
        error_type = InternalServerError
    else:
        error_type = {400: BadRequestError, 401: AuthenticationError,
                      403: PermissionDeniedError, 404: NotFoundError,
                      409: ConflictError, 422: UnprocessableEntityError,
                      429: RateLimitError}.get(status_code, APIStatusError)
    return error_type(status_code=status_code, end_status=end_status, **context)
