"""Bounded retries for ordinary OpenEvent RPCs."""

import time

from .errors import ConfigurationError


RETRYABLE_STATUSES = frozenset({"CANCELLED", "DEADLINE_EXCEEDED", "UNKNOWN",
                              "UNAVAILABLE", "INTERNAL"})


class RPCStopped(RuntimeError):
    """An internal owner stopped this operation before its next RPC attempt."""


def validate_retries(max_retries, retry_interval_ms):
    if type(max_retries) is not int or max_retries < 0:
        raise ConfigurationError("max_retries must be a non-negative integer")
    if type(retry_interval_ms) is not int or retry_interval_ms <= 0:
        raise ConfigurationError("retry_interval_ms must be a positive integer")


def status_name(exc):
    code = getattr(exc, "code", None)
    if not callable(code):
        return None
    try:
        status = code()
    except Exception:
        return None
    name = getattr(status, "name", None)
    if isinstance(name, str):
        return name
    if isinstance(status, str):
        return status.removeprefix("StatusCode.")
    return None


def is_retryable(exc):
    status = status_name(exc)
    return status is None or status in RETRYABLE_STATUSES


def call_rpc(operation, max_retries, retry_interval_ms, stop_event=None, before_rpc=None):
    validate_retries(max_retries, retry_interval_ms)
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            if stop_event is not None and stop_event.is_set():
                raise RPCStopped("RPC stopped")
            if before_rpc is not None:
                before_rpc()
        except RPCStopped:
            if last_error is not None:
                raise last_error
            raise
        try:
            return operation()
        except Exception as exc:
            if (attempt == max_retries or not is_retryable(exc)
                    or (stop_event is not None and stop_event.is_set())):
                raise
            last_error = exc
            delay = retry_interval_ms / 1000
            if stop_event is None:
                time.sleep(delay)
            elif stop_event.wait(delay):
                raise
