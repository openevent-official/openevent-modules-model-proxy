"""Publish one frozen event, reconciling its UUID only after ALREADY_EXISTS."""

from dataclasses import dataclass
import time

from .errors import CommitState, ConfigurationError, PayloadValidationError, ResultPublishError
from .models import (InferAppendInput, InferCancelInput, InferEndInput,
                     InferRequestInput, InferResultInput, positive_int)
from .rpc import RPCStopped, call_rpc, status_name, validate_retries


@dataclass(frozen=True)
class ModelProxyProtocolClient:
    openevent_client: object
    token: str
    max_retries: int = 3
    retry_interval_ms: int = 1000

    def __post_init__(self):
        if not isinstance(self.token, str) or not self.token:
            raise ConfigurationError("token must be a non-empty string")
        if not all(callable(getattr(self.openevent_client, name, None))
                   for name in ("get_uuid", "publish_auto_seq", "get_seq_by_uuid")):
            raise ConfigurationError("openevent_client must provide the OpenEvent publishing API")
        validate_retries(self.max_retries, self.retry_interval_ms)


def create_client(openevent_client, token, max_retries=3, retry_interval_ms=1000):
    return ModelProxyProtocolClient(openevent_client, token, max_retries, retry_interval_ms)


@dataclass(frozen=True)
class FrozenPublication:
    uuid: int
    payload: bytes
    channel_id: int
    principal: int
    recipients: tuple
    object_keys: tuple = ()


_NOT_COMMITTED = frozenset({"UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND",
                            "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED"})


def _positive_reply(value, field):
    if not positive_int(value):
        raise RuntimeError(f"OpenEvent returned an invalid {field}")
    return value


def _publish(client, channel_id, principal, event, expected_type, *,
             request_principal=None, before_publish=None, validate_payload=None,
             before_rpc=None, stop_event=None):
    if not isinstance(event, expected_type):
        raise PayloadValidationError("INVALID_PUBLISH_ARGUMENT",
                                     f"Expected {expected_type.__name__}")
    if (not positive_int(channel_id) or not positive_int(principal)
            or (request_principal is not None and not positive_int(request_principal))):
        raise PayloadValidationError("INVALID_PUBLISH_ARGUMENT",
                                     "Channel and principals must be positive integers",
                                     kind=event.KIND, stream_id=event.stream_id)
    if not isinstance(client, ModelProxyProtocolClient):
        raise PayloadValidationError("INVALID_PUBLISH_ARGUMENT", "Invalid protocol client")
    payload = event.to_payload(time.time_ns() // 1_000_000)
    if validate_payload is not None:
        validate_payload(payload)
    recipients = () if request_principal is None else (request_principal,)
    transport = client.openevent_client
    try:
        event_uuid = call_rpc(lambda: _positive_reply(transport.get_uuid(), "UUID"),
                              client.max_retries, client.retry_interval_ms,
                              stop_event=stop_event, before_rpc=before_rpc)
    except RPCStopped:
        raise
    except Exception as exc:
        raise ResultPublishError(commit_state=CommitState.NOT_COMMITTED,
                                 last_status=status_name(exc)) from exc
    frozen = FrozenPublication(event_uuid, payload, channel_id, principal, recipients)
    if before_publish is not None:
        before_publish(frozen)
    uncertain = False
    last_error = None
    for attempt in range(client.max_retries + 1):
        try:
            if stop_event is not None and stop_event.is_set():
                raise RPCStopped("Publication stopped")
            if before_rpc is not None:
                before_rpc()
        except RPCStopped:
            if last_error is not None:
                raise ResultPublishError(commit_state=CommitState.UNKNOWN, event_uuid=frozen.uuid,
                                         last_status=status_name(last_error)) from last_error
            raise
        try:
            response = transport.publish_auto_seq(
                principal=frozen.principal, token=client.token, channel_id=frozen.channel_id,
                payload=frozen.payload, uuid=frozen.uuid, recipients=frozen.recipients,
                object_keys=frozen.object_keys,
            )
            return _positive_reply(response.seq, "seq")
        except Exception as exc:
            status = status_name(exc)
            if status == "ALREADY_EXISTS":
                try:
                    return call_rpc(lambda: _positive_reply(transport.get_seq_by_uuid(frozen.uuid), "seq"),
                                    client.max_retries, client.retry_interval_ms,
                                    stop_event=stop_event, before_rpc=before_rpc)
                except RPCStopped:
                    raise ResultPublishError(commit_state=CommitState.COMMITTED,
                                             event_uuid=frozen.uuid, last_status=status) from exc
                except Exception as query_error:
                    raise ResultPublishError(commit_state=CommitState.COMMITTED,
                                             event_uuid=frozen.uuid,
                                             last_status=status_name(query_error)) from query_error
            if status in _NOT_COMMITTED:
                state = CommitState.UNKNOWN if uncertain else CommitState.NOT_COMMITTED
                raise ResultPublishError(commit_state=state, event_uuid=frozen.uuid,
                                         last_status=status) from exc
            uncertain = True
            if attempt == client.max_retries or (stop_event is not None and stop_event.is_set()):
                raise ResultPublishError(commit_state=CommitState.UNKNOWN, event_uuid=frozen.uuid,
                                         last_status=status) from exc
            last_error = exc
            delay = client.retry_interval_ms / 1000
            if stop_event is None:
                time.sleep(delay)
            elif stop_event.wait(delay):
                raise ResultPublishError(commit_state=CommitState.UNKNOWN, event_uuid=frozen.uuid,
                                         last_status=status) from exc


def publish_request(client, channel_id, principal, req, *, before_publish=None,
                    before_rpc=None, stop_event=None):
    """Internal request handoff; the hook runs after UUID allocation, before Publish."""
    return _publish(client, channel_id, principal, req, InferRequestInput,
                    before_publish=before_publish, before_rpc=before_rpc, stop_event=stop_event)


def publish_infer_request(client, channel_id, principal, req):
    return publish_request(client, channel_id, principal, req)


def _publish_output(client, channel_id, principal, request_principal, event, expected_type):
    if not positive_int(request_principal):
        raise PayloadValidationError("INVALID_PUBLISH_ARGUMENT", "request_principal must be a positive integer")
    return _publish(client, channel_id, principal, event, expected_type,
                    request_principal=request_principal)


def publish_infer_result(client, channel_id, principal, request_principal, result):
    return _publish_output(client, channel_id, principal, request_principal, result, InferResultInput)


def publish_infer_append(client, channel_id, principal, request_principal, event):
    return _publish_output(client, channel_id, principal, request_principal, event, InferAppendInput)


def publish_infer_end(client, channel_id, principal, request_principal, event):
    return _publish_output(client, channel_id, principal, request_principal, event, InferEndInput)


def publish_infer_cancel(client, channel_id, principal, cancel):
    return _publish(client, channel_id, principal, cancel, InferCancelInput)


def publish_cancel(client, channel_id, principal, cancel, *, before_rpc, stop_event):
    """Internal cancel publication, stopped by a final subscription failure."""
    return _publish(client, channel_id, principal, cancel, InferCancelInput,
                    before_rpc=before_rpc, stop_event=stop_event)
