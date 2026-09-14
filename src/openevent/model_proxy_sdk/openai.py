"""Synchronous OpenAI-like calls over the llm.v1 protocol."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

from openevent.sdk import OpenEventClient

from .errors import ConfigurationError, ProtocolError, StreamCancelledError, _copy_error, make_api_error
from .models import InferCancelInput, InferRequestInput
from .publishing import create_client, publish_cancel, publish_request
from .rpc import RPCStopped
from .subscription import ProtocolFault, Subscription


class _ObjectView:
    """Read-only attribute access to a JSON object, including nested objects."""

    __slots__ = ("_value",)

    def __init__(self, value):
        object.__setattr__(self, "_value", value)

    def __setattr__(self, name, value):
        raise AttributeError("JSON views are read-only")

    def __getitem__(self, key):
        return _view(self._value[key])

    def __iter__(self):
        return iter(self._value)

    def __len__(self):
        return len(self._value)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name) from None


def _view(value):
    if isinstance(value, dict):
        return _ObjectView(value)
    if isinstance(value, list):
        return tuple(_view(item) for item in value)
    return value


class _JSONResult:
    __slots__ = ("_body", "_metadata")

    def __init__(self, body, **metadata):
        object.__setattr__(self, "_body", deepcopy(body))
        object.__setattr__(self, "_metadata", deepcopy(metadata))

    def __setattr__(self, name, value):
        raise AttributeError("Response objects are read-only")

    @property
    def body(self):
        return deepcopy(self._body)

    def model_dump(self):
        return deepcopy(self._body)

    def __getattr__(self, name):
        if isinstance(self._body, dict) and name in self._body:
            return _view(self._body[name])
        raise AttributeError(name)


class OpenAIResponse(_JSONResult):
    __slots__ = ()

    stream_id = property(lambda self: self._metadata["stream_id"])
    request_seq = property(lambda self: self._metadata["request_seq"])
    result_openevent_seq = property(lambda self: self._metadata["result_openevent_seq"])
    status_code = property(lambda self: self._metadata["status_code"])
    headers = property(lambda self: deepcopy(self._metadata["headers"]))


class OpenAIChunk(_JSONResult):
    __slots__ = ()

    append_openevent_seq = property(lambda self: self._metadata["append_openevent_seq"])


class OpenAIStream:
    __slots__ = ("_owner", "_call", "_done")

    def __init__(self, owner, call):
        self._owner = owner
        self._call = call
        self._done = False

    @property
    def stream_id(self):
        return self._call.stream_id

    @property
    def request_seq(self):
        return self._call.request_seq

    def _metadata(self, field):
        sub = self._owner._subscription
        with sub.condition:
            result, completed = self._call.result, self._call.completed
        if field == "result_openevent_seq":
            return None if result is None else result.seq
        if field == "response_status_code":
            return None if result is None else result.payload.status_code
        if field == "response_headers":
            return None if result is None else result.payload.headers
        if field == "terminal_has_body":
            return None if completed is None else completed.payload.has_body
        return None if completed is None or not completed.payload.has_body else completed.payload.body

    result_openevent_seq = property(lambda self: self._metadata("result_openevent_seq"))
    response_status_code = property(lambda self: self._metadata("response_status_code"))
    response_headers = property(lambda self: self._metadata("response_headers"))
    terminal_has_body = property(lambda self: self._metadata("terminal_has_body"))
    terminal_body = property(lambda self: self._metadata("terminal_body"))

    def __iter__(self):
        return self

    def __next__(self):
        if self._done:
            raise StopIteration
        try:
            while True:
                message = self._owner._subscription.next_message(self._call)
                if isinstance(message, ProtocolFault):
                    raise self._owner._protocol_error(self._call, message)
                payload = message.payload
                if payload.kind == "infer.append":
                    return OpenAIChunk(payload.body, append_openevent_seq=message.seq)
                if payload.kind == "infer.result" and not payload.has_body:
                    continue
                context = self._owner._context(self._call, last_seq=message.seq)
                if payload.kind == "infer.cancel":
                    raise StreamCancelledError("Stream cancelled", status_code=60004, **context)
                context["body"] = payload.body if payload.has_body else None
                error = make_api_error(
                    payload.status_code,
                    end_status=payload.end_status if payload.kind == "infer.end" else None,
                    **context,
                )
                if error is not None:
                    raise error
                self._done = True
                raise StopIteration
        except Exception:
            self._done = True
            raise

    def close(self):
        self._owner._close_call(self._call)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False


class _Endpoint:
    def __init__(self, owner, path):
        self._owner = owner
        self._path = path

    def create(self, **kwargs):
        return self._owner._create(self._path, kwargs)


class OpenAI:
    def __init__(self, *, openevent_addr=None, openevent_token=None,
                 openevent_channel_id=None, openevent_principal=None,
                 rpc_timeout_ms=30000, max_retries=3, retry_interval_ms=1000,
                 on_subscription_error=None):
        for name, value in (("openevent_addr", openevent_addr), ("openevent_token", openevent_token)):
            if not isinstance(value, str) or not value.strip():
                raise ConfigurationError(f"{name} must be a nonempty string")
        for name, value, minimum in (
            ("openevent_channel_id", openevent_channel_id, 1),
            ("openevent_principal", openevent_principal, 1),
            ("rpc_timeout_ms", rpc_timeout_ms, 1), ("max_retries", max_retries, 0),
            ("retry_interval_ms", retry_interval_ms, 1),
        ):
            if type(value) is not int or value < minimum:
                raise ConfigurationError(f"{name} must be an integer >= {minimum}")
        if on_subscription_error is not None and not callable(on_subscription_error):
            raise ConfigurationError("on_subscription_error must be callable or None")
        self._client = OpenEventClient(openevent_addr, timeout_ms=rpc_timeout_ms)
        self._protocol = create_client(self._client, openevent_token, max_retries, retry_interval_ms)
        self._channel_id = openevent_channel_id
        self._principal = openevent_principal
        self._subscription = Subscription(
            self._client, principal=openevent_principal, token=openevent_token,
            channel_id=openevent_channel_id, timeout_ms=rpc_timeout_ms,
            max_retries=max_retries, retry_interval_ms=retry_interval_ms,
            on_error=on_subscription_error,
        )
        self.chat = SimpleNamespace(completions=_Endpoint(self, "/v1/chat/completions"))
        self.responses = _Endpoint(self, "/v1/responses")

    def _create(self, path, kwargs):
        stream_id = kwargs.pop("stream_id", "stream_" + uuid4().hex)
        provider = kwargs.pop("provider", None)
        prev_seq = kwargs.pop("prev_seq", None)
        request = InferRequestInput(
            stream_id=stream_id,
            method="POST", path=path, body=kwargs,
            provider=provider, prev_seq=prev_seq,
        )
        streaming = request.body.get("stream", False)
        sub = self._subscription
        sub.prepare()
        call = sub.begin(request.stream_id, streaming)
        try:
            seq = publish_request(
                self._protocol, self._channel_id, self._principal, request,
                before_publish=lambda frozen: sub.register(call, frozen),
                before_rpc=sub.begin_publication_rpc, stop_event=sub.failed,
            )
            sub.published(call, seq)
        except RPCStopped:
            sub.publication_failed(call)
            raise _copy_error(sub.error)
        except BaseException:
            sub.publication_failed(call)
            raise
        if streaming:
            sub.check_stream_return(call)
            return OpenAIStream(self, call)
        message = sub.next_message(call)
        if isinstance(message, ProtocolFault):
            raise self._protocol_error(call, message)
        payload = message.payload
        error = make_api_error(payload.status_code, body=payload.body, **self._context(call))
        if error is not None:
            raise error
        return OpenAIResponse(
            payload.body, stream_id=call.stream_id, request_seq=call.request_seq,
            result_openevent_seq=message.seq, status_code=payload.status_code, headers=payload.headers,
        )

    def _context(self, call, *, last_seq=None):
        with self._subscription.condition:
            result = call.result
            stream_id, request_seq = call.stream_id, call.request_seq
        return dict(
            stream_id=stream_id, request_seq=request_seq,
            result_openevent_seq=None if result is None else result.seq,
            last_stream_openevent_seq=last_seq,
            headers=None if result is None else result.payload.headers,
        )

    def _protocol_error(self, call, fault):
        event = fault.event
        context = self._context(call, last_seq=event.seq if call.streaming else None)
        payload = event.payload
        if payload.kind == "infer.result":
            # A malformed result shape still has useful diagnostic metadata;
            # it is not accepted as the stream's public response metadata.
            context["result_openevent_seq"] = event.seq
            context["headers"] = payload.headers
            context["status_code"] = payload.status_code
        else:
            with self._subscription.condition:
                result = call.result
            context["status_code"] = None if result is None else result.payload.status_code
        context["body"] = getattr(payload, "body", None)
        return ProtocolError(fault.message, **context)

    def _close_call(self, call):
        sub = self._subscription
        with sub.condition:
            while call.close_started and not call.close_done:
                sub.condition.wait()
            if call.close_done:
                if call.close_error is not None:
                    raise _copy_error(call.close_error)
                return
            call.close_started = True
            sub.stop_call_locked(call)
            cancel = (not sub.failed.is_set() and call.published
                      and (call.receive is None or not call.receive.terminal))
        error = None
        try:
            if cancel:
                publish_cancel(
                    self._protocol, self._channel_id, self._principal,
                    InferCancelInput(stream_id=call.stream_id, request_seq=call.request_seq),
                    before_rpc=sub.begin_publication_rpc, stop_event=sub.failed,
                )
        except RPCStopped:
            # Failure before the first Publish makes this a purely local close.
            pass
        except Exception as exc:
            error = exc
        finally:
            with sub.condition:
                call.close_error = None if error is None else _copy_error(error)
                call.close_done = True
                sub.condition.notify_all()
        if error is not None:
            raise error

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False
