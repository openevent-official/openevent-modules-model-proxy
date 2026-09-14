"""One ordered subscription, bounded provider tasks, and independent control output."""

from collections import deque
from dataclasses import dataclass, field
import json
import logging
import threading
import time

from openevent.sdk import OpenEventClient
from openevent.model_proxy_sdk import (
    InferAppend, InferAppendInput, InferCancel, InferEnd, InferEndInput,
    InferRequest, InferResult, InferResultInput, UNSET, create_client, parse_message,
)
from openevent.model_proxy_sdk.errors import PayloadValidationError, ProtocolError, ResultPublishError
from openevent.model_proxy_sdk.publishing import _publish
from openevent.model_proxy_sdk.rpc import call_rpc, is_retryable

from .provider import ProviderCall, ProviderCancelled, ProviderError


LOG = logging.getLogger(__name__)
ERROR_CODES = {
    60000: "MODEL_API_TIMEOUT", 60001: "MODEL_API_DNS_ERROR",
    60002: "MODEL_API_TLS_ERROR", 60003: "MODEL_API_CONNECTION_ERROR",
    60005: "DUPLICATE_REQUEST", 60007: "MODEL_PROXY_INTERRUPTED",
    60008: "PAYLOAD_TOO_LARGE", 60009: "INVALID_REQUEST",
}


class WorkerFatalError(RuntimeError):
    pass


class _Stopped(Exception):
    pass


@dataclass
class RequestState:
    channel_id: int
    request_seq: int
    request_principal: int
    stream_id: str
    streaming: bool
    path: str
    body: dict | None = field(default=None, repr=False)
    received_at: float = field(default_factory=time.monotonic)
    duplicate: bool = False
    provider: str | None = None
    phase: str = "pending"
    chain_seq: int | None = None
    terminal_seq: int | None = None
    operation: object = None


class Worker:
    def __init__(self, config, *, client=None, provider_factory=ProviderCall, on_fatal=None):
        self.config = config
        self.client = client or OpenEventClient(config.open_event.addr,
                                                timeout_ms=config.open_event.rpc_timeout_ms)
        self.protocol = create_client(self.client, config.token, config.worker.max_retries,
                                      config.worker.retry_interval_ms)
        self.provider_factory = provider_factory
        self.on_fatal = on_fatal
        self.condition = threading.Condition()
        self.stopped = threading.Event()
        self.ready = threading.Event()
        self.requests = {}
        self.originals = {}
        self.pending = deque()
        self.controls = deque()
        self.running = 0
        self.last_seen_seq = 0
        self._stream = None
        self._failure = None

    def _rpc(self, operation):
        return call_rpc(operation, self.config.worker.max_retries,
                        self.config.worker.retry_interval_ms, self.stopped)

    def _log(self, event, state=None, **details):
        if state is not None:
            details.update(channel_id=state.channel_id, request_seq=state.request_seq,
                           stream_id=state.stream_id, provider=state.provider,
                           phase=state.phase, elapsed_ms=round((time.monotonic() - state.received_at) * 1000))
        LOG.info(json.dumps(dict(event=event, **details), ensure_ascii=True))

    def stop(self):
        with self.condition:
            self.stopped.set()
            stream = self._stream
            operations = []
            for state in self.requests.values():
                state.body = None
                if state.operation is not None:
                    operations.append(state.operation)
            self.condition.notify_all()
        if stream is not None:
            stream.cancel()
        for operation in operations:
            operation.abort()

    def _fail(self, exc):
        with self.condition:
            if self._failure is not None:
                return
            self._failure = exc
        self.stop()
        if self.on_fatal is not None:
            self.on_fatal(exc)

    def _validate_channels(self):
        for channel_id in self.config.channels:
            channel = self._rpc(lambda: self.client.get_channel(
                principal=self.config.principal, token=self.config.token,
                channel_id=channel_id)).channel
            if (channel.protocol != "llm.v1" or channel.visibility not in (1, 2)
                    or self.config.principal not in channel.members):
                raise WorkerFatalError(f"Invalid model Channel {channel_id}")
            try:
                description = json.loads(channel.description)
                valid = (
                    type(description) is dict
                    and description.keys() == {"version", "updated_at_ms", "metadata"}
                    and description["version"] == "v1"
                    and type(description["updated_at_ms"]) is int
                    and description["updated_at_ms"] >= 0
                    and type(description["metadata"]) is dict
                )
            except (ValueError, TypeError):
                valid = False
            if not valid:
                raise WorkerFatalError(f"Invalid llm.v1 description in Channel {channel_id}")

    def _parse(self, message):
        try:
            return parse_message(message)
        except Exception as exc:
            self._log("invalid_message", channel_id=message.channel_id, seq=message.seq,
                      error_type=type(exc).__name__, error_code=getattr(exc, "code", None))
            raise

    def _accept(self, parsed, *, historical=False, payload_bytes=0):
        payload = parsed.payload
        body = payload.body if isinstance(payload, InferRequest) else None
        abort = None
        with self.condition:
            if self.stopped.is_set():
                return
            if isinstance(payload, InferRequest):
                if parsed.seq in self.requests:
                    return
                key = (parsed.channel_id, payload.stream_id)
                state = RequestState(
                    parsed.channel_id, parsed.seq, parsed.principal, payload.stream_id,
                    body.get("stream", False), payload.path, duplicate=key in self.originals,
                )
                self.originals.setdefault(key, parsed.seq)
                self.requests[parsed.seq] = state
                if not historical:
                    state.provider = payload.provider or self.config.default_provider
                    code = (60005 if state.duplicate else
                            60008 if payload_bytes > self.config.max_payload_bytes else
                            60009 if state.provider not in self.config.providers else None)
                    if code is None:
                        state.body = body
                        self.pending.append(state)
                    else:
                        self.controls.append((state, code))
                    self.condition.notify_all()
                self._log("request_received", state, duplicate=state.duplicate)
                return
            request_seq = payload.prev_seq if isinstance(payload, InferResult) else payload.request_seq
            state = self.requests.get(request_seq)
            if (state is None or state.channel_id != parsed.channel_id
                    or state.stream_id != payload.stream_id or state.terminal_seq is not None):
                return
            terminal = False
            if isinstance(payload, InferResult):
                if state.chain_seq is not None:
                    return
                if not state.streaming and not payload.has_body:
                    raise ProtocolError("Ordinary request received a bodyless result")
                state.chain_seq = parsed.seq
                terminal = payload.has_body
            elif isinstance(payload, InferAppend):
                if not state.streaming or state.chain_seq is None or payload.prev_seq != state.chain_seq:
                    raise ProtocolError("Invalid append chain")
                state.chain_seq = parsed.seq
            elif isinstance(payload, InferEnd):
                if not state.streaming:
                    return
                terminal = True
            elif isinstance(payload, InferCancel):
                if not state.streaming:
                    return
                terminal = True
                abort = state.operation
            if terminal:
                state.terminal_seq = parsed.seq
                state.body = None
                self._log("terminal_received", state, seq=parsed.seq, kind=payload.kind)
                self.condition.notify_all()
        if abort is not None:
            abort.abort()

    def recover(self):
        self._validate_channels()
        target = self._rpc(lambda: self.client.get_status(
            principal=self.config.principal, token=self.config.token)).max_seq
        cursor = 1
        previous = 0
        while cursor <= target:
            page = self._rpc(lambda: self.client.fetch(
                principal=self.config.principal, token=self.config.token, from_seq=cursor,
                limit=1000, only_my_recipient=False, channels=self.config.channels))
            if page.next_seq <= cursor:
                raise WorkerFatalError("History scan did not advance")
            for message in page.messages:
                if message.seq > target:
                    break
                if message.seq < cursor or message.seq <= previous:
                    raise WorkerFatalError("History is not ordered")
                previous = message.seq
                self._accept(self._parse(message), historical=True)
            cursor = page.next_seq
            # The scan retains only request state, not completed Fetch pages.
            message = None
            del page
        # Parse the entire snapshot before publishing any recovery result.
        for state in sorted(self.requests.values(), key=lambda s: s.request_seq):
            if state.terminal_seq is None:
                self._error_output(state, 60007, recovery=True)
                state.phase = "done"
        self.last_seen_seq = target

    def _can_output(self, state):
        return not self.stopped.is_set() and state.terminal_seq is None

    def _emit(self, state, event):
        with self.condition:
            if not self._can_output(state):
                raise _Stopped()

        def before_publish(frozen):
            with self.condition:
                if not self._can_output(state):
                    raise _Stopped()

        def validate_payload(payload):
            if len(payload) > self.config.max_payload_bytes:
                raise ProviderError(60008, "Encoded output exceeds max_payload_bytes")

        seq = _publish(self.protocol, state.channel_id, self.config.principal,
                       event, type(event), request_principal=state.request_principal,
                       before_publish=before_publish, validate_payload=validate_payload)
        self._log("output_published", state, seq=seq, kind=event.KIND)
        return seq

    def _error_output(self, state, code, *, recovery=False, rejection=False):
        body = {"error": {"code": ERROR_CODES[code], "message": ERROR_CODES[code],
                          "type": "model_proxy_error"}}
        if state.streaming and not rejection and not (recovery and state.duplicate):
            event = InferEndInput(stream_id=state.stream_id, request_seq=state.request_seq,
                                  status_code=code, end_status="interrupted", body=body)
        else:
            event = InferResultInput(stream_id=state.stream_id, prev_seq=state.request_seq,
                                     status_code=code, body=body)
        return self._emit(state, event)

    def _provider_task(self, state):
        operation = None
        try:
            with self.condition:
                if not self._can_output(state):
                    return
                body, state.body = state.body, None
            operation = self.provider_factory(
                self.config.providers[state.provider], body, state.path,
                self.config.max_payload_bytes)
            body = None
            with self.condition:
                if not self._can_output(state):
                    return
                state.operation = operation
            # abort() also works before the first socket is installed.
            with self.condition:
                if not self._can_output(state):
                    operation.abort()
                    return
            writer_seq = None
            for output in operation:
                with self.condition:
                    if not self._can_output(state):
                        break
                common = dict(stream_id=state.stream_id)
                if output.kind in ("headers", "result"):
                    event = InferResultInput(
                        **common, prev_seq=state.request_seq, status_code=output.status_code,
                        headers=output.headers or None,
                        body=output.body if output.has_body else UNSET)
                elif output.kind == "append":
                    if writer_seq is None:
                        raise WorkerFatalError("Provider yielded append before response headers")
                    event = InferAppendInput(**common, request_seq=state.request_seq,
                                             prev_seq=writer_seq, body=output.body)
                elif output.kind in ("completed", "failed"):
                    event = InferEndInput(
                        **common, request_seq=state.request_seq, status_code=output.status_code,
                        end_status=output.kind, body=output.body if output.has_body else UNSET)
                else:
                    raise WorkerFatalError("Unknown Provider event")
                writer_seq = self._emit(state, event)
        except (ProviderCancelled, _Stopped):
            pass
        except ProviderError as exc:
            try:
                self._error_output(state, exc.status_code)
            except _Stopped:
                pass
            except Exception as error:
                self._fail(error)
        except ResultPublishError as exc:
            self._fail(exc)
        except Exception as exc:
            self._log("provider_task_error", state, error_type=type(exc).__name__)
            try:
                self._error_output(state, 60007)
            except _Stopped:
                pass
            except Exception as error:
                self._fail(error)
        finally:
            if operation is not None:
                operation.abort()
            with self.condition:
                state.operation = None
                state.body = None
                state.phase = "done"
                self.running -= 1
                self._log("provider_finished", state)
                self.condition.notify_all()

    def _schedule(self):
        try:
            with self.condition:
                while not self.stopped.is_set():
                    self.condition.wait_for(lambda: self.stopped.is_set() or
                                            (self.pending and self.running < self.config.worker.max_concurrency))
                    if self.stopped.is_set():
                        return
                    state = self.pending.popleft()
                    if state.terminal_seq is not None:
                        state.phase = "done"
                        continue
                    state.phase = "running"
                    self.running += 1
                    self._log("provider_started", state)
                    threading.Thread(target=self._provider_task, args=(state,),
                                     name="model-proxy-provider", daemon=True).start()
        except Exception as exc:
            self._fail(exc)

    def _control_outputs(self):
        try:
            while not self.stopped.is_set():
                with self.condition:
                    self.condition.wait_for(lambda: self.stopped.is_set() or self.controls)
                    if self.stopped.is_set():
                        return
                    state, code = self.controls.popleft()
                try:
                    self._error_output(state, code, rejection=True)
                except _Stopped:
                    pass
                state.phase = "done"
        except Exception as exc:
            self._fail(exc)

    @staticmethod
    def _finish_stream(stream):
        stream.cancel()
        stream.code()

    def _connect(self):
        stream = self.client.subscribe(
            principal=self.config.principal, token=self.config.token,
            from_seq=self.last_seen_seq, channels=self.config.channels, only_my_recipient=False)
        with self.condition:
            stopped = self.stopped.is_set()
            if not stopped:
                self._stream = stream
        if stopped:
            self._finish_stream(stream)
            raise _Stopped()
        try:
            stream.wait_started(self.config.open_event.rpc_timeout_ms)
            return stream
        except Exception as exc:
            if is_retryable(exc):
                self._finish_stream(stream)
            else:
                stream.cancel()
            with self.condition:
                self._stream = None
            raise

    def run(self):
        try:
            self.recover()
            for name, target in (("scheduler", self._schedule), ("control", self._control_outputs)):
                threading.Thread(target=target, name=f"model-proxy-{name}", daemon=True).start()
            while not self.stopped.is_set():
                stream = self._rpc(self._connect)
                self.ready.set()
                self._log("subscription_ready", from_seq=self.last_seen_seq)
                try:
                    for response in stream:
                        if self.stopped.is_set():
                            break
                        message = response.message
                        if message.seq <= self.last_seen_seq:
                            del message, response
                            continue
                        parsed = self._parse(message)
                        self._accept(parsed, payload_bytes=len(message.payload))
                        self.last_seen_seq = message.seq
                        del parsed, message, response
                    if not self.stopped.is_set():
                        raise ConnectionError("Subscribe ended without a terminal status")
                except Exception as exc:
                    if self.stopped.is_set():
                        break
                    # Protocol errors are not transport failures and cannot be retried.
                    if isinstance(exc, (PayloadValidationError, ProtocolError)) or not is_retryable(exc):
                        self._fail(exc)
                        break
                    self._log("subscription_disconnected", error_type=type(exc).__name__)
                finally:
                    if self._failure is None:
                        self._finish_stream(stream)
                    else:
                        stream.cancel()
                    with self.condition:
                        self._stream = None
        except Exception as exc:
            if not self.stopped.is_set():
                self._fail(exc)
        finally:
            self.stop()
            if self._failure is None:
                self.client.close()
        if self._failure is not None:
            raise WorkerFatalError(str(self._failure)) from self._failure
