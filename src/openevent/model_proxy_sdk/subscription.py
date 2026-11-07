"""One reader and one lock for a client's subscribed calls.

Publishing is deliberately outside this module: the caller registers its request
UUID before publishing, and later supplies the publisher's final conclusion.
"""

from collections import deque
from dataclasses import dataclass, field
import logging
import threading

from .errors import OpenEventSubscriptionError, _copy_error
from .models import parse_message
from .rpc import RPCStopped, call_rpc, is_retryable, status_name


_LOG = logging.getLogger(__name__)


@dataclass
class ReceiveState:
    request_seq: int
    chain_seq: int
    head_seen: bool = False
    terminal: bool = False


@dataclass(frozen=True)
class ProtocolFault:
    message: str
    event: object


@dataclass(eq=False)
class Call:
    stream_id: str
    streaming: bool
    pending_uuid: int | None = None
    request_seq: int | None = None
    receive: ReceiveState | None = None
    queue: deque = field(default_factory=deque)
    error: Exception | None = None
    local_closed: bool = False
    result: object = None
    completed: object = None
    close_started: bool = False
    close_done: bool = False
    close_error: Exception | None = None


class Subscription:
    def __init__(self, client, *, principal, token, channel_id, timeout_ms,
                 max_retries, retry_interval_ms, on_error=None):
        self.client = client
        self.principal = principal
        self.token = token
        self.channel_id = channel_id
        self.timeout_ms = timeout_ms
        self.max_retries = max_retries
        self.retry_interval_ms = retry_interval_ms
        self.on_error = on_error
        self.condition = threading.Condition()
        self.failed = threading.Event()
        self.initial_from_seq = None
        self.last_seen_seq = None
        self.preparing = False
        self.reader = None
        self.active_subscribe = None
        self.calls_by_uuid = {}
        self.receives_by_seq = {}
        self.error = None

    def _check_locked(self):
        if self.error is not None:
            raise _copy_error(self.error)

    def prepare(self):
        with self.condition:
            while True:
                self._check_locked()
                if self.initial_from_seq is not None:
                    return
                if not self.preparing:
                    self.preparing = True
                    break
                self.condition.wait()
        failure = None
        try:
            status = call_rpc(
                lambda: self.client.get_status(self.principal, self.token),
                max_retries=self.max_retries,
                retry_interval_ms=self.retry_interval_ms,
                stop_event=self.failed,
            )
        except Exception as exc:
            with self.condition:
                error = OpenEventSubscriptionError(
                    "Unable to prepare subscription", reason="rpc",
                    last_status=status_name(exc),
                )
                failure = self._fail_locked(error)
        else:
            with self.condition:
                reader = threading.Thread(
                    target=self._read, name="model-proxy-subscription", daemon=True,
                )
                reader.start()
                # The reader needs this lock before it can connect. Commit the
                # replay anchor only after its thread has started successfully.
                self.reader = reader
                self.initial_from_seq = status.max_seq
                self.last_seen_seq = status.max_seq
        finally:
            with self.condition:
                self.preparing = False
                self.condition.notify_all()
        self._finish_failure(failure)
        with self.condition:
            self._check_locked()

    def begin(self, stream_id, streaming):
        with self.condition:
            self._check_locked()
            return Call(stream_id, streaming)

    def begin_publication_rpc(self):
        with self.condition:
            if self.failed.is_set():
                raise RPCStopped("Subscription has finally failed")
            # This lock boundary starts one RPC attempt. The RPC itself runs
            # outside the lock and may finish after failure has been recorded.

    def register(self, call, uuid: int):
        with self.condition:
            self._check_locked()
            call.pending_uuid = uuid
            self.calls_by_uuid[uuid] = call

    def published(self, call, seq):
        with self.condition:
            call.request_seq = seq
            self.condition.notify_all()

    def publication_failed(self, call):
        with self.condition:
            self._detach_locked(call)
            call.local_closed = True
            call.queue.clear()
            call.result = call.completed = call.receive = call.error = None
            self.condition.notify_all()

    def _detach_locked(self, call):
        if call.pending_uuid is not None:
            self.calls_by_uuid.pop(call.pending_uuid, None)
            call.pending_uuid = None
        if call.receive is not None:
            self.receives_by_seq.pop(call.receive.request_seq, None)
        # Keep the accepted terminal state available to stream.close().

    def stop_call_locked(self, call):
        call.local_closed = True
        self._detach_locked(call)
        self.condition.notify_all()

    def _fail_locked(self, error):
        if self.error is not None:
            return None
        error = _copy_error(error)
        self.error = error
        self.failed.set()
        for calls in (self.calls_by_uuid, self.receives_by_seq):
            for call in calls.values():
                call.error = error
                call.pending_uuid = None
            calls.clear()
        self.condition.notify_all()
        return error, self.active_subscribe

    @staticmethod
    def _cancel(stream):
        if stream is not None:
            try:
                stream.cancel()
                # gRPC code() waits until cancellation/termination is complete.
                stream.code()
            except Exception:
                _LOG.debug("Subscribe cleanup failed", exc_info=True)

    def _finish_failure(self, failure):
        if failure is None:
            return
        error, stream = failure
        self._cancel(stream)
        if self.on_error is not None:
            try:
                self.on_error(_copy_error(error))
            except Exception:
                _LOG.exception("Subscription error callback raised")

    def _connect(self):
        with self.condition:
            if self.failed.is_set():
                raise RuntimeError("Subscription stopped")
            from_seq = self.last_seen_seq
        stream = self.client.subscribe(
            principal=self.principal, token=self.token, from_seq=from_seq,
            only_my_recipient=False, channels=(self.channel_id,),
        )
        try:
            with self.condition:
                stopped = self.failed.is_set()
                if not stopped:
                    self.active_subscribe = stream
            if stopped:
                raise RuntimeError("Subscription stopped")
            stream.wait_started(self.timeout_ms)
            with self.condition:
                if self.failed.is_set():
                    raise RuntimeError("Subscription stopped")
                self.condition.notify_all()
            return stream
        except Exception:
            self._cancel(stream)
            with self.condition:
                if self.active_subscribe is stream:
                    self.active_subscribe = None
            raise

    def _read(self):
        stream = None
        try:
            while not self.failed.is_set():
                try:
                    stream = call_rpc(
                        self._connect, max_retries=self.max_retries,
                        retry_interval_ms=self.retry_interval_ms,
                        stop_event=self.failed,
                    )
                    while not self.failed.is_set():
                        response = next(stream)
                        if response.HasField("message"):
                            if not self._accept(response.message):
                                return
                except Exception as exc:
                    was_connected = stream is not None
                    self._cancel(stream)
                    stream = None
                    with self.condition:
                        self.active_subscribe = None
                        if self.failed.is_set():
                            return
                        # A failed _connect already exhausted its round.
                        if was_connected and is_retryable(exc):
                            self.condition.notify_all()
                            continue
                        error = OpenEventSubscriptionError(
                            "Subscription failed", reason="rpc", last_status=status_name(exc),
                        )
                        failure = self._fail_locked(error)
                    self._finish_failure(failure)
                    return
        finally:
            self._cancel(stream)
            with self.condition:
                self.active_subscribe = None
                self.condition.notify_all()

    def _accept(self, raw):
        with self.condition:
            if self.failed.is_set():
                return False
            if raw.seq <= self.last_seen_seq:
                return True
            if raw.principal == 0:
                # OpenEvent system messages share the cursor, not llm.v1 state.
                self.last_seen_seq = raw.seq
                self.condition.notify_all()
                return True
        try:
            message = parse_message(raw)
        except Exception as exc:
            with self.condition:
                if self.failed.is_set():
                    return False
                error = OpenEventSubscriptionError(
                    "Invalid subscribed message", reason="protocol", protocol_error=exc,
                )
                failure = self._fail_locked(error)
            self._finish_failure(failure)
            return False
        with self.condition:
            if self.failed.is_set():
                return False
            payload = message.payload
            if payload.kind == "infer.request":
                call = self.calls_by_uuid.pop(message.uuid, None)
                if call is not None:
                    call.receive = ReceiveState(message.seq, message.seq)
                    self.receives_by_seq[message.seq] = call
                    call.pending_uuid = None
            else:
                request_seq = payload.prev_seq if payload.kind == "infer.result" else payload.request_seq
                call = self.receives_by_seq.get(request_seq)
                if call is not None and payload.stream_id == call.stream_id:
                    self._accept_output_locked(call, message)
            self.last_seen_seq = message.seq
            self.condition.notify_all()
        return True

    def _accept_output_locked(self, call, message):
        payload, receive = message.payload, call.receive
        kind = payload.kind
        terminal = False
        fault = None
        if kind == "infer.result":
            if receive.head_seen:
                return
            if not call.streaming and not payload.has_body:
                fault = "A non-streaming request received a result without body"
            else:
                receive.head_seen = True
                call.result = message
                terminal = payload.has_body
                if not terminal:
                    receive.chain_seq = message.seq
        elif not call.streaming:
            return
        elif kind == "infer.append":
            if not receive.head_seen:
                fault = "Append arrived before the streaming result"
            elif payload.prev_seq != receive.chain_seq:
                fault = "Append does not continue the accepted stream chain"
            else:
                receive.chain_seq = message.seq
        elif kind in ("infer.end", "infer.cancel"):
            terminal = True
            if kind == "infer.end" and payload.end_status == "completed":
                call.completed = message
        if fault is not None:
            terminal = True
            call.queue.append(ProtocolFault(fault, message))
        else:
            call.queue.append(message)
        if terminal:
            receive.terminal = True
            self._detach_locked(call)

    def next_message(self, call):
        with self.condition:
            while True:
                if call.queue:
                    return call.queue.popleft()
                if call.error is not None:
                    raise _copy_error(call.error)
                if call.local_closed:
                    raise StopIteration
                self.condition.wait()

    def check_stream_return(self, call):
        with self.condition:
            if call.queue:
                return
            if call.error is not None:
                raise _copy_error(call.error)
