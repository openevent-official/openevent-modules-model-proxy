import gc
import json
import queue
import threading
import unittest
import weakref
from types import SimpleNamespace
from unittest.mock import patch

import grpc

from openevent.model_proxy_sdk import (
    APIError, ConfigurationError, OpenAI,
    OpenEventSubscriptionError, PayloadValidationError, ProtocolError,
    RateLimitError, ResultPublishError, StreamCancelledError,
    InferResultInput, parse_payload,
)


class RpcError(Exception):
    def __init__(self, status):
        self.status = status

    def code(self):
        return getattr(grpc.StatusCode, self.status)


class Task:
    def __init__(self, operation):
        self.result = self.error = None
        self.done = threading.Event()

        def run():
            try:
                self.result = operation()
            except Exception as exc:
                self.error = exc
            finally:
                self.done.set()

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def join(self):
        if not self.done.wait(3):
            raise AssertionError("Operation did not finish")
        self.thread.join()
        if self.error is not None:
            raise self.error
        return self.result


class FakeStream:
    def __init__(self, server, start_error=None):
        self.server = server
        self.queue = queue.Queue()
        self.accepted = threading.Event()
        self.cancelled = False
        self.start_error = start_error
        if server.allow_start:
            self.accepted.set()

    def wait_started(self, timeout_ms):
        if self.start_error is not None:
            raise self.start_error
        if not self.accepted.wait(timeout_ms / 1000):
            raise TimeoutError("Acceptance timeout")
        if self.cancelled:
            raise RpcError("CANCELLED")

    def __next__(self):
        item = self.queue.get(timeout=3)
        if isinstance(item, Exception):
            raise item
        return SimpleNamespace(message=item, HasField=lambda name: name == "message")

    def cancel(self):
        self.cancelled = True
        self.accepted.set()
        self.queue.put(RpcError("CANCELLED"))

    def code(self):
        return grpc.StatusCode.CANCELLED


class FakeServer:
    def __init__(self):
        self.lock = threading.Lock()
        self.log = []
        self.next_uuid = 1
        self.streams = []
        self.from_seqs = []
        self.allow_start = True
        self.start_errors = []
        self.status_count = 0
        self.status_error = None
        self.status_entered = threading.Event()
        self.status_release = threading.Event()
        self.status_release.set()
        self.uuid_entered = threading.Event()
        self.uuid_release = threading.Event()
        self.uuid_release.set()
        self.uuid_error = None
        self.on_publish = None
        self.closed = False

    def client(self, *args, **kwargs):
        return FakeClient(self)

    def get_status(self, principal, token):
        self.status_count += 1
        self.status_entered.set()
        if not self.status_release.wait(3):
            raise TimeoutError("Test status gate")
        if self.status_error:
            raise self.status_error
        with self.lock:
            return SimpleNamespace(max_seq=len(self.log))

    def get_uuid(self):
        self.uuid_entered.set()
        if not self.uuid_release.wait(3):
            raise TimeoutError("Test UUID gate")
        if self.uuid_error:
            raise self.uuid_error
        with self.lock:
            value = self.next_uuid
            self.next_uuid += 1
            return value

    def publish_auto_seq(self, **kwargs):
        raw = self.emit_raw(**{key: value for key, value in kwargs.items() if key != "token"})
        if self.on_publish is not None:
            result = self.on_publish(raw)
            if result is not None:
                return result
        return SimpleNamespace(seq=raw.seq)

    def get_seq_by_uuid(self, uuid):
        return next(raw.seq for raw in self.log if raw.uuid == uuid)

    def emit_raw(self, *, payload, uuid=900000, channel_id=1, principal=2,
                 recipients=(1,), object_keys=()):
        with self.lock:
            raw = SimpleNamespace(
                seq=len(self.log) + 1, uuid=uuid, channel_id=channel_id,
                principal=principal, recipients=recipients, object_keys=object_keys,
                payload=payload, ts_ms=5000,
            )
            self.log.append(raw)
            for stream in self.streams:
                if not stream.cancelled:
                    stream.queue.put(raw)
            return raw

    def emit(self, kind, stream_id, **fields):
        payload = dict(kind="infer." + kind, stream_id=stream_id, ts_ms=1, **fields)
        return self.emit_raw(payload=json.dumps(payload).encode())

    def subscribe(self, **kwargs):
        with self.lock:
            self.from_seqs.append(kwargs["from_seq"])
            assert kwargs["only_my_recipient"] is False
            assert kwargs["channels"] == (1,)
            assert all(stream.cancelled for stream in self.streams), "Competing Subscribe calls"
            stream = FakeStream(self, self.start_errors.pop(0) if self.start_errors else None)
            self.streams.append(stream)
            for raw in self.log:
                if raw.seq >= kwargs["from_seq"]:
                    stream.queue.put(raw)
            return stream

    def accept(self):
        with self.lock:
            self.allow_start = True
            for stream in self.streams:
                stream.accepted.set()

class FakeClient:
    """Closing the transport cancels RPCs and rejects later RPC attempts."""

    def __init__(self, server):
        self.server = server
        self.closed = False
        self.streams = []

    def __getattr__(self, name):
        operation = getattr(self.server, name)

        def invoke(*args, **kwargs):
            if self.closed:
                raise ValueError("Cannot invoke RPC on closed channel")
            result = operation(*args, **kwargs)
            if name == "subscribe":
                self.streams.append(result)
                if self.closed:
                    result.cancel()
            if self.closed:
                raise RpcError("CANCELLED")
            return result

        return invoke

    def close(self):
        self.closed = True
        self.server.closed = True
        for stream in self.streams:
            stream.cancel()
        self.server.uuid_release.set()
        self.server.status_release.set()


class OpenAITest(unittest.TestCase):
    def assertEquivalentError(self, actual, expected):
        self.assertIsNot(actual, expected)
        self.assertIs(type(actual), type(expected))
        self.assertEqual(actual.args, expected.args)
        for name in ("reason", "last_status", "code", "kind", "stream_id", "request_seq",
                     "result_openevent_seq", "last_stream_openevent_seq", "status_code",
                     "headers", "body", "end_status", "commit_state", "event_uuid", "committed_seq"):
            if hasattr(expected, name):
                self.assertEqual(getattr(actual, name), getattr(expected, name))
        if isinstance(expected, OpenEventSubscriptionError):
            if expected.protocol_error is None:
                self.assertIsNone(actual.protocol_error)
            else:
                self.assertEquivalentError(actual.protocol_error, expected.protocol_error)

    def assertNotification(self, errors, received):
        self.assertEqual(len(errors), 1)
        self.assertEquivalentError(errors[0], received)

    def setUp(self):
        self.server = FakeServer()
        self.patch = patch("openevent.model_proxy_sdk.openai.OpenEventClient", self.server.client)
        self.patch.start()
        self.clients = []

    def tearDown(self):
        self.server.uuid_release.set()
        self.server.status_release.set()
        self.server.on_publish = None
        self.server.accept()
        for client in self.clients:
            try:
                client.close()
            except Exception:
                pass
        for client in self.clients:
            reader = client._subscription.reader
            if reader is not None:
                reader.join(2)
                self.assertFalse(reader.is_alive(), "Subscription reader did not exit after transport close")
        self.patch.stop()

    def client(self, **kwargs):
        client = OpenAI(
            openevent_addr="test", openevent_token="token", openevent_channel_id=1,
            openevent_principal=1, max_retries=kwargs.pop("max_retries", 0),
            retry_interval_ms=kwargs.pop("retry_interval_ms", 1), rpc_timeout_ms=1000, **kwargs,
        )
        self.clients.append(client)
        return client

    def received(self, client, seq):
        sub = client._subscription
        with sub.condition:
            self.assertTrue(sub.condition.wait_for(
                lambda: sub.last_seen_seq is not None and sub.last_seen_seq >= seq,
                timeout=2,
            ), f"Reader did not accept seq {seq}; state={sub.state}")

    def failed(self, client):
        sub = client._subscription
        with sub.condition:
            self.assertTrue(sub.condition.wait_for(lambda: sub.state == "FAILED", timeout=2))

    def result(self, stream, *, body_marker=False, body=None, status=200):
        kwargs = dict(prev_seq=stream.request_seq, status_code=status)
        if body_marker:
            kwargs["body"] = body
        return self.server.emit("result", stream.stream_id, **kwargs)

    def end(self, stream, **fields):
        return self.server.emit(
            "end", stream.stream_id, request_seq=stream.request_seq,
            status_code=fields.pop("status_code", 200),
            end_status=fields.pop("end_status", "completed"), **fields,
        )

    def test_stream_returns_before_subscribe_acceptance_and_metadata_updates_without_next(self):
        self.server.allow_start = False
        client = self.client()
        stream = client.responses.create(stream=True, stream_id="s")
        self.assertEqual(stream.request_seq, 1)
        self.assertIsNone(stream.result_openevent_seq)
        result = self.result(stream)
        append = self.server.emit("append", "s", request_seq=1, prev_seq=result.seq, body={"delta": "hi"})
        end = self.end(stream, body=None)
        self.server.accept()
        self.received(client, end.seq)
        self.assertEqual(stream.result_openevent_seq, result.seq)
        self.assertEqual(stream.response_status_code, 200)
        self.assertTrue(stream.terminal_has_body)
        self.assertIsNone(stream.terminal_body)
        self.assertEqual(next(stream).append_openevent_seq, append.seq)
        self.assertEqual(list(stream), [])
        self.assertTrue(stream.terminal_has_body)
        stream.close()
        self.assertEqual(len(self.server.log), 4)
        self.assertEqual(self.server.from_seqs, [0])

    def test_reader_creation_or_start_failure_allows_a_later_call(self):
        for failure_point in ("Thread", "Thread.start"):
            with self.subTest(failure_point=failure_point):
                errors = []
                client = self.client(max_retries=3, on_subscription_error=errors.append)
                sub = client._subscription
                before = (len(self.server.log), self.server.next_uuid, self.server.status_count)
                failure = RuntimeError("cannot start reader")
                with patch("openevent.model_proxy_sdk.subscription.threading." + failure_point,
                           side_effect=failure):
                    with self.assertRaises(RuntimeError) as caught:
                        client.responses.create(input="not published")
                self.assertIs(caught.exception, failure)
                self.assertEqual(sub.state, "NEW")
                self.assertFalse(sub.preparing)
                self.assertFalse(sub.failed.is_set())
                self.assertIsNone(sub.reader)
                self.assertIsNone(sub.initial_from_seq)
                self.assertIsNone(sub.last_seen_seq)
                self.assertEqual(errors, [])
                self.assertEqual((len(self.server.log), self.server.next_uuid, self.server.status_count),
                                 (before[0], before[1], before[2] + 1))

                def respond(raw):
                    payload = json.loads(raw.payload)
                    self.server.emit("result", payload["stream_id"], prev_seq=raw.seq,
                                     status_code=200, body={"recovered": True})

                self.server.on_publish = respond
                try:
                    response = client.responses.create(input="published after recovery")
                    self.assertEqual(response.body, {"recovered": True})
                    self.assertEqual(self.server.status_count, before[2] + 2)
                    self.assertEqual(sub.initial_from_seq, before[0])
                    self.assertEqual(errors, [])
                finally:
                    client.close()
                    if sub.reader is not None:
                        sub.reader.join(2)

    def test_reader_start_failure_wakes_another_initial_call(self):
        errors = []
        client = self.client(on_subscription_error=errors.append)
        sub = client._subscription
        self.server.status_release.clear()
        waiting = threading.Event()
        original_wait, original_start = sub.condition.wait, threading.Thread.start
        readers = []
        failure = RuntimeError("cannot start first reader")

        def wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        def start(thread):
            if thread.name == "model-proxy-subscription":
                readers.append(thread)
                if len(readers) == 1:
                    raise failure
            return original_start(thread)

        with patch.object(sub.condition, "wait", side_effect=wait), \
                patch("openevent.model_proxy_sdk.subscription.threading.Thread.start", start):
            first = Task(lambda: client.responses.create(stream=True))
            self.assertTrue(self.server.status_entered.wait(2))
            second = Task(lambda: client.responses.create(stream=True))
            self.assertTrue(waiting.wait(2))
            self.server.status_release.set()
            with self.assertRaises(RuntimeError) as caught:
                first.join()
            self.assertIs(caught.exception, failure)
            stream = second.join()

        head = self.result(stream)
        self.server.emit("append", stream.stream_id, request_seq=stream.request_seq,
                         prev_seq=head.seq, body="recovered")
        end = self.end(stream)
        self.received(client, end.seq)
        self.assertEqual([chunk.body for chunk in stream], ["recovered"])
        self.assertEqual(self.server.status_count, 2)
        self.assertEqual(self.server.from_seqs, [0])
        self.assertEqual(len(readers), 2)
        self.assertIs(sub.reader, readers[1])
        self.assertEqual(errors, [])

    def test_reader_finishes_while_publish_waits_but_create_does_not_return(self):
        client = self.client()
        publish_entered, release = threading.Event(), threading.Event()

        def on_publish(raw):
            payload = json.loads(raw.payload)
            if payload["kind"] == "infer.request":
                result = self.server.emit("result", payload["stream_id"], prev_seq=raw.seq, status_code=200)
                self.server.emit("append", payload["stream_id"], request_seq=raw.seq, prev_seq=result.seq, body=[1, None])
                end = self.server.emit("end", payload["stream_id"], request_seq=raw.seq, status_code=200, end_status="completed")
                self.received(client, end.seq)
                publish_entered.set()
                if not release.wait(2):
                    raise TimeoutError("Test publish gate")

        self.server.on_publish = on_publish
        task = Task(lambda: client.chat.completions.create(stream=True))
        self.assertTrue(publish_entered.wait(2))
        self.assertFalse(task.done.is_set())
        self.assertEqual(client._subscription.calls_by_uuid, {})
        call = next(iter(client._subscription.calls))
        self.assertIsNone(call.frozen)
        self.assertEqual(call.receive.request_seq, 1)
        release.set()
        stream = task.join()
        self.assertFalse(stream.terminal_has_body)
        self.assertEqual([chunk.body for chunk in stream], [[1, None]])
        self.assertEqual(client._subscription.receives_by_seq, {})

    def test_stopped_stream_releases_request_and_preserves_received_output(self):
        for stop in ("terminal", "stream_close", "client_close", "subscription_failure"):
            with self.subTest(stop=stop):
                client = self.client()
                try:
                    stream = client.responses.create(stream=True, input="x" * (1024 * 1024))
                    frozen = weakref.ref(stream._call.frozen)
                    head = self.result(stream)
                    append = self.server.emit(
                        "append", stream.stream_id, request_seq=stream.request_seq,
                        prev_seq=head.seq, body={"delta": "saved"},
                    )
                    self.received(client, append.seq)
                    self.assertIsNotNone(frozen())
                    if stop == "terminal":
                        terminal = self.end(stream, body={"done": True})
                        self.received(client, terminal.seq)
                    elif stop == "stream_close":
                        stream.close()
                    elif stop == "client_close":
                        client.close()
                        self.failed(client)
                    else:
                        self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
                        self.failed(client)
                    self.assertIsNone(frozen())
                    self.assertEqual(stream.response_status_code, 200)
                    self.assertEqual(next(stream).body, {"delta": "saved"})
                    if stop in ("subscription_failure", "client_close"):
                        with self.assertRaises(OpenEventSubscriptionError):
                            next(stream)
                    else:
                        self.assertEqual(list(stream), [])
                    if stop == "terminal":
                        self.assertEqual(stream.terminal_body, {"done": True})
                finally:
                    client.close()
                    client._subscription.reader.join(2)

    def test_early_terminal_does_not_release_publishers_frozen_retry_message(self):
        client = self.client(max_retries=1)
        attempts = []

        def publish(**kwargs):
            attempts.append(kwargs)
            if len(attempts) > 1:
                raise RpcError("ALREADY_EXISTS")
            request = self.server.emit_raw(**{
                key: value for key, value in kwargs.items() if key != "token"
            })
            payload = parse_payload(request.payload)
            terminal = self.server.emit(
                "end", payload.stream_id, request_seq=request.seq,
                status_code=200, end_status="completed", body={"done": True},
            )
            self.received(client, terminal.seq)
            call = next(iter(client._subscription.calls))
            self.assertIsNone(call.frozen)
            self.assertEqual(call.receive.request_seq, request.seq)
            raise RpcError("UNAVAILABLE")

        with patch.object(self.server, "publish_auto_seq", side_effect=publish):
            stream = client.responses.create(stream=True, input="original")
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0], attempts[1])
        self.assertEqual(stream.request_seq, 1)
        self.assertEqual(stream.terminal_body, {"done": True})
        self.assertEqual(list(stream), [])

    def test_original_publication_failure_wins_over_early_terminal_and_subscription_failure(self):
        client = self.client()

        def on_publish(raw):
            payload = json.loads(raw.payload)
            if payload["kind"] == "infer.request":
                self.server.emit("result", payload["stream_id"], prev_seq=raw.seq, status_code=200, body={"ok": True})
                self.server.emit_raw(payload=b"not json")
                self.failed(client)
                raise RpcError("UNAVAILABLE")

        self.server.on_publish = on_publish
        with self.assertRaises(ResultPublishError) as captured:
            client.chat.completions.create()
        self.assertEqual(captured.exception.commit_state.value, "UNKNOWN")
        self.assertIsNone(captured.exception.committed_seq)
        self.assertEqual(client._subscription.calls_by_uuid, {})

    def test_shared_error_preserves_chunks_and_has_no_request_context(self):
        errors = []
        client = self.client(on_subscription_error=errors.append)
        first = client.chat.completions.create(stream=True, stream_id="same")
        second = client.chat.completions.create(stream=True, stream_id="same")
        head = self.result(first)
        self.server.emit("append", "same", request_seq=first.request_seq, prev_seq=head.seq, body="chunk")
        self.server.emit_raw(payload=b"{}")
        self.failed(client)
        self.assertEqual(next(first).body, "chunk")
        with self.assertRaises(OpenEventSubscriptionError) as one:
            next(first)
        with self.assertRaises(OpenEventSubscriptionError) as two:
            next(second)
        with self.assertRaises(OpenEventSubscriptionError) as three:
            client.chat.completions.create(stream=True)
        self.assertEquivalentError(one.exception, two.exception)
        self.assertEquivalentError(one.exception, three.exception)
        client._subscription.reader.join(2)
        self.assertNotification(errors, one.exception)
        self.assertFalse(hasattr(one.exception, "stream_id"))
        self.assertFalse(hasattr(one.exception, "request_seq"))
        self.assertEqual(one.exception.reason, "protocol")
        for error in (client._subscription.error, client._subscription.error.protocol_error):
            self.assertIsNone(error.__traceback__)
            self.assertIsNone(error.__context__)
            self.assertIsNone(error.__cause__)

    def test_failed_calls_do_not_retain_caller_locals_or_accumulate_tracebacks(self):
        class CallerState:
            pass

        self.server.status_error = RpcError("PERMISSION_DENIED")
        client = self.client()
        with self.assertRaises(OpenEventSubscriptionError):
            client.responses.create()
        retained, depths = [], []

        def call_failed_client():
            state = CallerState()
            state.body = {"input": "caller data"}
            retained.append(weakref.ref(state))
            try:
                client.responses.create(**state.body)
            except OpenEventSubscriptionError as error:
                depth, traceback = 0, error.__traceback__
                while traceback is not None:
                    depth += 1
                    traceback = traceback.tb_next
                depths.append(depth)
            else:
                self.fail("Failed subscription accepted a new call")

        for _ in range(20):
            call_failed_client()
        gc.collect()
        self.assertTrue(all(reference() is None for reference in retained))
        self.assertGreater(depths[0], 0)
        self.assertEqual(len(set(depths)), 1)
        self.assertIsNone(client._subscription.error.__traceback__)

    def test_callback_can_raise_its_error_without_changing_cached_failure(self):
        errors = []

        def callback(error):
            errors.append(error)
            raise error

        client = self.client(on_subscription_error=callback)
        stream = client.responses.create(stream=True)
        with self.assertLogs("openevent.model_proxy_sdk.subscription", level="ERROR"):
            self.server.emit_raw(payload=b"{}")
            self.failed(client)
            client._subscription.reader.join(2)
        self.assertEqual(len(errors), 1)
        self.assertIsNotNone(errors[0].__traceback__)
        with self.assertRaises(OpenEventSubscriptionError) as caught:
            next(stream)
        self.assertEquivalentError(caught.exception, errors[0])
        for error in (client._subscription.error, client._subscription.error.protocol_error):
            self.assertIsNone(error.__traceback__)
            self.assertIsNone(error.__context__)
            self.assertIsNone(error.__cause__)

    def test_bad_chain_only_fails_its_call(self):
        client = self.client()
        bad = client.chat.completions.create(stream=True)
        good = client.chat.completions.create(stream=True)
        bad_event = self.server.emit("append", bad.stream_id, request_seq=bad.request_seq, prev_seq=bad.request_seq, body={})
        self.received(client, bad_event.seq)
        with self.assertRaises(ProtocolError) as captured:
            next(bad)
        self.assertEqual(captured.exception.request_seq, bad.request_seq)
        self.assertEqual(client._subscription.state, "READY")
        end = self.end(good)
        self.received(client, end.seq)
        self.assertEqual(list(good), [])

    def test_same_stream_id_calls_use_request_seq_and_normal_json_is_preserved(self):
        client = self.client()

        def on_publish(raw):
            payload = json.loads(raw.payload)
            if payload["kind"] == "infer.request":
                self.assertNotIn("provider", payload["body"])
                self.assertNotIn("prev_seq", payload["body"])
                self.server.emit("result", payload["stream_id"], prev_seq=raw.seq, status_code=200,
                                 body={"request_seq": "provider", "items": [{"value": raw.seq}]})

        self.server.on_publish = on_publish
        a = client.responses.create(stream_id="same", provider="test", prev_seq=4)
        b = client.responses.create(stream_id="same")
        self.assertNotEqual(a.request_seq, b.request_seq)
        self.assertEqual(a.items[0].value, a.request_seq)
        self.assertEqual(a.body["request_seq"], "provider")
        body = a.model_dump()
        body["items"].clear()
        self.assertEqual(len(a.body["items"]), 1)
        with self.assertRaises(AttributeError):
            a.request_seq = 5
        with self.assertRaises(AttributeError):
            a.items[0].value = 6
        self.assertEqual(self.server.status_count, 1)
        self.assertEqual(len(self.server.streams), 1)

    def test_stream_http_error_waits_for_end_and_preserves_provider_body(self):
        client = self.client()
        stream = client.responses.create(stream=True)
        self.result(stream, status=429)
        end = self.end(stream, status_code=429, body={"error": "quota"})
        self.received(client, end.seq)
        self.assertEqual(stream.terminal_body, {"error": "quota"})
        with self.assertRaises(RateLimitError) as captured:
            next(stream)
        self.assertEqual(captured.exception.body, {"error": "quota"})
        self.assertEqual(captured.exception.end_status, "completed")

    def test_cancel_before_head_is_terminal_and_failed_end_does_not_update_completed_properties(self):
        client = self.client()
        cancelled = client.chat.completions.create(stream=True)
        raw = self.server.emit("cancel", cancelled.stream_id, request_seq=cancelled.request_seq)
        self.received(client, raw.seq)
        with self.assertRaises(StreamCancelledError):
            next(cancelled)
        failed = client.responses.create(stream=True)
        raw = self.end(failed, end_status="failed", body={"type": "response.failed"})
        self.received(client, raw.seq)
        self.assertIsNone(failed.terminal_has_body)
        with self.assertRaises(APIError) as captured:
            next(failed)
        self.assertEqual(captured.exception.end_status, "failed")

    def test_local_close_keeps_queued_output_and_publishes_cancel_once(self):
        client = self.client()
        stream = client.chat.completions.create(stream=True)
        head = self.result(stream)
        raw = self.server.emit("append", stream.stream_id, request_seq=stream.request_seq, prev_seq=head.seq, body=123)
        self.received(client, raw.seq)
        stream.close()
        stream.close()
        self.assertEqual([chunk.body for chunk in stream], [123])
        self.assertEqual(sum(json.loads(raw.payload)["kind"] == "infer.cancel" for raw in self.server.log), 1)
        self.assertEqual(stream.result_openevent_seq, head.seq)
        self.assertFalse(self.server.closed)

    def test_queue_temporarily_empty_waits_instead_of_ending(self):
        client = self.client()
        stream = client.chat.completions.create(stream=True)
        task = Task(lambda: next(stream))
        self.assertFalse(task.done.wait(0.02))
        head = self.result(stream)
        self.server.emit("append", stream.stream_id, request_seq=stream.request_seq, prev_seq=head.seq, body=True)
        self.assertIs(task.join().body, True)

    def test_initial_status_failure_calls_callback_without_publication(self):
        self.server.status_error = RpcError("PERMISSION_DENIED")
        errors = []
        client = self.client(on_subscription_error=errors.append)
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            client.chat.completions.create()
        self.assertNotification(errors, captured.exception)
        self.assertEqual(captured.exception.last_status, "PERMISSION_DENIED")
        self.assertEqual(self.server.log, [])
        self.assertEqual(self.server.streams, [])

    def test_configuration_and_controls_are_validated_before_network(self):
        with self.assertRaises(ConfigurationError):
            OpenAI()
        client = self.client()
        with self.assertRaises(PayloadValidationError):
            client.chat.completions.create(stream="true")
        with self.assertRaises(PayloadValidationError):
            client.chat.completions.create(stream_id=None)
        self.assertEqual(self.server.status_count, 0)

    def test_close_during_publish_returns_before_publication_finishes(self):
        client = self.client()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def on_publish(raw):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("Test publish gate")

        self.server.on_publish = on_publish
        creating = Task(lambda: client.responses.create(stream=True))
        self.assertTrue(entered.wait(2))
        Task(client.close).join()
        self.assertTrue(self.server.closed)
        self.assertFalse(creating.done.is_set())
        release.set()
        with self.assertRaises(ResultPublishError) as captured:
            creating.join()
        self.assertEqual(captured.exception.commit_state.value, "UNKNOWN")
        self.assertEqual(captured.exception.last_status, "CANCELLED")
        self.assertEqual([json.loads(raw.payload)["kind"] for raw in self.server.log], ["infer.request"])

    def test_close_during_uuid_allocation_preserves_publication_error(self):
        self.server.uuid_release.clear()
        client = self.client()
        creating = Task(lambda: client.responses.create(stream=True))
        self.assertTrue(self.server.uuid_entered.wait(2))
        Task(client.close).join()
        with self.assertRaises(ResultPublishError) as captured:
            creating.join()
        self.assertEqual(captured.exception.commit_state.value, "NOT_COMMITTED")
        self.assertEqual(captured.exception.last_status, "CANCELLED")
        self.assertEqual(self.server.log, [])

    def test_close_during_status_uses_normal_subscription_failure(self):
        self.server.status_release.clear()
        errors = []
        client = self.client(on_subscription_error=errors.append)
        creating = Task(lambda: client.responses.create(stream=True))
        self.assertTrue(self.server.status_entered.wait(2))
        Task(client.close).join()
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            creating.join()
        self.assertEqual(captured.exception.last_status, "CANCELLED")
        self.assertNotification(errors, captured.exception)
        self.assertEqual(client._subscription.state, "FAILED")
        self.assertEqual(self.server.streams, [])
        self.assertEqual(self.server.log, [])

    def test_close_leaves_retries_and_queued_results_to_normal_error_handling(self):
        errors = []
        client = self.client(max_retries=1, on_subscription_error=errors.append)
        active = client.responses.create(stream=True)
        completed = client.responses.create(stream=True)
        head = self.result(active)
        self.server.emit("append", active.stream_id, request_seq=active.request_seq,
                         prev_seq=head.seq, body="saved")
        end = self.end(completed, body={"done": True})
        self.received(client, end.seq)
        sub = client._subscription
        waiting, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def retry_wait(timeout):
            waiting.set()
            if not release.wait(2):
                raise TimeoutError("Test retry gate")
            return False

        with patch.object(sub.failed, "wait", side_effect=retry_wait), \
                patch.object(client._client, "subscribe", wraps=client._client.subscribe) as subscribe:
            Task(client.close).join()
            self.assertTrue(waiting.wait(2))
            self.assertTrue(sub.reader.is_alive())
            self.assertEqual(sub.state, "DEGRADED")
            self.assertFalse(sub.failed.is_set())
            self.assertEqual(next(active).body, "saved")
            self.assertEqual(list(completed), [])
            self.assertEqual(completed.terminal_body, {"done": True})
            reading = Task(lambda: next(active))
            self.assertFalse(reading.done.is_set())
            release.set()
            with self.assertRaises(OpenEventSubscriptionError) as captured:
                reading.join()
            sub.reader.join(2)
            self.assertEqual(subscribe.call_count, 2)
        self.assertNotification(errors, captured.exception)
        self.assertEqual(sub.state, "FAILED")
        self.assertFalse(any(json.loads(raw.payload)["kind"] == "infer.cancel" for raw in self.server.log))

    def test_close_wakes_normal_call_via_subscription_failure(self):
        client = self.client()
        entered = threading.Event()
        self.server.on_publish = lambda raw: entered.set()
        creating = Task(lambda: client.responses.create())
        self.assertTrue(entered.wait(2))
        self.received(client, 1)
        Task(client.close).join()
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            creating.join()
        self.assertEquivalentError(captured.exception, client._subscription.error)
        self.assertEqual(len(self.server.log), 1)

    def test_close_does_not_wait_for_callback_and_callback_can_close_client(self):
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        errors = []

        def callback(error):
            client.close()
            errors.append(error)
            entered.set()
            if not release.wait(2):
                raise TimeoutError("Test callback gate")

        client = self.client(on_subscription_error=callback)
        stream = client.responses.create(stream=True)
        self.server.emit_raw(payload=b"bad")
        self.assertTrue(entered.wait(2))
        Task(client.close).join()
        self.assertTrue(client._subscription.reader.is_alive())
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            next(stream)
        self.assertNotification(errors, captured.exception)
        release.set()
        client._subscription.reader.join(2)

    def test_concurrent_stream_close_shares_cancel_failure(self):
        client = self.client()
        stream = client.responses.create(stream=True)
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        cancel_requests = []

        def on_publish(raw):
            payload = json.loads(raw.payload)
            if payload["kind"] == "infer.cancel":
                cancel_requests.append(payload["request_seq"])
                entered.set()
                if not release.wait(2):
                    raise TimeoutError("Test cancel gate")
                raise RpcError("PERMISSION_DENIED")

        self.server.on_publish = on_publish
        first = Task(stream.close)
        self.assertTrue(entered.wait(2))
        second = Task(stream.close)
        release.set()
        with self.assertRaises(ResultPublishError) as first_error:
            first.join()
        with self.assertRaises(ResultPublishError) as second_error:
            second.join()
        self.assertEquivalentError(first_error.exception, second_error.exception)
        with self.assertRaises(ResultPublishError) as repeated_error:
            stream.close()
        self.assertEquivalentError(repeated_error.exception, first_error.exception)
        self.assertEquivalentError(repeated_error.exception, second_error.exception)
        cached = stream._call.close_error
        self.assertEquivalentError(cached, first_error.exception)
        self.assertIsNone(cached.__traceback__)
        self.assertIsNone(cached.__context__)
        self.assertIsNone(cached.__cause__)
        self.assertEqual(cancel_requests, [stream.request_seq])
        client.close()
        client.close()

    def test_close_before_first_call_uses_normal_status_retry_and_failure(self):
        errors = []
        client = self.client(max_retries=2, on_subscription_error=errors.append)
        client.close()
        client.close()
        with patch.object(client._client, "get_status", wraps=client._client.get_status) as status:
            with self.assertRaises(OpenEventSubscriptionError) as captured:
                client.responses.create(stream=True)
            self.assertEqual(status.call_count, 3)
        self.assertNotification(errors, captured.exception)
        self.assertEqual(client._subscription.state, "FAILED")
        self.assertEqual(self.server.log, [])

    def test_keyboard_interrupt_cleans_up_call_without_swallowing_interrupt(self):
        for stage in ("uuid", "publish"):
            with self.subTest(stage=stage):
                client = self.client()
                operation = "get_uuid" if stage == "uuid" else "publish_auto_seq"
                interrupted = KeyboardInterrupt()
                with patch.object(self.server, operation, side_effect=interrupted):
                    with self.assertRaises(KeyboardInterrupt) as captured:
                        client.responses.create(stream=True)
                self.assertIs(captured.exception, interrupted)
                self.assertEqual(client._subscription.calls_by_uuid, {})
                self.assertEqual(client._subscription.receives_by_seq, {})
                self.assertTrue(all(call.local_closed and call.frozen is None
                                    for call in client._subscription.calls))
                recovered = client.responses.create(stream=True)
                terminal = self.end(recovered)
                self.received(client, terminal.seq)
                self.assertEqual(list(recovered), [])
                Task(client.close).join()
                client._subscription.reader.join(2)

    def test_published_seq_conflict_discards_early_terminal_and_fails_subscription(self):
        errors = []
        client = self.client(on_subscription_error=errors.append)

        def on_publish(raw):
            payload = json.loads(raw.payload)
            result = self.server.emit("result", payload["stream_id"], prev_seq=raw.seq,
                                      status_code=200, body="wrong")
            self.received(client, result.seq)
            return SimpleNamespace(seq=raw.seq + 100)

        self.server.on_publish = on_publish
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            client.responses.create()
        self.assertEqual(captured.exception.reason, "protocol")
        self.assertNotification(errors, captured.exception)
        self.assertEqual(client._subscription.state, "FAILED")

    def test_locally_closed_stream_is_not_changed_by_later_subscription_failure(self):
        client = self.client()
        closed = client.responses.create(stream=True)
        active = client.responses.create(stream=True)
        closed.close()
        self.server.emit_raw(payload=b"bad")
        self.failed(client)
        self.assertEqual(list(closed), [])
        with self.assertRaises(OpenEventSubscriptionError):
            next(active)

    def test_reconnect_replays_from_cursor_and_does_not_block_new_publish(self):
        client = self.client()
        first = client.responses.create(stream=True)
        head = self.result(first)
        self.received(client, head.seq)
        self.server.allow_start = False
        self.server.streams[-1].queue.put(RpcError("UNAVAILABLE"))
        sub = client._subscription
        with sub.condition:
            self.assertTrue(sub.condition.wait_for(lambda: sub.state == "DEGRADED", timeout=2))
        second = client.responses.create(stream=True)
        end = self.end(second, body="second")
        self.server.accept()
        self.received(client, end.seq)
        self.assertEqual(self.server.status_count, 1)
        self.assertEqual(self.server.from_seqs, [0, head.seq])
        self.assertEqual(second.terminal_body, "second")
        self.assertEqual(list(second), [])
        self.assertEqual(first.result_openevent_seq, head.seq)

    def test_permanent_subscription_error_does_not_reconnect(self):
        client = self.client(max_retries=3)
        stream = client.responses.create(stream=True)
        self.received(client, stream.request_seq)
        self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
        self.failed(client)
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            next(stream)
        self.assertEqual(captured.exception.last_status, "PERMISSION_DENIED")
        self.assertEqual(len(self.server.streams), 1)

    def test_failed_client_and_stream_close_do_not_send_any_requests(self):
        client = self.client()
        active = client.responses.create(stream=True, stream_id="active")
        complete = client.responses.create(stream=True, stream_id="complete")
        head = self.result(active)
        self.server.emit("append", active.stream_id, request_seq=active.request_seq,
                         prev_seq=head.seq, body={"saved": True})
        end = self.end(complete, body={"done": True})
        self.received(client, end.seq)
        self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
        self.failed(client)
        error = client._subscription.error
        with patch.object(self.server, "get_uuid", wraps=self.server.get_uuid) as uuid, \
                patch.object(self.server, "publish_auto_seq", wraps=self.server.publish_auto_seq) as publish, \
                patch.object(self.server, "get_seq_by_uuid", wraps=self.server.get_seq_by_uuid) as query, \
                patch.object(self.server, "get_status", wraps=self.server.get_status) as status, \
                patch.object(self.server, "subscribe", wraps=self.server.subscribe) as subscribe:
            with self.assertRaises(OpenEventSubscriptionError) as rejected:
                client.responses.create(stream=True)
            self.assertEquivalentError(rejected.exception, error)
            active.close()
            active.close()
            client.close()
            complete.close()
            for rpc in (uuid, publish, query, status, subscribe):
                rpc.assert_not_called()
        self.assertEqual(next(active).body, {"saved": True})
        with self.assertRaises(OpenEventSubscriptionError) as failed:
            next(active)
        self.assertEquivalentError(failed.exception, error)
        self.assertEqual(active.response_status_code, 200)
        self.assertEqual(list(complete), [])
        self.assertEqual(complete.terminal_body, {"done": True})

    def test_failure_between_call_begin_and_first_uuid_prevents_uuid_rpc(self):
        client = self.client()
        warmup = client.responses.create(stream=True)
        self.received(client, warmup.request_seq)
        sub = client._subscription
        begin_rpc = sub.begin_publication_rpc

        def failure_before_rpc():
            self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
            self.failed(client)
            begin_rpc()

        with patch.object(sub, "begin_publication_rpc", side_effect=failure_before_rpc), \
                patch.object(self.server, "get_uuid", wraps=self.server.get_uuid) as uuid:
            with self.assertRaises(OpenEventSubscriptionError) as captured:
                client.responses.create(stream=True)
            uuid.assert_not_called()
        self.assertEquivalentError(captured.exception, sub.error)
        self.assertEqual(len(self.server.log), 1)

    def test_failed_subscription_interrupts_long_request_retry_waits(self):
        for stage in ("uuid", "publish", "query"):
            with self.subTest(stage=stage):
                client = self.client(max_retries=3, retry_interval_ms=30000)
                warmup = client.responses.create(stream=True)
                self.received(client, warmup.request_seq)
                sub = client._subscription
                waiting = threading.Event()
                original_wait = sub.failed.wait

                def retry_wait(timeout=None):
                    waiting.set()
                    return original_wait(timeout)

                publish_error = "ALREADY_EXISTS" if stage == "query" else "UNAVAILABLE"

                def fail_publish(raw):
                    raise RpcError(publish_error)

                self.server.on_publish = None if stage == "uuid" else fail_publish
                target_name = "get_uuid" if stage == "uuid" else "get_seq_by_uuid" if stage == "query" else "publish_auto_seq"
                target = getattr(self.server, target_name)
                with patch.object(sub.failed, "wait", side_effect=retry_wait), \
                        patch.object(self.server, target_name,
                                     side_effect=RpcError("UNAVAILABLE") if stage != "publish" else None,
                                     wraps=target if stage == "publish" else None) as rpc:
                    task = Task(lambda: client.responses.create(stream=True))
                    self.assertTrue(waiting.wait(2), "RPC did not reach its 30-second retry wait")
                    self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
                    self.failed(client)
                    with self.assertRaises(ResultPublishError) as captured:
                        task.join()
                    self.assertEqual(rpc.call_count, 1)
                self.assertEqual(captured.exception.commit_state.value,
                                 {"uuid": "NOT_COMMITTED", "publish": "UNKNOWN", "query": "COMMITTED"}[stage])
                self.assertEqual(captured.exception.last_status, "UNAVAILABLE")
                self.server.on_publish = None
                client.close()

    def test_already_exists_after_failure_does_not_start_uuid_query(self):
        client = self.client(max_retries=3)
        warmup = client.responses.create(stream=True)
        self.received(client, warmup.request_seq)

        def on_publish(raw):
            self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
            self.failed(client)
            raise RpcError("ALREADY_EXISTS")

        self.server.on_publish = on_publish
        with patch.object(self.server, "get_seq_by_uuid", wraps=self.server.get_seq_by_uuid) as query:
            with self.assertRaises(ResultPublishError) as captured:
                client.responses.create(stream=True)
            query.assert_not_called()
        self.assertEqual(captured.exception.commit_state.value, "COMMITTED")
        self.assertEqual(captured.exception.last_status, "ALREADY_EXISTS")
        self.assertEqual(len(self.server.log), 2)

    def test_inflight_success_after_failure_preserves_seq_and_accepted_terminal(self):
        client = self.client(max_retries=3)
        warmup = client.responses.create(stream=True)
        self.received(client, warmup.request_seq)

        def on_publish(raw):
            payload = json.loads(raw.payload)
            end = self.server.emit("end", payload["stream_id"], request_seq=raw.seq,
                                   status_code=200, end_status="completed", body={"done": True})
            self.received(client, end.seq)
            self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
            self.failed(client)

        self.server.on_publish = on_publish
        stream = client.responses.create(stream=True)
        self.assertEqual(stream.request_seq, 2)
        self.assertEqual(stream.terminal_body, {"done": True})
        self.assertEqual(list(stream), [])
        client.close()
        self.assertFalse(any(json.loads(raw.payload)["kind"] == "infer.cancel" for raw in self.server.log))

    def test_cancel_uuid_success_after_failure_finishes_close_without_publish(self):
        client = self.client(max_retries=3)
        stream = client.responses.create(stream=True)
        self.received(client, stream.request_seq)
        self.server.uuid_entered.clear()
        self.server.uuid_release.clear()
        closing = Task(stream.close)
        self.assertTrue(self.server.uuid_entered.wait(2))
        self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
        self.failed(client)
        self.server.uuid_release.set()
        closing.join()
        client.close()
        self.assertEqual(len(self.server.log), 1)

    def test_cancel_uuid_failure_after_failure_preserves_original_publish_error(self):
        client = self.client(max_retries=3)
        stream = client.responses.create(stream=True)
        self.received(client, stream.request_seq)
        self.server.uuid_entered.clear()
        self.server.uuid_release.clear()
        with patch.object(self.server, "get_uuid", wraps=self.server.get_uuid) as uuid:
            closing = Task(stream.close)
            self.assertTrue(self.server.uuid_entered.wait(2))
            self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
            self.failed(client)
            self.server.uuid_error = RpcError("UNAVAILABLE")
            self.server.uuid_release.set()
            with self.assertRaises(ResultPublishError) as captured:
                closing.join()
            self.assertEqual(uuid.call_count, 1)
        self.assertEqual(captured.exception.commit_state.value, "NOT_COMMITTED")
        with self.assertRaises(ResultPublishError) as repeated:
            stream.close()
        self.assertEquivalentError(repeated.exception, captured.exception)
        client.close()
        self.server.uuid_error = None
        self.assertEqual(len(self.server.log), 1)

    def test_failure_interrupts_cancel_publish_retry_and_concurrent_close(self):
        client = self.client(max_retries=3, retry_interval_ms=30000)
        stream = client.responses.create(stream=True)
        self.received(client, stream.request_seq)
        sub = client._subscription
        waiting = threading.Event()
        original_wait = sub.failed.wait

        def retry_wait(timeout=None):
            waiting.set()
            return original_wait(timeout)

        def on_publish(raw):
            if json.loads(raw.payload)["kind"] == "infer.cancel":
                raise RpcError("UNAVAILABLE")

        self.server.on_publish = on_publish
        with patch.object(sub.failed, "wait", side_effect=retry_wait), \
                patch.object(self.server, "publish_auto_seq", wraps=self.server.publish_auto_seq) as publish:
            closing = Task(stream.close)
            self.assertTrue(waiting.wait(2))
            self.server.streams[-1].queue.put(RpcError("PERMISSION_DENIED"))
            self.failed(client)
            whole = Task(client.close)
            with self.assertRaises(ResultPublishError) as one:
                closing.join()
            whole.join()
            self.assertEqual(one.exception.commit_state.value, "UNKNOWN")
            self.assertEqual(publish.call_count, 1)

    def test_concurrent_first_calls_share_one_status_anchor_and_reader(self):
        self.server.status_release.clear()
        client = self.client()
        tasks = [Task(lambda: client.responses.create(stream=True)) for _ in range(8)]
        self.assertTrue(self.server.status_entered.wait(2))
        self.assertEqual(self.server.status_count, 1)
        self.assertEqual(self.server.log, [])
        self.server.status_release.set()
        streams = [task.join() for task in tasks]
        self.received(client, max(stream.request_seq for stream in streams))
        self.assertEqual(self.server.status_count, 1)
        self.assertEqual(len(self.server.streams), 1)
        self.assertEqual(len({stream.request_seq for stream in streams}), 8)

    def test_first_subscribe_retries_keep_anchor_and_do_not_repeat_get_status(self):
        self.server.start_errors = [RpcError("UNAVAILABLE"), RpcError("UNKNOWN")]
        client = self.client(max_retries=2)
        stream = client.responses.create(stream=True)
        self.received(client, stream.request_seq)
        self.assertEqual(self.server.from_seqs, [0, 0, 0])
        self.assertEqual(self.server.status_count, 1)
        self.assertTrue(all(item.cancelled for item in self.server.streams[:-1]))

    def test_subscribe_retry_exhaustion_notifies_once_and_rejects_new_calls(self):
        self.server.start_errors = [RpcError("UNAVAILABLE") for _ in range(3)]
        errors = []
        client = self.client(max_retries=2, on_subscription_error=errors.append)
        task = Task(lambda: client.responses.create(stream=True))
        self.failed(client)
        client._subscription.reader.join(2)
        self.assertEqual(len(errors), 1)
        try:
            stream = task.join()
        except OpenEventSubscriptionError as exc:
            self.assertEquivalentError(exc, errors[0])
        else:
            with self.assertRaises(OpenEventSubscriptionError) as captured:
                next(stream)
            self.assertEquivalentError(captured.exception, errors[0])
        with self.assertRaises(OpenEventSubscriptionError):
            client.responses.create(stream=True)
        self.assertEqual(len(self.server.streams), 3)

    def test_frozen_request_metadata_mismatch_fails_subscription(self):
        client = self.client()
        original_emit = self.server.emit_raw

        def different_principal(**kwargs):
            if json.loads(kwargs["payload"])["kind"] == "infer.request":
                kwargs["principal"] = 999
            return original_emit(**kwargs)

        self.server.emit_raw = different_principal
        self.server.on_publish = lambda raw: self.failed(client)
        with self.assertRaises(OpenEventSubscriptionError) as captured:
            client.responses.create(stream=True)
        self.assertEqual(captured.exception.reason, "protocol")
        self.assertIsInstance(captured.exception.protocol_error, ProtocolError)

    def test_normal_call_without_body_is_local_protocol_error(self):
        client = self.client()

        def on_publish(raw):
            payload = json.loads(raw.payload)
            self.server.emit("result", payload["stream_id"], prev_seq=raw.seq, status_code=200,
                             headers=[{"name": "x-request-id", "value": "malformed-result"}])

        self.server.on_publish = on_publish
        with self.assertRaises(ProtocolError) as captured:
            client.responses.create()
        self.assertEqual(captured.exception.status_code, 200)
        self.assertEqual(captured.exception.result_openevent_seq, 2)
        self.assertEqual(captured.exception.headers, [{"name": "x-request-id", "value": "malformed-result"}])
        self.assertEqual(client._subscription.state, "READY")

    def test_initial_failure_callback_does_not_block_client_close(self):
        entered, release = threading.Event(), threading.Event()

        def callback(error):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("Test callback gate")

        self.server.status_error = RpcError("PERMISSION_DENIED")
        client = self.client(on_subscription_error=callback)
        creating = Task(lambda: client.responses.create(stream=True))
        self.assertTrue(entered.wait(2))
        closing = Task(client.close)
        closing.join()
        self.assertFalse(creating.done.is_set())
        release.set()
        with self.assertRaises(OpenEventSubscriptionError):
            creating.join()
        closing.join()

    def test_nested_json_properties_do_not_collide_with_mapping_methods(self):
        client = self.client()
        value = {"object": {"items": [1, {"keys": "key", "get": "value"}]}}

        def on_publish(raw):
            payload = json.loads(raw.payload)
            self.server.emit("result", payload["stream_id"], prev_seq=raw.seq, status_code=200, body=value)

        self.server.on_publish = on_publish
        response = client.responses.create()
        self.assertEqual(response.object.items[0], 1)
        self.assertEqual(response.object.items[1].keys, "key")
        self.assertEqual(response.object.items[1].get, "value")
        with self.assertRaises(AttributeError):
            response.object.items[1].keys = "changed"
        self.assertEqual(response.model_dump(), value)

    def test_protocol_chain_error_retains_known_status_headers_and_body(self):
        client = self.client()
        stream = client.responses.create(stream=True)
        head = self.server.emit(
            "result", stream.stream_id, prev_seq=stream.request_seq, status_code=200,
            headers=[{"name": "x-request-id", "value": "provider-request"}],
        )
        self.server.emit("append", stream.stream_id, request_seq=stream.request_seq,
                         prev_seq=stream.request_seq, body={"delta": "bad chain"})
        with self.assertRaises(ProtocolError) as captured:
            next(stream)
        error = captured.exception
        self.assertEqual(error.result_openevent_seq, head.seq)
        self.assertEqual(error.status_code, 200)
        self.assertEqual(error.headers, [{"name": "x-request-id", "value": "provider-request"}])
        self.assertEqual(error.body, {"delta": "bad chain"})
        body = error.body
        body.clear()
        self.assertEqual(error.body, {"delta": "bad chain"})
        with self.assertRaises(AttributeError):
            error.status_code = 500

    def test_large_json_integer_reaches_public_error_without_serialization_error(self):
        client = self.client()
        integer = 10 ** 5000 + 3

        def on_publish(raw):
            request = parse_payload(raw.payload)
            self.assertEqual(request.body["input"], integer)
            result = InferResultInput(
                stream_id=request.stream_id, prev_seq=raw.seq, status_code=429,
                body={"error": {"code": -integer}},
            )
            self.server.emit_raw(payload=result.to_payload(1))

        self.server.on_publish = on_publish
        with self.assertRaises(RateLimitError) as captured:
            client.responses.create(input=integer)
        self.assertEqual(captured.exception.body["error"]["code"], -integer)
        self.assertEqual(captured.exception.status_code, 429)
