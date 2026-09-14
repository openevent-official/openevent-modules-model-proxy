"""Faults around real installed-SDK RPCs, against the real OpenEvent server."""

import os
import queue
import threading
import unittest
from collections import Counter
from unittest.mock import patch

import grpc
from openevent.sdk import OpenEventClient
from openevent.model_proxy.config import parse_config
from openevent.model_proxy.worker import Worker, WorkerFatalError
from openevent.model_proxy_sdk import (
    CommitState, InferRequestInput, OpenEventSubscriptionError, ResultPublishError, create_client,
    publish_infer_request,
)

import test_e2e as support


class LostResponse(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.UNAVAILABLE

    def details(self):
        return "Injected loss of a successful RPC response"


class LoseCommittedResponse:
    """Every attempted RPC still goes to the server; selected replies are discarded."""

    def __init__(self, real, *, lose_queries=False):
        self.real = real
        self.lose_queries = lose_queries
        self.publish_calls = []
        self.query_calls = []
        self.committed_seq = None
        self.duplicate_statuses = []

    def __getattr__(self, name):
        return getattr(self.real, name)

    def publish_auto_seq(self, **kwargs):
        self.publish_calls.append(kwargs.copy())
        try:
            reply = self.real.publish_auto_seq(**kwargs)
        except grpc.RpcError as exc:
            self.duplicate_statuses.append(exc.code())
            raise
        self.committed_seq = reply.seq
        raise LostResponse()

    def get_seq_by_uuid(self, uuid):
        self.query_calls.append(uuid)
        seq = self.real.get_seq_by_uuid(uuid)
        if self.lose_queries:
            raise LostResponse()
        return seq


class DelaySubscribe:
    """Hold connection creation while keeping publication RPCs fully operational."""

    def __init__(self, real):
        self.real = real
        self.attempted = threading.Event()
        self.release = threading.Event()
        self.connected = threading.Event()
        self.anchors = []

    def __getattr__(self, name):
        return getattr(self.real, name)

    def subscribe(self, **kwargs):
        self.anchors.append(kwargs["from_seq"])
        self.attempted.set()
        if not self.release.wait(5):
            raise TimeoutError("Test did not release Subscribe gate")
        stream = self.real.subscribe(**kwargs)
        self.connected.set()
        return stream


class ObserveProcessedMessages:
    """The next read acknowledges that the reader handled the previous response."""

    def __init__(self, real, owner):
        self.real = real
        self.owner = owner
        self.previous = None

    def __getattr__(self, name):
        return getattr(self.real, name)

    def __next__(self):
        previous, self.previous = self.previous, None
        if previous is not None and previous.HasField("message"):
            payload = support.parse_message(previous.message).payload
            with self.owner.condition:
                self.owner.processed_messages[payload.kind, payload.stream_id] += 1
                self.owner.condition.notify_all()
        response = next(self.real)
        self.previous = response
        return response


class FailReconnect:
    """Cancel a real stream and make the real server reject subsequent Subscribe."""

    RPC_NAMES = ("get_uuid", "publish_auto_seq", "get_seq_by_uuid", "get_status", "subscribe")

    def __init__(self, real):
        self.real = real
        self.condition = threading.Condition()
        self.calls = Counter({name: 0 for name in self.RPC_NAMES})
        self.processed_messages = Counter()
        self.reject_reconnect = False
        self.stream = None

    def __getattr__(self, name):
        operation = getattr(self.real, name)
        if name not in self.RPC_NAMES:
            return operation

        def counted(*args, **kwargs):
            with self.condition:
                self.calls[name] += 1
            return operation(*args, **kwargs)

        return counted

    def subscribe(self, **kwargs):
        with self.condition:
            self.calls["subscribe"] += 1
            reject = self.reject_reconnect
        if reject:
            kwargs["token"] = "fault-injection-invalid-token"
        stream = self.real.subscribe(**kwargs)
        if reject:
            return stream
        with self.condition:
            self.stream = stream
        return ObserveProcessedMessages(stream, self)

    def wait_for_appends(self, stream_id, count):
        return self.wait_for_messages("infer.append", stream_id, count)

    def wait_for_messages(self, kind, stream_id, count=1):
        with self.condition:
            return self.condition.wait_for(
                lambda: self.processed_messages[kind, stream_id] >= count, timeout=3,
            )

    def disconnect(self):
        with self.condition:
            self.reject_reconnect = True
            stream = self.stream
        if stream is None:
            raise AssertionError("No real Subscribe was established")
        stream.cancel()
        stream.code()

    def snapshot(self):
        with self.condition:
            return dict(self.calls)


class DelayPublishedReply(FailReconnect):
    """Hold a real successful Publish reply while subscription keeps receiving."""

    def __init__(self, real):
        super().__init__(real)
        self.committed = threading.Event()
        self.release = threading.Event()
        self.committed_seq = None

    def publish_auto_seq(self, **kwargs):
        with self.condition:
            self.calls["publish_auto_seq"] += 1
        reply = self.real.publish_auto_seq(**kwargs)
        self.committed_seq = reply.seq
        self.committed.set()
        if not self.release.wait(5):
            raise TimeoutError("Test did not release Publish reply gate")
        return reply


def background(operation):
    results = queue.Queue()

    def run():
        try:
            results.put((True, operation()))
        except BaseException as exc:
            results.put((False, exc))

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, results


def result_within(results, timeout=4):
    try:
        success, result = results.get(timeout=timeout)
    except queue.Empty as exc:
        raise AssertionError("Operation exceeded its bounded test deadline") from exc
    if not success:
        raise result
    return result


@unittest.skipUnless(os.environ.get("OPENEVENT_RUN_E2E") == "1", "run with make e2e")
class FaultEndToEndTests(unittest.TestCase):
    def assertSubscriptionError(self, actual, expected):
        self.assertIsNot(actual, expected)
        self.assertIs(type(actual), type(expected))
        self.assertEqual(actual.args, expected.args)
        self.assertEqual((actual.reason, actual.last_status), (expected.reason, expected.last_status))
        self.assertIsNone(actual.protocol_error)
        self.assertIsNone(expected.protocol_error)

    # Delegate fixture/helpers without inheriting and rerunning its ordinary tests.
    @classmethod
    def setUpClass(cls):
        support.EndToEndTests.setUpClass.__func__(cls)

    @classmethod
    def stop_http(cls):
        support.EndToEndTests.stop_http.__func__(cls)

    setUp = support.EndToEndTests.setUp
    cleanup = support.EndToEndTests.cleanup
    start_worker = support.EndToEndTests.start_worker
    openai = support.EndToEndTests.openai
    messages = support.EndToEndTests.messages
    request = support.EndToEndTests.request

    def test_lost_committed_publish_response_reconciles_same_uuid(self):
        transport = LoseCommittedResponse(self.transport)
        protocol = create_client(transport, self.caller_token, max_retries=1, retry_interval_ms=10)
        _, results = background(lambda: publish_infer_request(
            protocol, self.channel, self.caller,
            InferRequestInput(stream_id="lost-reply", method="POST", path="/v1/responses", body={}),
        ))
        seq = result_within(results)
        self.assertEqual(seq, transport.committed_seq)
        self.assertEqual(len(transport.publish_calls), 2)
        self.assertEqual(transport.publish_calls[0], transport.publish_calls[1])
        self.assertEqual(transport.duplicate_statuses, [grpc.StatusCode.ALREADY_EXISTS])
        self.assertEqual(transport.query_calls, [transport.publish_calls[0]["uuid"]])
        messages = self.messages()
        self.assertEqual(len(messages), 1)
        self.assertEqual((messages[0].seq, messages[0].uuid), (seq, transport.query_calls[0]))
        self.assertEqual(messages[0].payload.stream_id, "lost-reply")

    def test_publication_returns_before_subscribe_is_created_and_replays_outputs(self):
        self.start_worker()
        real = OpenEventClient(self.event_addr, timeout_ms=1000)
        delayed = DelaySubscribe(real)
        # Cleanup must unblock a failed assertion before closing the client/reader.
        self.addCleanup(delayed.release.set)
        with patch("openevent.model_proxy_sdk.openai.OpenEventClient", return_value=delayed):
            client = self.openai()
        _, results = background(lambda: client.responses.create(stream=True, stream_id="before-subscribe"))
        try:
            self.assertTrue(delayed.attempted.wait(2))
            stream = result_within(results, timeout=2)
            self.assertFalse(delayed.connected.is_set())
            self.assertIsInstance(stream.request_seq, int)
            self.assertEqual(stream.stream_id, "before-subscribe")
            self.assertIsNone(stream.result_openevent_seq)
            messages = support.eventually(lambda: self.messages() if
                any(m.payload.kind == "infer.end" for m in self.messages()) else None, timeout=3)
            self.assertEqual([m.payload.kind for m in messages],
                             ["infer.request", "infer.result", "infer.append", "infer.append", "infer.end"])
            self.assertEqual(messages[0].seq, stream.request_seq)
            self.assertFalse(delayed.connected.is_set())
        finally:
            delayed.release.set()
        _, chunks = background(lambda: [part.body["delta"] for part in stream])
        self.assertEqual(result_within(chunks), ["hello", "world"])
        self.assertTrue(delayed.connected.is_set())
        self.assertEqual(len(delayed.anchors), 1)
        self.assertLess(delayed.anchors[0], stream.request_seq)
        self.assertEqual(stream.terminal_body["type"], "response.completed")

    def test_failed_subscription_closes_locally_and_preserves_accepted_outputs(self):
        self.start_worker()
        real = OpenEventClient(self.event_addr, timeout_ms=1000)
        self.addCleanup(real.close)
        transport = FailReconnect(real)
        errors, failed = [], threading.Event()

        def on_error(error):
            errors.append(error)
            failed.set()

        with patch("openevent.model_proxy_sdk.openai.OpenEventClient", return_value=transport):
            client = self.openai(on_subscription_error=on_error)
        completed = client.responses.create(stream=True, stream_id="completed-before-failure")
        support.eventually(lambda: completed.terminal_has_body is True, timeout=3)
        held = client.responses.create(stream=True, stream_id="held-before-failure", test_mode="hold")
        self.assertTrue(transport.wait_for_appends(held.stream_id, 2))
        waiting = client.responses.create(stream=True, stream_id="waiting-before-failure")
        self.assertIsNone(waiting.result_openevent_seq)
        self.assertIsNone(held.terminal_has_body)
        held_metadata = (held.result_openevent_seq, held.response_status_code, held.response_headers)
        completed_body = completed.terminal_body

        transport.disconnect()
        self.assertTrue(failed.wait(3), "Real Subscribe rejection did not reach final FAILED")
        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertIsInstance(error, OpenEventSubscriptionError)
        self.assertEqual((error.reason, error.last_status), ("rpc", "UNAUTHENTICATED"))
        before_close = transport.snapshot()
        self.assertEqual(before_close, {
            "get_uuid": 3, "publish_auto_seq": 3, "get_seq_by_uuid": 0,
            "get_status": 1, "subscribe": 2,
        })
        with self.assertRaises(OpenEventSubscriptionError) as caught:
            client.responses.create(stream=True, stream_id="rejected-after-failure")
        self.assertSubscriptionError(caught.exception, error)

        _, closed_stream = background(held.close)
        self.assertIsNone(result_within(closed_stream))
        self.assertEqual(transport.snapshot(), before_close)
        _, closed_client = background(client.close)
        self.assertIsNone(result_within(closed_client))
        for stream in (completed, held, waiting):
            stream.close()
        client.close()
        self.assertEqual(transport.snapshot(), before_close)

        self.assertEqual([chunk.body["delta"] for chunk in completed], ["hello", "world"])
        self.assertEqual(completed.terminal_body, completed_body)
        self.assertEqual([next(held).body["delta"] for _ in range(2)], ["hello", "world"])
        for stream in (held, waiting):
            with self.assertRaises(OpenEventSubscriptionError) as caught:
                next(stream)
            self.assertSubscriptionError(caught.exception, error)
        self.assertEqual((held.result_openevent_seq, held.response_status_code, held.response_headers),
                         held_metadata)
        self.assertIsNone(held.terminal_has_body)
        self.assertEqual(errors, [error])
        self.assertEqual(transport.snapshot(), before_close)
        self.assertNotIn("infer.cancel", [message.payload.kind for message in self.messages()])

    def test_client_close_cancels_real_subscription_and_preserves_received_outputs(self):
        self.start_worker()
        real = OpenEventClient(self.event_addr, timeout_ms=1000)
        self.addCleanup(real.close)
        transport = FailReconnect(real)
        errors = []
        callback_entered, callback_release, callback_done = (
            threading.Event(), threading.Event(), threading.Event(),
        )
        self.addCleanup(callback_release.set)

        def on_error(error):
            errors.append(error)
            callback_entered.set()
            try:
                if not callback_release.wait(5):
                    raise TimeoutError("Test did not release subscription callback gate")
            finally:
                callback_done.set()

        with patch("openevent.model_proxy_sdk.openai.OpenEventClient", return_value=transport):
            client = self.openai(on_subscription_error=on_error)
        completed = client.responses.create(stream=True, stream_id="completed-before-close")
        self.assertTrue(transport.wait_for_messages("infer.end", completed.stream_id))
        held = client.responses.create(stream=True, stream_id="held-before-close", test_mode="hold")
        self.assertTrue(transport.wait_for_appends(held.stream_id, 2))
        waiting = client.responses.create(stream=True, stream_id="waiting-before-close")
        self.assertIsNone(waiting.result_openevent_seq)
        _, waiting_result = background(lambda: next(waiting))
        held_metadata = (held.result_openevent_seq, held.response_status_code, held.response_headers)
        completed_body = completed.terminal_body
        active_subscribe = transport.stream

        _, close_result = background(client.close)
        self.assertIsNone(result_within(close_result, timeout=2))
        self.assertEqual(active_subscribe.code(), grpc.StatusCode.CANCELLED)
        self.assertTrue(callback_entered.wait(3))
        self.assertFalse(callback_done.is_set(), "close waited for the subscription callback")
        self.assertEqual(len(errors), 1)
        error = errors[0]
        self.assertIsInstance(error, OpenEventSubscriptionError)
        self.assertEqual(error.reason, "rpc")
        with self.assertRaises(OpenEventSubscriptionError) as caught:
            result_within(waiting_result)
        self.assertSubscriptionError(caught.exception, error)
        self.assertEqual([chunk.body["delta"] for chunk in completed], ["hello", "world"])
        self.assertEqual(completed.terminal_body, completed_body)
        self.assertEqual([next(held).body["delta"] for _ in range(2)], ["hello", "world"])
        with self.assertRaises(OpenEventSubscriptionError) as caught:
            next(held)
        self.assertSubscriptionError(caught.exception, error)
        self.assertEqual((held.result_openevent_seq, held.response_status_code, held.response_headers),
                         held_metadata)
        self.assertIsNone(held.terminal_has_body)
        self.assertNotIn("infer.cancel", [message.payload.kind for message in self.messages()])
        callback_release.set()
        self.assertTrue(callback_done.wait(2))

    def test_client_close_does_not_wait_for_published_reply(self):
        self.start_worker()
        real = OpenEventClient(self.event_addr, timeout_ms=1000)
        self.addCleanup(real.close)
        transport = DelayPublishedReply(real)
        self.addCleanup(transport.release.set)
        errors, failed = [], threading.Event()

        def on_error(error):
            errors.append(error)
            failed.set()

        with patch("openevent.model_proxy_sdk.openai.OpenEventClient", return_value=transport):
            client = self.openai(on_subscription_error=on_error)
        _, publication = background(
            lambda: client.responses.create(stream=True, stream_id="reply-after-close"),
        )
        self.assertTrue(transport.committed.wait(2))
        self.assertTrue(transport.wait_for_messages("infer.end", "reply-after-close"))
        self.assertTrue(publication.empty(), "Publish reply gate was not held")
        active_subscribe = transport.stream

        _, close_result = background(client.close)
        self.assertIsNone(result_within(close_result, timeout=2))
        self.assertFalse(transport.release.is_set())
        self.assertTrue(publication.empty(), "close waited for the Publish call")
        self.assertEqual(active_subscribe.code(), grpc.StatusCode.CANCELLED)
        self.assertTrue(failed.wait(3))
        self.assertIsInstance(errors[0], OpenEventSubscriptionError)
        transport.release.set()
        stream = result_within(publication)
        self.assertEqual(stream.request_seq, transport.committed_seq)
        self.assertEqual([chunk.body["delta"] for chunk in stream], ["hello", "world"])
        self.assertEqual(stream.terminal_body["type"], "response.completed")
        self.assertNotIn("infer.cancel", [message.payload.kind for message in self.messages()])

    def test_worker_exits_when_committed_output_seq_cannot_be_obtained(self):
        real = OpenEventClient(self.event_addr, timeout_ms=1000)
        transport = LoseCommittedResponse(real, lose_queries=True)
        self.addCleanup(real.close)
        config = parse_config({
            "protocol": "llm.v1", "open_event": {"addr": self.event_addr, "rpc_timeout_ms": 1000},
            "worker": {"max_concurrency": 1, "max_retries": 1, "retry_interval_ms": 10},
            "principal": self.proxy, "token": self.proxy_token, "channels": [self.channel],
            "max_payload_bytes": 8192, "default_provider": "fake",
            "providers": {"fake": {"type": "openai_compatible",
                "base_url": f"http://127.0.0.1:{self.http.server_port}", "api_key": "test-key",
                "timeout": {"response_header_ms": 2000, "idle_ms": 2000}}},
        })
        worker = Worker(config, client=transport)
        thread, errors = background(worker.run)

        def stop_worker():
            worker.stop()
            thread.join(3)
            self.assertFalse(thread.is_alive(), "Worker did not exit after stop")

        self.addCleanup(stop_worker)
        self.assertTrue(worker.ready.wait(3), "Worker did not establish its real Subscribe")
        _, request_result = background(lambda: self.request("committed-output"))
        request_seq = result_within(request_result)
        with self.assertRaises(WorkerFatalError) as caught:
            result_within(errors, timeout=3)
        thread.join(1)
        self.assertFalse(thread.is_alive())
        error = caught.exception.__cause__
        self.assertIsInstance(error, ResultPublishError)
        self.assertIs(error.commit_state, CommitState.COMMITTED)
        self.assertIsNone(error.committed_seq)
        self.assertEqual(error.last_status, "UNAVAILABLE")
        self.assertEqual(len(transport.publish_calls), 2)
        self.assertEqual(transport.publish_calls[0], transport.publish_calls[1])
        self.assertEqual(transport.query_calls, [error.event_uuid, error.event_uuid])
        self.assertEqual(transport.duplicate_statuses, [grpc.StatusCode.ALREADY_EXISTS])
        messages = self.messages()
        self.assertEqual([m.payload.kind for m in messages], ["infer.request", "infer.result"])
        self.assertEqual(messages[1].payload.prev_seq, request_seq)
        self.assertEqual(messages[1].payload.body["answer"], "ok")
        self.assertEqual(messages[1].uuid, error.event_uuid)
