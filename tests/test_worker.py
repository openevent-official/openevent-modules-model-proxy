import json
import queue
import threading
import time
from types import SimpleNamespace
import unittest
import weakref
from unittest.mock import patch

import grpc
from openevent.sdk import openevent_pb2 as pb

from openevent.model_proxy.config import Config, OpenEventConfig, ProviderConfig, ProviderTimeout, WorkerConfig
from openevent.model_proxy.provider import ProviderCancelled, ProviderEvent
from openevent.model_proxy.worker import Worker, WorkerFatalError
from openevent.model_proxy_sdk import (
    InferRequestInput, InferResultInput, InferAppendInput, InferEndInput, InferCancelInput,
    PayloadValidationError, parse_message, parse_payload,
)


def config(*, limit=4096, concurrency=1, retries=0):
    return Config("llm.v1", OpenEventConfig("unused:1", 100),
                  WorkerConfig(concurrency, retries, 1), 2, "worker-token", (3,), limit,
                  "main", {"main": ProviderConfig("openai_compatible", "http://unused", "key",
                                                    ProviderTimeout(100))})


def event(seq, model, *, principal=7, channel=3):
    return pb.EventMessage(uuid=1000 + seq, seq=seq, channel_id=channel,
                           principal=principal, payload=model.to_payload(seq), ts_ms=seq + 10)


def request(seq, stream_id=None, *, stream=False, provider=None, body=None, channel=3):
    return event(seq, InferRequestInput(stream_id=stream_id or f"s{seq}", method="POST",
                                       path="/v1/responses", body={"stream": stream} if body is None else body,
                                       provider=provider), channel=channel)


class RpcError(Exception):
    def __init__(self, status):
        self.status = status

    def code(self):
        return getattr(grpc.StatusCode, self.status)


class Subscription:
    def __init__(self, messages=(), *, start_error=None):
        self.items = queue.Queue()
        for message in messages:
            self.items.put(message)
        self.start_error = start_error
        self.cancelled = threading.Event()
        self.finished = False
        self.started_timeouts = []

    def wait_started(self, timeout):
        self.started_timeouts.append(timeout)
        if self.start_error is not None:
            raise self.start_error

    def __iter__(self):
        return self

    def __next__(self):
        item = self.items.get(timeout=3)
        if item is None:
            raise StopIteration
        if isinstance(item, Exception):
            raise item
        return pb.SubscribeResponse(message=item)

    def cancel(self):
        if not self.cancelled.is_set():
            self.cancelled.set()
            self.items.put(None)

    def code(self):
        self.finished = True
        return grpc.StatusCode.CANCELLED


class OpenEvent:
    def __init__(self, *, history=(), target=None, pages=None, subscriptions=()):
        self.history = list(history)
        self.target = max((m.seq for m in history), default=0) if target is None else target
        self.pages = pages
        self.subscription_plans = list(subscriptions)
        self.fetch_calls = []
        self.subscribe_calls = []
        self.streams = []
        self.published = []
        self.lock = threading.Condition()
        self.uuid = 2000
        self.seq = max(self.target, 100)
        self.closed = False
        self.deliver_outputs = True
        self.publish_error = None
        self.status_error = None
        self.channel = pb.ChannelInfo(channel_id=3, protocol="llm.v1", visibility=1, members=[2, 7],
                                      description=json.dumps({"version": "v1", "updated_at_ms": 0, "metadata": {}}))

    def get_channel(self, **kwargs):
        return pb.GetChannelResponse(channel=self.channel)

    def get_status(self, **kwargs):
        if self.status_error is not None:
            raise self.status_error
        return pb.GetStatusResponse(min_seq=0, max_seq=self.target)

    def fetch(self, **kwargs):
        self.fetch_calls.append(kwargs)
        if self.pages is not None:
            page = self.pages[len(self.fetch_calls) - 1]
            if isinstance(page, Exception):
                raise page
            return page
        return pb.FetchResponse(messages=self.history, next_seq=self.target + 1, last_seq=self.target)

    def subscribe(self, **kwargs):
        with self.lock:
            if self.streams and not self.streams[-1].finished:
                raise AssertionError("Two Subscribe calls compete")
            self.subscribe_calls.append(kwargs)
            stream = self.subscription_plans.pop(0) if self.subscription_plans else Subscription()
            self.streams.append(stream)
            self.lock.notify_all()
            return stream

    def get_uuid(self):
        with self.lock:
            self.uuid += 1
            return self.uuid

    def publish_auto_seq(self, **kwargs):
        if self.publish_error is not None:
            raise self.publish_error
        with self.lock:
            self.seq += 1
            message = pb.EventMessage(uuid=kwargs["uuid"], seq=self.seq, channel_id=kwargs["channel_id"],
                                      principal=kwargs["principal"], recipients=kwargs["recipients"],
                                      object_keys=kwargs["object_keys"], payload=kwargs["payload"], ts_ms=999)
            self.published.append(message)
            if self.deliver_outputs and self.streams and not self.streams[-1].cancelled.is_set():
                self.streams[-1].items.put(message)
            self.lock.notify_all()
            return SimpleNamespace(seq=self.seq)

    def get_seq_by_uuid(self, uuid):
        with self.lock:
            return next(m.seq for m in self.published if m.uuid == uuid)

    def close(self):
        self.closed = True

    def wait_published(self, count):
        with self.lock:
            return self.lock.wait_for(lambda: len(self.published) >= count, timeout=2)


class Provider:
    def __init__(self, outputs=(), *, blocked=False):
        self.outputs = outputs
        self.blocked = blocked
        self.aborted = threading.Event()

    def __iter__(self):
        if self.blocked:
            self.aborted.wait(3)
            raise ProviderCancelled()
        yield from self.outputs

    def abort(self):
        self.aborted.set()


class ProviderFactory:
    def __init__(self, *, outputs=(), blocked=False):
        self.outputs = outputs
        self.blocked = blocked
        self.calls = []
        self.instances = []
        self.called = threading.Event()

    def __call__(self, *args):
        self.calls.append(args)
        provider = Provider(self.outputs, blocked=self.blocked)
        self.instances.append(provider)
        self.called.set()
        return provider


def wait_until(predicate, timeout=2):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        threading.Event().wait(0.005)
    return predicate()


class WorkerTests(unittest.TestCase):
    def start(self, worker):
        errors = []

        def run():
            try:
                worker.run()
            except Exception as exc:
                errors.append(exc)

        thread = threading.Thread(target=run, daemon=True)
        thread.start()

        def cleanup():
            worker.stop()
            thread.join(2)
            self.assertFalse(thread.is_alive(), "Worker failed to stop")

        self.addCleanup(cleanup)
        return thread, errors

    def test_recovery_uses_fixed_watermark_and_continues_short_pages(self):
        first = request(2)
        second = request(7)
        too_late = pb.EventMessage(uuid=1008, seq=8, channel_id=3, principal=7, payload=b"invalid")
        transport = OpenEvent(target=7, pages=[
            pb.FetchResponse(messages=[first], next_seq=5),
            pb.FetchResponse(messages=[], next_seq=7),
            pb.FetchResponse(messages=[second, too_late], next_seq=9),
        ])
        provider = ProviderFactory()
        worker = Worker(config(), client=transport, provider_factory=provider)
        worker.recover()
        self.assertEqual([call["from_seq"] for call in transport.fetch_calls], [1, 5, 7])
        self.assertTrue(all(call["channels"] == (3,) and not call["only_my_recipient"]
                            for call in transport.fetch_calls))
        outputs = [parse_payload(m.payload) for m in transport.published]
        self.assertEqual([(m.prev_seq, m.status_code) for m in outputs], [(2, 60007), (7, 60007)])
        self.assertEqual(worker.last_seen_seq, 7)
        self.assertEqual(provider.calls, [])

    def test_empty_history_uses_zero_anchor_without_fetch(self):
        transport = OpenEvent(subscriptions=[Subscription()])
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
        thread, errors = self.start(worker)
        self.assertTrue(worker.ready.wait(2))
        self.assertEqual(transport.fetch_calls, [])
        self.assertEqual(len(transport.subscribe_calls), 1)
        self.assertEqual(transport.subscribe_calls[0]["from_seq"], 0)
        self.assertEqual(transport.streams[0].started_timeouts, [100])
        self.assertEqual(errors, [])
        self.assertTrue(thread.is_alive())

    def test_history_must_fully_parse_before_recovery_outputs(self):
        invalid = pb.EventMessage(uuid=1002, seq=2, channel_id=3, principal=7, payload=b"not-json")
        transport = OpenEvent(history=[request(1), invalid])
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
        with self.assertRaises(PayloadValidationError):
            worker.recover()
        self.assertEqual(transport.published, [])
        self.assertEqual(transport.subscribe_calls, [])

    def test_nonadvancing_or_failed_history_never_publishes_partial_recovery(self):
        for page in (pb.FetchResponse(next_seq=2), pb.FetchResponse(next_seq=1), RpcError("PERMISSION_DENIED")):
            with self.subTest(page=page):
                transport = OpenEvent(target=3, pages=[pb.FetchResponse(messages=[request(1)], next_seq=2), page])
                worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
                with self.assertRaises((WorkerFatalError, RpcError)):
                    worker.recover()
                self.assertEqual(transport.published, [])

    def test_failed_initial_status_cannot_start_subscription_or_provider(self):
        transport = OpenEvent()
        transport.status_error = RpcError("PERMISSION_DENIED")
        factory = ProviderFactory()
        worker = Worker(config(), client=transport, provider_factory=factory)
        with self.assertRaises(WorkerFatalError):
            worker.run()
        self.assertEqual(transport.fetch_calls, [])
        self.assertEqual(transport.subscribe_calls, [])
        self.assertEqual(transport.published, [])
        self.assertEqual(factory.calls, [])

    def test_recovery_keeps_original_and_duplicate_outcomes_separate(self):
        history = [
            request(1, "ordinary"), request(2, "ordinary", stream=True),
            request(3, "stream", stream=True), request(4, "stream"),
            request(5, "missing-provider", stream=True, provider="removed"),
            request(6, "large", body={"input": "x" * 5000}),
        ]
        transport = OpenEvent(history=history)
        provider = ProviderFactory()
        worker = Worker(config(), client=transport, provider_factory=provider)
        worker.recover()
        outputs = [parse_payload(m.payload) for m in transport.published]
        self.assertEqual([(p.kind, p.prev_seq if p.kind == "infer.result" else p.request_seq, p.status_code)
                          for p in outputs], [
            ("infer.result", 1, 60007), ("infer.result", 2, 60007),
            ("infer.end", 3, 60007), ("infer.result", 4, 60007),
            ("infer.end", 5, 60007), ("infer.result", 6, 60007),
        ])
        self.assertTrue(all(m.recipients == [7] for m in transport.published))
        self.assertEqual(provider.calls, [])

    def test_existing_terminal_and_late_chain_messages_do_not_trigger_recovery(self):
        history = [
            request(1, "ordinary"),
            event(2, InferResultInput(stream_id="ordinary", prev_seq=1, status_code=200, body=None)),
            request(3, "stream", stream=True),
            event(4, InferResultInput(stream_id="stream", prev_seq=3, status_code=200)),
            event(5, InferAppendInput(stream_id="stream", request_seq=3, prev_seq=4, body={"part": 1})),
            event(6, InferCancelInput(stream_id="stream", request_seq=3)),
            event(7, InferAppendInput(stream_id="stream", request_seq=3, prev_seq=4, body={"late": True})),
            event(8, InferEndInput(stream_id="stream", request_seq=3, status_code=200, end_status="completed")),
            request(9, "stream", stream=True),
            event(10, InferResultInput(stream_id="stream", prev_seq=9, status_code=60005, body={"error": {}})),
        ]
        transport = OpenEvent(history=history)
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
        worker.recover()
        self.assertEqual(transport.published, [])
        self.assertEqual(worker.requests[3].terminal_seq, 6)
        self.assertEqual(worker.requests[3].chain_seq, 5)
        self.assertEqual(worker.requests[9].terminal_seq, 10)

    def test_recovery_retains_identity_and_terminal_state_without_request_bodies(self):
        history = [
            request(1, "done", body={"input": "x" * 32768}),
            event(2, InferResultInput(stream_id="done", prev_seq=1, status_code=200, body={})),
            request(3, "unfinished", body={"stream": True, "input": "y" * 32768}),
        ]
        parsed_requests = []

        def record_parse(message):
            parsed = parse_message(message)
            if parsed.payload.kind == "infer.request":
                parsed_requests.append(weakref.ref(parsed))
            return parsed

        transport = OpenEvent(history=history)
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
        with patch.object(worker, "_parse", side_effect=record_parse):
            worker.recover()
        self.assertTrue(all(reference() is None for reference in parsed_requests))
        self.assertTrue(all(state.body is None for state in worker.requests.values()))
        self.assertEqual(worker.originals, {(3, "done"): 1, (3, "unfinished"): 3})
        self.assertEqual(worker.requests[1].terminal_seq, 2)
        self.assertFalse(worker.requests[1].streaming)
        self.assertTrue(worker.requests[3].streaming)
        self.assertEqual(worker.requests[3].request_principal, 7)
        self.assertEqual(worker.requests[3].phase, "done")
        self.assertEqual(len(transport.published), 1)
        interrupted = parse_payload(transport.published[0].payload)
        self.assertEqual((interrupted.kind, interrupted.request_seq, interrupted.status_code),
                         ("infer.end", 3, 60007))

    def test_channel_validation_is_fatal_before_scan_or_provider(self):
        for change in (dict(protocol="other"), dict(visibility=0), dict(members=[7]),
                       dict(description='{"version":"v1","updated_at_ms":true,"metadata":{}}')):
            with self.subTest(change=change):
                transport = OpenEvent(history=[request(1)])
                channel = dict(channel_id=3, protocol="llm.v1", visibility=1, members=[2],
                               description='{"version":"v1","updated_at_ms":0,"metadata":{}}')
                channel.update(change)
                transport.channel = pb.ChannelInfo(**channel)
                worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
                with self.assertRaises(WorkerFatalError):
                    worker.recover()
                self.assertEqual(transport.fetch_calls, [])
                self.assertEqual(transport.published, [])

    def test_rejections_do_not_wait_for_provider_capacity(self):
        # One long stream owns the only provider slot. Refusals still publish.
        messages = [request(1, "busy", stream=True), request(2, "busy", stream=True),
                    request(3, "large", body={"stream": True, "input": "x" * 5000}),
                    request(4, "unknown", stream=True, provider="missing"), request(5, "large")]
        transport = OpenEvent(subscriptions=[Subscription(messages)])
        factory = ProviderFactory(blocked=True)
        worker = Worker(config(), client=transport, provider_factory=factory)
        thread, errors = self.start(worker)
        self.assertTrue(transport.wait_published(4))
        self.assertTrue(factory.called.wait(2))
        outputs = [parse_payload(m.payload) for m in transport.published]
        self.assertEqual([(p.kind, p.prev_seq, p.status_code, p.has_body) for p in outputs],
                         [("infer.result", 2, 60005, True), ("infer.result", 3, 60008, True),
                          ("infer.result", 4, 60009, True), ("infer.result", 5, 60005, True)])
        self.assertEqual(len(factory.calls), 1)
        self.assertTrue(all(worker.requests[seq].body is None for seq in (2, 3, 4, 5)))
        self.assertEqual(errors, [])
        self.assertTrue(thread.is_alive())

    def test_cancel_before_provider_starts_prevents_provider_and_output(self):
        # Block the first call before delivering the queued call and its cancellation.
        stream = Subscription([request(1, "busy", stream=True)])
        transport = OpenEvent(subscriptions=[stream])
        factory = ProviderFactory(blocked=True)
        worker = Worker(config(), client=transport, provider_factory=factory)
        _, errors = self.start(worker)
        self.assertTrue(factory.called.wait(2))
        stream.items.put(request(2, "queued", stream=True))
        stream.items.put(event(3, InferCancelInput(stream_id="queued", request_seq=2)))
        self.assertTrue(wait_until(lambda: worker.last_seen_seq == 3))
        self.assertIsNone(worker.requests[2].body)
        stream.items.put(event(4, InferCancelInput(stream_id="busy", request_seq=1)))
        self.assertTrue(factory.instances[0].aborted.wait(2))
        self.assertTrue(wait_until(lambda: worker.running == 0 and not worker.pending))
        self.assertEqual(len(factory.calls), 1)
        self.assertEqual(transport.published, [])
        self.assertEqual(errors, [])

    def test_queued_body_reaches_provider_then_leaves_worker_state(self):
        stream = Subscription([request(1, "busy", stream=True)])
        transport = OpenEvent(subscriptions=[stream])
        transport.deliver_outputs = False
        factory = ProviderFactory(blocked=True)
        worker = Worker(config(), client=transport, provider_factory=factory)
        _, errors = self.start(worker)
        self.assertTrue(factory.called.wait(2))
        queued_body = {"model": "test", "input": [{"content": "queued prompt"}], "stream": False}
        stream.items.put(request(2, "queued", body=queued_body))
        self.assertTrue(wait_until(lambda: worker.last_seen_seq == 2))
        self.assertEqual(worker.requests[2].body, queued_body)
        self.assertEqual(worker.requests[2].phase, "pending")
        self.assertEqual(len(factory.calls), 1)

        factory.blocked = False
        factory.outputs = [ProviderEvent("result", 200, body={"answer": "ok"}, has_body=True)]
        stream.items.put(event(3, InferCancelInput(stream_id="busy", request_seq=1)))
        self.assertTrue(transport.wait_published(1))
        self.assertTrue(wait_until(lambda: worker.requests[2].phase == "done"))
        self.assertEqual(len(factory.calls), 2)
        self.assertEqual(factory.calls[1][1], queued_body)
        self.assertEqual(factory.calls[1][2], "/v1/responses")
        self.assertIsNone(worker.requests[1].body)
        self.assertIsNone(worker.requests[2].body)
        self.assertIsNone(worker.requests[2].operation)
        # Output delivery is paused: the body is released even before the reader
        # learns that the successfully published result is terminal.
        self.assertIsNone(worker.requests[2].terminal_seq)
        self.assertEqual(parse_payload(transport.published[0].payload).body, {"answer": "ok"})
        self.assertEqual(errors, [])

    def test_stopping_releases_pending_request_body(self):
        worker = Worker(config(), client=OpenEvent(), provider_factory=ProviderFactory())
        worker._accept(parse_message(request(1, body={"input": "queued"})))
        self.assertEqual(worker.requests[1].body, {"input": "queued"})
        worker.stop()
        self.assertIsNone(worker.requests[1].body)

    def test_encoded_output_overflow_becomes_small_error(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                large_body = {"text": "x" * 4076}
                outputs = ([ProviderEvent("headers", 200), ProviderEvent("append", 200, body=large_body, has_body=True)]
                           if streaming else [ProviderEvent("result", 200, body=large_body, has_body=True)])
                transport = OpenEvent(subscriptions=[Subscription([request(1, stream=streaming)])])
                factory = ProviderFactory(outputs=outputs)
                worker = Worker(config(), client=transport, provider_factory=factory)
                _, errors = self.start(worker)
                self.assertTrue(transport.wait_published(2 if streaming else 1))
                parsed = [parse_payload(m.payload) for m in transport.published]
                self.assertEqual(parsed[-1].status_code, 60008)
                self.assertEqual(parsed[-1].kind, "infer.end" if streaming else "infer.result")
                self.assertEqual(parsed[-1].body["error"]["code"], "PAYLOAD_TOO_LARGE")
                self.assertTrue(all(len(m.payload) <= 4096 for m in transport.published))
                if streaming:
                    self.assertEqual(parsed[0].kind, "infer.result")
                    self.assertFalse(parsed[0].has_body)
                    self.assertEqual(parsed[-1].end_status, "interrupted")
                self.assertEqual(errors, [])
                worker.stop()

    def test_stream_chain_uses_publish_seq_without_waiting_for_reader(self):
        outputs = [ProviderEvent("headers", 200), ProviderEvent("append", 200, body={"part": 1}, has_body=True),
                   ProviderEvent("append", 200, body={"part": 2}, has_body=True),
                   ProviderEvent("completed", 200, body={"type": "response.completed"}, has_body=True)]
        transport = OpenEvent(subscriptions=[Subscription([request(1, stream=True)])])
        transport.deliver_outputs = False
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory(outputs=outputs))
        _, errors = self.start(worker)
        self.assertTrue(transport.wait_published(4))
        messages = transport.published
        parsed = [parse_payload(m.payload) for m in messages]
        self.assertEqual([p.kind for p in parsed], ["infer.result", "infer.append", "infer.append", "infer.end"])
        self.assertEqual(parsed[0].prev_seq, 1)
        self.assertEqual([parsed[1].prev_seq, parsed[2].prev_seq], [messages[0].seq, messages[1].seq])
        self.assertNotIn("prev_seq", parsed[3].to_dict())
        self.assertEqual(worker.last_seen_seq, 1)
        self.assertEqual(errors, [])

    def test_reconnect_uses_last_processed_seq_and_ignores_overlap(self):
        first = event(6, InferCancelInput(stream_id="unknown", request_seq=1))
        second = event(7, InferCancelInput(stream_id="unknown", request_seq=1))
        transport = OpenEvent(target=5, pages=[pb.FetchResponse(next_seq=6)], subscriptions=[
            Subscription([first, RpcError("UNAVAILABLE")]), Subscription([first, second]),
        ])
        worker = Worker(config(), client=transport, provider_factory=ProviderFactory())
        _, errors = self.start(worker)
        self.assertTrue(wait_until(lambda: worker.last_seen_seq == 7))
        self.assertEqual([c["from_seq"] for c in transport.subscribe_calls], [5, 6])
        self.assertTrue(transport.streams[0].finished)
        self.assertEqual(len(transport.fetch_calls), 1)
        self.assertEqual(errors, [])

    def test_subscription_acceptance_failure_is_retried_without_competing_streams(self):
        transport = OpenEvent(subscriptions=[Subscription(start_error=RpcError("UNAVAILABLE")), Subscription()])
        worker = Worker(config(retries=1), client=transport, provider_factory=ProviderFactory())
        _, errors = self.start(worker)
        self.assertTrue(worker.ready.wait(2))
        self.assertEqual([c["from_seq"] for c in transport.subscribe_calls], [0, 0])
        self.assertTrue(transport.streams[0].finished)
        self.assertEqual(errors, [])

    def test_permanent_subscribe_failure_and_invalid_payload_exit_worker(self):
        invalid = pb.EventMessage(uuid=1, seq=1, channel_id=3, principal=7, payload=b"{")
        for stream in (Subscription(start_error=RpcError("PERMISSION_DENIED")), Subscription([invalid])):
            with self.subTest(stream=stream):
                transport = OpenEvent(subscriptions=[stream])
                factory = ProviderFactory()
                worker = Worker(config(retries=3), client=transport, provider_factory=factory)
                with self.assertRaises(WorkerFatalError):
                    worker.run()
                self.assertEqual(len(transport.subscribe_calls), 1)
                self.assertEqual(transport.published, [])
                self.assertEqual(factory.calls, [])
                self.assertTrue(worker.stopped.is_set())
                self.assertTrue(stream.cancelled.is_set())

    def test_output_publish_failure_is_fatal_and_aborts_provider(self):
        transport = OpenEvent(subscriptions=[Subscription([request(1)])])
        transport.publish_error = RpcError("RESOURCE_EXHAUSTED")
        factory = ProviderFactory(outputs=[ProviderEvent("result", 200, body={}, has_body=True)])
        worker = Worker(config(), client=transport, provider_factory=factory)
        thread, errors = self.start(worker)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], WorkerFatalError)
        self.assertTrue(factory.instances[0].aborted.is_set())
        self.assertTrue(worker.stopped.is_set())
        self.assertTrue(transport.streams[0].cancelled.is_set())

    def test_provider_thread_start_failure_is_fatal(self):
        stream = Subscription([request(1)])
        transport = OpenEvent(subscriptions=[stream])
        factory = ProviderFactory()
        failure = RuntimeError("can't start new thread")
        failures, attempts = [], []
        fatal = threading.Event()

        def on_fatal(exc):
            failures.append((exc, worker.stopped.is_set(), stream.cancelled.is_set()))
            fatal.set()

        worker = Worker(config(), client=transport, provider_factory=factory, on_fatal=on_fatal)
        original_start = threading.Thread.start

        def start(thread):
            if thread.name == "model-proxy-provider":
                attempts.append(thread)
                raise failure
            return original_start(thread)

        with patch.object(threading.Thread, "start", start):
            thread, errors = self.start(worker)
            self.assertTrue(fatal.wait(2), "Provider thread startup failure must trigger fatal exit")
            stream.items.put(request(2))
            thread.join(2)

        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [(failure, True, True)])
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], WorkerFatalError)
        self.assertIs(errors[0].__cause__, failure)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(set(worker.requests), {1})
        self.assertEqual(factory.calls, [])
        self.assertEqual(transport.published, [])

    def test_fatal_subscription_errors_do_not_wait_for_stream_cleanup(self):
        class NoCleanupWait(Subscription):
            def code(self):
                raise AssertionError("Fatal exit must not wait for Subscribe completion")

        invalid = pb.EventMessage(uuid=1, seq=1, channel_id=3, principal=7, payload=b"{")
        for stream in (NoCleanupWait([invalid]),
                       NoCleanupWait([RpcError("PERMISSION_DENIED")]),
                       NoCleanupWait(start_error=RpcError("PERMISSION_DENIED"))):
            with self.subTest(stream=stream):
                failures = []
                worker = Worker(config(), client=OpenEvent(subscriptions=[stream]), on_fatal=failures.append)
                with self.assertRaises(WorkerFatalError):
                    worker.run()
                self.assertEqual(len(failures), 1)
                self.assertNotIsInstance(failures[0], AssertionError)
                self.assertTrue(stream.cancelled.is_set())

    def test_fatal_callback_runs_without_waiting_for_reader(self):
        reader_blocked, release_reader, fatal = threading.Event(), threading.Event(), threading.Event()

        class BlockedReader(Subscription):
            def __next__(self):
                if not self.items.empty():
                    return super().__next__()
                reader_blocked.set()
                release_reader.wait(3)
                raise StopIteration

        class DelayedProvider(Provider):
            def __iter__(self):
                if not reader_blocked.wait(2):
                    raise AssertionError("Reader did not begin waiting")
                yield ProviderEvent("result", 200, body={}, has_body=True)

        transport = OpenEvent(subscriptions=[BlockedReader([request(1)])])
        transport.publish_error = RpcError("RESOURCE_EXHAUSTED")
        worker = Worker(config(), client=transport, provider_factory=lambda *_: DelayedProvider(),
                        on_fatal=lambda _: fatal.set())
        thread, errors = self.start(worker)
        self.addCleanup(release_reader.set)
        self.assertTrue(fatal.wait(2))
        self.assertTrue(thread.is_alive(), "The test must keep the reader blocked through the fatal callback")
        release_reader.set()
        thread.join(2)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], WorkerFatalError)
