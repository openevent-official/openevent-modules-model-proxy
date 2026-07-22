from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from threading import Event, Lock
from unittest.mock import patch

from openevent.model_proxy.config import ModelProxyConfig, OpenEventConfig, ProviderConfig, TimeoutConfig, WorkerConfig
from openevent.model_proxy.worker import ModelProxyWorker
from openevent.model_proxy.provider import ProviderError
from openevent.model_proxy_sdk.codec import dumps_payload
from openevent.model_proxy_sdk.model import Header


@dataclass
class Message:
    seq: int
    channel_id: int
    principal: int
    payload: bytes
    recipients: list[int] = field(default_factory=list)


@dataclass
class PublishResp:
    seq: int


@dataclass
class FetchResp:
    messages: list[Message]
    next_seq: int
    last_seq: int


@dataclass
class Channel:
    protocol: str = "llm.v1"
    visibility: int = 2
    members: list[int] = field(default_factory=lambda: [20001, 10])
    description: str = '{"version":"v1","updated_at_ms":1710000000000}'


@dataclass
class ChannelResp:
    channel: Channel


class FakeOpenEvent:
    def __init__(self, messages=None):
        self.published = []
        self.messages = list(messages or [])
        self.fetch_calls = []

    def get_channel(self, principal, token, channel_id, timeout=None):
        return ChannelResp(Channel())

    def publish_auto_seq(self, principal, token, channel_id, payload, recipients, timeout=None):
        self.published.append((channel_id, payload, tuple(recipients)))
        return PublishResp(seq=100 + len(self.published))

    def fetch(self, principal, token, from_seq, limit, only_my_recipient=False, channels=(), timeout=None):
        self.fetch_calls.append((from_seq, limit, only_my_recipient, tuple(channels)))
        matches = [message for message in self.messages if message.seq >= from_seq]
        if channels:
            requested_channels = {int(channel) for channel in channels}
            matches = [message for message in matches if int(message.channel_id) in requested_channels]
        batch = matches[:limit]
        last_seq = max((message.seq for message in self.messages), default=0)
        next_seq = batch[-1].seq + 1 if len(matches) > len(batch) else last_seq + 1
        return FetchResp(messages=batch, next_seq=next_seq, last_seq=last_seq)


def _config():
    return ModelProxyConfig(
        protocol="llm.v1",
        open_event=OpenEventConfig("addr"),
        worker=WorkerConfig(max_concurrency=2),
        principal=20001,
        token="t",
        channels=(1,),
        max_payload_bytes=16 * 1024,
        default_provider="main",
        providers={
            "main": ProviderConfig(
                name="main",
                type="openai_compatible",
                base_url="https://example.test",
                api_key="k",
                timeout=TimeoutConfig(1),
            )
        },
    )


def _request(seq, request_id, method="POST", path="/v1/chat/completions"):
    payload = dumps_payload(
        {
            "kind": "infer.request",
            "request_id": request_id,
            "method": method,
            "path": path,
            "ts_ms": 1710000000000,
            "body": {},
        }
    )
    return Message(seq=seq, channel_id=1, principal=10, payload=payload)


def _result(seq, request_id, prev_seq, status_code=200):
    payload = dumps_payload(
        {
            "kind": "infer.result",
            "request_id": request_id,
            "prev_seq": prev_seq,
            "ts_ms": 1710000000001,
            "status_code": status_code,
            "body": {},
        }
    )
    return Message(seq=seq, channel_id=1, principal=20001, recipients=[10], payload=payload)


class WorkerTests(unittest.TestCase):
    def test_disallowed_provider_request_publishes_60010(self):
        event = FakeOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        item = worker._observe_message(
            _request(1, "req_denied", method="DELETE", path="/v1/files"),
        )

        worker._process_original(item)

        self.assertEqual(len(event.published), 1)
        self.assertIn(b'"status_code":60010', event.published[0][1])
        self.assertIn(b'"code":"REQUEST_NOT_ALLOWED"', event.published[0][1])

    def test_duplicate_request_publishes_rejection(self):
        event = FakeOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        first = worker._observe_message(_request(1, "req_a"))
        self.assertIsNotNone(first)
        duplicate = worker._observe_message(_request(2, "req_a"))
        self.assertIsNotNone(duplicate)
        worker._run_task(duplicate)
        self.assertEqual(len(event.published), 1)
        self.assertIn(b'"status_code":60005', event.published[0][1])

    def test_invalid_request_with_valid_request_id_gets_60009(self):
        event = FakeOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        bad = dumps_payload(
            {
                "kind": "infer.request",
                "request_id": "req_bad",
                "method": "POST",
                "path": "/v1/chat/completions",
                "ts_ms": 1710000000000,
                "body": {},
            }
        ).replace(b'"method":"POST"', b'"method":"NOPE"')
        task = worker._observe_message(Message(seq=3, channel_id=1, principal=10, payload=bad))
        worker._run_task(task)
        self.assertEqual(len(event.published), 1)
        self.assertIn(b'"status_code":60009', event.published[0][1])

    def test_recovery_does_not_reject_original_already_in_store(self):
        event = FakeOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        item = worker._observe_message(_request(1, "req_a"))
        self.assertIsNotNone(item)
        same = worker._observe_message(_request(1, "req_a"))
        self.assertIsNotNone(same)
        self.assertEqual(len(event.published), 0)

    def test_recover_uses_next_seq_without_has_more(self):
        event = FakeOpenEvent(messages=[_request(1, "req_a")])
        worker = ModelProxyWorker(_config(), event)

        pending = worker.recover(1)

        self.assertEqual([item.seq for item in pending], [1])
        self.assertEqual(event.fetch_calls, [(1, 1000, False, (1,))])

    def test_unconfigured_channel_is_ignored_without_get_channel(self):
        class TrackingOpenEvent(FakeOpenEvent):
            def __init__(self):
                super().__init__()
                self.get_channel_calls = 0

            def get_channel(self, principal, token, channel_id, timeout=None):
                self.get_channel_calls += 1
                return super().get_channel(principal, token, channel_id, timeout)

        event = TrackingOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        message = _request(1, "req_other")
        message.channel_id = 2

        self.assertIsNone(worker._observe_message(message))
        self.assertEqual(event.get_channel_calls, 0)

    def test_recovery_does_not_duplicate_existing_duplicate_rejection(self):
        event = FakeOpenEvent()
        worker = ModelProxyWorker(_config(), event)
        worker._observe_message(_request(1, "req_a"))
        deferred = worker._observe_message(_request(2, "req_a"))
        worker._observe_message(_result(3, "req_a", prev_seq=2, status_code=60005))
        worker._run_task(deferred)
        self.assertEqual(len(event.published), 0)

    def test_response_header_filtering_drops_unimportant_headers_by_default(self):
        worker = ModelProxyWorker(_config(), FakeOpenEvent())
        headers = [
            Header("content-type", "application/json"),
            Header("date", "Sat, 16 May 2026 00:00:00 GMT"),
            Header("server", "nginx"),
            Header("x-request-id", "req-1"),
            Header("x-ratelimit-remaining-requests", "99"),
        ]

        filtered = worker._response_headers_for_result(headers)

        self.assertEqual(
            filtered,
            [
                Header("content-type", "application/json"),
                Header("x-request-id", "req-1"),
                Header("x-ratelimit-remaining-requests", "99"),
            ],
        )

    def test_get_channel_failure_is_fatal(self):
        class BrokenGetChannel(FakeOpenEvent):
            def get_channel(self, principal, token, channel_id, timeout=None):
                raise OSError("channel service unavailable")

        worker = ModelProxyWorker(_config(), BrokenGetChannel())
        with self.assertRaises(OSError):
            worker._observe_message(_request(1, "req_a"))

    def test_invalid_channel_is_logged_and_ignored(self):
        class PublicChannelOpenEvent(FakeOpenEvent):
            def get_channel(self, principal, token, channel_id, timeout=None):
                return ChannelResp(Channel(visibility=0))

        worker = ModelProxyWorker(_config(), PublicChannelOpenEvent())
        with patch("openevent.model_proxy.channel_resolver.LOG.warning") as warning:
            item = worker._observe_message(_request(1, "req_a"))

        self.assertIsNone(item)
        warning.assert_called_once()
        self.assertEqual(warning.call_args.kwargs["extra"]["channel_id"], 1)
        self.assertEqual(warning.call_args.kwargs["extra"]["reason"], "public_visibility")

    def test_request_tasks_run_concurrently_up_to_configured_limit(self):
        worker = ModelProxyWorker(_config(), FakeOpenEvent())
        both_started = Event()
        release = Event()
        lock = Lock()
        active = 0
        peak = 0

        def call(method, path, body):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
                if active == 2:
                    both_started.set()
            release.wait(1)
            with lock:
                active -= 1
            return ProviderError(60003, "connection failed")

        worker.provider.call = call
        worker._submit(worker._observe_message(_request(1, "req_a")))
        worker._submit(worker._observe_message(_request(2, "req_b")))
        self.assertTrue(both_started.wait(1))
        self.assertEqual(peak, 2)
        release.set()
        worker._shutdown()


if __name__ == "__main__":
    unittest.main()
