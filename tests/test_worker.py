from __future__ import annotations

import tempfile
import unittest
from dataclasses import dataclass, field
from pathlib import Path

from openevent.model_proxy.config import ModelProxyConfig, OpenEventConfig, ProviderConfig, TimeoutConfig
from openevent.model_proxy.worker import ModelProxyWorker
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
class Channel:
    protocol: str = "llm.v1"
    visibility: int = 2
    members: list[int] = field(default_factory=lambda: [20001, 10])
    description: str = '{"version":"v1","updated_at_ms":1710000000000}'


@dataclass
class ChannelResp:
    channel: Channel


class FakeOpenEvent:
    def __init__(self):
        self.published = []

    def get_channel(self, principal, token, channel_id):
        return ChannelResp(Channel())

    def publish_auto_seq(self, principal, token, channel_id, payload, recipients):
        self.published.append((channel_id, payload, tuple(recipients)))
        return PublishResp(seq=100 + len(self.published))


def _config(tmp_path):
    return ModelProxyConfig(
        protocol="llm.v1",
        open_event=OpenEventConfig("addr"),
        principal=20001,
        token="t",
        idempotency_dsn=f"sqlite:///{tmp_path / 'state.db'}",
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


def _config_without_header_filter(tmp_path):
    config = _config(tmp_path)
    return ModelProxyConfig(
        protocol=config.protocol,
        open_event=config.open_event,
        principal=config.principal,
        token=config.token,
        idempotency_dsn=config.idempotency_dsn,
        max_payload_bytes=config.max_payload_bytes,
        default_provider=config.default_provider,
        providers=config.providers,
        filter_response_headers=False,
    )


def _request(seq, request_id):
    payload = dumps_payload(
        {
            "kind": "infer.request",
            "request_id": request_id,
            "method": "POST",
            "path": "/v1/chat/completions",
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
    def test_duplicate_request_publishes_rejection(self):
        with tempfile.TemporaryDirectory() as td:
            event = FakeOpenEvent()
            worker = ModelProxyWorker(_config(Path(td)), event)
            first = worker._observe_message(_request(1, "req_a"), realtime=True)
            self.assertIsNotNone(first)
            duplicate = worker._observe_message(_request(2, "req_a"), realtime=True)
            self.assertIsNone(duplicate)
            self.assertEqual(len(event.published), 1)
            self.assertIn(b'"status_code":60005', event.published[0][1])

    def test_invalid_request_with_valid_request_id_gets_60009(self):
        with tempfile.TemporaryDirectory() as td:
            event = FakeOpenEvent()
            worker = ModelProxyWorker(_config(Path(td)), event)
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
            worker._observe_message(Message(seq=3, channel_id=1, principal=10, payload=bad), realtime=True)
            self.assertEqual(len(event.published), 1)
            self.assertIn(b'"status_code":60009', event.published[0][1])

    def test_recovery_does_not_reject_original_already_in_store(self):
        with tempfile.TemporaryDirectory() as td:
            event = FakeOpenEvent()
            worker = ModelProxyWorker(_config(Path(td)), event)
            item = worker._observe_message(_request(1, "req_a"), realtime=False, deferred_results=[])
            self.assertIsNotNone(item)
            same = worker._observe_message(_request(1, "req_a"), realtime=False, deferred_results=[])
            self.assertIsNotNone(same)
            self.assertEqual(len(event.published), 0)

    def test_recovery_does_not_duplicate_existing_duplicate_rejection(self):
        with tempfile.TemporaryDirectory() as td:
            event = FakeOpenEvent()
            worker = ModelProxyWorker(_config(Path(td)), event)
            deferred = []
            worker._observe_message(_request(1, "req_a"), realtime=False, deferred_results=deferred)
            worker._observe_message(_request(2, "req_a"), realtime=False, deferred_results=deferred)
            worker._observe_message(_result(3, "req_a", prev_seq=2, status_code=60005), realtime=False, deferred_results=deferred)
            for item in deferred:
                if not worker.store.has_result_for_request_seq(item.channel_id, item.result.prev_seq):
                    worker._publish_and_record(item.channel_id, item.request_principal, item.result, item.status)
            self.assertEqual(len(event.published), 0)

    def test_response_header_filtering_drops_unimportant_headers_by_default(self):
        with tempfile.TemporaryDirectory() as td:
            worker = ModelProxyWorker(_config(Path(td)), FakeOpenEvent())
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

    def test_response_header_filtering_can_be_disabled(self):
        with tempfile.TemporaryDirectory() as td:
            worker = ModelProxyWorker(_config_without_header_filter(Path(td)), FakeOpenEvent())
            headers = [Header("content-type", "application/json"), Header("server", "nginx")]

            self.assertEqual(worker._response_headers_for_result(headers), headers)


if __name__ == "__main__":
    unittest.main()
