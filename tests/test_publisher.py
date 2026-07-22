from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from unittest.mock import patch

from openevent.model_proxy.publisher import ResultPublishFatal, ResultPublisher
from openevent.model_proxy_sdk.client import ModelProxyProtocolClient
from openevent.model_proxy_sdk.model import InferResultInput


@dataclass
class Resp:
    seq: int


@dataclass
class StatusResp:
    max_seq: int


@dataclass
class FetchResp:
    messages: list
    next_seq: int


@dataclass
class Message:
    seq: int
    channel_id: int
    principal: int
    payload: bytes
    recipients: list[int] = field(default_factory=list)


class FakeOpenEvent:
    def __init__(self):
        self.payloads = []
        self.messages = []
        self.next_seq = 10

    def publish_auto_seq(self, principal, token, channel_id, payload, recipients, timeout=None):
        seq = self.next_seq
        self.next_seq += 1
        self.payloads.append(payload)
        self.messages.append(Message(seq, channel_id, principal, payload, list(recipients)))
        return Resp(seq=seq)

    def get_status(self, principal, token, timeout=None):
        return StatusResp(max((message.seq for message in self.messages), default=self.next_seq - 1))

    def fetch(self, principal, token, from_seq, limit, only_my_recipient=False, channels=(), timeout=None):
        messages = [message for message in self.messages if from_seq <= message.seq and message.channel_id in channels]
        return FetchResp(messages[:limit], self.get_status(principal, token).max_seq + 1)


def _publisher(event, max_payload_bytes=4096):
    return ResultPublisher(
        ModelProxyProtocolClient(event, "t"), principal=1, max_payload_bytes=max_payload_bytes, rpc_timeout_s=3
    )


def _result(body=None):
    return InferResultInput(request_id="req_a", prev_seq=7, status_code=200, body=body or {"ok": True})


class PublisherTests(unittest.TestCase):
    def test_result_too_large_rewritten_to_60008(self):
        event = FakeOpenEvent()
        seq, published = _publisher(event, 260).publish(
            channel_id=3, request_principal=4, result=_result({"text": "x" * 1000})
        )
        self.assertEqual(seq, 10)
        self.assertEqual(published.status_code, 60008)
        self.assertIn(b'"status_code":60008', event.payloads[0])

    @patch("openevent.model_proxy_sdk.result_publishing.time.sleep", return_value=None)
    def test_uncertain_publish_recovers_committed_result(self, _sleep):
        class CommitThenFail(FakeOpenEvent):
            def publish_auto_seq(self, *args, **kwargs):
                super().publish_auto_seq(*args, **kwargs)
                raise OSError("response lost")

        event = CommitThenFail()
        seq, _ = _publisher(event).publish(3, 4, _result())
        self.assertEqual(seq, 10)
        self.assertEqual(len(event.payloads), 1)

    @patch("openevent.model_proxy_sdk.result_publishing.time.sleep", return_value=None)
    def test_uncertain_publish_retries_same_frozen_payload_after_absence(self, _sleep):
        class FailBeforeCommit(FakeOpenEvent):
            def __init__(self):
                super().__init__()
                self.calls = 0
                self.attempted_payloads = []

            def publish_auto_seq(self, *args, **kwargs):
                self.calls += 1
                self.attempted_payloads.append(kwargs["payload"])
                if self.calls == 1:
                    raise OSError("not committed")
                return super().publish_auto_seq(*args, **kwargs)

        event = FailBeforeCommit()
        seq, _ = _publisher(event).publish(3, 4, _result())
        self.assertEqual(seq, 10)
        self.assertEqual(event.calls, 2)
        self.assertEqual(event.attempted_payloads[0], event.attempted_payloads[1])

    @patch("openevent.model_proxy_sdk.result_publishing.time.sleep", return_value=None)
    def test_reconciliation_failure_is_bounded(self, _sleep):
        class BrokenReconciliation(FakeOpenEvent):
            def publish_auto_seq(self, *args, **kwargs):
                raise OSError("uncertain")

            def get_status(self, *args, **kwargs):
                raise OSError("status unavailable")

        with self.assertRaises(ResultPublishFatal):
            _publisher(BrokenReconciliation()).publish(3, 4, _result())

    def test_guaranteed_non_commit_fails_without_retry(self):
        class Code:
            name = "PERMISSION_DENIED"

        class RpcFailure(Exception):
            def code(self):
                return Code()

        class Denied(FakeOpenEvent):
            def publish_auto_seq(self, *args, **kwargs):
                raise RpcFailure("denied")

        event = Denied()
        with self.assertRaises(ResultPublishFatal):
            _publisher(event).publish(3, 4, _result())
        self.assertEqual(event.payloads, [])


if __name__ == "__main__":
    unittest.main()
