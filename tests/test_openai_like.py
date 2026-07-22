from __future__ import annotations

import unittest
from dataclasses import dataclass, field

from openevent.model_proxy_sdk import (
    APIConnectionError,
    APIError,
    CompatibilityError,
    ConfigurationError,
    OpenAI,
    RateLimitError,
)
from openevent.model_proxy_sdk.codec import dumps_payload


@dataclass
class PublishResp:
    seq: int


@dataclass
class FetchResp:
    messages: list
    next_seq: int
    last_seq: int


@dataclass
class StatusResp:
    max_seq: int


@dataclass
class Message:
    seq: int
    channel_id: int
    principal: int
    payload: bytes
    recipients: list[int] = field(default_factory=list)


class FakeOpenEvent:
    def __init__(self):
        self.published = []
        self.results = []
        self.fetch_calls = []
        self.next_publish_seq = 10

    def get_status(self, principal, token, timeout=None):
        seqs = [item["seq"] for item in self.published]
        return StatusResp(max(seqs, default=self.next_publish_seq - 1))

    def publish_auto_seq(self, principal, token, channel_id, payload, recipients, timeout=None):
        seq = self.next_publish_seq
        self.next_publish_seq += 1
        self.published.append(
            {
                "seq": seq,
                "principal": principal,
                "token": token,
                "channel_id": channel_id,
                "payload": payload,
                "recipients": tuple(recipients),
            }
        )
        return PublishResp(seq)

    def fetch(self, principal, token, from_seq, limit, only_my_recipient=False, channels=(), timeout=None):
        self.fetch_calls.append(
            {
                "from_seq": from_seq,
                "limit": limit,
                "only_my_recipient": only_my_recipient,
                "channels": tuple(channels),
            }
        )
        published = [
            Message(
                seq=item["seq"],
                channel_id=item["channel_id"],
                principal=item["principal"],
                payload=item["payload"],
                recipients=list(item["recipients"]),
            )
            for item in self.published
        ]
        all_messages = sorted([*self.results, *published], key=lambda item: item.seq)
        matches = [item for item in all_messages if item.seq >= from_seq]
        if channels:
            requested_channels = {int(channel) for channel in channels}
            matches = [item for item in matches if int(item.channel_id) in requested_channels]
        messages = matches[:limit]
        last_seq = max((item.seq for item in all_messages), default=0)
        next_seq = messages[-1].seq + 1 if len(matches) > len(messages) else last_seq + 1
        return FetchResp(messages=messages, next_seq=next_seq, last_seq=last_seq)


def _result(seq, channel_id, request_id, prev_seq, status_code=200, body=None):
    payload = dumps_payload(
        {
            "kind": "infer.result",
            "request_id": request_id,
            "prev_seq": prev_seq,
            "ts_ms": 1710000000001,
            "status_code": status_code,
            "headers": [],
            "body": body if body is not None else {"choices": [{"message": {"content": "ok"}}]},
        }
    )
    return Message(seq=seq, channel_id=channel_id, principal=20001, payload=payload, recipients=[10])


class OpenAILikeTests(unittest.TestCase):
    def test_chat_completion_publishes_request_and_matches_result(self):
        event = FakeOpenEvent()
        event.results.extend(
            [
                _result(11, 99, "req_a", 10, body={"ignored": True}),
                _result(12, 1, "req_a", 9, body={"ignored": True}),
                _result(13, 1, "req_a", 10),
            ]
        )
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
        )

        response = client.chat.completions.create(
            model="m",
            messages=[{"role": "user", "content": "hello"}],
            request_id="req_a",
        )

        self.assertEqual(response.openevent_seq, 10)
        self.assertEqual(response.choices[0].message.content, "ok")
        self.assertEqual(event.published[0]["recipients"], ())
        self.assertEqual(event.fetch_calls[0]["channels"], (1,))
        self.assertIn(b'"/v1/chat/completions"', event.published[0]["payload"])

    def test_responses_create_publishes_responses_path(self):
        event = FakeOpenEvent()
        event.results.append(_result(11, 1, "req_b", 10, body={"output_text": "ok"}))
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
        )

        response = client.responses.create(model="m", input="hello", request_id="req_b")

        self.assertEqual(response.output_text, "ok")
        self.assertIn(b'"/v1/responses"', event.published[0]["payload"])

    def test_stream_true_rejected(self):
        client = OpenAI(
            openevent_client=FakeOpenEvent(),
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
        )
        with self.assertRaises(CompatibilityError):
            client.chat.completions.create(model="m", messages=[], stream=True)

    def test_api_key_initialization_rejected(self):
        with self.assertRaises(ConfigurationError):
            OpenAI(api_key="sk")

    def test_error_status_mapped(self):
        event = FakeOpenEvent()
        event.results.append(
            _result(
                11,
                1,
                "req_c",
                10,
                status_code=429,
                body={"error": {"message": "rate limited", "type": "rate_limit"}},
            )
        )
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
        )

        with self.assertRaises(RateLimitError) as ctx:
            client.chat.completions.create(model="m", messages=[], request_id="req_c")
        self.assertEqual(ctx.exception.status_code, 429)
        self.assertEqual(ctx.exception.request_id, "req_c")

    def test_result_wait_transport_failure_does_not_publish_again(self):
        class FailingFetchOpenEvent(FakeOpenEvent):
            def fetch(
                self, principal, token, from_seq, limit, only_my_recipient=False, channels=(), timeout=None
            ):
                raise OSError("temporary")

        event = FailingFetchOpenEvent()
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
            max_retries=1,
        )

        with self.assertRaises(APIConnectionError):
            client.responses.create(model="m", input="hello", request_id="req_fixed")
        self.assertEqual(len(event.published), 1)

        with self.assertRaises(APIConnectionError):
            client.responses.create(model="m", input="hello")
        self.assertEqual(len(event.published), 2)

    def test_uncertain_publish_recovers_committed_request_without_republish(self):
        class CommitThenFailOpenEvent(FakeOpenEvent):
            def publish_auto_seq(self, *args, **kwargs):
                super().publish_auto_seq(*args, **kwargs)
                raise OSError("response lost")

        event = CommitThenFailOpenEvent()
        event.results.append(_result(11, 1, "req_uncertain", 10))
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
            max_retries=1,
        )

        response = client.responses.create(model="m", input="hello", request_id="req_uncertain")

        self.assertEqual(response.openevent_seq, 10)
        self.assertEqual(len(event.published), 1)

    def test_uncertain_publish_retries_same_frozen_request_after_absence(self):
        class FailBeforeCommitOnceOpenEvent(FakeOpenEvent):
            def __init__(self):
                super().__init__()
                self.calls = 0

            def publish_auto_seq(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    raise OSError("connection lost before commit")
                return super().publish_auto_seq(*args, **kwargs)

        event = FailBeforeCommitOnceOpenEvent()
        event.results.append(_result(11, 1, "req_retry", 10))
        client = OpenAI(
            openevent_client=event,
            openevent_token="t",
            openevent_channel_id=1,
            openevent_principal=10,
            max_retries=1,
        )

        response = client.responses.create(model="m", input="hello", request_id="req_retry")

        self.assertEqual(response.openevent_seq, 10)
        self.assertEqual(event.calls, 2)
        self.assertEqual(len(event.published), 1)


if __name__ == "__main__":
    unittest.main()
