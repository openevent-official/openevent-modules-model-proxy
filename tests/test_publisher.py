from __future__ import annotations

import unittest
from dataclasses import dataclass

from openevent.model_proxy.publisher import ResultPublisher
from openevent.model_proxy_sdk.client import ModelProxyProtocolClient
from openevent.model_proxy_sdk.model import InferResultInput


@dataclass
class Resp:
    seq: int


class FakeOpenEvent:
    def __init__(self):
        self.payloads = []

    def publish_auto_seq(self, principal, token, channel_id, payload, recipients):
        self.payloads.append(payload)
        return Resp(seq=len(self.payloads))


class PublisherTests(unittest.TestCase):
    def test_result_too_large_rewritten_to_60008(self):
        event = FakeOpenEvent()
        publisher = ResultPublisher(ModelProxyProtocolClient(event, "t"), principal=1, max_payload_bytes=260)
        result = InferResultInput(
            request_id="req_a",
            prev_seq=7,
            status_code=200,
            body={"text": "x" * 1000},
        )
        seq, published = publisher.publish(channel_id=3, request_principal=4, result=result)
        self.assertEqual(seq, 1)
        self.assertEqual(published.status_code, 60008)
        payload = event.payloads[0].decode("utf-8")
        self.assertIn('"status_code":60008', payload)
        self.assertIn('"prev_seq":7', payload)


if __name__ == "__main__":
    unittest.main()
