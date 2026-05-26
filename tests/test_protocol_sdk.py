from __future__ import annotations

import unittest

from openevent.model_proxy_sdk.codec import dict_to_model, dumps_payload, loads_payload
from openevent.model_proxy_sdk.errors import ModelProxySDKError
from openevent.model_proxy_sdk.model import InferRequestInput
from openevent.model_proxy_sdk.openevent_io import parse_payload


class ProtocolSDKTests(unittest.TestCase):
    def test_request_round_trip_with_prev_seq(self):
        payload = {
            "kind": "infer.request",
            "request_id": "req_abc-1",
            "prev_seq": 10,
            "method": "POST",
            "path": "/v1/chat/completions",
            "ts_ms": 1710000000000,
            "body": {"model": "x", "messages": []},
        }
        encoded = dumps_payload(payload)
        parsed = parse_payload(encoded)
        self.assertEqual(parsed.request_id, "req_abc-1")
        self.assertEqual(parsed.prev_seq, 10)

    def test_invalid_request_id_rejected(self):
        payload = {
            "kind": "infer.request",
            "request_id": "bad id",
            "method": "POST",
            "path": "/v1/chat/completions",
            "ts_ms": 1710000000000,
            "body": {},
        }
        with self.assertRaises(ModelProxySDKError) as ctx:
            dumps_payload(payload)
        self.assertEqual(ctx.exception.code, "INVALID_REQUEST_ID")

    def test_unknown_field_rejected(self):
        payload = {
            "kind": "infer.request",
            "request_id": "req_abc",
            "method": "POST",
            "path": "/v1/chat/completions",
            "ts_ms": 1710000000000,
            "body": {},
            "extra": True,
        }
        with self.assertRaises(ModelProxySDKError) as ctx:
            dumps_payload(payload)
        self.assertEqual(ctx.exception.code, "UNKNOWN_FIELD")


if __name__ == "__main__":
    unittest.main()
