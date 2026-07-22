from __future__ import annotations

import unittest
from email.message import Message

from openevent.model_proxy.config import ProviderConfig, TimeoutConfig
from openevent.model_proxy.provider import ProviderClient


class ProviderTests(unittest.TestCase):
    def test_disallowed_request_is_rejected_before_http_call(self):
        config = ProviderConfig(
            name="p",
            type="openai_compatible",
            base_url="https://example.test",
            api_key="k",
            timeout=TimeoutConfig(total_ms=1),
        )

        for method, path in (
            ("GET", "/v1/chat/completions"),
            ("POST", "/v1/chat/completions/extra"),
        ):
            with self.subTest(method=method, path=path):
                result = ProviderClient(config).call(method, path, {})

                self.assertEqual(result.status_code, 60010)
                self.assertEqual(result.context, {"method": method, "path": path})

    def test_non_json_response_wrapped_as_base64(self):
        config = ProviderConfig(
            name="p",
            type="openai_compatible",
            base_url="https://example.test",
            api_key="k",
            timeout=TimeoutConfig(total_ms=1),
        )
        client = ProviderClient(config)
        headers = Message()
        headers["content-type"] = "text/plain"
        result = client._read_response(200, headers.items(), b"hello")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(
            result.body,
            {
                "non_json_body": {
                    "encoding": "base64",
                    "content_type": "text/plain",
                    "data": "aGVsbG8=",
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
