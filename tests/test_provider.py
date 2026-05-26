from __future__ import annotations

import unittest
from email.message import Message

from openevent.model_proxy.config import ProviderConfig, TimeoutConfig
from openevent.model_proxy.provider import ProviderClient


class ProviderTests(unittest.TestCase):
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
