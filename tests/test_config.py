from __future__ import annotations

import unittest

from openevent.model_proxy.config import parse_config


class ConfigTests(unittest.TestCase):
    def test_parse_config_defaults(self):
        config = parse_config(
            {
                "protocol": "llm.v1",
                "open_event": {"addr": "127.0.0.1:9527"},
                "principal": 1,
                "token": "t",
                "default_provider": "main",
                "providers": {
                    "main": {
                        "type": "openai_compatible",
                        "base_url": "https://example.test/",
                        "api_key": "k",
                        "timeout": {"total_ms": 3},
                    }
                },
            }
        )
        self.assertEqual(config.max_payload_bytes, 16 * 1024 * 1024)
        self.assertTrue(config.filter_response_headers)
        self.assertEqual(config.providers["main"].base_url, "https://example.test")

    def test_parse_config_can_disable_response_header_filtering(self):
        config = parse_config(
            {
                "protocol": "llm.v1",
                "open_event": {"addr": "127.0.0.1:9527"},
                "principal": 1,
                "token": "t",
                "filter_response_headers": False,
                "default_provider": "main",
                "providers": {
                    "main": {
                        "type": "openai_compatible",
                        "base_url": "https://example.test/",
                        "api_key": "k",
                        "timeout": {"total_ms": 3},
                    }
                },
            }
        )

        self.assertFalse(config.filter_response_headers)


if __name__ == "__main__":
    unittest.main()
