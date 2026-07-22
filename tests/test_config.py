from __future__ import annotations

import unittest

from openevent.model_proxy.config import ConfigError, parse_config


class ConfigTests(unittest.TestCase):
    def test_parse_config_defaults(self):
        config = parse_config(
            {
                "protocol": "llm.v1",
                "open_event": {"addr": "127.0.0.1:9527"},
                "principal": 1,
                "token": "t",
                "channels": [101, 102],
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
        self.assertEqual(config.worker.max_concurrency, 8)
        self.assertEqual(config.channels, (101, 102))
        self.assertEqual(config.providers["main"].base_url, "https://example.test")
        self.assertEqual(config.providers["main"].allowed_methods, frozenset({"POST"}))
        self.assertEqual(
            config.providers["main"].allowed_paths,
            frozenset({"/v1/chat/completions", "/v1/responses"}),
        )

    def test_removed_response_header_filter_option_is_rejected(self):
        with self.assertRaisesRegex(ConfigError, "fixed allowlist"):
            parse_config(
                {
                    "protocol": "llm.v1",
                    "open_event": {"addr": "127.0.0.1:9527"},
                    "principal": 1,
                    "token": "t",
                    "channels": [101],
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

    def test_parse_provider_request_allowlist(self):
        config = parse_config(
            {
                "protocol": "llm.v1",
                "open_event": {"addr": "127.0.0.1:9527"},
                "principal": 1,
                "token": "t",
                "channels": [101],
                "default_provider": "main",
                "providers": {
                    "main": {
                        "type": "openai_compatible",
                        "base_url": "https://example.test",
                        "api_key": "k",
                        "timeout": {"total_ms": 3},
                        "allowlist": {
                            "methods": ["GET", "POST"],
                            "paths": ["/v1/models", "/v1/responses"],
                        },
                    }
                },
            }
        )

        self.assertEqual(config.providers["main"].allowed_methods, frozenset({"GET", "POST"}))
        self.assertEqual(config.providers["main"].allowed_paths, frozenset({"/v1/models", "/v1/responses"}))

    def test_parse_provider_rejects_empty_allowlist(self):
        with self.assertRaisesRegex(ConfigError, "allowlist.paths must be a non-empty list"):
            parse_config(
                {
                    "protocol": "llm.v1",
                    "open_event": {"addr": "127.0.0.1:9527"},
                    "principal": 1,
                    "token": "t",
                    "channels": [101],
                    "default_provider": "main",
                    "providers": {
                        "main": {
                            "type": "openai_compatible",
                            "base_url": "https://example.test",
                            "api_key": "k",
                            "timeout": {"total_ms": 3},
                            "allowlist": {"paths": []},
                        }
                    },
                }
            )

    def test_channels_must_be_non_empty_unique_positive_integers(self):
        base = {
            "protocol": "llm.v1",
            "open_event": {"addr": "127.0.0.1:9527"},
            "principal": 1,
            "token": "t",
            "default_provider": "main",
            "providers": {
                "main": {
                    "type": "openai_compatible",
                    "base_url": "https://example.test",
                    "api_key": "k",
                    "timeout": {"total_ms": 3},
                }
            },
        }
        for channels in (None, [], [0], [-1], [True], [1, 1], ["1"]):
            data = dict(base)
            if channels is not None:
                data["channels"] = channels
            with self.subTest(channels=channels), self.assertRaisesRegex(ConfigError, "channels"):
                parse_config(data)

    def test_worker_max_concurrency_must_be_positive(self):
        with self.assertRaisesRegex(ConfigError, "worker.max_concurrency must be positive"):
            parse_config(
                {
                    "protocol": "llm.v1",
                    "open_event": {"addr": "127.0.0.1:9527"},
                    "worker": {"max_concurrency": 0},
                    "principal": 1,
                    "token": "t",
                    "channels": [101],
                    "default_provider": "main",
                    "providers": {
                        "main": {
                            "type": "openai_compatible",
                            "base_url": "https://example.test",
                            "api_key": "k",
                            "timeout": {"total_ms": 3},
                        }
                    },
                }
            )


if __name__ == "__main__":
    unittest.main()
