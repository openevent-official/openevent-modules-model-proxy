from pathlib import Path
import tempfile
import unittest

from openevent.model_proxy.config import ConfigurationError, load_config, parse_config


def valid_config():
    return {
        "protocol": "llm.v1", "open_event": {"addr": "localhost:9527"},
        "principal": 1, "token": "token", "channels": [2], "default_provider": "main",
        "providers": {"main": {"type": "openai_compatible", "base_url": "https://example.com/gateway/", "api_key": "key", "timeout": {"response_header_ms": 1000}}},
    }


class ConfigTests(unittest.TestCase):
    def test_defaults_and_fixed_retry_configuration(self):
        config = parse_config(valid_config())
        self.assertEqual((config.worker.max_concurrency, config.worker.max_retries, config.worker.retry_interval_ms), (8, 3, 1000))
        self.assertEqual(config.max_payload_bytes, 16777216)
        self.assertEqual(config.providers["main"].timeout.idle_ms, 30000)
        value = valid_config()
        value["worker"] = {"max_retries": 0, "retry_interval_ms": 5}
        self.assertEqual(parse_config(value).worker.max_retries, 0)

    def test_reject_invalid_fields_types_and_references(self):
        changes = [
            ("protocol", "llm.v0"), ("principal", True), ("channels", []),
            ("channels", [1, 1]), ("channels", [True]), ("max_payload_bytes", 4095),
            ("default_provider", "missing"), ("worker", {"max_retries": True}),
            ("worker", {"retry_interval_ms": 0}), ("open_event", {"addr": "ok", "rpc_timeot_ms": 1}),
            ("providers", {}), ("unknown", 1),
        ]
        for key, val in changes:
            with self.subTest(key=key, value=val):
                data = valid_config()
                data[key] = val
                with self.assertRaises(ConfigurationError):
                    parse_config(data)

    def test_provider_url_token_and_timeout(self):
        for key, value in [("base_url", "example.com"), ("base_url", "https://u:p@example.com"), ("base_url", "https://example.com?"), ("base_url", "https://example.com/#f"), ("api_key", " key"), ("api_key", "k\nx"), ("timeout", {"response_header_ms": False})]:
            with self.subTest(key=key, value=value):
                data = valid_config()
                data["providers"]["main"][key] = value
                with self.assertRaises(ConfigurationError):
                    parse_config(data)

    def test_duplicate_yaml_keys_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "config.yaml"
            path.write_text("protocol: llm.v1\nprotocol: llm.v1\n", encoding="utf-8")
            with self.assertRaisesRegex(ConfigurationError, "duplicate YAML key"):
                load_config(path)
