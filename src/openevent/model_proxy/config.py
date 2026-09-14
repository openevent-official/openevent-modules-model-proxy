"""Worker configuration: one strict translation of CONFIGURATION_cn.md."""

from dataclasses import dataclass
from pathlib import Path
import unicodedata
from urllib.parse import urlsplit

import yaml
from openevent.model_proxy_sdk.errors import ConfigurationError


@dataclass(frozen=True)
class OpenEventConfig:
    addr: str
    rpc_timeout_ms: int = 30000


@dataclass(frozen=True)
class WorkerConfig:
    max_concurrency: int = 8
    max_retries: int = 3
    retry_interval_ms: int = 1000


@dataclass(frozen=True)
class ProviderTimeout:
    response_header_ms: int
    idle_ms: int = 30000


@dataclass(frozen=True)
class ProviderConfig:
    type: str
    base_url: str
    api_key: str
    timeout: ProviderTimeout


@dataclass(frozen=True)
class Config:
    protocol: str
    open_event: OpenEventConfig
    worker: WorkerConfig
    principal: int
    token: str
    channels: tuple[int, ...]
    max_payload_bytes: int
    default_provider: str
    providers: dict[str, ProviderConfig]


class _Loader(yaml.SafeLoader):
    pass


def _mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=True)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise ConfigurationError("YAML mapping keys must be strings") from exc
        if duplicate:
            raise ConfigurationError(f"duplicate YAML key: {key!r}")
        result[key] = loader.construct_object(value_node, deep=True)
    return result


_Loader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _fields(value, allowed, required, location):
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    unknown = set(value) - set(allowed)
    missing = set(required) - set(value)
    if unknown or missing:
        raise ConfigurationError(f"{location}: unknown fields {sorted(map(str, unknown))}; missing fields {sorted(missing)}")
    return value


def _integer(value, location, minimum=1):
    if type(value) is not int or value < minimum:
        raise ConfigurationError(f"{location} must be an integer >= {minimum}")
    return value


def _string(value, location, clean=False):
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{location} must be a nonempty string")
    if clean and (value != value.strip() or any(unicodedata.category(c) == "Cc" for c in value)):
        raise ConfigurationError(f"{location} must not contain surrounding whitespace or control characters")
    return value


def parse_config(value):
    """Validate a decoded YAML mapping and return the worker configuration."""
    top = {"protocol", "open_event", "worker", "principal", "token", "channels", "max_payload_bytes", "default_provider", "providers"}
    data = _fields(value, top, top - {"worker", "max_payload_bytes"}, "config")
    if data["protocol"] != "llm.v1":
        raise ConfigurationError("protocol must be llm.v1")
    oe = _fields(data["open_event"], {"addr", "rpc_timeout_ms"}, {"addr"}, "open_event")
    wc = _fields(data.get("worker", {}), {"max_concurrency", "max_retries", "retry_interval_ms"}, set(), "worker")
    channels = data["channels"]
    if not isinstance(channels, list) or not channels:
        raise ConfigurationError("channels must be a nonempty list")
    channels = tuple(_integer(c, "channels[]") for c in channels)
    if len(set(channels)) != len(channels):
        raise ConfigurationError("channels must not contain duplicates")
    raw_providers = data["providers"]
    if not isinstance(raw_providers, dict) or not raw_providers:
        raise ConfigurationError("providers must be a nonempty mapping")
    providers = {}
    for name, raw in raw_providers.items():
        _string(name, "provider name", clean=True)
        p = _fields(raw, {"type", "base_url", "api_key", "timeout"}, {"type", "base_url", "api_key", "timeout"}, f"providers.{name}")
        if p["type"] != "openai_compatible":
            raise ConfigurationError(f"providers.{name}.type must be openai_compatible")
        base_url = _string(p["base_url"], "base_url", clean=True)
        try:
            url = urlsplit(base_url)
            valid = url.scheme in {"http", "https"} and url.hostname and url.netloc and not url.username and not url.password
            valid = valid and "@" not in url.netloc and "?" not in base_url and "#" not in base_url
            if url.port is not None and not 1 <= url.port <= 65535:
                valid = False
        except ValueError:
            valid = False
        if not valid:
            raise ConfigurationError("base_url must be an absolute HTTP(S) URL without userinfo, query or fragment")
        timeout = _fields(p["timeout"], {"response_header_ms", "idle_ms"}, {"response_header_ms"}, "provider timeout")
        providers[name] = ProviderConfig(
            p["type"], base_url, _string(p["api_key"], "api_key", clean=True),
            ProviderTimeout(_integer(timeout["response_header_ms"], "response_header_ms"), _integer(timeout.get("idle_ms", 30000), "idle_ms")),
        )
    default = _string(data["default_provider"], "default_provider")
    if default not in providers:
        raise ConfigurationError("default_provider must name a configured provider")
    return Config(
        "llm.v1",
        OpenEventConfig(_string(oe["addr"], "open_event.addr"), _integer(oe.get("rpc_timeout_ms", 30000), "rpc_timeout_ms")),
        WorkerConfig(_integer(wc.get("max_concurrency", 8), "max_concurrency"), _integer(wc.get("max_retries", 3), "max_retries", 0), _integer(wc.get("retry_interval_ms", 1000), "retry_interval_ms")),
        _integer(data["principal"], "principal"), _string(data["token"], "token"), channels,
        _integer(data.get("max_payload_bytes", 16777216), "max_payload_bytes", 4096), default, providers,
    )


def load_config(path):
    try:
        with Path(path).open(encoding="utf-8") as source:
            return parse_config(yaml.load(source, Loader=_Loader))
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        raise ConfigurationError(f"cannot load configuration: {exc}") from exc
