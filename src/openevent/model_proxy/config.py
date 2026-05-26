from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
DEFAULT_IDEMPOTENCY_DSN = "sqlite:///model_proxy.db"


@dataclass(frozen=True)
class TimeoutConfig:
    total_ms: int


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    type: str
    base_url: str
    api_key: str
    timeout: TimeoutConfig


@dataclass(frozen=True)
class OpenEventConfig:
    addr: str


@dataclass(frozen=True)
class ModelProxyConfig:
    protocol: str
    open_event: OpenEventConfig
    principal: int
    token: str
    idempotency_dsn: str
    max_payload_bytes: int
    default_provider: str
    providers: dict[str, ProviderConfig]
    filter_response_headers: bool = True


class ConfigError(ValueError):
    pass


def load_config(path: str | Path) -> ModelProxyConfig:
    data = _load_simple_yaml(Path(path).read_text(encoding="utf-8"))
    return parse_config(data)


def parse_config(data: dict[str, Any]) -> ModelProxyConfig:
    protocol = _required_str(data, "protocol")
    if protocol != "llm.v1":
        raise ConfigError("protocol must be llm.v1")
    open_event_data = _required_dict(data, "open_event")
    open_event = OpenEventConfig(addr=_required_str(open_event_data, "addr"))
    principal = _required_int(data, "principal")
    token = _required_str(data, "token")
    idempotency_dsn = _optional_str(data, "idempotency_dsn", DEFAULT_IDEMPOTENCY_DSN)
    max_payload_bytes = _optional_int(data, "max_payload_bytes", DEFAULT_MAX_PAYLOAD_BYTES)
    if max_payload_bytes <= 0:
        raise ConfigError("max_payload_bytes must be positive")
    default_provider = _required_str(data, "default_provider")
    filter_response_headers = _optional_bool(data, "filter_response_headers", True)
    providers_data = _required_dict(data, "providers")
    providers = {name: _parse_provider(name, value) for name, value in providers_data.items()}
    if default_provider not in providers:
        raise ConfigError("default_provider must reference an existing provider")
    return ModelProxyConfig(
        protocol=protocol,
        open_event=open_event,
        principal=principal,
        token=token,
        idempotency_dsn=idempotency_dsn,
        max_payload_bytes=max_payload_bytes,
        default_provider=default_provider,
        providers=providers,
        filter_response_headers=filter_response_headers,
    )


def _parse_provider(name: str, data: Any) -> ProviderConfig:
    if not isinstance(name, str) or not name:
        raise ConfigError("provider name must be a non-empty string")
    item = _as_dict(data, f"providers.{name}")
    provider_type = _required_str(item, "type")
    if provider_type != "openai_compatible":
        raise ConfigError(f"provider {name} type must be openai_compatible")
    timeout_data = _required_dict(item, "timeout")
    return ProviderConfig(
        name=name,
        type=provider_type,
        base_url=_required_str(item, "base_url").rstrip("/"),
        api_key=_required_str(item, "api_key"),
        timeout=TimeoutConfig(
            total_ms=_required_positive_int(timeout_data, "total_ms"),
        ),
    )


def _required_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    if key not in data:
        raise ConfigError(f"{key} is required")
    return _as_dict(data[key], key)


def _as_dict(value: Any, key: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{key} must be a mapping")
    return value


def _required_str(data: dict[str, Any], key: str) -> str:
    if key not in data or not isinstance(data[key], str) or not data[key]:
        raise ConfigError(f"{key} must be a non-empty string")
    return data[key]


def _optional_str(data: dict[str, Any], key: str, default: str) -> str:
    value = data.get(key, default)
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{key} must be a non-empty string")
    return value


def _required_int(data: dict[str, Any], key: str) -> int:
    if key not in data or not isinstance(data[key], int) or isinstance(data[key], bool):
        raise ConfigError(f"{key} must be an integer")
    return data[key]


def _required_positive_int(data: dict[str, Any], key: str) -> int:
    value = _required_int(data, key)
    if value <= 0:
        raise ConfigError(f"{key} must be positive")
    return value


def _optional_int(data: dict[str, Any], key: str, default: int) -> int:
    value = data.get(key, default)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"{key} must be an integer")
    return value


def _optional_bool(data: dict[str, Any], key: str, default: bool) -> bool:
    value = data.get(key, default)
    if not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    return value


def _load_simple_yaml(text: str) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError:
        return _parse_minimal_yaml(text)
    loaded = yaml.safe_load(text)
    if not isinstance(loaded, dict):
        raise ConfigError("config root must be a mapping")
    return loaded


def _parse_minimal_yaml(text: str) -> dict[str, Any]:
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        line = raw_line.strip()
        if ":" not in line:
            raise ConfigError(f"invalid config line: {raw_line}")
        key, value_text = line.split(":", 1)
        key = key.strip()
        value_text = value_text.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if not value_text:
            child: dict[str, Any] = {}
            parent[key] = child
            stack.append((indent, child))
        else:
            parent[key] = _parse_scalar(value_text)
    return root


def _parse_scalar(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    try:
        return int(value)
    except ValueError:
        return value.strip('"').strip("'")
