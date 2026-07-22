from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
DEFAULT_ALLOWED_METHODS = frozenset({"POST"})
DEFAULT_ALLOWED_PATHS = frozenset({"/v1/chat/completions", "/v1/responses"})
DEFAULT_MAX_CONCURRENCY = 8
SUPPORTED_METHODS = frozenset({"GET", "POST", "PUT", "PATCH", "DELETE"})


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
    allowed_methods: frozenset[str] = DEFAULT_ALLOWED_METHODS
    allowed_paths: frozenset[str] = DEFAULT_ALLOWED_PATHS


@dataclass(frozen=True)
class OpenEventConfig:
    addr: str


@dataclass(frozen=True)
class WorkerConfig:
    max_concurrency: int = DEFAULT_MAX_CONCURRENCY


@dataclass(frozen=True)
class ModelProxyConfig:
    protocol: str
    open_event: OpenEventConfig
    worker: WorkerConfig
    principal: int
    token: str
    channels: tuple[int, ...]
    max_payload_bytes: int
    default_provider: str
    providers: dict[str, ProviderConfig]


class ConfigError(ValueError):
    pass


def load_config(path: str | Path) -> ModelProxyConfig:
    data = _load_simple_yaml(Path(path).read_text(encoding="utf-8"))
    return parse_config(data)


def parse_config(data: dict[str, Any]) -> ModelProxyConfig:
    if "filter_response_headers" in data:
        raise ConfigError("filter_response_headers is no longer supported; response headers use a fixed allowlist")
    protocol = _required_str(data, "protocol")
    if protocol != "llm.v1":
        raise ConfigError("protocol must be llm.v1")
    open_event_data = _required_dict(data, "open_event")
    open_event = OpenEventConfig(addr=_required_str(open_event_data, "addr"))
    worker_data = data.get("worker", {})
    worker_data = _as_dict(worker_data, "worker")
    worker = WorkerConfig(
        max_concurrency=_optional_int(worker_data, "max_concurrency", DEFAULT_MAX_CONCURRENCY)
    )
    if worker.max_concurrency <= 0:
        raise ConfigError("worker.max_concurrency must be positive")
    principal = _required_int(data, "principal")
    token = _required_str(data, "token")
    channels = _required_positive_int_list(data, "channels")
    max_payload_bytes = _optional_int(data, "max_payload_bytes", DEFAULT_MAX_PAYLOAD_BYTES)
    if max_payload_bytes <= 0:
        raise ConfigError("max_payload_bytes must be positive")
    default_provider = _required_str(data, "default_provider")
    providers_data = _required_dict(data, "providers")
    providers = {name: _parse_provider(name, value) for name, value in providers_data.items()}
    if default_provider not in providers:
        raise ConfigError("default_provider must reference an existing provider")
    return ModelProxyConfig(
        protocol=protocol,
        open_event=open_event,
        worker=worker,
        principal=principal,
        token=token,
        channels=channels,
        max_payload_bytes=max_payload_bytes,
        default_provider=default_provider,
        providers=providers,
    )


def _parse_provider(name: str, data: Any) -> ProviderConfig:
    if not isinstance(name, str) or not name:
        raise ConfigError("provider name must be a non-empty string")
    item = _as_dict(data, f"providers.{name}")
    provider_type = _required_str(item, "type")
    if provider_type != "openai_compatible":
        raise ConfigError(f"provider {name} type must be openai_compatible")
    timeout_data = _required_dict(item, "timeout")
    allowlist = item.get("allowlist", {})
    allowlist = _as_dict(allowlist, f"providers.{name}.allowlist")
    allowed_methods = _optional_str_list(
        allowlist,
        "methods",
        DEFAULT_ALLOWED_METHODS,
        f"providers.{name}.allowlist.methods",
    )
    unsupported_methods = sorted(allowed_methods - SUPPORTED_METHODS)
    if unsupported_methods:
        raise ConfigError(
            f"providers.{name}.allowlist.methods contains unsupported methods: "
            + ", ".join(unsupported_methods)
        )
    allowed_paths = _optional_str_list(
        allowlist,
        "paths",
        DEFAULT_ALLOWED_PATHS,
        f"providers.{name}.allowlist.paths",
    )
    for path in allowed_paths:
        if (
            not path.startswith("/")
            or "://" in path
            or "?" in path
            or "#" in path
            or any(ord(ch) < 32 or ord(ch) == 127 for ch in path)
        ):
            raise ConfigError(f"providers.{name}.allowlist.paths contains invalid path: {path}")
    return ProviderConfig(
        name=name,
        type=provider_type,
        base_url=_required_str(item, "base_url").rstrip("/"),
        api_key=_required_str(item, "api_key"),
        timeout=TimeoutConfig(
            total_ms=_required_positive_int(timeout_data, "total_ms"),
        ),
        allowed_methods=allowed_methods,
        allowed_paths=allowed_paths,
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


def _optional_str_list(
    data: dict[str, Any],
    key: str,
    default: frozenset[str],
    qualified_key: str,
) -> frozenset[str]:
    value = data.get(key)
    if value is None:
        return default
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{qualified_key} must be a non-empty list")
    if any(not isinstance(item, str) or not item for item in value):
        raise ConfigError(f"{qualified_key} must contain non-empty strings")
    if len(set(value)) != len(value):
        raise ConfigError(f"{qualified_key} must not contain duplicates")
    return frozenset(value)


def _required_positive_int_list(data: dict[str, Any], key: str) -> tuple[int, ...]:
    value = data.get(key)
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{key} must be a non-empty list")
    if any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 for item in value):
        raise ConfigError(f"{key} must contain positive integers")
    if len(set(value)) != len(value):
        raise ConfigError(f"{key} must not contain duplicates")
    return tuple(value)


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
    if value.startswith("["):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid list value: {value}") from exc
        if not isinstance(parsed, list):
            raise ConfigError(f"invalid list value: {value}")
        return parsed
    try:
        return int(value)
    except ValueError:
        return value.strip('"').strip("'")
