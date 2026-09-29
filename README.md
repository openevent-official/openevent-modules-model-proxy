# OpenEvent Model Proxy

[中文版](README_cn.md)

OpenEvent Model Proxy is a model call proxy built on OpenEvent. It receives model
requests through the `llm.v1` event protocol, calls an OpenAI-compatible provider,
and writes results back to the same OpenEvent channel.

## Project Structure

- `src/openevent/model_proxy/`: the `model-proxy` worker and CLI entry point
- `src/openevent/model_proxy_sdk/`: `llm.v1` payload construction, validation, parsing, and publishing tools
- `openevent.model_proxy_sdk.OpenAI`: an OpenAI-like synchronous client supporting ordinary and streaming calls
- `docs/`: public documentation for users and contributors
- `tests/`: Python `unittest` tests

## Supported Features

- Protocol version: `llm.v1`
- Python version: `>=3.10`
- OpenEvent SDK: `openevent-sdk>=0.11.1`
- YAML parser: `PyYAML>=6.0`
- Provider type: `openai_compatible`; see the [llm.v1 compatibility requirements](docs/LLM_PROTOCOL.md#41-minimum-openai_compatible-contract)
- Endpoints: `POST /v1/chat/completions` and `POST /v1/responses`, both with ordinary and streaming calls
- Python API: protocol message construction, parsing, and publishing, plus an OpenAI-like synchronous client

## Build and Test

Use `make` for builds, tests, and installation. Wheels go in `dist/`; temporary files,
build dependencies, and caches go in `build/`. First install an SDK satisfying the requirement above
in the target Python environment:

```bash
cd openevent-sdk
make install
cd ..
```

Common commands:

```bash
make build    # Build only
make install  # Build and install
make clean    # Remove dist/ and build/
```

Builds remove this project's old wheels; installation and e2e use only a successful fresh build.
Installation resolves the wheel's dependencies, reuses satisfying versions, then replaces this
project's package even if its version is unchanged. Dependency resolution failure leaves the
project package unchanged. Select the Python environment with `PYTHON`; `INSTALL_ARGS` accepts
index and network options such as `--find-links`, but cannot change the destination or dependency
replacement policy.

Tests use the SDK already installed in the current environment without installing from the
submodule. The `packaging` test tool checks its version:

```bash
python3 -B -m pip install packaging
make test
```

End-to-end tests require an explicit OpenEvent server binary:

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

E2E exits before starting OpenEvent if the SDK is missing or incompatible. GNU `timeout` limits
the test process to `120` seconds by default; adjust it with `E2E_TIMEOUT_SECONDS`.
Build and installation time is excluded.

## Run

Start the worker:

```bash
model-proxy --config model-proxy.yaml
```

Alternatively, run from source:

```bash
PYTHONPATH=src python3 -B -m openevent.model_proxy.cli --config model-proxy.yaml
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for a configuration example.

## Documentation

| Document | Contents |
| --- | --- |
| [docs/LLM_PROTOCOL.md](docs/LLM_PROTOCOL.md) | `llm.v1` payload fields, message state machine, status codes, header forwarding, payload-size behavior, and observable recovery contract |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Worker configuration, provider URLs, and timeouts |
| [docs/OPEN_EVENT_RPC_RETRY.md](docs/OPEN_EVENT_RPC_RETRY.md) | Common retry rules for ordinary OpenEvent RPCs, including GetStatus, Fetch, UUID allocation and lookup, and Subscribe |
| [docs/RESULT_PUBLISHING.md](docs/RESULT_PUBLISHING.md) | UUID freezing, reliable publishing, UUID reconciliation, and `ResultPublishError` commit states for one event |
| [docs/SDK_API.md](docs/SDK_API.md) | Public Python SDK signatures, return objects, exceptions, and lifecycle |
| [docs/SDK_USAGE.md](docs/SDK_USAGE.md) | Python SDK usage examples |
