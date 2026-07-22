# OpenEvent Model Proxy

[中文版](README_cn.md)

OpenEvent Model Proxy is a model call proxy built on OpenEvent. It receives
model requests through the `llm.v1` event protocol, calls an OpenAI-compatible
provider, and writes results back to the same OpenEvent channel.

This repository contains:

- `src/openevent/model_proxy/`: `model-proxy` worker and CLI entry point.
- `src/openevent/model_proxy_sdk/`: `llm.v1` payload construction, validation,
  parsing, and publishing utilities.
- `openevent.model_proxy_sdk.OpenAI`: OpenAI-like synchronous non-streaming
  client.
- `docs/`: public documentation for users and contributors.
- `tests/`: Python `unittest` tests.

## Project Status

- Protocol version: `llm.v1`
- Python version: `>=3.10`
- OpenEvent SDK: `openevent-sdk>=0.4.0`
- YAML parser: `PyYAML>=6.0`
- Provider type: `openai_compatible`
- Default provider allowlist: `POST /v1/chat/completions` and
  `POST /v1/responses`. Other methods or paths require explicit configuration.
- Streaming response: `stream=True` is not supported yet.

## Build and Test

Build, test, and install tasks are wrapped by `make`. Wheel artifacts are
written to `dist/`. `build/` is reserved for build dependencies, test temporary
files, caches, and temporary files.

This project depends on `openevent-sdk>=0.4.0` as a Python package. Tests use
the SDK package already installed in the current Python environment and do not
install SDK from the submodule.

For normal runtime, build, or install flows, install the SDK in the target Python
environment first:

```bash
cd openevent-sdk
make install
cd ..
```

To install the SDK into a custom location, pass `pip install` arguments through
the SDK submodule's `INSTALL_ARGS`:

```bash
cd openevent-sdk
make install INSTALL_ARGS="--target /opt/openevent-sdk"
```

Build only, without installing into the current Python environment:

```bash
make build
```

The wheel is written to `dist/`.

Build and install into the current Python environment:

```bash
make install
```

Pass `pip install` options through `INSTALL_ARGS` when a custom install path is
needed:

```bash
make install INSTALL_ARGS="--target /opt/openevent-model-proxy"
make install INSTALL_ARGS="--prefix /opt/openevent-model-proxy"
```

Run tests:

```bash
make test
```

Run end-to-end tests with an explicit OpenEvent server binary:

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

If the installed SDK is missing or incompatible, the e2e script exits before
starting OpenEvent.

Clean build products and temporary files:

```bash
make clean
```

## Run

Start the worker:

```bash
model-proxy --config model-proxy.yaml
```

Run from source:

```bash
python3 -m openevent.model_proxy.cli --config model-proxy.yaml
```

See [docs/CONFIGURATION.md](docs/CONFIGURATION.md) for a configuration example.

## Python SDK

See [docs/SDK_USAGE.md](docs/SDK_USAGE.md) for OpenAI-like client and low-level
protocol SDK usage.

## Documentation

- [docs/CONFIGURATION.md](docs/CONFIGURATION.md): worker configuration.
- [docs/SDK_USAGE.md](docs/SDK_USAGE.md): Python SDK usage.
- [docs/LLM_PROTOCOL.md](docs/LLM_PROTOCOL.md): `llm.v1` event protocol.
- [docs/RESULT_PUBLISHING.md](docs/RESULT_PUBLISHING.md): worker result-publishing reliability.
