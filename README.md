# OpenEvent Model Proxy

[中文版](README_cn.md)

OpenEvent Model Proxy is a model call proxy built on OpenEvent. It receives model
requests through the `llm.v1` event protocol, calls an OpenAI-compatible provider,
and writes results back to the same OpenEvent channel.

This repository contains:

- `src/openevent/model_proxy/`: the `model-proxy` worker and CLI entry point
- `src/openevent/model_proxy_sdk/`: `llm.v1` payload construction, validation, parsing, and publishing tools
- `openevent.model_proxy_sdk.OpenAI`: an OpenAI-like synchronous client supporting ordinary and streaming calls
- `docs/`: public documentation for users and contributors
- `tests/`: Python `unittest` tests

## Project Status

- Protocol version: `llm.v1`
- Python version: `>=3.10`
- OpenEvent SDK: `openevent-sdk>=0.8.0`
- YAML parser: `PyYAML>=6.0`
- Provider type: `openai_compatible`
- Requests may select a configured provider through the optional top-level `provider` field;
  when omitted, the Worker uses `default_provider`.
- Supported endpoints are fixed to `POST /v1/chat/completions` and `POST /v1/responses`.
  `openai_compatible` means only that the Provider supports ordinary or streaming request and
  response transport under the [minimum llm.v1 compatibility contract](docs/LLM_PROTOCOL.md#41-minimum-openai_compatible-contract).
  It does not imply support for all OpenAI API models, fields, or product capabilities;
  any other method or path is an invalid request.
- Ordinary and streaming results are supported; the protocol document defines their exact
  message shapes, status codes, and terminal rules.

## Build and Test

Use `make` for builds, tests, and installation. Final wheel artifacts go in `dist/`.
`build/` is reserved for build dependencies, test temporary files, caches, and temporary files.

This project uses the Python package dependency `openevent-sdk>=0.8.0`. Tests use the SDK
already installed in the current Python environment; they do not install it from the
`openevent-sdk/` submodule.

Before running, building, or installing, install the SDK in the target Python environment:

```bash
cd openevent-sdk
make install
cd ..
```

Build without installing into the current Python environment:

```bash
make build
```

Each build first removes this project's old wheels from `dist/`, preserving artifacts from other packages.
Installation and e2e use the newly built wheel; if the build fails, they stop without installing or testing an old version.

Build and install the generated wheel into the current Python environment:

```bash
make install
```

Tests also need `packaging`, declared in the `test` extra, to check the installed SDK
against Python package version rules. Install this test tool before running tests:

```bash
python3 -B -m pip install packaging
make test
```

End-to-end tests require an explicit OpenEvent server binary:

```bash
OPENEVENT_SERVER_BIN=<openevent_server_binary> make e2e
```

If the SDK is missing or its version is incompatible, the e2e script reports an error
and exits before starting OpenEvent.
E2E uses GNU `timeout` to limit the test process runtime to `120` seconds by default; set `E2E_TIMEOUT_SECONDS` to change it. The preceding build and installation steps are outside this limit.

Clean build artifacts and temporary files:

```bash
make clean
```

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

## Python SDK

See [docs/SDK_API.md](docs/SDK_API.md) for the public Python API and
[docs/SDK_USAGE.md](docs/SDK_USAGE.md) for common examples.

## Documentation

Each category of design rules has one authoritative document. The README and SDK usage
guide provide entry points and examples without redefining protocol state or reliability rules.

| Document | Authoritative scope |
| --- | --- |
| [docs/LLM_PROTOCOL.md](docs/LLM_PROTOCOL.md) | `llm.v1` payload fields, message state machine, status codes, header forwarding, payload-size behavior, and observable recovery contract |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Worker configuration, provider URLs, and timeouts |
| [docs/OPEN_EVENT_RPC_RETRY.md](docs/OPEN_EVENT_RPC_RETRY.md) | Common retry rules for ordinary OpenEvent RPCs, including GetStatus, Fetch, UUID allocation and lookup, and Subscribe |
| [docs/RESULT_PUBLISHING.md](docs/RESULT_PUBLISHING.md) | UUID freezing, reliable publishing, UUID reconciliation, and `ResultPublishError` commit states for one event |
| [docs/SDK_API.md](docs/SDK_API.md) | Public Python SDK signatures, return objects, exceptions, and lifecycle |
| [docs/SDK_USAGE.md](docs/SDK_USAGE.md) | Non-authoritative Python SDK examples and usage notes |
