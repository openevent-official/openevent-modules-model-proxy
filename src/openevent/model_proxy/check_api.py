"""Check one configured Provider directly, without starting an OpenEvent Worker."""

import argparse
import json
import time

from .config import ConfigurationError, load_config
from .provider import ProviderCall, ProviderError


def _model(value):
    if not value.strip():
        raise argparse.ArgumentTypeError("model must not be empty")
    return value


def main(argv=None):
    parser = argparse.ArgumentParser(description="Send one small streaming request to a configured model API")
    parser.add_argument("--config", required=True, help="Existing model-proxy YAML configuration")
    parser.add_argument("--model", required=True, type=_model, help="Model to test")
    parser.add_argument("--provider", help="Configured provider name; defaults to default_provider")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except ConfigurationError:
        # YAML parser diagnostics can include source lines containing credentials.
        print("FAIL: cannot load configuration; check the file, YAML syntax and documented fields.", flush=True)
        return 2
    name = config.default_provider if args.provider is None else args.provider
    if name not in config.providers:
        print(f"FAIL: provider {name!r} is not configured.", flush=True)
        return 2
    provider = config.providers[name]
    path = "/v1/chat/completions"
    body = json.dumps({"model": args.model, "messages": [{"role": "user", "content": "Reply with OK."}],
                       "stream": True}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(body) > config.max_payload_bytes:
        print("FAIL: probe request exceeds max_payload_bytes.", flush=True)
        return 2
    print(f"Checking provider={name} model={args.model}", flush=True)
    print(f"Endpoint: {provider.base_url.rstrip('/')}{path}", flush=True)
    print(f"Timeouts: response headers={provider.timeout.response_header_ms} ms; "
          f"response idle={provider.timeout.idle_ms} ms", flush=True)
    print("Sending one streaming request...", flush=True)
    started = time.monotonic()
    operation = ProviderCall(provider, body, path, config.max_payload_bytes, streaming=True)
    events = iter(operation)
    phase = "before response headers"
    count = 0

    def elapsed():
        return round((time.monotonic() - started) * 1000)

    try:
        for event in events:
            if event.kind == "headers":
                print(f"HTTP {event.status_code} after {elapsed()} ms", flush=True)
                # Preserve the HTTP failure even if its body is HTML, malformed
                # SSE or never finishes. It already answers the availability check.
                if not 200 <= event.status_code < 300:
                    print(f"FAIL: HTTP {event.status_code} after {elapsed()} ms", flush=True)
                    return 1
                phase = "while reading response"
            elif event.kind == "append":
                count += 1
                if count == 1:
                    print(f"First data event after {elapsed()} ms", flush=True)
            elif event.kind == "completed":
                print(f"PASS: request completed in {elapsed()} ms (data events={count})", flush=True)
                return 0
        print(f"FAIL: response did not complete after {elapsed()} ms", flush=True)
        return 1
    except ProviderError as exc:
        print(f"FAIL: {exc.status_code} {phase}: {exc}; elapsed={elapsed()} ms", flush=True)
        return 1
    except KeyboardInterrupt:
        print(f"CANCELLED: {phase}; elapsed={elapsed()} ms", flush=True)
        return 130
    except Exception as exc:
        print(f"FAIL: unexpected {type(exc).__name__} {phase}; elapsed={elapsed()} ms", flush=True)
        return 1
    finally:
        operation.abort()
        events.close()


if __name__ == "__main__":
    raise SystemExit(main())
