from __future__ import annotations

import argparse
import logging

from openevent.sdk import OpenEventClient

from .config import load_config
from .worker import ModelProxyWorker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="model-proxy")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    config = load_config(args.config)
    client = OpenEventClient(config.open_event.addr)
    worker = ModelProxyWorker(config, client)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
