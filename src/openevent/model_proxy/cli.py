"""Run the Worker from one YAML configuration."""

import argparse
import logging
import os
import signal

from .config import load_config
from .worker import Worker


def _exit_on_failure(exc):
    logging.error("Worker failed: %s", exc)
    # Called where the failure occurs; no reader or provider join is needed.
    os._exit(1)


def main(argv=None):
    parser = argparse.ArgumentParser(description="OpenEvent llm.v1 model proxy")
    parser.add_argument("--config", required=True, help="Worker YAML configuration")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        worker = Worker(load_config(args.config), on_fatal=_exit_on_failure)
    except Exception as exc:
        logging.error("Worker startup failed: %s", exc)
        return 1
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: worker.stop())
    try:
        worker.run()
    except Exception as exc:
        _exit_on_failure(exc)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
