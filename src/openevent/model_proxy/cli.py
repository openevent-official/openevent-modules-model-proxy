"""Run the Worker from one YAML configuration."""

import argparse
import logging
import os
import signal
import threading

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
    stop_requested = False
    finished = threading.Event()

    def request_stop(*_):
        nonlocal stop_requested
        # A signal can interrupt code holding any Worker, provider or Event lock.
        # Even Event.set() is unsafe here: only record the request.
        stop_requested = True

    def stop_when_requested():
        while not finished.wait(0.05):
            if stop_requested:
                try:
                    # Also wakes an idle Subscribe; no new event is needed to exit.
                    worker.stop()
                except Exception as exc:
                    _exit_on_failure(exc)
                return

    stopper = threading.Thread(target=stop_when_requested, name="model-proxy-stop", daemon=True)
    previous_handlers = {}
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous_handlers[sig] = signal.signal(sig, request_stop)
        stopper.start()
        worker.run()
    except Exception as exc:
        _exit_on_failure(exc)
    finally:
        finished.set()
        if stopper.ident is not None:
            stopper.join()
        for sig, handler in previous_handlers.items():
            signal.signal(sig, handler)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
