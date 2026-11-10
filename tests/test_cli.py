"""Exercise real process signals without risking the test runner's own locks."""

import faulthandler
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from openevent.model_proxy import cli
from openevent.model_proxy.provider import ProviderCall
from openevent.model_proxy.worker import Worker
from openevent.model_proxy_sdk import InferCancelInput, parse_message
from test_worker import OpenEvent, Subscription, config, event, request


def run_cli(worker):
    with patch.object(cli, "load_config", return_value=config()), \
            patch.object(cli, "Worker", return_value=worker):
        return cli.main(["--config", "unused-test-config"])


def signal_during_cancel(signum):
    cfg = config()
    worker = Worker(cfg, client=OpenEvent())
    worker._accept(parse_message(request(1, stream=True)))
    state = worker.requests[1]
    state.phase = "running"
    operation = state.operation = ProviderCall(
        cfg.providers["main"], state.body, state.path,
        cfg.max_payload_bytes, streaming=True,
    )
    cancellation = parse_message(event(
        2, InferCancelInput(stream_id=state.stream_id, request_seq=1),
    ))
    sent = False

    def trace(frame, what, arg):
        nonlocal sent
        if (not sent and what == "line"
                and frame.f_code is ProviderCall._close.__code__
                and operation._lock.locked()):
            sent = True
            # Interrupt the real cancellation while its ordinary Lock is held.
            os.kill(os.getpid(), signum)
        return trace

    def run():
        worker._accept(cancellation)
        assert worker.stopped.wait(2), "Signal did not stop the worker"

    worker.run = run
    sys.settrace(trace)
    try:
        assert run_cli(worker) == 0
    finally:
        sys.settrace(None)
    assert sent, "Test never reached the provider's critical section"
    assert state.terminal_seq == 2
    assert operation._cancelled.is_set()
    assert not operation._lock.locked()


def signal_during_idle_subscription(signum):
    waiting = threading.Event()

    class IdleSubscription(Subscription):
        def __next__(self):
            waiting.set()
            return super().__next__()

    stream = IdleSubscription()
    transport = OpenEvent(subscriptions=[stream])
    worker = Worker(config(), client=transport)

    def send():
        if waiting.wait(2):
            os.kill(os.getpid(), signum)

    sender = threading.Thread(target=send, daemon=True)
    sender.start()
    assert run_cli(worker) == 0
    sender.join(2)
    assert not sender.is_alive()
    assert waiting.is_set()
    assert worker.stopped.is_set()
    assert stream.cancelled.is_set()
    assert stream.finished
    assert transport.closed


def repeated_signals_during_stop():
    finished = threading.Event()
    lock = threading.Lock()
    calls = []

    def stop():
        with lock:
            calls.append(True)
            os.kill(os.getpid(), signal.SIGINT)
            os.kill(os.getpid(), signal.SIGTERM)
            finished.set()

    def run():
        os.kill(os.getpid(), signal.SIGTERM)
        assert finished.wait(2), "Repeated signals blocked stop cleanup"

    assert run_cli(SimpleNamespace(run=run, stop=stop)) == 0
    assert len(calls) == 1, "Signals recursively or repeatedly entered cleanup"


def normal_return_restores_process_state():
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    threads = set(threading.enumerate())
    worker = SimpleNamespace(run=lambda: None, stop=lambda: None)
    for _ in range(2):
        assert run_cli(worker) == 0
        assert all(signal.getsignal(sig) is handler for sig, handler in handlers.items())
        assert set(threading.enumerate()) <= threads, "CLI leaked its helper thread"


def stop_failure_exits():
    def stop():
        raise RuntimeError("intentional stop failure")

    def run():
        os.kill(os.getpid(), signal.SIGTERM)
        threading.Event().wait(2)
        raise AssertionError("Stop failure did not terminate the process")

    run_cli(SimpleNamespace(run=run, stop=stop))


class CLISignalTests(unittest.TestCase):
    def child(self, case, signum=None, *, returncode=0):
        command = [sys.executable, "-B", str(Path(__file__).resolve()), case]
        if signum is not None:
            command.append(str(signum))
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, timeout=6)
        self.assertEqual(result.returncode, returncode, result.stdout)
        return result.stdout

    def test_signal_during_provider_cancel_does_not_reenter_lock(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signum):
                self.child("cancel", signum)

    def test_signal_wakes_idle_subscription(self):
        for signum in (signal.SIGTERM, signal.SIGINT):
            with self.subTest(signal=signum):
                self.child("idle", signum)

    def test_repeated_signals_do_not_reenter_stop(self):
        self.child("repeated")

    def test_normal_return_restores_handlers_and_releases_helper(self):
        self.child("normal")

    def test_stop_failure_exits_nonzero(self):
        output = self.child("failure", returncode=1)
        self.assertIn("intentional stop failure", output)


if __name__ == "__main__":
    # A broken signal handler must fail a child, never hang the whole test suite.
    faulthandler.dump_traceback_later(3, exit=True)
    try:
        case = sys.argv[1]
        if case == "cancel":
            signal_during_cancel(int(sys.argv[2]))
        elif case == "idle":
            signal_during_idle_subscription(int(sys.argv[2]))
        else:
            {"repeated": repeated_signals_during_stop,
             "normal": normal_return_restores_process_state,
             "failure": stop_failure_exits}[case]()
    finally:
        faulthandler.cancel_dump_traceback_later()
