from types import SimpleNamespace
import threading
import unittest
from unittest.mock import patch

import grpc

from openevent.model_proxy_sdk import (
    CommitState, ConfigurationError, PayloadValidationError, ResultPublishError,
    InferRequestInput, InferResultInput, InferAppendInput, InferEndInput, InferCancelInput,
    create_client, parse_payload, publish_infer_request, publish_infer_result,
    publish_infer_append, publish_infer_end, publish_infer_cancel,
)
from openevent.model_proxy_sdk.publishing import publish_request
from openevent.model_proxy_sdk.rpc import RPCStopped, call_rpc


class RpcError(Exception):
    def __init__(self, status):
        self.status = status

    def code(self):
        return getattr(grpc.StatusCode, self.status)


def outcome(items):
    value = items.pop(0)
    if isinstance(value, Exception):
        raise value
    return value


class Transport:
    def __init__(self, *, allocations=None, publishes=None, queries=None):
        self.allocations = [99] if allocations is None else list(allocations)
        self.publishes = [SimpleNamespace(seq=42)] if publishes is None else list(publishes)
        self.queries = [42] if queries is None else list(queries)
        self.allocation_calls = 0
        self.publish_calls = []
        self.query_calls = []

    def get_uuid(self):
        self.allocation_calls += 1
        return outcome(self.allocations)

    def publish_auto_seq(self, **kwargs):
        self.publish_calls.append(kwargs)
        return outcome(self.publishes)

    def get_seq_by_uuid(self, uuid):
        self.query_calls.append(uuid)
        return outcome(self.queries)


def req():
    return InferRequestInput(stream_id="s", method="POST", path="/v1/responses", body={})


class PublishingTests(unittest.TestCase):
    def setUp(self):
        self.sleep_patch = patch("openevent.model_proxy_sdk.publishing.time.sleep")
        self.sleep = self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    def publish(self, transport, *, retries=3, interval=1000):
        return publish_infer_request(create_client(transport, "token", retries, interval), 3, 4, req())

    def test_all_message_functions_set_recipients_and_freeze_payload(self):
        cases = [
            (publish_infer_request, req(), ()),
            (publish_infer_result, InferResultInput(stream_id="s", prev_seq=1, status_code=200, body=None), (5,)),
            (publish_infer_append, InferAppendInput(stream_id="s", request_seq=1, prev_seq=2, body={}), (5,)),
            (publish_infer_end, InferEndInput(stream_id="s", request_seq=1, status_code=200, end_status="completed"), (5,)),
            (publish_infer_cancel, InferCancelInput(stream_id="s", request_seq=1), ()),
        ]
        for function, event, recipients in cases:
            with self.subTest(function=function.__name__):
                transport = Transport()
                args = (create_client(transport, "token"), 3, 4)
                self.assertEqual(function(*args, *recipients, event), 42)
                call = transport.publish_calls[0]
                self.assertEqual(call["recipients"], recipients)
                self.assertEqual((call["channel_id"], call["principal"], call["uuid"]), (3, 4, 99))
                self.assertEqual(call["object_keys"], ())
                self.assertEqual(call["token"], "token")
                self.assertEqual(parse_payload(call["payload"]).kind, event.KIND)
                self.assertEqual(transport.allocation_calls, 1)

    def test_uncertain_retry_reuses_exact_request_and_uuid(self):
        transport = Transport(publishes=[RpcError("UNAVAILABLE"), OSError("reset"), SimpleNamespace(seq=42)])
        self.assertEqual(self.publish(transport, interval=17), 42)
        self.assertEqual(transport.allocation_calls, 1)
        self.assertEqual(len(transport.publish_calls), 3)
        self.assertTrue(all(call == transport.publish_calls[0] for call in transport.publish_calls))
        self.assertEqual(self.sleep.call_args_list, [((0.017,),), ((0.017,),)])
        self.assertEqual(transport.query_calls, [])

    def test_known_noncommit_ends_immediately(self):
        for status in ("UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND", "INVALID_ARGUMENT", "RESOURCE_EXHAUSTED"):
            with self.subTest(status=status):
                transport = Transport(publishes=[RpcError(status)])
                with self.assertRaises(ResultPublishError) as caught:
                    self.publish(transport)
                error = caught.exception
                self.assertIs(error.commit_state, CommitState.NOT_COMMITTED)
                self.assertEqual(error.event_uuid, 99)
                self.assertEqual(error.last_status, status)
                self.assertEqual(len(transport.publish_calls), 1)
                self.assertEqual(transport.query_calls, [])

    def test_later_known_noncommit_cannot_erase_earlier_uncertainty(self):
        transport = Transport(publishes=[RpcError("DEADLINE_EXCEEDED"), RpcError("PERMISSION_DENIED")])
        with self.assertRaises(ResultPublishError) as caught:
            self.publish(transport)
        self.assertIs(caught.exception.commit_state, CommitState.UNKNOWN)
        self.assertEqual(caught.exception.last_status, "PERMISSION_DENIED")
        self.assertEqual(len(transport.publish_calls), 2)

    def test_unknown_status_is_retryable_for_publish_only(self):
        transport = Transport(publishes=[RpcError("OUT_OF_RANGE"), SimpleNamespace(seq=42)])
        self.assertEqual(self.publish(transport), 42)
        transport = Transport(allocations=[RpcError("OUT_OF_RANGE")])
        with self.assertRaises(ResultPublishError) as caught:
            self.publish(transport)
        self.assertIs(caught.exception.commit_state, CommitState.NOT_COMMITTED)
        self.assertIsNone(caught.exception.event_uuid)
        self.assertEqual(transport.allocation_calls, 1)

    def test_exhaustion_never_queries_uuid_or_starts_another_publish(self):
        transport = Transport(publishes=[RpcError("UNAVAILABLE")] * 3)
        with self.assertRaises(ResultPublishError) as caught:
            self.publish(transport, retries=2)
        self.assertIs(caught.exception.commit_state, CommitState.UNKNOWN)
        self.assertIsNone(caught.exception.committed_seq)
        self.assertEqual(len(transport.publish_calls), 3)
        self.assertEqual(transport.query_calls, [])
        self.assertEqual(self.sleep.call_count, 2)

    def test_already_exists_queries_same_uuid_without_republish(self):
        transport = Transport(publishes=[RpcError("UNAVAILABLE"), RpcError("ALREADY_EXISTS")],
                              queries=[RpcError("INTERNAL"), 42])
        self.assertEqual(self.publish(transport, retries=1), 42)
        self.assertEqual(len(transport.publish_calls), 2)
        self.assertEqual(transport.query_calls, [99, 99])
        self.assertEqual(transport.allocation_calls, 1)

    def test_failed_uuid_lookup_is_committed_even_without_seq(self):
        for failures in ([RpcError("PERMISSION_DENIED")], [RpcError("UNAVAILABLE")] * 2):
            with self.subTest(failures=failures):
                transport = Transport(publishes=[RpcError("ALREADY_EXISTS")], queries=failures)
                with self.assertRaises(ResultPublishError) as caught:
                    self.publish(transport, retries=1)
                self.assertIs(caught.exception.commit_state, CommitState.COMMITTED)
                self.assertEqual(caught.exception.event_uuid, 99)
                self.assertIsNone(caught.exception.committed_seq)
                self.assertEqual(len(transport.publish_calls), 1)
                with self.assertRaises(AttributeError):
                    caught.exception.commit_state = CommitState.UNKNOWN

    def test_allocation_retries_then_uses_only_returned_uuid(self):
        transport = Transport(allocations=[RpcError("UNAVAILABLE"), 123])
        self.assertEqual(self.publish(transport), 42)
        self.assertEqual(transport.allocation_calls, 2)
        self.assertEqual(transport.publish_calls[0]["uuid"], 123)

    def test_failed_or_invalid_allocation_cannot_publish(self):
        for values in ([RpcError("INTERNAL")] * 2, [0, False]):
            with self.subTest(values=values):
                transport = Transport(allocations=values)
                with self.assertRaises(ResultPublishError) as caught:
                    self.publish(transport, retries=1)
                self.assertIs(caught.exception.commit_state, CommitState.NOT_COMMITTED)
                self.assertIsNone(caught.exception.event_uuid)
                self.assertEqual(transport.publish_calls, [])

    def test_invalid_success_seq_remains_uncertain(self):
        transport = Transport(publishes=[SimpleNamespace(seq=0)])
        with self.assertRaises(ResultPublishError) as caught:
            self.publish(transport, retries=0)
        self.assertIs(caught.exception.commit_state, CommitState.UNKNOWN)
        self.assertIsNone(caught.exception.last_status)

    def test_pre_publish_hook_observes_frozen_request_before_first_rpc(self):
        transport = Transport(publishes=[RpcError("UNAVAILABLE"), SimpleNamespace(seq=42)])
        calls = []

        def hook(frozen):
            self.assertEqual(transport.publish_calls, [])
            self.assertEqual(transport.allocation_calls, 1)
            self.assertEqual((frozen.uuid, frozen.channel_id, frozen.principal), (99, 3, 4))
            calls.append(frozen)

        result = publish_request(create_client(transport, "token"), 3, 4, req(), before_publish=hook)
        self.assertEqual(result, 42)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].payload, transport.publish_calls[0]["payload"])

    def test_hook_failure_is_original_error_and_never_publishes(self):
        original = RuntimeError("client closed while allocating UUID")
        transport = Transport()

        def hook(frozen):
            raise original

        with self.assertRaises(RuntimeError) as caught:
            publish_request(create_client(transport, "token"), 3, 4, req(), before_publish=hook)
        self.assertIs(caught.exception, original)
        self.assertEqual(transport.publish_calls, [])

    def test_invalid_publish_arguments_do_not_allocate(self):
        for channel, principal in ((True, 2), (0, 2), (1, False), (1, -1)):
            transport = Transport()
            with self.assertRaises(PayloadValidationError):
                publish_infer_request(create_client(transport, "token"), channel, principal, req())
            self.assertEqual(transport.allocation_calls, 0)
        transport = Transport()
        with self.assertRaises(PayloadValidationError):
            publish_infer_result(create_client(transport, "token"), 1, 2, None,
                                 InferResultInput(stream_id="s", prev_seq=1, status_code=200, body=None))
        self.assertEqual(transport.allocation_calls, 0)

    def test_retry_configuration(self):
        for retries, interval in ((True, 1), (-1, 1), (0, False), (0, 0), (0, 1.0)):
            with self.subTest(retries=retries, interval=interval):
                with self.assertRaises(ConfigurationError):
                    create_client(Transport(), "token", retries, interval)
        for token in (None, "", 1):
            with self.assertRaises(ConfigurationError):
                create_client(Transport(), token)
        transport = Transport(publishes=[RpcError("UNAVAILABLE")])
        with self.assertRaises(ResultPublishError):
            self.publish(transport, retries=0)
        self.assertFalse(self.sleep.called)

    def test_internal_stop_before_first_rpc_or_publish_does_not_publish(self):
        for stop_before_uuid in (True, False):
            with self.subTest(stop_before_uuid=stop_before_uuid):
                stop = threading.Event()
                transport = Transport()
                if stop_before_uuid:
                    stop.set()
                with self.assertRaises(RPCStopped):
                    publish_request(create_client(transport, "token"), 3, 4, req(),
                                    stop_event=stop, before_publish=lambda frozen: stop.set())
                self.assertEqual(transport.allocation_calls, 0 if stop_before_uuid else 1)
                self.assertEqual(transport.publish_calls, [])

    def test_internal_stop_after_publish_keeps_its_commit_evidence(self):
        for status, expected in ((None, None), ("PERMISSION_DENIED", CommitState.NOT_COMMITTED),
                                 ("UNAVAILABLE", CommitState.UNKNOWN), ("ALREADY_EXISTS", CommitState.COMMITTED)):
            with self.subTest(status=status):
                stop = threading.Event()
                transport = Transport(publishes=[SimpleNamespace(seq=42) if status is None else RpcError(status)])
                publish = transport.publish_auto_seq

                def finish_then_stop(**kwargs):
                    try:
                        return publish(**kwargs)
                    finally:
                        stop.set()

                transport.publish_auto_seq = finish_then_stop
                if status is None:
                    self.assertEqual(publish_request(create_client(transport, "token"), 3, 4, req(),
                                                     stop_event=stop), 42)
                else:
                    with self.assertRaises(ResultPublishError) as caught:
                        publish_request(create_client(transport, "token"), 3, 4, req(), stop_event=stop)
                    self.assertIs(caught.exception.commit_state, expected)
                    self.assertEqual(caught.exception.event_uuid, 99)
                    self.assertEqual(caught.exception.last_status, status)
                self.assertEqual(len(transport.publish_calls), 1)
                self.assertEqual(transport.query_calls, [])

    def test_internal_stop_during_uuid_allocation_keeps_original_failure(self):
        stop = threading.Event()
        original = RpcError("UNAVAILABLE")
        transport = Transport(allocations=[original])
        allocate = transport.get_uuid

        def allocate_then_stop():
            try:
                return allocate()
            finally:
                stop.set()

        transport.get_uuid = allocate_then_stop
        with self.assertRaises(ResultPublishError) as caught:
            publish_request(create_client(transport, "token"), 3, 4, req(), stop_event=stop)
        self.assertIs(caught.exception.commit_state, CommitState.NOT_COMMITTED)
        self.assertIs(caught.exception.__cause__, original)
        self.assertIsNone(caught.exception.event_uuid)
        self.assertEqual(transport.allocation_calls, 1)
        self.assertEqual(transport.publish_calls, [])

    def test_internal_stop_during_uuid_query_keeps_success_or_committed_error(self):
        for result in (42, RpcError("UNAVAILABLE")):
            with self.subTest(result=result):
                stop = threading.Event()
                transport = Transport(publishes=[RpcError("ALREADY_EXISTS")], queries=[result])
                query = transport.get_seq_by_uuid

                def query_then_stop(uuid):
                    try:
                        return query(uuid)
                    finally:
                        stop.set()

                transport.get_seq_by_uuid = query_then_stop
                if isinstance(result, int):
                    self.assertEqual(publish_request(create_client(transport, "token"), 3, 4, req(),
                                                     stop_event=stop), result)
                else:
                    with self.assertRaises(ResultPublishError) as caught:
                        publish_request(create_client(transport, "token"), 3, 4, req(), stop_event=stop)
                    self.assertIs(caught.exception.commit_state, CommitState.COMMITTED)
                    self.assertEqual(caught.exception.last_status, "UNAVAILABLE")
                    self.assertIs(caught.exception.__cause__, result)
                self.assertEqual(transport.query_calls, [99])
                self.assertEqual(len(transport.publish_calls), 1)

    def test_internal_rpc_guard_rechecks_each_retry_without_losing_last_error(self):
        for stage, stopped_attempt, expected in (("uuid", 2, CommitState.NOT_COMMITTED),
                                                 ("publish", 3, CommitState.UNKNOWN),
                                                 ("query", 4, CommitState.COMMITTED)):
            with self.subTest(stage=stage):
                transport = Transport(
                    allocations=[RpcError("UNAVAILABLE")] if stage == "uuid" else None,
                    publishes=[RpcError("ALREADY_EXISTS" if stage == "query" else "UNAVAILABLE")],
                    queries=[RpcError("UNAVAILABLE")],
                )
                attempts = []

                def guard():
                    attempts.append(1)
                    if len(attempts) == stopped_attempt:
                        raise RPCStopped("Stopped at the attempt boundary")

                with self.assertRaises(ResultPublishError) as caught:
                    publish_request(create_client(transport, "token"), 3, 4, req(), before_rpc=guard)
                self.assertIs(caught.exception.commit_state, expected)
                self.assertEqual(caught.exception.last_status, "UNAVAILABLE")
                self.assertEqual(transport.allocation_calls, 1)
                self.assertEqual(len(transport.publish_calls), 0 if stage == "uuid" else 1)
                self.assertEqual(len(transport.query_calls), 1 if stage == "query" else 0)


class RpcTests(unittest.TestCase):
    def test_ordinary_retry_error_classification_and_original_exception(self):
        for status in ("UNAUTHENTICATED", "PERMISSION_DENIED", "NOT_FOUND", "INVALID_ARGUMENT",
                       "RESOURCE_EXHAUSTED", "OUT_OF_RANGE", "UNIMPLEMENTED"):
            original = RpcError(status)
            calls = []

            def operation():
                calls.append(1)
                raise original

            with self.assertRaises(RpcError) as caught:
                call_rpc(operation, 3, 1000)
            self.assertIs(caught.exception, original)
            self.assertEqual(len(calls), 1)
        with patch("openevent.model_proxy_sdk.rpc.time.sleep") as sleep:
            for status in ("CANCELLED", "DEADLINE_EXCEEDED", "UNKNOWN", "UNAVAILABLE", "INTERNAL"):
                values = [RpcError(status), 42]
                self.assertEqual(call_rpc(lambda: outcome(values), 1, 25), 42)
            self.assertEqual(sleep.call_count, 5)
            sleep.assert_called_with(0.025)

    def test_close_interrupts_retry_wait(self):
        stop = threading.Event()
        original = OSError("disconnected")

        def operation():
            stop.set()
            raise original

        with self.assertRaises(OSError) as caught:
            call_rpc(operation, 3, 1000, stop_event=stop)
        self.assertIs(caught.exception, original)
