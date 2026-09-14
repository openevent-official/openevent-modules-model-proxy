import json
import sys
import unittest

from openevent.sdk import openevent_pb2
from openevent.model_proxy_sdk import (
    InferAppend, InferAppendInput, InferCancel, InferCancelInput,
    InferEnd, InferEndInput, InferRequest, InferRequestInput, InferResult,
    InferResultInput, PayloadValidationError, UNSET, parse_message, parse_payload,
)
from openevent.model_proxy_sdk.errors import (
    APIError, APIStatusError, APIConnectionError, APITimeoutError,
    BadRequestError, AuthenticationError, PermissionDeniedError, NotFoundError,
    ConflictError, UnprocessableEntityError, RateLimitError, InternalServerError,
    StreamCancelledError, OpenEventSubscriptionError, ProtocolError, make_api_error,
)


def request(**overrides):
    data = dict(kind="infer.request", stream_id="s_1", ts_ms=0, method="POST",
                path="/v1/chat/completions", body={})
    data.update(overrides)
    return data


class ProtocolTests(unittest.TestCase):
    def parse(self, data):
        return parse_payload(json.dumps(data).encode())

    def test_five_messages_roundtrip_and_read_only_json(self):
        cases = [
            (InferRequestInput(stream_id="s", method="POST", path="/v1/responses",
                               body={"input": ["你好"], "stream": True}, provider="p", prev_seq=3), InferRequest),
            (InferResultInput(stream_id="s", prev_seq=7, status_code=429,
                              headers=[{"name": "retry-after", "value": " 1 "}], body=None), InferResult),
            (InferAppendInput(stream_id="s", request_seq=7, prev_seq=8, body=[None, False, 1.5]), InferAppend),
            (InferEndInput(stream_id="s", request_seq=7, status_code=200,
                           end_status="completed", body={"type": "response.completed"}), InferEnd),
            (InferCancelInput(stream_id="s", request_seq=7), InferCancel),
        ]
        for source, model in cases:
            with self.subTest(model=model):
                parsed = parse_payload(source.to_payload(321))
                self.assertIsInstance(parsed, model)
                self.assertEqual(parsed.ts_ms, 321)
                self.assertEqual(parsed.stream_id, "s")
                with self.assertRaises(AttributeError):
                    parsed.stream_id = "changed"
        parsed = parse_payload(cases[0][0].to_payload(0))
        body = parsed.body
        body["input"].append("changed")
        self.assertEqual(parsed.body["input"], ["你好"])
        parsed = parse_payload(cases[1][0].to_payload(0))
        parsed.headers[0]["value"] = "changed"
        self.assertEqual(parsed.headers[0]["value"], " 1 ")

    def test_input_freezes_caller_owned_json(self):
        body = {"input": ["original"]}
        item = InferRequestInput(stream_id="s", method="POST", path="/v1/responses", body=body)
        body["input"].append("later")
        self.assertEqual(parse_payload(item.to_payload(0)).body, {"input": ["original"]})

    def test_direct_model_construction_validates_and_freezes_json(self):
        data = request(body={"input": ["original"]})
        parsed = InferRequest(data)
        data["body"]["input"].append("later")
        self.assertEqual(parsed.body, {"input": ["original"]})
        with self.assertRaises(PayloadValidationError) as caught:
            InferRequest(request(body={"stream": 1}))
        self.assertEqual(caught.exception.code, "INVALID_BODY")
        with self.assertRaises(PayloadValidationError) as caught:
            InferCancel(request())
        self.assertEqual(caught.exception.code, "INVALID_KIND")

    def test_missing_body_is_distinct_from_null(self):
        for body in (UNSET, None, False, 0, [], {}):
            with self.subTest(body=body):
                result = InferResultInput(stream_id="s", prev_seq=1, status_code=200, body=body)
                end = InferEndInput(stream_id="s", request_seq=1, status_code=200,
                                    end_status="completed", body=body)
                for item in (result, end):
                    parsed = parse_payload(item.to_payload(0))
                    self.assertEqual(parsed.has_body, body is not UNSET)
                    self.assertEqual("body" in parsed.to_dict(), body is not UNSET)
                    if body is not UNSET:
                        self.assertEqual(parsed.body, body)

    def test_optional_none_omits_fields(self):
        parsed = parse_payload(InferRequestInput(stream_id="s", method="POST",
                                                path="/v1/responses", body={}).to_payload(0))
        self.assertNotIn("provider", parsed.to_dict())
        self.assertNotIn("prev_seq", parsed.to_dict())
        result = InferResultInput(stream_id="s", prev_seq=1, status_code=200)
        self.assertNotIn("headers", parse_payload(result.to_payload(0)).to_dict())

    def test_boolean_and_invalid_control_fields(self):
        cases = [
            ({"stream_id": ""}, "INVALID_STREAM_ID"),
            ({"stream_id": "中文"}, "INVALID_STREAM_ID"),
            ({"stream_id": "s\n"}, "INVALID_STREAM_ID"),
            ({"stream_id": "a" * 129}, "INVALID_STREAM_ID"),
            ({"ts_ms": True}, "INVALID_TS_MS"),
            ({"ts_ms": -1}, "INVALID_TS_MS"),
            ({"provider": ""}, "INVALID_PROVIDER"),
            ({"provider": None}, "INVALID_PROVIDER"),
            ({"method": "GET"}, "INVALID_METHOD"),
            ({"path": "/other"}, "INVALID_PATH"),
            ({"prev_seq": True}, "INVALID_PREV_SEQ"),
            ({"body": []}, "INVALID_BODY"),
            ({"body": {"stream": 1}}, "INVALID_BODY"),
            ({"extra": 1}, "UNKNOWN_FIELD"),
            ({"kind": "infer.other"}, "INVALID_KIND"),
        ]
        for fields, code in cases:
            with self.subTest(fields=fields):
                with self.assertRaises(PayloadValidationError) as caught:
                    self.parse(request(**fields))
                self.assertEqual(caught.exception.code, code)

    def test_json_failures_have_no_partial_context(self):
        for payload in (b"\xff", b"{", b"{} trailing", b'\xef\xbb\xbf{}', b'NaN',
                        b'{"kind":"infer.request","body":Infinity}', b"1e9999", "{}"):
            with self.subTest(payload=payload):
                with self.assertRaises(PayloadValidationError) as caught:
                    parse_payload(payload)
                self.assertEqual(caught.exception.code, "INVALID_JSON")
                self.assertIsNone(caught.exception.kind)
                self.assertIsNone(caught.exception.stream_id)

    def test_diagnostic_fields_are_independently_validated(self):
        for data, expected_kind, expected_stream in [
            (None, None, None), (False, None, None), (1, None, None),
            ("text", None, None), ([], None, None), ({}, None, None),
            ({"kind": "bad", "stream_id": "s"}, None, "s"),
            ({"kind": [], "stream_id": "s"}, None, "s"),
            ({"kind": {}, "stream_id": "s"}, None, "s"),
            (request(stream_id="?"), "infer.request", None),
            (request(method="GET"), "infer.request", "s_1"),
        ]:
            with self.subTest(data=data):
                with self.assertRaises(PayloadValidationError) as caught:
                    self.parse(data)
                self.assertEqual(caught.exception.kind, expected_kind)
                self.assertEqual(caught.exception.stream_id, expected_stream)
                with self.assertRaises(AttributeError):
                    caught.exception.code = "changed"

    def test_missing_required_fields(self):
        data = request()
        del data["body"]
        with self.assertRaises(PayloadValidationError) as caught:
            self.parse(data)
        self.assertEqual(caught.exception.code, "MISSING_REQUIRED_FIELD")

    def test_input_body_rejects_non_json_and_cycles(self):
        cycle = []
        cycle.append(cycle)
        for body in (object(), (1, 2), {1: "wrong key"}, float("nan"), float("inf"), cycle):
            with self.subTest(body=type(body)):
                with self.assertRaises(PayloadValidationError) as caught:
                    InferAppendInput(stream_id="s", request_seq=1, prev_seq=2, body=body)
                self.assertEqual(caught.exception.code, "INVALID_BODY")

    def test_result_status_shape_matrix(self):
        for status in (100, 200, 599, 60000, 60001, 60002, 60003, 60005, 60007, 60008, 60009):
            self.assertEqual(InferResultInput(stream_id="s", prev_seq=1, status_code=status, body=None).status_code,
                             status)
        for status, body in ((True, None), (99, None), (600, None), (60004, None),
                             (60006, None), (60007, UNSET), (60009, UNSET)):
            with self.subTest(status=status, body=body):
                with self.assertRaises(PayloadValidationError) as caught:
                    InferResultInput(stream_id="s", prev_seq=1, status_code=status, body=body)
                self.assertEqual(caught.exception.code, "INVALID_STATUS_CODE")

    def test_end_shape_matrix(self):
        for state, status, body in (("completed", 429, UNSET), ("failed", 200, None),
                                    ("interrupted", 60003, {})):
            InferEndInput(stream_id="s", request_seq=1, status_code=status, end_status=state, body=body)
        for state, status, body, code in (
            ("bad", 200, {}, "INVALID_END_STATUS"),
            ("failed", 200, UNSET, "MISSING_REQUIRED_FIELD"),
            ("interrupted", 60007, UNSET, "MISSING_REQUIRED_FIELD"),
            ("completed", 60007, {}, "INVALID_STATUS_CODE"),
            ("interrupted", 200, {}, "INVALID_STATUS_CODE"),
            ("interrupted", 60004, {}, "INVALID_STATUS_CODE"),
        ):
            with self.subTest(state=state, status=status):
                with self.assertRaises(PayloadValidationError) as caught:
                    InferEndInput(stream_id="s", request_seq=1, status_code=status, end_status=state, body=body)
                self.assertEqual(caught.exception.code, code)
        data = dict(kind="infer.end", stream_id="s", request_seq=1, status_code=200,
                    end_status="completed", ts_ms=0, prev_seq=2)
        with self.assertRaises(PayloadValidationError) as caught:
            self.parse(data)
        self.assertEqual(caught.exception.code, "UNKNOWN_FIELD")

    def test_header_order_duplicates_and_validation(self):
        headers = [{"name": "x-request-id", "value": "first"},
                   {"name": "x-request-id", "value": "second"},
                   {"name": "x-ratelimit-tokens", "value": "5"}]
        result = InferResultInput(stream_id="s", prev_seq=1, status_code=200, headers=headers)
        self.assertEqual(parse_payload(result.to_payload(0)).headers, headers)
        for bad in ([], {}, [{"name": "Content-Type", "value": "x"}],
                    [{"name": "authorization", "value": "x"}],
                    [{"name": "content-type", "value": 1}],
                    [{"name": "content-type", "value": "x", "extra": "x"}]):
            with self.subTest(headers=bad):
                with self.assertRaises(PayloadValidationError) as caught:
                    InferResultInput(stream_id="s", prev_seq=1, status_code=200, headers=bad)
                self.assertEqual(caught.exception.code, "INVALID_HEADERS")

    def test_message_metadata_and_payload_timestamps_stay_separate(self):
        message = openevent_pb2.EventMessage(uuid=10, seq=11, channel_id=12, principal=13,
                                           recipients=[14], ts_ms=456,
                                           payload=InferCancelInput(stream_id="s", request_seq=1).to_payload(123))
        parsed = parse_message(message)
        self.assertEqual((parsed.uuid, parsed.seq, parsed.channel_id, parsed.principal), (10, 11, 12, 13))
        self.assertEqual(parsed.recipients, (14,))
        self.assertEqual((parsed.ts_ms, parsed.payload.ts_ms), (456, 123))

    def test_seq_types_and_large_timestamp(self):
        self.assertEqual(self.parse(request(ts_ms=2 ** 256)).ts_ms, 2 ** 256)
        for value in (True, 0, -1, 1.0, None):
            with self.subTest(value=value):
                with self.assertRaises(PayloadValidationError) as caught:
                    InferCancelInput(stream_id="s", request_seq=value)
                self.assertEqual(caught.exception.code, "INVALID_REQUEST_SEQ")

    def test_timestamp_and_body_integers_exceed_python_decimal_digit_limit(self):
        digit_limit = getattr(sys, "get_int_max_str_digits", lambda: None)
        before = digit_limit()
        integer = 10 ** 5000 + 9
        body = {"integer": integer, "nested": [-integer, True, None, "中文\ud800"]}
        request_input = InferRequestInput(
            stream_id="big", method="POST", path="/v1/responses", body=body,
        )
        encoded = request_input.to_payload(integer)
        parsed = parse_payload(encoded)
        self.assertEqual(parsed.ts_ms, integer)
        self.assertEqual(parsed.body, body)
        # Decode independently constructed wire JSON, not only our own encoder.
        raw = (b'{"kind":"infer.cancel","stream_id":"big","request_seq":1,"ts_ms":'
               + b"1" + b"0" * 5000 + b"}")
        self.assertEqual(parse_payload(raw).ts_ms, 10 ** 5000)
        self.assertEqual(digit_limit(), before)


class ErrorTests(unittest.TestCase):
    def test_status_mapping_and_failed_end_precedence(self):
        cases = {100: APIStatusError, 300: APIStatusError, 400: BadRequestError,
                 401: AuthenticationError, 403: PermissionDeniedError, 404: NotFoundError,
                 409: ConflictError, 422: UnprocessableEntityError, 429: RateLimitError,
                 500: InternalServerError, 599: InternalServerError, 60000: APITimeoutError,
                 60001: APIConnectionError, 60002: APIConnectionError, 60003: APIConnectionError,
                 60004: StreamCancelledError, 60005: APIError, 60007: APIError,
                 60008: APIError, 60009: BadRequestError}
        for status, cls in cases.items():
            with self.subTest(status=status):
                error = make_api_error(status, stream_id="s", request_seq=1, body={"raw": [1]})
                self.assertIs(type(error), cls)
                self.assertEqual(error.status_code, status)
                self.assertEqual(error.request_seq, 1)
                error.body["raw"].append(2)
                self.assertEqual(error.body, {"raw": [1]})
                with self.assertRaises(AttributeError):
                    error.request_seq = 2
        self.assertIsNone(make_api_error(200))
        self.assertIsNone(make_api_error(299, end_status="completed"))
        self.assertIs(type(make_api_error(200, end_status="failed")), APIError)

    def test_subscription_error_has_only_instance_context(self):
        cause = ProtocolError(stream_id="s", request_seq=1)
        error = OpenEventSubscriptionError(reason="protocol", protocol_error=cause)
        self.assertIs(error.protocol_error, cause)
        self.assertEqual(error.reason, "protocol")
        self.assertIsNone(error.last_status)
        for name in ("stream_id", "request_seq", "headers", "body", "end_status"):
            self.assertFalse(hasattr(error, name))
        with self.assertRaises(AttributeError):
            error.reason = "rpc"
