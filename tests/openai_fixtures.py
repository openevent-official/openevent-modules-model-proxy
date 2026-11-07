"""Text-only OpenAI fixtures shared by the fake provider and its assertions.

Shapes and event order follow the official Chat Completions and Responses API:
https://developers.openai.com/api/reference/resources/chat/subresources/completions
https://developers.openai.com/api/reference/resources/responses/methods/create
https://developers.openai.com/api/reference/resources/responses/streaming-events
"""


def responses_request(**overrides):
    return {"model": "fake", "input": "hello", **overrides}


def chat_request(**overrides):
    return {"model": "fake", "messages": [{"role": "user", "content": "hello"}], **overrides}


def http_error_body():
    return {"error": {"message": "rate limited", "type": "rate_limit_error",
                      "param": None, "code": "rate_limit_exceeded"}}


def chat_body(text="ok"):
    return {
        "id": "chatcmpl_fake", "object": "chat.completion", "created": 1700000000,
        "model": "fake", "choices": [{"index": 0, "finish_reason": "stop", "logprobs": None,
            "message": {"role": "assistant", "content": text, "refusal": None}}],
    }


def chat_chunk(delta, *, finish_reason=None):
    return {
        "id": "chatcmpl_fake", "object": "chat.completion.chunk", "created": 1700000000,
        "model": "fake", "choices": [{"index": 0, "delta": delta,
                                      "finish_reason": finish_reason, "logprobs": None}],
    }


def chat_chunks():
    return [chat_chunk({"role": "assistant", "content": "hello"}),
            chat_chunk({"content": "world"}), chat_chunk({}, finish_reason="stop")]


def output_text(text):
    return {"type": "output_text", "text": text, "annotations": [], "logprobs": []}


def output_message(text, *, status="completed"):
    return {"id": "msg_fake", "type": "message", "role": "assistant", "status": status,
            "content": [output_text(text)]}


def response_body(text="ok", *, status="completed"):
    return {
        "id": "resp_fake", "object": "response", "created_at": 1700000000,
        "access_programs": None, "status": status,
        "error": {"code": "server_error", "message": "generation failed"} if status == "failed" else None,
        "incomplete_details": {"reason": "max_output_tokens"} if status == "incomplete" else None,
        "instructions": None, "metadata": {}, "model": "fake",
        "output": [] if status == "in_progress" else [output_message(
            text, status="completed" if status == "completed" else "incomplete")],
        "parallel_tool_calls": True, "temperature": 1, "tool_choice": "auto", "tools": [], "top_p": 1,
    }


def response_delta(text, *, sequence_number):
    return {"type": "response.output_text.delta", "sequence_number": sequence_number,
            "item_id": "msg_fake", "output_index": 0, "content_index": 0,
            "delta": text, "logprobs": []}


def response_start_events():
    item = output_message("", status="in_progress")
    item["content"] = []
    return [
        {"type": "response.created", "sequence_number": 0,
         "response": response_body(status="in_progress")},
        {"type": "response.in_progress", "sequence_number": 1,
         "response": response_body(status="in_progress")},
        {"type": "response.output_item.added", "sequence_number": 2, "output_index": 0, "item": item},
        {"type": "response.content_part.added", "sequence_number": 3,
         "item_id": "msg_fake", "output_index": 0, "content_index": 0, "part": output_text("")},
        response_delta("hello", sequence_number=4),
        response_delta("world", sequence_number=5),
    ]


def response_end_events(status="completed"):
    events = []
    if status == "completed":
        events = [
            {"type": "response.output_text.done", "sequence_number": 6, "item_id": "msg_fake",
             "output_index": 0, "content_index": 0, "text": "helloworld", "logprobs": []},
            {"type": "response.content_part.done", "sequence_number": 7, "item_id": "msg_fake",
             "output_index": 0, "content_index": 0, "part": output_text("helloworld")},
            {"type": "response.output_item.done", "sequence_number": 8,
             "output_index": 0, "item": output_message("helloworld")},
        ]
    return [*events, {"type": f"response.{status}", "sequence_number": 6 + len(events),
                     "response": response_body("helloworld", status=status)}]
