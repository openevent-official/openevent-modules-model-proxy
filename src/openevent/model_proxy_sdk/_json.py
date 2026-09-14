"""JSON integers without Python's process-wide decimal digit limit.

Use the standard codec normally. The local fallbacks handle large integers in
small decimal groups without changing interpreter settings for the host app.
"""

import json


_DIGITS = 500
_BASE = 10 ** _DIGITS


def _parse_int(text):
    try:
        return int(text)
    except ValueError:
        negative = text.startswith("-")
        digits = text[1:] if negative else text
        value = 0
        for offset in range(0, len(digits), _DIGITS):
            group = digits[offset:offset + _DIGITS]
            value = value * 10 ** len(group) + int(group)
        return -value if negative else value


def _integer_text(value):
    try:
        return str(value)
    except ValueError:
        negative = value < 0
        value = abs(value)
        groups = []
        while value:
            value, remainder = divmod(value, _BASE)
            groups.append(str(remainder))
        text = groups.pop() + "".join(group.zfill(_DIGITS) for group in reversed(groups))
        return "-" + text if negative else text


def loads(value, **kwargs):
    return json.loads(value, parse_int=_parse_int, **kwargs)


def dumps(value, *, ensure_ascii=True, allow_nan=False, separators=(",", ":")):
    options = dict(ensure_ascii=ensure_ascii, allow_nan=allow_nan, separators=separators)
    try:
        return json.dumps(value, **options)
    except ValueError:
        # A large integer may occur anywhere in an otherwise ordinary JSON
        # value. Keep standard escaping/float handling for every other scalar.
        active = set()

        def encode(item):
            if type(item) is int:
                return _integer_text(item)
            if type(item) not in (dict, list):
                return json.dumps(item, **options)
            if id(item) in active:
                raise ValueError("Circular reference detected")
            active.add(id(item))
            try:
                if type(item) is dict:
                    return "{" + separators[0].join(
                        json.dumps(key, **options) + separators[1] + encode(member)
                        for key, member in item.items()
                    ) + "}"
                return "[" + separators[0].join(encode(member) for member in item) + "]"
            finally:
                active.remove(id(item))

        return encode(value)
