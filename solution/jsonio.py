"""Deterministic UTF-8 JSON input/output.

Reading is strict: duplicate object keys and non-standard constants such as
``NaN`` are rejected instead of silently overwritten, because both would
make a "reproducible" case document ambiguous.  Writing is canonical:
sorted keys, ``", "``/``": "`` separators, trailing newline, ``ensure_ascii``
left on so bytes are identical on every Windows console code page.
"""

import json

from .errors import SimError


def _no_duplicates(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise SimError("E_JSON_DUPLICATE_KEY", "",
                           "duplicate object key %r" % key)
        out[key] = value
    return out


def _reject_constant(name):
    raise SimError("E_JSON_INVALID", "", "%s is not valid JSON" % name)


def loads(text):
    try:
        return json.loads(text, object_pairs_hook=_no_duplicates,
                          parse_constant=_reject_constant)
    except SimError:
        raise
    except json.JSONDecodeError as exc:
        raise SimError("E_JSON_INVALID", "",
                       "line %d column %d: %s"
                       % (exc.lineno, exc.colno, exc.msg)) from None
    except (ValueError, RecursionError) as exc:
        raise SimError("E_JSON_INVALID", "", str(exc)) from None


def read(path):
    try:
        with open(path, "rb") as handle:
            raw = handle.read()
    except OSError as exc:
        raise SimError("E_INPUT_UNREADABLE", "",
                       "cannot read %s: %s" % (path, exc.strerror or exc))
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise SimError("E_INPUT_ENCODING", "",
                       "input is not UTF-8: %s" % exc.reason)
    return loads(text)


def dumps(payload):
    return json.dumps(payload, sort_keys=True, ensure_ascii=True,
                      indent=2, separators=(",", ": ")) + "\n"


def write_stdout(payload, stream):
    try:
        stream.write(dumps(payload))
        stream.flush()
    except OSError:  # pragma: no cover - closed pipe
        pass
