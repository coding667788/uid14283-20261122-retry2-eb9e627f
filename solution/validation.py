"""Strict structural validators for case documents.

The validators are deliberately independent of the semantic engine: they
only answer "is this document shaped the way the protocol requires, and if
not, where exactly is it wrong?".  Every failure carries a stable code and a
JSON-pointer path such as ``batches/1/events/3/seq``.

JSON has no fixed-width integers, so numeric fields are checked for type
(``bool`` is not an integer) and then for range.  Identifiers are restricted
to printable ASCII when ``control.strict_fields`` is true so that dumps are
stable across terminals.
"""

import re

from .errors import SimError

# ---------------------------------------------------------------------------
# primitive helpers
# ---------------------------------------------------------------------------

_IDENT_RE = re.compile(r"[A-Za-z0-9._:\-]+\Z")

GAP_KINDS = ("PROVE", "CANCEL")
FAILPOINTS = ("before_commit", "after_commit")

def _fail(code, path, detail):
    raise SimError(code, path, detail)


def _join(path, key):
    return "%s/%s" % (path, key) if path else str(key)


def join(path, *parts):
    out = path
    for part in parts:
        out = _join(out, part)
    return out


def as_object(value, path):
    if not isinstance(value, dict):
        _fail("E_TYPE_EXPECTED_OBJECT", path, "expected an object")
    return value


def as_list(value, path):
    if not isinstance(value, list):
        _fail("E_TYPE_EXPECTED_ARRAY", path, "expected an array")
    return value


def as_string(value, path, *, max_len=None, min_len=1,
              code="E_TYPE_EXPECTED_STRING"):
    if not isinstance(value, str):
        _fail(code, path, "expected a string")
    if len(value) < min_len:
        _fail("E_VALUE_EMPTY", path, "string must not be empty")
    if max_len is not None and len(value) > max_len:
        _fail("E_VALUE_TOO_LONG", path,
              "string longer than %d code points" % max_len)
    if "\x00" in value:
        _fail("E_VALUE_INVALID", path, "NUL is not allowed")
    return value


def as_int(value, path, *, minimum=None, maximum=None):
    if isinstance(value, bool) or not isinstance(value, int):
        _fail("E_TYPE_EXPECTED_INTEGER", path, "expected an integer")
    if minimum is not None and value < minimum:
        _fail("E_VALUE_OUT_OF_RANGE", path, "must be >= %d" % minimum)
    if maximum is not None and value > maximum:
        _fail("E_VALUE_OUT_OF_RANGE", path, "must be <= %d" % maximum)
    return value


def as_bool(value, path):
    if not isinstance(value, bool):
        _fail("E_TYPE_EXPECTED_BOOLEAN", path, "expected a boolean")
    return value


def require(obj, key, path):
    if key not in obj:
        _fail("E_FIELD_MISSING", _join(path, key), "field is required")
    return obj[key]


def reject_unknown(obj, allowed, path):
    for key in obj:
        if key not in allowed:
            _fail("E_FIELD_UNKNOWN", _join(path, key), "field is not allowed")


# ---------------------------------------------------------------------------
# control block
# ---------------------------------------------------------------------------

_CONTROL_KEYS = {"max_window", "max_batch", "batch_size", "strict_fields",
                 "fail_after", "failpoint"}


def validate_control(obj):
    as_object(obj, "control")
    reject_unknown(obj, _CONTROL_KEYS, "control")
    max_window = as_int(obj.get("max_window", 8), "control/max_window",
                        minimum=1, maximum=256)
    max_batch = as_int(obj.get("max_batch", 64), "control/max_batch",
                       minimum=1, maximum=4096)
    batch_size = obj.get("batch_size")
    if batch_size is not None:
        batch_size = as_int(batch_size, "control/batch_size",
                            minimum=1, maximum=4096)
        if batch_size > max_batch:
            _fail("E_VALUE_OUT_OF_RANGE", "control/batch_size",
                  "batch_size must not exceed max_batch (%d)" % max_batch)
    strict = as_bool(obj.get("strict_fields", True),
                     "control/strict_fields")
    fail_after = obj.get("fail_after")
    if fail_after is not None:
        fail_after = as_int(fail_after, "control/fail_after",
                            minimum=0, maximum=10_000)
    failpoint = obj.get("failpoint")
    if failpoint is not None:
        failpoint = as_string(failpoint, "control/failpoint", max_len=32)
        if failpoint not in FAILPOINTS:
            _fail("E_VALUE_INVALID", "control/failpoint",
                  "expected one of: %s" % ", ".join(FAILPOINTS))
    return {"max_window": max_window, "max_batch": max_batch,
            "batch_size": batch_size, "strict_fields": strict,
            "fail_after": fail_after, "failpoint": failpoint}


# ---------------------------------------------------------------------------
# identifier / scalar shapes
# ---------------------------------------------------------------------------


class CaseContext:
    __slots__ = ("case_id", "max_id_len", "strict_fields")

    def __init__(self, case_id, max_id_len, strict_fields):
        self.case_id = case_id
        self.max_id_len = max_id_len
        self.strict_fields = strict_fields

    def ident(self, value, path):
        text = as_string(value, path, max_len=self.max_id_len)
        if self.strict_fields and not _IDENT_RE.match(text):
            _fail("E_VALUE_INVALID", path,
                  "identifiers allow [A-Za-z0-9._:-] only when "
                  "strict_fields is true")
        return text


def _seq(value, path):
    return as_int(value, path, minimum=0, maximum=2 ** 31 - 1)


def _fingerprint(value, path):
    text = as_string(value, path, max_len=64, min_len=0,
                     code="E_FINGERPRINT_INVALID")
    if len(text) != 64 or any(c not in "0123456789abcdef" for c in text):
        _fail("E_FINGERPRINT_INVALID", path,
              "fingerprint must be 64 lower-case hex characters")
    return text


def _time(value, path):
    return as_int(value, path, minimum=0, maximum=2 ** 63 - 1)


# ---------------------------------------------------------------------------
# rules
# ---------------------------------------------------------------------------

def _routes(value, path, ctx):
    routes = as_list(value, path)
    out = []
    seen = set()
    for i, route in enumerate(routes):
        rp = join(path, i)
        as_object(route, rp)
        reject_unknown(route, {"dst", "allow"}, rp)
        dst = ctx.ident(require(route, "dst", rp), join(rp, "dst"))
        allow = as_bool(require(route, "allow", rp), join(rp, "allow"))
        if dst in seen:
            _fail("E_DUP_ROUTE", join(rp, "dst"),
                  "a rule may mention a destination once")
        seen.add(dst)
        out.append({"dst": dst, "allow": allow})
    return out


def validate_rule(rule, path, ctx):
    as_object(rule, path)
    reject_unknown(rule, {"id", "precedence", "effective_epoch", "routes"},
                   path)
    rule_id = ctx.ident(require(rule, "id", path), join(path, "id"))
    precedence = as_int(rule.get("precedence", 0), join(path, "precedence"),
                        minimum=0, maximum=1_000_000)
    effective = as_int(require(rule, "effective_epoch", path),
                       join(path, "effective_epoch"), minimum=0)
    routes = _routes(require(rule, "routes", path), join(path, "routes"), ctx)
    return {"id": rule_id, "precedence": precedence,
            "effective_epoch": effective, "routes": routes,
            "_path": path}


def validate_rules(value, ctx):
    items = as_list(value, "rules")
    out = []
    seen = set()
    for i, rule in enumerate(items):
        parsed = validate_rule(rule, join("rules", i), ctx)
        # A rule id may appear more than once: each appearance is one version
        # of the same policy, keyed by (id, effective_epoch).  Two versions at
        # the same epoch are a duplicate, not an evolution.
        key = (parsed["id"], parsed["effective_epoch"])
        if key in seen:
            _fail("E_DUP_RULE", join(join("rules", i), "effective_epoch"),
                  "rule %r already has a version at epoch %d"
                  % (parsed["id"], parsed["effective_epoch"]))
        seen.add(key)
        out.append(parsed)
    return out


# ---------------------------------------------------------------------------
# announcements
# ---------------------------------------------------------------------------

def validate_announcements(value, ctx):
    items = as_list(value, "announcements")
    out = []
    seen = set()
    for i, ann in enumerate(items):
        p = join("announcements", i)
        as_object(ann, p)
        reject_unknown(ann, {"src", "gen", "start_seq", "prev_end_seq"}, p)
        src = ctx.ident(require(ann, "src", p), join(p, "src"))
        gen = _seq(require(ann, "gen", p), join(p, "gen"))
        if gen < 1:
            _fail("E_VALUE_OUT_OF_RANGE", join(p, "gen"), "must be >= 1")
        start = _seq(require(ann, "start_seq", p), join(p, "start_seq"))
        prev = ann.get("prev_end_seq")
        if prev is not None:
            prev = _seq(prev, join(p, "prev_end_seq"))
        if (src, gen) in seen:
            _fail("E_DUP_ANNOUNCEMENT", join(p, "gen"),
                  "a (src, gen) pair may be announced once")
        seen.add((src, gen))
        out.append({"src": src, "gen": gen, "start_seq": start,
                    "prev_end_seq": prev, "index": i, "_path": p})
    return out


# ---------------------------------------------------------------------------
# events and batches
# ---------------------------------------------------------------------------

_EVENT_KEYS = {"gateway", "src", "gen", "seq", "dst", "business_id",
               "fingerprint", "created_at", "predecessors"}


def validate_refs(value, path, ctx):
    items = as_list(value, path)
    out = []
    for j, ref in enumerate(items):
        rp = join(path, j)
        as_object(ref, rp)
        reject_unknown(ref, {"src", "gen", "seq"}, rp)
        src = ctx.ident(require(ref, "src", rp), join(rp, "src"))
        gen = _seq(require(ref, "gen", rp), join(rp, "gen"))
        seq = _seq(require(ref, "seq", rp), join(rp, "seq"))
        if gen < 1:
            _fail("E_VALUE_OUT_OF_RANGE", join(rp, "gen"), "must be >= 1")
        out.append({"src": src, "gen": gen, "seq": seq})
    return out


def validate_event(ev, path, ctx):
    as_object(ev, path)
    reject_unknown(ev, _EVENT_KEYS, path)
    parsed = {
        "gateway": ctx.ident(require(ev, "gateway", path),
                             join(path, "gateway")),
        "src": ctx.ident(require(ev, "src", path), join(path, "src")),
        "gen": _seq(require(ev, "gen", path), join(path, "gen")),
        "seq": _seq(require(ev, "seq", path), join(path, "seq")),
        "dst": ctx.ident(require(ev, "dst", path), join(path, "dst")),
        "business_id": ctx.ident(require(ev, "business_id", path),
                                 join(path, "business_id")),
        "fingerprint": _fingerprint(require(ev, "fingerprint", path),
                                    join(path, "fingerprint")),
        "created_at": _time(require(ev, "created_at", path),
                            join(path, "created_at")),
    }
    if parsed["gen"] < 1:
        _fail("E_VALUE_OUT_OF_RANGE", join(path, "gen"), "must be >= 1")
    parsed["predecessors"] = validate_refs(
        ev.get("predecessors", []), join(path, "predecessors"), ctx)
    parsed["_path"] = path
    return parsed


def validate_batches(value, ctx, max_batch, batch_size=None):
    items = as_list(value, "batches")
    out = []
    seen = set()
    for i, batch in enumerate(items):
        p = join("batches", i)
        as_object(batch, p)
        reject_unknown(batch, {"id", "arrival", "epoch", "events", "rules"}, p)
        batch_id = ctx.ident(require(batch, "id", p), join(p, "id"))
        arrival = _time(require(batch, "arrival", p), join(p, "arrival"))
        epoch = as_int(require(batch, "epoch", p), join(p, "epoch"),
                       minimum=0)
        events = as_list(require(batch, "events", p), join(p, "events"))
        if not events:
            _fail("E_BATCH_EMPTY", join(p, "events"),
                  "an event batch must contain at least one event")
        if len(events) > max_batch:
            _fail("E_BATCH_SIZE", join(p, "events"),
                  "batch has %d events, max_batch is %d"
                  % (len(events), max_batch))
        if batch_size is not None and len(events) != batch_size:
            _fail("E_BATCH_SIZE", join(p, "events"),
                  "batch has %d events but control.batch_size is %d"
                  % (len(events), batch_size))
        parsed_events = [validate_event(ev, join(p, "events", j), ctx)
                         for j, ev in enumerate(events)]
        rules = [validate_rule(item, join(join(p, "rules"), j), ctx)
                 for j, item in enumerate(
                     as_list(batch.get("rules", []), join(p, "rules")))]
        if (arrival, batch_id) in seen:
            _fail("E_DUP_BATCH", join(p, "id"),
                  "arrival and id must be unique within a case")
        seen.add((arrival, batch_id))
        out.append({"id": batch_id, "arrival": arrival, "epoch": epoch,
                    "events": parsed_events, "rules": rules, "index": i,
                    "_path": p})
    return out


# ---------------------------------------------------------------------------
# gaps
# ---------------------------------------------------------------------------

def validate_gaps(value, ctx):
    items = as_list(value, "gaps")
    out = []
    seen = set()
    for i, gap in enumerate(items):
        p = join("gaps", i)
        as_object(gap, p)
        reject_unknown(
            gap, {"src", "gen", "seq", "kind", "reason", "authority",
                  "arrival"}, p)
        parsed = {
            "src": ctx.ident(require(gap, "src", p), join(p, "src")),
            "gen": _seq(require(gap, "gen", p), join(p, "gen")),
            "seq": _seq(require(gap, "seq", p), join(p, "seq")),
            "kind": as_string(require(gap, "kind", p), join(p, "kind"),
                              max_len=16),
            "reason": as_string(require(gap, "reason", p), join(p, "reason"),
                                max_len=512),
            "authority": ctx.ident(gap.get("authority", "auditor"),
                                   join(p, "authority")),
            "arrival": _time(gap.get("arrival", 0), join(p, "arrival")),
        }
        if parsed["gen"] < 1:
            _fail("E_VALUE_OUT_OF_RANGE", join(p, "gen"), "must be >= 1")
        if parsed["kind"] not in GAP_KINDS:
            _fail("E_VALUE_INVALID", join(p, "kind"),
                  "expected one of: %s" % ", ".join(GAP_KINDS))
        key = (parsed["src"], parsed["gen"], parsed["seq"])
        if key in seen:
            # The same instruction may legitimately be replayed later in the
            # timeline (that is how idempotent gap replays are exercised), so
            # the timeline orders the two entries.  Only an instruction that
            # contradicts an earlier one at the *same* arrival is ambiguous.
            prior = next((g for g in out
                          if (g["src"], g["gen"], g["seq"]) == key
                          and g["arrival"] == parsed["arrival"]), None)
            if prior is not None and (
                    prior["kind"], prior["reason"], prior["authority"]) != (
                        parsed["kind"], parsed["reason"],
                        parsed["authority"]):
                _fail("E_GAP_CONFLICT", join(p, "seq"),
                      "conflicting gap instructions for the same sequence")
        else:
            seen.add(key)
        parsed["index"] = i
        parsed["_path"] = p
        out.append(parsed)
    return out


# ---------------------------------------------------------------------------
# case document
# ---------------------------------------------------------------------------

_CASE_KEYS = {"case_id", "max_id_len", "control", "announcements", "rules",
              "batches", "gaps"}
_REQUIRED = ("announcements", "rules")


def validate_case(doc):
    as_object(doc, "")
    reject_unknown(doc, _CASE_KEYS, "")
    case_id = as_string(require(doc, "case_id", ""), "case_id", max_len=200)
    max_id_len = as_int(doc.get("max_id_len", 128), "max_id_len",
                        minimum=1, maximum=512)
    control = validate_control(doc.get("control", {}))
    ctx = CaseContext(case_id, max_id_len, control["strict_fields"])
    for table in _REQUIRED:
        if table not in doc:
            _fail("E_FIELD_MISSING", table, "table is required")
    announcements = validate_announcements(doc["announcements"], ctx)
    rules = validate_rules(doc["rules"], ctx)
    batches = validate_batches(doc.get("batches", []), ctx,
                               control["max_batch"], control["batch_size"])
    gaps = validate_gaps(doc.get("gaps", []), ctx)
    return {
        "case_id": case_id,
        "max_id_len": max_id_len,
        "control": control,
        "announcements": announcements,
        "rules": rules,
        "batches": batches,
        "gaps": gaps,
    }


def validate_state_document(doc):
    """A case file passed to ``dump``/``inspect`` only needs a case id.

    A full case document is accepted too so the same file can drive
    ``ingest`` and then ``dump``.
    """
    as_object(doc, "")
    case_id = doc.get("case_id")
    if case_id is None:
        return None
    return as_string(case_id, "case_id", max_len=200)
