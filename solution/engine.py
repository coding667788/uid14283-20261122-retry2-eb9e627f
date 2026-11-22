"""Pure receive-side state machine for the isolated maintenance gateway.

The engine performs no I/O.  It mutates a plain state mapping that the
storage layer loads from and flushes to SQLite, so "adjudicate + advance
window + persist receipts" is one transaction by construction.

Protocol invariants implemented here:

``(src, gen)`` windows
    Every source generation has its own receiving window.  ``cursor`` is the
    smallest sequence number without a receipt.  Sequence numbers are only
    ever compared inside one generation; no code path orders two generations
    by their sequence numbers.

Sequence numbers are not re-used
    A decided number keeps its slot forever: the cursor advances past it
    whether the outcome was allow or reject, and a late arrival at that slot
    is answered with the original receipt rather than a new one.

Generation hand-over
    A new generation may buffer messages immediately, but it can only emit
    receipts once *every* number up to the previous generation's announced
    ``prev_end_seq`` has a receipt.  That check is not a graph edge: it is a
    plain window condition, so a new generation cannot be used to unblock the
    old one.

Rule versioning
    Adjudication always uses the newest rule version whose
    ``effective_epoch`` is at or below the current logical epoch, so a
    message that waited can never be judged by a superseded rule.

Cycles
    Cycles are derived, not stored: they are recomputed from the wait graph
    after every action.  Breaking one member with an auditor instruction lets
    the rest resolve on their own.
"""

import hashlib
import json

from .errors import SimError

ACCEPT = "ACCEPT"
REJECT = "REJECT"
DUPLICATE = "DUPLICATE"

ALLOW = "ALLOW"
DENY = "DENY"

PENDING = "PENDING"
RESOLVED = "RESOLVED"
CYCLED = "CYCLED"

WAIT_GATE = "WAIT_GATE"
WAIT_DEP = "WAIT_DEP"
WAIT_DUP = "WAIT_DUP"

#: Reject reason -> rejection class.  ``None`` means "not a rejection".
REASON_CLASS = {
    "RETRANSMIT": None,
    "NO_RULE": "PERMISSION",
    "REVOKED": "PERMISSION",
    "DEPENDENCY_REJECTED": "DEPENDENCY",
    "ID_MISMATCH": "PROTOCOL",
    "GAP_CLOSED": "PROTOCOL",
    "CYCLE_CANCELLED": "PROTOCOL",
}


class BatchRejected(Exception):
    """A batch discarded whole, without aborting the rest of the run."""

    def __init__(self, code, path, detail=""):
        super().__init__(code)
        self.code = code
        self.path = path
        self.detail = detail

    def to_dict(self):
        return {"code": self.code, "path": self.path, "detail": self.detail}


class InjectedFault(Exception):
    """Raised by the test-only failpoint to abort the transaction."""

    def __init__(self, detail, issued=0):
        super().__init__(detail)
        self.issued = issued


class Run:
    """Per-invocation options plus the receipt-abort budget."""

    def __init__(self, state, opts, abort_after=None):
        self.state = state
        self.opts = opts
        self.abort_after = abort_after
        self.issued = 0

    def before_receipt(self):
        if self.abort_after is not None and self.issued >= self.abort_after:
            raise InjectedFault("stopped after %d receipts" % self.issued,
                                self.issued)


# ---------------------------------------------------------------------------
# state construction
# ---------------------------------------------------------------------------

def new_state():
    return {
        "case_id": None,
        "epoch": 0,
        "next_receipt": 1,
        "announcements": {},   # (src, gen) -> announcement
        "rules": [],           # installed rule versions
        "windows": {},         # (src, gen) -> {"cursor": int}
        "entries": {},         # (src, gen, seq) -> entry
        "business": {},        # business_id -> (src, gen, seq) first seen
        "receipts": [],        # index i has number i + 1
        "receipt_index": {},   # (src, gen, seq) -> receipt number
        "applied": {},         # action key -> content digest
        "gaps": {},            # (src, gen, seq) -> gap record
        "cycles": [],
        "_checksum": None,
    }


def win_key(src, gen):
    return (src, gen)


def ent_key(src, gen, seq):
    return (src, gen, seq)


def window(state, src, gen):
    key = win_key(src, gen)
    found = state["windows"].get(key)
    if found is None:
        ann = state["announcements"][key]
        found = {"cursor": ann["start_seq"]}
        state["windows"][key] = found
    return found


# ---------------------------------------------------------------------------
# trusted inputs
# ---------------------------------------------------------------------------

def install_announcement(state, ann):
    """Install one trusted generation announcement.  Repeats are no-ops."""
    key = win_key(ann["src"], ann["gen"])
    existing = state["announcements"].get(key)
    if existing is not None:
        if (existing["start_seq"] != ann["start_seq"]
                or existing["prev_end_seq"] != ann["prev_end_seq"]):
            raise SimError(
                "E_ANNOUNCEMENT_CONFLICT", ann.get("_path", ""),
                "generation %s/%s was already announced with different bounds"
                % (ann["src"], ann["gen"]))
        return False
    prev_end = ann["prev_end_seq"]
    if prev_end is not None:
        prev = state["announcements"].get(win_key(ann["src"], ann["gen"] - 1))
        if prev is None:
            raise SimError(
                "E_ANNOUNCEMENT_PREV_MISSING", ann.get("_path", ""),
                "generation %s/%s names prev_end_seq %d but generation %d "
                "has not been announced"
                % (ann["src"], ann["gen"], prev_end, ann["gen"] - 1))
        if prev_end < prev["start_seq"]:
            raise SimError(
                "E_ANNOUNCEMENT_BOUNDS", ann.get("_path", ""),
                "prev_end_seq %d is below the start of generation %d (%d)"
                % (prev_end, ann["gen"] - 1, prev["start_seq"]))
    state["announcements"][key] = {
        "src": ann["src"], "gen": ann["gen"],
        "start_seq": ann["start_seq"], "prev_end_seq": prev_end,
    }
    return True


def install_rule(state, rule, batch_id=None):
    """Install one rule version.  Repeating an identical version is a no-op."""
    routes = {r["dst"]: r["allow"] for r in rule["routes"]}
    for installed in state["rules"]:
        if installed["id"] != rule["id"]:
            continue
        if installed["effective_epoch"] == rule["effective_epoch"]:
            if (installed["precedence"] != rule["precedence"]
                    or installed["routes"] != routes):
                raise SimError(
                    "E_RULE_VERSION_CONFLICT", rule.get("_path", ""),
                    "rule %r already has a different version at epoch %d"
                    % (rule["id"], rule["effective_epoch"]))
            return False
    for installed in state["rules"]:
        if installed["id"] == rule["id"]:
            continue
        if (installed["effective_epoch"] == rule["effective_epoch"]
                and installed["precedence"] == rule["precedence"]):
            overlap = sorted(set(installed["routes"]) & set(routes))
            if overlap:
                raise SimError(
                    "E_RULE_AMBIGUOUS", rule.get("_path", ""),
                    "rules %r and %r tie on epoch %d precedence %d for "
                    "destination(s) %s"
                    % (installed["id"], rule["id"],
                       rule["effective_epoch"], rule["precedence"],
                       ", ".join(overlap)))
    state["rules"].append({
        "id": rule["id"],
        "precedence": rule["precedence"],
        "effective_epoch": rule["effective_epoch"],
        "routes": routes,
        "batch": batch_id,
    })
    return True


def pick_rule(state, dst, epoch):
    """Policy version in force for ``dst`` at logical time ``epoch``.

    The newest version at or below ``epoch`` wins.  Adjudication happens at
    the logical time of the transaction that releases the message, so a
    message that waited across a policy update is judged by the *new*
    version and a superseded one is never read for it.
    """
    best_rank = None
    best = None
    for rule in state["rules"]:
        if rule["effective_epoch"] > epoch or dst not in rule["routes"]:
            continue
        rank = (rule["effective_epoch"], rule["precedence"], rule["id"])
        if best_rank is None or rank > best_rank:
            best_rank = rank
            best = rule
    return best


# ---------------------------------------------------------------------------
# receipts
# ---------------------------------------------------------------------------

def _append_receipt(run, entry, status, outcome, reason, *, rule=None,
                    depends_on=None, gap=None):
    run.before_receipt()
    state = run.state
    number = state["next_receipt"]
    receipt = {
        "receipt": number,
        "src": entry["src"],
        "gen": entry["gen"],
        "seq": entry["seq"],
        "status": status,
        "outcome": outcome,
        "reason": reason,
        "reject_class": REASON_CLASS.get(reason) if outcome == DENY else None,
        "dst": entry.get("dst"),
        "business_id": entry.get("business_id"),
        "fingerprint": entry.get("fingerprint"),
        "created_at": entry.get("created_at"),
        "rule_id": rule["id"] if rule else None,
        "rule_epoch": rule["effective_epoch"] if rule else None,
        "epoch": state["epoch"],
        "batch": entry.get("batch"),
        "depends_on": depends_on,
        "gateways": list(entry.get("gateways") or []),
        "gap_authority": gap["authority"] if gap else None,
        "gap_reason": gap["reason"] if gap else None,
        "gap_kind": gap["kind"] if gap else None,
    }
    state["receipts"].append(receipt)
    state["next_receipt"] = number + 1
    state["receipt_index"][ent_key(entry["src"], entry["gen"],
                                   entry["seq"])] = number
    entry["state"] = RESOLVED
    entry["wait"] = None
    entry["receipt"] = number
    window(state, entry["src"], entry["gen"])["cursor"] = entry["seq"] + 1
    run.issued += 1
    return receipt


def receipt_for(state, src, gen, seq):
    number = state["receipt_index"].get(ent_key(src, gen, seq))
    return None if number is None else state["receipts"][number - 1]


# ---------------------------------------------------------------------------
# adjudication
# ---------------------------------------------------------------------------

def gate_open(state, src, gen):
    """True when the previous generation decided everything up to its end."""
    ann = state["announcements"][win_key(src, gen)]
    prev_end = ann["prev_end_seq"]
    if prev_end is None:
        return True
    prev_ann = state["announcements"].get(win_key(src, gen - 1))
    if prev_ann is None:
        return True
    return window(state, src, gen - 1)["cursor"] > prev_end


def _adjudicate(run, entry):
    """Decide the buffered entry at a window head, or leave it waiting."""
    state = run.state

    if entry["gap"] is not None:
        kind = entry["gap"]["kind"]
        _append_receipt(
            run, entry, REJECT, DENY,
            "CYCLE_CANCELLED" if kind == "CANCEL" else "GAP_CLOSED",
            gap=entry["gap"])
        return True

    if not gate_open(state, entry["src"], entry["gen"]):
        entry["wait"] = WAIT_GATE
        return False

    # A refused predecessor is permanent, so it wins over a predecessor that
    # simply has not arrived yet: the message could never be accepted again
    # even if the missing one showed up.
    rejected_by = None
    waiting_on = None
    for ref in entry["predecessors"]:
        receipt = receipt_for(state, ref["src"], ref["gen"], ref["seq"])
        if receipt is None:
            waiting_on = waiting_on or ent_key(ref["src"], ref["gen"],
                                               ref["seq"])
        elif receipt["outcome"] == DENY:
            rejected_by = receipt["receipt"]
            break
    if rejected_by is not None:
        _append_receipt(run, entry, REJECT, DENY, "DEPENDENCY_REJECTED",
                        depends_on=rejected_by)
        return True
    if waiting_on is not None:
        entry["wait"] = WAIT_DEP
        return False

    key = ent_key(entry["src"], entry["gen"], entry["seq"])
    original = state["business"].get(entry["business_id"])
    if original is not None and original != key:
        source = state["entries"].get(original)
        if source is not None and source["fingerprint"] != entry["fingerprint"]:
            _append_receipt(run, entry, REJECT, DENY, "ID_MISMATCH")
            return True
        if source is not None and source["state"] != RESOLVED:
            entry["wait"] = WAIT_DUP
            return False
        origin_receipt = (state["receipts"][source["receipt"] - 1]
                          if source is not None and source["receipt"] else None)
        if origin_receipt is not None and origin_receipt["outcome"] == DENY:
            _append_receipt(run, entry, DUPLICATE, DENY, origin_receipt["reason"],
                            depends_on=origin_receipt["receipt"])
        else:
            _append_receipt(run, entry, DUPLICATE, ALLOW, "RETRANSMIT",
                            depends_on=(origin_receipt["receipt"]
                                        if origin_receipt else None))
        return True

    rule = pick_rule(state, entry["dst"], state["epoch"])
    if rule is None:
        _append_receipt(run, entry, REJECT, DENY, "NO_RULE")
        return True
    if not rule["routes"][entry["dst"]]:
        _append_receipt(run, entry, REJECT, DENY, "REVOKED", rule=rule)
        return True
    _append_receipt(run, entry, ACCEPT, ALLOW, "ACCEPT", rule=rule)
    return True


def drive_windows(run):
    """Adjudicate every window head that can decide, to a fixpoint."""
    state = run.state
    progress = True
    while progress:
        progress = False
        detect_cycles(state)
        for key in sorted(state["windows"]):
            w = state["windows"][key]
            while True:
                entry = state["entries"].get(ent_key(key[0], key[1],
                                                     w["cursor"]))
                if entry is None or entry["state"] != PENDING:
                    break
                if not _adjudicate(run, entry):
                    break
                progress = True
        detect_cycles(state)


# ---------------------------------------------------------------------------
# cycle detection
# ---------------------------------------------------------------------------

def blocker_keys(state, entry):
    """All keys whose decision this buffered entry waits for.

    ``inspect`` reports these.  Cycle detection uses :func:`wait_edges`,
    which drops references to numbers that were never received, because such
    a number is closed with an auditor gap instruction rather than by
    resolving a cycle.
    """
    key = ent_key(entry["src"], entry["gen"], entry["seq"])
    wait = entry.get("wait")
    out = []
    if entry["state"] != PENDING or entry["gap"] is not None:
        return out
    if wait is None:
        # A number ahead of the cursor waits for the window head, whether or
        # not the head has been delivered yet.
        cursor = window(state, entry["src"], entry["gen"])["cursor"]
        if entry["seq"] > cursor:
            out.append(ent_key(entry["src"], entry["gen"], cursor))
        return out
    if wait == WAIT_GATE:
        ann = state["announcements"][win_key(entry["src"], entry["gen"])]
        prev_end = ann["prev_end_seq"]
        if prev_end is not None:
            prev = window(state, entry["src"], entry["gen"] - 1)
            if prev["cursor"] <= prev_end:
                out.append(ent_key(entry["src"], entry["gen"] - 1,
                                   prev["cursor"]))
        return out
    if wait == WAIT_DEP:
        for ref in entry["predecessors"]:
            candidate = ent_key(ref["src"], ref["gen"], ref["seq"])
            if candidate not in state["receipt_index"]:
                out.append(candidate)
        return out
    if wait == WAIT_DUP:
        original = state["business"].get(entry["business_id"])
        if original is not None and original != key:
            found = state["entries"].get(original)
            if found is not None and found["state"] != RESOLVED:
                out.append(original)
        return out
    return out


def wait_edges(state, entry):
    """Edges of the dependency graph used for cycle detection.

    A window head is decided strictly in sequence order, so any buffered
    number above the cursor waits for its predecessor number and therefore
    (transitively) for the head.  The head itself waits only on the explicit
    conditions recorded in ``entry["wait"]``: its predecessors, a
    retransmission's original, or the generation gate.

    Blocks on a key with no buffered entity are dropped: such a number is
    either decided already or was never delivered, and a never-delivered
    number is closed with an auditor gap instruction, not by breaking a
    cycle.
    """
    if entry["state"] != PENDING:
        return []
    cur = window(state, entry["src"], entry["gen"])["cursor"]
    if entry["seq"] > cur:
        return [ent_key(entry["src"], entry["gen"], entry["seq"] - 1)]
    if entry.get("wait") is None:
        return []
    return blocker_keys(state, entry)


def _tarjan(nodes, adjacency):
    """Iterative Tarjan; returns strongly connected components."""
    index = {}
    low = {}
    on_stack = {}
    stack = []
    result = []
    counter = 0
    members = set(nodes)

    for root in nodes:
        if root in index:
            continue
        work = [(root, 0)]
        while work:
            node, child_i = work.pop()
            if child_i == 0:
                index[node] = low[node] = counter
                counter += 1
                stack.append(node)
                on_stack[node] = True
            children = adjacency[node]
            pushed = False
            i = child_i
            while i < len(children):
                child = children[i]
                i += 1
                if child not in members:
                    continue
                if child not in index:
                    work.append((node, i))
                    work.append((child, 0))
                    pushed = True
                    break
                if on_stack.get(child):
                    low[node] = min(low[node], index[child])
            if pushed:
                continue
            if low[node] == index[node]:
                component = []
                while True:
                    top = stack.pop()
                    on_stack[top] = False
                    component.append(top)
                    if top == node:
                        break
                result.append(sorted(component))
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])
    return result


def shortest_cycle_through(start, adjacency):
    """Shortest simple cycle containing ``start``; ties broken lexically."""
    best = None
    queue = [(start, [start])]
    while queue:
        node, path = queue.pop(0)
        for child in adjacency.get(node, ()):
            if child == start:
                rank = (len(path), [_key_text(k) for k in path])
                if best is None or rank < best[0]:
                    best = (rank, list(path))
                continue
            if child in path:
                continue
            queue.append((child, path + [child]))
    return best[1] if best else None


def _key_text(key):
    return "%s/%s/%s" % key


def detect_cycles(state):
    """Recompute derived cycle marks; cycles come back shortest first."""
    for entry in state["entries"].values():
        if entry["state"] == CYCLED:
            entry["state"] = PENDING

    nodes = sorted(k for k, e in state["entries"].items()
                   if e["state"] == PENDING)
    adjacency = {}
    for key in nodes:
        adjacency[key] = sorted({c for c in
                                 wait_edges(state, state["entries"][key])
                                 if c in state["entries"]})

    cycles = []
    for component in _tarjan(nodes, adjacency):
        if len(component) == 1 and component[0] not in adjacency[component[0]]:
            continue
        best = None
        for start in component:
            found = shortest_cycle_through(start, adjacency)
            if found is None:
                continue
            rank = (len(found), [_key_text(k) for k in found])
            if best is None or rank < best[0]:
                best = (rank, found)
        if best is not None:
            cycles.append(best[1])

    cycles.sort(key=lambda m: (len(m), [_key_text(k) for k in m]))
    for members in cycles:
        for key in members:
            state["entries"][key]["state"] = CYCLED
    state["cycles"] = [cycle_view(m) for m in cycles]
    return cycles


def cycle_view(members):
    return {
        "length": len(members),
        "members": [{"src": k[0], "gen": k[1], "seq": k[2]}
                    for k in members],
        "edges": [{"from": {"src": a[0], "gen": a[1], "seq": a[2]},
                   "to": {"src": b[0], "gen": b[1], "seq": b[2]}}
                  for a, b in zip(members, members[1:] + members[:1])],
    }


# ---------------------------------------------------------------------------
# admission
# ---------------------------------------------------------------------------

def _merge_gateway(state, entry, gateway):
    """Record a second (or third) gateway that forwarded the same request."""
    if entry["gateways"] is None:
        entry["gateways"] = [gateway]
    elif gateway not in entry["gateways"]:
        entry["gateways"] = entry["gateways"] + [gateway]
    else:
        return
    # A decided message keeps exactly one receipt; the extra gateway is added
    # to that receipt so the audit trail shows every forwarder.
    number = entry.get("receipt")
    if number is not None:
        receipt = state["receipts"][number - 1]
        if gateway not in receipt["gateways"]:
            receipt["gateways"] = receipt["gateways"] + [gateway]


def admit(run, event, batch_id, deferrals):
    """Buffer one event; returns 'new', 'deduped' or 'deferred'."""
    state = run.state
    key = ent_key(event["src"], event["gen"], event["seq"])
    ann = state["announcements"].get(win_key(event["src"], event["gen"]))
    if ann is None:
        raise BatchRejected("E_GEN_NOT_ANNOUNCED", event.get("_path", ""),
                            "generation %s/%s has not been announced"
                            % (event["src"], event["gen"]))
    if event["seq"] < ann["start_seq"]:
        raise BatchRejected("E_SEQ_BEFORE_START", event.get("_path", ""),
                            "sequence %d is below the start of generation "
                            "%s/%s (%d)"
                            % (event["seq"], event["src"], event["gen"],
                               ann["start_seq"]))

    w = window(state, event["src"], event["gen"])
    if event["seq"] < w["cursor"]:
        existing = state["entries"].get(key)
        if existing is not None:
            _merge_gateway(state, existing, event["gateway"])
        return "deduped"
    if event["seq"] >= w["cursor"] + run.opts["max_window"]:
        deferrals.append({
            "src": event["src"], "gen": event["gen"], "seq": event["seq"],
            "gateway": event["gateway"],
            "reason": "WINDOW_FULL",
            "window_left": w["cursor"],
            "window_right": w["cursor"] + run.opts["max_window"] - 1,
        })
        return "deferred"

    existing = state["entries"].get(key)
    if existing is not None:
        _merge_gateway(state, existing, event["gateway"])
        return "deduped"

    state["entries"][key] = {
        "src": event["src"], "gen": event["gen"], "seq": event["seq"],
        "dst": event["dst"],
        "business_id": event["business_id"],
        "fingerprint": event["fingerprint"],
        "created_at": event["created_at"],
        "gateways": [event["gateway"]],
        "predecessors": [dict(r) for r in event["predecessors"]],
        "state": PENDING,
        "wait": None,
        "receipt": None,
        "gap": None,
        "batch": batch_id,
    }
    state["business"].setdefault(event["business_id"], key)
    return "new"


def _check_batch_consistency(state, events, batch_id):
    """Whole-batch rejection when one slot is bound to two requests."""
    violation = _batch_violations(state, events, batch_id)
    if violation:
        raise BatchRejected(*violation)


def _batch_violations(state, events, batch_id):
    """Return ``(code, path, detail)`` for the first structural problem."""
    seen = {}
    for event in events:
        key = ent_key(event["src"], event["gen"], event["seq"])
        prior = seen.get(key)
        if prior is not None and prior != event["fingerprint"]:
            return ("E_SEQ_CONFLICT", event.get("_path", ""),
                    "%s/%s/%s carries two different requests inside batch %r"
                    % (event["src"], event["gen"], event["seq"], batch_id))
        seen[key] = event["fingerprint"]
        existing = state["entries"].get(key)
        if (existing is not None and existing["fingerprint"] is not None
                and existing["fingerprint"] != event["fingerprint"]):
            return ("E_SEQ_CONFLICT", event.get("_path", ""),
                    "%s/%s/%s was already received with a different request"
                    % (event["src"], event["gen"], event["seq"]))
        receipt = receipt_for(state, *key)
        if (receipt is not None and receipt["fingerprint"] is not None
                and receipt["fingerprint"] != event["fingerprint"]):
            return ("E_SEQ_CONFLICT", event.get("_path", ""),
                    "%s/%s/%s was already decided for a different request"
                    % (event["src"], event["gen"], event["seq"]))
    ids = {}
    for event in events:
        prior = ids.get(event["business_id"])
        if prior is not None and prior != event["fingerprint"]:
            return ("E_ID_CONFLICT", event.get("_path", ""),
                    "business id %r carries two different requests inside "
                    "batch %r" % (event["business_id"], batch_id))
        ids[event["business_id"]] = event["fingerprint"]
    return None


def apply_batch(run, batch):
    """Apply one event batch."""
    state = run.state
    report = {
        "batch": batch["id"],
        "arrival": batch["arrival"],
        "epoch": batch["epoch"],
        "applied": False,
        "replayed": False,
        "admitted": 0,
        "rules_installed": [],
        "receipts": [],
        "deduped": [],
        "deferred": [],
    }

    applied_key = "batch:%s" % batch["id"]
    recorded = state["applied"].get(applied_key)
    if recorded is not None:
        if recorded != batch["_digest"]:
            raise BatchRejected(
                "E_BATCH_REPLAY_CONFLICT", batch.get("_path", ""),
                "batch %r was applied before with different content"
                % batch["id"])
        report["replayed"] = True
        report["applied"] = True
        for event in batch["events"]:
            key = ent_key(event["src"], event["gen"], event["seq"])
            number = state["receipt_index"].get(key)
            if number is not None:
                report["receipts"].append(number)
            report["deduped"].append({
                "src": event["src"], "gen": event["gen"],
                "seq": event["seq"], "gateway": event["gateway"],
                "receipt": number, "replayed": True,
            })
        return report

    # Structural checks run first and inspect the batch on its own, so a
    # refused batch leaves no trace: no rule installed, no epoch bumped, no
    # message buffered.  "Reject the whole batch" would be a lie otherwise.
    events = sorted(batch["events"],
                    key=lambda e: (e["src"], e["gen"], e["seq"],
                                   e["business_id"]))
    violation = _batch_violations(state, events, batch["id"])
    if violation:
        raise BatchRejected(*violation)

    # Every event of the batch must belong to an announced generation and
    # respect its start, so this is checked up front too.
    for event in events:
        ann = state["announcements"].get(win_key(event["src"], event["gen"]))
        if ann is None:
            raise BatchRejected(
                "E_GEN_NOT_ANNOUNCED", event.get("_path", ""),
                "generation %s/%s has not been announced"
                % (event["src"], event["gen"]))
        if event["seq"] < ann["start_seq"]:
            raise BatchRejected(
                "E_SEQ_BEFORE_START", event.get("_path", ""),
                "sequence %d is below the start of generation %s/%s (%d)"
                % (event["seq"], event["src"], event["gen"],
                   ann["start_seq"]))

    # Rule installation can also refuse the batch (E_RULE_VERSION_CONFLICT,
    # E_RULE_AMBIGUOUS).  Validation runs before any rule is installed, so
    # a refusal here still leaves nothing behind.
    previous_epoch = state["epoch"]
    state["epoch"] = max(state["epoch"], batch["epoch"])
    installed = []
    try:
        for rule in batch["rules"]:
            if install_rule(state, rule, batch["id"]):
                installed.append(rule["id"])
        report["rules_installed"] = installed
        return _admit_batch(run, batch, events, report, applied_key)
    except SimError:
        for rule in installed:
            _remove_rule(state, rule, batch["id"])
        state["epoch"] = previous_epoch
        raise
    except BatchRejected:
        for rule in installed:
            _remove_rule(state, rule, batch["id"])
        state["epoch"] = previous_epoch
        raise


def _remove_rule(state, rule_id, batch_id):
    """Undo one rule installation from a batch that is being rolled back."""
    state["rules"] = [r for r in state["rules"]
                      if not (r["id"] == rule_id and r["batch"] == batch_id)]


def _admit_batch(run, batch, events, report, applied_key):
    state = run.state
    before = len(state["receipts"])
    deferrals = []
    for event in events:
        result = admit(run, event, batch["id"], deferrals)
        if result == "deduped":
            key = ent_key(event["src"], event["gen"], event["seq"])
            report["deduped"].append({
                "src": event["src"], "gen": event["gen"],
                "seq": event["seq"], "gateway": event["gateway"],
                "receipt": state["receipt_index"].get(key),
            })
        elif result == "new":
            report["admitted"] += 1

    drive_windows(run)
    for item in report["deduped"]:
        if item["receipt"] is None:
            item["receipt"] = state["receipt_index"].get(
                ent_key(item["src"], item["gen"], item["seq"]))
    report["receipts"] = [r["receipt"] for r in state["receipts"][before:]]
    report["deferred"] = deferrals
    report["applied"] = not deferrals
    if not deferrals:
        state["applied"][applied_key] = batch["_digest"]
    return report


# ---------------------------------------------------------------------------
# auditor instructions
# ---------------------------------------------------------------------------

def _gap_entity(gap):
    return {
        "src": gap["src"], "gen": gap["gen"], "seq": gap["seq"],
        "dst": None, "business_id": None, "fingerprint": None,
        "created_at": None, "gateways": [], "predecessors": [],
        "state": PENDING, "wait": None, "receipt": None,
        "gap": None, "batch": None,
    }


def apply_gap(run, gap):
    """Apply one auditor instruction, ``PROVE`` or ``CANCEL``."""
    state = run.state
    report = {
        "gap": {"src": gap["src"], "gen": gap["gen"], "seq": gap["seq"],
                "kind": gap["kind"], "reason": gap["reason"],
                "authority": gap["authority"]},
        "idempotent": False,
        "applied": False,
        "receipt": None,
        "cancelled_cycle": False,
        "collateral": [],
    }

    if gap["authority"] != run.opts["gap_authority"]:
        raise SimError("E_GAP_AUTHORITY", gap.get("_path", ""),
                       "gap authority %r is not the configured auditor %r"
                       % (gap["authority"], run.opts["gap_authority"]))
    if win_key(gap["src"], gap["gen"]) not in state["announcements"]:
        raise SimError("E_GEN_NOT_ANNOUNCED", gap.get("_path", ""),
                       "generation %s/%s has not been announced"
                       % (gap["src"], gap["gen"]))
    key = ent_key(gap["src"], gap["gen"], gap["seq"])
    w = window(state, gap["src"], gap["gen"])
    recorded = state["gaps"].get(key)
    applied_key = "gap:%s/%s/%s:%s" % (gap["src"], gap["gen"], gap["seq"],
                                       gap["kind"])

    if gap["seq"] < w["cursor"]:
        receipt = receipt_for(state, *key)
        if receipt is None:
            raise SimError("E_GAP_BELOW_CURSOR", gap.get("_path", ""),
                           "%s/%s/%s is below the cursor but has no receipt"
                           % (gap["src"], gap["gen"], gap["seq"]))
        report["idempotent"] = True
        report["receipt"] = receipt["receipt"]
        return report

    existing = state["entries"].get(key)

    if gap["kind"] == "PROVE":
        if recorded is not None and recorded["kind"] == "PROVE":
            if state["applied"].get(applied_key) not in (None, gap["_digest"]):
                raise SimError("E_GAP_CONFLICT", gap.get("_path", ""),
                               "a different PROVE already closed this gap")
            report["idempotent"] = True
            receipt = receipt_for(state, *key)
            report["receipt"] = receipt["receipt"] if receipt else None
            return report
        if existing is not None and existing["state"] == CYCLED:
            raise SimError("E_GAP_CYCLED", gap.get("_path", ""),
                           "%s/%s/%s is part of a cycle; CANCEL is the "
                           "instruction that resolves cycle members"
                           % (gap["src"], gap["gen"], gap["seq"]))
        if existing is not None and existing["state"] == RESOLVED:
            report["idempotent"] = True
            report["receipt"] = existing["receipt"]
            return report
        # PROVE closes a gap: at an empty sequence it manufactures the
        # rejection receipt, at a sequence holding a buffered ordinary
        # message it rejects that message, and at a decided sequence it is a
        # no-op.  Either way the slot keeps its place in the window; the
        # receipt appears when the cursor reaches it.
        if existing is None:
            existing = _gap_entity(gap)
            state["entries"][key] = existing
        existing["gap"] = {"kind": "PROVE", "reason": gap["reason"],
                           "authority": gap["authority"]}
        existing["predecessors"] = []
        existing["wait"] = None
        state["gaps"][key] = {"kind": "PROVE", "reason": gap["reason"],
                              "authority": gap["authority"]}
        state["applied"][applied_key] = gap["_digest"]
        report["applied"] = True
    else:  # CANCEL
        if recorded is not None and recorded["kind"] == "CANCEL":
            if state["applied"].get(applied_key) not in (None, gap["_digest"]):
                raise SimError("E_GAP_CONFLICT", gap.get("_path", ""),
                               "a different CANCEL already closed this cycle")
            report["idempotent"] = True
            receipt = receipt_for(state, *key)
            report["receipt"] = receipt["receipt"] if receipt else None
            return report
        if existing is None or existing["state"] != CYCLED:
            # The member may already have been decided by an earlier
            # instruction in this same run; then the cancellation is a
            # no-op, not an error.  Only a genuinely unresolvable target is
            # refused.
            if existing is not None and existing["state"] == RESOLVED \
                    and (existing["gap"] or {}).get("kind") == "CANCEL":
                report["idempotent"] = True
                report["receipt"] = existing["receipt"]
                return report
            if existing is not None and existing["state"] == RESOLVED:
                raise SimError("E_GAP_NOT_CYCLED", gap.get("_path", ""),
                               "%s/%s/%s was already decided as %s; CANCEL "
                               "only resolves cycle members"
                               % (gap["src"], gap["gen"], gap["seq"],
                                  existing["receipt"]))
            raise SimError("E_GAP_NOT_CYCLED", gap.get("_path", ""),
                           "%s/%s/%s is not a cycle member; CANCEL only "
                           "resolves cycles"
                           % (gap["src"], gap["gen"], gap["seq"]))
        existing["gap"] = {"kind": "CANCEL", "reason": gap["reason"],
                           "authority": gap["authority"]}
        existing["predecessors"] = []
        existing["wait"] = None
        state["gaps"][key] = {"kind": "CANCEL", "reason": gap["reason"],
                              "authority": gap["authority"]}
        state["applied"][applied_key] = gap["_digest"]
        report["applied"] = True
        report["cancelled_cycle"] = True

    before = len(state["receipts"])
    drive_windows(run)
    receipt = receipt_for(state, *key)
    report["receipt"] = receipt["receipt"] if receipt else None
    report["collateral"] = [
        {"src": r["src"], "gen": r["gen"], "seq": r["seq"],
         "receipt": r["receipt"], "status": r["status"],
         "reason": r["reason"]}
        for r in state["receipts"][before:]]
    return report


# ---------------------------------------------------------------------------
# orchestration
# ---------------------------------------------------------------------------

def action_order(case):
    """Deterministic timeline: by arrival, batches before gaps on a tie."""
    actions = []
    for batch in case["batches"]:
        actions.append((batch["arrival"], 0, batch["id"], "batch", batch))
    for gap in case["gaps"]:
        actions.append((gap["arrival"], 1,
                        "%s/%s/%s" % (gap["src"], gap["gen"], gap["seq"]),
                        "gap", gap))
    actions.sort(key=lambda a: (a[0], a[1], a[2]))
    return actions


def prepare_case(case):
    """Attach the content digests used for action-level idempotence."""
    for batch in case["batches"]:
        batch["_digest"] = digest({
            "kind": "batch", "id": batch["id"],
            "arrival": batch["arrival"], "epoch": batch["epoch"],
            "rules": batch["rules"], "events": batch["events"],
        })
    for gap in case["gaps"]:
        gap["_digest"] = digest({
            "kind": "gap", "src": gap["src"], "gen": gap["gen"],
            "seq": gap["seq"], "action": gap["kind"],
            "reason": gap["reason"], "authority": gap["authority"],
        })
    return case


def digest(payload):
    """SHA-256 of a JSON value with ``_``-prefixed annotations removed."""
    blob = json.dumps(_strip(payload), sort_keys=True, ensure_ascii=True,
                      separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _strip(value):
    if isinstance(value, dict):
        return {k: _strip(v) for k, v in value.items()
                if not k.startswith("_")}
    if isinstance(value, list):
        return [_strip(v) for v in value]
    return value


def run_case(state, case, opts, abort_after=None):
    """Apply a whole case document; returns ``(report, run)``.

    Per-action document problems are contained: a batch that binds one
    sequence to two requests is discarded and reported, and the rest of the
    case still runs.  Trusted inputs are not contained: a bad announcement or
    a contradictory rule version raises :class:`SimError` and aborts the run,
    because every later decision would rest on policy that does not exist.
    """
    run = Run(state, opts, abort_after=abort_after)
    state["case_id"] = case["case_id"]
    report = {
        "case_id": case["case_id"],
        "announcements": [],
        "rules_installed": [],
        "batches": [],
        "gap_results": [],
        "rejected": [],
        "deduped": [],
        "deferred": [],
    }

    for ann in case["announcements"]:
        if install_announcement(state, ann):
            report["announcements"].append({
                "src": ann["src"], "gen": ann["gen"],
                "start_seq": ann["start_seq"],
                "prev_end_seq": ann["prev_end_seq"]})
    for rule in case["rules"]:
        if install_rule(state, rule):
            report["rules_installed"].append(rule["id"])

    for _, _, ident, kind, payload in action_order(case):
        try:
            if kind == "batch":
                result = apply_batch(run, payload)
                report["batches"].append(result)
                report["deduped"].extend(
                    dict(d, batch=ident) for d in result["deduped"])
                report["deferred"].extend(
                    dict(d, batch=ident) for d in result["deferred"])
            else:
                report["gap_results"].append(apply_gap(run, payload))
        except BatchRejected as exc:
            report["rejected"].append({
                "kind": kind, "id": ident, "code": exc.code,
                "path": exc.path, "detail": exc.detail})
        except SimError as exc:
            report["rejected"].append({
                "kind": kind, "id": ident, "code": exc.code,
                "path": exc.path, "detail": exc.detail})

    detect_cycles(state)
    return report, run


# ---------------------------------------------------------------------------
# read-only views
# ---------------------------------------------------------------------------

def next_to_adjudicate(state):
    """The single buffered message the endpoint would evaluate next.

    Ordering is by ``(source, generation, sequence)`` -- never by sequence
    alone, because two generations may not be compared that way.  A message
    that is waiting on a condition is still "next" for its window; the
    endpoint does not skip over it to a decision it could make.
    """
    best = None
    for key in sorted(state["entries"]):
        entry = state["entries"][key]
        if entry["state"] == RESOLVED:
            continue
        cursor = window(state, entry["src"], entry["gen"])["cursor"]
        if entry["seq"] != cursor:
            continue
        if best is None:
            best = key
    if best is None:
        return None
    entry = state["entries"][best]
    return {
        "src": best[0], "gen": best[1], "seq": best[2],
        "state": entry["state"],
        "wait": entry["wait"],
        "blocked_on": blockers_view(state, entry),
    }


def blockers_view(state, entry):
    return [{"src": k[0], "gen": k[1], "seq": k[2]}
            for k in blocker_keys(state, entry)]


def view_state(state):
    """Snapshot shared by ``dump`` and ``inspect``."""
    windows = []
    for key in sorted(state["windows"]):
        w = state["windows"][key]
        ann = state["announcements"][key]
        windows.append({
            "src": key[0], "gen": key[1],
            "start_seq": ann["start_seq"],
            "prev_end_seq": ann["prev_end_seq"],
            "cursor": w["cursor"],
            "gate_open": gate_open(state, key[0], key[1]),
        })
    pending = []
    for key in sorted(state["entries"]):
        entry = state["entries"][key]
        if entry["state"] == RESOLVED:
            continue
        pending.append({
            "src": key[0], "gen": key[1], "seq": key[2],
            "state": entry["state"],
            "wait": entry["wait"],
            "business_id": entry["business_id"],
            "dst": entry["dst"],
            "gateways": list(entry["gateways"] or []),
            "blocked_on": blockers_view(state, entry),
            "gap": entry["gap"],
        })
    rules = []
    for rule in sorted(state["rules"],
                       key=lambda r: (r["effective_epoch"], r["precedence"],
                                      r["id"])):
        rules.append({
            "id": rule["id"],
            "effective_epoch": rule["effective_epoch"],
            "precedence": rule["precedence"],
            "routes": [{"dst": d, "allow": a}
                       for d, a in sorted(rule["routes"].items())],
            "active": rule["effective_epoch"] <= state["epoch"],
            "origin_batch": rule["batch"],
        })
    return {
        "epoch": state["epoch"],
        "next_receipt": state["next_receipt"],
        "windows": windows,
        "pending": pending,
        "next_to_adjudicate": next_to_adjudicate(state),
        "cycles": state["cycles"],
        "rules": rules,
        "gaps": [{"src": k[0], "gen": k[1], "seq": k[2],
                  "kind": g["kind"], "reason": g["reason"],
                  "authority": g["authority"]}
                 for k, g in sorted(state["gaps"].items())],
        "receipt_count": len(state["receipts"]),
    }
