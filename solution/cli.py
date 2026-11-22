"""Command line surface: ``python -m solution <command> CASE.json``.

Every invocation writes exactly one JSON object to stdout, encoded as UTF-8
with ASCII escapes, and exits with a documented status code.  Nothing here
opens a socket, spawns a process or reads the clock: the only inputs are the
case file and the SQLite file named by ``--db``.
"""

import sys

from . import engine, jsonio, storage, validation
from .errors import SimError

USAGE = (
    "usage: python -m solution <command> CASE.json [--db DB.sqlite]\n"
    "commands:\n"
    "  simulate  CASE.json              in-memory dry run, no database\n"
    "  ingest    CASE.json --db DB.sqlite\n"
    "  dump      CASE.json --db DB.sqlite\n"
    "  inspect   CASE.json --db DB.sqlite\n"
    "  verify    CASE.json --db DB.sqlite\n"
)

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INPUT = 3
EXIT_DB = 4

_INPUT_PREFIXES = ("E_JSON", "E_TYPE", "E_FIELD", "E_VALUE", "E_INPUT",
                   "E_FINGERPRINT", "E_BATCH", "E_DUP_", "E_RULE",
                   "E_ANNOUNCEMENT", "E_SEQ_", "E_GEN_", "E_ID_",
                   "E_GAP_", "E_CASE", "E_CONTROL", "E_EVENT")
_DB_PREFIXES = ("E_DB_",)

ALIASES = {"verify-db": "verify"}
COMMANDS = ("ingest", "dump", "inspect", "simulate", "verify")
NO_DB = ("simulate",)


def exit_code_for(code):
    if code == "E_USAGE":
        return EXIT_USAGE
    if code.startswith(_DB_PREFIXES):
        return EXIT_DB
    if code.startswith(_INPUT_PREFIXES):
        return EXIT_INPUT
    return EXIT_ERROR


class Args:
    __slots__ = ("command", "case_path", "db_path")


def parse_argv(argv):
    if not argv:
        raise SimError("E_USAGE", "", USAGE.strip())
    command = ALIASES.get(argv[0], argv[0])
    if command not in COMMANDS:
        raise SimError("E_USAGE", "command",
                       "unknown command %r; expected one of: %s"
                       % (argv[0], ", ".join(COMMANDS)))
    case_path = None
    db_path = None
    index = 1
    while index < len(argv):
        token = argv[index]
        if token == "--db":
            if index + 1 >= len(argv):
                raise SimError("E_USAGE", "--db", "missing value")
            db_path = argv[index + 1]
            index += 2
            continue
        if token.startswith("--db="):
            db_path = token[len("--db="):]
            index += 1
            continue
        if token in ("--help", "-h"):
            raise SimError("E_USAGE", "", USAGE.strip())
        if token.startswith("-"):
            raise SimError("E_USAGE", token, "unknown option")
        if case_path is not None:
            raise SimError("E_USAGE", token,
                           "exactly one CASE.json argument is allowed")
        case_path = token
        index += 1
    if case_path is None:
        raise SimError("E_USAGE", "CASE.json", "missing case file argument")
    if command in NO_DB and db_path is not None:
        raise SimError("E_USAGE", "--db",
                       "%s runs in memory and takes no --db" % command)
    if command not in NO_DB and db_path is None:
        raise SimError("E_USAGE", "--db",
                       "%s reads persistent state and needs --db DB.sqlite"
                       % command)
    args = Args()
    args.command = command
    args.case_path = case_path
    args.db_path = db_path
    return args


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _load_case(args):
    return engine.prepare_case(validation.validate_case(
        jsonio.read(args.case_path)))


def _opts(case):
    control = case["control"]
    return {
        "max_window": control["max_window"],
        "strict_fields": control["strict_fields"],
        "gap_authority": "auditor",
        "case_id": case["case_id"],
        "failpoint": control["failpoint"],
        "fail_after": control["fail_after"],
    }


def _counts(receipts):
    counts = {"ACCEPT": 0, "REJECT": 0, "DUPLICATE": 0}
    for receipt in receipts:
        counts[receipt["status"]] = counts.get(receipt["status"], 0) + 1
    return counts


def _state_view(state):
    engine.detect_cycles(state)
    return engine.view_state(state)


def _adopt_case_id(state, case_id, db_path):
    stored = state.get("case_id")
    if stored is not None and stored != case_id:
        raise SimError("E_CASE_MISMATCH", "case_id",
                       "database %s holds case %r, not %r"
                       % (db_path, stored, case_id))
    state["case_id"] = case_id


def _read_only(args, fn):
    conn = storage.open_database(args.db_path, create=False)
    try:
        storage.begin(conn, write=False)
        state = storage.load_state(conn)
        return fn(state)
    finally:
        conn.close()


def _input_case_id(args):
    doc = jsonio.read(args.case_path)
    if not isinstance(doc, dict):
        raise SimError("E_TYPE_EXPECTED_OBJECT", "", "expected an object")
    return doc.get("case_id")


# ---------------------------------------------------------------------------
# simulate
# ---------------------------------------------------------------------------

def cmd_simulate(args):
    case = _load_case(args)
    opts = _opts(case)
    state = engine.new_state()
    fault = None
    try:
        report, run = engine.run_case(state, case, opts,
                                      abort_after=opts["fail_after"])
    except engine.InjectedFault as exc:
        # In-memory only: nothing was durable and the partial run is
        # discarded, exactly like a database transaction rolling back.
        state = engine.new_state()
        report = {"announcements": [], "rules_installed": [],
                  "batches": [], "gap_results": [], "rejected": [],
                  "deferred": []}
        run = exc
        fault = {"point": opts["failpoint"] or "before_commit",
                 "committed": False, "receipts_rolled_back": exc.issued}
    view = _state_view(state)
    result = {
        "command": "simulate",
        "ok": True,
        "durable": False,
        "case_id": case["case_id"],
        "announcements": report["announcements"],
        "rules_installed": report["rules_installed"],
        "batches": report["batches"],
        "gap_results": report["gap_results"],
        "rejected": report["rejected"],
        "deferred": report["deferred"],
        "receipts": state["receipts"],
        "counts": _counts(state["receipts"]),
        "state": view,
    }
    if fault is not None:
        result["fault"] = fault
    elif opts["failpoint"] is not None:
        result["fault"] = {"point": opts["failpoint"],
                           "committed": False,
                           "receipts_written": run.issued}
    return result, EXIT_OK


# ---------------------------------------------------------------------------
# ingest
# ---------------------------------------------------------------------------

def cmd_ingest(args):
    case = _load_case(args)
    opts = _opts(case)
    conn = storage.open_database(args.db_path, create=True)
    try:
        storage.begin(conn, write=True)
        state = storage.load_state(conn)
        _adopt_case_id(state, case["case_id"], args.db_path)
        before = len(state["receipts"])
        try:
            report, run = engine.run_case(state, case, opts,
                                          abort_after=opts["fail_after"])
        except engine.InjectedFault as exc:
            storage.rollback(conn)
            return _fault_result(args, case, opts, "before_commit",
                                 exc.issued), EXIT_OK
        if opts["failpoint"] == "before_commit":
            # The whole run is computed but never made durable.
            storage.rollback(conn)
            return _fault_result(args, case, opts, "before_commit",
                                 run.issued), EXIT_OK
        engine.detect_cycles(state)
        storage.commit(conn, state, state["epoch"])
        new_receipts = state["receipts"][before:]
        result = {
            "command": "ingest",
            "ok": True,
            "durable": True,
            "committed": True,
            "case_id": case["case_id"],
            "db": args.db_path,
            "announcements": report["announcements"],
            "rules_installed": report["rules_installed"],
            "batches": report["batches"],
            "gap_results": report["gap_results"],
            "rejected": report["rejected"],
            "deferred": report["deferred"],
            "receipts": [r["receipt"] for r in new_receipts],
            "new_receipts": new_receipts,
            "counts": _counts(state["receipts"]),
            "checksum": state.get("_checksum"),
            "state": _state_view(state),
        }
        if opts["failpoint"] == "after_commit":
            result["fault"] = {"point": "after_commit", "committed": True}
        return result, EXIT_OK
    finally:
        conn.close()


def _fault_result(args, case, opts, point, written):
    return {
        "command": "ingest",
        "ok": True,
        "durable": False,
        "committed": False,
        "case_id": case["case_id"],
        "db": args.db_path,
        "fault": {"point": point, "committed": False,
                  "receipts_rolled_back": written},
        "receipts": [],
        "note": "the transaction was rolled back; no receipt became durable",
    }


# ---------------------------------------------------------------------------
# dump / inspect / verify
# ---------------------------------------------------------------------------

def cmd_dump(args):
    case_id = _input_case_id(args)

    def build(state):
        if case_id is not None and state.get("case_id") is not None \
                and case_id != state["case_id"]:
            raise SimError("E_CASE_MISMATCH", "case_id",
                           "database holds case %r, not %r"
                           % (state["case_id"], case_id))
        view = _state_view(state)
        return {
            "command": "dump",
            "ok": True,
            "case_id": state.get("case_id"),
            "db": args.db_path,
            "epoch": view["epoch"],
            "checksum": state.get("_checksum"),
            "next_receipt": view["next_receipt"],
            "counts": _counts(state["receipts"]),
            "receipts": state["receipts"],
            "results": [
                {"src": r["src"], "gen": r["gen"], "seq": r["seq"],
                 "receipt": r["receipt"], "status": r["status"],
                 "reason": r["reason"], "reject_class": r["reject_class"],
                 "rule_id": r["rule_id"], "rule_epoch": r["rule_epoch"]}
                for r in state["receipts"]],
            "rules": view["rules"],
            "windows": view["windows"],
            "pending": view["pending"],
            "cycles": view["cycles"],
            "gaps": view["gaps"],
        }

    return _read_only(args, build), EXIT_OK


def cmd_inspect(args):
    case_id = _input_case_id(args)

    def build(state):
        if case_id is not None and state.get("case_id") is not None \
                and case_id != state["case_id"]:
            raise SimError("E_CASE_MISMATCH", "case_id",
                           "database holds case %r, not %r"
                           % (state["case_id"], case_id))
        view = _state_view(state)
        shortest = view["cycles"][0] if view["cycles"] else None
        return {
            "command": "inspect",
            "ok": True,
            "case_id": state.get("case_id"),
            "db": args.db_path,
            "epoch": view["epoch"],
            "windows": view["windows"],
            "gated_windows": [w for w in view["windows"]
                              if not w["gate_open"]],
            "pending": view["pending"],
            "cycles": view["cycles"],
            "cycle_count": len(view["cycles"]),
            "shortest_cycle": shortest,
            "gaps": view["gaps"],
            "rules": view["rules"],
            "next_receipt": view["next_receipt"],
            "next_to_adjudicate": view["next_to_adjudicate"],
            "blocked": bool(view["pending"]),
        }

    return _read_only(args, build), EXIT_OK


def cmd_verify(args):
    conn = storage.open_database(args.db_path, create=False)
    try:
        report = storage.verify(conn)
    finally:
        conn.close()
    result = {"command": "verify", "ok": report["ok"],
              "db": args.db_path, "report": report}
    return result, EXIT_OK if report["ok"] else EXIT_DB


_HANDLERS = {
    "simulate": cmd_simulate,
    "ingest": cmd_ingest,
    "dump": cmd_dump,
    "inspect": cmd_inspect,
    "verify": cmd_verify,
}


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def run(argv, stdout, stderr=None):
    command = None
    try:
        args = parse_argv(argv)
        command = args.command
        result, code = _HANDLERS[command](args)
    except SimError as exc:
        result = {"command": command, "ok": False,
                  "error": exc.to_dict()}
        code = exit_code_for(exc.code)
    except engine.InjectedFault as exc:  # pragma: no cover - internal guard
        result = {"command": command, "ok": False,
                  "error": {"code": "E_FAULT", "path": "",
                            "detail": str(exc)}}
        code = EXIT_ERROR
    jsonio.write_stdout(result, stdout)
    return code


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)
    return run(argv, sys.stdout, sys.stderr)
