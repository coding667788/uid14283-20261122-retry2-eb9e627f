"""SQLite persistence for the simulator.

One invocation is one write transaction.  The engine mutates an in-memory
state mapping; :func:`commit` serialises that mapping into SQLite exactly
once, so "adjudicate + advance window + persist receipts" is atomic by
construction.  A failpoint that aborts before the commit leaves the file
byte-identical to what it was before the run, which is what the fault
recovery tests assert.

The store also carries a checksum row so :func:`verify` can tell a
truncated, modified or foreign file from a healthy one.
"""

import hashlib
import json
import os
import sqlite3

from . import engine
from .errors import SimError

SCHEMA_VERSION = "1"
FORMAT_NAME = "isolated-maintenance-gateway"

DDL = """
CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS announcements (
    src          TEXT NOT NULL,
    gen          INTEGER NOT NULL,
    start_seq    INTEGER NOT NULL,
    prev_end_seq INTEGER,
    PRIMARY KEY (src, gen)
);
CREATE TABLE IF NOT EXISTS rules (
    id              TEXT NOT NULL,
    effective_epoch INTEGER NOT NULL,
    precedence      INTEGER NOT NULL,
    routes          TEXT NOT NULL,
    PRIMARY KEY (id, effective_epoch)
);
CREATE TABLE IF NOT EXISTS windows (
    src    TEXT NOT NULL,
    gen    INTEGER NOT NULL,
    cursor INTEGER NOT NULL,
    PRIMARY KEY (src, gen)
);
CREATE TABLE IF NOT EXISTS entries (
    src          TEXT NOT NULL,
    gen          INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    dst          TEXT,
    business_id  TEXT,
    fingerprint  TEXT,
    created_at   INTEGER,
    gateways     TEXT NOT NULL,
    predecessors TEXT NOT NULL,
    state        TEXT NOT NULL,
    wait         TEXT,
    receipt      INTEGER,
    gap          TEXT,
    batch        TEXT,
    PRIMARY KEY (src, gen, seq)
);
CREATE TABLE IF NOT EXISTS business (
    business_id TEXT PRIMARY KEY,
    src         TEXT NOT NULL,
    gen         INTEGER NOT NULL,
    seq         INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS receipts (
    number        INTEGER PRIMARY KEY,
    src           TEXT NOT NULL,
    gen           INTEGER NOT NULL,
    seq           INTEGER NOT NULL,
    dst           TEXT,
    business_id   TEXT,
    fingerprint   TEXT,
    created_at    INTEGER,
    status        TEXT NOT NULL,
    outcome       TEXT NOT NULL,
    reason        TEXT NOT NULL,
    reject_class  TEXT,
    rule_id       TEXT,
    rule_epoch    INTEGER,
    epoch         INTEGER NOT NULL,
    batch         TEXT,
    depends_on    INTEGER,
    gateways      TEXT NOT NULL,
    gap_authority TEXT,
    gap_reason    TEXT,
    UNIQUE (src, gen, seq)
);
CREATE TABLE IF NOT EXISTS gaps (
    src       TEXT NOT NULL,
    gen       INTEGER NOT NULL,
    seq       INTEGER NOT NULL,
    kind      TEXT NOT NULL,
    reason    TEXT NOT NULL,
    authority TEXT NOT NULL,
    PRIMARY KEY (src, gen, seq)
);
CREATE TABLE IF NOT EXISTS actions (
    key    TEXT PRIMARY KEY,
    digest TEXT NOT NULL
);
"""

TABLES = ("announcements", "rules", "windows", "entries", "business",
          "receipts", "gaps", "actions")


def digest_of(payload):
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


# ---------------------------------------------------------------------------
# opening
# ---------------------------------------------------------------------------

def _statements():
    return [s.strip() for s in DDL.split(";") if s.strip()]


def open_database(path, *, create):
    if path is None:
        raise SimError("E_DB_REQUIRED", "",
                       "this command needs --db DB.sqlite")
    if not create and not os.path.exists(path):
        raise SimError("E_DB_MISSING", "", "database %s does not exist" % path)
    try:
        conn = sqlite3.connect(path, timeout=30.0, isolation_level=None)
    except sqlite3.Error as exc:
        raise SimError("E_DB_UNOPENABLE", "", "%s: %s" % (path, exc)) from None
    try:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        _ensure_schema(conn, path)
    except SimError:
        conn.close()
        raise
    except sqlite3.DatabaseError as exc:
        conn.close()
        raise SimError("E_DB_NOT_A_DATABASE", "",
                       "%s: %s" % (path, exc)) from None
    except Exception:
        conn.close()
        raise
    return conn


def _ensure_schema(conn, path):
    try:
        conn.execute("BEGIN IMMEDIATE")
        for statement in _statements():
            conn.execute(statement)
        row = conn.execute(
            "SELECT value FROM meta WHERE key = 'format'").fetchone()
        if row is None:
            conn.execute("INSERT INTO meta (key, value) VALUES ('format', ?)",
                         (FORMAT_NAME,))
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('schema_version', ?)",
                (SCHEMA_VERSION,))
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('epoch', '0')")
            conn.execute(
                "INSERT INTO meta (key, value) "
                "VALUES ('next_receipt', '1')")
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('checksum', '')")
        else:
            if row["value"] != FORMAT_NAME:
                raise SimError("E_DB_NOT_A_DATABASE", "",
                               "%s is not a simulator database" % path)
            version = conn.execute(
                "SELECT value FROM meta WHERE key = 'schema_version'"
            ).fetchone()
            if version is None or version["value"] != SCHEMA_VERSION:
                raise SimError("E_DB_SCHEMA", "",
                               "unsupported schema version in %s" % path)
        conn.execute("COMMIT")
    except sqlite3.DatabaseError as exc:
        _safe_rollback(conn)
        raise SimError("E_DB_NOT_A_DATABASE", "",
                       "%s: %s" % (path, exc)) from None


def _safe_rollback(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass


def begin(conn, write=False):
    """Open the read (or write) transaction every command runs inside.

    ``ingest`` takes ``BEGIN IMMEDIATE`` so two concurrent ingests serialise
    here, before either reads a cursor; the second one then re-reads the
    state the first committed and cannot hand out a receipt that already
    exists.
    """
    try:
        conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
    except sqlite3.Error as exc:
        raise SimError("E_DB_LOCKED", "", str(exc)) from None


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

def _json(value):
    return json.loads(value) if value is not None else None


def load_state(conn):
    state = engine.new_state()
    for row in conn.execute("SELECT src, gen, start_seq, prev_end_seq "
                            "FROM announcements ORDER BY src, gen"):
        state["announcements"][(row["src"], row["gen"])] = {
            "src": row["src"], "gen": row["gen"],
            "start_seq": row["start_seq"],
            "prev_end_seq": row["prev_end_seq"],
        }
    for row in conn.execute("SELECT id, effective_epoch, precedence, routes "
                            "FROM rules ORDER BY effective_epoch, "
                            "precedence, id"):
        state["rules"].append({
            "id": row["id"],
            "effective_epoch": row["effective_epoch"],
            "precedence": row["precedence"],
            "routes": _json(row["routes"]),
            "batch": None,
        })
    for row in conn.execute("SELECT src, gen, cursor FROM windows"):
        state["windows"][(row["src"], row["gen"])] = {"cursor": row["cursor"]}
    for row in conn.execute("SELECT * FROM entries"):
        state["entries"][(row["src"], row["gen"], row["seq"])] = {
            "src": row["src"], "gen": row["gen"], "seq": row["seq"],
            "dst": row["dst"], "business_id": row["business_id"],
            "fingerprint": row["fingerprint"],
            "created_at": row["created_at"],
            "gateways": _json(row["gateways"]),
            "predecessors": _json(row["predecessors"]),
            "state": row["state"], "wait": row["wait"],
            "receipt": row["receipt"], "gap": _json(row["gap"]),
            "batch": row["batch"],
        }
    for row in conn.execute("SELECT business_id, src, gen, seq FROM business"):
        state["business"][row["business_id"]] = (row["src"], row["gen"],
                                                 row["seq"])
    for row in conn.execute("SELECT * FROM receipts ORDER BY number"):
        state["receipts"].append({
            "receipt": row["number"], "src": row["src"], "gen": row["gen"],
            "seq": row["seq"], "dst": row["dst"],
            "business_id": row["business_id"],
            "fingerprint": row["fingerprint"],
            "created_at": row["created_at"],
            "status": row["status"], "outcome": row["outcome"],
            "reason": row["reason"],
            "reject_class": row["reject_class"],
            "rule_id": row["rule_id"], "rule_epoch": row["rule_epoch"],
            "epoch": row["epoch"], "batch": row["batch"],
            "depends_on": row["depends_on"],
            "gateways": _json(row["gateways"]),
            "gap_authority": row["gap_authority"],
            "gap_reason": row["gap_reason"],
        })
        state["receipt_index"][(row["src"], row["gen"], row["seq"])] = \
            row["number"]
    for row in conn.execute("SELECT key, digest FROM actions"):
        state["applied"][row["key"]] = row["digest"]
    for row in conn.execute("SELECT * FROM gaps"):
        state["gaps"][(row["src"], row["gen"], row["seq"])] = {
            "kind": row["kind"], "reason": row["reason"],
            "authority": row["authority"],
        }
    meta = {row["key"]: row["value"]
            for row in conn.execute("SELECT key, value FROM meta")}
    state["epoch"] = int(meta.get("epoch", 0))
    state["next_receipt"] = int(meta.get("next_receipt", 1))
    state["case_id"] = meta.get("case_id")
    state["_checksum"] = meta.get("checksum", "")
    return state


# ---------------------------------------------------------------------------
# flush
# ---------------------------------------------------------------------------

def _dump(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":"))


def _write_rows(conn, state):
    for table in TABLES:
        conn.execute("DELETE FROM %s" % table)
    conn.execute("DELETE FROM meta")
    conn.executemany(
        "INSERT INTO announcements (src, gen, start_seq, prev_end_seq) "
        "VALUES (?, ?, ?, ?)",
        [(a["src"], a["gen"], a["start_seq"], a["prev_end_seq"])
         for a in state["announcements"].values()])
    conn.executemany(
        "INSERT INTO rules (id, effective_epoch, precedence, routes) "
        "VALUES (?, ?, ?, ?)",
        [(r["id"], r["effective_epoch"], r["precedence"], _dump(r["routes"]))
         for r in state["rules"]])
    conn.executemany(
        "INSERT INTO windows (src, gen, cursor) VALUES (?, ?, ?)",
        [(k[0], k[1], w["cursor"]) for k, w in state["windows"].items()])
    conn.executemany(
        "INSERT INTO entries (src, gen, seq, dst, business_id, fingerprint, "
        "created_at, gateways, predecessors, state, wait, receipt, gap, "
        "batch) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(e["src"], e["gen"], e["seq"], e["dst"], e["business_id"],
          e["fingerprint"], e["created_at"], _dump(e["gateways"]),
          _dump(e["predecessors"]), e["state"], e["wait"], e["receipt"],
          _dump(e["gap"]) if e["gap"] is not None else None,
          e["batch"])
         for e in state["entries"].values()])
    conn.executemany(
        "INSERT INTO business (business_id, src, gen, seq) "
        "VALUES (?, ?, ?, ?)",
        [(bid, k[0], k[1], k[2]) for bid, k in state["business"].items()])
    conn.executemany(
        "INSERT INTO receipts (number, src, gen, seq, dst, business_id, "
        "fingerprint, created_at, status, outcome, reason, reject_class, "
        "rule_id, rule_epoch, epoch, batch, depends_on, gateways, "
        "gap_authority, gap_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
        "?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(r["receipt"], r["src"], r["gen"], r["seq"], r["dst"],
          r["business_id"], r["fingerprint"], r["created_at"], r["status"],
          r["outcome"], r["reason"], r["reject_class"], r["rule_id"],
          r["rule_epoch"],
          r["epoch"], r["batch"], r["depends_on"], _dump(r["gateways"]),
          r["gap_authority"], r["gap_reason"])
         for r in state["receipts"]])
    conn.executemany(
        "INSERT INTO gaps (src, gen, seq, kind, reason, authority) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [(k[0], k[1], k[2], g["kind"], g["reason"], g["authority"])
         for k, g in state["gaps"].items()])
    conn.executemany(
        "INSERT INTO actions (key, digest) VALUES (?, ?)",
        sorted(state["applied"].items()))


def _write_meta(conn, state, epoch):
    meta = [("format", FORMAT_NAME),
            ("schema_version", SCHEMA_VERSION),
            ("epoch", str(epoch)),
            ("next_receipt", str(state["next_receipt"])),
            ("checksum", "")]
    if state.get("case_id") is not None:
        meta.append(("case_id", state["case_id"]))
    conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta)


def commit(conn, state, epoch):
    """Write the whole state and commit once.

    Caller already holds the write transaction opened by :func:`begin`, so
    this only writes and commits: no nested ``BEGIN``.
    """
    state["epoch"] = epoch
    try:
        _write_rows(conn, state)
        _write_meta(conn, state, epoch)
        checksum = content_checksum(conn)
        conn.execute("UPDATE meta SET value = ? WHERE key = 'checksum'",
                     (checksum,))
        conn.execute("COMMIT")
    except sqlite3.Error as exc:
        _safe_rollback(conn)
        raise SimError("E_DB_WRITE", "", str(exc)) from None
    state["_checksum"] = checksum
    return checksum


def rollback(conn):
    _safe_rollback(conn)


# ---------------------------------------------------------------------------
# checksum / verification
# ---------------------------------------------------------------------------

def content_checksum(conn):
    """SHA-256 over every logical row, independent of physical page layout."""
    rows = []
    for table in TABLES:
        columns = [d[1] for d in
                   conn.execute("PRAGMA table_info(%s)" % table)]
        order = ", ".join(columns)
        for row in conn.execute("SELECT %s FROM %s ORDER BY %s"
                                % (order, table, order)):
            rows.append([table] + [row[i] for i in range(len(columns))])
    blob = json.dumps(rows, sort_keys=True, ensure_ascii=True,
                      separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def verify(conn):
    """Return a report describing the structural health of the store."""
    problems = []
    meta = {row["key"]: row["value"]
            for row in conn.execute("SELECT key, value FROM meta")}
    if meta.get("format") != FORMAT_NAME:
        problems.append({"code": "E_DB_FORMAT", "detail":
                         "missing or foreign format marker"})
    stored = meta.get("checksum", "")
    actual = content_checksum(conn)
    if stored != actual:
        problems.append({"code": "E_DB_CHECKSUM", "detail":
                         "stored checksum does not match content"})

    cursor_by_window = {}
    for row in conn.execute("SELECT src, gen, cursor FROM windows"):
        cursor_by_window[(row["src"], row["gen"])] = row["cursor"]
    receipt_numbers = []
    for row in conn.execute("SELECT number, src, gen, seq, status, outcome, "
                            "reason FROM receipts ORDER BY number"):
        receipt_numbers.append(row["number"])
        window = cursor_by_window.get((row["src"], row["gen"]))
        if window is None:
            problems.append({"code": "E_DB_ORPHAN_RECEIPT",
                             "detail": "%s/%s/%s has no window"
                                       % (row["src"], row["gen"], row["seq"])})
        elif row["seq"] >= window:
            problems.append({"code": "E_DB_CURSOR", "detail":
                             "%s/%s/%s is not below cursor %d"
                             % (row["src"], row["gen"], row["seq"], window)})
    if receipt_numbers and receipt_numbers != list(
            range(1, len(receipt_numbers) + 1)):
        problems.append({"code": "E_DB_RECEIPT_GAP", "detail":
                         "receipt numbers are not contiguous from 1"})
    next_receipt = int(meta.get("next_receipt", "1"))
    if next_receipt != len(receipt_numbers) + 1:
        problems.append({"code": "E_DB_RECEIPT_COUNTER", "detail":
                         "next_receipt %d does not follow %d receipts"
                         % (next_receipt, len(receipt_numbers))})
    for row in conn.execute("SELECT src, gen, start_seq FROM announcements"):
        window = cursor_by_window.get((row["src"], row["gen"]))
        if window is not None and window < row["start_seq"]:
            problems.append({"code": "E_DB_CURSOR", "detail":
                             "%s/%s cursor %d is below start_seq %d"
                             % (row["src"], row["gen"], window,
                                row["start_seq"])})
    return {
        "ok": not problems,
        "problems": problems,
        "checksum": actual,
        "stored_checksum": stored,
        "receipts": len(receipt_numbers),
        "windows": len(cursor_by_window),
    }
