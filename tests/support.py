"""Shared helpers for the assertion tests.

Every test builds a case document from the fixtures below, runs the command
line through :func:`run_cli` (never a real subprocess, never a socket) and
asserts on the parsed JSON.  Working directories are fresh temporary
directories so tests never see each other's SQLite files.
"""

import hashlib
import io
import json
import os
import shutil
import sys
import tempfile
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from solution import cli  # noqa: E402


def fingerprint(label):
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def event(gateway, src, gen, seq, dst, business_id, label, created_at,
          predecessors=None):
    payload = {
        "gateway": gateway,
        "src": src,
        "gen": gen,
        "seq": seq,
        "dst": dst,
        "business_id": business_id,
        "fingerprint": fingerprint(label),
        "created_at": created_at,
    }
    if predecessors is not None:
        payload["predecessors"] = predecessors
    return payload


def rule(rule_id, effective_epoch, routes, precedence=0):
    return {
        "id": rule_id,
        "effective_epoch": effective_epoch,
        "precedence": precedence,
        "routes": [{"dst": dst, "allow": allow} for dst, allow in routes],
    }


def case(case_id, announcements, rules, batches, gaps=None, **control):
    document = {
        "case_id": case_id,
        "announcements": announcements,
        "rules": rules,
        "batches": batches,
        "gaps": gaps or [],
    }
    if control:
        document["control"] = control
    return document


def batch(batch_id, arrival, events, epoch=0, rules=None):
    payload = {"id": batch_id, "arrival": arrival, "epoch": epoch,
               "events": events}
    if rules is not None:
        payload["rules"] = rules
    return payload


def gap(src, gen, seq, kind="PROVE", reason="audited", arrival=0,
        authority="auditor"):
    return {"src": src, "gen": gen, "seq": seq, "kind": kind,
            "reason": reason, "arrival": arrival, "authority": authority}


def run_cli(argv):
    """Run one command in-process.  Returns ``(status, payload)``."""
    stdout = io.StringIO()
    status = cli.run(list(argv), stdout, io.StringIO())
    text = stdout.getvalue()
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:  # pragma: no cover - a real bug
        raise AssertionError("stdout was not JSON: %r" % text)
    return status, payload


class CaseTest(unittest.TestCase):
    """Base class providing a scratch directory and case-file plumbing."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="sim-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self._written = 0
        self.last_case_id = "test-case"

    def path(self, name):
        return os.path.join(self.tmp, name)

    def write_case(self, document, name=None):
        self._written += 1
        name = name or "case-%02d.json" % self._written
        target = self.path(name)
        if isinstance(document, dict) and "case_id" in document:
            self.last_case_id = document["case_id"]
        with open(target, "w", encoding="utf-8") as handle:
            json.dump(document, handle)
        return target

    def write_raw(self, text, name="raw.json"):
        target = self.path(name)
        with open(target, "w", encoding="utf-8") as handle:
            handle.write(text)
        return target

    def simulate(self, document):
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(status, 0, payload)
        self.assertTrue(payload["ok"], payload)
        return payload

    def ingest(self, document, db="state.sqlite"):
        status, payload = run_cli(["ingest", self.write_case(document),
                                   "--db", self.path(db)])
        self.assertEqual(status, 0, payload)
        self.assertTrue(payload["ok"], payload)
        return payload

    def dump(self, db="state.sqlite", case_id=None):
        status, payload = run_cli(
            ["dump", self.write_case({"case_id": case_id or self.last_case_id}),
             "--db", self.path(db)])
        self.assertEqual(status, 0, payload)
        return payload

    def inspect(self, db="state.sqlite", case_id=None):
        status, payload = run_cli(
            ["inspect",
             self.write_case({"case_id": case_id or self.last_case_id}),
             "--db", self.path(db)])
        self.assertEqual(status, 0, payload)
        return payload

    def db_exists(self, db="state.sqlite"):
        return os.path.exists(self.path(db))


def receipts_by_slot(payload):
    return {(r["src"], r["gen"], r["seq"]): r for r in payload["receipts"]}


def receipt_tuples(payload):
    return [(r["src"], r["gen"], r["seq"], r["status"], r["reason"])
            for r in payload["receipts"]]


def pending_by_slot(payload):
    return {(p["src"], p["gen"], p["seq"]): p
            for p in payload["state"]["pending"]}
