"""Concurrent ingest: many writers, one database, no duplicated receipts.

SQLite serialises writers; the simulator relies on that plus a single
transaction per invocation.  The tests below drive real threads with their
own connections, which is the closest an offline CPU-only simulator gets to
"two gateways forwarding at the same moment".
"""

import sqlite3
import threading
import unittest

from support import (CaseTest, batch, case, event, receipt_tuples,
                     receipts_by_slot, rule, run_cli)

ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


def announce(src, gen=1, start=1):
    return {"src": src, "gen": gen, "start_seq": start}


def ev(seq, bid=None, label=None):
    return event("g", "a", 1, seq, "x", bid or "m%d" % seq,
                 label or "m%d" % seq, seq)


class ConcurrencyTest(CaseTest):
    def _run_threads(self, documents, expect_status=0):
        results = [None] * len(documents)
        errors = []

        def worker(index, document):
            path = self.write_case(document, "thread-%02d.json" % index)
            try:
                results[index] = run_cli(
                    ["ingest", path, "--db", self.path("state.sqlite")])
            except Exception as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i, doc))
                   for i, doc in enumerate(documents)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual(errors, [])
        for result in results:
            self.assertIsNotNone(result)
            status, payload = result
            self.assertEqual(status, expect_status, payload)
        return [payload for _, payload in results]

    def test_distinct_batches_ingested_concurrently(self):
        documents = [
            case("conc", [announce("a")], ALLOW_ALL,
                 [batch("b%d" % seq, seq, [ev(seq)])])
            for seq in range(1, 9)
        ]
        payloads = self._run_threads(documents)
        dumped = self.dump()
        self.assertEqual(len(dumped["receipts"]), 8)
        self.assertEqual([r["receipt"] for r in dumped["receipts"]],
                         list(range(1, 9)))
        self.assertEqual({(r["src"], r["seq"]) for r in dumped["receipts"]},
                         {("a", seq) for seq in range(1, 9)})
        # Across all invocations no slot was decided twice.
        issued = sorted(number for payload in payloads
                        for number in payload["receipts"])
        self.assertEqual(issued, list(range(1, 9)))

    def test_same_batch_ingested_concurrently_issues_once(self):
        document = case("same", [announce("a")], ALLOW_ALL,
                        [batch("b1", 0, [ev(1), ev(2)])])
        payloads = self._run_threads([document] * 6)
        issued = sorted(number for payload in payloads
                        for number in payload["receipts"])
        self.assertEqual(issued, [1, 2])
        self.assertEqual(len(self.dump()["receipts"]), 2)

    def test_two_gateways_racing_the_same_request(self):
        left = case("race", [announce("a")], ALLOW_ALL,
                    [batch("b-left", 0, [
                        event("gw-left", "a", 1, 1, "x", "m1", "m1", 1)])])
        right = case("race", [announce("a")], ALLOW_ALL,
                     [batch("b-right", 0, [
                         event("gw-right", "a", 1, 1, "x", "m1", "m1", 1)])])
        payloads = self._run_threads([left, right, left, right])
        dumped = self.dump()
        self.assertEqual(len(dumped["receipts"]), 1)
        receipt = dumped["receipts"][0]
        self.assertEqual(receipt["status"], "ACCEPT")
        self.assertEqual(set(receipt["gateways"]), {"gw-left", "gw-right"})
        issued = [number for payload in payloads
                  for number in payload["receipts"]]
        self.assertEqual(issued, [1])

    def test_concurrent_conflicting_body_rejects_one_side(self):
        one = case("clash", [announce("a")], ALLOW_ALL,
                   [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1",
                                         "one", 1)])])
        two = case("clash", [announce("a")], ALLOW_ALL,
                   [batch("b2", 0, [event("g", "a", 1, 1, "x", "m1",
                                         "two", 1)])])
        payloads = self._run_threads([one, two])
        rejected = [p["rejected"] for p in payloads if p["rejected"]]
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0][0]["code"], "E_SEQ_CONFLICT")
        self.assertEqual(len(self.dump()["receipts"]), 1)

    def test_verify_after_concurrent_load(self):
        documents = [
            case("load", [announce("a")], ALLOW_ALL,
                 [batch("b%d" % seq, seq, [ev(seq)])])
            for seq in range(1, 5)
        ]
        self._run_threads(documents)
        status, payload = run_cli(
            ["verify", self.write_case({"case_id": "load"}),
             "--db", self.path("state.sqlite")])
        self.assertEqual(status, 0, payload)
        self.assertTrue(payload["ok"])


if __name__ == "__main__":
    unittest.main()
