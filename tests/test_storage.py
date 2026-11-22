"""Durability: transaction boundaries, crash recovery, dump and verify."""

import os
import shutil
import sqlite3
import unittest

from support import (CaseTest, batch, case, event, gap, receipt_tuples,
                     receipts_by_slot, rule, run_cli)

ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


def announce(src, gen=1, start=1, prev=None):
    payload = {"src": src, "gen": gen, "start_seq": start}
    if prev is not None:
        payload["prev_end_seq"] = prev
    return payload


def ref(src, gen, seq):
    return {"src": src, "gen": gen, "seq": seq}


def simple_case(case_id="durable", count=3):
    return case(
        case_id, [announce("a")], ALLOW_ALL,
        [batch("b1", 0, [event("g", "a", 1, seq, "x", "m%d" % seq,
                               "m%d" % seq, seq)
                         for seq in range(1, count + 1)])])


class TransactionTest(CaseTest):
    def test_receipts_are_written_durably(self):
        self.ingest(simple_case())
        payload = self.dump()
        self.assertEqual(len(payload["receipts"]), 3)
        self.assertEqual([r["receipt"] for r in payload["receipts"]],
                         [1, 2, 3])

    def test_dump_reports_rule_version_per_result(self):
        document = case(
            "rulev", [announce("a")],
            [rule("allow-x", 0, [("x", True)])],
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, epoch=3,
                   rules=[rule("deny-x", 3, [("x", False)])],
                   events=[event("g", "a", 1, 2, "x", "m2", "m2", 2)])])
        self.ingest(document)
        payload = self.dump()
        results = {(r["src"], r["seq"]): r for r in payload["results"]}
        self.assertEqual(results[("a", 1)]["rule_id"], "allow-x")
        self.assertEqual(results[("a", 1)]["rule_epoch"], 0)
        self.assertEqual(results[("a", 2)]["rule_id"], "deny-x")
        self.assertEqual(results[("a", 2)]["rule_epoch"], 3)
        self.assertEqual(
            [{"id": r["id"], "epoch": r["effective_epoch"]}
             for r in payload["rules"]],
            [{"id": "allow-x", "epoch": 0}, {"id": "deny-x", "epoch": 3}])

    def test_replaying_identical_ingest_makes_no_new_receipts(self):
        document = simple_case()
        first = self.ingest(document)
        second = self.ingest(document)
        self.assertEqual(first["receipts"], [1, 2, 3])
        self.assertEqual(second["receipts"], [])
        self.assertEqual(self.dump()["checksum"], first["checksum"])

    def test_replaying_with_different_content_is_refused(self):
        document = simple_case()
        self.ingest(document)
        changed = simple_case()
        changed["batches"][0]["events"][0]["fingerprint"] = "0" * 64
        payload = self.ingest(changed)
        self.assertEqual(payload["rejected"][0]["code"],
                         "E_BATCH_REPLAY_CONFLICT")
        self.assertEqual(len(self.dump()["receipts"]), 3)

    def test_split_across_invocations_is_equivalent_to_simulate(self):
        whole = case(
            "split", [announce("a"), announce("b")],
            [rule("allow-all", 0, [("x", True)])],
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "a1", "a1", 1,
                      predecessors=[ref("b", 1, 1)])]),
             batch("b2", 10, [event("g", "b", 1, 1, "x", "b1", "b1", 2)]),
             batch("b3", 20, [event("g", "a", 1, 2, "x", "a2", "a2", 3)])])
        simulated = self.simulate(whole)
        for step in whole["batches"]:
            part = dict(whole)
            part["batches"] = [step]
            self.ingest(part)
        stored = self.dump()
        self.assertEqual(
            [(r["src"], r["seq"], r["status"], r["reason"])
             for r in stored["receipts"]],
            [(r["src"], r["seq"], r["status"], r["reason"])
             for r in simulated["receipts"]])


class AtomicityTest(CaseTest):
    """A refused batch must leave no partial effect behind."""

    def test_refused_batch_installs_no_rule_and_bumps_no_epoch(self):
        document = case(
            "atomic", [announce("a")], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, epoch=9,
                   rules=[rule("deny-x", 9, [("x", False)])],
                   events=[
                       event("g", "a", 1, 2, "x", "m2", "one", 2),
                       event("g", "a", 1, 2, "x", "m2", "two", 3)])])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"], "E_SEQ_CONFLICT")
        self.assertEqual(payload["state"]["epoch"], 0)
        self.assertEqual([r["id"] for r in payload["state"]["rules"]],
                         ["allow-all"])
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 1)]["rule_id"], "allow-all")
        self.assertEqual(len(payload["receipts"]), 1)

    def test_refused_unannounced_batch_buffers_nothing(self):
        document = case(
            "atomic-gen", [announce("a")], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "m1", "m1", 1),
                event("g", "b", 1, 1, "x", "m2", "m2", 2)])])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"],
                         "E_GEN_NOT_ANNOUNCED")
        self.assertEqual(payload["state"]["pending"], [])
        self.assertEqual(payload["state"]["windows"], [])


class FaultTest(CaseTest):
    def test_before_commit_rollback_leaves_no_receipt(self):
        document = simple_case()
        document["control"] = {"failpoint": "before_commit",
                               "fail_after": 2}
        payload = self.ingest(document)
        self.assertFalse(payload["committed"])
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(self.dump()["receipts"], [])

    def test_stop_after_n_receipts_rolls_back_everything(self):
        document = simple_case(count=5)
        document["control"] = {"failpoint": "before_commit",
                               "fail_after": 0}
        payload = self.ingest(document)
        self.assertEqual(payload["fault"]["receipts_rolled_back"], 0)
        # Nothing was made durable: no window was even opened.
        dumped = self.dump()
        self.assertEqual(dumped["receipts"], [])
        self.assertEqual(dumped["windows"], [])

    def test_restart_after_rollback_does_not_duplicate(self):
        document = simple_case()
        document["control"] = {"failpoint": "before_commit",
                               "fail_after": 2}
        self.ingest(document)
        recovered = self.ingest(simple_case())
        self.assertEqual(recovered["receipts"], [1, 2, 3])
        self.assertEqual(recovered["counts"]["ACCEPT"], 3)
        self.assertEqual(len(self.dump()["receipts"]), 3)

    def test_after_commit_reports_committed_and_is_not_reissued(self):
        document = simple_case()
        document["control"] = {"failpoint": "after_commit"}
        payload = self.ingest(document)
        self.assertTrue(payload["fault"]["committed"])
        retry = self.ingest(simple_case())
        self.assertEqual(retry["receipts"], [])
        self.assertEqual(len(self.dump()["receipts"]), 3)

    def test_database_survives_process_style_reopen(self):
        self.ingest(simple_case())
        checksum = self.dump()["checksum"]
        # Force a fresh connection the way a restarted process would.
        self.dump()
        self.ingest(case("durable", [announce("a")], ALLOW_ALL, []))
        self.assertEqual(self.dump()["checksum"], checksum)

    def test_case_id_must_match_database(self):
        self.ingest(simple_case("first"))
        status, payload = run_cli(
            ["ingest", self.write_case(simple_case("second")),
             "--db", self.path("state.sqlite")])
        self.assertEqual(status, 3)
        self.assertEqual(payload["error"]["code"], "E_CASE_MISMATCH")


class VerifyTest(CaseTest):
    def test_fresh_database_verifies(self):
        self.ingest(simple_case())
        status, payload = run_cli(
            ["verify", self.write_case({"case_id": "durable"}),
             "--db", self.path("state.sqlite")])
        self.assertEqual(status, 0)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["report"]["problems"], [])

    def test_tampered_row_is_detected(self):
        self.ingest(simple_case())
        db = self.path("state.sqlite")
        connection = sqlite3.connect(db)
        connection.execute("UPDATE receipts SET reason = 'HACKED' "
                           "WHERE number = 2")
        connection.commit()
        connection.close()
        status, payload = run_cli(
            ["verify", self.write_case({"case_id": "durable"}), "--db", db])
        self.assertEqual(status, 4)
        codes = [p["code"] for p in payload["report"]["problems"]]
        self.assertIn("E_DB_CHECKSUM", codes)

    def test_foreign_file_is_not_a_database(self):
        target = self.path("foreign.sqlite")
        with open(target, "wb") as handle:
            handle.write(b"this is not sqlite" * 100)
        status, payload = run_cli(
            ["dump", self.write_case({"case_id": "x"}), "--db", target])
        self.assertEqual(status, 4)
        self.assertEqual(payload["error"]["code"], "E_DB_NOT_A_DATABASE")


class InspectTest(CaseTest):
    def test_inspect_lists_pending_and_gated_windows(self):
        document = case(
            "inspectable", [announce("a"), announce("a", 2, 10, 2)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 2, 10, "x", "n1", "n1", 1)])])
        self.ingest(document)
        payload = self.inspect()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["cycle_count"], 0)
        self.assertIsNone(payload["shortest_cycle"])
        self.assertEqual(len(payload["pending"]), 1)
        self.assertEqual(payload["pending"][0]["wait"], "WAIT_GATE")
        self.assertEqual([w["gen"] for w in payload["gated_windows"]], [2])

    def test_inspect_does_not_modify_state(self):
        self.ingest(simple_case())
        before = self.dump()["checksum"]
        self.inspect()
        self.assertEqual(self.dump()["checksum"], before)


if __name__ == "__main__":
    unittest.main()
