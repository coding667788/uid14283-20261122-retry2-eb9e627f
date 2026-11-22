"""Legal-but-mutual message rings and the auditor instructions that break them."""

import unittest

from support import (CaseTest, batch, case, event, gap, receipt_tuples,
                     receipts_by_slot, rule)

ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


def announce(src, start=1):
    return {"src": src, "gen": 1, "start_seq": start}


def ref(src, gen, seq):
    return {"src": src, "gen": gen, "seq": seq}


def two_source_ring():
    """Two sources, each locally contiguous, mutually deadlocked.

    a/1 waits on b/1, b/1 waits on a/1; a/2 and b/2 are behind their own
    heads.  Either window alone would make progress, together they cannot.
    """
    return case(
        "ring", [announce("a"), announce("b")], ALLOW_ALL,
        [batch("b1", 0, [
            event("g", "a", 1, 1, "x", "a1", "a1", 1,
                  predecessors=[ref("b", 1, 1)]),
            event("g", "a", 1, 2, "x", "a2", "a2", 2),
            event("g", "b", 1, 1, "x", "b1", "b1", 3,
                  predecessors=[ref("a", 1, 1)]),
            event("g", "b", 1, 2, "x", "b2", "b2", 4),
        ])])


class RingTest(CaseTest):
    def test_two_source_deadlock_stops_both_windows(self):
        payload = self.simulate(two_source_ring())
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(len(payload["state"]["cycles"]), 1)
        cycle = payload["state"]["cycles"][0]
        # The ring is a/1 -> b/1 -> a/1.  b/2 is behind b/1 and is marked
        # as part of the stalled component, but the *reported* shortest cycle
        # is the two-member ring itself.
        self.assertEqual(cycle["length"], 2)
        self.assertEqual(cycle["members"],
                         [{"src": "a", "gen": 1, "seq": 1},
                          {"src": "b", "gen": 1, "seq": 1}])
        self.assertEqual(payload["state"]["windows"][0]["cursor"], 1)
        self.assertEqual(payload["state"]["windows"][1]["cursor"], 1)

    def test_inspect_reports_shortest_cycle(self):
        self.ingest(two_source_ring())
        payload = self.inspect()
        self.assertEqual(payload["cycle_count"], 1)
        shortest = payload["shortest_cycle"]
        self.assertEqual(shortest["length"], 2)
        edges = [(e["from"]["src"], e["to"]["src"])
                 for e in shortest["edges"]]
        self.assertEqual(edges, [("a", "b"), ("b", "a")])
        self.assertTrue(payload["blocked"])

    def test_long_ring_reports_only_the_shortest(self):
        document = case(
            "rings", [announce("a"), announce("b")], ALLOW_ALL,
            [batch("b1", 0, [
                # 2-cycle a/1 <-> b/1
                event("g", "a", 1, 1, "x", "m1", "m1", 1,
                      predecessors=[ref("b", 1, 1)]),
                event("g", "b", 1, 1, "x", "m2", "m2", 2,
                      predecessors=[ref("a", 1, 1)]),
            ])])
        payload = self.simulate(document)
        self.assertEqual(payload["state"]["cycles"][0]["length"], 2)
        self.assertEqual(
            [{"src": m["src"], "seq": m["seq"]}
             for m in payload["state"]["cycles"][0]["members"]],
            [{"src": "a", "seq": 1}, {"src": "b", "seq": 1}])

    def test_cancel_breaks_ring_and_cascades(self):
        document = two_source_ring()
        document["gaps"] = [gap("a", 1, 1, kind="CANCEL",
                                reason="audited deadlock", arrival=50)]
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 1)]["reason"], "CYCLE_CANCELLED")
        self.assertEqual(by_slot[("a", 1, 1)]["reject_class"], "PROTOCOL")
        self.assertEqual(by_slot[("b", 1, 1)]["reason"],
                         "DEPENDENCY_REJECTED")
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("b", 1, 2)]["status"], "ACCEPT")
        self.assertEqual(payload["state"]["cycles"], [])

    def test_cancel_collateral_is_reported(self):
        document = two_source_ring()
        document["gaps"] = [gap("a", 1, 1, kind="CANCEL", reason="deadlock",
                                arrival=50)]
        payload = self.simulate(document)
        result = payload["gap_results"][0]
        self.assertTrue(result["cancelled_cycle"])
        reasons = {c["reason"] for c in result["collateral"]}
        self.assertIn("DEPENDENCY_REJECTED", reasons)

    def test_prove_cannot_be_used_inside_a_ring(self):
        document = two_source_ring()
        document["gaps"] = [gap("a", 1, 1, kind="PROVE", arrival=50)]
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"], "E_GAP_CYCLED")
        self.assertEqual(payload["receipts"], [])

    def test_cancel_outside_a_ring_is_refused(self):
        document = case(
            "nocancel", [announce("a")], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)])],
            gaps=[gap("a", 1, 2, kind="CANCEL", arrival=5)])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"], "E_GAP_NOT_CYCLED")
        self.assertEqual(receipt_tuples(payload),
                         [("a", 1, 1, "ACCEPT", "ACCEPT")])

    def test_cancel_at_an_already_decided_slot_returns_its_receipt(self):
        document = case(
            "cancel-decided", [announce("a")], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)])],
            gaps=[gap("a", 1, 1, kind="CANCEL", arrival=5)])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"], [])
        self.assertTrue(payload["gap_results"][0]["idempotent"])
        self.assertEqual(payload["gap_results"][0]["receipt"], 1)

    def test_cancel_is_idempotent(self):
        document = two_source_ring()
        document["gaps"] = [
            gap("a", 1, 1, kind="CANCEL", reason="deadlock", arrival=50),
            gap("a", 1, 1, kind="CANCEL", reason="deadlock", arrival=60),
        ]
        payload = self.simulate(document)
        self.assertTrue(payload["gap_results"][1]["idempotent"])
        self.assertEqual(len(payload["receipts"]), 4)

    def test_cancel_rejects_ring_member_despite_pending_entity(self):
        document = two_source_ring()
        document["gaps"] = [gap("b", 1, 1, kind="CANCEL", reason="halt",
                                arrival=50)]
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("b", 1, 1)]["reason"], "CYCLE_CANCELLED")
        self.assertEqual(by_slot[("a", 1, 1)]["reason"],
                         "DEPENDENCY_REJECTED")

    def test_new_messages_after_cycle_are_not_frozen(self):
        document = two_source_ring()
        document["batches"].append(batch("b2", 100, [
            event("g", "c", 1, 1, "x", "c1", "c1", 100)]))
        document["announcements"].append(announce("c"))
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("c", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(len(payload["state"]["cycles"]), 1)

    def test_retransmission_does_not_break_a_ring(self):
        document = two_source_ring()
        document["batches"].append(batch("b2", 100, [
            event("g", "a", 1, 1, "x", "a1", "a1", 1,
                  predecessors=[ref("b", 1, 1)]),
            event("g", "b", 1, 1, "x", "b1", "b1", 3,
                  predecessors=[ref("a", 1, 1)]),
        ]))
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(len(payload["state"]["cycles"]), 1)


if __name__ == "__main__":
    unittest.main()
