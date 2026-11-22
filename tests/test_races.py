"""Races between retransmission, dependency release and gap closure."""

import unittest

from support import (CaseTest, batch, case, event, gap, receipt_tuples,
                     receipts_by_slot, rule)

ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


def announce(src, gen=1, start=1, prev=None):
    payload = {"src": src, "gen": gen, "start_seq": start}
    if prev is not None:
        payload["prev_end_seq"] = prev
    return payload


def ref(src, gen, seq):
    return {"src": src, "gen": gen, "seq": seq}


class RaceTest(CaseTest):
    def test_retransmission_arriving_with_the_closing_predecessor(self):
        """The original and its retransmission arrive in the same batch that
        finally supplies the blocked predecessor."""
        document = case(
            "race-1", [announce("a"), announce("b")], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "shared", "body", 1,
                      predecessors=[ref("b", 1, 1)])]),
             batch("b2", 10, [
                 event("g", "b", 1, 1, "x", "trigger", "trigger", 2),
                 event("g2", "a", 1, 1, "x", "shared", "body", 1,
                       predecessors=[ref("b", 1, 1)])])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        # Only one receipt for a/1/1: the second copy is a gateway resend.
        self.assertEqual(sorted(by_slot), [("a", 1, 1), ("b", 1, 1)])
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("a", 1, 1)]["gateways"], ["g", "g2"])
        self.assertEqual(len(payload["receipts"]), 2)

    def test_retransmission_racing_into_the_same_batch(self):
        document = case(
            "race-2", [announce("a")], ALLOW_ALL,
            [batch("b1", 0, [
                event("g1", "a", 1, 1, "x", "bid", "body", 1),
                event("g2", "a", 1, 1, "x", "bid", "body", 1)])])
        payload = self.simulate(document)
        self.assertEqual(len(payload["receipts"]), 1)
        self.assertEqual(payload["receipts"][0]["gateways"], ["g1", "g2"])
        self.assertEqual(payload["batches"][0]["deduped"][0]["seq"], 1)
        self.assertEqual(payload["batches"][0]["deduped"][0]["receipt"], 1)

    def test_predecessor_closed_by_gap_and_message_in_flight(self):
        document = case(
            "race-3", [announce("a"), announce("b")], ALLOW_ALL,
            [batch("b1", 10, [
                event("g", "a", 1, 2, "x", "after", "after", 2,
                      predecessors=[ref("b", 1, 1)])]),
             batch("b2", 20, [
                 event("g", "a", 1, 1, "x", "first", "first", 3)])],
            gaps=[gap("b", 1, 1, arrival=5, reason="never sent")])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("b", 1, 1)]["reason"], "GAP_CLOSED")
        # a/1/2 waits on b/1/1, which was refused by the auditor, so the
        # dependent message is permanently refused as a dependency failure.
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "REJECT")
        self.assertEqual(by_slot[("a", 1, 2)]["reason"],
                         "DEPENDENCY_REJECTED")
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")

    def test_gap_and_message_land_in_the_same_arrival(self):
        document = case(
            "race-4", [announce("a")], ALLOW_ALL,
            [batch("b1", 5, [
                event("g", "a", 1, 1, "x", "m1", "m1", 5),
                event("g", "a", 1, 3, "x", "m3", "m3", 6)])],
            gaps=[gap("a", 1, 2, arrival=5, reason="lost in transit")])
        payload = self.simulate(document)
        # Batches run before gap instructions at the same arrival; the gap
        # then closes the hole and everything after it resolves in order.
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "REJECT", "GAP_CLOSED"),
            ("a", 1, 3, "ACCEPT", "ACCEPT"),
        ])

    def test_duplicate_business_id_waits_for_the_original(self):
        document = case(
            "race-5", [announce("a"), announce("b")], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "b", 1, 1, "x", "bid", "body", 2,
                      predecessors=[ref("a", 1, 1)]),
                event("g", "a", 1, 1, "x", "bid", "body", 1)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        # a/1/1 was the first message seen in (src, gen, seq) order, so it
        # is the original; b/1/1 is a retransmission and its own dependency
        # on the original is satisfied by the original's receipt.
        self.assertEqual(sorted(by_slot), [("a", 1, 1), ("b", 1, 1)])
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("b", 1, 1)]["status"], "DUPLICATE")
        self.assertEqual(by_slot[("b", 1, 1)]["depends_on"],
                         by_slot[("a", 1, 1)]["receipt"])

    def test_dependency_on_a_retransmission_uses_its_outcome(self):
        document = case(
            "race-6", [announce("a"), announce("b")], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "bid", "body", 1),
                event("g", "a", 1, 2, "x", "bid", "body", 2),
                event("g", "b", 1, 1, "x", "later", "later", 3,
                      predecessors=[ref("a", 1, 2)])])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "DUPLICATE")
        self.assertEqual(by_slot[("b", 1, 1)]["status"], "ACCEPT")

    def test_retransmitted_rejection_blocks_dependents(self):
        document = case(
            "race-7", [announce("a"), announce("b")],
            [rule("deny-y", 0, [("x", True), ("y", False)])],
            [batch("b1", 0, [
                event("g", "a", 1, 1, "y", "bid", "body", 1),
                event("g", "a", 1, 2, "x", "bid", "body", 2),
                event("g", "b", 1, 1, "x", "later", "later", 3,
                      predecessors=[ref("a", 1, 2)])])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "DUPLICATE")
        self.assertEqual(by_slot[("a", 1, 2)]["outcome"], "DENY")
        self.assertEqual(by_slot[("b", 1, 1)]["reason"],
                         "DEPENDENCY_REJECTED")


if __name__ == "__main__":
    unittest.main()
