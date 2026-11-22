"""Receiving-window behaviour: ordering, borders, gaps and duplicates.

Every test states the window it exercises in its name.  The engine exposes
no clock, so arrival times are the only ordering input and all assertions
are on receipts, which are the audit surface.
"""

import unittest

from support import (CaseTest, batch, case, event, gap, receipt_tuples,
                     receipts_by_slot, rule)


def announce(src, gen, start, prev=None):
    payload = {"src": src, "gen": gen, "start_seq": start}
    if prev is not None:
        payload["prev_end_seq"] = prev
    return payload


ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


class WindowBorderTest(CaseTest):
    def test_right_border_is_inclusive_for_max_window(self):
        document = case(
            "right", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, seq, "x", "m%d" % seq,
                                   "m%d" % seq, seq)
                             for seq in range(1, 5)])],
            max_window=4)
        payload = self.simulate(document)
        self.assertEqual(len(payload["receipts"]), 4)
        self.assertEqual([r["receipt"] for r in payload["receipts"]],
                         [1, 2, 3, 4])
        self.assertEqual(payload["deferred"], [])

    def test_no_batch_is_entirely_deferred_retains_none(self):
        document = case(
            "bound", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 5, "x", "m5", "m5", 5)])],
            max_window=4)
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        self.assertFalse(payload["batches"][0]["applied"])
        self.assertEqual(payload["deferred"][0]["reason"], "WINDOW_FULL")
        self.assertEqual(payload["deferred"][0]["window_left"], 1)
        self.assertEqual(payload["deferred"][0]["window_right"], 4)

    def test_deferred_window_slides_when_head_decides(self):
        document = case(
            "slide", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, [event("g", "a", 1, 4, "x", "m4", "m4", 4)]),
             batch("b3", 20, [event("g", "a", 1, 2, "x", "m2", "m2", 2),
                              event("g", "a", 1, 3, "x", "m3", "m3", 3)]),
             batch("b4", 30, [event("g", "a", 1, 4, "x", "m4", "m4", 4)])],
            max_window=2)
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
            ("a", 1, 3, "ACCEPT", "ACCEPT"),
            ("a", 1, 4, "ACCEPT", "ACCEPT"),
        ])
        # Sequence 4 was outside the window on its first delivery and was
        # refused outright; re-delivery after the window slid decided it.
        self.assertEqual(len(payload["deferred"]), 1)
        self.assertEqual(payload["deferred"][0]["seq"], 4)
        self.assertEqual(payload["deferred"][0]["window_right"], 3)

    def test_out_of_order_within_batch_is_sorted(self):
        document = case(
            "sort", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, seq, "x", "m%d" % seq,
                                   "m%d" % seq, seq)
                             for seq in (3, 1, 2)])])
        payload = self.simulate(document)
        self.assertEqual([r["seq"] for r in payload["receipts"]], [1, 2, 3])

    def test_rejected_number_still_advances_window(self):
        document = case(
            "revoke", [announce("a", 1, 1)],
            [rule("allow-a", 0, [("x", True), ("y", False)])],
            [batch("b1", 0, [event("g", "a", 1, 1, "y", "m1", "m1", 1),
                             event("g", "a", 1, 2, "x", "m2", "m2", 2)])])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "REJECT", "REVOKED"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
        ])
        window = payload["state"]["windows"][0]
        self.assertEqual(window["cursor"], 3)

    def test_no_rule_rejects_as_permission(self):
        document = case(
            "norule", [announce("a", 1, 1)],
            [rule("other", 0, [("y", True)])],
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)])])
        payload = self.simulate(document)
        receipt = payload["receipts"][0]
        self.assertEqual(receipt["reason"], "NO_RULE")
        self.assertEqual(receipt["reject_class"], "PERMISSION")

    def test_sequence_below_start_is_a_batch_error(self):
        document = case(
            "below", [announce("a", 1, 10)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 9, "x", "m", "m", 1)]),
             batch("b2", 10, [event("g", "a", 1, 10, "x", "n", "n", 2)])])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"],
                         "E_SEQ_BEFORE_START")
        self.assertEqual(payload["rejected"][0]["kind"], "batch")
        self.assertEqual(payload["rejected"][0]["path"],
                         "batches/0/events/0")
        self.assertEqual(receipt_tuples(payload),
                         [("a", 1, 10, "ACCEPT", "ACCEPT")])

    def test_generation_must_be_announced(self):
        document = case(
            "unannounced", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 2, 1, "x", "m", "m", 1)])])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"],
                         "E_GEN_NOT_ANNOUNCED")
        self.assertFalse(self.db_exists())


class DuplicateTest(CaseTest):
    def test_two_gateways_resend_same_request(self):
        document = case(
            "gwdup", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("gw-1", "a", 1, 1, "x", "m1", "body", 1)]),
             batch("b2", 5, [event("gw-2", "a", 1, 1, "x", "m1", "body", 1)])])
        payload = self.simulate(document)
        self.assertEqual(len(payload["receipts"]), 1)
        self.assertEqual(payload["batches"][1]["admitted"], 0)
        self.assertEqual(payload["batches"][1]["deduped"][0]["gateway"],
                         "gw-2")
        stored = payload["receipts"][0]
        self.assertEqual(stored["gateways"], ["gw-1", "gw-2"])

    def test_same_slot_two_gateways_different_body_rejects_batch(self):
        document = case(
            "conflict", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("gw-1", "a", 1, 1, "x", "m1", "one", 1)]),
             batch("b2", 5, [event("gw-2", "a", 1, 1, "x", "m1", "two", 1)]),
             batch("b3", 9, [event("gw-1", "a", 1, 2, "x", "m2", "m2", 2)])])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
        ])
        self.assertEqual(payload["rejected"][0]["code"], "E_SEQ_CONFLICT")
        self.assertEqual(payload["rejected"][0]["path"],
                         "batches/1/events/0")

    def test_same_slot_twice_in_one_batch(self):
        document = case(
            "intrabatch", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [
                event("gw-1", "a", 1, 1, "x", "m1", "one", 1),
                event("gw-2", "a", 1, 1, "x", "m1", "two", 1)])])
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(payload["rejected"][0]["code"], "E_SEQ_CONFLICT")

    def test_late_arrival_returns_original_receipt(self):
        document = case(
            "late", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1),
                             event("g", "a", 1, 2, "x", "m2", "m2", 2)]),
             batch("b2", 10, [event("other-gw", "a", 1, 1, "x", "m1",
                                    "m1", 1)])])
        payload = self.simulate(document)
        self.assertEqual(len(payload["receipts"]), 2)
        self.assertEqual(payload["batches"][1]["deduped"][0]["receipt"], 1)

    def test_business_id_retransmit_gets_duplicate_receipt(self):
        document = case(
            "retx", [announce("a", 1, 1)],
            [rule("allow-x", 0, [("x", True), ("y", False)]), ],
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "same-id",
                                   "body", 1)]),
             batch("b2", 5, [event("g", "a", 1, 2, "y", "same-id",
                                   "body", 2)])])
        payload = self.simulate(document)
        duplicate = payload["receipts"][1]
        self.assertEqual(duplicate["status"], "DUPLICATE")
        self.assertEqual(duplicate["outcome"], "ALLOW")
        self.assertEqual(duplicate["reason"], "RETRANSMIT")
        self.assertEqual(duplicate["depends_on"], 1)

    def test_business_id_reuse_with_different_body_is_protocol_reject(self):
        document = case(
            "idmismatch", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "bid", "one", 1)]),
             batch("b2", 5, [event("g", "a", 1, 2, "x", "bid", "two", 2)])])
        payload = self.simulate(document)
        receipt = payload["receipts"][1]
        self.assertEqual(receipt["status"], "REJECT")
        self.assertEqual(receipt["reason"], "ID_MISMATCH")
        self.assertEqual(receipt["reject_class"], "PROTOCOL")

    def test_retransmit_of_rejected_message_stays_rejected(self):
        document = case(
            "retxreject", [announce("a", 1, 1)],
            [rule("deny-y", 0, [("x", True), ("y", False)])],
            [batch("b1", 0, [event("g", "a", 1, 1, "y", "bid", "body", 1)]),
             batch("b2", 5, [event("g", "a", 1, 2, "x", "bid", "body", 2)])])
        payload = self.simulate(document)
        duplicate = payload["receipts"][1]
        self.assertEqual(duplicate["status"], "DUPLICATE")
        self.assertEqual(duplicate["outcome"], "DENY")
        self.assertEqual(duplicate["reason"], "REVOKED")
        self.assertEqual(duplicate["depends_on"], 1)

    def test_business_ids_never_tie_across_generations(self):
        document = case(
            "genid", [announce("a", 1, 1), announce("a", 2, 10, 2)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "bid", "body", 1),
                event("g", "a", 1, 2, "x", "other", "o", 2)]),
             batch("b2", 5, [event("g", "a", 2, 10, "x", "bid",
                                   "body", 3)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 2, 10)]["status"], "DUPLICATE")
        self.assertEqual(by_slot[("a", 2, 10)]["depends_on"],
                         by_slot[("a", 1, 1)]["receipt"])


class GenerationTest(CaseTest):
    def test_new_generation_buffers_but_waits_for_old_end(self):
        document = case(
            "gate", [announce("a", 1, 1), announce("a", 2, 10, 3)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "m1", "m1", 1),
                event("g", "a", 2, 10, "x", "n1", "n1", 2)])])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload),
                         [("a", 1, 1, "ACCEPT", "ACCEPT")])
        pending = payload["state"]["pending"]
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["gen"], 2)
        self.assertEqual(pending[0]["wait"], "WAIT_GATE")

    def test_new_generation_opens_after_old_generation_gap_proven(self):
        document = case(
            "gate-open", [announce("a", 1, 1), announce("a", 2, 10, 3)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "m1", "m1", 1),
                event("g", "a", 2, 10, "x", "n1", "n1", 2)]),
             batch("b2", 5, [event("g", "a", 1, 2, "x", "m2", "m2", 3)])],
            gaps=[gap("a", 1, 3, arrival=10, reason="never sent")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
            ("a", 1, 3, "REJECT", "GAP_CLOSED"),
            ("a", 2, 10, "ACCEPT", "ACCEPT"),
        ])
        self.assertEqual(payload["state"]["pending"], [])

    def test_gap_below_end_leaves_generation_gate_shut(self):
        document = case(
            "gate-shut", [announce("a", 1, 1), announce("a", 2, 10, 3)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 2, 10, "x", "n1", "n1", 1)])],
            gaps=[gap("a", 1, 1, arrival=5, reason="never sent")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload),
                         [("a", 1, 1, "REJECT", "GAP_CLOSED")])
        pending = payload["state"]["pending"]
        self.assertEqual(pending[0]["seq"], 10)
        self.assertEqual(pending[0]["wait"], "WAIT_GATE")
        window = payload["state"]["windows"]
        self.assertFalse([w for w in window if w["gen"] == 2][0]["gate_open"])

    def test_prev_end_must_exist(self):
        document = case(
            "badprev", [announce("a", 2, 10, 3)], ALLOW_ALL, [])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"],
                         "E_ANNOUNCEMENT_PREV_MISSING")

    def test_prev_end_below_prev_start(self):
        document = case(
            "badbounds", [announce("a", 1, 10), announce("a", 2, 20, 3)],
            ALLOW_ALL, [])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_ANNOUNCEMENT_BOUNDS")

    def test_generation_numbers_are_never_compared_by_sequence(self):
        # Generation 2 starts at a *lower* sequence than generation 1's end.
        document = case(
            "downward", [announce("a", 1, 50), announce("a", 2, 1, 51)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 50, "x", "m1", "m1", 1),
                event("g", "a", 1, 51, "x", "m2", "m2", 2)]),
             batch("b2", 5, [event("g", "a", 2, 1, "x", "m3", "m3", 3)])])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 50, "ACCEPT", "ACCEPT"),
            ("a", 1, 51, "ACCEPT", "ACCEPT"),
            ("a", 2, 1, "ACCEPT", "ACCEPT"),
        ])


class GapTest(CaseTest):
    def test_gap_closes_missing_number_with_reason(self):
        document = case(
            "gap", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 2, "x", "m2", "m2", 1)])],
            gaps=[gap("a", 1, 1, arrival=5, reason="lost at source",
                      authority="auditor")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "REJECT", "GAP_CLOSED"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
        ])
        receipt = payload["receipts"][0]
        self.assertEqual(receipt["gap_authority"], "auditor")
        self.assertEqual(receipt["gap_reason"], "lost at source")
        self.assertEqual(receipt["reject_class"], "PROTOCOL")

    def test_late_message_at_closed_gap_returns_original_receipt(self):
        document = case(
            "gap-late", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, [event("g", "a", 1, 2, "x", "m2", "m2", 2)])],
            gaps=[gap("a", 1, 2, arrival=5, reason="skip")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "REJECT", "GAP_CLOSED"),
        ])
        self.assertEqual(payload["batches"][1]["deduped"][0]["receipt"], 2)

    def test_gap_replay_with_same_reason_is_idempotent(self):
        document = case(
            "gap-idem", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 2, "x", "m2", "m2", 1)])],
            gaps=[gap("a", 1, 1, arrival=5, reason="lost"),
                  gap("a", 1, 1, arrival=6, reason="lost")])
        payload = self.simulate(document)
        self.assertEqual(len(payload["gap_results"]), 2)
        self.assertTrue(payload["gap_results"][1]["idempotent"])
        self.assertEqual(len(payload["receipts"]), 2)
        self.assertEqual(payload["gap_results"][1]["receipt"],
                         payload["gap_results"][0]["receipt"])

    def test_gap_outside_announced_generation(self):
        document = case(
            "gap-src", [announce("a", 1, 1)], ALLOW_ALL, [],
            gaps=[gap("b", 1, 1)])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"], "E_GEN_NOT_ANNOUNCED")

    def test_gap_from_untrusted_authority(self):
        document = case(
            "gap-auth", [announce("a", 1, 1)], ALLOW_ALL, [],
            gaps=[gap("a", 1, 1, authority="intruder")])
        payload = self.simulate(document)
        self.assertEqual(payload["rejected"][0]["code"], "E_GAP_AUTHORITY")
        self.assertEqual(payload["receipts"], [])

    def test_gap_at_an_empty_slot_does_not_touch_neighbours(self):
        document = case(
            "gap-buffered", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)])],
            gaps=[gap("a", 1, 2, arrival=1)])
        payload = self.simulate(document)
        # The neighbour keeps its own outcome; the gap only fills its slot.
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "ACCEPT", "ACCEPT"),
            ("a", 1, 2, "REJECT", "GAP_CLOSED"),
        ])

    def test_gap_at_occupied_slot_proves_it(self):
        document = case(
            "gap-deny", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 10, [event("g", "a", 1, 1, "x", "m1", "m1", 1)])],
            gaps=[gap("a", 1, 1, arrival=5, reason="policy")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "REJECT", "GAP_CLOSED")])

    def test_gap_rejects_a_buffered_waiting_message(self):
        document = case(
            "gap-buffered-wait", [announce("a", 1, 1), announce("b", 1, 1)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "m1", "m1", 1,
                      predecessors=[{"src": "b", "gen": 1, "seq": 1}])])],
            gaps=[gap("a", 1, 1, arrival=10,
                      reason="auditor rejects the buffered request")])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        # Heads the window but waits on a predecessor that never arrives;
        # a PROVE at that slot rejects the buffered ordinary message.
        self.assertEqual(by_slot[("a", 1, 1)]["reason"], "GAP_CLOSED")
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "REJECT")
        self.assertEqual(by_slot[("a", 1, 1)]["gap_reason"],
                         "auditor rejects the buffered request")
        self.assertEqual(payload["state"]["pending"], [])

    def test_late_message_at_proven_buffered_slot_returns_that_receipt(self):
        document = case(
            "gap-proven-slot", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 2, "x", "m2", "m2", 1)]),
             batch("b2", 20, [event("g", "a", 1, 1, "x", "m1", "m1", 2)]),
             batch("b3", 30, [event("g", "a", 1, 1, "x", "m1", "m1", 2),
                              event("g2", "a", 1, 2, "x", "m2", "m2", 1)])],
            gaps=[gap("a", 1, 1, arrival=10, reason="lost")])
        payload = self.simulate(document)
        self.assertEqual(receipt_tuples(payload), [
            ("a", 1, 1, "REJECT", "GAP_CLOSED"),
            ("a", 1, 2, "ACCEPT", "ACCEPT"),
        ])
        self.assertEqual(len(payload["batches"][2]["deduped"]), 2)
        for item in payload["batches"][2]["deduped"]:
            self.assertIn(item["receipt"], (1, 2))


from support import run_cli  # noqa: E402  (used by error-path tests)


if __name__ == "__main__":
    unittest.main()
