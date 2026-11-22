"""Cross-source causal predecessors and the emergency policy barrier."""

import unittest

from support import (CaseTest, batch, case, event, gap, receipt_tuples,
                     receipts_by_slot, rule)

ALLOW_ALL = [rule("allow-all", 0, [("x", True)])]


def announce(src, gen, start, prev=None):
    payload = {"src": src, "gen": gen, "start_seq": start}
    if prev is not None:
        payload["prev_end_seq"] = prev
    return payload


def ref(src, gen, seq):
    return {"src": src, "gen": gen, "seq": seq}


class DependencyTest(CaseTest):
    def test_predecessor_from_another_source_blocks_adjudication(self):
        document = case(
            "cross", [announce("a", 1, 1), announce("b", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "ma", "ma", 1,
                                   predecessors=[ref("b", 1, 1)])])])
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        pending = payload["state"]["pending"][0]
        self.assertEqual(pending["wait"], "WAIT_DEP")
        self.assertEqual(pending["blocked_on"],
                         [{"src": "b", "gen": 1, "seq": 1}])

    def test_predecessor_arriving_later_releases_head(self):
        document = case(
            "cross2", [announce("a", 1, 1), announce("b", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "ma", "ma", 1,
                                   predecessors=[ref("b", 1, 1)])]),
             batch("b2", 10, [event("g", "b", 1, 1, "x", "mb", "mb", 2)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(set(by_slot), {("a", 1, 1), ("b", 1, 1)})
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("a", 1, 1)]["receipt"],
                         by_slot[("b", 1, 1)]["receipt"] + 1)
        self.assertIn(by_slot[("a", 1, 1)]["receipt"],
                      payload["batches"][1]["receipts"])

    def test_rejected_predecessor_permanently_rejects_current(self):
        document = case(
            "dep-reject", [announce("a", 1, 1), announce("b", 1, 1)],
            [rule("deny-b", 0, [("x", True), ("deny", False)])],
            [batch("b1", 0, [event("g", "b", 1, 1, "deny", "mb", "mb", 1)]),
             batch("b2", 5, [event("g", "a", 1, 1, "x", "ma", "ma", 2,
                                   predecessors=[ref("b", 1, 1)])])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("b", 1, 1)]["reject_class"], "PERMISSION")
        dependent = by_slot[("a", 1, 1)]
        self.assertEqual(dependent["status"], "REJECT")
        self.assertEqual(dependent["reason"], "DEPENDENCY_REJECTED")
        self.assertEqual(dependent["reject_class"], "DEPENDENCY")
        self.assertEqual(dependent["depends_on"],
                         by_slot[("b", 1, 1)]["receipt"])

    def test_dependency_and_permission_rejects_are_distinguishable(self):
        document = case(
            "classes", [announce("a", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 1, "nowhere", "m1",
                                   "m1", 1)])])
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"][0]["reject_class"], "PERMISSION")
        self.assertEqual(payload["receipts"][0]["reason"], "NO_RULE")

    def test_head_never_jumps_over_earlier_sequence(self):
        document = case(
            "jump", [announce("a", 1, 1), announce("b", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [event("g", "a", 1, 2, "x", "ma2", "ma2", 1,
                                   predecessors=[ref("b", 1, 1)]),
                             event("g", "a", 1, 1, "x", "ma1", "ma1", 2,
                                   predecessors=[ref("b", 1, 2)])])])
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        pending = {(p["src"], p["seq"]): p for p in payload["state"]["pending"]}
        # Only the window head is evaluated; the number behind it is
        # buffered and untouched.
        self.assertEqual(pending[("a", 1)]["wait"], "WAIT_DEP")
        self.assertIsNone(pending[("a", 2)]["wait"])

    def test_deterministic_next_pending_choice(self):
        document = case(
            "next", [announce("a", 1, 1), announce("b", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 2, "x", "a2", "a2", 1),
                event("g", "b", 1, 1, "x", "b1", "b1", 2,
                      predecessors=[ref("a", 1, 1)]),
            ])])
        payload = self.simulate(document)
        # a/1/1 was never delivered, so the chain is not a cycle: b/1/1 waits
        # on a key that simply does not exist and inspect names it.
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(payload["state"]["cycles"], [])
        pending = {(p["src"], p["seq"]): p for p in payload["state"]["pending"]}
        self.assertEqual(pending[("b", 1)]["blocked_on"],
                         [{"src": "a", "gen": 1, "seq": 1}])
        # ... and the missing number is resolved by an auditor gap proof.
        document["gaps"] = [gap("a", 1, 1, reason="never delivered",
                                arrival=10)]
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 1)]["reason"], "GAP_CLOSED")
        self.assertEqual(by_slot[("b", 1, 1)]["status"], "REJECT")
        self.assertEqual(by_slot[("b", 1, 1)]["reason"],
                         "DEPENDENCY_REJECTED")

    def test_mutual_wait_is_reported_as_a_cycle(self):
        document = case(
            "mutual", [announce("a", 1, 1), announce("b", 1, 1)], ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "a1", "a1", 1,
                      predecessors=[ref("b", 1, 2)]),
                event("g", "a", 1, 2, "x", "a2", "a2", 2),
                event("g", "b", 1, 1, "x", "b1", "b1", 3,
                      predecessors=[ref("a", 1, 1)]),
                event("g", "b", 1, 2, "x", "b2", "b2", 4),
            ])])
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        self.assertEqual(len(payload["state"]["cycles"]), 1)
        cycle = payload["state"]["cycles"][0]
        self.assertEqual(cycle["length"], 3)
        self.assertEqual(
            [{"src": m["src"], "seq": m["seq"]} for m in cycle["members"]],
            [{"src": "a", "seq": 1}, {"src": "b", "seq": 2},
             {"src": "b", "seq": 1}])

    def test_dependency_on_buffered_new_generation_message(self):
        document = case(
            "newgen-dep", [announce("a", 1, 1),
                           announce("b", 1, 1)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "a1", "a1", 1,
                      predecessors=[ref("b", 2, 1)]),
                event("g", "b", 2, 1, "x", "b1", "b1", 2)])])
        document["announcements"] = [
            {"src": "a", "gen": 1, "start_seq": 1},
            {"src": "b", "gen": 1, "start_seq": 1},
            {"src": "b", "gen": 2, "start_seq": 1, "prev_end_seq": 1},
        ]
        payload = self.simulate(document)
        self.assertEqual(payload["receipts"], [])
        pending = {(p["src"], p["gen"], p["seq"]): p
                   for p in payload["state"]["pending"]}
        self.assertEqual(pending[("b", 2, 1)]["wait"], "WAIT_GATE")
        # a/1/1 still sees no receipt for b/2/1 and must keep waiting.
        self.assertEqual(pending[("a", 1, 1)]["wait"], "WAIT_DEP")

    def test_pre_release_is_not_a_receipt_substitute(self):
        """A new-generation message is buffered, not decided; predecessors
        waiting on it stay waiting until the generation gate opens."""
        document = case(
            "pre", [announce("a", 1, 1),
                    announce("a", 2, 10, 2),
                    announce("b", 1, 1)],
            ALLOW_ALL,
            [batch("b1", 0, [
                event("g", "a", 2, 10, "x", "n1", "n1", 1)]),
             batch("b2", 5, [
                event("g", "b", 1, 1, "x", "b1", "b1", 2,
                      predecessors=[ref("a", 2, 10)])]),
             batch("b3", 100, [event("g", "a", 1, 2, "x", "m2", "m2", 100)])],
            gaps=[gap("a", 1, 1, arrival=50, reason="never sent")])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 1)]["reason"], "GAP_CLOSED")
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("a", 2, 10)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("b", 1, 1)]["status"], "ACCEPT")
        # The dependent message could only be decided after the generation
        # gate opened, i.e. after a/2/10 had a receipt.
        self.assertGreater(by_slot[("b", 1, 1)]["receipt"],
                           by_slot[("a", 2, 10)]["receipt"])


class BarrierTest(CaseTest):
    def test_superseded_rule_never_judges_a_released_message(self):
        document = case(
            "barrier", [announce("a", 1, 1), announce("b", 1, 1)],
            [rule("initial-allow", 0, [("x", True)])],
            [batch("b1", 0, [
                event("g", "a", 1, 1, "x", "a1", "a1", 1,
                      predecessors=[ref("b", 1, 1)])]),
             batch("b2", 10, [event("g", "b", 1, 1, "x", "b1", "b1", 2)]),
             batch("b3", 20, rules=[rule("revoke-x", 5, [("x", False)])],
                   epoch=5, events=[
                       event("g", "b", 1, 2, "x", "b2", "b2", 3)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        # b/1/1 unblocked a/1/1 while the original allow rule was in force,
        # so a/1/1 must have been decided at that moment, not later.
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("a", 1, 1)]["rule_epoch"], 0)
        self.assertEqual(by_slot[("b", 1, 2)]["status"], "REJECT")
        self.assertEqual(by_slot[("b", 1, 2)]["rule_epoch"], 5)

    def test_barrier_is_the_highest_epoch_at_or_below_logical_time(self):
        document = case(
            "epochs", [announce("a", 1, 1)],
            [rule("r0", 0, [("x", True)])],
            [batch("b1", 0, epoch=3, events=[
                event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, epoch=5, rules=[rule("r5", 5, [("x", False)])],
                   events=[event("g", "a", 1, 2, "x", "m2", "m2", 2)]),
             batch("b3", 20, epoch=9, rules=[rule("r9", 9, [("x", True)])],
                   events=[event("g", "a", 1, 3, "x", "m3", "m3", 3)])])
        payload = self.simulate(document)
        rules = [(r["seq"], r["rule_id"], r["rule_epoch"])
                 for r in payload["receipts"]]
        self.assertEqual(rules, [(1, "r0", 0), (2, "r5", 5), (3, "r9", 9)])

    def test_rule_versions_must_not_tie_ambiguously(self):
        document = case(
            "tie", [announce("a", 1, 1)],
            [rule("left", 4, [("x", True)], precedence=1),
             rule("right", 4, [("x", False)], precedence=1)], [])
        from support import run_cli
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_RULE_AMBIGUOUS")

    def test_conflicting_same_rule_version(self):
        document = case(
            "conflict-rule", [announce("a", 1, 1)],
            [rule("r", 2, [("x", True)])],
            [batch("b1", 10, epoch=2, rules=[rule("r", 2, [("x", False)])],
                   events=[event("g", "a", 1, 1, "x", "m1", "m1", 1)])])
        payload = self.simulate(document)
        # A batch that installs a contradicting version of an existing one
        # is refused as a whole and recorded; the run continues.
        self.assertEqual(payload["rejected"][0]["code"],
                         "E_RULE_VERSION_CONFLICT")
        self.assertEqual(payload["rejected"][0]["path"], "batches/0/rules/0")
        self.assertEqual(payload["receipts"], [])

    def test_rule_update_applies_to_buffered_message(self):
        document = case(
            "revoked-buffer", [announce("a", 1, 1)],
            [rule("allow-x", 0, [("x", True)])],
            [batch("b1", 0, [
                event("g", "a", 1, 2, "x", "m2", "m2", 1,
                      predecessors=[ref("a", 1, 1)])]),
             batch("b2", 10, epoch=4, rules=[rule("deny-x", 4, [("x", False)])],
                   events=[event("g", "a", 1, 1, "x", "m1", "m1", 2)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        # Both messages are adjudicated inside the epoch-4 transaction, so
        # neither is judged by the superseded allow rule.
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "REJECT")
        self.assertEqual(by_slot[("a", 1, 1)]["rule_id"], "deny-x")
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "REJECT")
        self.assertEqual(by_slot[("a", 1, 2)]["reason"],
                         "DEPENDENCY_REJECTED")
        self.assertEqual(by_slot[("a", 1, 2)]["reject_class"], "DEPENDENCY")

    def test_message_decided_before_update_keeps_old_rule(self):
        document = case(
            "pre-update", [announce("a", 1, 1)],
            [rule("allow-x", 0, [("x", True)])],
            [batch("b1", 0, [event("g", "a", 1, 1, "x", "m1", "m1", 1)]),
             batch("b2", 10, epoch=4,
                   rules=[rule("deny-x", 4, [("x", False)])],
                   events=[event("g", "a", 1, 2, "x", "m2", "m2", 2)])])
        payload = self.simulate(document)
        by_slot = receipts_by_slot(payload)
        self.assertEqual(by_slot[("a", 1, 1)]["status"], "ACCEPT")
        self.assertEqual(by_slot[("a", 1, 1)]["rule_id"], "allow-x")
        self.assertEqual(by_slot[("a", 1, 2)]["status"], "REJECT")
        self.assertEqual(by_slot[("a", 1, 2)]["rule_id"], "deny-x")


if __name__ == "__main__":
    unittest.main()
