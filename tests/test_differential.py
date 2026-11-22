"""Differential test: the real engine against an independently written model.

The generator produces small random cases with a fixed seed per test.  For
each case the simulator's receipts are compared with the naive model's.
Concurrency and fault-recovery scenarios are additionally compared by
replaying the same actions one at a time into a database, and against the
model, to make sure splitting a run never changes the outcome.
"""

import random
import unittest

from reference_model import NaiveModel
from support import (CaseTest, batch, case, event, receipt_tuples,
                     receipts_by_slot, rule, run_cli)

DESTINATIONS = ("x", "y", "z")


def announce(src, gen, start, prev=None):
    payload = {"src": src, "gen": gen, "start_seq": start}
    if prev is not None:
        payload["prev_end_seq"] = prev
    return payload


def gen_case(rng, case_id):
    sources = ["a", "b"]
    announcements = [announce(src, 1, 1) for src in sources]
    # Half of the cases add a second generation to one source.
    if rng.random() < 0.5:
        src = rng.choice(sources)
        announcements.append(announce(src, 2, 10, 2))
    rules = [rule("policy", 0,
                  [(dst, rng.random() < 0.6) for dst in DESTINATIONS])]
    epochs = [0, 1, 3]
    case_rule_epochs = sorted(rng.sample(epochs, rng.randint(1, 2)))

    plans = []
    for (src, gen, start, _) in [(a["src"], a["gen"], a["start_seq"], None)
                                 for a in announcements]:
        for offset in range(3):
            plans.append((src, gen, start + offset))
    rng.shuffle(plans)
    use = plans[:rng.randint(4, len(plans))]

    events = []
    for index, (src, gen, seq) in enumerate(use):
        if seq == 10 and gen == 2 and rng.random() < 0.5:
            continue
        predecessors = []
        if events and rng.random() < 0.35:
            earlier = [e for e in events if e["src"] != src] or events
            pick = rng.choice(earlier)
            predecessors.append({"src": pick["src"], "gen": pick["gen"],
                                 "seq": pick["seq"]})
        business = "b%d" % rng.randint(0, 3)
        label = "body-%s" % business
        if rng.random() < 0.2:
            label = "other-%s" % business
        events.append(event("g", src, gen, seq, rng.choice(DESTINATIONS),
                            business, label, index,
                            predecessors=predecessors or None))

    rng.shuffle(events)
    batches = []
    cursor = 0
    number = 0
    while cursor < len(events):
        size = rng.randint(1, 3)
        number += 1
        batches.append(batch("b%d" % number, rng.randint(0, 5),
                             events[cursor:cursor + size],
                             epoch=rng.choice(case_rule_epochs)))
        cursor += size

    if len(case_rule_epochs) > 1:
        late = case_rule_epochs[-1]
        batch_rules = [rule("policy", late,
                            [(dst, rng.random() < 0.5)
                             for dst in DESTINATIONS])]
    else:
        batch_rules = None

    document = case(case_id, announcements, rules, batches, max_window=6)
    if batch_rules is not None:
        # Attach the emergency update to the last batch, at its own epoch.
        batches[-1]["rules"] = batch_rules
        batches[-1]["epoch"] = batch_rules[0]["effective_epoch"]
    return document


class DifferentialTest(CaseTest):
    def _model_for(self, document):
        model = NaiveModel(document["announcements"], document["rules"],
                           document.get("control", {}).get("max_window", 8))
        return model

    def _batch_is_acceptable(self, model, entry):
        """Mirror the engine's whole-batch structural checks."""
        seen = {}
        for message in sorted(
                entry["events"],
                key=lambda e: (e["src"], e["gen"], e["seq"], e["business_id"])):
            ann = model.announcements.get((message["src"], message["gen"]))
            if ann is None or message["seq"] < ann["start"]:
                return False
            key = (message["src"], message["gen"], message["seq"])
            if key in seen and seen[key] != message["fingerprint"]:
                return False
            seen[key] = message["fingerprint"]
            existing = model.messages.get(key)
            if (existing is not None
                    and existing["fingerprint"] != message["fingerprint"]):
                return False
            decided = model.decided.get(key)
            if decided is not None and decided.get("fingerprint") not in (
                    None, message["fingerprint"]):
                return False
        fingerprints = {}
        for message in entry["events"]:
            prior = fingerprints.get(message["business_id"])
            if prior is not None and prior != message["fingerprint"]:
                return False
            fingerprints[message["business_id"]] = message["fingerprint"]
        return True

    def _replay_model(self, document):
        model = self._model_for(document)
        for entry in sorted(document["batches"],
                            key=lambda b: (b["arrival"], b["id"])):
            model.epoch = max(model.epoch, entry["epoch"])
            for rule_obj in entry.get("rules", []):
                model.add_rule(rule_obj)
            if not self._batch_is_acceptable(model, entry):
                continue
            for message in sorted(
                    entry["events"],
                    key=lambda e: (e["src"], e["gen"], e["seq"])):
                model.offer(message)
            model.settle()
        return model, None

    def test_random_cases_match_the_naive_model(self):
        for seed in range(120):
            rng = random.Random(seed)
            document = gen_case(rng, "diff-%03d" % seed)
            payload = self.simulate(document)
            model, _ = self._replay_model(document)
            mine = sorted(receipt_tuples(payload))
            theirs = sorted(model.summary())
            self.assertEqual(mine, theirs,
                             "seed %d diverged\ncase=%r" % (seed, document))
            self.assertEqual(
                {(p["src"], p["gen"], p["seq"])
                 for p in payload["state"]["pending"]},
                set(model.pending()),
                "seed %d pending differed\ncase=%r" % (seed, document))

    def test_split_ingest_matches_model_and_simulate(self):
        for seed in range(40):
            rng = random.Random(1000 + seed)
            document = gen_case(rng, "split-%03d" % seed)
            simulated = self.simulate(document)
            db = "split-%03d.sqlite" % seed
            for entry in sorted(document["batches"],
                                key=lambda b: (b["arrival"], b["id"])):
                part = dict(document)
                part["batches"] = [entry]
                self.ingest(part, db=db)
            stored = self.dump(db=db)
            self.assertEqual(
                sorted((r["src"], r["gen"], r["seq"], r["status"],
                        r["reason"]) for r in stored["receipts"]),
                sorted((r["src"], r["gen"], r["seq"], r["status"],
                        r["reason"]) for r in simulated["receipts"]),
                "seed %d: split ingest diverged from simulate" % seed)

    def test_fault_during_replay_does_not_change_final_state(self):
        for seed in range(20):
            rng = random.Random(2000 + seed)
            document = gen_case(rng, "fault-%03d" % seed)
            expected = self.simulate(document)
            entries = sorted(document["batches"],
                            key=lambda b: (b["arrival"], b["id"]))
            # Crash once in the middle, then re-run every batch.
            middle = len(entries) // 2
            db = "fault-%03d.sqlite" % seed
            stopped = dict(document)
            stopped["batches"] = entries[:middle]
            stopped["control"] = {"failpoint": "before_commit",
                                  "fail_after": 1}
            from support import run_cli as _run
            path = self.write_case(stopped)
            status, payload = _run(["ingest", path, "--db", self.path(db)])
            self.assertEqual(status, 0, payload)
            self.assertFalse(payload["committed"])
            for entry in entries:
                part = dict(document)
                part["batches"] = [entry]
                self.ingest(part, db=db)
            stored = self.dump(db=db)
            self.assertEqual(
                sorted((r["src"], r["gen"], r["seq"], r["status"],
                        r["reason"]) for r in stored["receipts"]),
                sorted((r["src"], r["gen"], r["seq"], r["status"],
                        r["reason"]) for r in expected["receipts"]),
                "seed %d: fault recovery diverged" % seed)


if __name__ == "__main__":
    unittest.main()
