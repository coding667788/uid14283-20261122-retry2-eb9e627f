"""An independent, deliberately naive model of the receive endpoint.

This module exists purely to cross-check the real engine.  It is written as
a different kind of program on purpose: it keeps every message in a list and
recomputes the whole outcome from scratch after each arrival, instead of
walking windows with a cursor.  It covers the feature subset used by the
randomised differential tests -- announced generations, permissions, sequence
order, retransmission by business id and causal predecessors -- and nothing
about cycles or auditor instructions.

If the two implementations ever disagree, one of them is wrong, and the
differential test says exactly which message they disagree about.
"""

ACCEPT = "ACCEPT"
REJECT = "REJECT"
DUPLICATE = "DUPLICATE"


class NaiveModel:
    def __init__(self, announcements, rules, max_window):
        self.announcements = {}
        for ann in announcements:
            self.announcements[(ann["src"], ann["gen"])] = {
                "start": ann["start_seq"],
                "prev_end": ann.get("prev_end_seq"),
            }
        self.rules = []
        for rule in rules:
            self.rules.append({
                "id": rule["id"],
                "epoch": rule["effective_epoch"],
                "precedence": rule.get("precedence", 0),
                "routes": {r["dst"]: r["allow"] for r in rule["routes"]},
            })
        self.max_window = max_window
        self.epoch = 0
        # slot -> message dict
        self.messages = {}
        # business id -> (src, gen, seq) of the first message seen
        self.first_seen = {}
        # slot -> outcome dict
        self.decided = {}
        self.receipts = []

    # -- policy ----------------------------------------------------------

    def _rule(self, dst):
        chosen = None
        for rule in self.rules:
            if rule["epoch"] > self.epoch or dst not in rule["routes"]:
                continue
            rank = (rule["epoch"], rule["precedence"], rule["id"])
            if chosen is None or rank > chosen[0]:
                chosen = (rank, rule)
        return chosen[1] if chosen else None

    def add_rule(self, rule):
        self.rules.append({
            "id": rule["id"],
            "epoch": rule["effective_epoch"],
            "precedence": rule.get("precedence", 0),
            "routes": {r["dst"]: r["allow"] for r in rule["routes"]},
        })

    # -- helpers ---------------------------------------------------------

    def _slots(self, src, gen):
        return sorted(seq for (s, g, seq) in self.messages
                      if s == src and g == gen)

    def _cursor(self, src, gen):
        ann = self.announcements[(src, gen)]
        cursor = ann["start"]
        while (src, gen, cursor) in self.decided:
            cursor += 1
        return cursor

    def _gate_open(self, src, gen):
        ann = self.announcements[(src, gen)]
        prev_end = ann["prev_end"]
        if prev_end is None or (src, gen - 1) not in self.announcements:
            return True
        return self._cursor(src, gen - 1) > prev_end

    # -- protocol --------------------------------------------------------

    def offer(self, message):
        """Register a message.  Returns 'new', 'deduped' or 'deferred'."""
        key = (message["src"], message["gen"], message["seq"])
        ann = self.announcements.get((message["src"], message["gen"]))
        if ann is None:
            return "rejected"
        if message["seq"] < ann["start"]:
            return "rejected"
        cursor = self._cursor(message["src"], message["gen"])
        if message["seq"] < cursor:
            return "deduped"
        if message["seq"] >= cursor + self.max_window:
            return "deferred"
        if key in self.messages:
            self.messages[key]["gateways"].append(message["gateway"])
            return "deduped"
        stored = dict(message)
        stored["gateways"] = [message["gateway"]]
        self.messages[key] = stored
        self.first_seen.setdefault(message["business_id"], key)
        self.settle()
        return "new"

    def settle(self):
        """Decide everything that can be decided, from scratch, repeatedly."""
        while True:
            progressed = False
            for (src, gen), ann in sorted(self.announcements.items()):
                cursor = self._cursor(src, gen)
                key = (src, gen, cursor)
                if key not in self.messages:
                    continue
                if not self._gate_open(src, gen):
                    continue
                message = self.messages[key]
                outcome = self._judge(message)
                if outcome is None:
                    continue
                self.decided[key] = outcome
                self.receipts.append(dict(outcome, **{
                    "src": src, "gen": gen, "seq": cursor}))
                progressed = True
            if not progressed:
                return

    def _judge(self, message):
        """Return an outcome dict, or None when the message must wait."""
        for ref in message.get("predecessors", []):
            ref_key = (ref["src"], ref["gen"], ref["seq"])
            if ref_key not in self.decided:
                return None
            if self.decided[ref_key]["status"] == REJECT:
                return {"status": REJECT, "reason": "DEPENDENCY_REJECTED"}
        key = (message["src"], message["gen"], message["seq"])
        first = self.first_seen.get(message["business_id"])
        if first is not None and first != key:
            if self.messages[first]["fingerprint"] != message["fingerprint"]:
                return {"status": REJECT, "reason": "ID_MISMATCH"}
            origin = self.decided.get(first)
            if origin is None:
                return None
            if origin["status"] == REJECT:
                return {"status": DUPLICATE, "reason": origin["reason"]}
            return {"status": DUPLICATE, "reason": "RETRANSMIT"}
        rule = self._rule(message["dst"])
        if rule is None:
            return {"status": REJECT, "reason": "NO_RULE"}
        if not rule["routes"][message["dst"]]:
            return {"status": REJECT, "reason": "REVOKED"}
        return {"status": ACCEPT, "reason": "ACCEPT", "rule": rule["id"]}

    def pending(self):
        out = []
        for key in sorted(self.messages):
            if key in self.decided:
                continue
            out.append(key)
        return out

    def summary(self):
        return [(r["src"], r["gen"], r["seq"], r["status"], r["reason"])
                for r in self.receipts]

    def slots_decided(self):
        return set(self.decided)
