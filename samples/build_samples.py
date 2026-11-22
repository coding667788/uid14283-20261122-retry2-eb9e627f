"""Generate the documented, directly runnable sample case files.

Run once with ``python samples/build_samples.py`` after editing a fixture;
the JSON files it writes are committed so the documented commands work on a
fresh checkout without running any generator.
"""

import hashlib
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))


def fp(label):
    return hashlib.sha256(label.encode("utf-8")).hexdigest()


def event(gateway, src, gen, seq, dst, business_id, label, created_at,
          predecessors=None):
    payload = {
        "gateway": gateway, "src": src, "gen": gen, "seq": seq,
        "dst": dst, "business_id": business_id, "fingerprint": fp(label),
        "created_at": created_at,
    }
    if predecessors:
        payload["predecessors"] = predecessors
    return payload


def rule(rule_id, epoch, routes, precedence=0):
    return {"id": rule_id, "effective_epoch": epoch, "precedence": precedence,
            "routes": [{"dst": d, "allow": a} for d, a in routes]}


def ref(src, gen, seq):
    return {"src": src, "gen": gen, "seq": seq}


def write(name, document):
    path = os.path.join(HERE, name)
    with open(path, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(document, handle, indent=2, sort_keys=False)
        handle.write("\n")


# ---------------------------------------------------------------------------
# 1. both gateways forward the same control-plane message
# ---------------------------------------------------------------------------
write("01_two_gateways.json", {
    "case_id": "01-two-gateways",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 100},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True), ("zone-2", False)]),
    ],
    "batches": [
        {"id": "arrival-a", "arrival": 1000, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 100, "zone-1", "cfg-42",
                  "enable maintenance window", 1000),
            event("relay-a", "gw-north", 1, 101, "zone-2", "cfg-43",
                  "retire mirror", 1001),
        ]},
        {"id": "arrival-b", "arrival": 1010, "epoch": 0, "events": [
            event("relay-b", "gw-north", 1, 100, "zone-1", "cfg-42",
                  "enable maintenance window", 1000),
            event("relay-b", "gw-north", 1, 102, "zone-1", "cfg-44",
                  "drain queue", 1002),
        ]},
    ],
    "gaps": [],
})

# ---------------------------------------------------------------------------
# 2. generation hand-over stuck behind a lost sequence number
# ---------------------------------------------------------------------------
write("02_generation_gap.json", {
    "case_id": "02-generation-gap",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
        {"src": "gw-north", "gen": 2, "start_seq": 1, "prev_end_seq": 4},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "batches": [
        {"id": "old-gen", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-1", "m-1", "one", 100),
            event("relay-a", "gw-north", 1, 2, "zone-1", "m-2", "two", 101),
        ]},
        {"id": "new-gen-pre", "arrival": 200, "epoch": 0, "events": [
            event("relay-b", "gw-north", 2, 1, "zone-1", "n-1", "new-one", 200),
        ]},
    ],
    "gaps": [
        {"src": "gw-north", "gen": 1, "seq": 3, "kind": "PROVE",
         "reason": "gateway lost the frame; audit A-17", "arrival": 300,
         "authority": "auditor"},
        {"src": "gw-north", "gen": 1, "seq": 4, "kind": "PROVE",
         "reason": "gateway lost the frame; audit A-17", "arrival": 301,
         "authority": "auditor"},
    ],
})

# ---------------------------------------------------------------------------
# 3. cross-source causality and an audited ring
# ---------------------------------------------------------------------------
write("03_cycle_and_release.json", {
    "case_id": "03-cycle-and-release",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
        {"src": "gw-south", "gen": 1, "start_seq": 1},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "batches": [
        {"id": "mutual", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-1", "n-1", "north-one",
                  100, predecessors=[ref("gw-south", 1, 1)]),
            event("relay-a", "gw-south", 1, 1, "zone-1", "s-1", "south-one",
                  101, predecessors=[ref("gw-north", 1, 1)]),
            event("relay-a", "gw-north", 1, 2, "zone-1", "n-2", "north-two",
                  102),
        ]},
    ],
    "gaps": [
        {"src": "gw-north", "gen": 1, "seq": 1, "kind": "CANCEL",
         "reason": "audited deadlock, ticket OPS-88", "arrival": 400,
         "authority": "auditor"},
    ],
})

# ---------------------------------------------------------------------------
# 4. emergency policy barrier over a message that had to wait
# ---------------------------------------------------------------------------
write("04_policy_barrier.json", {
    "case_id": "04-policy-barrier",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
        {"src": "gw-south", "gen": 1, "start_seq": 1},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True), ("zone-2", True)]),
    ],
    "batches": [
        {"id": "waiting", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-2", "n-1",
                  "hold zone-2", 100,
                  predecessors=[ref("gw-south", 1, 1)]),
        ]},
        # The predecessor becomes ready in the very transaction that revokes
        # zone-2, so the released message must be judged by the new version.
        {"id": "emergency", "arrival": 300, "epoch": 5, "events": [
            event("relay-a", "gw-south", 1, 1, "zone-1", "s-1",
                  "south announce", 200),
            event("relay-b", "gw-north", 1, 2, "zone-2", "n-2",
                  "hold zone-2 again", 300),
        ],
         "rules": [
             rule("fleet-default", 5, [("zone-1", True), ("zone-2", False)]),
         ]},
    ],
    "gaps": [],
})

# ---------------------------------------------------------------------------
# 5. window borders and a duplicate business id with a different body
# ---------------------------------------------------------------------------
write("05_window_and_ids.json", {
    "case_id": "05-window-and-ids",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 10},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "control": {"max_window": 3},
    "batches": [
        {"id": "b-1", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 10, "zone-1", "cfg-1", "a", 100),
            event("relay-a", "gw-north", 1, 13, "zone-1", "cfg-4", "d", 103),
        ]},
        {"id": "b-2", "arrival": 200, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 11, "zone-1", "cfg-2", "b", 101),
            event("relay-b", "gw-north", 1, 12, "zone-1", "cfg-3", "c", 102),
        ]},
        {"id": "b-3", "arrival": 300, "epoch": 0, "events": [
            event("relay-c", "gw-north", 1, 13, "zone-1", "cfg-4", "d", 103),
            event("relay-c", "gw-north", 1, 14, "zone-1", "cfg-1",
                  "not the same request", 104),
        ]},
    ],
    "gaps": [],
})

# ---------------------------------------------------------------------------
# 6. fault injection: a crash that must not duplicate receipts
# ---------------------------------------------------------------------------
write("06_fault_recovery.json", {
    "case_id": "06-fault-recovery",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "control": {"failpoint": "before_commit", "fail_after": 2},
    "batches": [
        {"id": "abort", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-1", "m-1", "one", 100),
            event("relay-a", "gw-north", 1, 2, "zone-1", "m-2", "two", 101),
            event("relay-a", "gw-north", 1, 3, "zone-1", "m-3", "three", 102),
        ]},
    ],
    "gaps": [],
})

write("06_fault_recovery_retry.json", {
    "case_id": "06-fault-recovery",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "batches": [
        {"id": "abort", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-1", "m-1", "one", 100),
            event("relay-a", "gw-north", 1, 2, "zone-1", "m-2", "two", 101),
            event("relay-a", "gw-north", 1, 3, "zone-1", "m-3", "three", 102),
        ]},
    ],
    "gaps": [],
})

# ---------------------------------------------------------------------------
# 7. a document with a structural error, used to show the error contract
# ---------------------------------------------------------------------------
write("07_bad_field.json", {
    "case_id": "07-bad-field",
    "announcements": [
        {"src": "gw-north", "gen": 1, "start_seq": 1},
    ],
    "rules": [
        rule("fleet-default", 0, [("zone-1", True)]),
    ],
    "batches": [
        {"id": "broken", "arrival": 100, "epoch": 0, "events": [
            event("relay-a", "gw-north", 1, 1, "zone-1", "m-1", "one", 100),
        ]},
    ],
    "gaps": [],
})
# Break the second event by hand so the JSON stays otherwise valid.
broken = json.load(open(os.path.join(HERE, "07_bad_field.json"),
                        encoding="utf-8"))
broken["batches"][0]["events"].append({
    "gateway": "relay-a", "src": "gw-north", "gen": 1, "seq": 2,
    "dst": "zone-1", "business_id": "m-2", "fingerprint": "not-a-sha256",
    "created_at": 101,
})
write("07_bad_field.json", broken)
