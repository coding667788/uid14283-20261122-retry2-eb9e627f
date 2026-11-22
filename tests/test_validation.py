"""Structural validation and CLI contract tests."""

import json
import os
import unittest

from support import CaseTest, case, rule, run_cli, event, batch


class UsageTest(CaseTest):
    def test_no_arguments_reports_usage(self):
        status, payload = run_cli([])
        self.assertEqual(status, 2)
        self.assertEqual(payload["error"]["code"], "E_USAGE")

    def test_unknown_command(self):
        status, payload = run_cli(["transmit", "x.json"])
        self.assertEqual(status, 2)
        self.assertEqual(payload["error"]["code"], "E_USAGE")

    def test_ingest_requires_db(self):
        path = self.write_case({"case_id": "x"})
        status, payload = run_cli(["ingest", path])
        self.assertEqual(status, 2)
        self.assertEqual(payload["error"]["code"], "E_USAGE")

    def test_simulate_rejects_db(self):
        path = self.write_case({"case_id": "x"})
        status, payload = run_cli(["simulate", path, "--db", "x.sqlite"])
        self.assertEqual(status, 2)
        self.assertEqual(payload["error"]["code"], "E_USAGE")

    def test_dump_on_missing_database(self):
        path = self.write_case({"case_id": "x"})
        status, payload = run_cli(["dump", path, "--db", self.path("nope")])
        self.assertEqual(status, 4)
        self.assertEqual(payload["error"]["code"], "E_DB_MISSING")

    def test_missing_case_file(self):
        status, payload = run_cli(["simulate", self.path("nope.json")])
        self.assertEqual(status, 3)
        self.assertEqual(payload["error"]["code"], "E_INPUT_UNREADABLE")

    def test_verify_db_alias(self):
        document = case("alias", [{"src": "a", "gen": 1, "start_seq": 1}],
                        [rule("r", 0, [("x", True)])], [])
        self.ingest(document)
        status, payload = run_cli(
            ["verify-db", self.write_case({"case_id": "alias"}),
             "--db", self.path("state.sqlite")])
        self.assertEqual(status, 0, payload)
        self.assertTrue(payload["ok"])


class JsonTest(CaseTest):
    def test_invalid_json(self):
        status, payload = run_cli(
            ["simulate", self.write_raw("{\"case_id\": ")])
        self.assertEqual(status, 3)
        self.assertEqual(payload["error"]["code"], "E_JSON_INVALID")

    def test_duplicate_key_rejected(self):
        text = ('{"case_id": "a", "case_id": "b", "announcements": [], '
                '"rules": []}')
        status, payload = run_cli(["simulate", self.write_raw(text)])
        self.assertEqual(payload["error"]["code"], "E_JSON_DUPLICATE_KEY")

    def test_nan_rejected(self):
        text = '{"case_id": "a", "announcements": [], "rules": [], ' \
               '"x": NaN}'
        status, payload = run_cli(["simulate", self.write_raw(text)])
        self.assertEqual(payload["error"]["code"], "E_JSON_INVALID")

    def test_non_object_document(self):
        status, payload = run_cli(["simulate", self.write_raw("[1, 2]")])
        self.assertEqual(payload["error"]["code"], "E_TYPE_EXPECTED_OBJECT")

    def test_utf8_bom_accepted(self):
        text = '﻿{"case_id": "bom", "announcements": [], "rules": []}'
        status, payload = run_cli(["simulate", self.write_raw(text)])
        self.assertEqual(status, 0, payload)

    def test_output_is_ascii_json(self):
        document = case("zz", [{"src": "a", "gen": 1, "start_seq": 1}],
                        [rule("r", 0, [("x", True)])], [])
        import io
        from solution import cli
        buffer = io.StringIO()
        cli.run(["simulate", self.write_case(document)], buffer)
        text = buffer.getvalue()
        text.encode("ascii")
        self.assertTrue(text.endswith("\n"))


class StructureTest(CaseTest):
    def _minimal(self, **overrides):
        document = case("struct", [{"src": "a", "gen": 1, "start_seq": 1}],
                        [rule("r", 0, [("x", True)])], [])
        document.update(overrides)
        return document

    def test_missing_table_has_path(self):
        document = {"case_id": "s", "rules": []}
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_FIELD_MISSING")
        self.assertEqual(payload["error"]["path"], "announcements")

    def test_missing_event_field_has_indexed_path(self):
        document = self._minimal(batches=[
            batch("b", 0, [event("g", "a", 1, 1, "x", "m", "m", 1)])])
        del document["batches"][0]["events"][0]["dst"]
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_FIELD_MISSING")
        self.assertEqual(payload["error"]["path"], "batches/0/events/0/dst")

    def test_unknown_field_rejected(self):
        document = self._minimal()
        document["surprise"] = True
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_FIELD_UNKNOWN")
        self.assertEqual(payload["error"]["path"], "surprise")

    def test_boolean_is_not_an_integer(self):
        document = self._minimal(batches=[
            batch("b", 0, [event("g", "a", 1, 1, "x", "m", "m", 1)])])
        document["batches"][0]["events"][0]["seq"] = True
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_TYPE_EXPECTED_INTEGER")
        self.assertEqual(payload["error"]["path"], "batches/0/events/0/seq")

    def test_out_of_range_sequence(self):
        document = self._minimal(batches=[
            batch("b", 0, [event("g", "a", 1, 1, "x", "m", "m", 1)])])
        document["batches"][0]["events"][0]["seq"] = -1
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_VALUE_OUT_OF_RANGE")

    def test_bad_fingerprint(self):
        document = self._minimal(batches=[
            batch("b", 0, [event("g", "a", 1, 1, "x", "m", "m", 1)])])
        document["batches"][0]["events"][0]["fingerprint"] = "abc"
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_FINGERPRINT_INVALID")
        self.assertEqual(payload["error"]["path"],
                         "batches/0/events/0/fingerprint")

    def test_identifier_charset_under_strict_fields(self):
        document = self._minimal(batches=[
            batch("b", 0, [event("g", "a b", 1, 1, "x", "m", "m", 1)])])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_VALUE_INVALID")

    def test_empty_batch_rejected(self):
        document = self._minimal(batches=[batch("b", 0, [])])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_BATCH_EMPTY")

    def test_duplicate_rule_version(self):
        document = self._minimal(rules=[rule("r", 0, [("x", True)]),
                                        rule("r", 0, [("y", True)])])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_DUP_RULE")
        self.assertEqual(payload["error"]["path"], "rules/1/effective_epoch")

    def test_bad_gap_kind(self):
        document = self._minimal(gaps=[{"src": "a", "gen": 1, "seq": 1,
                                        "kind": "OPEN", "reason": "x"}])
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_VALUE_INVALID")
        self.assertEqual(payload["error"]["path"], "gaps/0/kind")

    def test_control_types(self):
        document = self._minimal(control={"max_window": 0})
        status, payload = run_cli(["simulate", self.write_case(document)])
        self.assertEqual(payload["error"]["code"], "E_VALUE_OUT_OF_RANGE")
        self.assertEqual(payload["error"]["path"], "control/max_window")

    def test_deterministic_output(self):
        document = case(
            "det", [{"src": "a", "gen": 1, "start_seq": 1}],
            [rule("r", 0, [("x", True)])],
            [batch("b", 0, [event("g", "a", 1, 1, "x", "m", "m", 1)])])
        path = self.write_case(document)
        first = run_cli(["simulate", path])[1]
        second = run_cli(["simulate", path])[1]
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))


if __name__ == "__main__":
    unittest.main()
