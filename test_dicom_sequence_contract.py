import ast
import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).parent


def load_contract_functions(path, names):
    source = path.read_text()
    tree = ast.parse(source)
    selected = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            selected.append(node)
        elif isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id in names
            for target in node.targets
        ):
            selected.append(node)
    namespace = {"json": json, "re": re}
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


class DicomSequenceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.manager = load_contract_functions(
            ROOT / "add_or_update_dicom_tag.py",
            {
                "TAG_PATH_PATTERN",
                "STRING_VALUE_VRS",
                "discover_paths_in_payload",
                "path_tag_chain",
                "extract_values_from_path",
                "analyze_payload_partition",
                "infer_value_mode",
                "count_sequence_items_json",
            },
        )
        cls.extractor = load_contract_functions(
            ROOT / "extract_imaging_metastore_extension.py",
            {
                "configured_path_tag_chain",
                "extract_sequence_items_from_path",
                "serialize_sequence_items",
            },
        )
        cls.procedure_items = [
            {
                "00080100": {"vr": "SH", "Value": ["PROC-A"]},
                "00080102": {"vr": "SH", "Value": ["99PROV"]},
                "00080104": {"vr": "LO", "Value": ["Procedure A"]},
            },
            {
                "00080100": {"vr": "SH", "Value": ["PROC-B"]},
                "00080120": {"vr": "UR", "Value": ["urn:example:procedure-b"]},
            },
        ]
        cls.requested_item = {
            "00080100": {"vr": "SH", "Value": ["REQ-A"]},
            "00080104": {"vr": "LO", "Value": ["Requested α"]},
        }
        cls.payload = json.dumps(
            {
                "00081032": {"vr": "SQ", "Value": cls.procedure_items},
                "00400275": {
                    "vr": "SQ",
                    "Value": [
                        {
                            "00321064": {
                                "vr": "SQ",
                                "Value": [cls.requested_item],
                            }
                        }
                    ],
                },
            },
            ensure_ascii=False,
        )

    def test_manager_selects_sequence_mode_and_counts_every_item(self):
        summary = next(
            self.manager["analyze_payload_partition"](
                [{"_metadata_json": self.payload}], "00081032", "SQ", 10
            )
        )
        paths = sorted(summary["pathCounts"])
        self.assertEqual("SEQUENCE_ITEMS_JSON", self.manager["infer_value_mode"](paths, "SQ", 2))
        self.assertEqual(2, summary["sequenceItems"])
        self.assertEqual(2, summary["maximumValues"])
        self.assertEqual(0, summary["invalidSequenceValues"])

    def test_extractor_preserves_complete_top_level_and_nested_items(self):
        root = json.loads(self.payload)
        procedure = self.extractor["serialize_sequence_items"](
            root, ["$.00081032.Value[*]"]
        )
        requested = self.extractor["serialize_sequence_items"](
            root, ["$.00400275.Value[*].00321064.Value[*]"]
        )
        self.assertEqual(self.procedure_items, json.loads(procedure))
        self.assertEqual([self.requested_item], json.loads(requested))
        self.assertIn("Requested α", requested)

    def test_multiple_paths_preserve_listed_order_without_deduplication(self):
        root = json.loads(self.payload)
        root["00321064"] = {
            "vr": "SQ",
            "Value": [{"00080100": {"vr": "SH", "Value": ["TOP"]}}],
        }
        stored = self.extractor["serialize_sequence_items"](
            root,
            [
                "$.00321064.Value[*]",
                "$.00400275.Value[*].00321064.Value[*]",
            ],
        )
        codes = [item["00080100"]["Value"][0] for item in json.loads(stored)]
        self.assertEqual(["TOP", "REQ-A"], codes)

    def test_non_object_sequence_items_are_rejected(self):
        invalid_payload = json.dumps(
            {"00081032": {"vr": "SQ", "Value": ["not-an-item"]}}
        )
        summary = next(
            self.manager["analyze_payload_partition"](
                [{"_metadata_json": invalid_payload}], "00081032", "SQ", 10
            )
        )
        self.assertEqual(1, summary["invalidSequenceValues"])
        with self.assertRaisesRegex(ValueError, "non-object sequence item"):
            self.extractor["serialize_sequence_items"](
                json.loads(invalid_payload), ["$.00081032.Value[*]"]
            )

    def test_non_object_target_element_blocks_preview_even_with_valid_rows(self):
        malformed_element = json.dumps({"00081032": "not-an-element"})
        summary = next(
            self.manager["analyze_payload_partition"](
                [
                    {"_metadata_json": self.payload},
                    {"_metadata_json": malformed_element},
                ],
                "00081032",
                "SQ",
                10,
            )
        )
        self.assertEqual(2, summary["sequenceItems"])
        self.assertEqual(1, summary["invalidSequenceValues"])

    def test_global_nested_path_detects_malformed_parent_value(self):
        malformed_parent = json.dumps(
            {
                "00400275": {
                    "vr": "SQ",
                    "Value": {
                        "00321064": {
                            "vr": "SQ",
                            "Value": [self.requested_item],
                        }
                    },
                }
            }
        )
        configured_path = "$.00400275.Value[*].00321064.Value[*]"
        summary = next(
            self.manager["analyze_payload_partition"](
                [
                    {"_metadata_json": self.payload},
                    {"_metadata_json": malformed_parent},
                ],
                "00321064",
                "SQ",
                10,
                [configured_path],
            )
        )
        self.assertEqual(1, summary["sequenceItems"])
        self.assertEqual(1, summary["invalidSequenceValues"])

    def test_bounded_validation_count_requires_array_of_objects(self):
        count_items = self.manager["count_sequence_items_json"]
        self.assertEqual(2, count_items(json.dumps(self.procedure_items)))
        self.assertEqual(0, count_items(None))
        for invalid in ('{"not":"an-array"}', '["not-an-object"]', "not-json"):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    count_items(invalid)


if __name__ == "__main__":
    unittest.main()
