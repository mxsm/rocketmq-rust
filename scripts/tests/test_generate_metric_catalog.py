#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

import generate_metric_catalog as catalog  # noqa: E402


class MetricCatalogGeneratorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.metric_symbols = {
            "KNOWN_METRIC": "known_metric",
            "ANOTHER_METRIC": "another_metric",
        }
        self.label_symbols = {"KNOWN_LABEL", "ANOTHER_LABEL"}

    @staticmethod
    def document(metric: dict[object, object] | None = None) -> dict[str, object]:
        return {
            "schema_version": 1,
            "metric": [
                metric
                or {
                    "index": 0,
                    "symbol": "KNOWN_METRIC",
                    "kind": "Counter",
                    "unit": "",
                    "labels": ["KNOWN_LABEL"],
                    "source": "Runtime",
                }
            ],
        }

    def validate(self, document: dict[str, object], *, count: int = 1) -> list[dict[str, object]]:
        return catalog.validate_partition(
            document,
            path=Path("fixture.toml"),
            partition="fixture",
            first_index=0,
            expected_count=count,
            metric_symbols=self.metric_symbols,
            label_symbols=self.label_symbols,
        )

    def assert_schema_error(self, document: dict[str, object], message: str, *, count: int = 1) -> None:
        with self.assertRaisesRegex(catalog.SchemaError, message):
            self.validate(document, count=count)

    def test_valid_descriptor_accepts_an_empty_unit(self) -> None:
        descriptors = self.validate(self.document())
        self.assertEqual("", descriptors[0]["unit"])
        self.assertEqual(("KNOWN_LABEL",), descriptors[0]["labels"])
        self.assertEqual("known_metric", descriptors[0]["name"])

    def test_unknown_root_key_and_schema_version_are_rejected(self) -> None:
        unknown_key = self.document()
        unknown_key["typo"] = True
        self.assert_schema_error(unknown_key, "unknown root keys")

        wrong_version = self.document()
        wrong_version["schema_version"] = 2
        self.assert_schema_error(wrong_version, "schema_version must be the integer 1")

        boolean_version = self.document()
        boolean_version["schema_version"] = True
        self.assert_schema_error(boolean_version, "schema_version must be the integer 1")

    def test_partition_size_and_contiguous_indexes_are_enforced(self) -> None:
        self.assert_schema_error(self.document(), "must contain 2 metrics", count=2)
        non_contiguous = self.document()
        non_contiguous["metric"][0]["index"] = 1
        self.assert_schema_error(non_contiguous, "index must be contiguous")

        boolean_index = self.document()
        boolean_index["metric"][0]["index"] = True
        self.assert_schema_error(boolean_index, "index must be an integer")

    def test_descriptor_shape_and_symbols_are_strict(self) -> None:
        missing_unit = self.document()
        del missing_unit["metric"][0]["unit"]
        self.assert_schema_error(missing_unit, "missing required keys")

        unknown_key = self.document()
        unknown_key["metric"][0]["unexpected"] = "value"
        self.assert_schema_error(unknown_key, "unknown keys")

        invalid_symbol = self.document()
        invalid_symbol["metric"][0]["symbol"] = "not_a_rust_identifier"
        self.assert_schema_error(invalid_symbol, "uppercase Rust identifier")

        unknown_symbol = self.document()
        unknown_symbol["metric"][0]["symbol"] = "MISSING_METRIC"
        self.assert_schema_error(unknown_symbol, "unknown semantic metric symbol")

    def test_kind_source_and_labels_are_strict(self) -> None:
        invalid_kind = self.document()
        invalid_kind["metric"][0]["kind"] = "Summary"
        self.assert_schema_error(invalid_kind, "unknown metric kind")

        non_string_kind = self.document()
        non_string_kind["metric"][0]["kind"] = []
        self.assert_schema_error(non_string_kind, "unknown metric kind")

        invalid_source = self.document()
        invalid_source["metric"][0]["source"] = "Dashboard"
        self.assert_schema_error(invalid_source, "unknown metric source")

        non_string_source = self.document()
        non_string_source["metric"][0]["source"] = []
        self.assert_schema_error(non_string_source, "unknown metric source")

        duplicate_labels = self.document()
        duplicate_labels["metric"][0]["labels"] = ["KNOWN_LABEL", "KNOWN_LABEL"]
        self.assert_schema_error(duplicate_labels, "labels must not contain duplicates")

        unknown_label = self.document()
        unknown_label["metric"][0]["labels"] = ["MISSING_LABEL"]
        self.assert_schema_error(unknown_label, "unknown semantic label symbol")

    def test_required_fields_reject_non_string_values(self) -> None:
        cases = (
            ("symbol", [], "uppercase Rust identifier"),
            ("kind", [], "unknown metric kind"),
            ("unit", [], "unit must be a string"),
            ("labels", "KNOWN_LABEL", "labels must be an array of strings"),
            ("source", [], "unknown metric source"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                invalid = self.document()
                invalid["metric"][0][field] = value
                self.assert_schema_error(invalid, message)

        invalid_description = self.document()
        invalid_description["metric"][0]["description"] = []
        self.assert_schema_error(invalid_description, "description must be a non-empty string")

    def test_units_reject_c0_control_characters(self) -> None:
        for character in map(chr, range(0x20)):
            with self.subTest(character=repr(character)):
                invalid_unit = self.document()
                invalid_unit["metric"][0]["unit"] = f"ms{character}"
                self.assert_schema_error(invalid_unit, "must not contain C0 control characters")

    def test_global_duplicate_symbols_are_rejected(self) -> None:
        duplicate = [
            {"index": index, "symbol": "KNOWN_METRIC", "name": f"metric_{index}"}
            for index in range(sum(count for _, _, count in catalog.PARTITIONS))
        ]
        with self.assertRaisesRegex(catalog.SchemaError, "duplicate semantic metric symbols"):
            catalog.validate_global_catalog(duplicate)

    def test_global_duplicate_resolved_names_are_rejected(self) -> None:
        descriptors = [
            {
                "index": index,
                "symbol": f"METRIC_{index}",
                "name": "duplicate_metric" if index < 2 else f"metric_{index}",
            }
            for index in range(sum(count for _, _, count in catalog.PARTITIONS))
        ]
        with self.assertRaisesRegex(catalog.SchemaError, "duplicate resolved metric names"):
            catalog.validate_global_catalog(descriptors)

    def test_semantic_constants_decode_rust_escapes(self) -> None:
        source = '''
pub mod metrics {
    pub const QUOTED: &str = "rocketmq_\\\"quoted\\\"\\\\metric";
}
pub mod labels {
    pub const LABEL: &str = "label";
}
'''
        self.assertEqual(
            {"QUOTED": 'rocketmq_"quoted"\\metric'},
            catalog.parse_semantic_string_constants(source, "metrics"),
        )

    def test_render_is_deterministic_and_uses_static_label_slices(self) -> None:
        descriptor = {
            "partition": "java",
            "index": 0,
            "symbol": "KNOWN_METRIC",
            "name": "known_metric",
            "kind": "Counter",
            "unit": "",
            "labels": ("KNOWN_LABEL",),
            "source": "Runtime",
        }
        rendered = catalog.render_catalog([descriptor])
        self.assertEqual(rendered, catalog.render_catalog([descriptor]))
        self.assertIn("const METRIC_LABELS_0: &[&str]", rendered)
        self.assertIn("labels: METRIC_LABELS_0", rendered)
        self.assertNotIn("HashMap", rendered)

    def test_check_mode_does_not_write_a_stale_generated_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            generated_path = Path(temporary_directory) / "generated.rs"
            generated_path.write_text("stale\n", encoding="utf-8")
            with patch.object(catalog, "GENERATED_PATH", generated_path):
                self.assertEqual(1, catalog.run(check=True))
            self.assertEqual("stale\n", generated_path.read_text(encoding="utf-8"))

    def test_check_mode_does_not_write_a_current_generated_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            generated_path = Path(temporary_directory) / "generated.rs"
            current = catalog.render_catalog(catalog.load_catalog())
            generated_path.write_text(current, encoding="utf-8")
            with patch.object(catalog, "GENERATED_PATH", generated_path):
                self.assertEqual(0, catalog.run(check=True))
            self.assertEqual(current, generated_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
