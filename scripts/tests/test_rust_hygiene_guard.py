# Copyright 2023 The RocketMQ Rust Authors
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

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "rust_hygiene_guard.py"


def load_guard():
    sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("rust_hygiene_guard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


class RustHygieneGuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = load_guard()

    def test_rejects_unsafe_without_safety_comment(self):
        safety, _ = self.guard.scan_source(
            "fn read(pointer: *const u8) { unsafe { pointer.read(); } }",
            "crate/src/lib.rs",
        )

        self.assertEqual(1, len(safety))

    def test_accepts_adjacent_safety_comment(self):
        safety, _ = self.guard.scan_source(
            """
fn read(pointer: *const u8) {
    // SAFETY: the caller validated that pointer addresses an initialized byte.
    unsafe { pointer.read(); }
}
""",
            "crate/src/lib.rs",
        )

        self.assertEqual([], safety)

    def test_accepts_multiline_safety_comment(self):
        safety, _ = self.guard.scan_source(
            """
fn read(pointer: *const u8) {
    // SAFETY: the caller validated the pointer.
    // The initialized allocation remains alive for the read.
    unsafe { pointer.read(); }
}
""",
            "crate/src/lib.rs",
        )

        self.assertEqual([], safety)

    def test_rejects_safe_public_raw_pointer_contract(self):
        safety, _ = self.guard.scan_source(
            """
unsafe extern "C" {
    fn external_write(pointer: *mut u8, len: usize);
}

pub fn write(pointer: *mut u8, len: usize) {
    // SAFETY: the caller promises that the pointer and capacity are valid.
    unsafe { external_write(pointer, len) }
}
""",
            "crate/src/lib.rs",
        )

        self.assertTrue(any("safe public function write" in finding.reason for finding in safety))

    def test_rejects_safe_public_extern_raw_pointer_contract(self):
        safety, _ = self.guard.scan_source(
            'pub extern "C" fn write(pointer: *mut u8, len: usize) {}',
            "crate/src/lib.rs",
        )

        self.assertTrue(any("safe public function write" in finding.reason for finding in safety))

    def test_rejects_safe_public_trait_method_raw_pointer_contract(self):
        safety, _ = self.guard.scan_source(
            "pub trait Writer { fn write(pointer: *mut u8, len: usize); }",
            "crate/src/lib.rs",
        )

        self.assertTrue(any("safe public function write" in finding.reason for finding in safety))

    def test_rejects_safe_public_raw_pointer_under_mixed_test_cfg(self):
        safety, _ = self.guard.scan_source(
            "#[cfg(any(test, unix))]\npub fn write(pointer: *mut u8) {}",
            "crate/src/lib.rs",
        )

        self.assertTrue(any("safe public function write" in finding.reason for finding in safety))

    def test_explicit_test_support_probe_is_test_only(self):
        safety, debt = self.guard.scan_source(
            '#[cfg(any(test, feature = "test-support"))]\npub fn write(pointer: *mut u8) { panic!(); }',
            "crate/src/lib.rs",
        )

        self.assertEqual(([], []), (safety, debt))

    def test_rejects_safe_public_raw_pointer_after_nested_generics(self):
        safety, _ = self.guard.scan_source(
            "pub fn write<T: Fn() -> Option<Vec<u8>>>(pointer: *mut u8, value: T) {}",
            "crate/src/lib.rs",
        )

        self.assertTrue(any("safe public function write" in finding.reason for finding in safety))

    def test_accepts_safe_slice_wrapper_and_explicit_unsafe_contract(self):
        safety, _ = self.guard.scan_source(
            """
pub fn read(bytes: &[u8]) -> Option<u8> {
    bytes.first().copied()
}

/// # Safety
/// `pointer` must address an initialized byte.
pub unsafe fn read_raw(pointer: *const u8) -> u8 {
    // SAFETY: upheld by this function's caller contract.
    unsafe { pointer.read() }
}
""",
            "crate/src/lib.rs",
        )

        self.assertEqual([], safety)

    def test_inventory_is_item_scoped_and_stable_across_line_shifts(self):
        first = "fn decode(value: Option<u8>) { value.unwrap(); }\n"
        shifted = "\n\nfn decode(value: Option<u8>) { value.unwrap(); }\n"

        _, first_debt = self.guard.scan_source(first, "crate/src/lib.rs")
        _, shifted_debt = self.guard.scan_source(shifted, "crate/src/lib.rs")

        self.assertEqual(first_debt[0]["identity"], shifted_debt[0]["identity"])

    def test_structural_debt_identity_contains_no_content_fingerprint(self):
        _, debt = self.guard.scan_source(
            "fn decode(value: Option<u8>) { value.unwrap(); }\n",
            "rocketmq-client/src/decode.rs",
        )

        self.assertEqual("rocketmq-client/src/decode.rs:panic_surface:decode:0", debt[0]["identity"])
        self.assertNotIn("fingerprint", debt[0])

    def test_core_tree_scan_uses_the_phase_zero_package_scope(self):
        safety, debt = self.guard.scan_tree(ROOT, scope="core-release")

        self.assertEqual([], safety)
        self.assertTrue(debt)
        self.assertFalse(any(entry["path"].startswith("rocketmq-ai/rocketmq-sre/") for entry in debt))
        self.assertFalse(any("rocketmq-mcp" in entry["path"] for entry in debt))
        self.assertFalse(any(entry["path"].startswith("rocketmq-dashboard/") for entry in debt))
        self.assertEqual(527, sum(entry["kind"] == "panic_surface" for entry in debt))
        self.assertEqual(0, sum(entry["kind"].startswith("unsafe_") for entry in debt))
        self.assertEqual(12, sum(entry["kind"] == "legacy_mod_rs" for entry in debt))

    def test_ignores_test_module_debt_and_unsafe(self):
        source = """
#[cfg(test)]
mod tests {
    pub fn write(pointer: *mut u8) {}

    fn probe(value: Option<u8>) {
        unsafe { std::ptr::read(&0) };
        value.unwrap();
    }
}
"""

        self.assertEqual(([], []), self.guard.scan_source(source, "crate/src/lib.rs"))

    def test_ignores_cfg_test_items_and_test_named_sources(self):
        source = """
#[cfg(test)]
fn test_runtime() {
    Option::<u8>::None.unwrap();
}
"""

        self.assertEqual(([], []), self.guard.scan_source(source, "crate/src/runtime.rs"))
        self.assertEqual(
            ([], []),
            self.guard.scan_source("fn probe() { panic!(); }", "crate/src/behavior_tests.rs"),
        )
        self.assertEqual(
            ([], []),
            self.guard.scan_source("fn probe() { panic!(); }", "crate/src/component/tests.rs"),
        )

    def test_scan_tree_excludes_external_modules_reachable_only_from_test_cfg(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate" / "src"
            qualification = source / "qualification"
            fixtures = source / "fixtures"
            qualification.mkdir(parents=True)
            fixtures.mkdir()
            (source / "lib.rs").write_text(
                (
                    "mod runtime;\n"
                    "#[cfg(test)]\nmod qualification;\n"
                    "#[cfg(test)]\n#[path = \"fixtures/custom.rs\"]\nmod custom;\n"
                ),
                encoding="utf-8",
            )
            (source / "runtime.rs").write_text("fn run() { panic!(); }\n", encoding="utf-8")
            (source / "qualification.rs").write_text(
                "mod fixture;\nfn qualify() { panic!(); }\n",
                encoding="utf-8",
            )
            (qualification / "fixture.rs").write_text("fn fixture() { panic!(); }\n", encoding="utf-8")
            (fixtures / "custom.rs").write_text("fn custom() { panic!(); }\n", encoding="utf-8")

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual([], safety)
        self.assertEqual(["crate/src/runtime.rs"], [entry["path"] for entry in debt])

    def test_scan_tree_follows_test_only_module_named_build(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate" / "src"
            fixtures = source / "fixtures"
            cases = fixtures / "cases"
            cases.mkdir(parents=True)
            (source / "lib.rs").write_text(
                "#[cfg(test)]\nmod fixtures;\n",
                encoding="utf-8",
            )
            (source / "fixtures.rs").write_text("mod build;\n", encoding="utf-8")
            (fixtures / "build.rs").write_text(
                '#[path = "cases/sample.rs"]\nmod sample;\n',
                encoding="utf-8",
            )
            (cases / "sample.rs").write_text("fn sample() { panic!(); }\n", encoding="utf-8")

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual([], safety)
        self.assertEqual([], debt)

    def test_scan_tree_keeps_modules_that_can_compile_without_test_cfg(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate" / "src"
            source.mkdir(parents=True)
            (source / "lib.rs").write_text(
                "#[cfg(any(test, unix))]\nmod platform;\n",
                encoding="utf-8",
            )
            (source / "platform.rs").write_text("fn run() { panic!(); }\n", encoding="utf-8")

            _, debt = self.guard.scan_tree(root)

        self.assertEqual(["crate/src/platform.rs"], [entry["path"] for entry in debt])

    def test_detects_manual_pin_projection(self):
        _, debt = self.guard.scan_source(
            "fn poll(value: Pin<&mut Value>) { value.get_unchecked_mut(); }\n",
            "crate/src/lib.rs",
        )

        self.assertEqual("manual_pin", debt[0]["kind"])
        self.assertEqual("unsafe_invariant", debt[0]["classification"])
        self.assertEqual("2.0.0", debt[0]["expiry"])

    def test_protocol_unsafe_ledger_uses_owner_ordinals_without_fingerprints(self):
        source = """
// SAFETY: the marker trait contract is upheld by A.
unsafe impl Send for A {}
unsafe fn raw() {}
unsafe trait Raw {}
unsafe extern "C" { fn call(); }
fn decode() {
    // SAFETY: the fixture pointer is never dereferenced.
    unsafe { core::ptr::null::<u8>().read(); }
}
"""
        safety, debt = self.guard.scan_source(source, "rocketmq-protocol/src/lib.rs")

        self.assertEqual([], safety)
        self.assertEqual(
            {"unsafe_block", "unsafe_fn", "unsafe_impl", "unsafe_trait", "unsafe_extern"},
            {entry["kind"] for entry in debt},
        )
        self.assertTrue(all("sha256" not in entry and "fingerprint" not in entry for entry in debt))
        self.assertTrue(all(str(entry["identity"]).endswith(str(entry["ordinal"])) for entry in debt))

    def test_protocol_unsafe_identity_is_stable_across_line_shifts(self):
        source = "fn decode() {\n    // SAFETY: fixture invariant.\n    unsafe { call(); }\n}\n"
        _, first = self.guard.scan_source(source, "rocketmq-protocol/src/lib.rs")
        _, shifted = self.guard.scan_source("\n\n" + source, "rocketmq-protocol/src/lib.rs")

        self.assertEqual(first[0]["identity"], shifted[0]["identity"])

    def test_runtime_rule_allows_only_the_definition_and_root_reexport(self):
        canonical = "pub enum RocketMQRuntime { A } impl RocketMQRuntime { fn get() { let _ = RocketMQRuntime::A; } }"
        reexport = "pub use legacy::RocketMQRuntime;"

        self.assertEqual(([], []), self.guard.scan_source(canonical, "rocketmq-runtime/src/legacy.rs"))
        self.assertEqual(([], []), self.guard.scan_source(reexport, "rocketmq-runtime/src/lib.rs"))
        safety, _ = self.guard.scan_source(
            "use rocketmq_runtime::RocketMQRuntime;", "crate/src/lib.rs"
        )
        self.assertEqual(1, len(safety))

    def test_typed_filter_classifier_matches_only_frozen_definitions(self):
        compile_message = (
            "use of deprecated method `rocketmq_filter::filter::Filter::compile`: "
            "use Filter::try_compile and FilterCompileError"
        )
        error_message = (
            "use of deprecated associated function `filter::filter_spi::FilterError::new`: "
            "use Filter::try_compile and FilterCompileError"
        )
        error_projection = (
            "use of deprecated method `rocketmq_filter::filter::FilterError::message`: "
            "use Filter::try_compile and FilterCompileError"
        )
        unrelated_compile = "use of deprecated method `crate::Compiler::compile`: another migration"
        unrelated_error = "use of deprecated struct `rocketmq_error::FilterError`: another migration"

        self.assertEqual("legacy_filter_compile", self.guard.filter_deprecation_kind(compile_message))
        self.assertEqual("local_filter_error", self.guard.filter_deprecation_kind(error_message))
        self.assertEqual("local_filter_error", self.guard.filter_deprecation_kind(error_projection))
        self.assertIsNone(self.guard.filter_deprecation_kind(unrelated_compile))
        self.assertIsNone(self.guard.filter_deprecation_kind(unrelated_error))

    def test_typed_filter_classifier_fails_closed_for_drift_without_matching_other_crates(self):
        with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "note drifted"):
            self.guard.filter_deprecation_kind(
                "use of deprecated method `rocketmq_filter::filter::Filter::compile`: different note"
            )
        self.assertIsNone(
            self.guard.filter_deprecation_kind(
                "use of deprecated method `consumer::filter::Filter::compile`: "
                "use Filter::try_compile and FilterCompileError"
            )
        )

    def test_typed_filter_anchor_validation_binds_attributes_to_the_frozen_items(self):
        note = self.guard.FILTER_DEPRECATION_NOTE

        def write_fixture(root, compile_item):
            target = root / "rocketmq-filter" / "src" / "filter"
            target.mkdir(parents=True, exist_ok=True)
            (target / "filter_spi.rs").write_text(
                f'''#[deprecated(since = "1.0.0", note = "{note}")]
#[derive(Debug, Clone)]
pub struct FilterError;

pub trait Filter {{
{compile_item}
}}
''',
                encoding="utf-8",
            )

        valid_item = f'''    #[deprecated(since = "1.0.0", note = "{note}")]
    #[allow(
        deprecated,
        reason = "fixture",
    )]
    fn compile(&self, expr: &str);
'''
        moved_item = f'''    #[deprecated(since = "1.0.0", note = "{note}")]
    #[allow(
        deprecated,
        reason = "fixture",
    )]
    fn unrelated(&self);
    fn compile(&self, expr: &str);
'''
        deleted_item = "    fn compile(&self, expr: &str);\n"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root, valid_item)
            self.guard.validate_filter_deprecation_anchors(root)
            write_fixture(root, moved_item)
            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "anchors"):
                self.guard.validate_filter_deprecation_anchors(root)
            write_fixture(root, deleted_item)
            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "anchors"):
                self.guard.validate_filter_deprecation_anchors(root)

    def test_typed_filter_anchor_validation_masks_trait_braces_in_comments_and_strings(self):
        note = self.guard.FILTER_DEPRECATION_NOTE

        def write_fixture(root, trait_item, unrelated_item=""):
            target = root / "rocketmq-filter" / "src" / "filter"
            target.mkdir(parents=True, exist_ok=True)
            (target / "filter_spi.rs").write_text(
                f'''#[deprecated(since = "1.0.0", note = "{note}")]
#[derive(Debug, Clone)]
pub struct FilterError;

pub trait Filter {{
    // {{
    /* {{ */
    const QUOTED: &str = "{{";
    const RAW: &str = r#" }} "#;
{trait_item}
}}

impl Unrelated {{
{unrelated_item}
}}
// }}
/* }} */
const OUTSIDE_QUOTED: &str = "}}";
const OUTSIDE_RAW: &str = r#" {{ "#;
''',
                encoding="utf-8",
            )

        anchor = f'''    #[deprecated(since = "1.0.0", note = "{note}")]
    #[allow(
        deprecated,
        reason = "fixture",
    )]
    fn compile(&self, expr: &str);
'''
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_fixture(root, anchor)
            self.guard.validate_filter_deprecation_anchors(root)
            write_fixture(root, "    fn compile(&self, expr: &str);\n", anchor)
            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "anchors"):
                self.guard.validate_filter_deprecation_anchors(root)

    def test_typed_filter_baseline_rejects_forged_identity_and_metadata(self):
        entry = {
            "identity": "rocketmq-filter/src/filter.rs:local_filter_error:<module>:0",
            "path": "rocketmq-filter/src/filter.rs",
            "kind": "local_filter_error",
            "item": "<module>",
            "line": 87,
            "classification": "legacy_filter_compatibility",
            "owner": "rocketmq-filter maintainers",
            "ordinal": 0,
            "reachability": "production-compatibility-owner",
            "justification": "compiler-resolved legacy Filter compatibility use retained only in a canonical owner",
            "expiry": "2.0.0",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "baseline.json"
            for key, value, expected in (
                ("identity", "rocketmq-broker/src/lib.rs:local_filter_error:<module>:0", "identity"),
                (
                    "identity",
                    "rocketmq-filter/src/filter.rs:legacy_filter_compile:evil:0",
                    "disguised typed",
                ),
                ("owner", "rocketmq-broker maintainers", "invalid typed"),
            ):
                fixture = dict(entry)
                fixture[key] = value
                path.write_text(
                    json.dumps(
                        {
                            "schema_version": 4,
                            "policy": self.guard.BASELINE_POLICY,
                            "entries": [fixture],
                        }
                    ),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ValueError, expected):
                    self.guard.load_baseline(path, scope="core-release", root=ROOT)

    def test_typed_filter_builtin_cfg_requires_test_and_never_feature_only(self):
        self.assertTrue(self.guard.cfg_requires_builtin_test("test"))
        self.assertTrue(
            self.guard.cfg_requires_builtin_test('all(test, feature = "test-support")')
        )
        self.assertTrue(
            self.guard.cfg_requires_builtin_test('any(test, all(test, feature = "test-support"))')
        )
        self.assertFalse(self.guard.cfg_requires_builtin_test('feature = "test-support"'))
        self.assertFalse(
            self.guard.cfg_requires_builtin_test('any(test, feature = "test-support")')
        )

    def test_typed_filter_metadata_reverse_closure_is_dynamic(self):
        metadata = {
            "workspace_members": ["filter", "broker", "proxy-local", "proxy", "unrelated"],
            "packages": [
                {"id": "filter", "name": "rocketmq-filter"},
                {"id": "broker", "name": "rocketmq-broker"},
                {"id": "proxy-local", "name": "rocketmq-proxy-local"},
                {"id": "proxy", "name": "rocketmq-proxy"},
                {"id": "unrelated", "name": "unrelated"},
            ],
            "resolve": {
                "nodes": [
                    {"id": "filter", "deps": []},
                    {"id": "broker", "deps": [{"pkg": "filter"}]},
                    {"id": "proxy-local", "deps": [{"pkg": "filter"}]},
                    {"id": "proxy", "deps": [{"pkg": "proxy-local"}]},
                    {"id": "unrelated", "deps": []},
                ]
            },
        }

        self.assertEqual(
            ["rocketmq-broker", "rocketmq-filter", "rocketmq-proxy", "rocketmq-proxy-local"],
            self.guard.filter_reverse_dependency_packages(metadata),
        )
        with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "resolve graph"):
            self.guard.filter_reverse_dependency_packages({"packages": [], "workspace_members": []})

    def test_typed_filter_diagnostic_fails_closed_for_missing_primary_or_target(self):
        message = (
            "use of deprecated method `rocketmq_filter::filter::Filter::compile`: "
            "use Filter::try_compile and FilterCompileError"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "consumer" / "src" / "lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text("fn legacy() {}\n", encoding="utf-8")
            record = {
                "reason": "compiler-message",
                "target": {"kind": ["lib"]},
                "message": {"code": {"code": "deprecated"}, "message": message, "spans": []},
            }
            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "unique primary"):
                self.guard.typed_filter_diagnostic(record, root)
            record["message"]["spans"] = [
                {
                    "is_primary": True,
                    "file_name": str(source),
                    "byte_start": 0,
                    "line_start": 1,
                }
            ]
            record.pop("target")
            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "target kind"):
                self.guard.typed_filter_diagnostic(record, root)

    def test_typed_filter_diagnostic_exempts_only_test_targets_not_benches_or_examples(self):
        message = (
            "use of deprecated method `rocketmq_filter::filter::Filter::compile`: "
            "use Filter::try_compile and FilterCompileError"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "consumer" / "src" / "lib.rs"
            source.parent.mkdir(parents=True)
            source.write_text("fn legacy() {}\n", encoding="utf-8")

            def record(kind):
                return {
                    "reason": "compiler-message",
                    "target": {"kind": [kind]},
                    "message": {
                        "code": {"code": "deprecated"},
                        "message": message,
                        "spans": [
                            {
                                "is_primary": True,
                                "file_name": str(source),
                                "byte_start": 0,
                                "line_start": 1,
                            }
                        ],
                    },
                }

            self.assertIsNotNone(self.guard.typed_filter_diagnostic(record("bench"), root))
            self.assertIsNotNone(self.guard.typed_filter_diagnostic(record("example"), root))
            self.assertIsNone(self.guard.typed_filter_diagnostic(record("test"), root))

    def test_typed_filter_primary_span_requires_a_utf8_byte_boundary(self):
        message = (
            "use of deprecated method `rocketmq_filter::filter::Filter::compile`: "
            "use Filter::try_compile and FilterCompileError"
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "consumer" / "src" / "lib.rs"
            source.parent.mkdir(parents=True)
            contents = 'fn legacy() { let label = "秘密"; }\n'
            source.write_text(contents, encoding="utf-8")
            boundary = len(contents[:contents.index("秘")].encode("utf-8"))

            def record(byte_start):
                return {
                    "reason": "compiler-message",
                    "target": {"kind": ["lib"]},
                    "message": {
                        "code": {"code": "deprecated"},
                        "message": message,
                        "spans": [
                            {
                                "is_primary": True,
                                "file_name": str(source),
                                "byte_start": byte_start,
                                "line_start": 1,
                            }
                        ],
                    },
                }

            with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "not on a UTF-8 boundary"):
                self.guard.typed_filter_diagnostic(record(boundary + 1), root)
            finding = self.guard.typed_filter_diagnostic(record(boundary), root)

        self.assertEqual(contents.index("秘"), finding.offset)
        self.assertEqual(1, finding.line)

    def test_typed_filter_clippy_stream_fails_closed_for_malformed_json_and_nonzero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            malformed = self.guard.subprocess.CompletedProcess(
                ["cargo"], 0, stdout="{bad\n", stderr="fixture stderr"
            )
            with mock.patch.object(self.guard.subprocess, "run", return_value=malformed):
                with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "malformed JSON"):
                    self.guard.run_typed_filter_clippy(root, ["probe"], all_features=False)
            nonzero = self.guard.subprocess.CompletedProcess(
                ["cargo"], 7, stdout="", stderr="fixture stderr"
            )
            with mock.patch.object(self.guard.subprocess, "run", return_value=nonzero):
                with self.assertRaisesRegex(self.guard.TypedFilterGuardError, "exit 7"):
                    self.guard.run_typed_filter_clippy(root, ["probe"], all_features=True)

    def test_typed_filter_compiler_guard_covers_renames_aliases_and_test_boundaries(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            filter_crate = root / "rocketmq-filter"
            consumer = root / "consumer"
            (filter_crate / "src" / "filter").mkdir(parents=True)
            (consumer / "src").mkdir(parents=True)
            (consumer / "tests").mkdir()
            (root / "Cargo.toml").write_text(
                '[workspace]\nmembers = ["rocketmq-filter", "consumer"]\nresolver = "2"\n',
                encoding="utf-8",
            )
            (filter_crate / "Cargo.toml").write_text(
                '[package]\nname = "rocketmq-filter"\nversion = "0.1.0"\nedition = "2021"\n',
                encoding="utf-8",
            )
            (filter_crate / "src" / "lib.rs").write_text("pub mod filter;\n", encoding="utf-8")
            (filter_crate / "src" / "filter.rs").write_text(
                "pub mod filter_spi;\npub use filter_spi::{Filter, FilterError};\n",
                encoding="utf-8",
            )
            (filter_crate / "src" / "filter" / "filter_spi.rs").write_text(
                """#[deprecated(since = \"1.0.0\", note = \"use Filter::try_compile and FilterCompileError\")]
#[derive(Debug, Clone)]
pub struct FilterError { message: String }
impl FilterError {
    pub fn new(message: impl Into<String>) -> Self { Self { message: message.into() } }
    pub fn message(&self) -> &str { &self.message }
}
pub trait Filter {
    #[deprecated(since = \"1.0.0\", note = \"use Filter::try_compile and FilterCompileError\")]
    #[allow(
        deprecated,
        reason = \"fixture keeps the frozen compatibility signature\",
    )]
    fn compile(&self, expression: &str) -> Result<(), FilterError>;
}
""",
                encoding="utf-8",
            )
            (consumer / "Cargo.toml").write_text(
                """[package]
name = "consumer"
version = "0.1.0"
edition = "2021"

[features]
test-support = []

[dependencies]
legacy_filter = { package = "rocketmq-filter", path = "../rocketmq-filter" }
""",
                encoding="utf-8",
            )
            (consumer / "src" / "lib.rs").write_text(
                """use legacy_filter::filter::*;
use legacy_filter::filter as module_alias;

#[derive(Debug)]
pub struct External;

#[allow(deprecated)]
impl Filter for External {
    fn compile(&self, _: &str) -> Result<(), FilterError> { Ok(()) }
}

fn make() -> External { External }

pub fn glob_legacy() { let _ = make().compile("glob"); }

mod nested {
    use legacy_filter::filter::{Filter as NestedFilter, FilterError as NestedError};
    use super::External;
    pub fn nested_legacy() {
        let value = External;
        let _ = value.compile("nested");
        let _ = NestedError::new("nested");
    }
}

pub fn module_alias_legacy() {
    let value: &dyn module_alias::Filter = &External;
    let _ = value.compile("alias");
}

pub fn dependency_rename_legacy() {
    let value: &dyn legacy_filter::filter::Filter = &External;
    let _ = value.compile("rename");
}

pub fn generic_receiver_legacy<T: Filter>(value: &T) { let _ = value.compile("generic"); }
pub fn factory_inferred_name_legacy() { let pigeon = make(); let _ = pigeon.compile("factory"); }
pub struct FilterFactory;
impl FilterFactory { pub fn get(&self) -> External { External } }
pub fn factory_object_arbitrary_name_legacy() {
    let factory = FilterFactory;
    let marmot = factory.get();
    let _ = marmot.compile("factory-object");
}
pub fn chained_receiver_legacy() { let _ = make().compile("chain"); }
pub fn fully_qualified_legacy() {
    let value = make();
    let _ = legacy_filter::filter::Filter::compile(&value, "qualified");
}
type LegacyAlias = Box<dyn Filter>;
pub struct Holder { pub value: LegacyAlias }
pub fn field_and_type_alias_legacy(holder: &Holder) { let _ = holder.value.compile("field"); }

#[allow(deprecated)]
pub fn allow_is_still_reported() { let _ = make().compile("allow"); }

#[expect(deprecated)]
pub fn expect_is_still_reported() { let _ = make().compile("expect"); }

#[cfg(feature = "test-support")]
pub fn feature_test_support_is_production() { let _ = make().compile("feature"); }

#[cfg(any(test, feature = "test-support"))]
pub fn mixed_cfg_is_production() { let _ = make().compile("mixed"); }

#[cfg(test)]
fn builtin_test_is_exempt() { let _ = make().compile("builtin"); }

pub struct Compiler;
impl Compiler { pub fn compile(&self) {} }
pub fn unrelated_compilers(filter: Compiler, image_filter: Compiler) {
    filter.compile();
    image_filter.compile();
    let _ = core::mem::size_of::<crate::rocketmq_error::FilterError>();
}
pub mod rocketmq_error { pub struct FilterError; }
""",
                encoding="utf-8",
            )
            (consumer / "tests" / "integration.rs").write_text(
                """use legacy_filter::filter::Filter;

#[derive(Debug)]
struct External;
impl Filter for External {
    fn compile(&self, _: &str) -> Result<(), legacy_filter::filter::FilterError> { Ok(()) }
}

#[test]
fn integration_test_is_exempt() { let _ = External.compile("integration"); }
""",
                encoding="utf-8",
            )

            generated = self.guard.subprocess.run(
                ["cargo", "generate-lockfile"], cwd=root, capture_output=True, text=True, check=False
            )
            self.assertEqual(0, generated.returncode, generated.stderr)
            entries = self.guard.scan_typed_filter_deprecations(root)

        identities = {entry["identity"] for entry in entries}
        self.assertTrue(identities)
        self.assertTrue(any(identity.startswith("rocketmq-filter/src/filter/") for identity in identities))
        self.assertTrue(any("glob_legacy" in identity for identity in identities))
        self.assertTrue(any("nested_legacy" in identity for identity in identities))
        self.assertTrue(any("module_alias_legacy" in identity for identity in identities))
        self.assertTrue(any("dependency_rename_legacy" in identity for identity in identities))
        self.assertTrue(any("generic_receiver_legacy" in identity for identity in identities))
        self.assertTrue(any("factory_inferred_name_legacy" in identity for identity in identities))
        self.assertTrue(any("factory_object_arbitrary_name_legacy" in identity for identity in identities))
        self.assertTrue(any("chained_receiver_legacy" in identity for identity in identities))
        self.assertTrue(any("fully_qualified_legacy" in identity for identity in identities))
        self.assertTrue(any("field_and_type_alias_legacy" in identity for identity in identities))
        self.assertTrue(any("allow_is_still_reported" in identity for identity in identities))
        self.assertTrue(any("expect_is_still_reported" in identity for identity in identities))
        self.assertTrue(any("feature_test_support_is_production" in identity for identity in identities))
        self.assertTrue(any("mixed_cfg_is_production" in identity for identity in identities))
        self.assertFalse(any("builtin_test_is_exempt" in identity for identity in identities))
        self.assertFalse(any("integration_test_is_exempt" in identity for identity in identities))
        self.assertFalse(any("unrelated_compilers" in identity for identity in identities))

    def test_panic_aliases_are_counted_but_panic_module_members_are_not(self):
        source = """
use std::panic::AssertUnwindSafe;
use std::{panic as boom, unreachable as nope};
pub use self::boom as crash;
fn fail() { boom!(); crash!(); nope!(); }
"""
        _, debt = self.guard.scan_source(source, "crate/src/lib.rs")

        self.assertEqual(5, sum(entry["kind"] == "panic_surface" for entry in debt))

    def test_cargo_custom_library_is_scanned_and_src_orphan_is_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "rocketmq-protocol"
            (crate / "src").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\n[lib]\npath="production.rs"\n',
                encoding="utf-8",
            )
            (crate / "production.rs").write_text(
                (
                    "fn live() {\n"
                    "    panic!();\n"
                    "    let _ = RocketMQRuntime::new();\n"
                    "    // SAFETY: the empty fixture block has no unsafe operation.\n"
                    "    unsafe {}\n"
                    "}\n"
                ),
                encoding="utf-8",
            )
            (crate / "src/orphan_fixture.rs").write_text(
                "fn orphan() { panic!(); let _ = RocketMQRuntime::new(); }\n", encoding="utf-8"
            )

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual(1, len(safety))
        self.assertEqual(
            ["rocketmq-protocol/production.rs", "rocketmq-protocol/production.rs"],
            [entry["path"] for entry in debt],
        )

    def test_cargo_root_named_like_a_test_remains_production(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "rocketmq-protocol"
            (crate / "src").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\n[lib]\npath="src/behavior_tests.rs"\n',
                encoding="utf-8",
            )
            (crate / "src/behavior_tests.rs").write_text(
                (
                    "fn live() {\n"
                    "    panic!();\n"
                    "    let _ = RocketMQRuntime::new();\n"
                    "    // SAFETY: the empty fixture block has no unsafe operation.\n"
                    "    unsafe {}\n"
                    "}\n"
                ),
                encoding="utf-8",
            )

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual(1, len(safety))
        self.assertEqual(
            ["panic_surface", "unsafe_block"],
            sorted(entry["kind"] for entry in debt),
        )

    def test_cargo_bins_build_and_autobins_false_are_classified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crate"
            (crate / "src").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                (
                    '[package]\nname="probe"\nversion="0.1.0"\n'
                    'autolib=false\nautobins=false\nbuild="build.rs"\n'
                    '[[bin]]\nname="app"\npath="app.rs"\n'
                ),
                encoding="utf-8",
            )
            (crate / "app.rs").write_text("fn main() { panic!(); }\n", encoding="utf-8")
            (crate / "build.rs").write_text("fn main() { panic!(); }\n", encoding="utf-8")
            (crate / "src/main.rs").write_text("fn main() { panic!(); }\n", encoding="utf-8")

            _, debt = self.guard.scan_tree(root)

        self.assertEqual(["crate/app.rs", "crate/build.rs"], sorted(entry["path"] for entry in debt))

    def test_ambiguous_cargo_binary_targets_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crate"
            (crate / "src/bin/other").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                (
                    '[package]\nname="probe"\nversion="0.1.0"\nautolib=false\n'
                    '[[bin]]\nname="other"\n'
                ),
                encoding="utf-8",
            )
            (crate / "src/bin/other.rs").write_text("fn main() {}\n", encoding="utf-8")
            (crate / "src/bin/other/main.rs").write_text("fn main() {}\n", encoding="utf-8")

            safety, _ = self.guard.scan_tree(root)

        self.assertTrue(any("ambiguous Cargo binary target other" in finding.reason for finding in safety))

    def test_package_named_binary_prefers_src_main(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crate"
            (crate / "src/bin/probe").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                (
                    '[package]\nname="probe"\nversion="0.1.0"\nautolib=false\nautobins=false\n'
                    '[[bin]]\nname="probe"\n'
                ),
                encoding="utf-8",
            )
            (crate / "src/main.rs").write_text("fn main() { panic!(); }\n", encoding="utf-8")
            (crate / "src/bin/probe/main.rs").write_text("fn main() {}\n", encoding="utf-8")

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual([], safety)
        self.assertEqual(["crate/src/main.rs"], [entry["path"] for entry in debt])

    def test_duplicate_automatic_binary_names_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crate = root / "crate"
            (crate / "src/bin/foo").mkdir(parents=True)
            (crate / "Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\nautolib=false\n',
                encoding="utf-8",
            )
            (crate / "src/bin/foo.rs").write_text("fn main() {}\n", encoding="utf-8")
            (crate / "src/bin/foo/main.rs").write_text("fn main() {}\n", encoding="utf-8")

            safety, _ = self.guard.scan_tree(root)

        self.assertTrue(any("duplicate automatic binary target foo" in finding.reason for finding in safety))

    def test_literal_modules_paths_and_cfg_test_edges_are_classified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate/src"
            source.mkdir(parents=True)
            (root / "crate/Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\n', encoding="utf-8"
            )
            (source / "lib.rs").write_text(
                'mod live;\n#[path="custom.rs"] mod custom;\n#[cfg(test)] mod only_test;\n',
                encoding="utf-8",
            )
            (source / "live.rs").write_text("fn live() { panic!(); }\n", encoding="utf-8")
            (source / "custom.rs").write_text("fn custom() { panic!(); }\n", encoding="utf-8")
            (source / "only_test.rs").write_text(
                "fn test() { panic!(); let _ = RocketMQRuntime::new(); }\n", encoding="utf-8"
            )

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual([], safety)
        self.assertEqual(
            ["crate/src/custom.rs", "crate/src/live.rs"], sorted(entry["path"] for entry in debt)
        )

    def test_unresolved_production_module_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate/src"
            source.mkdir(parents=True)
            (root / "crate/Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\n', encoding="utf-8"
            )
            (source / "lib.rs").write_text("mod missing;\n", encoding="utf-8")

            safety, debt = self.guard.scan_tree(root)

        self.assertEqual([], debt)
        self.assertTrue(any("missing production module missing" in finding.reason for finding in safety))

    def test_configured_module_path_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "crate/src"
            source.mkdir(parents=True)
            (root / "crate/Cargo.toml").write_text(
                '[package]\nname="probe"\nversion="0.1.0"\n', encoding="utf-8"
            )
            (source / "lib.rs").write_text(
                '#[cfg_attr(unix, path="unix.rs")] mod platform;\n', encoding="utf-8"
            )
            (source / "unix.rs").write_text("fn live() {}\n", encoding="utf-8")

            safety, _ = self.guard.scan_tree(root)

        self.assertTrue(
            any("unsupported configured path for module platform" in finding.reason for finding in safety)
        )


if __name__ == "__main__":
    unittest.main()
