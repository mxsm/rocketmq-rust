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
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "arc_mut_guard.py"


def load_guard():
    spec = importlib.util.spec_from_file_location("arc_mut_guard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(module)
    return module


class ArcMutScannerTests(unittest.TestCase):
    def scan(self, source: str, relative: str = "crate/src/lib.rs"):
        guard = load_guard()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / relative
            path.parent.mkdir(parents=True)
            path.write_text(source, encoding="utf-8")
            return guard.scan_tree(Path(tmp))

    def kinds(self, source: str):
        return {item.kind for item in self.scan(source)}

    def test_ignores_comments_normal_and_raw_strings_and_chars(self):
        findings = self.scan(
            '// ArcMut::new(x)\n/* mut_from_ref */\nlet a="WeakArcMut";\n'
            'let b=r###"SyncUnsafeCellWrapper"###; let c=\'A\';'
        )
        self.assertEqual([], findings)

    def test_unterminated_block_comment_fails_closed(self):
        guard = load_guard()
        with self.assertRaises(guard.LexError):
            guard.tokenize("/* ArcMut")

    def test_unterminated_string_fails_closed(self):
        guard = load_guard()
        with self.assertRaises(guard.LexError):
            guard.tokenize('let x = "ArcMut')

    def test_rust_character_escapes_are_ignored(self):
        findings = self.scan(r"let a='\n'; let b='\x41'; let c='\u{1F980}'; let d='\\';")
        self.assertEqual([], findings)

    def test_unterminated_character_after_assignment_fails_closed(self):
        guard = load_guard()
        with self.assertRaises(guard.LexError):
            guard.tokenize("let c = 'x;")

    def test_lifetimes_are_not_character_literals(self):
        guard = load_guard()
        values = [token.value for token in guard.tokenize("fn f<'a>(x: &'a str) -> &'static str { x }")]
        self.assertEqual(3, values.count("'"))

    def test_type_reference(self):
        self.assertIn("type_reference", self.kinds("fn f(x: ArcMut<Value>) {}"))

    def test_constructor(self):
        self.assertIn("constructor", self.kinds("fn f(){ let x=ArcMut::new(1); }"))

    def test_import(self):
        self.assertIn("import", self.kinds("use rocketmq_rust::ArcMut;"))

    def test_pub_reexport(self):
        self.assertIn("reexport", self.kinds("pub use inner::ArcMut;"))

    def test_use_alias_fixed_point(self):
        findings = self.scan("use rocketmq_rust::ArcMut as Shared; type Shared2<T> = Shared<T>; fn f(x: Shared2<u8>){}")
        self.assertTrue(any(f.symbol == "Shared2" and f.kind == "type_reference" for f in findings))

    def test_type_alias(self):
        self.assertIn("alias", self.kinds("type Shared<T> = ArcMut<T>;"))

    def test_weak_arc_mut(self):
        self.assertIn("type_reference", self.kinds("fn f(x: WeakArcMut<Value>) {}"))

    def test_sync_unsafe_cell_wrapper(self):
        self.assertIn("constructor", self.kinds("let x=SyncUnsafeCellWrapper::new(1);"))

    def test_mut_from_ref_definition(self):
        self.assertIn("mut_from_ref_definition", self.kinds("impl X { pub fn mut_from_ref(&self)->&mut T { todo!() } }"))

    def test_mut_from_ref_call(self):
        self.assertIn("mut_from_ref_call", self.kinds("let x = shared.mut_from_ref();"))

    def test_as_mut_impl(self):
        self.assertIn("dangerous_as_mut_impl", self.kinds("impl<T> AsMut<T> for ArcMut<T> { fn as_mut(&mut self)->&mut T { todo!() } }"))

    def test_deref_mut_impl_through_alias(self):
        self.assertIn("dangerous_deref_mut_impl", self.kinds("type Shared<T>=ArcMut<T>; impl<T> DerefMut for Shared<T> { fn deref_mut(&mut self)->&mut T { todo!() } }"))

    def test_shared_unsafe_cell_wrapper(self):
        self.assertIn("shared_unsafe_cell_wrapper", self.kinds("struct Shared<T>{ inner: UnsafeCell<T> } unsafe impl<T> Sync for Shared<T> {}"))

    def test_unsafe_cell_wrapper_is_brace_aware_and_type_matched(self):
        source = (
            "struct Innocent<T>{ value:T } struct Cell<T>{ inner:UnsafeCell<T> } "
            "unsafe impl<T> Sync for Innocent<T>{}"
        )
        self.assertNotIn("shared_unsafe_cell_wrapper", self.kinds(source))

    def test_arc_unsafe_cell_safe_escape_is_shared_wrapper(self):
        source = (
            "struct Shared<T>{ inner:Arc<UnsafeCell<T>> } "
            "impl<T> Shared<T>{ fn expose(&self)->&mut T { unsafe { &mut *self.inner.get() } } }"
        )
        self.assertIn("shared_unsafe_cell_wrapper", self.kinds(source))

    def test_direct_unsafe_cell_without_escape_or_sync_is_not_shared_wrapper(self):
        self.assertNotIn("shared_unsafe_cell_wrapper", self.kinds("struct Local<T>{ inner:UnsafeCell<T> }"))

    def test_tuple_shared_cell_wrapper_is_detected_but_unit_and_local_tuple_are_not(self):
        hazardous = (
            "struct Shared<T>(Arc<UnsafeCell<T>>); unsafe impl<T> Sync for Shared<T>{} "
            "impl<T> Shared<T>{fn expose(&self)->&mut T{unsafe{&mut *self.0.get()}}}"
        )
        self.assertIn("shared_unsafe_cell_wrapper", self.kinds(hazardous))
        self.assertNotIn(
            "shared_unsafe_cell_wrapper",
            self.kinds("struct Unit; struct Local<T>(UnsafeCell<T>);"),
        )

    def test_shared_cell_wrapper_evidence_follows_same_module_alias_chain(self):
        source = (
            "struct Shared<T>(UnsafeCell<T>); type First<T>=Shared<T>; type Alias<T>=First<T>; "
            "unsafe impl<T> Sync for Alias<T>{} "
            "impl<T> Alias<T>{fn expose(&self)->&mut T{unsafe{&mut *self.0.get()}}}"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "Shared" and f.kind == "shared_unsafe_cell_wrapper" for f in findings))
        self.assertTrue(any(f.symbol == "Alias" for f in findings))

    def test_wrapper_alias_uses_qualified_visible_path(self):
        source = (
            "mod source { pub(crate) struct Shared<T>(UnsafeCell<T>); } "
            "mod consumer { type Alias<T>=crate::source::Shared<T>; "
            "unsafe impl<T> Sync for Alias<T>{} }"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "Shared" and f.kind == "shared_unsafe_cell_wrapper" for f in findings))
        self.assertTrue(any(f.symbol == "Alias" for f in findings))

    def test_direct_glob_wrapper_then_unsafe_sync_is_detected(self):
        source = (
            "mod source { pub struct Shared<T>(UnsafeCell<T>); } "
            "mod consumer { use crate::source::*; unsafe impl<T> Sync for Shared<T>{} }"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "Shared" and f.kind == "shared_unsafe_cell_wrapper" for f in findings))

    def test_chained_glob_wrapper_then_safe_escape_is_detected(self):
        source = (
            "mod source { pub struct Shared<T>{inner:Arc<UnsafeCell<T>>} } "
            "mod bridge { pub use crate::source::*; } "
            "mod consumer { use crate::bridge::*; impl<T> Shared<T>{ "
            "fn expose(&self)->&mut T{unsafe{&mut *self.inner.get()}} } }"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "Shared" and f.kind == "shared_unsafe_cell_wrapper" for f in findings))

    def test_globbed_local_cell_without_sync_or_escape_is_safe(self):
        source = (
            "mod source { pub struct Shared<T>(UnsafeCell<T>); } "
            "mod consumer { use crate::source::*; fn ordinary(_:Shared<u8>){} }"
        )
        self.assertNotIn("shared_unsafe_cell_wrapper", self.kinds(source))

    def test_inline_sibling_wrapper_names_do_not_cross_match(self):
        source = (
            "mod hazardous { struct Shared<T>(UnsafeCell<T>); unsafe impl<T> Sync for Shared<T>{} }\n"
            "mod safe { struct Shared<T>(UnsafeCell<T>); }"
        )
        findings = self.scan(source)
        occurrences = [o for f in findings if f.kind == "shared_unsafe_cell_wrapper" for o in f.occurrences]
        self.assertTrue(occurrences)
        self.assertTrue(all(o["line"] == 1 for o in occurrences))

    def test_production_category(self):
        self.assertEqual({"production"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}")})

    def test_test_category(self):
        self.assertEqual({"test"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}", "crate/tests/a.rs")})

    def test_compat_category(self):
        self.assertEqual({"compatibility"}, {f.category for f in self.scan("pub use x::ArcMut;", "rocketmq/src/lib.rs")})

    def test_semantic_identity_ignores_line_movement(self):
        a = self.scan("fn f(){let x=ArcMut::new(1);}")
        b = self.scan("\n\nfn f(){ let x = ArcMut::new(1); }")
        self.assertEqual([x.identity for x in a], [x.identity for x in b])
        self.assertEqual([o["id"] for o in a[0].occurrences], [o["id"] for o in b[0].occurrences])

    def test_inline_cfg_test_overrides_production_file(self):
        findings = self.scan("#[cfg(test)] mod tests { fn f(x: ArcMut<X>){} }")
        self.assertEqual({"test"}, {f.category for f in findings})

    def test_benches_and_examples_are_test_but_example_project_src_is_not(self):
        self.assertEqual({"test"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}", "tests/root.rs")})
        self.assertEqual({"test"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}", "crate/benches/a.rs")})
        self.assertEqual({"test"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}", "crate/examples/a.rs")})
        self.assertEqual({"production"}, {f.category for f in self.scan("fn f(x: ArcMut<X>){}", "rocketmq-example/src/a.rs")})

    def test_renaming_alias_does_not_hide_usage(self):
        self.assertTrue(self.scan("use x::ArcMut as Innocent; fn f(x: Innocent<T>){}"))

    def test_grouped_and_nested_use_trees_track_each_alias(self):
        findings = self.scan(
            "use x::{ArcMut as A, Other as B, nested::{WeakArcMut as W}}; "
            "fn f(a:A<T>, w:W<T>){}"
        )
        symbols = {finding.symbol for finding in findings}
        self.assertTrue({"ArcMut", "A", "WeakArcMut", "W"}.issubset(symbols))
        self.assertFalse(any(f.symbol == "B" for f in findings))

    def test_grouped_pub_use_is_reexport(self):
        findings = self.scan("pub use x::{ArcMut as A, WeakArcMut};")
        self.assertTrue(findings)
        self.assertEqual({"reexport"}, {finding.kind for finding in findings})

    def test_cross_file_alias_and_reexport_fixed_point(self):
        guard = load_guard()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.rs").write_text("pub type Shared<T> = ArcMut<T>;", encoding="utf-8")
            (root / "bridge.rs").write_text("pub use crate::a::Shared as Exported;", encoding="utf-8")
            (root / "b.rs").write_text(
                "use crate::bridge::Exported as Local; fn consume(x:Local<T>){} "
                "impl<T> AsMut<T> for Local<T>{fn as_mut(&mut self)->&mut T{todo!()}} "
                "impl<T> DerefMut for crate::a::Shared<T>{fn deref_mut(&mut self)->&mut T{todo!()}}",
                encoding="utf-8",
            )
            findings = guard.scan_tree(root)
        b_findings = [f for f in findings if f.path == "b.rs"]
        self.assertTrue(any(f.symbol == "Local" and f.kind == "type_reference" for f in b_findings))
        self.assertTrue(any(f.kind == "dangerous_as_mut_impl" for f in b_findings))
        self.assertTrue(any(f.kind == "dangerous_deref_mut_impl" for f in b_findings))

    def test_private_alias_is_visible_to_descendant_but_not_sibling(self):
        guard = load_guard()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); (root / "parent").mkdir()
            (root / "parent.rs").write_text("type Shared<T>=ArcMut<T>; mod child;", encoding="utf-8")
            (root / "parent" / "child.rs").write_text("use super::Shared as Child; fn f(x:Child<T>){}", encoding="utf-8")
            (root / "sibling.rs").write_text("struct Shared<T>(T); fn f(x:Shared<T>){}", encoding="utf-8")
            findings = guard.scan_tree(root)
        self.assertTrue(any(f.path == "parent/child.rs" and f.symbol == "Child" for f in findings))
        self.assertFalse(any(f.path == "sibling.rs" and f.symbol == "Shared" for f in findings))

    def test_pub_crate_and_pub_super_visibility_in_inline_modules(self):
        source = (
            "mod parent { pub(crate) type CrateShared<T>=ArcMut<T>; "
            "mod inner { pub(super) type ParentShared<T>=ArcMut<T>; } "
            "fn parent_ok(x:inner::ParentShared<T>){} }\n"
            "mod sibling { fn crate_ok(x:crate::parent::CrateShared<T>){} "
            "struct ParentShared<T>(T); fn safe(x:ParentShared<T>){} }"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "CrateShared" for f in findings))
        parent_occurrences = [o for f in findings if f.symbol == "ParentShared" for o in f.occurrences]
        self.assertTrue(parent_occurrences)
        self.assertTrue(all(o["line"] == 1 for o in parent_occurrences))

    def test_glob_reexport_and_chained_glob_propagate_hazards(self):
        guard = load_guard()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "a.rs").write_text("pub type Shared<T>=ArcMut<T>;", encoding="utf-8")
            (root / "bridge.rs").write_text("pub use crate::a::*;", encoding="utf-8")
            (root / "chain.rs").write_text("pub use crate::bridge::*;", encoding="utf-8")
            (root / "consumer.rs").write_text("use crate::chain::*; fn f(x:Shared<T>){}", encoding="utf-8")
            findings = guard.scan_tree(root)
        self.assertTrue(any(f.path == "consumer.rs" and f.symbol == "Shared" for f in findings))

    def test_explicit_safe_alias_shadows_hazardous_glob(self):
        source = (
            "struct Safe<T>(T); mod hazard { pub type Shared<T>=ArcMut<T>; } "
            "mod consumer { type Shared<T>=crate::Safe<T>; use crate::hazard::*; fn f(x:Shared<T>){} }"
        )
        findings = self.scan(source)
        consumer_lines = [o for f in findings if f.symbol == "Shared" for o in f.occurrences]
        self.assertTrue(consumer_lines)  # hazard declaration remains governed
        self.assertFalse(any(o["item"] == "fn f" for o in consumer_lines))

    def test_explicit_hazardous_alias_still_wins_over_glob(self):
        source = (
            "mod hazard { pub type Shared<T>=ArcMut<T>; } "
            "mod consumer { type Shared<T>=ArcMut<T>; use crate::hazard::*; fn f(x:Shared<T>){} }"
        )
        findings = self.scan(source)
        self.assertTrue(any(f.symbol == "Shared" and any(o["item"] == "fn f" for o in f.occurrences) for f in findings))

    def test_inline_module_scopes_prevent_safe_sibling_name_collision(self):
        source = (
            "mod hazardous { type Shared<T>=ArcMut<T>; fn bad(x:Shared<T>){} "
            "mod nested { fn inherited(x:super::Shared<T>){} } }\n"
            "mod safe { struct Shared<T>(T); fn okay(x:Shared<T>){} }"
        )
        findings = self.scan(source)
        occurrences = [o for f in findings if f.symbol == "Shared" for o in f.occurrences]
        self.assertTrue(occurrences)
        self.assertTrue(all(o["line"] == 1 for o in occurrences))


class ArcMutBaselineTests(unittest.TestCase):
    def entry(self, identity="abc", remove_by="M03"):
        return {
            "identity": identity, "path": "a/src/lib.rs", "symbol": "ArcMut",
            "kind": "type_reference", "category": "production", "owner": "runtime",
            "reason": "legacy shared ownership", "remove_by": remove_by,
            "adr": "ADR-002", "occurrences": [
                {"id": identity + "-occ", "fingerprint": "fingerprint", "item": "fn f", "line": 10}
            ],
        }

    def baseline(self, entries):
        return {"schema_version": 1, "current_milestone": "M01", "entries": entries}

    def test_new_stale_moved_changed_and_expired_are_violations(self):
        guard = load_guard()
        old = self.baseline([self.entry(), self.entry("expired", "M01")])
        actual = [guard.Finding("new", "b.rs", "ArcMut", "constructor", "production", (
            {"id": "new-occ", "fingerprint": "f", "item": "fn f", "line": 1},
        ))]
        issues = guard.compare_findings(actual, old, "M02")
        codes = {x.code for x in issues}
        self.assertTrue({"NEW", "STALE", "EXPIRED"}.issubset(codes))

    def test_changed_and_stale_occurrences_are_independent(self):
        guard = load_guard()
        entry = self.entry()
        entry["occurrences"].append({"id": "gone", "fingerprint": "g", "item": "fn g", "line": 20})
        actual = [guard.Finding("abc", "a/src/lib.rs", "ArcMut", "type_reference", "production", (
            {"id": "abc-occ", "fingerprint": "fingerprint", "item": "fn renamed", "line": 99},
        ))]
        codes = {x.code for x in guard.compare_findings(actual, self.baseline([entry]), "M01")}
        self.assertEqual({"CHANGED", "STALE"}, codes)

    def test_moved_occurrence_is_new_and_stale(self):
        guard = load_guard()
        actual = [guard.Finding("moved", "b/src/lib.rs", "ArcMut", "type_reference", "production", (
            {"id": "moved-occ", "fingerprint": "fingerprint", "item": "fn f", "line": 1},
        ))]
        codes = {x.code for x in guard.compare_findings(actual, self.baseline([self.entry()]), "M01")}
        self.assertEqual({"NEW", "STALE"}, codes)

    def test_baseline_entries_require_governance_fields(self):
        guard = load_guard()
        entry = self.entry(); entry["owner"] = ""
        with self.assertRaises(guard.BaselineError):
            guard.validate_baseline(self.baseline([entry]))

    def test_baseline_rejects_placeholder_governance_and_permanent_deadlines(self):
        guard = load_guard()
        for field, value in (
            ("owner", "UNASSIGNED"), ("reason", "REQUIRES_TRIAGE"),
            ("remove_by", "M13"), ("remove_by", "never"), ("adr", "ADR-TBD"),
        ):
            entry = self.entry(); entry[field] = value
            with self.subTest(field=field, value=value), self.assertRaises(guard.BaselineError):
                guard.validate_baseline(self.baseline([entry]))

    def test_m12_is_valid_and_m11_is_expired_at_m12(self):
        guard = load_guard()
        guard.validate_baseline(self.baseline([self.entry(remove_by="M12")]))
        actual = [guard.Finding("abc", "a/src/lib.rs", "ArcMut", "type_reference", "production", (
            {"id": "abc-occ", "fingerprint": "fingerprint", "item": "fn f", "line": 10},
        ))]
        issues = guard.compare_findings(actual, self.baseline([self.entry(remove_by="M11")]), "M12")
        self.assertIn("EXPIRED", {issue.code for issue in issues})

    def test_baseline_milestone_is_used_when_cli_does_not_override_it(self):
        guard = load_guard()
        baseline = self.baseline([self.entry(remove_by="M02")])
        baseline["current_milestone"] = "M03"

        self.assertEqual("M03", guard.resolve_current_milestone(None, baseline))
        self.assertEqual("M01", guard.resolve_current_milestone("M01", baseline))

    def test_compare_rejects_deadline_extension_to_m12(self):
        guard = load_guard()
        old = self.baseline([self.entry(remove_by="M11")])
        new = self.baseline([self.entry(remove_by="M12")])
        self.assertIn("DEADLINE_EXTENDED", {issue.code for issue in guard.compare_baselines(old, new)})

    def test_compare_baseline_allows_subset_and_earlier_deadline_only(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04"), self.entry("b", "M05")])
        new = self.baseline([self.entry("a", "M03")])
        self.assertEqual([], guard.compare_baselines(old, new))
        new["entries"][0]["occurrences"][0]["line"] = 999
        self.assertEqual([], guard.compare_baselines(old, new))
        new["entries"][0]["remove_by"] = "M06"
        self.assertTrue(guard.compare_baselines(old, new))

    def test_promote_preserves_governance_for_monotonic_debt_reduction(self):
        guard = load_guard()
        old_entry = self.entry("a", "M04")
        old_entry["owner"] = "runtime-foundation"
        old_entry["reason"] = "Compatibility debt retained during ownership migration"
        old_entry["occurrences"].append(
            {"id": "old-extra", "fingerprint": "extra", "item": "fn old", "line": 20}
        )
        findings = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "new-location", "fingerprint": "new", "item": "fn f", "line": 40},),
        )]

        promoted = guard.promote_findings(
            findings,
            self.baseline([old_entry]),
            "M03",
            {
                ("a", "new-location"): {
                    "from": "a-occ",
                    "reason": "The governed item stayed fixed while adjacent token context changed",
                    "adr": "ADR-013",
                }
            },
        )

        self.assertEqual("M03", promoted["current_milestone"])
        self.assertEqual("runtime-foundation", promoted["entries"][0]["owner"])
        self.assertEqual(
            "Compatibility debt retained during ownership migration",
            promoted["entries"][0]["reason"],
        )
        self.assertEqual(findings[0].occurrences, tuple(promoted["entries"][0]["occurrences"]))

    def test_promote_rejects_identity_or_occurrence_expansion(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04")])
        new_identity = [guard.Finding(
            "b", "b/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "b-occ", "fingerprint": "b", "item": "fn b", "line": 1},),
        )]
        with self.assertRaises(guard.BaselineError):
            guard.promote_findings(new_identity, old, "M03")

        expanded = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            (
                {"id": "one", "fingerprint": "one", "item": "fn one", "line": 1},
                {"id": "two", "fingerprint": "two", "item": "fn two", "line": 2},
            ),
        )]
        with self.assertRaises(guard.BaselineError):
            guard.promote_findings(expanded, old, "M03")

    def test_promote_rejects_replaced_occurrence_without_approval(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04")])
        replacement = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "replacement", "fingerprint": "new", "item": "fn f", "line": 2},),
        )]

        with self.assertRaises(guard.BaselineError):
            guard.promote_findings(replacement, old, "M03")

    def test_promote_allows_reviewed_one_to_one_occurrence_relocation(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04")])
        replacement = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "replacement", "fingerprint": "new", "item": "fn f", "line": 2},),
        )]
        approvals = {
            ("a", "replacement"): {
                "from": "a-occ",
                "reason": "Adjacent owned-state migration changed token context only",
                "adr": "ADR-013",
            }
        }

        promoted = guard.promote_findings(replacement, old, "M03", approvals)

        self.assertEqual("replacement", promoted["entries"][0]["occurrences"][0]["id"])

    def test_promote_rejects_same_occurrence_fingerprint_change_without_approval(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04")])
        changed = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "a-occ", "fingerprint": "changed", "item": "fn f", "line": 20},),
        )]

        with self.assertRaises(guard.BaselineError):
            guard.promote_findings(changed, old, "M03")

    def test_promote_allows_reviewed_same_occurrence_fingerprint_change(self):
        guard = load_guard()
        old = self.baseline([self.entry("a", "M04")])
        changed = [guard.Finding(
            "a", "a/src/lib.rs", "ArcMut", "type_reference", "production",
            ({"id": "a-occ", "fingerprint": "changed", "item": "fn f", "line": 20},),
        )]
        approvals = {
            ("a", "a-occ"): {
                "from": "a-occ",
                "reason": "The governed item stayed fixed while adjacent token context changed",
                "adr": "ADR-013",
            }
        }

        promoted = guard.promote_findings(changed, old, "M03", approvals)

        self.assertEqual("changed", promoted["entries"][0]["occurrences"][0]["fingerprint"])

    def test_compare_rejects_unused_occurrence_relocation_approval(self):
        guard = load_guard()
        baseline = self.baseline([self.entry("a", "M04")])
        approvals = {
            ("a", "replacement"): {
                "from": "a-occ",
                "reason": "Adjacent owned-state migration changed token context only",
                "adr": "ADR-013",
            }
        }

        issues = guard.compare_baselines(baseline, baseline, approvals)

        self.assertIn("UNUSED_RELOCATION", {issue.code for issue in issues})

    def test_bootstrap_rejects_non_target_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            completed = subprocess.run(
                [sys.executable, str(SCRIPT), "--root", tmp, "--bootstrap", str(ROOT / "forbidden.json")],
                text=True, capture_output=True,
            )
        self.assertEqual(2, completed.returncode)

    def test_builtin_fixture_matrix(self):
        completed = subprocess.run([sys.executable, str(SCRIPT), "--fixtures"], text=True, capture_output=True)
        self.assertEqual(0, completed.returncode, completed.stdout + completed.stderr)
        self.assertIn("FIXTURES_OK", completed.stdout)


if __name__ == "__main__":
    unittest.main()
