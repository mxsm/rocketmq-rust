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

import tempfile
import unittest
from pathlib import Path
import stat

from scripts.tests.release_test_support import (
    ROOT,
    create_source_bundle,
    load_module,
    read_json,
    write_gate_evidence,
    write_json,
)


class ReleaseLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series_for_lifecycle", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run_for_lifecycle", "distribution/candidate_run.py")
        cls.lifecycle = load_module("release_lifecycle_guard", "scripts/release_lifecycle_guard.py")
        cls.snapshot = load_module(
            "create_candidate_source_snapshot_for_lifecycle",
            "distribution/create_candidate_source_snapshot.py",
        )

    def create_rc(self, root: Path, series: Path, suffix: int) -> Path:
        return self.candidate.create_candidate(
            root / "candidates", f"1.0.0-rc.{suffix}", f"rc{suffix}", 1, series
        )

    def make_ready(self, manifest: Path) -> None:
        value = read_json(manifest)
        bundle = create_source_bundle(
            manifest.parent / "CORE_SOURCE_TRANSFER.tar",
            version=value["version"],
            run_id=value["run_id"],
            attempt=value["attempt"],
        )
        self.candidate.record_build_source_bundle(manifest, bundle)
        self.snapshot.create_snapshot(manifest)
        evidence = write_gate_evidence(manifest.parent / "gate-evidence.json", value["candidate_id"])
        self.lifecycle.transition_candidate(manifest, "staged-rc", phase=5)
        self.lifecycle.transition_candidate(manifest, "rc-candidate-ready", phase=6, gate_evidence=evidence)

    def test_two_consecutive_sealed_rcs_are_required_for_final(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            self.make_ready(rc1)
            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(root / "candidates", "1.0.0", "final-too-early", 1, series)

            rc2 = self.create_rc(root, series, 2)
            self.make_ready(rc2)
            final = self.candidate.create_candidate(root / "candidates", "1.0.0", "final", 1, series)
            evidence = write_gate_evidence(final.parent / "gate-evidence.json", read_json(final)["candidate_id"])
            self.lifecycle.transition_candidate(final, "ga-candidate-ready", phase=6, gate_evidence=evidence)
            self.assertEqual(read_json(final)["state"], "ga-candidate-ready")
            self.assertFalse(read_json(final)["sealed"])

    def test_rejected_rc_resets_the_consecutive_success_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            self.make_ready(rc1)
            rc2 = self.create_rc(root, series, 2)
            self.lifecycle.transition_candidate(rc2, "staged-rc", phase=5)
            self.lifecycle.transition_candidate(rc2, "rejected", phase=6, rejection_reason="test failure")
            rc3 = self.create_rc(root, series, 3)
            self.make_ready(rc3)

            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(root / "candidates", "1.0.0", "final", 1, series)
            self.assertEqual(read_json(series)["consecutive_successful_rcs"], 1)

    def test_remote_states_and_incomplete_gate_evidence_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            self.lifecycle.transition_candidate(rc1, "staged-rc", phase=5)
            value = read_json(rc1)
            evidence = write_gate_evidence(root / "failed-evidence.json", value["candidate_id"], complete=False)

            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(rc1, "rc-candidate-ready", phase=6, gate_evidence=evidence)
            for state in ("publishing", "released"):
                with self.assertRaises(self.lifecycle.LifecycleError):
                    self.lifecycle.transition_candidate(rc1, state, phase=6)

    def test_missing_capability_or_release_result_cannot_be_hidden_by_a_summary_flag(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            self.lifecycle.transition_candidate(rc1, "staged-rc", phase=5)
            evidence = write_gate_evidence(root / "gate-evidence.json", read_json(rc1)["candidate_id"])
            value = read_json(evidence)
            value["capability_results"].pop("G-05")
            write_json(evidence, value)

            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(rc1, "rc-candidate-ready", phase=6, gate_evidence=evidence)

    def test_final_can_be_rejected_and_a_later_rc_uses_a_new_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            for suffix in (1, 2):
                self.make_ready(self.create_rc(root, series, suffix))
            final = self.candidate.create_candidate(root / "candidates", "1.0.0", "final-a", 1, series)
            self.lifecycle.transition_candidate(final, "rejected", phase=6, rejection_reason="handoff failed")
            rc3 = self.create_rc(root, series, 3)

            self.assertEqual(read_json(rc3)["parent_manifest"], str(final.resolve()))
            self.assertEqual(read_json(series)["consecutive_successful_rcs"], 0)
            self.make_ready(rc3)
            rc4 = self.create_rc(root, series, 4)
            self.make_ready(rc4)
            second_final = self.candidate.create_candidate(
                root / "candidates", "1.0.0", "final-b", 2, series
            )
            self.assertGreater(read_json(second_final)["ordinal"], read_json(final)["ordinal"])
            self.assertEqual(read_json(second_final)["parent_manifest"], str(rc4.resolve()))

    def test_interrupted_lifecycle_transaction_recovers_without_double_counting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(
                    rc1,
                    "rejected",
                    phase=5,
                    rejection_reason="fixture",
                    fail_after="candidate-write",
                )
            self.assertEqual(self.lifecycle.recover_pending_transition(series), "committed")
            self.assertTrue(read_json(rc1)["sealed"])
            self.assertEqual(read_json(series)["head"]["state"], "rejected")

            rc2 = self.create_rc(root, series, 2)
            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(
                    rc2,
                    "staged-rc",
                    phase=5,
                    fail_after="series-pending",
                )
            self.assertEqual(self.lifecycle.recover_pending_transition(series), "abandoned")
            self.assertEqual(read_json(rc2)["state"], "development")

    def test_schemas_and_lifecycle_config_are_closed_and_digest_free(self) -> None:
        schemas = [
            "candidate-run.schema.json",
            "release-series.schema.json",
            "candidate-source-snapshot.schema.json",
            "release-execution-event.schema.json",
            "candidate-execution-context.schema.json",
        ]

        def forbidden(value) -> bool:
            if isinstance(value, dict):
                return any(
                    key.lower() in {"sha", "sha1", "sha256", "digest", "checksum", "content_hash"}
                    or forbidden(child)
                    for key, child in value.items()
                )
            return isinstance(value, list) and any(forbidden(child) for child in value)

        for name in schemas:
            schema = read_json(ROOT / "distribution" / name)
            self.assertEqual(schema["$schema"], "https://json-schema.org/draft/2020-12/schema")
            self.assertFalse(schema.get("additionalProperties", True), name)
            self.assertFalse(forbidden(schema), name)
            self.assertTrue(set(schema["required"]).issubset(schema["properties"]), name)
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series_manifest = self.series.create_series(root / "series", "1.0", "schema-v1")
            candidate_manifest = self.create_rc(root, series_manifest, 1)
            series_schema = read_json(ROOT / "distribution/release-series.schema.json")
            candidate_schema = read_json(ROOT / "distribution/candidate-run.schema.json")
            self.assertTrue(set(read_json(series_manifest)).issubset(series_schema["properties"]))
            self.assertTrue(set(read_json(candidate_manifest)).issubset(candidate_schema["properties"]))
        config = read_json(ROOT / "distribution/config/release-lifecycle.json")
        self.assertEqual(config["remote_publication_states"], ["publishing", "released"])
        self.assertEqual(config["minimum_consecutive_successful_rcs"], 2)
        self.assertIn("rejected", config["candidate_kinds"]["final"]["sealed_states"])

    def test_publication_marker_only_appears_after_candidate_and_series_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            for suffix in (1, 2):
                self.make_ready(self.create_rc(root, series, suffix))
            final = self.candidate.create_candidate(root / "candidates", "1.0.0", "final", 1, series)
            evidence = write_gate_evidence(final.parent / "gate-evidence.json", read_json(final)["candidate_id"])
            self.lifecycle.transition_candidate(final, "ga-candidate-ready", phase=6, gate_evidence=evidence)
            marker = final.parent / "handoff/PUBLICATION_READY.json"

            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(
                    final,
                    "publication-ready",
                    phase=6,
                    handoff_ready=True,
                    publication_marker=marker,
                    fail_after="series-commit",
                )
            self.assertFalse(marker.exists())
            self.assertIsNotNone(read_json(series)["pending_operation"])
            self.assertEqual(self.lifecycle.recover_pending_transition(series), "committed")
            self.assertTrue(marker.is_file())
            self.assertEqual(read_json(marker)["series_generation"], read_json(series)["generation"])
            self.assertIsNone(read_json(series)["pending_operation"])
            self.assertTrue(read_json(final)["sealed"])

    def test_publication_temp_marker_is_recoverable_before_candidate_commit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            for suffix in (1, 2):
                self.make_ready(self.create_rc(root, series, suffix))
            final = self.candidate.create_candidate(root / "candidates", "1.0.0", "final", 1, series)
            evidence = write_gate_evidence(
                final.parent / "gate-evidence.json", read_json(final)["candidate_id"]
            )
            self.lifecycle.transition_candidate(
                final, "ga-candidate-ready", phase=6, gate_evidence=evidence
            )
            marker = final.parent / "handoff/PUBLICATION_READY.json"

            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(
                    final,
                    "publication-ready",
                    phase=6,
                    handoff_ready=True,
                    publication_marker=marker,
                    fail_after="marker-temp",
                )

            pending = read_json(series)["pending_operation"]
            self.assertEqual(pending["to_state"], "publication-ready")
            self.assertFalse(marker.exists())
            self.assertEqual(self.lifecycle.recover_pending_transition(series), "abandoned")
            self.assertFalse(Path(pending["marker_temp"]).exists())
            self.assertEqual(read_json(final)["state"], "ga-candidate-ready")

    def test_same_size_source_snapshot_tampering_blocks_rc_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            value = read_json(rc1)
            bundle = create_source_bundle(
                rc1.parent / "CORE_SOURCE_TRANSFER.tar",
                version=value["version"],
                run_id=value["run_id"],
                attempt=value["attempt"],
            )
            self.candidate.record_build_source_bundle(rc1, bundle)
            snapshot = self.snapshot.create_snapshot(rc1)
            cargo_toml = snapshot.parent / "source/Cargo.toml"
            original = cargo_toml.read_bytes()
            cargo_toml.chmod(cargo_toml.stat().st_mode | stat.S_IWUSR)
            cargo_toml.write_bytes(b"x" * len(original))
            cargo_toml.chmod(
                cargo_toml.stat().st_mode & ~(stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)
            )
            evidence = write_gate_evidence(
                rc1.parent / "gate-evidence.json", value["candidate_id"]
            )
            self.lifecycle.transition_candidate(rc1, "staged-rc", phase=5)

            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(
                    rc1, "rc-candidate-ready", phase=6, gate_evidence=evidence
                )

    def test_unresolved_high_and_unapproved_inherited_issues_block_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.create_rc(root, series, 1)
            self.candidate.record_known_issue(
                rc1,
                issue_id="RMQ-HIGH",
                severity="High",
                impact="message loss risk",
                workaround="none",
                owner="broker-team",
                target_version="1.0.1",
            )
            value = read_json(rc1)
            bundle = create_source_bundle(
                rc1.parent / "CORE_SOURCE_TRANSFER.tar",
                version=value["version"],
                run_id=value["run_id"],
                attempt=value["attempt"],
            )
            self.candidate.record_build_source_bundle(rc1, bundle)
            self.snapshot.create_snapshot(rc1)
            evidence = write_gate_evidence(rc1.parent / "gate-evidence.json", value["candidate_id"])
            self.lifecycle.transition_candidate(rc1, "staged-rc", phase=5)
            with self.assertRaises(self.lifecycle.LifecycleError):
                self.lifecycle.transition_candidate(rc1, "rc-candidate-ready", phase=6, gate_evidence=evidence)


if __name__ == "__main__":
    unittest.main()
