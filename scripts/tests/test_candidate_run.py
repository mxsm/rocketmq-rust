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

import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from scripts.tests.release_test_support import load_module, read_json


class CandidateRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.series = load_module("release_series_for_candidate", "distribution/release_series.py")
        cls.candidate = load_module("candidate_run", "distribution/candidate_run.py")
        cls.lifecycle = load_module("release_lifecycle_for_candidate", "scripts/release_lifecycle_guard.py")

    def test_candidate_is_the_only_run_scoped_selector(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)

            value = read_json(candidate)
            self.assertEqual(value["candidate_kind"], "rc")
            self.assertEqual(value["state"], "development")
            self.assertFalse(value["sealed"])
            self.assertEqual(value["ordinal"], 1)
            self.assertIsNone(value["parent_manifest"])
            self.assertEqual(Path(value["candidate_root"]), candidate.parent.resolve())
            self.assertEqual(read_json(series)["head"]["candidate_manifest"], str(candidate.resolve()))
            self.assertIn("route_denominator", value)
            self.assertEqual(1, value["route_denominator"]["schema_version"])
            self.assertEqual(
                ["R11-aggregate-validate"],
                value["route_denominator"]["audit_points"]["release-preparation-aggregate"],
            )
            self.assertEqual(
                [
                    "H01-LINUX",
                    "H01-WINDOWS",
                    "H01-MACOS",
                    "H01-MERGE",
                    "H01-REFRESH",
                    "H02-DRAFT-SEMANTIC",
                ],
                value["route_denominator"]["audit_points"]["handoff-draft"],
            )

    def test_unsealed_head_duplicate_rc_and_stale_parent_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)

            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(root / "candidates", "1.0.0-rc.2", "rc2", 1, series)
            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(
                    root / "candidates",
                    "1.0.0-rc.1",
                    "duplicate",
                    2,
                    series,
                    parent_manifest=rc1,
                )

    def test_known_issues_are_inherited_without_reusing_approval(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            self.candidate.record_known_issue(
                rc1,
                issue_id="RMQ-1",
                severity="Medium",
                impact="CLI warning",
                workaround="Retry once",
                owner="release-team",
                target_version="1.0.1",
                approver="approver",
                waiver_expiry="2026-09-01",
            )
            self.lifecycle.transition_candidate(rc1, "rejected", phase=5, rejection_reason="fixture rejection")
            rc2 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.2", "rc2", 1, series)

            inherited = read_json(rc2)["known_issues"]
            self.assertEqual(len(inherited), 1)
            self.assertEqual(inherited[0]["approval_status"], "inherited-pending-approval")
            self.assertIsNone(inherited[0]["approver"])
            self.assertIsNone(inherited[0]["waiver_expiry"])

            self.candidate.approve_known_issue(
                rc2,
                issue_id="RMQ-1",
                approver="release-approver",
                waiver_expiry="2026-10-01",
            )
            reapproved = read_json(rc2)["known_issues"][0]
            self.assertEqual(reapproved["approval_status"], "approved")
            self.assertEqual(reapproved["approver"], "release-approver")
            self.assertEqual(reapproved["waiver_expiry"], "2026-10-01")

    def test_unresolved_high_issue_can_only_stop_blocking_after_explicit_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            self.candidate.record_known_issue(
                rc1,
                issue_id="RMQ-HIGH",
                severity="High",
                impact="message correctness",
                workaround="none",
                owner="broker-team",
                target_version="1.0.0-rc.2",
            )
            self.candidate.close_known_issue(
                rc1,
                issue_id="RMQ-HIGH",
                resolved_by="broker-owner",
                resolution="fixed and regression-tested",
            )

            resolved = read_json(rc1)["known_issues"][0]
            self.assertEqual(resolved["resolution_status"], "closed")
            self.assertEqual(resolved["resolved_by"], "broker-owner")
            self.assertEqual(resolved["resolution"], "fixed and regression-tested")

    def test_interrupted_cross_file_creation_is_completed_or_abandoned_without_a_fork(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(
                    root / "candidates",
                    "1.0.0-rc.1",
                    "rc1",
                    1,
                    series,
                    fail_after="candidate-write",
                )
            self.assertEqual(self.candidate.recover_pending_creation(series), "committed")
            self.assertEqual(len(read_json(series)["entries"]), 1)
            self.lifecycle.transition_candidate(
                Path(read_json(series)["head"]["candidate_manifest"]),
                "rejected",
                phase=5,
                rejection_reason="fixture",
            )

            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(
                    root / "candidates",
                    "1.0.0-rc.2",
                    "rc2",
                    1,
                    series,
                    fail_after="series-pending",
                )
            self.assertEqual(self.candidate.recover_pending_creation(series), "abandoned")
            self.assertEqual(len(read_json(series)["entries"]), 1)

    def test_explicit_stale_parent_cannot_fork_the_current_series_head(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            rc1 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.1", "rc1", 1, series)
            self.lifecycle.transition_candidate(rc1, "rejected", phase=5, rejection_reason="fixture")
            rc2 = self.candidate.create_candidate(root / "candidates", "1.0.0-rc.2", "rc2", 1, series)
            self.lifecycle.transition_candidate(rc2, "rejected", phase=5, rejection_reason="fixture")

            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.create_candidate(
                    root / "candidates",
                    "1.0.0-rc.3",
                    "rc3",
                    1,
                    series,
                    parent_manifest=rc1,
                )

    def test_candidate_validation_rejects_fields_outside_the_closed_schema(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            value = read_json(candidate)
            value["unexpected"] = True

            with self.assertRaises(self.candidate.ReleaseStateError):
                self.candidate.validate_candidate(value)

    def test_candidate_validation_rejects_duplicate_route_denominator_entries(self) -> None:
        """A duplicated required route would make the frozen denominator ambiguous."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            value = read_json(candidate)
            routes = value["route_denominator"]["audit_points"]["handoff-draft"]
            routes.append(routes[0])

            with self.assertRaisesRegex(
                self.candidate.ReleaseStateError,
                "route denominator is invalid",
            ):
                self.candidate.validate_candidate(value)

    def test_candidate_validation_types_route_denominator_errors(self) -> None:
        """Malformed nested route values must fail as release-state errors, not raw TypeError."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            value = read_json(candidate)
            value["route_denominator"]["audit_points"]["handoff-draft"] = [["nested"]]

            try:
                self.candidate.validate_candidate(value)
            except Exception as error:  # noqa: BLE001 - the assertion checks the public error boundary.
                self.assertIsInstance(error, self.candidate.ReleaseStateError)
            else:
                self.fail("malformed route denominator was accepted")

    def test_candidate_creation_rejects_a_malformed_route_policy_before_series_mutation(self) -> None:
        """A bad repository policy must not create an invalid candidate or advance the series."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            invalid_policy = root / "invalid-route-policy.json"
            invalid_policy.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "audit_points": {"handoff-draft": ["H01-LINUX", "H01-LINUX"]},
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch.object(self.candidate, "ROUTE_DENOMINATOR", invalid_policy):
                with self.assertRaisesRegex(
                    self.candidate.ReleaseStateError,
                    "route denominator is invalid",
                ):
                    self.candidate.create_candidate(
                        root / "candidates",
                        "1.0.0-rc.1",
                        "rc1",
                        1,
                        series,
                    )

            self.assertIsNone(read_json(series)["head"])
            self.assertFalse((root / "candidates").exists())

    def test_candidate_metadata_updates_require_a_consistent_current_series_head(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            series = self.series.create_series(root / "series", "1.0", "community-v1")
            candidate = self.candidate.create_candidate(
                root / "candidates", "1.0.0-rc.1", "rc1", 1, series
            )
            series_value = read_json(series)
            series_value["head"] = None
            series.write_text(json.dumps(series_value), encoding="utf-8")

            with self.assertRaises(self.candidate.CandidateError):
                self.candidate.record_known_issue(
                    candidate,
                    issue_id="RMQ-1",
                    severity="Low",
                    impact="minor",
                    workaround="retry",
                    owner="release-team",
                    target_version="1.0.1",
                )


if __name__ == "__main__":
    unittest.main()
