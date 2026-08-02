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

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from scripts import architecture_evidence_bundle as bundle


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "scripts/tests/fixtures/architecture-evidence-bundle"
CANDIDATE = "0123456789abcdef0123456789abcdef01234567"


class ArchitectureEvidenceBundleTests(unittest.TestCase):
    def copy_fixture_record(self, root: Path, fixture: str, name: str) -> Path:
        source = FIXTURES / fixture
        record = json.loads(source.read_text(encoding="utf-8"))
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        for artifact in record.get("artifacts", []):
            artifact_target = target.parent / artifact["path"]
            artifact_target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(FIXTURES / artifact["path"], artifact_target)
        return target

    def write_record(
        self,
        root: Path,
        category: str,
        *,
        name: str | None = None,
        status: str = "pass",
        fixture: object = False,
        source: str | None = None,
        with_artifact: bool = True,
    ) -> tuple[Path, Path | None]:
        record_path = root / (name or f"{category}.json")
        record_path.parent.mkdir(parents=True, exist_ok=True)
        artifacts: list[dict[str, str]] = []
        artifact_path: Path | None = None
        if with_artifact:
            artifact_path = record_path.parent / f"{record_path.stem}.artifact.txt"
            encoded = f"{category} artifact\n".encode()
            artifact_path.write_bytes(encoded)
            artifacts.append(
                {
                    "path": artifact_path.name,
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                }
            )
        record = {
            "schema_version": 1,
            "candidate_commit": CANDIDATE,
            "category": category,
            "source": source or bundle.CATEGORY_SOURCES[category],
            "fixture": fixture,
            "status": status,
            "artifacts": artifacts,
        }
        record_path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
        return record_path, artifact_path

    def run_cli(self, root: Path, *arguments: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(ROOT / "scripts/architecture_evidence_bundle.py"), *arguments],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_missing_categories_are_not_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_fixture_record(root, "pass.json", "performance.json")

            manifest = bundle.assemble(
                CANDIDATE,
                root,
                {"performance": "performance.json", "disaster_recovery": "absent.json"},
            )

            self.assertEqual("not-run", manifest["status"])
            self.assertEqual("pass", manifest["evidence"][0]["status"])
            self.assertEqual(["not-run"] * 4, [item["status"] for item in manifest["evidence"][1:]])
            self.assertEqual("absent.json", manifest["evidence"][2]["record_path"])
            self.assertEqual([], bundle.validate_manifest(manifest, root))

    def test_explicit_failure_remains_a_valid_failure_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_fixture_record(root, "fail.json", "performance.json")

            manifest = bundle.assemble(CANDIDATE, root, {"performance": "performance.json"})

            self.assertEqual("fail", manifest["status"])
            self.assertEqual("fail", manifest["evidence"][0]["status"])
            self.assertEqual([], bundle.validate_manifest(manifest, root))

    def test_all_five_non_fixture_pass_records_produce_a_pass_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence: dict[str, str] = {}
            for category in bundle.CATEGORIES:
                record, _ = self.write_record(root, category)
                evidence[category] = record.name

            manifest = bundle.assemble(CANDIDATE, root, evidence)

            self.assertEqual("pass", manifest["status"])
            self.assertTrue(all(item["status"] == "pass" for item in manifest["evidence"]))
            self.assertTrue(all(item["artifacts"] for item in manifest["evidence"]))
            self.assertEqual([], bundle.validate_manifest(manifest, root))

    def test_wrong_source_category_and_non_boolean_fixture_fail_closed(self) -> None:
        mutations = {
            "source": "wrong-source",
            "category": "disaster_recovery",
            "fixture": "false",
        }
        for field, value in mutations.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                record_path, _ = self.write_record(root, "performance")
                record = json.loads(record_path.read_text(encoding="utf-8"))
                record[field] = value
                record_path.write_text(json.dumps(record), encoding="utf-8")

                manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})

                self.assertEqual("fail", manifest["status"])
                self.assertIn(field, manifest["evidence"][0]["finding"])

    def test_fixture_pass_and_pass_without_artifacts_fail_closed(self) -> None:
        cases = ({"fixture": True}, {"with_artifact": False})
        for options in cases:
            with self.subTest(options=options), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                record_path, _ = self.write_record(root, "performance", **options)

                manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})

                self.assertEqual("fail", manifest["status"])
                self.assertIsNotNone(manifest["evidence"][0]["finding"])

    def test_existing_slo_fixture_is_compatible_but_not_production_pass(self) -> None:
        fixture = ROOT / "scripts/tests/fixtures/m11-slo/pass/run.json"
        record = json.loads(fixture.read_text(encoding="utf-8"))

        manifest = bundle.assemble(
            record["candidate_commit"],
            fixture.parent,
            {"ha_soak_rpo_rto": fixture.name},
        )

        ha_evidence = manifest["evidence"][1]
        self.assertEqual("not-run", manifest["status"])
        self.assertEqual("not-run", ha_evidence["status"])
        self.assertEqual("architecture-slo-evidence", ha_evidence["source"])
        self.assertEqual([], bundle.validate_manifest(manifest, fixture.parent))

    def test_candidate_mismatch_and_invalid_status_fail_closed(self) -> None:
        mutations = {"candidate_commit": "f" * 40, "status": ["pass"]}
        for field, value in mutations.items():
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                record_path, _ = self.write_record(root, "performance")
                record = json.loads(record_path.read_text(encoding="utf-8"))
                record[field] = value
                record_path.write_text(json.dumps(record), encoding="utf-8")

                manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})

                self.assertEqual("fail", manifest["status"])
                self.assertIn(field.split("_")[0], manifest["evidence"][0]["finding"])

    def test_pass_and_not_run_artifact_tampering_is_detected(self) -> None:
        for status in ("pass", "not-run"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                record_path, artifact_path = self.write_record(root, "performance", status=status)
                manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})
                assert artifact_path is not None
                artifact_path.write_text("tampered\n", encoding="utf-8")

                findings = bundle.validate_manifest(manifest, root)
                rebuilt = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})

                self.assertTrue(any("does not match its source record" in item for item in findings))
                self.assertEqual("fail", rebuilt["evidence"][0]["status"])
                self.assertIn("artifact hash mismatch", rebuilt["evidence"][0]["finding"])

    def test_record_tampering_is_detected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record_path, _ = self.write_record(root, "performance")
            manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})
            record_path.write_text(record_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

            findings = bundle.validate_manifest(manifest, root)

            self.assertTrue(any("does not match its source record" in item for item in findings))

    def test_unsafe_and_duplicate_artifacts_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            record_path, _ = self.write_record(root, "performance")
            record = json.loads(record_path.read_text(encoding="utf-8"))
            record["artifacts"].append(dict(record["artifacts"][0]))
            record_path.write_text(json.dumps(record), encoding="utf-8")

            manifest = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})

            self.assertEqual("fail", manifest["status"])
            self.assertIn("duplicate artifact path", manifest["evidence"][0]["finding"])
            record["artifacts"] = [record["artifacts"][0] | {"path": "../outside.txt"}]
            record_path.write_text(json.dumps(record), encoding="utf-8")
            unsafe = bundle.assemble(CANDIDATE, root, {"performance": record_path.name})
            self.assertIn("stay below", unsafe["evidence"][0]["finding"])
            with self.assertRaises(bundle.BundleError):
                bundle.safe_relative_path(root, "../outside.json", "fixture")
            with self.assertRaises(bundle.BundleError):
                bundle.parse_evidence_arguments(["performance=a.json", "performance=b.json"])

    def test_assemble_output_cannot_overwrite_record_or_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record_path, artifact_path = self.write_record(root, "performance")
            assert artifact_path is not None
            original_record = record_path.read_bytes()
            original_artifact = artifact_path.read_bytes()
            common = (
                "assemble",
                "--candidate",
                CANDIDATE,
                "--evidence-root",
                str(root),
                "--evidence",
                f"performance={record_path.name}",
            )

            record_collision = self.run_cli(root, *common, "--output", record_path.name)
            artifact_collision = self.run_cli(root, *common, "--output", artifact_path.name)
            record_alias = root / "record-hardlink.json"
            artifact_alias = root / "artifact-hardlink.json"
            os.link(record_path, record_alias)
            os.link(artifact_path, artifact_alias)
            record_hardlink_collision = self.run_cli(root, *common, "--output", record_alias.name)
            artifact_hardlink_collision = self.run_cli(root, *common, "--output", artifact_alias.name)

            self.assertEqual(2, record_collision.returncode)
            self.assertEqual(2, artifact_collision.returncode)
            self.assertEqual(2, record_hardlink_collision.returncode)
            self.assertEqual(2, artifact_hardlink_collision.returncode)
            self.assertEqual(original_record, record_path.read_bytes())
            self.assertEqual(original_artifact, artifact_path.read_bytes())
            self.assertTrue(record_path.samefile(record_alias))
            self.assertTrue(artifact_path.samefile(artifact_alias))

    def test_scoped_category_pass_binds_candidate_and_source_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            record_path, artifact_path = self.write_record(root, "performance")
            assert artifact_path is not None
            assembled = self.run_cli(
                root,
                "assemble",
                "--candidate",
                CANDIDATE,
                "--evidence-root",
                str(root),
                "--evidence",
                f"performance={record_path.name}",
                "--output",
                "bundle.json",
            )
            self.assertEqual(0, assembled.returncode, assembled.stdout + assembled.stderr)
            manifest = json.loads((root / "bundle.json").read_text(encoding="utf-8"))
            self.assertEqual("not-run", manifest["status"])
            self.assertEqual("pass", manifest["evidence"][0]["status"])
            self.assertEqual(["not-run"] * 4, [item["status"] for item in manifest["evidence"][1:]])
            scoped = (
                "validate",
                "--evidence-root",
                str(root),
                "--manifest",
                "bundle.json",
                "--require-category-pass",
                "performance",
            )

            missing_candidate = self.run_cli(root, *scoped)
            wrong_candidate = self.run_cli(root, *scoped, "--candidate", "f" * 40)
            accepted = self.run_cli(root, *scoped, "--candidate", CANDIDATE)

            self.assertEqual(1, missing_candidate.returncode)
            self.assertEqual(1, wrong_candidate.returncode)
            self.assertEqual(0, accepted.returncode, accepted.stdout + accepted.stderr)

            original_record = record_path.read_bytes()
            record = json.loads(original_record.decode("utf-8"))
            record["source"] = "wrong-source"
            record_path.write_text(json.dumps(record), encoding="utf-8")
            wrong_source = self.run_cli(root, *scoped, "--candidate", CANDIDATE)
            self.assertEqual(1, wrong_source.returncode)

            record_path.write_bytes(original_record)
            artifact_path.write_text("tampered\n", encoding="utf-8")
            artifact_tamper = self.run_cli(root, *scoped, "--candidate", CANDIDATE)
            self.assertEqual(1, artifact_tamper.returncode)

    def test_require_pass_requires_and_matches_full_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence: dict[str, str] = {}
            for category in bundle.CATEGORIES:
                record, _ = self.write_record(root, category)
                evidence[category] = record.name
            manifest = bundle.assemble(CANDIDATE, root, evidence)
            (root / "bundle.json").write_text(json.dumps(manifest), encoding="utf-8")
            common = (
                "validate",
                "--evidence-root",
                str(root),
                "--manifest",
                "bundle.json",
                "--require-pass",
            )

            missing = self.run_cli(root, *common)
            short = self.run_cli(root, *common, "--candidate", "abc1234")
            mismatch = self.run_cli(root, *common, "--candidate", "f" * 40)
            accepted = self.run_cli(root, *common, "--candidate", CANDIDATE)

            self.assertEqual(1, missing.returncode)
            self.assertEqual(1, short.returncode)
            self.assertEqual(1, mismatch.returncode)
            self.assertEqual(0, accepted.returncode, accepted.stdout + accepted.stderr)


if __name__ == "__main__":
    unittest.main()
