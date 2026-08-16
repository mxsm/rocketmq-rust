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

import importlib.util
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GUARD_PATH = ROOT / "scripts" / "final_candidate_delta_guard.py"
POLICY_PATH = ROOT / "distribution" / "final-candidate-delta-policy.json"


def load_guard():
    spec = importlib.util.spec_from_file_location("final_candidate_delta_guard", GUARD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load final candidate delta guard")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FinalCandidateDeltaGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(GUARD_PATH.is_file(), "the final candidate delta guard must be implemented")
        self.assertTrue(POLICY_PATH.is_file(), "the final candidate delta policy must be implemented")
        self.guard = load_guard()

    def test_approved_version_and_release_metadata_changes_pass_without_digests(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            output = fixture["root"] / "FINAL_DELTA.json"
            exit_code = self.guard.main(
                [
                    "--candidate-manifest",
                    str(fixture["final_manifest"]),
                    "--parent-manifest",
                    str(fixture["parent_manifest"]),
                    "--source-root",
                    str(fixture["final_source"]),
                    "--policy",
                    str(POLICY_PATH),
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(0, exit_code)
            report = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual("passed", report["status"])
            self.assertEqual(6, report["comparedFiles"])
            self.assertEqual("not-executed", report["remotePublication"])
            serialized = json.dumps(report).lower()
            for forbidden in ("sha256", "checksum", "content_hash"):
                self.assertNotIn(forbidden, serialized)

    def test_rust_source_change_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            (fixture["final_source"] / "rocketmq-client" / "src" / "lib.rs").write_text(
                "pub fn changed() {}\n", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "byte content changed"):
                self._compare(fixture)

    def test_unapproved_cargo_field_change_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            cargo = fixture["final_source"] / "Cargo.toml"
            cargo.write_text(cargo.read_text(encoding="utf-8").replace('description = "core"', 'description = "changed"'), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unapproved TOML field"):
                self._compare(fixture)

    def test_added_or_deleted_source_paths_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            (fixture["final_source"] / "new.rs").write_text("fn new_file() {}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source denominator drift"):
                self._compare(fixture)

        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            (fixture["final_source"] / "rocketmq-client" / "src" / "lib.rs").unlink()
            with self.assertRaisesRegex(ValueError, "source denominator drift"):
                self._compare(fixture)

    def test_allowed_fields_still_require_exact_rc_to_final_transition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            cargo = fixture["final_source"] / "Cargo.toml"
            cargo.write_text(cargo.read_text(encoding="utf-8").replace('version = "1.0.0"', 'version = "1.0.1"'), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "version transition"):
                self._compare(fixture)

    def test_parent_must_be_direct_successful_sealed_rc(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            parent = json.loads(fixture["parent_manifest"].read_text(encoding="utf-8"))
            parent["sealed"] = False
            parent["state"] = "staged-rc"
            parent["outcome"] = None
            fixture["parent_manifest"].write_text(json.dumps(parent), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "successful sealed RC"):
                self._compare(fixture)

    def test_same_size_parent_snapshot_and_final_tamper_is_rejected_against_source_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            relative = Path("rocketmq-client/src/lib.rs")
            tampered = "pub fn hacked() {}\n"
            self.assertEqual(
                len(tampered.encode()),
                len((fixture["final_source"] / relative).read_bytes()),
            )
            (fixture["parent_source"] / relative).write_bytes(tampered.encode())
            (fixture["final_source"] / relative).write_bytes(tampered.encode())
            with self.assertRaisesRegex(ValueError, "snapshot content differs"):
                self._compare(fixture)

    def _compare(self, fixture):
        return self.guard.compare_candidate(
            fixture["final_manifest"],
            fixture["parent_manifest"],
            fixture["final_source"],
            POLICY_PATH,
        )

    @staticmethod
    def _fixture(root: Path):
        parent_root = root / "parent"
        final_root = root / "final"
        snapshot_root = parent_root / "source-snapshot"
        parent_source = snapshot_root / "source"
        final_source = root / "final-source"
        old = "1.0.0-rc.2"
        new = "1.0.0"
        files = {
            "Cargo.toml": (
                f'[workspace.package]\nversion = "{old}"\ndescription = "core"\n\n'
                f'[workspace.dependencies]\nrocketmq-error = {{ version = "{old}", path = "rocketmq-error" }}\n'
            ),
            "Cargo.lock": (
                'version = 4\n\n[[package]]\nname = "rocketmq-error"\n'
                f'version = "{old}"\n\n[[package]]\nname = "serde"\nversion = "1.0.0"\n'
                'source = "registry+https://github.com/rust-lang/crates.io-index"\n'
            ),
            "docker/core-container-policy.json": json.dumps(
                {"schema_version": 1, "release_version": old, "services": ["rocketmq-broker"]}, indent=2
            )
            + "\n",
            "distribution/helm/rocketmq-rust-core/Chart.yaml": (
                f"apiVersion: v2\nname: rocketmq-rust-core\nversion: {old}\nappVersion: \"{old}\"\n"
            ),
            "distribution/helm/rocketmq-rust-core/values.yaml": (
                f'global:\n  candidateVersion: "{old}"\n  imageRegistry: "ghcr.io/mxsm/rocketmq-rust"\n'
            ),
            "rocketmq-client/src/lib.rs": "pub fn stable() {}\n",
        }
        records = []
        for relative, content in files.items():
            parent_path = parent_source / relative
            final_path = final_source / relative
            parent_path.parent.mkdir(parents=True, exist_ok=True)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            parent_path.write_text(content, encoding="utf-8", newline="\n")
            final_path.write_text(content.replace(old, new), encoding="utf-8", newline="\n")
            records.append({"path": relative, "type": "file", "size": len(content.encode())})

        snapshot = snapshot_root / "SOURCE_SNAPSHOT.json"
        bundle = root / "RC_SOURCE.tar"
        source_manifest = {
            "schema_version": 1,
            "version": old,
            "run_id": "rc2-run",
            "attempt": 1,
            "files": records,
        }
        with tarfile.open(bundle, "w") as archive:
            manifest_bytes = json.dumps(source_manifest).encode()
            manifest_info = tarfile.TarInfo("CORE_SOURCE_MANIFEST.json")
            manifest_info.size = len(manifest_bytes)
            archive.addfile(manifest_info, io.BytesIO(manifest_bytes))
            for relative, content in files.items():
                payload = content.encode()
                info = tarfile.TarInfo(f"source/{relative}")
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
        snapshot.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_id": "rc2",
                    "version": old,
                    "run_id": "rc2-run",
                    "attempt": 1,
                    "source_bundle": str(bundle),
                    "files": records,
                    "sealed": True,
                }
            ),
            encoding="utf-8",
        )
        parent_manifest = parent_root / "CANDIDATE_RUN.json"
        final_manifest = final_root / "CANDIDATE_RUN.json"
        series = root / "RELEASE_SERIES.json"
        series.write_text("{}", encoding="utf-8")

        def candidate(kind, version, candidate_id, candidate_root, state, sealed, outcome, ordinal, parent):
            return {
                "schema_version": 1,
                "candidate_id": candidate_id,
                "candidate_kind": kind,
                "version": version,
                "run_id": f"{candidate_id}-run",
                "attempt": 1,
                "ordinal": ordinal,
                "candidate_root": str(candidate_root),
                "series_manifest": str(series),
                "series_id": "community-v1",
                "series_generation": ordinal,
                "parent_manifest": str(parent) if parent else None,
                "state": state,
                "sealed": sealed,
                "outcome": outcome,
                "rejection_reason": None,
                "known_issues": [],
                "generation": 1,
                "build_source_bundle": str(bundle) if kind == "rc" else None,
                "source_snapshot": str(snapshot) if kind == "rc" else None,
                "artifact_index": None,
                "evidence_index": None,
                "event_index": None,
                "execution_context_index": None,
                "creation_operation_id": "fixture",
                "created_at": "2026-08-16T00:00:00Z",
                "updated_at": "2026-08-16T00:00:00Z",
            }

        parent_root.mkdir(exist_ok=True)
        final_root.mkdir(exist_ok=True)
        parent_manifest.write_text(
            json.dumps(candidate("rc", old, "rc2", parent_root, "rc-candidate-ready", True, "success", 2, None)),
            encoding="utf-8",
        )
        final_manifest.write_text(
            json.dumps(candidate("final", new, "final", final_root, "development", False, None, 3, parent_manifest)),
            encoding="utf-8",
        )
        return {
            "root": root,
            "parent_manifest": parent_manifest,
            "final_manifest": final_manifest,
            "final_source": final_source,
            "parent_source": parent_source,
        }


if __name__ == "__main__":
    unittest.main()
