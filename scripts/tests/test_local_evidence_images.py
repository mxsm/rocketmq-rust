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
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POWERSHELL = shutil.which("pwsh") or shutil.which("powershell")
SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")


def run_powershell(script: str, *arguments: str) -> subprocess.CompletedProcess[str]:
    if POWERSHELL is None:
        raise unittest.SkipTest("PowerShell is unavailable")
    return subprocess.run(
        [
            POWERSHELL,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts" / script),
            *arguments,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )


def git(*arguments: str, cwd: Path) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=cwd,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def create_checkout(path: Path, marker: str) -> str:
    path.mkdir()
    git("init", "--quiet", cwd=path)
    git("config", "user.name", "RocketMQ Rust Test", cwd=path)
    git("config", "user.email", "test@rocketmq.apache.org", cwd=path)
    (path / "marker.txt").write_text(marker + "\n", encoding="utf-8")
    git("add", "marker.txt", cwd=path)
    git("commit", "--quiet", "-m", marker, cwd=path)
    return git("rev-parse", "HEAD", cwd=path)


class LocalEvidenceImagesTest(unittest.TestCase):
    def test_bootstrap_validate_reports_pinned_tools(self) -> None:
        result = run_powershell("bootstrap-local-message-path-evidence.ps1", "-Mode", "Validate")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("name=kind version=v0.27.0", result.stdout)
        self.assertIn("name=kubectl version=v1.32.2", result.stdout)
        self.assertIn("LOCAL_EVIDENCE_BOOTSTRAP_VALID", result.stdout)

    def test_plan_requires_distinct_clean_checkouts(self) -> None:
        target = ROOT / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as temporary:
            temporary_root = Path(temporary)
            baseline = temporary_root / "baseline"
            candidate = temporary_root / "candidate"
            baseline_commit = create_checkout(baseline, "baseline")
            candidate_commit = create_checkout(candidate, "candidate")

            result = run_powershell(
                "prepare-local-evidence-images.ps1",
                "-Mode",
                "Plan",
                "-BaselineRoot",
                str(baseline),
                "-CandidateRoot",
                str(candidate),
            )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(f"baseline={baseline_commit}", result.stdout)
        self.assertIn(f"candidate={candidate_commit}", result.stdout)
        self.assertIn("registry=127.0.0.1:5001", result.stdout)

    def test_validate_binds_maps_to_provenance_hashes(self) -> None:
        target = ROOT / "target"
        target.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=target) as temporary:
            output = Path(temporary)
            baseline_map = {
                service: f"127.0.0.1:5001/evidence/baseline-{service}@sha256:{'1' * 64}"
                for service in SERVICES
            }
            candidate_map = {
                service: f"127.0.0.1:5001/evidence/candidate-{service}@sha256:{'2' * 64}"
                for service in SERVICES
            }
            baseline_path = output / "baseline-images.json"
            candidate_path = output / "candidate-images.json"
            baseline_path.write_text(json.dumps(baseline_map, indent=2) + "\n", encoding="utf-8")
            candidate_path.write_text(json.dumps(candidate_map, indent=2) + "\n", encoding="utf-8")
            provenance = {
                "schema_version": 1,
                "artifact_kind": "rocketmq_local_evidence_image_provenance",
                "baseline": {
                    "commit": "1" * 40,
                    "image_map_sha256": "sha256:" + hashlib.sha256(baseline_path.read_bytes()).hexdigest(),
                },
                "candidate": {
                    "commit": "2" * 40,
                    "image_map_sha256": "sha256:" + hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
                },
            }
            (output / "image-provenance.json").write_text(
                json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
            )

            valid = run_powershell(
                "prepare-local-evidence-images.ps1",
                "-Mode",
                "Validate",
                "-OutputDirectory",
                str(output),
            )

            incomplete_map = dict(candidate_map)
            incomplete_map.pop("mcp")
            candidate_path.write_text(json.dumps(incomplete_map) + "\n", encoding="utf-8")
            provenance["candidate"]["image_map_sha256"] = (
                "sha256:" + hashlib.sha256(candidate_path.read_bytes()).hexdigest()
            )
            (output / "image-provenance.json").write_text(
                json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
            )
            incomplete = run_powershell(
                "prepare-local-evidence-images.ps1",
                "-Mode",
                "Validate",
                "-OutputDirectory",
                str(output),
            )

            mutable_map = {**candidate_map, "broker": "127.0.0.1:5001/evidence/broker:latest"}
            candidate_path.write_text(json.dumps(mutable_map) + "\n", encoding="utf-8")
            provenance["candidate"]["image_map_sha256"] = (
                "sha256:" + hashlib.sha256(candidate_path.read_bytes()).hexdigest()
            )
            (output / "image-provenance.json").write_text(
                json.dumps(provenance, indent=2) + "\n", encoding="utf-8"
            )
            mutable = run_powershell(
                "prepare-local-evidence-images.ps1",
                "-Mode",
                "Validate",
                "-OutputDirectory",
                str(output),
            )

        self.assertEqual(valid.returncode, 0, valid.stderr)
        self.assertIn("LOCAL_EVIDENCE_IMAGE_MAPS_VALID", valid.stdout)
        self.assertNotEqual(incomplete.returncode, 0)
        self.assertNotEqual(mutable.returncode, 0)

    def test_output_directory_cannot_escape_repository(self) -> None:
        with tempfile.TemporaryDirectory() as outside:
            result = run_powershell(
                "prepare-local-evidence-images.ps1",
                "-Mode",
                "Validate",
                "-OutputDirectory",
                outside,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must remain inside the current repository", result.stderr)


if __name__ == "__main__":
    unittest.main()
