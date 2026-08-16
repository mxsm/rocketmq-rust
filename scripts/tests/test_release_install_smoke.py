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
import json
import os
import tempfile
import textwrap
import unittest
from pathlib import Path
from unittest import mock
from zipfile import ZIP_DEFLATED, ZipFile


ROOT = Path(__file__).resolve().parents[2]
RUNNER_PATH = ROOT / "scripts" / "release_install_smoke.py"
WINDOWS_TARGET = "x86_64-pc-windows-msvc"


def load_runner():
    spec = importlib.util.spec_from_file_location("release_install_smoke", RUNNER_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("unable to load release install smoke runner")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ReleaseInstallSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(RUNNER_PATH.is_file(), "the release install smoke runner must be implemented")
        self.runner = load_runner()

    def test_platform_result_mapping_and_required_profiles_are_closed(self) -> None:
        self.assertEqual("S01", self.runner.result_id_for_target("x86_64-unknown-linux-gnu"))
        self.assertEqual("S02", self.runner.result_id_for_target(WINDOWS_TARGET))
        self.assertEqual("S03", self.runner.result_id_for_target("x86_64-apple-darwin"))
        self.assertEqual(("single", "controller-3"), self.runner.parse_profiles("single,controller-3"))
        with self.assertRaisesRegex(ValueError, "exactly"):
            self.runner.parse_profiles("single")

    def test_windows_archive_smoke_emits_s02_from_verified_archive_and_driver(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate, archive = self._write_candidate_archive(root)
            driver = self._write_driver(root)
            output = root / "S02.json"
            with self._verified_archive(candidate), mock.patch.object(
                self.runner, "host_target", return_value=WINDOWS_TARGET
            ):
                exit_code = self.runner.main(
                    [
                        "--candidate-manifest",
                        str(candidate),
                        "--archive-id",
                        WINDOWS_TARGET,
                        "--profiles",
                        "single,controller-3",
                        "--result-id",
                        "S02",
                        "--case-driver",
                        str(driver),
                        "--output",
                        str(output),
                        "--timeout-seconds",
                        "2",
                    ]
                )
            self.assertEqual(0, exit_code)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual("S02", result["resultId"])
            self.assertEqual(WINDOWS_TARGET, result["target"])
            self.assertEqual(["single", "controller-3"], result["profiles"])
            self.assertEqual("passed", result["status"])
            self.assertEqual("not-executed", result["remotePublication"])
            self.assertEqual(archive.name, Path(result["archive"]).name)
            for evidence in result["evidence"].values():
                self.assertTrue((output.parent / evidence).is_file(), evidence)

    def test_missing_skipped_stale_partial_exit_timeout_and_escape_fail_closed(self) -> None:
        cases = [
            ("missing", "missing result"),
            ("skipped", "status must be passed"),
            ("stale", "candidateId"),
            ("partial", "every assertion must pass"),
            ("exit", "exited with 7"),
            ("timeout", "timed out"),
            ("escape", "evidence escapes"),
        ]
        for mode, message in cases:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                candidate, _archive = self._write_candidate_archive(root)
                driver = self._write_driver(root)
                output = root / "S02.json"
                with (
                    self._verified_archive(candidate),
                    mock.patch.object(self.runner, "host_target", return_value=WINDOWS_TARGET),
                    mock.patch.dict(os.environ, {"FAKE_INSTALL_MODE": mode}),
                ):
                    exit_code = self.runner.main(
                        [
                            "--candidate-manifest",
                            str(candidate),
                            "--archive-id",
                            WINDOWS_TARGET,
                            "--profiles",
                            "single,controller-3",
                            "--result-id",
                            "S02",
                            "--case-driver",
                            str(driver),
                            "--output",
                            str(output),
                            "--timeout-seconds",
                            "1",
                        ]
                    )
                self.assertEqual(1, exit_code)
                failure = json.loads(output.read_text(encoding="utf-8"))
                self.assertEqual("failed", failure["status"])
                self.assertIn(message, failure["error"])

    def test_wrong_host_target_and_result_id_fail_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate, _archive = self._write_candidate_archive(root)
            driver = self._write_driver(root)
            with mock.patch.object(self.runner, "host_target", return_value="x86_64-unknown-linux-gnu"):
                exit_code = self.runner.main(
                    [
                        "--candidate-manifest",
                        str(candidate),
                        "--archive-id",
                        WINDOWS_TARGET,
                        "--profiles",
                        "single,controller-3",
                        "--result-id",
                        "S01",
                        "--case-driver",
                        str(driver),
                    ]
                )
            self.assertEqual(1, exit_code)

    def test_installed_supported_surface_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate, _archive = self._write_candidate_archive(root, readme="BrokerContainer command is available")
            driver = self._write_driver(root)
            output = root / "S02.json"
            with self._verified_archive(candidate), mock.patch.object(
                self.runner, "host_target", return_value=WINDOWS_TARGET
            ):
                exit_code = self.runner.main(
                    [
                        "--candidate-manifest",
                        str(candidate),
                        "--archive-id",
                        WINDOWS_TARGET,
                        "--profiles",
                        "single,controller-3",
                        "--result-id",
                        "S02",
                        "--case-driver",
                        str(driver),
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(1, exit_code)
            self.assertIn("excluded support surface", json.loads(output.read_text(encoding="utf-8"))["error"])

    def test_unsafe_archive_path_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            candidate, archive = self._write_candidate_archive(root)
            with ZipFile(archive, "a", ZIP_DEFLATED) as value:
                value.writestr("../escape.txt", "escape")
            driver = self._write_driver(root)
            output = root / "S02.json"
            with self._verified_archive(candidate), mock.patch.object(
                self.runner, "host_target", return_value=WINDOWS_TARGET
            ):
                exit_code = self.runner.main(
                    [
                        "--candidate-manifest",
                        str(candidate),
                        "--archive-id",
                        WINDOWS_TARGET,
                        "--profiles",
                        "single,controller-3",
                        "--result-id",
                        "S02",
                        "--case-driver",
                        str(driver),
                        "--output",
                        str(output),
                    ]
                )
            self.assertEqual(1, exit_code)
            self.assertIn("unsafe path", json.loads(output.read_text(encoding="utf-8"))["error"])

    def _verified_archive(self, candidate: Path):
        def verify(_candidate, _archive, *, smoke):
            self.assertTrue(smoke)
            output = candidate.parent / "evidence" / WINDOWS_TARGET / "HOST_SMOKE.json"
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "candidate_id": "1.0.0-rc.1-local-attempt1",
                        "target": WINDOWS_TARGET,
                        "archive_manifest": f"archives/{WINDOWS_TARGET}.manifest.json",
                        "results": [
                            {"component": component, "exit_code": 0, "stdout": "verified"}
                            for component in ["namesrv", "broker", "controller", "proxy", "admin", "store-inspect"]
                        ],
                        "status": "passed",
                    }
                ),
                encoding="utf-8",
            )
            return output

        return mock.patch.object(self.runner.archive_verifier, "verify_archive", side_effect=verify)

    @staticmethod
    def _write_candidate_archive(root: Path, *, readme: str = "Community distribution") -> tuple[Path, Path]:
        candidate_root = root / "candidate"
        archives = candidate_root / "archives"
        archives.mkdir(parents=True)
        candidate = candidate_root / "CANDIDATE_RUN.json"
        candidate.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "candidate_id": "1.0.0-rc.1-local-attempt1",
                    "version": "1.0.0-rc.1",
                    "run_id": "local",
                    "attempt": 1,
                    "candidate_root": str(candidate_root),
                }
            ),
            encoding="utf-8",
        )
        archive = archives / f"rocketmq-rust-1.0.0-rc.1-{WINDOWS_TARGET}.zip"
        package = "rocketmq-rust-1.0.0-rc.1"
        with ZipFile(archive, "w", ZIP_DEFLATED) as value:
            value.writestr(f"{package}/README.md", readme)
            for service in ("namesrv", "broker", "controller", "proxy"):
                value.writestr(f"{package}/conf/{service}.toml", "[service]\nenabled = true\n")
            for binary in (
                "rocketmq-namesrv-rust.exe",
                "rocketmq-broker-rust.exe",
                "rocketmq-controller-rust.exe",
                "rocketmq-proxy-rust.exe",
                "rocketmq-admin-cli.exe",
                "rocketmq-store-inspect.exe",
            ):
                value.writestr(f"{package}/bin/{binary}", "binary")
        return candidate, archive

    @staticmethod
    def _write_driver(root: Path) -> Path:
        driver = root / "fake_install_driver.py"
        driver.write_text(
            textwrap.dedent(
                """
                import argparse
                import json
                import os
                import time
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("--contract", type=Path, required=True)
                parser.add_argument("--candidate-manifest", type=Path, required=True)
                parser.add_argument("--package-root", type=Path, required=True)
                parser.add_argument("--result", type=Path, required=True)
                parser.add_argument("--work-dir", type=Path, required=True)
                args = parser.parse_args()
                mode = os.environ.get("FAKE_INSTALL_MODE", "passed")
                if mode == "timeout":
                    time.sleep(5)
                if mode == "exit":
                    raise SystemExit(7)
                if mode == "missing":
                    raise SystemExit(0)
                contract = json.loads(args.contract.read_text(encoding="utf-8"))
                candidate = json.loads(args.candidate_manifest.read_text(encoding="utf-8"))
                evidence_root = args.work_dir / "evidence"
                evidence_root.mkdir(parents=True, exist_ok=True)
                for name in contract["requiredEvidence"]:
                    (evidence_root / f"{name}.txt").write_text(name, encoding="utf-8")
                evidence = {
                    name: f"work/evidence/{name}.txt" for name in contract["requiredEvidence"]
                }
                if mode == "escape":
                    outside = args.work_dir.parent.parent / "outside.txt"
                    outside.write_text("escape", encoding="utf-8")
                    evidence[contract["requiredEvidence"][0]] = "../../outside.txt"
                assertions = {name: True for name in contract["requiredAssertions"]}
                if mode == "partial":
                    assertions[contract["requiredAssertions"][0]] = False
                result = {
                    "schemaVersion": 1,
                    "resultId": contract["resultId"],
                    "candidateId": "stale" if mode == "stale" else candidate["candidate_id"],
                    "version": candidate["version"],
                    "runId": candidate["run_id"],
                    "attempt": candidate["attempt"],
                    "target": contract["target"],
                    "profiles": contract["profiles"],
                    "status": "skipped" if mode == "skipped" else "passed",
                    "assertions": assertions,
                    "evidence": evidence,
                    "remotePublication": "not-executed",
                }
                args.result.write_text(json.dumps(result), encoding="utf-8")
                """
            ),
            encoding="utf-8",
        )
        return driver


if __name__ == "__main__":
    unittest.main()
