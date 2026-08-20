# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
MODULE = ROOT / "distribution" / "merge_handoff_platform_results.py"


def load_module():
    spec = importlib.util.spec_from_file_location("merge_handoff_platform_results_test", MODULE)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load handoff platform merger")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class HandoffPlatformResultMergeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(MODULE.is_file(), "handoff platform merger must be implemented")
        self.merger = load_module()

    def test_merges_three_self_contained_platform_bundles(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._candidate(root)
            bundles = root / "bundles"
            for result_id, platform, target in self._platforms():
                self._bundle(bundles / result_id, result_id, platform, target)

            output = self.merger.merge_platform_results(
                manifest,
                bundles,
                root / "evidence",
                root / "events",
                root / "contexts",
            )

            value = json.loads(output.read_text(encoding="utf-8"))
            self.assertEqual([item[0] for item in self._platforms()], value["result_ids"])
            self.assertEqual(3, len(list((root / "evidence").glob("H01-*.json"))))
            self.assertEqual(6, len(list((root / "events").glob("*.json"))))
            self.assertEqual(3, len(list((root / "contexts").glob("*.json"))))

    def test_missing_platform_or_worker_mismatch_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._candidate(root)
            bundles = root / "bundles"
            for result_id, platform, target in self._platforms()[:2]:
                self._bundle(bundles / result_id, result_id, platform, target)
            with self.assertRaisesRegex(ValueError, "exactly one bundle"):
                self.merger.merge_platform_results(
                    manifest, bundles, root / "evidence", root / "events", root / "contexts"
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._candidate(root)
            bundles = root / "bundles"
            for result_id, platform, target in self._platforms():
                bundle = self._bundle(bundles / result_id, result_id, platform, target)
                if result_id == "H01-WINDOWS":
                    context = bundle / "contexts" / "worker-H01-WINDOWS.json"
                    value = json.loads(context.read_text(encoding="utf-8"))
                    value["worker_id"] = "another-worker"
                    context.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "worker identity"):
                self.merger.merge_platform_results(
                    manifest, bundles, root / "evidence", root / "events", root / "contexts"
                )

    def test_missing_binary_smoke_results_are_rejected(self) -> None:
        """A forged H01 pass without six process results must not enter canonical evidence."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._candidate(root)
            bundles = root / "bundles"
            for result_id, platform, target in self._platforms():
                bundle = self._bundle(bundles / result_id, result_id, platform, target)
                if result_id == "H01-WINDOWS":
                    result_path = bundle / f"{result_id}.json"
                    value = json.loads(result_path.read_text(encoding="utf-8"))
                    value.pop("archive_smoke_results")
                    result_path.write_text(json.dumps(value), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "binary smoke denominator"):
                self.merger.merge_platform_results(
                    manifest, bundles, root / "evidence", root / "events", root / "contexts"
                )

    def test_failed_or_forged_binary_smoke_is_rejected(self) -> None:
        """A non-zero process or forged build metadata must invalidate H01 evidence."""

        for mutation in ("exit-code", "metadata"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                manifest = self._candidate(root)
                bundles = root / "bundles"
                for result_id, platform, target in self._platforms():
                    bundle = self._bundle(bundles / result_id, result_id, platform, target)
                    if result_id == "H01-LINUX":
                        result_path = bundle / f"{result_id}.json"
                        value = json.loads(result_path.read_text(encoding="utf-8"))
                        broker = next(
                            item for item in value["archive_smoke_results"] if item["component"] == "broker"
                        )
                        if mutation == "exit-code":
                            broker["exit_code"] = 9
                        else:
                            broker["stdout"] = broker["stdout"].replace(
                                "effective_features=", "effective_features=forged,"
                            )
                        result_path.write_text(json.dumps(value), encoding="utf-8")

                with self.assertRaisesRegex(ValueError, "binary smoke result"):
                    self.merger.merge_platform_results(
                        manifest, bundles, root / "evidence", root / "events", root / "contexts"
                    )

    def test_archive_path_must_match_the_target_manifest(self) -> None:
        """A result must bind the command output to the frozen target archive path."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._candidate(root)
            bundles = root / "bundles"
            for result_id, platform, target in self._platforms():
                bundle = self._bundle(bundles / result_id, result_id, platform, target)
                if result_id == "H01-MACOS":
                    result_path = bundle / f"{result_id}.json"
                    value = json.loads(result_path.read_text(encoding="utf-8"))
                    value["archive"] = "archives/forged.tar.gz"
                    result_path.write_text(json.dumps(value), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "archive path mismatch"):
                self.merger.merge_platform_results(
                    manifest, bundles, root / "evidence", root / "events", root / "contexts"
                )

    @staticmethod
    def _platforms():
        return [
            ("H01-LINUX", "linux", "x86_64-unknown-linux-gnu"),
            ("H01-WINDOWS", "windows", "x86_64-pc-windows-msvc"),
            ("H01-MACOS", "macos", "x86_64-apple-darwin"),
        ]

    @staticmethod
    def _candidate(root: Path) -> Path:
        candidate_root = root / "candidate"
        candidate_root.mkdir()
        series = root / "RELEASE_SERIES.json"
        series.write_text("{}", encoding="utf-8")
        manifest = candidate_root / "CANDIDATE_RUN.json"
        value = {
            "schema_version": 1,
            "candidate_id": "final-1",
            "candidate_kind": "final",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "ordinal": 3,
            "candidate_root": str(candidate_root),
            "series_manifest": str(series),
            "series_id": "community-v1",
            "series_generation": 3,
            "parent_manifest": str(root / "parent.json"),
            "state": "ga-candidate-ready",
            "sealed": False,
            "outcome": "success",
            "rejection_reason": None,
            "known_issues": [],
            "generation": 4,
            "build_source_bundle": None,
            "source_snapshot": None,
            "artifact_index": None,
            "evidence_index": None,
            "event_index": None,
            "execution_context_index": None,
            "creation_operation_id": "fixture",
            "created_at": "2026-08-16T00:00:00Z",
            "updated_at": "2026-08-16T00:00:00Z"
        }
        manifest.write_text(json.dumps(value), encoding="utf-8")
        return manifest

    @staticmethod
    def _bundle(root: Path, result_id: str, platform: str, target: str) -> Path:
        worker = f"worker-{result_id}"
        (root / "events").mkdir(parents=True)
        (root / "contexts").mkdir()
        identity = {
            "candidate_id": "final-1",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "worker_id": worker,
        }
        layout = json.loads(
            (ROOT / "distribution" / "release-layout.json").read_text(encoding="utf-8")
        )
        smoke_results = []
        for binary in layout["binaries"]:
            smoke_results.append(
                {
                    "component": binary["id"],
                    "exit_code": 0,
                    "stdout": (
                        f"component={binary['id']}\n"
                        "version=1.0.0\n"
                        f"artifact_id=final-1.{target}.{binary['id']}\n"
                        f"requested_features={','.join(binary['requested_features'])}\n"
                        f"effective_features={','.join(binary['effective_features'])}\n"
                    ),
                }
            )
        extension = ".zip" if platform == "windows" else ".tar.gz"
        result = {
            "schema_version": 1,
            **identity,
            "result_id": result_id,
            "platform": platform,
            "target": target,
            "archive_id": f"final-1.{target}.archive",
            "status": "passed",
            "skipped": False,
            "assertions": [{"name": "archive-install-smoke", "status": "passed"}],
            "archive": f"archives/rocketmq-rust-1.0.0-{target}{extension}",
            "archive_manifest": f"archives/rocketmq-rust-1.0.0-{target}.manifest.json",
            "archive_smoke_results": smoke_results,
        }
        (root / f"{result_id}.json").write_text(json.dumps(result), encoding="utf-8")
        started = {"schema_version": 1, **identity, "route_id": result_id, "context_path": f"contexts/{worker}.json"}
        completed = {**started, "exit_code": 0}
        (root / "events" / f"{result_id}.started.json").write_text(json.dumps(started), encoding="utf-8")
        (root / "events" / f"{result_id}.completed.json").write_text(json.dumps(completed), encoding="utf-8")
        context = {
            "schema_version": 1,
            **identity,
            "publish_input": False,
            "publishing_credentials_provided": False,
        }
        (root / "contexts" / f"{worker}.json").write_text(json.dumps(context), encoding="utf-8")
        return root


if __name__ == "__main__":
    unittest.main()
