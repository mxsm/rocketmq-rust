# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from scripts.tests.release_archive_test_support import create_candidate
from scripts.tests.release_test_support import load_module, read_json, write_json


class CandidatePartialMergeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.merger = load_module("merge_candidate_partials", "distribution/merge_candidate_partials.py")

    def test_three_target_partials_merge_by_explicit_target(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            candidate = create_candidate(base)
            value = read_json(candidate)
            download = base / "download"
            targets = [
                "x86_64-unknown-linux-gnu",
                "x86_64-pc-windows-msvc",
                "x86_64-apple-darwin",
            ]
            for target in targets:
                bundle = download / target
                archive = bundle / "archives" / f"{target}.zip"
                archive.parent.mkdir(parents=True, exist_ok=True)
                archive.write_bytes(target.encode())
                started = bundle / "events" / target / "build.started.json"
                completed = bundle / "events" / target / "build.completed.json"
                context = bundle / "contexts" / target / "context.json"
                write_json(started, {"status": "started"})
                write_json(completed, {"status": "passed", "exit_code": 0})
                write_json(context, {"worker_id": f"release-{target}"})
                write_json(
                    bundle / "partials" / f"CANDIDATE_PARTIAL.{target}.json",
                    {
                        "schema_version": 1,
                        "candidate_id": value["candidate_id"],
                        "version": value["version"],
                        "run_id": value["run_id"],
                        "attempt": value["attempt"],
                        "target": target,
                        "worker_id": f"release-{target}",
                        "sealed": True,
                        "artifacts": [
                            {
                                "id": identifier,
                                "kind": identifier,
                                "path": f"archives/{target}.zip",
                            }
                            for identifier in (
                                "binary-admin",
                                "binary-broker",
                                "binary-controller",
                                "binary-namesrv",
                                "binary-proxy",
                                "binary-store-inspect",
                                "archive",
                                "archive-manifest",
                                "common-inputs",
                                "component-sbom",
                                "host-smoke",
                            )
                        ],
                        "events": [
                            {
                                "id": "event",
                                "started": f"events/{target}/build.started.json",
                                "completed": f"events/{target}/build.completed.json",
                            }
                        ],
                        "execution_contexts": [
                            {"id": "context", "path": f"contexts/{target}/context.json"}
                        ],
                    },
                )

            output = self.merger.merge_partials(candidate, download, targets)

            merged = read_json(output)
            self.assertEqual(targets, merged["targets"])
            self.assertEqual(33, len(merged["artifacts"]))
            self.assertEqual("not-executed", merged["remote_publication"])


if __name__ == "__main__":
    unittest.main()
