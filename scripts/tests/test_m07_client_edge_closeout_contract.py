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
import subprocess
import sys
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"
GUARD = ROOT / "scripts" / "architecture_dependency_guard.py"
HANDOFF = (
    ROOT
    / "docs"
    / "plans"
    / "architecture-refactor-migration"
    / "phase-2-core-boundaries"
    / "07-client-edge-closeout-handoff.md"
)

EXPECTED_MANIFEST_ALLOWLIST = {
    (
        "rocketmq-admin-core",
        "rocketmq-client-rust",
        "normal",
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml",
        "rocketmq_client_rust",
    ),
    (
        "rocketmq-proxy-cluster",
        "rocketmq-client-rust",
        "normal",
        "rocketmq-proxy-cluster/Cargo.toml",
        "rocketmq_client_rust",
    ),
    (
        "rocketmq-example",
        "rocketmq-client-rust",
        "dev",
        "rocketmq-example/Cargo.toml",
        "rocketmq_client_rust",
    ),
}
EXPECTED_SOURCE_ALLOWLIST = {
    (
        "rocketmq-admin-core",
        "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/client_adapter/",
        ("rocketmq_client_rust",),
    ),
    ("rocketmq-proxy-cluster", "rocketmq-proxy-cluster/src/", ("rocketmq_client_rust",)),
    ("rocketmq-example", "rocketmq-example/examples/", ("rocketmq_client_rust",)),
}
EXPECTED_PROXY_SOURCE_PATHS = {
    "rocketmq-proxy/src/cluster.rs",
    "rocketmq-proxy/src/remoting.rs",
}


class ClientEdgeCloseoutContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = json.loads(POLICY.read_text(encoding="utf-8"))
        cls.baseline = json.loads(BASELINE.read_text(encoding="utf-8"))

    def test_target_client_allowlist_is_exact(self) -> None:
        client_policy = self.policy["client_policy"]

        manifest_allowlist = {
            (entry["caller"], entry["target"], entry["kind"], entry["path"], entry["alias"])
            for entry in client_policy["target_manifest_allowlist"]
        }
        source_allowlist = {
            (entry["caller"], entry["path_prefix"], tuple(entry["aliases"]))
            for entry in client_policy["target_source_allowlist"]
        }

        self.assertEqual(EXPECTED_MANIFEST_ALLOWLIST, manifest_allowlist)
        self.assertEqual(EXPECTED_SOURCE_ALLOWLIST, source_allowlist)

    def test_only_the_example_standalone_manifest_directly_uses_client(self) -> None:
        roots = self.policy["roots"]["standalone_manifests"]
        consumers = set()
        for relative in roots:
            manifest = tomllib.loads((ROOT / relative).read_text(encoding="utf-8"))
            for kind in ("dependencies", "dev-dependencies", "build-dependencies"):
                for alias, value in manifest.get(kind, {}).items():
                    package = value.get("package", alias) if isinstance(value, dict) else alias
                    if package == "rocketmq-client-rust":
                        consumers.add(relative)

        self.assertEqual({"rocketmq-example/Cargo.toml"}, consumers)

    def test_proxy_client_bypass_has_an_exact_m08_ledger(self) -> None:
        manifest_entries = [
            item
            for item in self.baseline["manifest_exceptions"]
            if item.get("rule") is None
            and item["caller"] == "rocketmq-proxy"
            and item["target"] == "rocketmq-client-rust"
        ]
        self.assertEqual(1, len(manifest_entries))
        self.assertEqual("proxy", manifest_entries[0]["owner"])
        self.assertEqual("M08", manifest_entries[0]["remove_by"])
        self.assertEqual(1, manifest_entries[0]["count"])

        source_entries = [
            item
            for item in self.baseline["source_exceptions"]
            if item["path"].startswith("rocketmq-proxy/")
        ]
        self.assertEqual(EXPECTED_PROXY_SOURCE_PATHS, {item["path"] for item in source_entries})
        self.assertEqual({"proxy"}, {item["owner"] for item in source_entries})
        self.assertEqual({"M08"}, {item["remove_by"] for item in source_entries})
        self.assertEqual(13, sum(item["count"] for item in source_entries))
        self.assertEqual(
            1,
            len([item for item in self.baseline["manifest_exceptions"] if item.get("rule") is None]),
        )
        self.assertEqual(source_entries, self.baseline["source_exceptions"])

    def test_target_guard_reports_client_violations_only_for_proxy(self) -> None:
        completed = subprocess.run(
            [
                sys.executable,
                str(GUARD),
                "--mode",
                "target",
                "--allow-missing-planned-crates",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(1, completed.returncode, completed.stdout + completed.stderr)
        findings = [
            line
            for line in completed.stdout.splitlines()
            if "rule=client-manifest-allowlist" in line or "rule=client-source-allowlist" in line
        ]
        self.assertEqual(14, len(findings), completed.stdout)
        self.assertTrue(all("caller=rocketmq-proxy " in line for line in findings), completed.stdout)
        self.assertEqual(1, sum("rule=client-manifest-allowlist" in line for line in findings))
        self.assertEqual(13, sum("rule=client-source-allowlist" in line for line in findings))

    def test_forbidden_normal_closures_cannot_reach_client(self) -> None:
        expected_callers = {
            "rocketmq-broker",
            "rocketmq-namesrv",
            "rocketmq-proxy-core",
            "rocketmq-proxy-local",
            "rocketmq-common",
            "rocketmq-remoting",
        }
        matching_rules = [
            rule
            for rule in self.policy["closure_rules"]
            if set(rule["callers"]) == expected_callers
        ]

        self.assertEqual(1, len(matching_rules))
        self.assertEqual(["rocketmq-client-rust"], matching_rules[0]["forbidden_targets"])

    def test_m08_handoff_freezes_owned_pull_outcome_and_proxy_debt(self) -> None:
        handoff = HANDOFF.read_text(encoding="utf-8")

        self.assertIn("PullOutcome<MessageExt>", handoff)
        self.assertIn("`rocketmq-proxy/src/cluster.rs` | 12", handoff)
        self.assertIn("`rocketmq-proxy/src/remoting.rs` | 1", handoff)
        self.assertIn("owner=`proxy`、remove_by=`M08`", handoff)
        self.assertIn("Client manifest 1、Client source 13", handoff)


if __name__ == "__main__":
    unittest.main()
