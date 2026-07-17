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
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
BASELINE = ROOT / "scripts" / "architecture-dependency-baseline.json"
EVIDENCE = (
    ROOT
    / "docs"
    / "plans"
    / "architecture-refactor-migration"
    / "phase-2-core-boundaries"
    / "09-facade-purity-closeout-evidence.md"
)


def manifest(relative: str) -> dict:
    return tomllib.loads((ROOT / relative / "Cargo.toml").read_text(encoding="utf-8"))


def production_source(relative: str) -> str:
    source_root = ROOT / relative / "src"
    return "\n".join(path.read_text(encoding="utf-8") for path in source_root.rglob("*.rs"))


class FacadePurityCloseoutTests(unittest.TestCase):
    def test_m09_02_inputs_are_resolved_without_extending_the_ledger(self) -> None:
        policy = json.loads(POLICY.read_text(encoding="utf-8"))
        baseline = json.loads(BASELINE.read_text(encoding="utf-8"))

        self.assertEqual(
            {
                "rocketmq-remoting": {"rocketmq-error", "rocketmq-runtime"},
                "rocketmq-store": {"rocketmq-error", "rocketmq-runtime", "rocketmq-observability"},
                "rocketmq-proxy": {"rocketmq-error", "rocketmq-model"},
                "rocketmq-namesrv": {"rocketmq-controller"},
            },
            {
                package: set(policy["target_dag"][package]) & promoted
                for package, promoted in {
                    "rocketmq-remoting": {"rocketmq-error", "rocketmq-runtime"},
                    "rocketmq-store": {"rocketmq-error", "rocketmq-runtime", "rocketmq-observability"},
                    "rocketmq-proxy": {"rocketmq-error", "rocketmq-model"},
                    "rocketmq-namesrv": {"rocketmq-controller"},
                }.items()
            },
        )
        self.assertFalse(
            any(item["remove_by"] == "M09-02" for item in baseline["compatibility_manifest_exceptions"])
        )
        self.assertEqual(35, len(baseline["compatibility_manifest_exceptions"]))

    def test_filter_and_remoting_remove_non_owner_dependencies(self) -> None:
        filter_manifest = manifest("rocketmq-filter")
        filter_source = production_source("rocketmq-filter")
        remoting_manifest = manifest("rocketmq-remoting")
        remoting_source = production_source("rocketmq-remoting")

        self.assertNotIn("rocketmq-common", filter_manifest["dependencies"])
        self.assertIn("rocketmq-protocol", filter_manifest["dependencies"])
        self.assertNotIn("rocketmq_common", filter_source)
        self.assertIn("rocketmq_protocol::common::filter::expression_type::ExpressionType", filter_source)

        self.assertNotIn("rocketmq-macros", remoting_manifest["dependencies"])
        self.assertNotIn("rocketmq_macros", remoting_source)

    def test_store_inspect_uses_the_exact_local_parser_reexport(self) -> None:
        inspect_manifest = manifest("rocketmq-tools/rocketmq-store-inspect")
        inspect_source = production_source("rocketmq-tools/rocketmq-store-inspect")
        facade = (ROOT / "rocketmq-store" / "src" / "inspection.rs").read_text(encoding="utf-8")

        self.assertNotIn("rocketmq-common", inspect_manifest["dependencies"])
        self.assertEqual({"rocketmq-error", "rocketmq-store"}, {
            name for name in inspect_manifest["dependencies"] if name.startswith("rocketmq-")
        })
        self.assertNotIn("rocketmq_common", inspect_source)
        self.assertIn("rocketmq_store::inspection::decode_commit_log_record", inspect_source)
        self.assertIn(
            "pub use rocketmq_store_local::commit_log::record_parser::decode_commit_log_record;",
            facade,
        )
        self.assertNotIn("pub fn ", facade)
        self.assertNotIn("pub struct ", facade)

    def test_namesrv_controller_edge_is_confined_to_combined_service_composition(self) -> None:
        sources = {
            path.relative_to(ROOT).as_posix()
            for path in (ROOT / "rocketmq-namesrv" / "src").rglob("*.rs")
            if "rocketmq_controller" in path.read_text(encoding="utf-8")
        }

        self.assertEqual(
            {
                "rocketmq-namesrv/src/bootstrap.rs",
                "rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs",
            },
            sources,
        )

    def test_evidence_freezes_all_five_facade_responsibilities_and_rollback(self) -> None:
        evidence = EVIDENCE.read_text(encoding="utf-8")
        for facade in (
            "rocketmq-common",
            "rocketmq-remoting",
            "rocketmq-store",
            "rocketmq-proxy",
            "rocketmq-rust",
        ):
            self.assertIn(f"`{facade}`", evidence)
        self.assertIn("forwarding adapter", evidence)
        self.assertIn("49 → 38", evidence)
        self.assertIn("55/82", evidence)


if __name__ == "__main__":
    unittest.main()
