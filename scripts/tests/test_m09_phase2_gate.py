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

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))

import error_architecture_guard as error_guard  # noqa: E402


class Phase2GateContractTests(unittest.TestCase):
    def test_broker_permission_mapping_preserves_fields_without_source_stringification(self) -> None:
        source = (ROOT / "rocketmq-broker/src/auth/auth_admin_service.rs").read_text(encoding="utf-8")
        start = source.index("fn map_authz_error")
        end = source.index("fn permission_denied", start)
        mapping = source[start:end]

        self.assertIn("AuthorizationError::PermissionDenied", mapping)
        self.assertIn("subject,", mapping)
        self.assertIn("resource,", mapping)
        self.assertIn("reason,", mapping)
        self.assertNotIn("error.to_string()", mapping)

    def test_mcp_process_uses_typed_canonical_error_paths(self) -> None:
        main = (ROOT / "rocketmq-tools/rocketmq-mcp/src/main.rs").read_text(encoding="utf-8")
        app = (ROOT / "rocketmq-tools/rocketmq-mcp/src/app.rs").read_text(encoding="utf-8")
        stdio = (ROOT / "rocketmq-tools/rocketmq-mcp/src/transport/stdio.rs").read_text(encoding="utf-8")
        http = (ROOT / "rocketmq-tools/rocketmq-mcp/src/transport/streamable_http.rs").read_text(
            encoding="utf-8"
        )

        self.assertNotIn("anyhow::", main)
        self.assertIn("Result<(), McpError>", main)
        self.assertIn("McpApp::bootstrap_typed", main)
        for source, canonical, legacy in (
            (app, "bootstrap_typed", "bootstrap"),
            (app, "init_tracing_typed", "init_tracing"),
            (stdio, "serve_typed", "serve"),
            (http, "build_router_typed", "build_router"),
        ):
            self.assertIn(f"fn {canonical}", source)
            self.assertIn(f"fn {legacy}", source)
            self.assertRegex(source, rf'#\[deprecated\([^\]]*note = "use [^"]*{canonical}"\)\]')

    def test_mcp_compatibility_allowlist_is_exact_and_canonical_paths_are_documented(self) -> None:
        expected = {
            "rocketmq-tools/rocketmq-mcp/src/app.rs",
            "rocketmq-tools/rocketmq-mcp/src/transport/stdio.rs",
            "rocketmq-tools/rocketmq-mcp/src/transport/streamable_http.rs",
        }
        actual = {
            path for path in error_guard.ANYHOW_RESULT_ALLOWLIST if path.startswith("rocketmq-tools/rocketmq-mcp/")
        }
        self.assertEqual(expected, actual)

        allowlist = (ROOT / "docs/07-error-hygiene-allowlist.md").read_text(encoding="utf-8")
        for canonical in (
            "McpApp::bootstrap_typed",
            "init_tracing_typed",
            "transport::stdio::serve_typed",
            "serve_typed, build_router_typed",
        ):
            self.assertIn(canonical, allowlist)

    def test_error_governance_documents_cover_every_stable_code(self) -> None:
        self.assertEqual([], error_guard.check_error_governance_artifacts())
        error_codes = (ROOT / "docs/error-codes.md").read_text(encoding="utf-8")
        for code in error_guard.current_error_codes():
            self.assertIn(f"`{code}`", error_codes)

    def test_phase2_gate_evidence_binds_one_candidate_and_next_work_package(self) -> None:
        evidence_root = ROOT / "docs/plans/architecture-refactor-migration/phase-2-core-boundaries"
        candidate = "490c583e94b31dc7ae1b83c55ed811e2b90d4cce"
        for name in (
            "09-phase-2-candidate-snapshot.md",
            "09-phase-2-review-report.md",
            "09-phase-2-test-report.md",
            "09-phase-2-gate-evidence.md",
        ):
            report = (evidence_root / name).read_text(encoding="utf-8")
            self.assertIn(candidate, report)

        gate = (evidence_root / "09-phase-2-gate-evidence.md").read_text(encoding="utf-8")
        self.assertIn("59/82 completed, 23 remaining", gate)
        self.assertIn("`PR-M10-01`", gate)


if __name__ == "__main__":
    unittest.main()
