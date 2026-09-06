# Copyright 2023 The RocketMQ Rust Authors
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

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "error_architecture_guard.py"


def load_guard():
    sys.path.insert(0, str(SCRIPT.parent))
    spec = importlib.util.spec_from_file_location("error_architecture_guard", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class ErrorArchitectureGuardTests(unittest.TestCase):
    def setUp(self):
        self.guard = load_guard()

    def test_rejects_backend_json_source_discard(self):
        findings = self.guard.backend_source_loss_messages(
            "rocketmq-tieredstore/src/dispatcher/progress_persistence.rs",
            "serde_json::from_slice(bytes).map_err(|_| corrupted())",
        )

        self.assertEqual(["persisted progress JSON errors must remain typed"], findings)

    def test_rejects_backend_absence_collapse(self):
        findings = self.guard.backend_source_loss_messages(
            "rocketmq-tieredstore/src/metadata/metadata_store.rs",
            "if fs::metadata(&path).await.is_err() { return Ok(()); }",
        )

        self.assertEqual(["metadata errors other than NotFound must remain typed"], findings)

    def test_rejects_backend_runtime_fallback(self):
        findings = self.guard.backend_source_loss_messages(
            "rocketmq-tieredstore/src/runtime.rs",
            "let joined = operation.cancel_and_wait(group, timeout).await.unwrap_or(false);",
        )

        self.assertEqual(
            ["Tiered shutdown runtime errors must not be replaced by a fallback value"],
            findings,
        )

    def test_rejects_source_to_text_under_both_backend_roots(self):
        expected = "source stringification requires a typed source wrapper or SOURCE_STRINGIFICATION_ALLOWLIST entry"

        for backend_root in ("rocketmq-store-rocksdb", "rocketmq-tieredstore"):
            with self.subTest(backend_root=backend_root):
                self.assertEqual(
                    expected,
                    self.guard.source_stringification_message(
                        f"{backend_root}/src/probe.rs",
                        "let detail = error.to_string();",
                    ),
                )
                self.assertEqual(
                    expected,
                    self.guard.source_stringification_message(
                        f"{backend_root}/src/probe.rs",
                        'let error = RocketMQError::Internal(format!("backend failed: {error}"));',
                    ),
                )
                self.assertEqual(
                    expected,
                    self.guard.source_stringification_message(
                        f"{backend_root}/src/probe.rs",
                        "let detail = source.to_string();",
                    ),
                )
                self.assertEqual(
                    expected,
                    self.guard.source_stringification_message(
                        f"{backend_root}/src/probe.rs",
                        'let detail = format!("backend failed: {source}");',
                    ),
                )

        self.assertIn(("rocketmq-store-rocksdb", "src"), self.guard.SOURCE_STRINGIFICATION_DOMAIN_ROOTS)
        self.assertIn(("rocketmq-tieredstore", "src"), self.guard.SOURCE_STRINGIFICATION_DOMAIN_ROOTS)

    def test_recognizes_allowlisted_external_unit_test_module(self):
        test_path = ROOT / "rocketmq-store-rocksdb" / "src" / "release_checkpoint_tests.rs"

        self.assertTrue(self.guard.is_test_source_path(test_path))
        self.assertTrue(self.guard.is_test_context(test_path, 1))

    def test_does_not_skip_arbitrary_tests_suffix(self):
        source_root = ROOT / "rocketmq-store-rocksdb" / "src"
        with tempfile.TemporaryDirectory(prefix="guard-fixture-", dir=source_root) as directory:
            test_path = Path(directory) / "fake_tests.rs"
            source_line = "let detail = error.to_string();"
            test_path.write_text(f"{source_line}\n", encoding="utf-8")

            self.assertFalse(self.guard.is_test_source_path(test_path))
            self.assertFalse(self.guard.is_test_context(test_path, 1))
            self.assertEqual([(1, source_line)], list(self.guard.iter_non_test_lines(test_path)))

    def test_allows_only_the_canonical_generic_result_alias(self):
        alias = "pub type Result<T> = std::result::Result<T, Error>;"

        self.assertIsNone(
            self.guard.generic_public_result_message("rocketmq-error/src/error.rs", alias)
        )
        self.assertIsNotNone(
            self.guard.generic_public_result_message("rocketmq-model/src/lib.rs", alias)
        )
        self.assertIsNotNone(
            self.guard.generic_public_result_message(
                "rocketmq-error/src/error.rs",
                "pub type Result<T> = anyhow::Result<T>;",
            )
        )

    def test_client_retry_boundary_tracks_the_single_typed_policy(self):
        self.assertEqual([], self.guard.check_client_retry_boundary())

    def test_client_retry_boundary_rejects_phase_inference(self):
        relative_paths = (
            Path("rocketmq-client/src/common/retry_policy.rs"),
            Path("rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/retry.rs"),
            Path("rocketmq-client/src/implementation/mq_client_api_impl/producer_retry.rs"),
        )
        with tempfile.TemporaryDirectory(prefix="client-retry-guard-") as directory:
            fixture_root = Path(directory)
            for relative_path in relative_paths:
                fixture_path = fixture_root / relative_path
                fixture_path.parent.mkdir(parents=True, exist_ok=True)
                fixture_path.write_text((ROOT / relative_path).read_text(encoding="utf-8"), encoding="utf-8")
            retry_path = fixture_root / relative_paths[0]
            retry_path.write_text(
                retry_path.read_text(encoding="utf-8") + "\nfn infer_stage() { let _ = fields::PHASE; }\n",
                encoding="utf-8",
            )

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                findings = self.guard.check_client_retry_boundary()
            finally:
                self.guard.ROOT = original_root

        self.assertIn(
            "client retry decisions must use typed request stage, not PHASE context",
            [finding.message for finding in findings],
        )

    def test_proxy_remoting_boundary_rejects_dynamic_public_remarks_and_source_stringification(self):
        unsafe_lines = (
            "entry.status.message().to_owned(),",
            "(!plan.status.is_ok()).then(|| plan.status.message().to_owned()),",
            'RocketMQError::response_process_failed("proxy_remoting_response", error.to_string())',
            'format!("the consumer group[{}] not online", header.consumer_group),',
            'format!("no consumer for this group, {}", header.consumer_group),',
            '"no remoting channel for consumer group {}, clients are online",',
            '"no matching remoting lite consumer for group {}, clientId {}",',
            'format!("parent topic \'{}\' has no lite subscriptions", topic),',
            '"lite topic \'{}\' under \'{}\' has no subscribers",',
            '"group \'{}\' has no lite subscription for \'{}\'",',
        )

        for line in unsafe_lines:
            with self.subTest(line=line):
                self.assertIsNotNone(self.guard.proxy_remoting_boundary_message(line))

    def test_proxy_remoting_boundary_allows_fixed_catalog_and_business_remarks(self):
        safe_lines = (
            "safe_send_status_remark(&entry.status),",
            "safe_pull_status_remark(&plan.status).to_owned(),",
            "safe_offset_status_remark(&plan.status).to_owned(),",
            'owner_error_with_source(&CORE_INTERNAL_FAILURE, "build Proxy remoting response", error)',
            '"Consumer group is not online",',
            '"Parent topic has no lite subscriptions",',
        )

        for line in safe_lines:
            with self.subTest(line=line):
                self.assertIsNone(self.guard.proxy_remoting_boundary_message(line))

        self.assertEqual([], self.guard.check_proxy_remoting_boundary())

    def test_dashboard_http_boundary_rejects_dynamic_public_error_text(self):
        unsafe_lines = (
            "let body = self.response_message();",
            "let message = rocketmq_response_message(error);",
            "let message = admin_response_message(error);",
            "let status = error.http_status();",
            'let code = error.code().unwrap_or("ADMIN_ERROR");',
            "let message = reason.clone();",
            "let details = context.clone();",
            "let message = error.to_string();",
            "let message = self.to_string();",
        )

        for line in unsafe_lines:
            with self.subTest(line=line):
                self.assertIsNotNone(self.guard.dashboard_http_boundary_message(line))

    def test_dashboard_http_boundary_allows_fixed_and_public_view_projection(self):
        safe_lines = (
            "PublicErrorView::try_new(error.descriptor(), &context)",
            "for field in view.fields() {",
            'DashboardHttpProjection::fixed(StatusCode::BAD_REQUEST, "VALIDATION_ERROR", "Request validation failed")',
            "descriptor_by_code(code)",
            "DashboardHttpProjection::unknown()",
        )

        for line in safe_lines:
            with self.subTest(line=line):
                self.assertIsNone(self.guard.dashboard_http_boundary_message(line))

        self.assertEqual([], self.guard.check_dashboard_http_boundary())


if __name__ == "__main__":
    unittest.main()
