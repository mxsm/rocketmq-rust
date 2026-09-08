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
        expected = "source stringification requires a typed source wrapper"

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
                        format_source_interpolation=True,
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
                        format_source_interpolation=True,
                    ),
                )

        self.assertIn(("rocketmq-store-rocksdb", "src"), self.guard.SOURCE_STRINGIFICATION_DOMAIN_ROOTS)
        self.assertIn(("rocketmq-tieredstore", "src"), self.guard.SOURCE_STRINGIFICATION_DOMAIN_ROOTS)

    def test_format_comment_interpolation_and_fake_format_macros_are_ignored(self):
        source = '''
fn render_constant() {
    let rendered = format!("constant"); // {source}
    let also_constant = format!(
        "constant" /* {source} */
    );
}

/* format!("comment: {source}") */
const FAKE_FORMAT: &str = r#"format!("raw: {source}")"#;
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-format-negative-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-tieredstore" / "src" / "probe.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([], findings)

    def test_multiline_format_source_interpolation_is_reported_once(self):
        source = '''
fn render_source() {
    let rendered = format!(
        "failure: {source}",
        source = source.to_string(),
    );
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-format-multiline-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-tieredstore" / "src" / "probe.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            format_line = source.splitlines().index("    let rendered = format!(") + 1

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([format_line], [finding.line for finding in findings])

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

    def test_inline_test_modules_allow_intermediate_attributes_and_share_context_detection(self):
        source = '''
fn production() {
    operation().map_err(|error| error.to_string())?;
}

#[cfg(test)] /* multi */ mod tests
{ /* opening comment */
    fn unit_fixture() {
        let opening = "{";
        let raw_opening = r#"{"#;
        // }
        let raw_multiline = r###"
}
"###;
/*
}
*/
        operation().map_err(|error| error.to_string())?;
    }
} /* close */

#[cfg(any(test, feature = "test-support"))]
/* The cfg remains attached across a
 * multiline block comment and attributes. */
// This exact feature-gated module is test support, not production.
#[doc(hidden)]
pub mod test_support {
    fn feature_fixture() {
        let closing = "}";
        // {
        operation().map_err(|error| error.to_string())?;
    }
}

fn production_after_test_modules() {
    operation().map_err(|error| error.to_string())?;
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-context-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-broker" / "src" / "lib.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            lines = source.splitlines()
            production_lines = [
                index + 1
                for index, line in enumerate(lines)
                if line == "    operation().map_err(|error| error.to_string())?;"
            ]
            test_lines = [
                index + 1
                for index, line in enumerate(lines)
                if line == "        operation().map_err(|error| error.to_string())?;"
            ]
            unit_line, feature_line = test_lines

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                self.assertTrue(all(not self.guard.is_test_context(fixture_path, line) for line in production_lines))
                self.assertTrue(self.guard.is_test_context(fixture_path, unit_line))
                self.assertTrue(self.guard.is_test_context(fixture_path, feature_line))
                non_test_lines = dict(self.guard.iter_non_test_lines(fixture_path))
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertTrue(all(line in non_test_lines for line in production_lines))
        self.assertNotIn(unit_line, non_test_lines)
        self.assertNotIn(feature_line, non_test_lines)
        self.assertEqual(production_lines, [finding.line for finding in findings])

    def test_near_miss_test_module_cfg_or_name_does_not_hide_production_source_stringification(self):
        source = '''
#[cfg(test)]
mod tests; // {
fn production_after_external_test_module() {
    operation().map_err(|error| error.to_string())?;
}

#[cfg(any(test, feature = "other-support"))]
// A non-canonical feature must not hide this module.
mod test_support {
    fn first() {
        operation().map_err(|error| error.to_string())?;
    }
}

#[cfg(any(test, feature = "test-support"))]
// A non-canonical module name must not inherit the test-support exemption.
mod support_helpers {
    fn second() {
        operation().map_err(|error| error.to_string())?;
    }
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-near-miss-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-broker" / "src" / "lib.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            lines = source.splitlines()
            external_module_line = lines.index("mod tests; // {") + 1
            external_production_line = lines.index("    operation().map_err(|error| error.to_string())?;") + 1

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                self.assertFalse(self.guard.is_test_context(fixture_path, external_module_line))
                self.assertFalse(self.guard.is_test_context(fixture_path, external_production_line))
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual(3, len(findings))

    def test_nested_test_module_ends_at_the_closing_brace_with_its_declaration_indentation(self):
        source = '''
mod outer {
    #[cfg(test)]
    mod tests {
        fn fixture() {
            let opening = "{";
            // }
            operation().map_err(|error| error.to_string())?;
        }
    }

    fn production_after_nested_tests() {
        operation().map_err(|error| error.to_string())?;
    }
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-nested-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-broker" / "src" / "lib.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            lines = source.splitlines()
            test_line = lines.index("            operation().map_err(|error| error.to_string())?;") + 1
            production_line = lines.index("        operation().map_err(|error| error.to_string())?;") + 1

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                self.assertTrue(self.guard.is_test_context(fixture_path, test_line))
                self.assertFalse(self.guard.is_test_context(fixture_path, production_line))
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([production_line], [finding.line for finding in findings])

    def test_fake_test_modules_in_comments_and_multiline_literals_do_not_hide_production(self):
        fixtures = {
            "block_comment": '''
/*
#[cfg(test)]
mod tests {
}
*/
fn production() {
    operation().map_err(|error| error.to_string())?;
}
''',
            "multiline_string": '''
const FAKE_MODULE: &str = "
#[cfg(test)]
mod tests {
}
";
fn production() {
    operation().map_err(|error| error.to_string())?;
}
''',
            "raw_string": '''
const FAKE_MODULE: &str = r###"
#[cfg(test)]
mod tests {
}
"###;
fn production() {
    operation().map_err(|error| error.to_string())?;
}
''',
        }
        with tempfile.TemporaryDirectory(prefix="error-guard-masked-near-miss-") as directory:
            fixture_root = Path(directory)
            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                for name, source in fixtures.items():
                    with self.subTest(name=name):
                        fixture_path = fixture_root / "rocketmq-broker" / "src" / f"{name}.rs"
                        fixture_path.parent.mkdir(parents=True, exist_ok=True)
                        fixture_path.write_text(source, encoding="utf-8")
                        production_line = source.splitlines().index(
                            "    operation().map_err(|error| error.to_string())?;"
                        ) + 1

                        findings = self.guard.find_source_stringification([fixture_path])

                        self.assertEqual([production_line], [finding.line for finding in findings])
            finally:
                self.guard.ROOT = original_root

    def test_test_module_range_ignores_indentation_and_masks_trailing_multiline_comment(self):
        source = '''
#[cfg(test)]
#[allow(dead_code)]
mod tests {
    fn fixture() {
        operation().map_err(|error| error.to_string())?;
    }
      } /* trailing comment after the differently indented closing brace
operation().map_err(|error| error.to_string())?;
*/
fn production_after_tests() {
    operation().map_err(|error| error.to_string())?;
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-range-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-broker" / "src" / "range.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            lines = source.splitlines()
            test_line = lines.index("        operation().map_err(|error| error.to_string())?;") + 1
            comment_line = lines.index("operation().map_err(|error| error.to_string())?;") + 1
            production_line = lines.index("    operation().map_err(|error| error.to_string())?;") + 1

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                self.assertTrue(self.guard.is_test_context(fixture_path, test_line))
                self.assertFalse(self.guard.is_test_context(fixture_path, comment_line))
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([production_line], [finding.line for finding in findings])

    def test_feature_test_support_module_is_exempt_only_in_broker_lib(self):
        source = '''
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub mod test_support {
    fn fixture() {
        operation().map_err(|error| error.to_string())?;
    }
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-test-support-path-") as directory:
            fixture_root = Path(directory)
            fixture_path = fixture_root / "rocketmq-controller" / "src" / "lib.rs"
            fixture_path.parent.mkdir(parents=True)
            fixture_path.write_text(source, encoding="utf-8")
            finding_line = source.splitlines().index(
                "        operation().map_err(|error| error.to_string())?;"
            ) + 1

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                findings = self.guard.find_source_stringification([fixture_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([finding_line], [finding.line for finding in findings])

    def test_identity_token_arc_unit_is_the_only_non_sensitive_derive_debug_exception(self):
        safe_source = '''
#[derive(Debug)]
struct EndpointLease {
    identity_token: Arc<()>,
}
'''
        unsafe_source = '''
#[derive(Debug)]
struct EndpointLease {
    identity_token: Arc<()>,
}

#[derive(Debug)]
struct StringToken {
    identity_token: String,
}

#[derive(Debug)]
struct OtherToken {
    session_token: Arc<()>,
}
'''
        with tempfile.TemporaryDirectory(prefix="error-guard-debug-") as directory:
            fixture_root = Path(directory)
            fixture_dir = fixture_root / "rocketmq-transport" / "src" / "clients" / "rocketmq_tokio_client"
            fixture_dir.mkdir(parents=True)
            safe_path = fixture_dir / "endpoint_state.rs"
            unsafe_path = fixture_dir / "other.rs"
            safe_path.write_text(safe_source, encoding="utf-8")
            unsafe_path.write_text(unsafe_source, encoding="utf-8")

            original_root = self.guard.ROOT
            self.guard.ROOT = fixture_root
            try:
                safe_findings = self.guard.find_sensitive_derive_debug_fields([safe_path])
                unsafe_findings = self.guard.find_sensitive_derive_debug_fields([unsafe_path])
            finally:
                self.guard.ROOT = original_root

        self.assertEqual([], safe_findings)
        self.assertEqual(3, len(unsafe_findings))
        self.assertTrue(all("manual redacted Debug" in finding.message for finding in unsafe_findings))

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

    def test_cli_boundary_rejects_legacy_context_and_raw_top_level_errors(self):
        unsafe_lines = (
            "let view = error.boundary_view();",
            "rendered.push_str(&self.context.to_string());",
            '"Error: code={}, component={}, exit_code={}, message={}"',
            'rendered.push_str(", context={");',
            "view.render_stderr()",
            "view.render_verbose_stderr()",
            'eprintln!("failed to spawn rocketmq-admin-cli main thread: {error}");',
            'eprintln!("failed to initialize or shut down rocketmq-admin-cli: {error}");',
            'eprintln!("rocketmq-admin-cli main thread terminated unexpectedly");',
            'eprintln!("startup failed: {error}");',
            'eprintln!("{error:?}");',
            "std::process::exit(1);",
        )

        for line in unsafe_lines:
            with self.subTest(line=line):
                self.assertIsNotNone(self.guard.cli_boundary_message(line))

    def test_cli_boundary_allows_safe_default_and_controlled_verbose_views(self):
        safe_lines = (
            "PublicErrorView::try_new(self.descriptor, &self.context)",
            "DiagnosticView::try_new(descriptor, context)",
            'format!("ERROR {}: {}", public.code(), public.message())',
            "CliErrorView::from_error(error).output(verbosity)",
            "output.exit_code().as_i32()",
            'eprintln!("{}", output.stderr());',
        )

        for line in safe_lines:
            with self.subTest(line=line):
                self.assertIsNone(self.guard.cli_boundary_message(line))

        self.assertEqual([], self.guard.check_cli_boundary())

    def test_cli_boundary_rejects_store_source_and_path_stringification(self):
        for line in ("let reason = error.to_string();", "let path = path.display().to_string();"):
            with self.subTest(line=line):
                self.assertIsNotNone(self.guard.cli_store_boundary_message(line))

    def test_cli_boundary_rejects_positional_and_multiline_raw_output_sinks(self):
        source = '''
eprintln!("{}", error);
eprintln!(
    "{:#?}",
    source
);
println!("{}", error);
'''
        findings = self.guard.cli_raw_output_sinks(source)

        self.assertEqual(3, len(findings))
        self.assertTrue(all("raw" in message or "CliOutput" in message for _, message in findings))

    def test_cli_boundary_allows_catalog_stderr_and_non_error_stdout(self):
        source = '''
eprintln!("{}", output.stderr());
println!("version={}", env!("CARGO_PKG_VERSION"));
println!("source inventory complete");
'''

        self.assertEqual([], self.guard.cli_raw_output_sinks(source))

    def test_authorization_decision_contract_rejects_denial_errors_and_dynamic_remarks(self):
        unsafe_lines = (
            (
                "rocketmq-auth/src/authorization/provider.rs",
                "AuthorizationError::PermissionDenied { subject, resource, reason }",
            ),
            (
                "rocketmq-auth/src/authorization/provider.rs",
                "let message = error.to_string();",
            ),
            (
                "rocketmq-transport/src/dispatch/authorized_dispatcher/core.rs",
                "reason.to_string(),",
            ),
            (
                "rocketmq-security-api/src/lib.rs",
                "pub enum Decision {",
            ),
        )

        for relative_path, line in unsafe_lines:
            with self.subTest(relative_path=relative_path, line=line):
                self.assertIsNotNone(
                    self.guard.authorization_decision_contract_message(relative_path, line)
                )

    def test_authorization_decision_contract_accepts_closed_values_and_fixed_catalog_output(self):
        safe_lines = (
            (
                "rocketmq-auth/src/authorization/provider.rs",
                "Ok(AuthorizationDecision::Deny(AuthorizationDenial::PermissionDenied))",
            ),
            (
                "rocketmq-transport/src/dispatch/authorized_dispatcher/core.rs",
                "AUTH_PERMISSION_DENIED.public_message(),",
            ),
            (
                "rocketmq-security-api/src/lib.rs",
                "pub use layered_authorization::AuthorizationDecision;",
            ),
        )

        for relative_path, line in safe_lines:
            with self.subTest(relative_path=relative_path, line=line):
                self.assertIsNone(
                    self.guard.authorization_decision_contract_message(relative_path, line)
                )

        self.assertEqual([], self.guard.check_authorization_decision_contract())


if __name__ == "__main__":
    unittest.main()
