#!/usr/bin/env python3
#
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

"""Guard the typed error architecture against known regression classes."""

from __future__ import annotations

import argparse
import dataclasses
import re
import sys
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
RUST_SUFFIX = ".rs"

INTERNAL_ERROR_ALLOWLIST = (
    "rocketmq-auth/src/authorization/provider.rs",
    "rocketmq-broker/src/",
    "rocketmq-client/src/",
    "rocketmq-common/src/common/controller/controller_config.rs",
    "rocketmq-controller/src/",
    "rocketmq-namesrv/src/",
    "rocketmq-proxy/src/",
    "rocketmq-remoting/src/",
    "rocketmq-remoting/src/protocol/body/consumer_running_info.rs",
    "rocketmq-remoting/src/protocol/static_topic/",
    "rocketmq-tieredstore/src/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/admin/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/admin_facade/",
)

ANYHOW_RESULT_ALLOWLIST: dict[str, str] = {
    "rocketmq/src/schedule.rs": "internal scheduler futures use anyhow at the outer async worker boundary",
    "rocketmq-broker/src/broker_runtime.rs": "broker runtime stores scheduled worker handles and placeholders",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/build.rs": "build script boundary",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/db.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/runtime.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/service.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/proxy/db.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/proxy/service.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/lib.rs": "web backend process boundary",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/main.rs": "web backend process boundary",
    "rocketmq-remoting/src/remoting_server/rocketmq_tokio_server.rs": "internal remoting accept-loop worker boundary",
    "rocketmq-store/src/ha/default_ha_client.rs": "internal HA replication worker boundary",
    "rocketmq-store/src/stats/broker_stats_manager.rs": "internal scheduled worker handle boundary",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/main.rs": "TUI process boundary",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/rocketmq_tui_app.rs": "TUI terminal runtime boundary",
}

PROCESSOR_GENERIC_RESPONSE_ALLOWLIST: dict[str, str] = {
    "rocketmq-broker/src/processor/admin_broker_processor/": "admin remoting APIs retain Java-compatible local response codes pending typed handler migration",
    "rocketmq-broker/src/processor/change_invisible_time_processor.rs": "pop invisible-time protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/client_manage_processor.rs": "client management protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/consumer_manage_processor.rs": "consumer management protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/end_transaction_processor.rs": "transaction end protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/lite_manager_processor.rs": "lite management protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/lite_subscription_ctl_processor.rs": "lite subscription protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/notification_processor.rs": "long-poll notification protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/peek_message_processor.rs": "peek message protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/polling_info_processor.rs": "polling info protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/pop_lite_message_processor.rs": "lite pop protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/pop_message_processor.rs": "pop message protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/pull_message_processor.rs": "pull message protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/query_assignment_processor.rs": "assignment query protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/query_message_processor.rs": "message query protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/recall_message_processor.rs": "recall message protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/reply_message_processor.rs": "reply message protocol keeps Java-compatible broker response codes",
    "rocketmq-broker/src/processor/send_message_processor.rs": "send message protocol keeps Java-compatible broker response codes",
}

PROCESSOR_GENERIC_RESPONSE_TERMS = (
    "ResponseCode::SystemError",
    "ResponseCode::InvalidParameter",
    "ResponseCode::NoPermission",
    "ResponseCode::QueryNotFound",
    "RemotingSysResponseCode::SystemError",
    "RemotingSysResponseCode::NoPermission",
)


@dataclasses.dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    message: str

    def render(self) -> str:
        rel = self.path.relative_to(ROOT).as_posix()
        return f"{rel}:{self.line}: {self.message}"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def rust_files_under(*parts: str) -> list[Path]:
    root = ROOT.joinpath(*parts)
    if not root.exists():
        return []
    return sorted(path for path in root.rglob(f"*{RUST_SUFFIX}") if "target" not in path.parts)


def rust_files() -> list[Path]:
    return sorted(path for path in ROOT.rglob(f"*{RUST_SUFFIX}") if "target" not in path.parts)


def rel_path(path: Path) -> str:
    return path.relative_to(ROOT).as_posix()


def is_test_context(path: Path, line_number: int) -> bool:
    """Best-effort detector for unit-test modules inside Rust source files."""
    rel_parts = path.relative_to(ROOT).parts
    if "tests" in rel_parts or "benches" in rel_parts or "examples" in rel_parts:
        return True
    if "src" in rel_parts and "bin" in rel_parts:
        return True

    depth = 0
    test_module_depth: int | None = None
    pending_test_cfg = False
    for current_line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if stripped == "#[cfg(test)]":
            pending_test_cfg = True
        elif pending_test_cfg and re.match(r"(pub\([^)]*\)\s+|pub\s+)?mod\s+tests\b", stripped):
            test_module_depth = depth + stripped.count("{") - stripped.count("}")
            pending_test_cfg = False
        elif stripped and not stripped.startswith("#"):
            pending_test_cfg = False

        if current_line_number == line_number:
            return test_module_depth is not None

        depth += line.count("{") - line.count("}")
        if test_module_depth is not None and depth < test_module_depth:
            test_module_depth = None
    return False


def is_anyhow_result_allowlisted(path: Path) -> bool:
    rel = rel_path(path)
    rel_parts = path.relative_to(ROOT).parts
    if "examples" in rel_parts or "tests" in rel_parts or "benches" in rel_parts:
        return True
    if "src" in rel_parts and "bin" in rel_parts:
        return True
    if path.name == "build.rs":
        return True
    return rel in ANYHOW_RESULT_ALLOWLIST


def is_processor_generic_response_allowlisted(path: Path) -> bool:
    rel = rel_path(path)
    return any(rel == prefix or rel.startswith(prefix) for prefix in PROCESSOR_GENERIC_RESPONSE_ALLOWLIST)


def scan_forbidden_terms(paths: Iterable[Path], forbidden: dict[str, str]) -> list[Finding]:
    findings: list[Finding] = []
    for path in paths:
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            for term, message in forbidden.items():
                if term in line:
                    findings.append(Finding(path, line_number, message))
    return findings


def check_error_and_common_public_surface() -> list[Finding]:
    forbidden = {
        "RocketmqError": "legacy RocketmqError must not re-enter core public error code",
        "RocketMqError": "legacy RocketMqError spelling must not re-enter core public error code",
        "rocketmq_error::Result": "old rocketmq_error::Result alias must not be used",
        "anyhow::Result": "rocketmq-error/common must not expose public anyhow Result",
        "anyhow::Error": "rocketmq-error/common must not expose anyhow Error",
        "pub type Result": "rocketmq-error/common must use typed crate-local aliases, not generic public Result",
    }
    return scan_forbidden_terms(
        [*rust_files_under("rocketmq-error", "src"), *rust_files_under("rocketmq-common", "src")],
        forbidden,
    )


def check_processor_boundary_mappings() -> list[Finding]:
    findings: list[Finding] = []
    processor_roots = [
        ROOT / "rocketmq-broker" / "src" / "processor.rs",
        ROOT / "rocketmq-broker" / "src" / "processor",
        ROOT / "rocketmq-namesrv" / "src" / "processor.rs",
        ROOT / "rocketmq-namesrv" / "src" / "processor",
    ]
    paths: list[Path] = []
    for root in processor_roots:
        if root.is_file():
            paths.append(root)
        elif root.is_dir():
            paths.extend(rust_files_under(*root.relative_to(ROOT).parts))

    for path in sorted(set(paths)):
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            if "RequestCodeNotSupported" in line:
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "processor unsupported-code responses must use rocketmq_remoting::error_response",
                    )
                )
    return findings


def check_processor_generic_response_allowlist() -> list[Finding]:
    findings: list[Finding] = []
    processor_roots = [
        ROOT / "rocketmq-broker" / "src" / "processor.rs",
        ROOT / "rocketmq-broker" / "src" / "processor",
        ROOT / "rocketmq-namesrv" / "src" / "processor.rs",
        ROOT / "rocketmq-namesrv" / "src" / "processor",
    ]
    paths: list[Path] = []
    for root in processor_roots:
        if root.is_file():
            paths.append(root)
        elif root.is_dir():
            paths.extend(rust_files_under(*root.relative_to(ROOT).parts))

    for path in sorted(set(paths)):
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("//") or is_test_context(path, line_number):
                continue
            if not any(term in line for term in PROCESSOR_GENERIC_RESPONSE_TERMS):
                continue
            if not is_processor_generic_response_allowlisted(path):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "generic processor response codes require typed error_response helpers or an allowlist entry",
                    )
                )
    return findings


def check_required_mapping_adapters() -> list[Finding]:
    checks = {
        ROOT / "rocketmq-remoting" / "src" / "error_response.rs": [
            "error.spec().remoting.code.as_i32()",
            "apply_error_to_response",
            "invalid_parameter_with_remark",
            "request_code_not_supported_with_remark",
            "query_not_found_with_remark",
            "internal_error_with_opaque",
        ],
        ROOT / "rocketmq-proxy" / "src" / "status.rs": [
            "error.spec().grpc",
            "grpc_payload_to_code",
            "grpc_status_to_tonic_code",
        ],
    }
    findings: list[Finding] = []
    for path, needles in checks.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error boundary adapter file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required mapping adapter token missing: {needle}"))
    return findings


def check_error_spec_contract() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-error" / "src" / "kind.rs": [
            "pub enum ErrorCategory",
            "pub const fn category(self) -> ErrorCategory",
        ],
        ROOT / "rocketmq-error" / "src" / "context.rs": [
            "pub enum RedactionPolicy",
            "pub const fn for_kind(kind: ErrorKind) -> Self",
        ],
        ROOT / "rocketmq-error" / "src" / "spec.rs": [
            "pub category: ErrorCategory",
            "pub redact: RedactionPolicy",
            "category: kind.category()",
            "redact: RedactionPolicy::for_kind(kind)",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_spec_registry.rs": [
            "assert_eq!(spec.category, spec.kind.category())",
            "assert_eq!(spec.redact, RedactionPolicy::for_kind(spec.kind))",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_redaction.rs": [
            "rocketmq_error_exposes_public_message_and_redacted_context",
            "internal_error=<redacted>",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error spec contract file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required error spec contract token missing: {needle}"))
    return findings


def is_internal_error_allowlisted(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return any(rel == prefix or rel.startswith(prefix) for prefix in INTERNAL_ERROR_ALLOWLIST)


def check_internal_error_allowlist() -> list[Finding]:
    findings: list[Finding] = []
    for path in rust_files():
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if "RocketMQError::Internal(" not in line:
                continue
            if is_test_context(path, line_number):
                continue
            if not is_internal_error_allowlisted(path):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "RocketMQError::Internal requires a typed variant or an internal-error allowlist entry",
                    )
                )
    return findings


def check_anyhow_result_allowlist() -> list[Finding]:
    findings: list[Finding] = []
    forbidden_terms = ("anyhow::Result", "anyhow::Error", "use anyhow::Result")
    for path in rust_files():
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            stripped = line.strip()
            if not any(term in line for term in forbidden_terms):
                continue
            if stripped.startswith("//") or stripped.startswith("///") or is_test_context(path, line_number):
                continue
            if not is_anyhow_result_allowlisted(path):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "anyhow Result/Error requires a typed result or an anyhow allowlist entry",
                    )
                )
    return findings


def check_redaction_guards() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-error" / "src" / "context.rs": [
            'pub const REDACTED: &str = "<redacted>";',
            'f.write_str("Sensitive(<redacted>)")',
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_redaction.rs": [
            "secret_key=<redacted>",
            "token=<redacted>",
            "error_context_redacts_sensitive_fields",
        ],
        ROOT / "rocketmq-common" / "src" / "common" / "base" / "plain_access_config.rs": [
            "debug_and_display_redact_secret_key",
            "<redacted>",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required redaction guard file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required redaction token missing: {needle}"))

    sensitive_field_pattern = re.compile(r'\.field\(".*(secret|password|token|signature).*",', re.I)
    for path in [
        *rust_files_under("rocketmq-error", "src"),
        *rust_files_under("rocketmq-common", "src"),
        *rust_files_under("rocketmq-client", "src"),
        *rust_files_under("rocketmq-remoting", "src"),
    ]:
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            if (
                sensitive_field_pattern.search(line)
                and "<redacted>" not in line
                and "REDACTED" not in line
                and "redacted_value" not in line
            ):
                findings.append(Finding(path, line_number, "sensitive Debug field must be explicitly redacted"))
    return findings


def run() -> int:
    checks = [
        ("core public surface", check_error_and_common_public_surface),
        ("processor boundary mappings", check_processor_boundary_mappings),
        ("processor generic response allowlist", check_processor_generic_response_allowlist),
        ("required mapping adapters", check_required_mapping_adapters),
        ("error spec contract", check_error_spec_contract),
        ("internal error allowlist", check_internal_error_allowlist),
        ("anyhow result allowlist", check_anyhow_result_allowlist),
        ("redaction guards", check_redaction_guards),
    ]
    all_findings: list[Finding] = []
    for name, check in checks:
        findings = check()
        if findings:
            print(f"ERROR_ARCHITECTURE_GUARD_FAIL {name}", file=sys.stderr)
            for finding in findings:
                print(finding.render(), file=sys.stderr)
            all_findings.extend(findings)
        else:
            print(f"ERROR_ARCHITECTURE_GUARD_OK {name}")
    if all_findings:
        return 1
    print("ERROR_ARCHITECTURE_GUARD_OK all")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
