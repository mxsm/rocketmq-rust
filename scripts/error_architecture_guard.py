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

SENSITIVE_FIELD_TERMS = (
    "secret",
    "password",
    "token",
    "signature",
    "authorization",
    "credential",
)

SENSITIVE_DEBUG_FIELD_TERMS = tuple(term for term in SENSITIVE_FIELD_TERMS if term != "authorization")

NON_SENSITIVE_DEBUG_FIELD_NAMES = {
    "signature_algorithm",
}

INTERNAL_ERROR_ALLOWLIST = (
    "rocketmq-broker/src/",
    "rocketmq-client/src/",
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

SOURCE_STRINGIFICATION_ALLOWLIST: dict[str, str] = {
    "rocketmq-auth/src/acl/loader.rs": "ACL file loader still maps serde and filesystem failures into public auth storage errors",
    "rocketmq-auth/src/authentication/factory/authentication_factory.rs": "authentication factory exposes stable auth config errors while provider errors remain string reasons",
    "rocketmq-auth/src/authentication/provider/default_authentication_provider.rs": "default authentication provider maps AuthError into public authentication failure text",
    "rocketmq-auth/src/authentication/provider/local_authentication_metadata_provider.rs": "local authentication metadata provider persists JSON/filesystem details as public storage reasons",
    "rocketmq-auth/src/authentication/strategy.rs": "authentication strategy trait returns local AuthError without a source-bearing variant",
    "rocketmq-auth/src/authorization/builder/default_authorization_context_builder.rs": "authorization context builder maps parser failures into user-facing invalid-context reasons",
    "rocketmq-auth/src/authorization/factory.rs": "authorization factory exposes stable auth config errors while provider errors remain string reasons",
    "rocketmq-auth/src/authorization/manager/metadata_manager.rs": "authorization metadata manager keeps provider failures as public configuration reasons",
    "rocketmq-auth/src/authorization/manager/metadata_manager_impl.rs": "authorization metadata manager implementation keeps provider failures as public configuration reasons",
    "rocketmq-auth/src/authorization/metadata_provider/local.rs": "local authorization metadata provider persists JSON/filesystem details as public storage reasons",
    "rocketmq-auth/src/authorization/provider.rs": "authorization provider converts domain AuthorizationError into RocketMQError at the auth boundary",
    "rocketmq-auth/src/authorization/strategy/abstract_authorization_strategy.rs": "authorization strategy keeps provider failures as configuration reasons",
    "rocketmq-auth/src/lib.rs": "auth bootstrap helpers expose storage errors through public RocketMQError storage variants",
    "rocketmq-auth/src/migration/alc/plain_permission_manager.rs": "legacy ACL migration keeps parser detail as compatibility text",
    "rocketmq-auth/src/runtime.rs": "auth runtime composes provider failures into public authentication errors",
    "rocketmq-auth/src/runtime_bridge.rs": "runtime bridge exports string diagnostics across a trait boundary",
    "rocketmq-broker/src/command.rs": "broker CLI argument parser stores address parse detail in command error text",
    "rocketmq-broker/src/processor/admin_broker_processor.rs": "admin remoting parser keeps Java-compatible request body remarks",
    "rocketmq-broker/src/processor/admin_broker_processor/message_related_handler.rs": "message admin remoting path keeps decode detail as protocol remark",
    "rocketmq-broker/src/topic/manager/topic_queue_mapping_manager.rs": "topic queue mapping persistence currently reports executor/persist failures as broker internal diagnostics",
    "rocketmq-controller/src/controller/open_raft_controller.rs": "OpenRaft controller startup and scheduler runtime boundaries report task and bind failures as typed controller diagnostics",
    "rocketmq-controller/src/processor/controller_request_processor.rs": "controller config remoting endpoint maps UTF-8 parser detail into request validation text",
    "rocketmq-controller/src/openraft/log_store.rs": "OpenRaft log store trait requires std::io::Error at the storage boundary",
    "rocketmq-controller/src/openraft/network/grpc_client.rs": "OpenRaft network API requires std::io::Error-backed NetworkError values",
    "rocketmq-controller/src/openraft/state_machine.rs": "OpenRaft state machine and snapshot traits require std::io::Error at the storage boundary",
    "rocketmq-store/src/ha/auto_switch/auto_switch_ha_service.rs": "HA service trait exposes string service diagnostics",
    "rocketmq-store/src/ha/default_ha_connection.rs": "HA connection joins async runtime errors into service diagnostics",
    "rocketmq-store/src/ha/default_ha_service.rs": "HA service joins async runtime errors into service diagnostics",
    "rocketmq-store/src/ha/group_transfer_service.rs": "HA group transfer keeps request wait failures as service diagnostics",
    "rocketmq-store/src/ha/ha_connection_state_notification_service.rs": "HA notification service reports channel failures as service diagnostics",
    "rocketmq-store/src/message_store/local_file_message_store.rs": "local file message store records recovery progress and HA/storage diagnostics as display text",
    "rocketmq-store/src/rocksdb/consume_queue.rs": "RocksDB group commit background worker stores task failure text for later reporting",
    "rocketmq-store/src/rocksdb/error.rs": "rocksdb crate errors are converted to RocketMQ storage variants until the error kernel grows external source slots",
    "rocketmq-store/src/tieredstore.rs": "tiered store test helper creates temporary directories and maps setup failures to RocketMQError",
    "rocketmq-store/src/utils/ffi.rs": "FFI helpers expose OS error reasons across a C-compatible boundary",
}


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


def iter_non_test_lines(path: Path) -> Iterable[tuple[int, str]]:
    """Yield source lines while skipping test-only modules in one pass."""
    rel_parts = path.relative_to(ROOT).parts
    if "tests" in rel_parts or "benches" in rel_parts or "examples" in rel_parts:
        return
    if "src" in rel_parts and "bin" in rel_parts:
        return

    depth = 0
    test_module_depth: int | None = None
    pending_test_cfg = False
    for line_number, line in enumerate(read_text(path).splitlines(), start=1):
        stripped = line.strip()
        if stripped == "#[cfg(test)]":
            pending_test_cfg = True
        elif pending_test_cfg and re.match(r"(pub\([^)]*\)\s+|pub\s+)?mod\s+tests\b", stripped):
            test_module_depth = depth + stripped.count("{") - stripped.count("}")
            pending_test_cfg = False
        elif stripped and not stripped.startswith("#"):
            pending_test_cfg = False

        if test_module_depth is None:
            yield line_number, line

        depth += line.count("{") - line.count("}")
        if test_module_depth is not None and depth < test_module_depth:
            test_module_depth = None


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


def contains_redaction_marker(line: str) -> bool:
    return any(
        marker in line
        for marker in (
            "<redacted>",
            "REDACTED",
            "redacted_if_present",
            "redacted_value",
            "Sensitive::new",
        )
    )


def sensitive_debug_field_name(field_name: str) -> bool:
    lower = field_name.lower()
    if lower in NON_SENSITIVE_DEBUG_FIELD_NAMES:
        return False
    return any(term in lower for term in SENSITIVE_DEBUG_FIELD_TERMS)


def find_sensitive_derive_debug_fields(paths: Iterable[Path]) -> list[Finding]:
    findings: list[Finding] = []
    field_pattern = re.compile(r"(?:pub(?:\([^)]*\))?\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*:")
    struct_pattern = re.compile(r"(?:pub(?:\([^)]*\))?\s+)?struct\s+([A-Za-z_][A-Za-z0-9_]*)\b")

    for path in paths:
        lines = list(iter_non_test_lines(path))
        pending_debug_derive_line: int | None = None
        for index, (line_number, line) in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith("#[derive") and "Debug" in stripped:
                pending_debug_derive_line = line_number
                continue
            if pending_debug_derive_line is None:
                continue
            if stripped.startswith("#[") or not stripped:
                continue

            if not struct_pattern.match(stripped):
                pending_debug_derive_line = None
                continue

            depth = line.count("{") - line.count("}")
            for field_index in range(index + 1, len(lines)):
                _, field_line = lines[field_index]
                field_match = field_pattern.match(field_line.strip())
                if field_match and sensitive_debug_field_name(field_match.group(1)):
                    findings.append(
                        Finding(
                            path,
                            pending_debug_derive_line,
                            "derive(Debug) on a struct with sensitive fields requires a manual redacted Debug impl",
                        )
                    )
                    break
                depth += field_line.count("{") - field_line.count("}")
                if depth <= 0:
                    break

            pending_debug_derive_line = None
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
            "error.boundary_view()",
            "apply_error_to_response",
            "invalid_parameter_with_remark",
            "request_code_not_supported_with_remark",
            "query_not_found_with_remark",
            "internal_error_with_opaque",
        ],
        ROOT / "rocketmq-proxy" / "src" / "status.rs": [
            "ProxyErrorKind",
            "broker_response_payload_override",
            "boundary_view().grpc()",
            "grpc_payload_to_code",
            "grpc_status_to_tonic_code",
            "local_error_grpc_mapping",
            "view.message()",
        ],
        ROOT
        / "rocketmq-dashboard"
        / "rocketmq-dashboard-web"
        / "backend"
        / "src"
        / "error"
        / "dashboard_error.rs": [
            "error.boundary_view().http().status",
            "error.boundary_view().code().as_str()",
            "view.message()",
            "view.context()",
            "config_source",
            "internal_source",
            "response_message",
        ],
        ROOT / "rocketmq-error" / "src" / "cli.rs": [
            "error.boundary_view()",
            "view.cli().exit_code",
            "render_stderr",
        ],
        ROOT / "rocketmq-tools" / "rocketmq-admin" / "rocketmq-admin-cli" / "src" / "rocketmq_cli.rs": [
            "CliErrorView::from_error",
            "view.exit_code().as_i32()",
            "RocketMQError::validation_failed",
        ],
        ROOT
        / "rocketmq-tools"
        / "rocketmq-admin"
        / "rocketmq-admin-core"
        / "src"
        / "core"
        / "error_view.rs": [
            "error.spec().code.as_str()",
            "error.public_message()",
            "error.context()",
        ],
        ROOT
        / "rocketmq-tools"
        / "rocketmq-store-inspect"
        / "src"
        / "bin"
        / "rocketmq_cli.rs": [
            "CliErrorView::from_error",
            "view.exit_code().as_i32()",
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
        if path == ROOT / "rocketmq-remoting" / "src" / "error_response.rs":
            for line_number, line in enumerate(text.splitlines(), start=1):
                if "command_from_error_with_remark(error, error.to_string())" in line:
                    findings.append(
                        Finding(
                            path,
                            line_number,
                            "remoting default error response must use public_message(), not raw Display",
                        )
                    )
    return findings


def check_proxy_grpc_boundary() -> list[Finding]:
    path = ROOT / "rocketmq-proxy" / "src" / "status.rs"
    if not path.exists():
        return [Finding(path, 1, "proxy gRPC status mapper is missing")]

    forbidden = {
        "tonic_code_from_payload_code": "proxy gRPC transport status must come from central spec, broker override, or local-only kind",
        "is_topic_route_not_found_message": "RocketMQ gRPC mapping must not parse display text for topic-route errors",
    }
    return scan_forbidden_terms([path], forbidden)


def check_dashboard_http_boundary() -> list[Finding]:
    error_path = (
        ROOT
        / "rocketmq-dashboard"
        / "rocketmq-dashboard-web"
        / "backend"
        / "src"
        / "error"
        / "dashboard_error.rs"
    )
    if not error_path.exists():
        return [Finding(error_path, 1, "dashboard HTTP error mapper is missing")]

    forbidden = {
        'Self::RocketMq(_) => "ROCKETMQ_ERROR"': "dashboard RocketMQ API code must come from ErrorSpec.code",
        "Self::RocketMq(_) => StatusCode::BAD_GATEWAY": "dashboard RocketMQ HTTP status must come from ErrorSpec.http",
        "ApiResponse::failure(self.code(), self.to_string())": "dashboard error body must use public/redacted response messages",
    }
    findings = scan_forbidden_terms([error_path], forbidden)

    backend_paths = rust_files_under("rocketmq-dashboard", "rocketmq-dashboard-web", "backend", "src")
    for path in backend_paths:
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            if "{error}" not in line:
                continue
            if "DashboardError::Config(format!" in line or "DashboardError::Internal(format!" in line:
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "dashboard config/internal source errors must use typed source or redacted response messages",
                    )
                )
    return findings


def check_client_callback_boundary() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-client" / "src" / "consumer" / "pull_callback.rs": [
            "fn on_exception(&mut self, e: RocketMQError)",
            "fn broker_response_code(error: &RocketMQError)",
            "RocketMQError::BrokerOperationFailed",
        ],
        ROOT / "rocketmq-client" / "src" / "consumer" / "pop_callback.rs": [
            "fn on_error(&mut self, e: RocketMQError)",
            "fn broker_response_code(error: &RocketMQError)",
            "RocketMQError::BrokerOperationFailed",
        ],
        ROOT / "rocketmq-client" / "src" / "producer" / "request_callback.rs": [
            "Option<&RocketMQError>",
        ],
        ROOT / "rocketmq-client" / "src" / "producer" / "request_response_future.rs": [
            "type RequestCause = Arc<RocketMQError>",
            "pub fn set_cause(&self, cause: RocketMQError)",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "client callback boundary file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required client callback boundary token missing: {needle}"))

    forbidden = {
        "downcast_ref::<RocketMQError>": "client callback error paths must use typed RocketMQError directly",
        "downcast_ref::<rocketmq_error::RocketMQError>": "client callback error paths must use typed RocketMQError directly",
        "broker_response_code(error: &(dyn": "client broker response code lookup must not downcast dyn Error",
        "type RequestCause = Arc<dyn": "request future cause must store RocketMQError directly",
        "Option<&dyn std::error::Error>": "request callback must expose RocketMQError directly",
        "Box<dyn std::error::Error + Send>": "pull/pop callbacks must expose RocketMQError directly",
    }
    guarded_paths = list(required_tokens)
    return [*findings, *scan_forbidden_terms(guarded_paths, forbidden)]


def check_client_retry_boundary() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-client" / "src" / "common" / "retry_decision.rs": [
            "error.kind().spec().recovery.retry",
            "pub(crate) struct ClientRetryEffect",
            "pub(crate) fn producer_send_retry_decision",
            "pub(crate) fn producer_send_fault_decision",
            "retry_response_codes.contains(code)",
            "Java producer retries only configured broker response codes for send.",
        ],
        ROOT
        / "rocketmq-client"
        / "src"
        / "producer"
        / "producer_impl"
        / "default_mq_producer_impl.rs": [
            "producer_send_retry_decision(error, self.producer_config.retry_response_codes())",
            "producer_send_fault_decision(error, self.mq_fault_strategy.is_start_detector_enable())",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "client retry boundary file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required client retry boundary token missing: {needle}"))

    forbidden = {
        "to_string()": "client retry decisions must not parse or build display strings",
        "format!(": "client retry decisions must use ErrorSpec recovery policy, not local text construction",
        "downcast_ref": "client retry decisions must not use runtime downcast",
    }
    retry_path = ROOT / "rocketmq-client" / "src" / "common" / "retry_decision.rs"
    return [*findings, *scan_forbidden_terms([retry_path], forbidden)]


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


def is_source_stringification_allowlisted(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return rel in SOURCE_STRINGIFICATION_ALLOWLIST


def is_source_stringification_line(line: str) -> bool:
    if ".to_string()" not in line:
        return "RocketMQError::Internal(format!(" in line

    if re.search(r"\b(error|err|e)\b", line) is None:
        return False

    if "map_err(" in line:
        return True
    if "std::io::Error::other(" in line or "io::Error::other(" in line:
        return True
    if "RocketMQError::Internal(" in line:
        return True
    if "RocketMQError::storage_" in line or "RocketMQError::auth_config_invalid(" in line:
        return True
    if "RocketMQError::authentication_failed(" in line or "RocketMQError::request_body_invalid(" in line:
        return True
    if "StoreError::" in line or "HAError::" in line or "MappedFileError::" in line:
        return True
    if "AuthorizationError::" in line or "AuthError::" in line:
        return True
    return False


def check_source_stringification_allowlist() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-store" / "src" / "store_error.rs": [
            "pub fn rocksdb(source: RocketMQError) -> Self",
            "#[source]",
            "source: Box<RocketMQError>",
        ],
        ROOT / "rocketmq-store" / "src" / "log_file" / "mapped_file" / "mapped_file_error.rs": [
            "MmapFailed(#[source] io::Error)",
            "FlushFailed(#[source] io::Error)",
        ],
        ROOT / "rocketmq-store" / "src" / "message_store" / "rocksdb_message_store.rs": [
            "map_err(StoreError::rocksdb)",
            "StoreError::rocksdb(error)",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required source-preservation file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required source-preservation token missing: {needle}"))

    domain_paths = [
        *rust_files_under("rocketmq-store", "src"),
        *rust_files_under("rocketmq-controller", "src"),
        *rust_files_under("rocketmq-auth", "src"),
        *rust_files_under("rocketmq-broker", "src"),
    ]
    for path in domain_paths:
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("//") or stripped.startswith("///") or is_test_context(path, line_number):
                continue
            if not is_source_stringification_line(line):
                continue
            if not is_source_stringification_allowlisted(path):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "source stringification requires a typed source wrapper or SOURCE_STRINGIFICATION_ALLOWLIST entry",
                    )
                )
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
        ROOT / "rocketmq-client" / "src" / "common" / "session_credentials.rs": [
            "session_credentials_debug_redacts_sensitive_fields",
            "session_credentials_display",
        ],
        ROOT / "rocketmq-remoting" / "src" / "protocol" / "body" / "user_info.rs": [
            "debug_user_info_redacts_password",
            "password=<redacted>",
        ],
        ROOT / "rocketmq-auth" / "src" / "config.rs": [
            "auth_config_debug_redacts_embedded_credentials",
            "<redacted>",
        ],
        ROOT / "rocketmq-auth" / "src" / "authentication" / "model" / "user.rs": [
            "user_debug_redacts_password",
            "<redacted>",
        ],
        ROOT / "rocketmq-auth" / "src" / "migration" / "alc" / "plain_access_resource.rs": [
            "plain_access_resource_debug_redacts_secrets",
            "signature-value",
        ],
        ROOT / "rocketmq-auth" / "src" / "migration" / "alc" / "plain_access_config.rs": [
            "plain_access_config_debug_redacts_secret_key",
            "<redacted>",
        ],
        ROOT / "rocketmq-auth" / "src" / "authentication" / "acl_client_rpc_hook.rs": [
            "secret_key",
            "<redacted>",
        ],
        ROOT
        / "rocketmq-tools"
        / "rocketmq-admin"
        / "rocketmq-admin-core"
        / "tests"
        / "auth_core_models.rs": [
            "auth_user_request_debug_redacts_passwords",
            "<redacted>",
        ],
        ROOT / "rocketmq-dashboard" / "rocketmq-dashboard-web" / "backend" / "src" / "model" / "acl_model.rs": [
            "acl_user_debug_redacts_passwords",
            "REDACTED",
        ],
        ROOT / "rocketmq-dashboard" / "rocketmq-dashboard-web" / "backend" / "src" / "model" / "auth_model.rs": [
            "auth_model_debug_redacts_password_and_session_id",
            "REDACTED",
        ],
        ROOT / "rocketmq-dashboard" / "rocketmq-dashboard-web" / "backend" / "src" / "config" / "app_config.rs": [
            "auth_config_debug_redacts_password",
            "<redacted>",
        ],
        ROOT
        / "rocketmq-dashboard"
        / "rocketmq-dashboard-web"
        / "backend"
        / "src"
        / "admin"
        / "dashboard_admin_client.rs": [
            "map_acl_user_does_not_expose_password",
            "password: None",
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

    debug_field_pattern = re.compile(r'\.field\("([^"]+)",')
    redaction_paths = [
        *rust_files_under("rocketmq-error", "src"),
        *rust_files_under("rocketmq-common", "src"),
        *rust_files_under("rocketmq-client", "src"),
        *rust_files_under("rocketmq-remoting", "src"),
        *rust_files_under("rocketmq-auth", "src"),
        *rust_files_under("rocketmq-tools", "rocketmq-admin"),
        *rust_files_under("rocketmq-dashboard", "rocketmq-dashboard-web", "backend", "src"),
    ]
    for path in redaction_paths:
        for line_number, line in iter_non_test_lines(path):
            match = debug_field_pattern.search(line)
            if match and sensitive_debug_field_name(match.group(1)) and not contains_redaction_marker(line):
                findings.append(Finding(path, line_number, "sensitive Debug field must be explicitly redacted"))
    findings.extend(find_sensitive_derive_debug_fields(redaction_paths))
    return findings


def run() -> int:
    checks = [
        ("core public surface", check_error_and_common_public_surface),
        ("processor boundary mappings", check_processor_boundary_mappings),
        ("processor generic response allowlist", check_processor_generic_response_allowlist),
        ("required mapping adapters", check_required_mapping_adapters),
        ("proxy grpc boundary", check_proxy_grpc_boundary),
        ("dashboard http boundary", check_dashboard_http_boundary),
        ("client callback boundary", check_client_callback_boundary),
        ("client retry boundary", check_client_retry_boundary),
        ("error spec contract", check_error_spec_contract),
        ("source stringification allowlist", check_source_stringification_allowlist),
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
