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
import bisect
import dataclasses
import functools
import re
import sys
from pathlib import Path
from typing import Iterable

import environment_write_guard as rust_source
import rust_hygiene_guard


ROOT = Path(__file__).resolve().parents[1]
RUST_SUFFIX = ".rs"
PROXY_STATUS_MAPPER = ROOT / "rocketmq-proxy-core" / "src" / "status.rs"
PROXY_REMOTING_BOUNDARY = ROOT / "rocketmq-proxy" / "src" / "remoting.rs"
EXTERNAL_CFG_TEST_MODULES = frozenset(
    {
        "rocketmq-store-rocksdb/src/release_checkpoint_tests.rs",
    }
)

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
    # Boolean policy switch; it contains no lease token value.
    "require_fencing_token",
    "signature_algorithm",
}

INTERNAL_ERROR_ALLOWLIST = (
    "rocketmq-broker/src/",
    "rocketmq-client/src/",
    "rocketmq-controller/src/",
    "rocketmq-namesrv/src/",
    "rocketmq-proxy/src/",
    "rocketmq-transport/src/error_response.rs",
    "rocketmq-tieredstore/src/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/admin/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/admin_facade/",
)

ANYHOW_RESULT_ALLOWLIST: dict[str, str] = {
    "rocketmq-dashboard/rocketmq-dashboard-gpui/build.rs": "build script boundary",
    "rocketmq-dashboard/rocketmq-dashboard-gpui/src/main.rs": "standalone GPUI process boundary",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/db.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/runtime.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/nameserver/service.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/proxy/db.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/proxy/service.rs": "standalone Tauri boundary pending dashboard alignment",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/lib.rs": "web backend process boundary",
    "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/main.rs": "web backend process boundary",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/main.rs": "TUI process boundary",
    "rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/rocketmq_tui_app.rs": "TUI terminal runtime boundary",
    "rocketmq-ai/rocketmq-mcp/src/app.rs": "R0 public compatibility wrappers forward to typed McpError bootstrap and tracing APIs",
    "rocketmq-ai/rocketmq-mcp/src/transport/stdio.rs": "R0 public compatibility wrapper forwards to the typed stdio service API",
    "rocketmq-ai/rocketmq-mcp/src/transport/streamable_http.rs": "R0 public compatibility wrappers forward to typed HTTP service and router APIs",
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

PROCESSOR_FIXED_SAFE_RESPONSE_ALLOWLIST: dict[str, frozenset[tuple[str, str]]] = {
    "rocketmq-namesrv/src/processor/default_request_processor.rs": frozenset(
        {
            (
                "ResponseCode::QueryNotFound,",
                '"NameServer KV configuration was not found",',
            ),
            (
                "ResponseCode::QueryNotFound,",
                '"NameServer KV namespace was not found",',
            ),
        }
    ),
}

PROCESSOR_GENERIC_RESPONSE_TERMS = (
    "ResponseCode::SystemError",
    "ResponseCode::InvalidParameter",
    "ResponseCode::NoPermission",
    "ResponseCode::QueryNotFound",
    "RemotingSysResponseCode::SystemError",
    "RemotingSysResponseCode::NoPermission",
)

TRANSPORT_REMOTING_ERROR_LEGACY_FUNCTIONS = (
    "command_from_error",
    "remoting_response_from_error",
    "command_from_error_with_factory",
    "command_from_error_with_opaque",
    "command_from_error_with_factory_and_opaque",
    "command_from_error_with_remark",
    "command_from_error_with_remark_and_factory",
    "apply_error_to_response",
    "command_from_error_with_remark_and_opaque",
    "command_from_error_with_factory_remark_and_opaque",
    "request_code_not_supported",
    "request_code_not_supported_with_factory",
    "request_code_not_supported_with_factory_and_opaque",
    "request_code_not_supported_with_remark",
    "request_code_not_supported_with_factory_and_remark",
    "request_code_not_supported_with_opaque",
    "request_code_not_supported_with_remark_and_opaque",
    "request_code_not_supported_with_factory_remark_and_opaque",
    "invalid_parameter_with_remark",
    "invalid_parameter_with_remark_and_opaque",
    "no_permission_with_remark",
    "no_permission_with_remark_and_opaque",
    "query_not_found_with_remark",
    "query_not_found_with_remark_and_opaque",
    "internal_error",
    "internal_error_with_opaque",
    "internal_error_with_factory_and_opaque",
)

PROTOCOL_REMOTING_ERROR_LEGACY_FUNCTIONS = (
    "create_response_command_from_error",
    "create_response_command_from_error_with_remark",
)

SOURCE_STRINGIFICATION_ALLOWLIST: dict[str, str] = {
    "rocketmq-auth/src/acl/loader.rs": "ACL file loader still maps serde and filesystem failures into public auth storage errors",
    "rocketmq-auth/src/authentication/factory/authentication_factory.rs": "authentication factory exposes stable auth config errors while provider errors remain string reasons",
    "rocketmq-auth/src/authentication/provider/default_authentication_provider.rs": "default authentication provider maps AuthError into public authentication failure text",
    "rocketmq-auth/src/authentication/provider/local_authentication_metadata_provider.rs": "local authentication metadata provider persists JSON/filesystem details as public storage reasons",
    "rocketmq-auth/src/authentication/strategy.rs": "authentication strategy trait returns local AuthError without a source-bearing variant",
    "rocketmq-auth/src/lib.rs": "auth bootstrap helpers expose storage errors through public RocketMQError storage variants",
    "rocketmq-auth/src/migration/alc/plain_permission_manager.rs": "legacy ACL migration keeps parser detail as compatibility text",
    "rocketmq-auth/src/runtime.rs": "auth runtime composes provider failures into public authentication errors",
    "rocketmq-auth/src/runtime_bridge.rs": "runtime bridge exports string diagnostics across a trait boundary",
    "rocketmq-broker/src/command.rs": "broker CLI argument parser stores address parse detail in command error text",
    "rocketmq-broker/src/broker/broker_registration_runtime.rs": "broker registration reports coordinator domain failures through its compatibility diagnostic enum",
    "rocketmq-broker/src/broker/log_filter_control.rs": "log filter control translates scheduler, reload, and audit boundary errors into its typed control error",
    "rocketmq-broker/src/processor/admin_broker_processor.rs": "admin remoting parser keeps Java-compatible request body remarks",
    "rocketmq-broker/src/processor/admin_broker_processor/message_related_handler.rs": "message admin remoting path keeps decode detail as protocol remark",
    "rocketmq-broker/src/schedule/schedule_message_service.rs": "legacy schedule compatibility constructs local runtime capabilities through a string-result facade",
    "rocketmq-broker/src/topic/manager/topic_queue_mapping_manager.rs": "topic queue mapping persistence currently reports executor/persist failures as broker internal diagnostics",
    "rocketmq-controller/src/controller/open_raft_controller.rs": "OpenRaft controller startup and scheduler runtime boundaries report task and bind failures as typed controller diagnostics",
    "rocketmq-controller/src/processor/controller_request_processor.rs": "controller config remoting endpoint maps UTF-8 parser detail into request validation text",
    "rocketmq-controller/src/openraft/log_store.rs": "OpenRaft log store trait requires std::io::Error at the storage boundary",
    "rocketmq-controller/src/openraft/network/grpc_client.rs": "OpenRaft network API requires std::io::Error-backed NetworkError values",
    "rocketmq-controller/src/openraft/state_machine.rs": "OpenRaft state machine and snapshot traits require std::io::Error at the storage boundary",
    "rocketmq-store/src/message_store/local_file_message_store.rs": "local file message store records recovery progress and HA/storage diagnostics as display text",
    "rocketmq-store/src/rocksdb/consume_queue.rs": "RocksDB group commit background worker stores task failure text for later reporting",
    "rocketmq-store/src/utils/ffi.rs": "FFI helpers expose OS error reasons across a C-compatible boundary",
}

BACKEND_SOURCE_PRESERVATION_TOKENS: dict[str, tuple[tuple[str, int], ...]] = {
    "rocketmq-tieredstore/src/metadata/metadata_store.rs": (
        ("Err(source) if source.kind() == std::io::ErrorKind::NotFound => Ok(false),", 1),
        ("Err(source) => Err(error::io_failed(StoreOperation::Load, source)),", 1),
        ("map_err(|source| error::state_corrupted_source(StoreOperation::Load, source))?;", 1),
    ),
    "rocketmq-tieredstore/src/dispatcher/progress_persistence.rs": (
        ("map_err(|source| crate::error::state_corrupted_source(StoreOperation::Load, source))", 1),
    ),
    "rocketmq-tieredstore/src/file/index_file_segment.rs": (
        ("map_err(|source| error::state_corrupted_source(StoreOperation::Load, source))?", 2),
    ),
    "rocketmq-tieredstore/src/file/index_file_segment/codec.rs": (
        ("map_err(|source| error::state_corrupted_source(operation, source))?", 2),
    ),
    "rocketmq-tieredstore/src/runtime.rs": (
        ("map_err(|source| crate::error::runtime_error(StoreOperation::Shutdown, source))?;", 1),
    ),
}

BACKEND_SOURCE_LOSS_PATTERNS: dict[str, tuple[tuple[str, str], ...]] = {
    "rocketmq-tieredstore/src/metadata/metadata_store.rs": (
        (
            r"fs::metadata\([^;]{0,240}\)\.await\s*\.is_err\(\)",
            "metadata errors other than NotFound must remain typed",
        ),
        (
            r"serde_json::from_slice[^;]{0,320}\.map_err\(\|_\|",
            "metadata JSON errors must remain typed",
        ),
    ),
    "rocketmq-tieredstore/src/dispatcher/progress_persistence.rs": (
        (
            r"serde_json::from_slice[^;]{0,320}\.map_err\(\|_\|",
            "persisted progress JSON errors must remain typed",
        ),
    ),
    "rocketmq-tieredstore/src/file/index_file_segment.rs": (
        (
            r"std::str::from_utf8[^;]{0,320}\.map_err\(\|_\|",
            "persisted index UTF-8 errors must remain typed",
        ),
        (
            r"\.parse::<i64>\(\)[^;]{0,240}\.map_err\(\|_\|",
            "persisted index integer errors must remain typed",
        ),
    ),
    "rocketmq-tieredstore/src/file/index_file_segment/codec.rs": (
        (
            r"std::str::from_utf8[^;]{0,320}\.map_err\(\|_\|",
            "persisted index UTF-8 errors must remain typed",
        ),
    ),
    "rocketmq-tieredstore/src/runtime.rs": (
        (
            r"cancel_and_wait[^;]{0,400}\.await\s*\.is_err\(\)",
            "Tiered shutdown runtime errors must remain typed",
        ),
        (
            r"cancel_and_wait[^;]{0,400}\.await\s*\.unwrap_or(?:_else)?\(",
            "Tiered shutdown runtime errors must not be replaced by a fallback value",
        ),
    ),
}

SOURCE_STRINGIFICATION_DOMAIN_ROOTS = (
    ("rocketmq-store", "src"),
    ("rocketmq-store-local", "src"),
    ("rocketmq-store-rocksdb", "src"),
    ("rocketmq-tieredstore", "src"),
    ("rocketmq-controller", "src"),
    ("rocketmq-auth", "src"),
    ("rocketmq-broker", "src"),
)


@dataclasses.dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    message: str

    def render(self) -> str:
        rel = self.path.relative_to(ROOT).as_posix()
        return f"{rel}:{self.line}: {self.message}"


@functools.cache
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


def is_test_source_path(path: Path) -> bool:
    """Return whether a Rust source path is unambiguously test-only."""
    rel_parts = path.relative_to(ROOT).parts
    return (
        "tests" in rel_parts
        or "benches" in rel_parts
        or "examples" in rel_parts
        or rel_path(path) in EXTERNAL_CFG_TEST_MODULES
        or ("src" in rel_parts and "bin" in rel_parts)
    )


BUILTIN_TEST_CFG = re.compile(r"\s*test\s*")
BROKER_TEST_SUPPORT_CFG = re.compile(
    r'\s*any\s*\(\s*test\s*,\s*feature\s*=\s*"test-support"\s*\)\s*'
)
UNIT_TEST_MODULE = re.compile(
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+tests\s*(?P<opening>\{)"
)
BROKER_TEST_SUPPORT_MODULE = re.compile(r"pub\s+mod\s+test_support\s*(?P<opening>\{)")
BROKER_TEST_SUPPORT_PATH = "rocketmq-broker/src/lib.rs"


@dataclasses.dataclass(frozen=True)
class RustSourceAnalysis:
    source: str
    masked: str
    lines: tuple[str, ...]
    masked_lines: tuple[str, ...]
    line_starts: tuple[int, ...]
    test_ranges: tuple[tuple[int, int], ...]


def skip_outer_attributes(masked: str, cursor: int) -> int:
    """Skip whitespace and attributes between a cfg and its item."""
    while True:
        whitespace = re.match(r"\s*", masked[cursor:])
        cursor += len(whitespace.group(0)) if whitespace is not None else 0
        if not masked.startswith("#[", cursor):
            return cursor
        closing = rust_hygiene_guard.matching_delimiter(masked, cursor + 1, "[", "]")
        if closing is None:
            return len(masked)
        cursor = closing + 1


def exact_test_module_ranges(source: str, masked: str, relative_path: str) -> tuple[tuple[int, int], ...]:
    """Find only the repository's two explicitly recognized inline test modules."""
    ranges: list[tuple[int, int]] = []
    for cfg in rust_hygiene_guard.CFG_ATTRIBUTE.finditer(masked):
        body = source[cfg.start("body") : cfg.end("body")]
        if BUILTIN_TEST_CFG.fullmatch(body):
            module_pattern = UNIT_TEST_MODULE
        elif relative_path == BROKER_TEST_SUPPORT_PATH and BROKER_TEST_SUPPORT_CFG.fullmatch(body):
            module_pattern = BROKER_TEST_SUPPORT_MODULE
        else:
            continue

        cursor = skip_outer_attributes(masked, cfg.end())
        module = module_pattern.match(masked, cursor)
        if module is None:
            continue
        opening = module.start("opening")
        closing = rust_hygiene_guard.matching_delimiter(masked, opening, "{", "}")
        if closing is not None:
            ranges.append((cfg.start(), closing + 1))
    return tuple(ranges)


@functools.cache
def rust_source_analysis(path: Path, relative_path: str) -> RustSourceAnalysis:
    """Read, mask, and classify a Rust source file once."""
    source = read_text(path)
    masked = rust_source.mask_comments_and_literals(source)
    lines = tuple(source.splitlines())
    line_starts = [0]
    line_starts.extend(match.end() for match in re.finditer("\n", source))
    ranges = (
        ((0, len(source)),)
        if is_test_source_path(path)
        else exact_test_module_ranges(source, masked, relative_path)
    )
    return RustSourceAnalysis(
        source,
        masked,
        lines,
        tuple(masked.splitlines()),
        tuple(line_starts[: len(lines)]),
        ranges,
    )


def analysis_for(path: Path) -> RustSourceAnalysis:
    return rust_source_analysis(path, rel_path(path))


def line_bounds(analysis: RustSourceAnalysis, line_number: int) -> tuple[int, int] | None:
    if line_number < 1 or line_number > len(analysis.lines):
        return None
    start = analysis.line_starts[line_number - 1]
    return start, start + len(analysis.lines[line_number - 1])


def overlaps_test_range(start: int, end: int, ranges: tuple[tuple[int, int], ...]) -> bool:
    return any(range_start < end and start < range_end for range_start, range_end in ranges)


def without_test_ranges(line: str, start: int, ranges: tuple[tuple[int, int], ...]) -> tuple[str, bool]:
    """Blank test-module spans on one line while preserving production on partial lines."""
    characters = list(line)
    end = start + len(line)
    overlapped = False
    for range_start, range_end in ranges:
        overlap_start = max(start, range_start)
        overlap_end = min(end, range_end)
        if overlap_start >= overlap_end:
            continue
        overlapped = True
        characters[overlap_start - start : overlap_end - start] = " " * (overlap_end - overlap_start)
    return "".join(characters), overlapped


def iter_non_test_line_pairs(path: Path) -> Iterable[tuple[int, str, str]]:
    """Yield aligned original/masked production lines from one cached source analysis."""
    analysis = analysis_for(path)
    for line_number, (line, masked_line, start) in enumerate(
        zip(analysis.lines, analysis.masked_lines, analysis.line_starts),
        start=1,
    ):
        production_line, overlapped = without_test_ranges(line, start, analysis.test_ranges)
        production_masked, _ = without_test_ranges(masked_line, start, analysis.test_ranges)
        if overlapped and not production_line.strip():
            continue
        yield line_number, production_line, production_masked


def is_test_context(path: Path, line_number: int) -> bool:
    """Return whether a line overlaps an exactly recognized test-only module."""
    analysis = analysis_for(path)
    bounds = line_bounds(analysis, line_number)
    return bounds is not None and overlaps_test_range(*bounds, analysis.test_ranges)


def iter_non_test_lines(path: Path) -> Iterable[tuple[int, str]]:
    """Yield production source lines from one cached masked/range analysis."""
    for line_number, line, _ in iter_non_test_line_pairs(path):
        yield line_number, line


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


def is_processor_fixed_safe_response_allowlisted(path: Path, lines: list[str], line_index: int) -> bool:
    allowed = PROCESSOR_FIXED_SAFE_RESPONSE_ALLOWLIST.get(rel_path(path))
    if allowed is None:
        return False

    previous = next((line.strip() for line in reversed(lines[:line_index]) if line.strip()), "")
    following = next((line.strip() for line in lines[line_index + 1 :] if line.strip()), "")
    return (
        previous.endswith("create_response_command_with_code_remark(")
        and (lines[line_index].strip(), following) in allowed
    )


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


def check_error_and_model_public_surface() -> list[Finding]:
    forbidden = {
        "RocketmqError": "legacy RocketmqError must not re-enter core public error code",
        "RocketMqError": "legacy RocketMqError spelling must not re-enter core public error code",
        "rocketmq_error::Result": "old rocketmq_error::Result alias must not be used",
        "anyhow::Result": "rocketmq-error/model must not expose public anyhow Result",
        "anyhow::Error": "rocketmq-error/model must not expose anyhow Error",
    }
    paths = [*rust_files_under("rocketmq-error", "src"), *rust_files_under("rocketmq-model", "src")]
    findings = scan_forbidden_terms(paths, forbidden)
    for path in paths:
        relative_path = rel_path(path)
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            message = generic_public_result_message(relative_path, line)
            if message:
                findings.append(Finding(path, line_number, message))
    return findings


def generic_public_result_message(relative_path: str, line: str) -> str | None:
    if "pub type Result" not in line:
        return None
    if (
        relative_path == "rocketmq-error/src/error.rs"
        and line.strip() == "pub type Result<T> = std::result::Result<T, Error>;"
    ):
        return None
    return "rocketmq-error/model must use typed crate-local aliases, not generic public Result"


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


def is_known_non_sensitive_debug_field(path: Path, struct_name: str, field_name: str, declaration: str) -> bool:
    """Recognize exact field/type pairs whose token name does not carry secret data."""
    if (
        rel_path(path) != "rocketmq-transport/src/clients/rocketmq_tokio_client/endpoint_state.rs"
        or struct_name != "EndpointLease"
        or field_name != "identity_token"
    ):
        return False
    return re.fullmatch(r"identity_token\s*:\s*Arc\s*<\s*\(\s*\)\s*>\s*,?", declaration) is not None


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

            struct_match = struct_pattern.match(stripped)
            if struct_match is None:
                pending_debug_derive_line = None
                continue
            struct_name = struct_match.group(1)

            depth = line.count("{") - line.count("}")
            for field_index in range(index + 1, len(lines)):
                _, field_line = lines[field_index]
                field_match = field_pattern.match(field_line.strip())
                if (
                    field_match
                    and sensitive_debug_field_name(field_match.group(1))
                    and not is_known_non_sensitive_debug_field(
                        path,
                        struct_name,
                        field_match.group(1),
                        field_line.strip(),
                    )
                ):
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
        lines = read_text(path).splitlines()
        for line_index, line in enumerate(lines):
            line_number = line_index + 1
            stripped = line.strip()
            if stripped.startswith("//") or is_test_context(path, line_number):
                continue
            if not any(term in line for term in PROCESSOR_GENERIC_RESPONSE_TERMS):
                continue
            if not is_processor_generic_response_allowlisted(path) and not is_processor_fixed_safe_response_allowlisted(
                path, lines, line_index
            ):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "generic processor response codes require typed error_response helpers or an allowlist entry",
                    )
                )
    return findings


def public_response_policy_functions(source: str) -> list[str]:
    return re.findall(r"(?m)^\s*pub(?:\(crate\))?\s+fn\s+([A-Za-z_][A-Za-z0-9_]*)\b", source)


def deleted_function_tokens(source: str, function_names: Iterable[str]) -> list[str]:
    return [name for name in function_names if re.search(rf"\b{re.escape(name)}\b", source)]


def check_required_mapping_adapters() -> list[Finding]:
    transport_adapter_path = ROOT / "rocketmq-transport" / "src" / "error_response.rs"
    protocol_factory_path = ROOT / "rocketmq-protocol" / "src" / "protocol" / "remoting_command_defaults.rs"
    checks = {
        transport_adapter_path: [
            "use rocketmq_error::PublicErrorView;",
            "pub enum RemotingErrorTarget<'a>",
            "Fresh(&'a RemotingCommandFactory)",
            "Existing(RemotingCommand)",
            "pub fn error_response(view: PublicErrorView<'_>, target: RemotingErrorTarget<'_>) -> RemotingCommand",
            "let code = view.projection().remoting().code.as_i32();",
            "let message = view.message();",
            "RemotingErrorTarget::Fresh(factory) =>",
            "RemotingErrorTarget::Reply { factory, opaque } =>",
            "RemotingErrorTarget::Existing(response) =>",
        ],
        PROXY_STATUS_MAPPER: [
            "ProxyErrorKind",
            "let descriptor = error.descriptor();",
            "descriptor.projection().grpc()",
            "descriptor.public_message()",
            "grpc_payload_to_code",
            "grpc_status_to_tonic_code",
        ],
        ROOT
        / "rocketmq-dashboard"
        / "rocketmq-dashboard-web"
        / "backend"
        / "src"
        / "error"
        / "dashboard_error.rs": [
            "PublicErrorView::try_new(error.descriptor(), &context)",
            "for field in view.fields()",
            "view.projection().http().status.as_u16()",
            "DashboardErrorResponse::from(projection)",
            "descriptor_by_code(code)",
            "DashboardHttpProjection::unknown",
            "config_source",
            "internal_source",
        ],
        ROOT / "rocketmq-error" / "src" / "cli.rs": [
            "pub enum CliVerbosity",
            "pub struct CliOutput",
            "PublicErrorView::try_new(self.descriptor, &self.context)",
            "DiagnosticView::try_new(descriptor, context)",
            "public.projection().cli().exit_code",
            "ViewValueRef::Redacted",
            "pub fn output(&self, verbosity: CliVerbosity) -> CliOutput",
        ],
        ROOT / "rocketmq-tools" / "rocketmq-admin" / "rocketmq-admin-cli" / "src" / "rocketmq_cli.rs": [
            "CliErrorView::from_error",
            "CliVerbosity::Verbose",
            ".output(verbosity)",
            "output.exit_code().as_i32()",
            "RocketMQError::validation_failed",
        ],
        ROOT / "rocketmq-tools" / "rocketmq-admin" / "rocketmq-admin-cli" / "src" / "main.rs": [
            "render_cli_error(&error, verbosity)",
            "verbosity_requested()",
        ],
        ROOT
        / "rocketmq-tools"
        / "rocketmq-admin"
        / "rocketmq-admin-core"
        / "src"
        / "client_adapter"
        / "services"
        / "error_view.rs": [
            "error.boundary_view()",
            "boundary.code().as_str()",
            "boundary.message()",
            "boundary.context()",
        ],
        ROOT
        / "rocketmq-tools"
        / "rocketmq-store-inspect"
        / "src"
        / "bin"
        / "rocketmq_cli.rs": [
            "CliErrorView::from_error",
            "CliVerbosity::Verbose",
            ".output(verbosity)",
            "ProcessOutcome::Error(output.exit_code())",
            "Error::caused_by(&STORAGE_WRITE_FAILED, source)",
            ".with_secret_presence(fields::SOURCE_PRESENT)",
        ],
    }
    findings: list[Finding] = []
    for path, needles in checks.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error boundary adapter file is missing"))
            continue
        text = (
            "\n".join(line for _, line in iter_non_test_lines(path))
            if path == transport_adapter_path
            else read_text(path)
        )
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required mapping adapter token missing: {needle}"))
        if path == transport_adapter_path:
            public_functions = public_response_policy_functions(text)
            if public_functions != ["error_response"]:
                findings.append(
                    Finding(
                        path,
                        1,
                        f"Transport public remoting response-policy functions must be exactly ['error_response']; found {public_functions}",
                    )
                )
            for removed in deleted_function_tokens(text, TRANSPORT_REMOTING_ERROR_LEGACY_FUNCTIONS):
                findings.append(Finding(path, 1, f"deleted remoting error adapter returned: {removed}"))

    if not protocol_factory_path.exists():
        findings.append(Finding(protocol_factory_path, 1, "required remoting command factory file is missing"))
    else:
        protocol_source = "\n".join(line for _, line in iter_non_test_lines(protocol_factory_path))
        for removed in deleted_function_tokens(protocol_source, PROTOCOL_REMOTING_ERROR_LEGACY_FUNCTIONS):
            findings.append(Finding(protocol_factory_path, 1, f"deleted Protocol error factory returned: {removed}"))
    return findings


def check_proxy_grpc_boundary() -> list[Finding]:
    path = PROXY_STATUS_MAPPER
    if not path.exists():
        return [Finding(path, 1, "proxy gRPC status mapper is missing")]

    forbidden = {
        "tonic_code_from_payload_code": "proxy gRPC transport status must come from the central spec or a local-only kind",
        "is_topic_route_not_found_message": "RocketMQ gRPC mapping must not parse display text for topic-route errors",
        "broker_response_payload_override": "broker response codes must be normalized once at Proxy ingress",
        "ResponseCode::from": "Proxy status mapping must consume canonical projections instead of raw Broker response codes",
    }
    return scan_forbidden_terms([path], forbidden)


PROXY_REMOTING_FORBIDDEN_TERMS = {
    "entry.status.message()": "Proxy remoting send responses must not expose arbitrary payload status messages",
    "plan.status.message()": "Proxy remoting pull/offset responses must not expose arbitrary payload status messages",
    'response_process_failed("proxy_remoting_response", error.to_string())': (
        "Proxy remoting response construction must retain the typed transport contract violation"
    ),
    'format!("the consumer group[{}] not online"': "Proxy remoting remarks must not echo consumer-group input",
    'format!("no consumer for this group, {}"': "Proxy remoting remarks must not echo consumer-group input",
    '"no remoting channel for consumer group {}, clients are online"': (
        "Proxy remoting remarks must not echo consumer-group input"
    ),
    '"no matching remoting lite consumer for group {}, clientId {}"': (
        "Proxy remoting remarks must not echo consumer or client input"
    ),
    'format!("parent topic \'{}\' has no lite subscriptions"': "Proxy remoting remarks must not echo topic input",
    '"lite topic \'{}\' under \'{}\' has no subscribers"': "Proxy remoting remarks must not echo topic input",
    '"group \'{}\' has no lite subscription for \'{}\'"': (
        "Proxy remoting remarks must not echo group or topic input"
    ),
}


def proxy_remoting_boundary_message(line: str) -> str | None:
    for term, message in PROXY_REMOTING_FORBIDDEN_TERMS.items():
        if term in line:
            return message
    return None


def check_proxy_remoting_boundary() -> list[Finding]:
    path = PROXY_REMOTING_BOUNDARY
    if not path.exists():
        return [Finding(path, 1, "Proxy remoting boundary is missing")]

    findings: list[Finding] = []
    for line_number, line in iter_non_test_lines(path):
        if message := proxy_remoting_boundary_message(line):
            findings.append(Finding(path, line_number, message))
    source = "\n".join(line for _, line in iter_non_test_lines(path))
    for token in (
        "local if local.local_kind().is_some()",
        "PublicErrorView::try_new(local.descriptor(), &context)",
    ):
        if token not in source:
            findings.append(Finding(path, 1, f"Proxy-local remoting catalog projection is missing: {token}"))
    return findings


DASHBOARD_HTTP_FORBIDDEN_TERMS = {
    "self.response_message()": "dashboard HTTP responses must use the single safe projection",
    "rocketmq_response_message": "dashboard RocketMQ responses must use PublicErrorView fields",
    "admin_response_message": "dashboard Admin responses must not render reason, context, or Display",
    "error.http_status()": "dashboard Admin HTTP status must not be accepted without an explicit allowlist",
    'error.code().unwrap_or("ADMIN_ERROR")': "dashboard Admin code must not be copied from arbitrary metadata",
    "reason.clone()": "dashboard HTTP responses must not copy dynamic Admin reason text",
    "context.clone()": "dashboard HTTP responses must not copy dynamic Admin context text",
    "error.to_string()": "dashboard HTTP responses must not render diagnostic Display text",
    "self.to_string()": "dashboard HTTP responses must not render DashboardError Display text",
}


def dashboard_http_boundary_message(line: str) -> str | None:
    for term, message in DASHBOARD_HTTP_FORBIDDEN_TERMS.items():
        if term in line:
            return message
    return None


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

    findings = []
    for line_number, line in iter_non_test_lines(error_path):
        if message := dashboard_http_boundary_message(line):
            findings.append(Finding(error_path, line_number, message))

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


CLI_BOUNDARY_FORBIDDEN_TERMS = {
    "error.boundary_view()": "CLI output must use descriptor-validated safe views",
    "self.context.to_string()": "default CLI stderr must not render context",
    "component={}, exit_code={}": "default CLI stderr must contain only code and public message",
    '", context={"': "default CLI stderr must not include context",
    ".render_stderr()": "CLI sinks must consume the paired CliOutput projection",
    ".render_verbose_stderr()": "CLI sinks must consume the paired CliOutput projection",
    'eprintln!("failed to spawn rocketmq-admin-cli main thread: {error}")': (
        "admin CLI startup failures must use CliErrorView"
    ),
    'eprintln!("failed to initialize or shut down rocketmq-admin-cli: {error}")': (
        "admin CLI lifecycle failures must use CliErrorView"
    ),
    'eprintln!("rocketmq-admin-cli main thread terminated unexpectedly")': (
        "admin CLI thread failures must use CliErrorView"
    ),
    "std::process::exit(1)": "CLI failures must use descriptor-owned exit codes",
}


def cli_boundary_message(line: str) -> str | None:
    for term, message in CLI_BOUNDARY_FORBIDDEN_TERMS.items():
        if term in line:
            return message
    if ("eprintln!" in line or "println!" in line) and re.search(
        r"\{(?:error|source)(?::\?)?\}", line
    ):
        return "CLI top-level output must not render raw error or source values"
    return None


def cli_store_boundary_message(line: str) -> str | None:
    if "error.to_string()" in line or ".display().to_string()" in line:
        return "Store Inspect CLI must retain typed sources and must not stringify paths at the process boundary"
    return None


CLI_OUTPUT_MACRO = re.compile(r"\b(?P<macro>e?println!)\s*\((?P<body>.*?)\)\s*;", re.DOTALL)
CLI_RAW_VALUE = re.compile(r"\{\s*(?:error|source)(?::[^}]*)?\}|,\s*(?:error|source)\b")


def cli_raw_output_sinks(source: str) -> list[tuple[int, str]]:
    findings: list[tuple[int, str]] = []
    for match in CLI_OUTPUT_MACRO.finditer(source):
        macro = match.group("macro")
        body = match.group("body")
        line_number = source.count("\n", 0, match.start()) + 1
        if macro == "eprintln!" and "output.stderr()" not in body:
            findings.append((line_number, "CLI stderr must use the single CliOutput stderr sink"))
        elif macro == "println!" and CLI_RAW_VALUE.search(body):
            findings.append((line_number, "CLI stdout must not render raw error or source values"))
    return findings


def check_cli_boundary() -> list[Finding]:
    paths = [
        ROOT / "rocketmq-error" / "src" / "cli.rs",
        ROOT / "rocketmq-tools" / "rocketmq-admin" / "rocketmq-admin-cli" / "src" / "main.rs",
        ROOT / "rocketmq-tools" / "rocketmq-admin" / "rocketmq-admin-cli" / "src" / "rocketmq_cli.rs",
        ROOT / "rocketmq-tools" / "rocketmq-store-inspect" / "src" / "bin" / "rocketmq_cli.rs",
    ]
    findings: list[Finding] = []
    for path in paths:
        if not path.exists():
            findings.append(Finding(path, 1, "CLI error boundary file is missing"))
            continue
        for line_number, line in iter_non_test_lines(path):
            if message := cli_boundary_message(line):
                findings.append(Finding(path, line_number, message))
            if path == paths[-1] and (message := cli_store_boundary_message(line)):
                findings.append(Finding(path, line_number, message))
        for line_number, message in cli_raw_output_sinks(read_text(path)):
            if not is_test_context(path, line_number):
                findings.append(Finding(path, line_number, message))
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
    retry_path = ROOT / "rocketmq-client" / "src" / "common" / "retry_policy.rs"
    required_tokens = {
        retry_path: [
            "pub(crate) struct RetryPolicy",
            "pub(crate) enum RetryInput",
            "RetryInput::Transport(error)",
            "descriptor: error.shared_error().descriptor()",
            "stage: error.request_stage()",
            "RetryInput::Rejected(rejection)",
            "RetryInput::Contract(_)",
            "RetryInput::Response { code, retry_after, .. }",
            "OutboundRequestStage::BeforeWrite",
            "producer_retry_response_codes",
            "codes.contains(&code)",
            "pub(crate) fn producer_send_fault_decision",
            "RetryInput::Response { .. }",
        ],
        ROOT
        / "rocketmq-client"
        / "src"
        / "producer"
        / "producer_impl"
        / "default_mq_producer_impl"
        / "retry.rs": [
            "RetryPolicy::decide(",
        ],
        ROOT
        / "rocketmq-client"
        / "src"
        / "implementation"
        / "mq_client_api_impl"
        / "producer_retry.rs": [
            "RetryPolicy::decide(",
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
        "format!(": "client retry decisions must use descriptor recovery policy, not local text construction",
        "fields::PHASE": "client retry decisions must use typed request stage, not PHASE context",
        ".source()": "client retry decisions must not infer policy from error sources",
        "downcast_ref": "client retry decisions must not infer policy through runtime downcast",
        "retry_decision": "legacy client retry decision authority must not return",
        "ClientRetryDecision": "legacy client retry decision type must not return",
        "ClientRetryEffect": "legacy client retry effect type must not return",
        "producer_send_retry_decision": "legacy producer retry decision helper must not return",
        "retry_policy_error": "legacy retry error unwrapping helper must not return",
        "SharedRocketMQError": "legacy shared compatibility carrier must not return",
        "RocketMQError::Network": "legacy Network carrier must not return",
    }
    return [*findings, *scan_forbidden_terms([retry_path], forbidden)]


def check_error_descriptor_contract() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-error" / "src" / "descriptor.rs": [
            "pub struct ErrorDescriptor",
            "pub(crate) const fn try_new(",
            "pub const fn recovery_hint(&self) -> RecoveryHint",
        ],
        ROOT / "rocketmq-error" / "src" / "context.rs": [
            "pub(crate) fn public_projection(&self, descriptor: &'static ErrorDescriptor) -> Self",
            "if matches!(descriptor.exposure(), Exposure::Generic)",
        ],
        ROOT / "rocketmq-error" / "src" / "catalog.rs": [
            "macro_rules! define_error_catalog",
            "pub const ALL_DESCRIPTORS: &[ErrorDescriptor]",
            "pub fn descriptor_by_code(code: &str)",
        ],
        ROOT / "rocketmq-error" / "src" / "domain.rs": [
            "fn descriptor(&self) -> &'static ErrorDescriptor",
            "BoundaryErrorView::new(self.descriptor(), self.context())",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_descriptor_catalog.rs": [
            "EXPECTED_DESCRIPTOR_SNAPSHOTS.len(), 128",
            "descriptor_catalog_snapshot_is_exact",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_redaction.rs": [
            "rocketmq_error_exposes_public_message_and_redacted_context",
            "source_present=<redacted>",
            "view.context().is_empty()",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error descriptor contract file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required error descriptor contract token missing: {needle}"))
    return findings


def is_source_stringification_allowlisted(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return rel in SOURCE_STRINGIFICATION_ALLOWLIST


FORMAT_MACRO = re.compile(r"\bformat\s*!\s*(?P<opening>\()")
FORMAT_STRING_LITERAL = re.compile(
    r'\s*(?:"(?:\\.|[^"\\])*"|r(?P<hash>#{0,255})".*?"(?P=hash))',
    re.DOTALL,
)
SOURCE_INTERPOLATION = re.compile(r"\{(error|err|e|source)(?::[^}]*)?\}")


def is_backend_source_path(relative_path: str) -> bool:
    return any(
        relative_path == root or relative_path.startswith(f"{root}/")
        for root in ("rocketmq-store-rocksdb", "rocketmq-tieredstore")
    )


def source_stringification_message(
    relative_path: str,
    code: str,
    *,
    format_source_interpolation: bool = False,
) -> str | None:
    backend_source_to_text = is_backend_source_path(relative_path) and (
        re.search(r"\b(error|err|e|source)\.to_string\(\)", code) is not None
        or format_source_interpolation
    )
    if (backend_source_to_text or is_source_stringification_line(code)) and relative_path not in SOURCE_STRINGIFICATION_ALLOWLIST:
        return "source stringification requires a typed source wrapper or SOURCE_STRINGIFICATION_ALLOWLIST entry"
    return None


def source_interpolating_format_ranges(analysis: RustSourceAnalysis) -> tuple[tuple[int, int], ...]:
    """Find real format! calls whose first string literal interpolates a source."""
    ranges: list[tuple[int, int]] = []
    for macro in FORMAT_MACRO.finditer(analysis.masked):
        if any(start <= macro.start() < end for start, end in analysis.test_ranges):
            continue
        opening = macro.start("opening")
        closing = rust_hygiene_guard.matching_delimiter(analysis.masked, opening, "(", ")")
        if closing is None:
            continue
        arguments = analysis.source[opening + 1 : closing]
        format_string = FORMAT_STRING_LITERAL.match(arguments)
        if format_string is not None and SOURCE_INTERPOLATION.search(format_string.group(0)) is not None:
            ranges.append((macro.start(), closing + 1))
    return tuple(ranges)


def find_source_stringification(paths: Iterable[Path]) -> list[Finding]:
    """Find source-to-text promotion outside narrowly recognized test modules."""
    findings: list[Finding] = []
    for path in paths:
        relative_path = rel_path(path)
        analysis = analysis_for(path)
        format_ranges = (
            source_interpolating_format_ranges(analysis)
            if is_backend_source_path(relative_path)
            else ()
        )
        for start, _ in format_ranges:
            message = source_stringification_message(
                relative_path,
                "",
                format_source_interpolation=True,
            )
            if message is not None:
                findings.append(Finding(path, bisect.bisect_right(analysis.line_starts, start), message))
        for line_number, _, masked_line in iter_non_test_line_pairs(path):
            line_start = analysis.line_starts[line_number - 1]
            code, _ = without_test_ranges(masked_line, line_start, format_ranges)
            message = source_stringification_message(relative_path, code)
            if message is not None:
                findings.append(Finding(path, line_number, message))
    return sorted(findings, key=lambda finding: (finding.path, finding.line, finding.message))


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


def check_store_error_detail_stringification(paths: list[Path]) -> list[Finding]:
    """Reject typed StoreError sources interpolated into private detail."""
    source_interpolation = re.compile(r"\{(error|err|e)(?::[^}]*)?\}")
    facade_detail_terms = (
        ".with_detail(",
        "store_config_error(",
        "store_unsupported_error(",
        "store_internal_error(",
        "store_read_error(",
        "store_write_error(",
    )
    findings: list[Finding] = []
    for path in paths:
        lines = read_text(path).splitlines()
        for index, line in enumerate(lines):
            if "format!(" not in line:
                continue
            format_window = "\n".join(lines[index : min(len(lines), index + 8)])
            if source_interpolation.search(format_window) is None:
                continue
            line_number = index + 1
            if is_test_context(path, line_number):
                continue
            promotion_window = "\n".join(lines[max(0, index - 5) : min(len(lines), index + 8)])
            if any(term in promotion_window for term in facade_detail_terms):
                findings.append(
                    Finding(
                        path,
                        line_number,
                        "StoreError promotion must retain the typed source with with_source instead of formatting it into private detail",
                    )
                )
    return findings


def backend_source_loss_messages(relative_path: str, source: str) -> list[str]:
    """Return source-loss findings for the enumerated backend repair sites."""
    messages: list[str] = []
    for pattern, message in BACKEND_SOURCE_LOSS_PATTERNS.get(relative_path, ()):
        if re.search(pattern, source, re.DOTALL):
            messages.append(message)
    return messages


def check_backend_source_preservation() -> list[Finding]:
    findings: list[Finding] = []
    for relative_path, required in BACKEND_SOURCE_PRESERVATION_TOKENS.items():
        path = ROOT / relative_path
        if not path.exists():
            findings.append(Finding(path, 1, "required backend source-preservation file is missing"))
            continue
        source = "\n".join(line for _, line in iter_non_test_lines(path))
        for token, minimum in required:
            if source.count(token) < minimum:
                findings.append(Finding(path, 1, f"required backend source-preservation token missing: {token}"))
        for message in backend_source_loss_messages(relative_path, source):
            findings.append(Finding(path, 1, message))
    return findings


def check_source_stringification_allowlist() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-store-api" / "src" / "error.rs": [
            "error: CanonicalError",
            "private_detail: Option<Sensitive<String>>",
            "pub fn with_source",
            "pub fn with_boxed_source",
            "pub fn public_view",
            "pub fn diagnostic_view",
            "impl StdError for StoreError",
            "self.error.source()",
        ],
        ROOT / "rocketmq-store-local" / "src" / "mapped_file" / "mapped_file_error.rs": [
            "MmapFailed(#[source] io::Error)",
            "FlushFailed(#[source] io::Error)",
        ],
        ROOT / "rocketmq-store-rocksdb" / "src" / "error.rs": [
            "source: ::rocksdb::Error",
            "StoreComponent::RocksDb",
            ".with_source(source)",
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

    domain_paths: list[Path] = []
    for parts in SOURCE_STRINGIFICATION_DOMAIN_ROOTS:
        domain_paths.extend(rust_files_under(*parts))
    store_facade_paths = [
        *rust_files_under("rocketmq-store-api", "src"),
        *rust_files_under("rocketmq-store", "src"),
        *rust_files_under("rocketmq-store-local", "src"),
        *rust_files_under("rocketmq-store-rocksdb", "src"),
        *rust_files_under("rocketmq-tieredstore", "src"),
        *rust_files_under("rocketmq-broker", "src"),
        *rust_files_under("rocketmq-controller", "src"),
        *rust_files_under("rocketmq-tools", "rocketmq-store-inspect", "src"),
    ]
    findings.extend(check_store_error_detail_stringification(store_facade_paths))
    findings.extend(check_backend_source_preservation())
    findings.extend(find_source_stringification(domain_paths))
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
        ROOT / "rocketmq-error" / "src" / "field.rs": [
            "pub enum ContextVisibility",
            "    Public,",
            "    Diagnostic,",
            "SecretPresenceOnly",
            "pub enum FieldValueKind",
            "pub struct FieldSchema",
            "pub struct FieldKey<T>",
        ],
        ROOT / "rocketmq-error" / "src" / "context.rs": [
            'pub const REDACTED: &str = "<redacted>";',
            'f.write_str("Sensitive(<redacted>)")',
            "pub fn with_secret_presence(mut self, key: FieldKey<SecretPresenceField>) -> Self",
            "pub fn public_fields(&self)",
            "pub const fn is_truncated(&self) -> bool",
        ],
        ROOT / "rocketmq-error" / "src" / "view.rs": [
            "pub struct PublicErrorView",
            "pub struct DiagnosticView",
            "pub enum ViewContextViolation",
            "ViewValueRef::Redacted",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_safe_views.rs": [
            "const SENTINEL",
            "secret_sentinel_never_enters_safe_views_or_violations",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_redaction.rs": [
            "error_context_redacts_sensitive_fields",
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_visibility.rs": [
            "const SENTINEL",
            "Bearer token-secret secret_key=sk signature=sig password=pw",
            "sentinel_never_enters_context_or_safe_boundary_output",
            "with_secret_presence(fields::CREDENTIALS_PRESENT)",
        ],
        ROOT / "rocketmq-model" / "src" / "common" / "base" / "plain_access_config.rs": [
            "debug_and_display_redact_secret_key",
            "<redacted>",
        ],
        ROOT / "rocketmq-client" / "src" / "common" / "session_credentials.rs": [
            "session_credentials_debug_redacts_sensitive_fields",
            "session_credentials_display",
        ],
        ROOT / "rocketmq-protocol" / "src" / "protocol" / "body" / "user_info.rs": [
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
            "auth_model_debug_redacts_password",
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

    context_path = ROOT / "rocketmq-error" / "src" / "context.rs"
    context_text = read_text(context_path)
    for removed_builder in ("with_field", "push_field", "with_sensitive", "push_sensitive"):
        if re.search(rf"pub\s+fn\s+{removed_builder}\b", context_text):
            findings.append(Finding(context_path, 1, f"public string-key context builder remains: {removed_builder}"))

    debug_field_pattern = re.compile(r'\.field\("([^"]+)",')
    redaction_paths = [
        *rust_files_under("rocketmq-error", "src"),
        *rust_files_under("rocketmq-model", "src"),
        *rust_files_under("rocketmq-client", "src"),
        *rust_files_under("rocketmq-protocol", "src"),
        *rust_files_under("rocketmq-transport", "src"),
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


def authorization_decision_contract_message(relative_path: str, line: str) -> str | None:
    """Reject the small set of constructs that can merge auth denials with failures again."""
    if relative_path == "rocketmq-security-api/src/lib.rs" and re.search(r"\bpub\s+enum\s+Decision\b", line):
        return "the final authorization decision must use AuthorizationDecision"
    if relative_path.startswith("rocketmq-auth/src/authorization/") and (
        "AuthorizationError::PermissionDenied" in line or "PermissionDenied {" in line
    ):
        return "permission denial must be an AuthorizationDecision value, not an operational error"
    if relative_path == "rocketmq-auth/src/authorization/provider.rs" and "let message = error.to_string();" in line:
        return "authorization errors must not be stringified before boundary projection"
    if relative_path.startswith("rocketmq-transport/src/dispatch/authorized_dispatcher") and "reason.to_string()" in line:
        return "transport authorization denial remarks must use the fixed catalog message"
    return None


def check_authorization_decision_contract() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-security-api" / "src" / "layered_authorization.rs": [
            "pub enum AuthorizationDecision",
            "Deny(AuthorizationDenial)",
            "pub enum AuthorizationDenial",
            ") -> LayerEvaluation<AuthorizationDecision>",
        ],
        ROOT / "rocketmq-auth" / "src" / "authorization" / "provider.rs": [
            ") -> AuthServiceResult<AuthorizationDecision>",
        ],
        ROOT / "rocketmq-transport" / "src" / "dispatch" / "authorized_dispatcher.rs": [
            "AUTH_PERMISSION_DENIED.public_message()",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required authorization decision contract file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required authorization decision token missing: {needle}"))

    paths = [
        ROOT / "rocketmq-security-api" / "src" / "lib.rs",
        *rust_files_under("rocketmq-auth", "src", "authorization"),
        *rust_files_under("rocketmq-transport", "src", "dispatch", "authorized_dispatcher"),
    ]
    for path in paths:
        for line_number, line in iter_non_test_lines(path):
            message = authorization_decision_contract_message(rel_path(path), line)
            if message is not None:
                findings.append(Finding(path, line_number, message))
    return findings


def current_error_codes() -> list[str]:
    kind_path = ROOT / "rocketmq-error" / "src" / "kind.rs"
    match = re.search(
        r"pub const fn code\(self\).*?ErrorCode::new\(match self \{(.*?)\}\)\s*\}",
        read_text(kind_path),
        re.DOTALL,
    )
    if match is None:
        return []
    return re.findall(r'Self::\w+\s*=>\s*"([^"]+)"', match.group(1))


def check_error_governance_artifacts() -> list[Finding]:
    required_tokens = {
        ROOT / "scripts" / "check-error-hygiene.ps1": [
            "error_architecture_guard.py",
            "python3",
            "python",
        ],
        ROOT / "CONTRIBUTING.md": [
            "### Error architecture",
            "python scripts/error_architecture_guard.py",
            r".\scripts\check-error-hygiene.ps1",
        ],
    }
    findings: list[Finding] = []
    for path, needles in required_tokens.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error governance artifact is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required error governance token missing: {needle}"))

    return findings


def run() -> int:
    checks = [
        ("core public surface", check_error_and_model_public_surface),
        ("processor boundary mappings", check_processor_boundary_mappings),
        ("processor generic response allowlist", check_processor_generic_response_allowlist),
        ("required mapping adapters", check_required_mapping_adapters),
        ("proxy grpc boundary", check_proxy_grpc_boundary),
        ("proxy remoting boundary", check_proxy_remoting_boundary),
        ("dashboard http boundary", check_dashboard_http_boundary),
        ("cli boundary", check_cli_boundary),
        ("client callback boundary", check_client_callback_boundary),
        ("client retry boundary", check_client_retry_boundary),
        ("error descriptor contract", check_error_descriptor_contract),
        ("source stringification allowlist", check_source_stringification_allowlist),
        ("internal error allowlist", check_internal_error_allowlist),
        ("anyhow result allowlist", check_anyhow_result_allowlist),
        ("redaction guards", check_redaction_guards),
        ("authorization decision contract", check_authorization_decision_contract),
        ("error governance artifacts", check_error_governance_artifacts),
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
