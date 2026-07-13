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

from __future__ import annotations

import re
import tomllib
import unittest
from pathlib import Path
from typing import Any
from typing import Callable


ROOT = Path(__file__).resolve().parents[2]
LOCAL_CRATE = ROOT / "rocketmq-store-local"
STORE_CRATE = ROOT / "rocketmq-store"
LEAF_FILES = {
    "direct_io.rs",
    "flush_strategy.rs",
    "io_uring_impl.rs",
    "mapped_buffer.rs",
    "mapped_file_error.rs",
    "metrics.rs",
}
KERNEL_ITEMS = {
    "MappedFileProgress": "struct",
    "ReferenceResource": "trait",
    "ReferenceResourceBase": "struct",
    "ReferenceResourceCounter": "struct",
}
FILE_ITEMS = {
    "MappedFileStorage": "struct",
    "FilePreallocateOutcome": "enum",
    "PREALLOCATE_UNSUPPORTED_ERRNO": "const",
    "classify_file_preallocate_result": "fn",
    "preallocate_file": "fn",
}
FILE_PLATFORM_REEXPORTS = set(FILE_ITEMS) - {"MappedFileStorage"}
MAPPING_ITEMS = {
    "LazyMmapStats": "struct",
    "MappedFileMapping": "struct",
}
DEFAULT_MAPPED_FILE_ALIAS_BRACE_USE_ALLOWLIST = {
    "use crate::utils::ffi::mlock as lock_memory",
    "use crate::utils::ffi::munlock as unlock_memory",
    "use windows::Win32::System::Memory::{VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT}",
}
DEFAULT_MAPPED_FILE_TYPE_ALIAS_ALLOWLIST = {"type Target = [u8]"}
PROGRESS_FIELDS = {
    "file_size",
    "wrote_position",
    "committed_position",
    "flushed_position",
    "store_timestamp",
    "last_flush_time",
    "start_timestamp",
    "stop_timestamp",
}
CANONICAL_ITEMS = {
    "DirectIoBuffer": ("struct", "direct_io.rs"),
    "DirectIoRequest": ("struct", "direct_io.rs"),
    "DirectIoValidationError": ("enum", "direct_io.rs"),
    "FlushStrategy": ("enum", "flush_strategy.rs"),
    "IoUringBackendStatus": ("enum", "io_uring_impl.rs"),
    "IoUringFallbackReason": ("enum", "io_uring_impl.rs"),
    "IoUringOpcodeSupport": ("struct", "io_uring_impl.rs"),
    "IoUringRuntimeCapability": ("struct", "io_uring_impl.rs"),
    "LinuxKernelVersion": ("struct", "io_uring_impl.rs"),
    "MappedBuffer": ("struct", "mapped_buffer.rs"),
    "MappedFileError": ("enum", "mapped_file_error.rs"),
    "MappedFileMetrics": ("struct", "metrics.rs"),
    "MappedFileResult": ("type", "mapped_file_error.rs"),
    "io_uring_backend_status": ("fn", "io_uring_impl.rs"),
}
COMMIT_LOG_CANONICAL_ITEMS = {
    "LoadStatistics": ("struct", "load.rs"),
    "RecoveryMmapAdvice": ("enum", "load.rs"),
    "RecoveryFilePrefetch": ("enum", "load.rs"),
    "RecoveryStatistics": ("struct", "recovery.rs"),
    "AbnormalRecoveryFileRange": ("struct", "recovery.rs"),
    "AbnormalRecoveryWindow": ("struct", "recovery.rs"),
    "plan_abnormal_recovery_window_from_ranges": ("fn", "recovery.rs"),
    "MESSAGE_MAGIC_CODE": ("const", "record.rs"),
    "MESSAGE_MAGIC_CODE_V2": ("const", "record.rs"),
    "BLANK_MAGIC_CODE": ("const", "record.rs"),
    "is_blank_message": ("fn", "record.rs"),
    "CommitLogFrameSource": ("trait", "record.rs"),
    "CommitLogFrameCursor": ("struct", "record.rs"),
}
COMMIT_LOG_LOAD_OWNER_ITEMS = {
    "CommitLogFileMetadata": "struct",
    "CommitLogFileLoadDecision": "enum",
    "CommitLogFileValidationError": "struct",
    "validate_commit_log_file": "fn",
}
COMMIT_LOG_RECORD_PARSER_ITEMS = {
    "CommitLogRecordVersion": "enum",
    "CommitLogRecordBodyMode": "enum",
    "CommitLogRecordChecksum": "trait",
    "CommitLogRecordField": "enum",
    "CommitLogRecordErrorKind": "enum",
    "CommitLogRecordError": "struct",
    "CommitLogRecord": "struct",
    "CommitLogRecordOutcome": "enum",
    "decode_commit_log_record": "fn",
}
NORMAL_RECOVERY_ITEMS = {
    "NormalRecoveryPolicy": "enum",
    "NormalRecoveryEvent": "enum",
    "NormalRecoveryAction": "enum",
    "NormalRecoveryOffsetError": "enum",
    "NormalRecoverySummary": "struct",
    "NormalRecoveryState": "struct",
}
COMMIT_LOG_FACADE_ITEMS = {
    "commit_log_loader.rs": {
        "LoadStatistics": "load",
        "RecoveryMmapAdvice": "load",
        "RecoveryFilePrefetch": "load",
    },
    "commit_log_recovery.rs": {
        "RecoveryStatistics": "recovery",
        "AbnormalRecoveryFileRange": "recovery",
        "AbnormalRecoveryWindow": "recovery",
        "plan_abnormal_recovery_window_from_ranges": "recovery",
        "is_blank_message": "record",
    },
}
FACADE_ROOT_ITEMS = {
    "DirectIoBuffer",
    "DirectIoRequest",
    "DirectIoValidationError",
    "FlushStrategy",
    "IoUringBackendStatus",
    "io_uring_backend_status",
    "MappedBuffer",
    "MappedFileError",
    "MappedFileMetrics",
    "MappedFileResult",
}
FORBIDDEN_DEPENDENCIES = {
    "rocketmq-common",
    "rocketmq-rust",
    "rocketmq-remoting",
    "rocketmq-store",
    "rocketmq-broker",
    "rocksdb",
    "rocketmq-store-rocksdb",
    "rocketmq-tieredstore",
}
FORBIDDEN_SOURCE_TOKENS = (
    "rocketmq_common",
    "rocketmq_rust",
    "rocketmq_remoting",
    "rocketmq_store",
    "rocketmq_broker",
    "rocksdb",
    "rocketmq_store_rocksdb",
    "rocketmq_tieredstore",
)


def dependency_tables(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    tables = [
        manifest.get("dependencies", {}),
        manifest.get("build-dependencies", {}),
    ]
    for target in manifest.get("target", {}).values():
        tables.extend(
            [
                target.get("dependencies", {}),
                target.get("build-dependencies", {}),
            ]
        )
    return tables


def active_facade_reexports(source: str) -> set[str]:
    source = active_rust_source(source)
    return set(re.findall(r"pub\s+use\s+rocketmq_store_local::mapped_file::([A-Za-z_][A-Za-z0-9_]*)\s*;", source))


def active_commit_log_facade_reexports(source: str) -> dict[str, str]:
    source = active_rust_source(source)
    return {
        item: module
        for module, item in re.findall(
            r"pub\s+use\s+rocketmq_store_local::commit_log::(load|recovery|record)::([A-Za-z_][A-Za-z0-9_]*)\s*;",
            source,
        )
    }


def commit_log_record_owner_occurrences(
    sources: dict[Path, str], item: str
) -> list[tuple[Path, str]]:
    return file_item_owner_occurrences(sources, item)


def commit_log_record_boundary_violations(source: str) -> list[str]:
    active = active_rust_source(source)
    violations: list[str] = []
    if re.search(
        r"\bdyn\s+(?:(?:::)?[A-Za-z_][A-Za-z0-9_]*::)*CommitLogFrameSource\b",
        active,
    ):
        violations.append("dynamic CommitLogFrameSource")
    if any(
        kind == "use" and (" as " in body or "{" in body)
        for kind, _, body, _ in active_import_records(source)
    ):
        violations.append("forbidden alias/brace import")
    return violations


def commit_log_record_parser_boundary_violations(source: str) -> list[str]:
    production = source.split("#[cfg(test)]", maxsplit=1)[0]
    active = active_rust_source(production)
    violations: list[str] = []
    if re.search(
        r"pub\s+fn\s+decode_commit_log_record\s*<[^>]+>\s*\(\s*input\s*:\s*&Bytes\b",
        active,
    ) is None:
        violations.append("decoder input must be read-only Bytes")
    if re.search(
        r"\bdyn\s+(?:(?:::)?[A-Za-z_][A-Za-z0-9_]*::)*CommitLogRecordChecksum\b",
        active,
    ):
        violations.append("dynamic checksum port")
    if re.search(r"\bBox\b", active):
        violations.append("per-record heap allocation")
    if re.search(r"\btype\s+[A-Za-z_][A-Za-z0-9_]*\s*=", active):
        violations.append("parser type alias")
    declared_boundary = re.search(r"\bif\s+input\.len\s*\(\s*\)\s*<\s*declared_len\b", active)
    blank_return = re.search(r"\bif\s+magic_code\s*==\s*BLANK_MAGIC_CODE\b", active)
    if blank_return is not None and (declared_boundary is None or declared_boundary.start() > blank_return.start()):
        violations.append("blank marker bypasses declared frame boundary")
    if re.search(r"\buse\s+bytes::Buf(?:Mut)?\s*;", active) or re.search(
        r"\.(?:get_i16|get_i32|get_i64|copy_to_bytes|advance)\s*\(",
        active,
    ):
        violations.append("unchecked Buf cursor parsing")
    if any(
        kind == "use" and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(production)
    ):
        violations.append("forbidden alias/brace/glob import")
    return violations


def named_function_body(source: str, function_name: str) -> str | None:
    active = active_rust_source(source)
    match = re.search(rf"\bfn\s+{re.escape(function_name)}\b", active)
    if match is None:
        return None
    opening_brace = active.find("{", match.end())
    if opening_brace == -1:
        return None
    extracted = braced_body(active, opening_brace)
    return None if extracted is None else extracted[0]


def named_function_signature(source: str, function_name: str) -> str | None:
    active = active_rust_source(source)
    match = re.search(rf"\bpub\s+async\s+fn\s+{re.escape(function_name)}\b", active)
    if match is None:
        return None
    opening_brace = active.find("{", match.end())
    if opening_brace == -1:
        return None
    return re.sub(r"\s+", "", active[match.start():opening_brace])


def active_item_body(source: str, item_kind: str, item_name: str) -> str | None:
    active = active_rust_source(source)
    match = re.search(rf"\b{re.escape(item_kind)}\s+{re.escape(item_name)}\b", active)
    if match is None:
        return None
    opening_brace = active.find("{", match.end())
    if opening_brace == -1:
        return None
    extracted = braced_body(active, opening_brace)
    return None if extracted is None else extracted[0]


def active_impl_body(source: str, type_name: str) -> str | None:
    active = active_rust_source(source)
    match = re.search(rf"\bimpl\s+{re.escape(type_name)}\b", active)
    if match is None:
        return None
    opening_brace = active.find("{", match.end())
    if opening_brace == -1:
        return None
    extracted = braced_body(active, opening_brace)
    return None if extracted is None else extracted[0]


def normal_recovery_event_match_body(function_body: str, event: str) -> str | None:
    active = active_rust_source(function_body)
    match = re.search(
        rf"\bmatch\s+normal_recovery\.apply\s*\(\s*NormalRecoveryEvent::{re.escape(event)}\b",
        active,
    )
    if match is None:
        return None
    call_open = active.find("(", match.start())
    if call_open == -1:
        return None
    depth = 0
    for index in range(call_open, len(active)):
        if active[index] == "(":
            depth += 1
        elif active[index] == ")":
            depth -= 1
            if depth == 0:
                match_open = active.find("{", index + 1)
                if match_open == -1:
                    return None
                extracted = braced_body(active, match_open)
                return None if extracted is None else extracted[0]
    return None


def abnormal_recovery_event_match(
    function_body: str, event: str
) -> tuple[int, str, int] | None:
    active = active_rust_source(function_body)
    match = re.search(
        rf"\bmatch\s+abnormal_recovery\.apply\s*\(\s*AbnormalRecoveryEvent::{re.escape(event)}\b",
        active,
    )
    if match is None:
        return None
    call_open = active.find("(", match.start())
    if call_open == -1:
        return None
    depth = 0
    for index in range(call_open, len(active)):
        if active[index] == "(":
            depth += 1
        elif active[index] == ")":
            depth -= 1
            if depth == 0:
                match_open = active.find("{", index + 1)
                if match_open == -1:
                    return None
                extracted = braced_body(active, match_open)
                if extracted is None:
                    return None
                match_body, match_end = extracted
                return match.start(), match_body, match_end
    return None


def normal_recovery_state_boundary_violations(source: str) -> list[str]:
    production = source.split("#[cfg(test)]", maxsplit=1)[0]
    active = active_rust_source(production)
    violations: list[str] = []
    state_body = active_item_body(production, "struct", "NormalRecoveryState")
    if state_body is None or re.sub(r"\s+", "", state_body) != (
        "last_valid_offset:u64,truncate_offset:u64,policy:NormalRecoveryPolicy,"
    ):
        violations.append("Local normal recovery state fields changed")

    event_body = active_item_body(production, "enum", "NormalRecoveryEvent")
    if event_body is None or re.search(r"\bSourceEnded\s*\{", event_body):
        violations.append("SourceEnded must remain a unit event")

    impl_body = active_impl_body(production, "NormalRecoveryState")
    if impl_body is None:
        violations.append("Local normal recovery reducer is missing")
    else:
        normalized = re.sub(r"\s+", "", impl_body)
        required_transitions = [
            "NormalRecoveryPolicy::Standard=>(NormalRecoveryAction::ContinueRecord,start_offset,end_offset)",
            "NormalRecoveryPolicy::Optimized=>(NormalRecoveryAction::ContinueRecord,end_offset,end_offset)",
        ]
        if any(transition not in normalized for transition in required_transitions):
            violations.append("Local normal recovery start/end transition changed")
        if re.search(r"\b(?:segment_base\s*\+\s*relative_start|start_offset\s*\+\s*size)\b", impl_body):
            violations.append("Local normal recovery uses unchecked offset arithmetic")
        if re.search(r"\b(?:ArcMut|DispatchRequest|MessageStoreConfig|controller|segment_index|while|loop)\b", impl_body):
            violations.append("Local normal recovery absorbed Store orchestration")
        if re.search(r"\b(?:as|unwrap|expect|panic)\b", impl_body):
            violations.append("Local normal recovery uses forbidden conversion or panic")
        if re.search(
            r"\bpub\s+const\s+fn\s+try_new\s*\(\s*initial_offset\s*:\s*u64\s*,\s*"
            r"policy\s*:\s*NormalRecoveryPolicy\s*,?\s*\)\s*"
            r"->\s*Result\s*<\s*Self\s*,\s*NormalRecoveryOffsetError\s*>",
            impl_body,
        ) is None:
            violations.append("Local normal recovery constructor must be fallible")
        if re.search(r"\bfn\s+new\b", impl_body) or len(re.findall(r"\bfn\s+try_new\b", impl_body)) != 1:
            violations.append("Local normal recovery constructor owner changed")
        if re.search(
            r"\bif\s+initial_offset\s*>\s*MAX_SIGNED_OFFSET\s*\{\s*"
            r"return\s+Err\s*\(\s*NormalRecoveryOffsetError::OffsetExceedsI64\s*\{\s*"
            r"offset\s*:\s*initial_offset\s*,?\s*\}\s*\)\s*;\s*\}",
            impl_body,
        ) is None:
            violations.append("Local normal recovery initial offset guard changed")

    if any(
        kind == "use" and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(production)
    ):
        violations.append("Local normal recovery forbids alias/brace/glob imports")
    if re.search(r"\bdyn\b", active):
        violations.append("Local normal recovery forbids dynamic ports")
    return violations


def abnormal_recovery_state_boundary_violations(source: str) -> list[str]:
    production = source.split("#[cfg(test)]", maxsplit=1)[0]
    violations: list[str] = []
    fields = active_struct_fields(production, "AbnormalRecoveryState")
    expected_fields = [
        ("last_valid_offset", "u64"),
        ("confirm_valid_offset", "u64"),
        ("truncate_offset", "u64"),
        ("policy", "AbnormalRecoveryPolicy"),
    ]
    if fields != expected_fields:
        violations.append("Local abnormal recovery state fields changed")

    gate_body = active_item_body(production, "enum", "AbnormalRecoveryDispatchGate")
    if gate_body is None or re.sub(r"\s+", "", gate_body) != "Ungated,ConfirmBounded{confirm_offset:u64},":
        violations.append("Local abnormal recovery dispatch gate changed")
    event_body = active_item_body(production, "enum", "AbnormalRecoveryEvent")
    normalized_event = "" if event_body is None else re.sub(r"\s+", "", event_body)
    expected_message = (
        "MessageAccepted{segment_base:u64,relative_start:u64,validated_size:u64,"
        "confirm_candidate_end:i64,dispatch_gate:AbnormalRecoveryDispatchGate,}"
    )
    if expected_message not in normalized_event or "input_size:u64" in normalized_event:
        violations.append("Local abnormal recovery message event fields changed")

    action_body = active_item_body(production, "enum", "AbnormalRecoveryAction")
    action_names = [] if action_body is None else re.findall(r"\b([A-Z][A-Za-z0-9_]*)\b", action_body)
    expected_actions = [
        "ContinueRecord",
        "DispatchMessage",
        "SkipMessageDispatch",
        "NotifyFileEndAndContinueNextSegment",
        "ContinueNextSegment",
        "StopRecovery",
    ]
    if action_names != expected_actions:
        violations.append("Local abnormal recovery action matrix changed")

    impl_body = active_impl_body(production, "AbnormalRecoveryState")
    if impl_body is None:
        violations.append("Local abnormal recovery reducer missing")
    else:
        normalized = re.sub(r"\s+", "", impl_body)
        required = [
            "confirm_candidate<=confirm_offset",
            "self.policy==AbnormalRecoveryPolicy::Optimized||matches!(dispatch_gate,AbnormalRecoveryDispatchGate::ConfirmBounded{..})",
            "AbnormalRecoveryPolicy::Standard=>AbnormalRecoveryAction::StopRecovery",
            "AbnormalRecoveryPolicy::Optimized=>AbnormalRecoveryAction::ContinueNextSegment",
            "AbnormalRecoveryAction::NotifyFileEndAndContinueNextSegment",
            "letmessage_start=self.truncate_offset",
            "(message_end,message_start)",
            "segment_base.checked_add(relative_start)",
            "(message_end,message_end)",
        ]
        if any(fragment not in normalized for fragment in required):
            violations.append("Local abnormal recovery transition matrix changed")
        if re.search(r"\b(?:segment_base\s*\+\s*relative_start|message_start\s*\+\s*validated_size)\b", impl_body):
            violations.append("Local abnormal recovery uses unchecked offset arithmetic")
        if re.search(r"\b(?:ArcMut|DispatchRequest|MessageStoreConfig|controller|checkpoint|window|stats|dyn)\b", impl_body):
            violations.append("Local abnormal recovery absorbed Store orchestration")
        if re.search(r"\b(?:as|unwrap|expect|panic)\b", impl_body):
            violations.append("Local abnormal recovery uses forbidden conversion or panic")

    if any(
        kind == "use" and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(production)
    ):
        violations.append("Local abnormal recovery forbids alias/brace/glob imports")
    return violations


def store_normal_recovery_adapter_violations(commit_log: str) -> list[str]:
    violations: list[str] = []
    signatures = {
        name: (
            f"pubasyncfn{name}(&mutself,max_phy_offset_of_consume_queue:i64,"
            "mutmessage_store:ArcMut<LocalFileMessageStore>,)"
        )
        for name in [
            "recover_normally_optimized",
            "recover_normally",
            "recover_abnormally_optimized",
            "recover_abnormally",
        ]
    }
    for name, expected in signatures.items():
        if named_function_signature(commit_log, name) != expected:
            violations.append(f"{name} public signature changed")

    recovery_prefix = "rocketmq_store_local::commit_log::recovery::"
    recovery_imports = {
        body.removeprefix(recovery_prefix)
        for kind, _, body, _ in active_import_records(commit_log)
        if kind == "use"
        and body.startswith(recovery_prefix)
        and body.removeprefix(recovery_prefix).startswith("Normal")
    }
    expected_recovery_imports = {
        "NormalRecoveryAction",
        "NormalRecoveryEvent",
        "NormalRecoveryPolicy",
        "NormalRecoveryState",
    }
    if recovery_imports != expected_recovery_imports:
        violations.append("Store normal recovery imports must be exact Local imports")
    if any(
        kind == "use"
        and body.startswith(recovery_prefix)
        and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(commit_log)
    ):
        violations.append("Store normal recovery imports forbid alias/brace/glob")

    truncate_helper = named_function_body(commit_log, "should_truncate_normal_recovery_consume_queue")
    if truncate_helper is None or re.sub(r"\s+", "", truncate_helper) != (
        "max<0||u64::try_from(max).is_ok_and(|value|value>=truncate)"
    ):
        violations.append("Store normal recovery ConsumeQueue predicate changed")

    expected_events = ["SegmentStarted", "MessageAccepted", "Blank", "InvalidRecord", "SourceEnded"]
    for name, policy in [
        ("recover_normally", "Standard"),
        ("recover_normally_optimized", "Optimized"),
    ]:
        body = named_function_body(commit_log, name)
        if body is None:
            violations.append(f"{name} body missing")
            continue
        if body.count(f"NormalRecoveryPolicy::{policy}") != 1:
            violations.append(f"{name} Local policy construction changed")
        if len(re.findall(r"\bmatch\s+NormalRecoveryState::try_new\s*\(", body)) != 1:
            violations.append(f"{name} must explicitly handle fallible Local state construction")
        constructor_match = re.search(
            rf"let\s+mut\s+normal_recovery\s*=\s*match\s+NormalRecoveryState::try_new\s*\(\s*"
            rf"initial_offset\s*,\s*NormalRecoveryPolicy::{policy}\s*\)\s*\{{\s*"
            r"Ok\s*\(\s*state\s*\)\s*=>\s*state\s*,\s*"
            r"Err\s*\(\s*error\s*\)\s*=>\s*\{(?P<error>.*?)\}\s*\}\s*;",
            body,
            re.DOTALL,
        )
        if constructor_match is None or re.search(
            r"\bwarn\s*!\s*\(.*?\)\s*;\s*return\s*;",
            constructor_match.group("error"),
            re.DOTALL,
        ) is None:
            violations.append(f"{name} must log and return on Local state construction error")
        if "NormalRecoveryState::new" in body or re.search(r"\b(?:unwrap|expect|panic)\b", body):
            violations.append(f"{name} uses forbidden Local construction or panic")
        if re.search(r"\bmatch\s+NormalRecoveryPolicy\b", body):
            violations.append(f"{name} copied Local recovery policy match")
        for event in expected_events:
            if body.count(f"NormalRecoveryEvent::{event}") != 1:
                violations.append(f"{name} must route {event} through Local reducer")
        if len(re.findall(r"\bnormal_recovery\.apply\s*\(", body)) != 5:
            violations.append(f"{name} must apply exactly five Local recovery events")
        if len(re.findall(r"\bmatch\s+normal_recovery\.apply\s*\(", body)) != 4:
            violations.append(f"{name} must act on every record outcome from Local reducer")

        message_match = normal_recovery_event_match_body(body, "MessageAccepted")
        if message_match is None:
            violations.append(f"{name} MessageAccepted action match missing")
        else:
            for action in ["ContinueRecord", "ContinueNextSegment", "StopRecovery"]:
                if message_match.count(f"NormalRecoveryAction::{action}") != 1:
                    violations.append(f"{name} MessageAccepted {action} action changed")
            if re.search(
                r"Ok\s*\(\s*NormalRecoveryAction::ContinueRecord\s*\)\s*=>\s*\{\s*\}",
                message_match,
            ) is None:
                violations.append(f"{name} MessageAccepted ContinueRecord action changed")
            if policy == "Standard":
                continue_next = r"=>\s*break\s*,"
            else:
                continue_next = r"=>\s*\{\s*record_closed_segment\s*=\s*true\s*;\s*break\s*;\s*\}"
            if re.search(
                rf"Ok\s*\(\s*NormalRecoveryAction::ContinueNextSegment\s*\)\s*{continue_next}",
                message_match,
            ) is None:
                violations.append(f"{name} MessageAccepted ContinueNextSegment action changed")
            if re.search(
                r"Ok\s*\(\s*NormalRecoveryAction::StopRecovery\s*\)\s*=>\s*break\s+'segments\s*,",
                message_match,
            ) is None:
                violations.append(f"{name} MessageAccepted StopRecovery action changed")

        if name == "recover_normally":
            checked_position = re.search(
                r"let\s+Some\s*\(\s*next_position\s*\)\s*=\s*"
                r"current_pos\.checked_add\s*\(\s*size\s*\)\s*else\s*\{(?P<failure>.*?)\}\s*;",
                body,
                re.DOTALL,
            )
            if checked_position is None or re.search(r"\bbreak\s+'segments\s*;", checked_position.group("failure")) is None:
                violations.append(f"{name} current_pos must checked_add and stop globally on error")
            if re.search(r"\bcurrent_pos\s*\+\s*size\b", body):
                violations.append(f"{name} current_pos uses unchecked addition")
        elif "current_pos" in body:
            violations.append(f"{name} copied standard current_pos state")

        if body.count("normal_recovery.summary()") != 1:
            violations.append(f"{name} must bind exactly one Local recovery summary")
        normalized_body = re.sub(r"\s+", "", body)
        required_summary_flow = [
            "letsummary=normal_recovery.summary();",
            "letlast_valid_offset=matchi64::try_from(summary.last_valid_offset)",
            "letprocess_offset=matchi64::try_from(summary.truncate_offset)",
            "should_truncate_normal_recovery_consume_queue(max_phy_offset_of_consume_queue,summary.truncate_offset)",
            "self.set_confirm_offset(last_valid_offset)",
            "message_store.truncate_dirty_logic_files(process_offset)",
            "self.mapped_file_queue.set_flushed_where(process_offset)",
            "self.mapped_file_queue.set_committed_where(process_offset)",
            "self.mapped_file_queue.truncate_dirty_files(process_offset)",
        ]
        controller_confirm_offset = "process_offset" if policy == "Standard" else "last_valid_offset"
        required_summary_flow.append(
            "self.clamp_controller_recover_confirm_offset("
            f"message_store.get_min_phy_offset(),{controller_confirm_offset})"
        )
        if any(fragment not in normalized_body for fragment in required_summary_flow):
            violations.append(f"{name} final recovery writes must flow from Local summary")
        if "NormalRecoverySummary{" in normalized_body:
            violations.append(f"{name} must not construct a Store-owned recovery summary")
        if body.count("should_truncate_normal_recovery_consume_queue(") != 1:
            violations.append(f"{name} must use the shared ConsumeQueue predicate exactly once")
        if re.search(r"\b(?:last_valid_msg_phy_offset|mapped_file_offset)\b", body):
            violations.append(f"{name} copied Local recovery watermark state")
        mutable_names = re.findall(r"\blet\s+mut\s+([A-Za-z_][A-Za-z0-9_]*)\b", body)
        if any("last_valid" in mutable_name or "truncate" in mutable_name for mutable_name in mutable_names):
            violations.append(f"{name} copied Local recovery policy state")
        empty_branch = body.find("mapped_files_inner.is_empty()")
        state_creation = body.find("NormalRecoveryState::try_new")
        if state_creation == -1 or (empty_branch != -1 and state_creation < empty_branch):
            violations.append(f"{name} empty-file path must bypass Local reducer")
    return violations


def store_abnormal_recovery_adapter_violations(commit_log: str) -> list[str]:
    violations: list[str] = []
    recovery_prefix = "rocketmq_store_local::commit_log::recovery::"
    expected_imports = {
        "AbnormalRecoveryAction",
        "AbnormalRecoveryDispatchGate",
        "AbnormalRecoveryEvent",
        "AbnormalRecoveryPolicy",
        "AbnormalRecoveryState",
    }
    imports = {
        body.removeprefix(recovery_prefix)
        for kind, _, body, _ in active_import_records(commit_log)
        if kind == "use" and body.startswith(recovery_prefix) and body.removeprefix(recovery_prefix).startswith("Abnormal")
    }
    if imports != expected_imports:
        violations.append("Store abnormal recovery imports must be exact Local imports")
    if any(
        kind == "use"
        and body.startswith(recovery_prefix)
        and body.removeprefix(recovery_prefix).startswith("Abnormal")
        and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(commit_log)
    ):
        violations.append("Store abnormal recovery imports forbid alias/brace/glob")

    candidate_helper = named_function_body(commit_log, "abnormal_confirm_candidate_end")
    normalized_candidate = "" if candidate_helper is None else re.sub(r"\s+", "", candidate_helper)
    for fragment in [
        "ifcommit_log_offset<0",
        "i64::try_from(input_size)",
        "commit_log_offset.checked_add(input_size)",
    ]:
        if fragment not in normalized_candidate:
            violations.append("Store abnormal confirm candidate helper changed")
            break
    cq_helper = named_function_body(commit_log, "should_truncate_abnormal_recovery_consume_queue")
    if cq_helper is None or re.sub(r"\s+", "", cq_helper) != (
        "max<0||u64::try_from(max).is_ok_and(|value|value>=truncate)"
    ):
        violations.append("Store abnormal recovery ConsumeQueue predicate changed")

    for name, policy in [
        ("recover_abnormally", "Standard"),
        ("recover_abnormally_optimized", "Optimized"),
    ]:
        body = named_function_body(commit_log, name)
        if body is None:
            violations.append(f"{name} body missing")
            continue
        if body.count(f"AbnormalRecoveryPolicy::{policy}") != 1:
            violations.append(f"{name} Local abnormal policy construction changed")
        if body.count("AbnormalRecoveryState::try_new") != 1:
            violations.append(f"{name} must construct one Local abnormal state")
        normalized = re.sub(r"\s+", "", body)
        if policy == "Optimized":
            seed = "letinitial_offset=ifindex==0{first_recovery_file.get_file_from_offset()}else{0};"
            input_size = "msg_size"
        else:
            seed = "letinitial_offset=first_recovery_file.get_file_from_offset();"
            input_size = "input_size"
        if seed not in normalized:
            violations.append(f"{name} abnormal recovery seed changed")
        if normalized.count(
            f"abnormal_confirm_candidate_end(dispatch_request.commit_log_offset,{input_size})"
        ) != 1:
            violations.append(f"{name} confirm candidate must use raw input size")
        if body.count("self.get_confirm_offset().max(0)") != 1:
            violations.append(f"{name} must read a fresh confirm limit per message")
        if body.count("AbnormalRecoveryDispatchGate::ConfirmBounded") != 1:
            violations.append(f"{name} must build one Local confirm-bounded gate")
        for event in ["SegmentStarted", "MessageAccepted", "Blank", "InvalidRecord", "SourceEnded"]:
            if body.count(f"AbnormalRecoveryEvent::{event}") != 1:
                violations.append(f"{name} must route {event} through Local abnormal reducer")
        if body.count("abnormal_recovery.summary()") != 1:
            violations.append(f"{name} must bind exactly one Local abnormal summary")
        if "AbnormalRecoverySummary{" in normalized:
            violations.append(f"{name} must not construct an abnormal summary")
        if "letdo_dispatch=true;" not in normalized or "do_dispatch=false" in normalized:
            violations.append(f"{name} abnormal recovery dispatch mode changed")
        dispatch_arm = (
            "Ok(AbnormalRecoveryAction::DispatchMessage)=>{"
            "self.on_commit_log_dispatch(&mutdispatch_request,do_dispatch,true,false);}"
        )
        message_match = abnormal_recovery_event_match(body, "MessageAccepted")
        if message_match is None:
            violations.append(f"{name} MessageAccepted action match is missing")
            message_action_body = ""
            message_match_end = -1
        else:
            _, message_action_body, message_match_end = message_match
        normalized_message_actions = re.sub(r"\s+", "", message_action_body)
        if (
            dispatch_arm not in normalized_message_actions
            or "Ok(AbnormalRecoveryAction::SkipMessageDispatch)=>{}" not in normalized_message_actions
        ):
            violations.append(f"{name} must obey Local message dispatch actions")
        if any(
            fragment not in normalized_message_actions
            for fragment in [
                "Ok(action)=>{warn!();break'segments;}",
                "Err(error)=>{warn!();break'segments;}",
            ]
        ):
            violations.append(f"{name} must stop after non-message actions or reducer errors")
        blank_hook = (
            "Ok(AbnormalRecoveryAction::NotifyFileEndAndContinueNextSegment)=>{"
            "self.on_commit_log_dispatch(&mutdispatch_request,do_dispatch,true,true);"
        )
        if blank_hook not in normalized or "self.dispatcher.dispatch" in normalized:
            violations.append(f"{name} blank must use only the compatibility file-end hook")
        if policy == "Standard":
            checked_cursor = "current_pos.checked_add(input_size)"
            if normalized.count(checked_cursor) != 1 or "current_pos+input_size" in normalized:
                violations.append(f"{name} raw input cursor advancement changed")
        else:
            process_message = (
                "letmutdispatch_request="
                "recovery_ctx.process_message(&mutmsg_bytes,absolute_offset);"
            )
            ordered_message_flow = [
                process_message,
                "abnormal_confirm_candidate_end(dispatch_request.commit_log_offset,msg_size)",
                "letdispatch_gate=",
                "matchabnormal_recovery.apply(AbnormalRecoveryEvent::MessageAccepted{",
            ]
            positions = [normalized.find(fragment) for fragment in ordered_message_flow]
            if (
                normalized.count(process_message) != 1
                or any(position == -1 for position in positions)
                or positions != sorted(positions)
            ):
                violations.append(
                    f"{name} must parse each message before candidate, gate, and reducer application"
                )

            active_body = active_rust_source(body)
            if "file_processed" in message_action_body:
                violations.append(f"{name} file-processed marker must be outside dispatch action arms")
            match_tail = "" if message_match_end == -1 else active_body[message_match_end:]
            if re.match(r"\s*file_processed\s*=\s*true\s*;", match_tail) is None:
                violations.append(
                    f"{name} must mark the file immediately after a successful message action match"
                )
            if len(re.findall(r"\bfile_processed\s*=\s*true\s*;", active_body)) != 1:
                violations.append(f"{name} must mark each file from one accepted-message site")

            stats_increment = "recovery_ctx.stats.files_processed+=1;"
            if normalized.count(stats_increment) != 1:
                violations.append(f"{name} must increment file statistics exactly once")
            stats_guard = re.search(r"\bif\s+file_processed\s*\{", active_body)
            if stats_guard is None:
                violations.append(f"{name} file statistics must be guarded by file_processed")
            else:
                stats_open = active_body.find("{", stats_guard.start())
                stats_body = braced_body(active_body, stats_open)
                if stats_body is None or re.sub(r"\s+", "", stats_body[0]) != stats_increment:
                    violations.append(f"{name} file statistics guard changed")
            if len(re.findall(r"\bfile_processed\b", active_body)) != 3:
                violations.append(f"{name} file-processed marker escaped its structural contract")
        required_final = [
            "letsummary=abnormal_recovery.summary();",
            "i64::try_from(summary.last_valid_offset)",
            "i64::try_from(summary.confirm_valid_offset)",
            "i64::try_from(summary.truncate_offset)",
            "self.clamp_controller_recover_confirm_offset(message_store.get_min_phy_offset(),confirm_valid_offset)",
            "self.set_confirm_offset(last_valid_offset)",
            "should_truncate_abnormal_recovery_consume_queue(max_phy_offset_of_consume_queue,summary.truncate_offset)",
            "message_store.truncate_dirty_logic_files(process_offset)",
            "self.mapped_file_queue.set_flushed_where(process_offset)",
            "self.mapped_file_queue.set_committed_where(process_offset)",
            "self.mapped_file_queue.truncate_dirty_files(process_offset)",
        ]
        if any(fragment not in normalized for fragment in required_final):
            violations.append(f"{name} final abnormal watermarks changed")
        if re.search(r"\b(?:last_valid_msg_phy_offset|last_confirm_valid_msg_phy_offset|mapped_file_offset)\b", body):
            violations.append(f"{name} copied Local abnormal watermark state")
        if re.search(r"\bmatch\s+AbnormalRecoveryPolicy\b", body):
            violations.append(f"{name} copied Local abnormal policy match")
        if re.search(r"\b(?:as|unwrap|expect|panic)\b", body):
            violations.append(f"{name} uses unchecked conversion or panic")
    return violations


def store_record_parser_wrapper_violations(log_file_root: str, commit_log: str) -> list[str]:
    violations: list[str] = []
    if re.search(r"\bmod\s+commit_log_record_parser\b", active_rust_source(log_file_root)):
        violations.append("Store parser module copy")

    prefix = "rocketmq_store_local::commit_log::record_parser::"
    expected = {
        "decode_commit_log_record",
        "CommitLogRecordBodyMode",
        "CommitLogRecordChecksum",
        "CommitLogRecordErrorKind",
        "CommitLogRecordOutcome",
    }
    imports = {
        body.removeprefix(prefix)
        for kind, _, body, _ in active_import_records(commit_log)
        if kind == "use" and body.startswith(prefix)
    }
    if imports != expected:
        violations.append("Store parser imports must be exact Local imports")
    if any(
        kind == "use"
        and body.startswith("rocketmq_store_local::commit_log::record_parser")
        and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(commit_log)
    ):
        violations.append("Store parser imports forbid alias/brace/glob")

    active_commit_log = active_rust_source(commit_log)
    signature_match = re.search(r"\bpub\s+fn\s+check_message_and_return_size\b", active_commit_log)
    if signature_match is None:
        violations.append("Store parser wrapper signature changed")
    else:
        opening_brace = active_commit_log.find("{", signature_match.end())
        signature = (
            "" if opening_brace == -1 else re.sub(r"\s+", "", active_commit_log[signature_match.start():opening_brace])
        )
        expected_signature = (
            "pubfncheck_message_and_return_size(bytes:&mutBytes,check_crc:bool,"
            "check_dup_info:bool,read_body:bool,message_store_config:&Arc<MessageStoreConfig>,"
            "max_delay_level:i32,delay_level_table:&BTreeMap<i32,i64>,)->DispatchRequest"
        )
        if signature != expected_signature:
            violations.append("Store parser wrapper signature changed")

    body = named_function_body(commit_log, "check_message_and_return_size")
    if body is None:
        violations.append("Store parser wrapper is missing")
    else:
        if len(re.findall(r"\bdecode_commit_log_record\s*\(", body)) != 1:
            violations.append("Store wrapper must delegate exactly once to Local decoder")
        if re.search(r"\bfrom_be_bytes\s*\(", body) or re.search(
            r"\bbytes\s*(?:\[|\.\s*(?:get_[A-Za-z0-9_]+|copy_to_bytes|copy_to_slice|slice)\s*\()",
            body,
        ):
            violations.append("Store wrapper copied raw parser")
        advance_calls = [
            re.sub(r"\s+", "", argument)
            for argument in re.findall(r"\bbytes\s*\.\s*advance\s*\(([^()]*)\)\s*;", body)
        ]
        if advance_calls != ["8", "total_sizeasusize", "total_sizeasusize"]:
            violations.append("Store wrapper transaction advances changed")
    return violations


def braced_body(source: str, opening_brace: int) -> tuple[str, int] | None:
    depth = 0
    for index in range(opening_brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[opening_brace + 1:index], index + 1
    return None


def normalized_batch_iterator_methods(source: str) -> dict[str, tuple[str, str]] | None:
    active = active_rust_source(source)
    impl_blocks: list[tuple[str, str]] = []
    for impl_match in re.finditer(r"\bimpl\b", active):
        opening_brace = active.find("{", impl_match.end())
        semicolon = active.find(";", impl_match.end())
        if opening_brace == -1 or (semicolon != -1 and semicolon < opening_brace):
            continue
        header = active[impl_match.start():opening_brace]
        if re.search(r"\bBatchMessageIterator\b", header) is None:
            continue
        extracted = braced_body(active, opening_brace)
        if extracted is None:
            return None
        body, _ = extracted
        impl_blocks.append((re.sub(r"\s+", "", header), body))

    if len(impl_blocks) != 1 or impl_blocks[0][0] != "impl<'a>BatchMessageIterator<'a>":
        return None

    body = impl_blocks[0][1]
    methods: dict[str, tuple[str, str]] = {}
    depth = 0
    index = 0
    method_start = re.compile(r"(?:pub\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)\b")
    while index < len(body):
        if body[index] == "{":
            depth += 1
            index += 1
            continue
        if body[index] == "}":
            depth -= 1
            index += 1
            continue
        if depth != 0:
            index += 1
            continue
        match = method_start.match(body, index)
        if match is None:
            index += 1
            continue
        opening_brace = body.find("{", match.end())
        if opening_brace == -1:
            return None
        extracted = braced_body(body, opening_brace)
        if extracted is None or match.group(1) in methods:
            return None
        method_body, method_end = extracted
        methods[match.group(1)] = (
            re.sub(r"\s+", "", body[match.start():opening_brace]),
            re.sub(r"\s+", "", method_body),
        )
        index = method_end
    return methods


def batch_iterator_method_contract_violations(source: str) -> list[str]:
    methods = normalized_batch_iterator_methods(source)
    expected = {
        "new": (
            "pubfnnew(mapped_file:&'aArc<DefaultMappedFile>)->Self",
            "Self{inner:CommitLogFrameCursor::new(MappedFileFrameSource{mapped_file}),}",
        ),
        "next_message": (
            "pubfnnext_message(&mutself)->Option<(Bytes,usize,usize)>",
            "self.inner.next_message()",
        ),
        "current_offset": (
            "pubfncurrent_offset(&self)->usize",
            "self.inner.current_offset()",
        ),
    }
    return [] if methods == expected else ["legacy iterator methods must be exact pure delegates"]


VALID_COMMIT_LOG_RECORD_FACADE = '''
pub use rocketmq_store_local::commit_log::record::BLANK_MAGIC_CODE;
pub use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE;
'''
VALID_STORE_RECORD_PARSER_WRAPPER = '''
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordBodyMode;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordChecksum;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordErrorKind;
use rocketmq_store_local::commit_log::record_parser::CommitLogRecordOutcome;
use rocketmq_store_local::commit_log::record_parser::decode_commit_log_record;

pub fn check_message_and_return_size(
    bytes: &mut Bytes,
    check_crc: bool,
    check_dup_info: bool,
    read_body: bool,
    message_store_config: &Arc<MessageStoreConfig>,
    max_delay_level: i32,
    delay_level_table: &BTreeMap<i32, i64>,
) -> DispatchRequest {
    let _ = decode_commit_log_record(bytes, body_mode, &checksum);
    bytes.advance(8);
    bytes.advance(total_size as usize);
    bytes.advance(total_size as usize);
}
'''
VALID_RECOVERY_RECORD_FACADE = '''
pub use rocketmq_store_local::commit_log::record::is_blank_message;
pub struct BatchMessageIterator<'a> {
    inner: CommitLogFrameCursor<MappedFileFrameSource<'a, Arc<DefaultMappedFile>>>,
}
impl<'a> BatchMessageIterator<'a> {
    pub fn new(mapped_file: &'a Arc<DefaultMappedFile>) -> Self {
        Self {
            inner: CommitLogFrameCursor::new(MappedFileFrameSource { mapped_file }),
        }
    }
    pub fn next_message(&mut self) -> Option<(Bytes, usize, usize)> {
        self.inner.next_message()
    }
    pub fn current_offset(&self) -> usize {
        self.inner.current_offset()
    }
}
'''


def store_record_facade_violations(commit_log: str, recovery: str) -> list[str]:
    violations: list[str] = []
    expected_commit_log = {
        "MESSAGE_MAGIC_CODE": "record",
        "BLANK_MAGIC_CODE": "record",
    }
    actual_commit_log = {
        item: module
        for item, module in active_commit_log_facade_reexports(commit_log).items()
        if item in expected_commit_log
    }
    if actual_commit_log != expected_commit_log:
        violations.append("commit_log constants must be exact record re-exports")

    expected_recovery = {"is_blank_message": "record"}
    actual_recovery = {
        item: module
        for item, module in active_commit_log_facade_reexports(recovery).items()
        if item in expected_recovery
    }
    if actual_recovery != expected_recovery:
        violations.append("recovery blank helper must be an exact record re-export")

    active_recovery = active_rust_source(recovery)
    if re.search(r"\bconst\s+(?:PARSE_BATCH_SIZE|MIN_MESSAGE_SIZE)\b", active_recovery):
        violations.append("legacy iterator constants copied")
    iterator_body = active_struct_body(recovery, "BatchMessageIterator")
    if iterator_body != (
        "\n    inner: CommitLogFrameCursor<"
        "MappedFileFrameSource<'a, Arc<DefaultMappedFile>>>,\n"
    ):
        violations.append("legacy iterator must wrap only the Local cursor")
    violations.extend(batch_iterator_method_contract_violations(recovery))
    if any(
        kind == "use" and (" as " in body or "{" in body)
        for kind, _, body, _ in active_import_records(recovery)
    ):
        violations.append("recovery boundary forbids alias/brace imports")
    return violations


def active_tracing_info_targets(source: str) -> list[str | None]:
    active_source = active_rust_source(source)
    targets: list[str | None] = []
    for invocation in re.finditer(r"\binfo\s*!\s*\(", active_source):
        original_invocation = source[invocation.start():]
        target = re.match(
            r'info\s*!\s*\(\s*target\s*:\s*"([^"]+)"',
            original_invocation,
        )
        targets.append(target.group(1) if target else None)
    return targets


def active_rust_source(source: str) -> str:
    output: list[str] = []
    index = 0
    length = len(source)

    def mask(start: int, end: int) -> None:
        output.extend("\n" if character == "\n" else " " for character in source[start:end])

    while index < length:
        if source.startswith("//", index):
            end = source.find("\n", index + 2)
            end = length if end == -1 else end
            mask(index, end)
            index = end
            continue

        if source.startswith("/*", index):
            start = index
            index += 2
            depth = 1
            while index < length and depth:
                if source.startswith("/*", index):
                    depth += 1
                    index += 2
                elif source.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            mask(start, index)
            continue

        raw = re.match(r'(?:br|cr|r)(?P<hashes>#{0,255})"', source[index:])
        if raw:
            start = index
            delimiter = '"' + raw.group("hashes")
            index += raw.end()
            end = source.find(delimiter, index)
            index = length if end == -1 else end + len(delimiter)
            mask(start, index)
            continue

        string_prefix = 1 if source[index] == '"' else 2 if source[index:index + 2] in {'b"', 'c"'} else 0
        if string_prefix:
            start = index
            index += string_prefix
            while index < length:
                if source[index] == "\\":
                    index = min(index + 2, length)
                elif source[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            mask(start, index)
            continue

        output.append(source[index])
        index += 1

    return "".join(output)


def has_linux_only_optional_tokio_uring(manifest: dict[str, Any]) -> bool:
    occurrences: list[tuple[str | None, str, Any]] = []
    table_names = ("dependencies", "build-dependencies", "dev-dependencies")
    for table_name in table_names:
        table = manifest.get(table_name, {})
        if "tokio-uring" in table:
            occurrences.append((None, table_name, table["tokio-uring"]))
    for target, target_manifest in manifest.get("target", {}).items():
        for table_name in table_names:
            table = target_manifest.get(table_name, {})
            if "tokio-uring" in table:
                occurrences.append((target, table_name, table["tokio-uring"]))

    if len(occurrences) != 1:
        return False
    target, table_name, specification = occurrences[0]
    return (
        target == 'cfg(target_os = "linux")'
        and table_name == "dependencies"
        and isinstance(specification, dict)
        and specification.get("optional") is True
    )


def has_unix_only_normal_libc(manifest: dict[str, Any]) -> bool:
    occurrences: list[tuple[str | None, str, Any]] = []
    table_names = ("dependencies", "build-dependencies", "dev-dependencies")
    for table_name in table_names:
        table = manifest.get(table_name, {})
        if "libc" in table:
            occurrences.append((None, table_name, table["libc"]))
    for target, target_manifest in manifest.get("target", {}).items():
        for table_name in table_names:
            table = target_manifest.get(table_name, {})
            if "libc" in table:
                occurrences.append((target, table_name, table["libc"]))

    return occurrences == [('cfg(unix)', "dependencies", "0.2.186")]


def canonical_definition_paths(sources: dict[Path, str], item: str, item_kind: str) -> list[Path]:
    pattern = re.compile(rf"\bpub\s+{re.escape(item_kind)}\s+{re.escape(item)}\b")
    return [
        path
        for path, source in sources.items()
        if pattern.search(source) and pattern.search(active_rust_source(source))
    ]


def active_kernel_reexports(source: str) -> set[str]:
    return set(
        re.findall(
            r"pub(?:\s*\(crate\))?\s+use\s+rocketmq_store_local::mapped_file::kernel::"
            r"([A-Za-z_][A-Za-z0-9_]*)\s*;",
            active_rust_source(source),
        )
    )


def active_file_use_statements(source: str) -> list[str]:
    file_prefix = "rocketmq_store_local::mapped_file::file::"
    statements: list[str] = []
    for visibility, body, _ in active_use_records(source):
        if not body.startswith(file_prefix):
            continue
        prefix = f"{visibility} " if visibility else ""
        statements.append(f"{prefix}use {body.removeprefix(file_prefix)}")
    return statements


def active_mapping_use_statements(source: str) -> list[str]:
    mapping_prefix = "rocketmq_store_local::mapped_file::mapping::"
    statements: list[str] = []
    for visibility, body, _ in active_use_records(source):
        if not body.startswith(mapping_prefix):
            continue
        prefix = f"{visibility} " if visibility else ""
        statements.append(f"{prefix}use {body.removeprefix(mapping_prefix)}")
    return statements


def file_item_owner_occurrences(
    sources: dict[Path, str],
    item: str,
) -> list[tuple[Path, str]]:
    declaration = re.compile(
        rf"\b(?:pub(?:\s*\([^)]*\))?\s+)?"
        rf"(?P<kind>struct|trait|type|enum|union|const|static|fn|mod)\s+{re.escape(item)}\b"
    )
    aliased_import = re.compile(rf"\bas\s+{re.escape(item)}\b")
    occurrences: list[tuple[Path, str]] = []
    for path, source in sources.items():
        if item not in source:
            continue
        active_source = active_rust_source(source)
        occurrences.extend((path, match.group("kind")) for match in declaration.finditer(active_source))
        occurrences.extend((path, "alias") for _ in aliased_import.finditer(active_source))
    return sorted(occurrences, key=lambda occurrence: (str(occurrence[0]), occurrence[1]))


def active_import_records(source: str) -> list[tuple[str, str, str, str]]:
    active_source = active_rust_source(source)
    matches: list[tuple[int, str, re.Match[str]]] = []
    patterns = {
        "use": re.compile(
            r"(?m)^[ \t]*(?P<visibility>pub(?:\s*\([^)]*\))?[ \t]+)?"
            r"use[ \t\r\n]+(?P<body>[^;]+);"
        ),
        "extern crate": re.compile(
            r"(?m)^[ \t]*(?P<visibility>pub(?:\s*\([^)]*\))?[ \t]+)?"
            r"extern[ \t]+crate[ \t]+(?P<body>[^;]+);"
        ),
    }
    for kind, pattern in patterns.items():
        matches.extend((match.start(), kind, match) for match in pattern.finditer(active_source))

    records: list[tuple[str, str, str, str]] = []
    for _, kind, match in sorted(matches, key=lambda entry: entry[0]):
        visibility = re.sub(r"\s+", "", (match.group("visibility") or "").strip())
        body = re.sub(r"\s*::\s*", "::", match.group("body").strip())
        body = re.sub(r"\s+as\s+", " as ", body)
        body = re.sub(r"\s+", " ", body).strip()
        prefix = f"{visibility} " if visibility else ""
        records.append((kind, visibility, body, f"{prefix}{kind} {body}"))
    return records


def active_use_records(source: str) -> list[tuple[str, str, str]]:
    return [
        (visibility, body, statement)
        for kind, visibility, body, statement in active_import_records(source)
        if kind == "use"
    ]


def active_kernel_use_statements(source: str) -> list[str]:
    kernel_prefix = "rocketmq_store_local::mapped_file::kernel::"
    statements: list[str] = []
    for visibility, body, _ in active_use_records(source):
        if not body.startswith(kernel_prefix):
            continue
        prefix = f"{visibility} " if visibility else ""
        statements.append(f"{prefix}use {body.removeprefix(kernel_prefix)}")
    return statements


def kernel_facade_boundary_uses(
    sources: dict[Path, str],
) -> list[tuple[Path, str]]:
    records_by_path = {
        path: active_import_records(source)
        for path, source in sources.items()
        if "use" in source or "extern crate" in source
    }
    boundary_uses: list[tuple[Path, str]] = []
    for path in sorted(records_by_path, key=str):
        for kind, visibility, body, statement in records_by_path[path]:
            rooted_at_local = body.removeprefix("::").startswith("rocketmq_store_local")
            canonical_alias_or_tree = rooted_at_local and (
                "{" in body or re.search(r"\bas\b", body)
            )
            public_use = kind == "use" and visibility.startswith("pub")
            public_glob = public_use and "*" in body
            public_kernel_item = public_use and any(
                re.search(rf"\b{re.escape(item)}\b", body)
                for item in KERNEL_ITEMS
            )
            public_direct_kernel = public_use and rooted_at_local and (
                "mapped_file::kernel" in body
            )
            if canonical_alias_or_tree or public_glob or public_kernel_item or public_direct_kernel:
                boundary_uses.append((path, statement))
    return boundary_uses


def kernel_item_owner_occurrences(
    sources: dict[Path, str],
    item: str,
) -> list[tuple[Path, str]]:
    declaration = re.compile(
        rf"\b(?:pub(?:\s*\([^)]*\))?\s+)?"
        rf"(?P<kind>struct|trait|type|enum|union|mod)\s+{re.escape(item)}\b"
    )
    aliased_import = re.compile(rf"\bas\s+{re.escape(item)}\b")
    occurrences: list[tuple[Path, str]] = []
    for path, source in sources.items():
        if item not in source:
            continue
        active_source = active_rust_source(source)
        occurrences.extend(
            (path, match.group("kind"))
            for match in declaration.finditer(active_source)
        )
        occurrences.extend(
            (path, "use-as")
            for statement in active_source.split(";")
            if re.search(r"\buse\b", statement) and aliased_import.search(statement)
        )
    return occurrences


def active_struct_body(source: str, struct_name: str) -> str:
    source = active_rust_source(source)
    declaration = re.search(
        rf"\bstruct\s+{re.escape(struct_name)}(?:\s*<[^{{}};]*>)?\s*\{{",
        source,
    )
    if declaration is None:
        return ""
    start = declaration.end()
    depth = 1
    for index in range(start, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start:index]
    return ""


def active_struct_fields(source: str, struct_name: str) -> list[tuple[str, str]]:
    body = active_struct_body(source, struct_name)
    if not body:
        return []

    segments: list[str] = []
    start = 0
    depths = {"<": 0, "(": 0, "[": 0, "{": 0}
    closing = {">": "<", ")": "(", "]": "[", "}": "{"}
    for index, character in enumerate(body):
        if character in depths:
            depths[character] += 1
        elif character in closing:
            opener = closing[character]
            depths[opener] = max(0, depths[opener] - 1)
        elif character == "," and not any(depths.values()):
            segments.append(body[start:index])
            start = index + 1
    segments.append(body[start:])

    fields: list[tuple[str, str]] = []
    field_pattern = re.compile(
        r"(?s)^\s*(?:#\s*\[[^\]]*\]\s*)*"
        r"(?:pub(?:\s*\([^)]*\))?\s+)?"
        r"(?:r#)?(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*:\s*(?P<type>.+?)\s*$"
    )
    for segment in segments:
        match = field_pattern.match(segment)
        if match:
            field_type = re.sub(r"\s+", " ", match.group("type")).strip()
            fields.append((match.group("name"), field_type))
    return fields


def default_mapped_file_progress_violations(source: str) -> list[str]:
    if not active_struct_body(source, "DefaultMappedFile"):
        return ["DefaultMappedFile struct missing"]

    fields = active_struct_fields(source, "DefaultMappedFile")
    violations = [
        f"legacy progress field: {name}"
        for name, _ in fields
        if name in PROGRESS_FIELDS
    ]
    progress_fields = [
        (name, field_type)
        for name, field_type in fields
        if re.search(r"\bMappedFileProgress\b", field_type)
    ]
    if [name for name, _ in progress_fields] != ["progress"]:
        violations.append("MappedFileProgress fields must be exactly: progress")
    elif not re.fullmatch(
        r"(?:[A-Za-z_][A-Za-z0-9_]*::)*MappedFileProgress",
        progress_fields[0][1],
    ):
        violations.append("progress field must have exact MappedFileProgress type")
    return violations


def default_mapped_file_storage_violations(source: str) -> list[str]:
    if not active_struct_body(source, "DefaultMappedFile"):
        return ["DefaultMappedFile struct missing"]

    fields = active_struct_fields(source, "DefaultMappedFile")
    violations = default_mapped_file_syntax_violations(source)
    violations.extend(
        f"legacy storage field: {name}"
        for name, _ in fields
        if name in {"file", "file_from_offset"}
    )
    storage_fields = [
        (name, field_type)
        for name, field_type in fields
        if re.search(r"\bMappedFileStorage\b", field_type)
    ]
    if [name for name, _ in storage_fields] != ["storage"]:
        violations.append("MappedFileStorage fields must be exactly: storage")
    elif not re.fullmatch(
        r"(?:[A-Za-z_][A-Za-z0-9_]*::)*MappedFileStorage",
        storage_fields[0][1],
    ):
        violations.append("storage field must have exact MappedFileStorage type")

    resolve_type = storage_field_type_resolver(source)
    forbidden_types = {
        "std::fs::File": "file",
        "std::path::PathBuf": "path",
        "u64": "offset",
    }
    for name, field_type in fields:
        owner = forbidden_types.get(resolve_type(field_type))
        if owner is not None:
            violations.append(f"direct {owner} owner field: {name}")
    return violations


def normalize_policy_use_statement(statement: str) -> str:
    statement = re.sub(r"\s*::\s*", "::", statement.strip())
    statement = re.sub(r"\s+", " ", statement)
    statement = re.sub(r"\{\s*", "{", statement)
    statement = re.sub(r"\s*,\s*", ", ", statement)
    statement = re.sub(r",\s*}", "}", statement)
    statement = re.sub(r"\s*}", "}", statement)
    return statement


def active_type_alias_statements(source: str) -> list[str]:
    active_source = active_rust_source(source)
    type_alias = re.compile(
        r"(?m)^[ \t]*(?P<visibility>pub(?:\s*\([^)]*\))?\s+)?"
        r"type\s+(?P<body>[^;]+);"
    )
    statements: list[str] = []
    for match in type_alias.finditer(active_source):
        visibility = re.sub(r"\s+", "", (match.group("visibility") or "").strip())
        body = re.sub(r"\s*::\s*", "::", match.group("body").strip())
        body = re.sub(r"\s*=\s*", " = ", body)
        body = re.sub(r"\s+", " ", body)
        prefix = f"{visibility} " if visibility else ""
        statements.append(f"{prefix}type {body}")
    return statements


def default_mapped_file_syntax_violations(source: str) -> list[str]:
    violations: list[str] = []
    for kind, _, body, statement in active_import_records(source):
        if kind != "use" or (" as " not in body and "{" not in body):
            continue
        normalized = normalize_policy_use_statement(statement)
        if normalized not in DEFAULT_MAPPED_FILE_ALIAS_BRACE_USE_ALLOWLIST:
            violations.append(f"forbidden alias/brace use: {normalized}")

    for statement in active_type_alias_statements(source):
        if statement not in DEFAULT_MAPPED_FILE_TYPE_ALIAS_ALLOWLIST:
            violations.append(f"forbidden type alias: {statement}")
    return violations


def storage_field_type_resolver(source: str) -> Callable[[str], str]:
    aliases: dict[str, str] = {}
    for kind, _, body, _ in active_import_records(source):
        if kind != "use":
            continue
        imported = re.fullmatch(r"(?P<path>(?:::)?std::(?:fs::File|path::PathBuf))", body)
        if imported:
            path = imported.group("path").removeprefix("::")
            local_name = path.rsplit("::", 1)[-1]
            aliases[local_name] = path

    def normalize(field_type: str) -> str:
        normalized = re.sub(r"\s*::\s*", "::", field_type.strip())
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.removeprefix("::")

    def resolve(field_type: str) -> str:
        normalized = normalize(field_type)
        return aliases.get(normalized, normalized)

    return resolve


def mapped_file_storage_owner_violations(source: str) -> list[str]:
    fields = active_struct_fields(source, "MappedFileStorage")
    expected = [
        ("file", "File"),
        ("path", "PathBuf"),
        ("file_from_offset", "u64"),
    ]
    return [] if fields == expected else [f"MappedFileStorage fields must be exactly: {expected!r}"]


def mapped_file_mapping_owner_violations(source: str) -> list[str]:
    fields = active_struct_fields(source, "MappedFileMapping")
    expected = [
        ("value", "OnceLock<M>"),
        ("init_lock", "Mutex<()>"),
        ("lazy_enabled", "bool"),
        ("map_operations", "AtomicU64"),
        ("map_failures", "AtomicU64"),
        ("total_millis", "AtomicU64"),
        ("last_millis", "AtomicU64"),
    ]
    return [] if fields == expected else [f"MappedFileMapping fields must be exactly: {expected!r}"]


def default_mapped_file_mapping_violations(source: str) -> list[str]:
    if not active_struct_body(source, "DefaultMappedFile"):
        return ["DefaultMappedFile struct missing"]

    fields = active_struct_fields(source, "DefaultMappedFile")
    violations = default_mapped_file_syntax_violations(source)
    mapping_fields = [
        (name, field_type)
        for name, field_type in fields
        if re.search(r"\bMappedFileMapping\b", field_type)
    ]
    if mapping_fields != [("mapping", "MappedFileMapping<ArcMut<MmapMut>>")]:
        violations.append(
            "MappedFileMapping fields must be exactly: mapping: MappedFileMapping<ArcMut<MmapMut>>"
        )

    legacy_names = {
        "mmapped_file",
        "mmap_init_lock",
        "lazy_mmap_enabled",
        "lazy_mmap_operations",
        "lazy_mmap_failures",
        "lazy_mmap_total_millis",
        "lazy_mmap_last_millis",
    }
    violations.extend(
        f"legacy mapping field: {name}"
        for name, _ in fields
        if name in legacy_names
    )
    for name, field_type in fields:
        normalized_type = re.sub(r"\s+", "", field_type)
        if "OnceLock<" in normalized_type:
            violations.append(f"direct mapping cell owner field: {name}")
        if re.search(r"(?:^|::)Mutex<()>$", normalized_type):
            violations.append(f"direct mapping init lock owner field: {name}")
        if field_type == "bool" and name != "first_create_in_queue":
            violations.append(f"direct mapping enablement candidate field: {name}")
        if re.search(r"(?:^|::)AtomicU64$", normalized_type) and name != "swap_map_time":
            violations.append(f"direct mapping statistic candidate field: {name}")
    return violations


def legacy_mapping_getter_signature_violations(source: str) -> list[str]:
    active = active_rust_source(source)
    signatures = {
        "is_lazy_mmap_enabled": r"pub\s+fn\s+is_lazy_mmap_enabled\s*\(\s*&self\s*\)\s*->\s*bool",
        "is_mapped": r"pub\s+fn\s+is_mapped\s*\(\s*&self\s*\)\s*->\s*bool",
        "lazy_mmap_stats": r"pub\s+fn\s+lazy_mmap_stats\s*\(\s*&self\s*\)\s*->\s*LazyMmapStats",
        "get_mapped_file_mut": r"pub\s+fn\s+get_mapped_file_mut\s*\(\s*&self\s*\)\s*->\s*&mut\s+MmapMut",
        "get_mapped_file": r"pub\s+fn\s+get_mapped_file\s*\(\s*&self\s*\)\s*->\s*&MmapMut",
        "get_mapped_file_arcmut": (
            r"pub\s+fn\s+get_mapped_file_arcmut\s*\(\s*&self\s*\)\s*"
            r"->\s*ArcMut\s*<\s*MmapMut\s*>"
        ),
    }
    return [name for name, pattern in signatures.items() if re.search(pattern, active) is None]


def commit_log_file_validation_owner_violations(source: str) -> list[str]:
    production = source.split("#[cfg(test)]", maxsplit=1)[0]
    active = active_rust_source(production)
    normalized = re.sub(r"\s+", "", active)
    violations: list[str] = []

    if active_struct_fields(production, "CommitLogFileMetadata") != [
        ("path", "PathBuf"),
        ("size", "u64"),
    ]:
        violations.append("Local CommitLog metadata fields changed")
    metadata_body = active_item_body(production, "struct", "CommitLogFileMetadata")
    if metadata_body is None or re.sub(r"\s+", "", metadata_body) != "pubpath:PathBuf,pubsize:u64,":
        violations.append("Local CommitLog metadata visibility changed")

    decision_body = active_item_body(production, "enum", "CommitLogFileLoadDecision")
    if decision_body is None or re.sub(r"\s+", "", decision_body) != "Load,RemoveEmptyLast,":
        violations.append("Local CommitLog load decisions changed")

    if active_struct_fields(production, "CommitLogFileValidationError") != [
        ("path", "PathBuf"),
        ("actual", "u64"),
        ("expected", "u64"),
    ]:
        violations.append("Local CommitLog validation error fields changed")
    error_body = active_item_body(production, "struct", "CommitLogFileValidationError")
    if error_body is None or re.sub(r"\s+", "", error_body) != (
        "pubpath:PathBuf,pubactual:u64,pubexpected:u64,"
    ):
        violations.append("Local CommitLog validation error visibility changed")
    if re.search(
        r"#\s*\[derive\([^\]]*\bError\b[^\]]*\)\]\s*"
        r"#\s*\[error\(\s*\"\{\} length \{actual\} not matched expected size "
        r"\{expected\}, please check it manually\"\s*,\s*path\.display\(\)\s*\)\]\s*"
        r"pub\s+struct\s+CommitLogFileValidationError\b",
        production,
    ) is None:
        violations.append("Local CommitLog validation error derive or message changed")

    signature = re.search(
        r"pub\s+fn\s+validate_commit_log_file\s*\(\s*"
        r"metadata\s*:\s*&CommitLogFileMetadata\s*,\s*"
        r"expected\s*:\s*u64\s*,\s*is_last\s*:\s*bool\s*,?\s*\)\s*"
        r"->\s*Result\s*<\s*CommitLogFileLoadDecision\s*,\s*CommitLogFileValidationError\s*>",
        active,
    )
    if signature is None:
        violations.append("Local CommitLog validation signature changed")
    body = named_function_body(production, "validate_commit_log_file")
    if body is None:
        violations.append("Local CommitLog validation function missing")
    else:
        normalized_body = re.sub(r"\s+", "", body)
        expected_fragments = [
            "ifmetadata.size==0&&is_last{returnOk(CommitLogFileLoadDecision::RemoveEmptyLast);}",
            "ifmetadata.size!=expected{returnErr(CommitLogFileValidationError{path:metadata.path.clone(),actual:metadata.size,expected,});}",
            "Ok(CommitLogFileLoadDecision::Load)",
        ]
        positions = [normalized_body.find(fragment) for fragment in expected_fragments]
        if any(position == -1 for position in positions) or positions != sorted(positions):
            violations.append("Local CommitLog validation decision matrix changed")
        if normalized_body.count("CommitLogFileLoadDecision::") != 2:
            violations.append("Local CommitLog validation added decision paths")

    forbidden = ["std::fs", "rayon", "cheetah", "DefaultMappedFile", "remove_file", "metadata("]
    if any(token in active for token in forbidden):
        violations.append("Local CommitLog validation absorbed Store I/O or orchestration")
    if any(
        kind == "use" and (" as " in body or "{" in body or "*" in body)
        for kind, _, body, _ in active_import_records(production)
    ):
        violations.append("Local CommitLog validation forbids alias/brace/glob imports")
    if normalized.count("fnvalidate_commit_log_file") != 1:
        violations.append("Local CommitLog validation owner count changed")
    return violations


def _closure_body(function_body: str, marker: str) -> str | None:
    active = active_rust_source(function_body)
    marker_index = active.find(marker)
    if marker_index == -1:
        return None
    opening_brace = active.find("{", marker_index + len(marker))
    if opening_brace == -1:
        return None
    extracted = braced_body(active, opening_brace)
    return None if extracted is None else extracted[0]


def store_commit_log_file_validation_violations(source: str) -> list[str]:
    production = source.split("#[cfg(test)]", maxsplit=1)[0]
    active = active_rust_source(production)
    normalized = re.sub(r"\s+", "", active)
    violations: list[str] = []

    local_prefix = "rocketmq_store_local::commit_log::load::"
    private_imports = []
    for visibility, body, _ in active_use_records(production):
        if not body.startswith(local_prefix):
            continue
        if " as " in body or "{" in body or "*" in body:
            violations.append("Store CommitLog validation imports forbid alias/brace/glob")
        if not visibility:
            private_imports.append(body.removeprefix(local_prefix))
    if sorted(private_imports) != sorted(
        [
            "validate_commit_log_file",
            "CommitLogFileLoadDecision",
            "CommitLogFileMetadata",
        ]
    ):
        violations.append("Store CommitLog validation imports changed")

    if re.search(r"\bstruct\s+FileMetadata\b|\bfile_name\s*:", active):
        violations.append("Store retained private CommitLog metadata copy")
    if re.search(r"\bsize\s*==\s*0\b|\bsize\s*!=\s*expected(?:_size)?\b", active):
        violations.append("Store copied CommitLog size validation")
    if "not matched expected size" in production:
        violations.append("Store copied CommitLog validation message")
    if re.search(r"\b(?:stats\.)?files_removed\s*(?:\+=|=)", active):
        violations.append("Store changed legacy files_removed accounting")

    load_body = named_function_body(production, "load_optimized")
    if load_body is None:
        violations.append("CommitLogLoader load entrypoint missing")
    else:
        normalized_load = re.sub(r"\s+", "", load_body)
        collect_position = normalized_load.find(
            "letfile_metadata:Vec<CommitLogFileMetadata>=ifself.enable_parallel&&file_paths.len()>4"
        )
        create_position = normalized_load.find(
            "let(mapped_files,mmap_advice_stats)=ifself.enable_parallel&&file_metadata.len()>4"
        )
        if (
            collect_position == -1
            or create_position == -1
            or collect_position >= create_position
            or "self.create_mapped_files_parallel(&file_metadata)?" not in normalized_load
            or "self.create_mapped_files_sequential(&file_metadata)?" not in normalized_load
        ):
            violations.append("Store mmap creation no longer follows complete metadata validation")

    expected_public_signatures = [
        "pubfnnew(store_path:String,mapped_file_size:u64,enable_parallel:bool)->Self",
        "pubfnnew_with_recovery_mmap_advice(store_path:String,mapped_file_size:u64,enable_parallel:bool,recovery_mmap_advice:RecoveryMmapAdvice,)->Self",
        "pubfnnew_with_recovery_hints(store_path:String,mapped_file_size:u64,enable_parallel:bool,recovery_mmap_advice:RecoveryMmapAdvice,recovery_file_prefetch:RecoveryFilePrefetch,)->Self",
        "pubfnwith_lazy_mmap(mutself,lazy_mmap_enable:bool)->Self",
        "pubfnload_optimized(&self)->io::Result<(Vec<Arc<DefaultMappedFile>>,LoadStatistics)>",
    ]
    if any(signature not in normalized for signature in expected_public_signatures):
        violations.append("CommitLogLoader public signatures changed")

    parallel = named_function_body(production, "collect_metadata_parallel")
    sequential = named_function_body(production, "collect_metadata_sequential")
    if parallel is None or sequential is None:
        violations.append("Store CommitLog metadata collectors missing")
        return violations

    parallel_normalized = re.sub(r"\s+", "", parallel)
    sequential_normalized = re.sub(r"\s+", "", sequential)
    parallel_closure = _closure_body(parallel, ".map(|(idx, path)|")
    sequential_loop = _closure_body(sequential, "for (idx, path) in paths.iter().enumerate()")
    collectors = [
        ("parallel", parallel, parallel_normalized, parallel_closure, "idx==last_file_idx"),
        ("sequential", sequential, sequential_normalized, sequential_loop, "idx==last_file_idx"),
    ]
    for name, body, body_normalized, scope, last_expression in collectors:
        if scope is None:
            violations.append(f"Store {name} CommitLog validation scope changed")
            continue
        scope_normalized = re.sub(r"\s+", "", scope)
        call = (
            "validate_commit_log_file(&metadata,expected_size,"
            f"{last_expression})"
        )
        if scope_normalized.count(call) != 1 or body_normalized.count("validate_commit_log_file(") != 1:
            violations.append(f"Store {name} CommitLog validation call changed")
        metadata_build = "letmetadata=CommitLogFileMetadata{path:path.clone(),size,};"
        positions = [
            scope_normalized.find("fs::metadata(path)"),
            scope_normalized.find(metadata_build),
            scope_normalized.find(call),
        ]
        if any(position == -1 for position in positions) or positions != sorted(positions):
            violations.append(f"Store {name} CommitLog validation ordering changed")
        load_adapter = (
            "Ok(CommitLogFileLoadDecision::Load)=>Ok(Some(metadata))"
            if name == "parallel"
            else "Ok(CommitLogFileLoadDecision::Load)=>metadata_list.push(metadata)"
        )
        error_adapter = (
            "Err(error)=>Err(io::Error::new(io::ErrorKind::InvalidData,error))"
            if name == "parallel"
            else "Err(error)=>returnErr(io::Error::new(io::ErrorKind::InvalidData,error))"
        )
        required_decisions = [
            load_adapter,
            "Ok(CommitLogFileLoadDecision::RemoveEmptyLast)=>",
            error_adapter,
        ]
        if any(decision not in scope_normalized for decision in required_decisions):
            violations.append(f"Store {name} CommitLog validation decision adapter changed")
        removal_index = scope_normalized.find("Ok(CommitLogFileLoadDecision::RemoveEmptyLast)=>")
        removal_scope = scope_normalized[removal_index:] if removal_index >= 0 else ""
        if (
            "ifletErr(error)=fs::remove_file(path)" not in removal_scope
            or "warn!(" not in removal_scope
            or (name == "parallel" and "Ok(None)" not in removal_scope)
            or re.search(r"fs::remove_file\(path\)[^;{}]*\?", removal_scope)
        ):
            violations.append(f"Store {name} empty-last removal semantics changed")

    if not all(
        token in parallel_normalized
        for token in [
            ".par_iter().enumerate().map(|(idx,path)|",
            ".collect();",
            "results.map(|metadata|metadata.into_iter().flatten().collect())",
        ]
    ):
        violations.append("Store parallel CommitLog order or flatten contract changed")
    if "for(idx,path)inpaths.iter().enumerate()" not in sequential_normalized:
        violations.append("Store sequential CommitLog first-error traversal changed")

    expected_private_signatures = [
        "fncollect_metadata_parallel(&self,paths:&[PathBuf])->io::Result<Vec<CommitLogFileMetadata>>",
        "fncollect_metadata_sequential(&self,paths:&[PathBuf])->io::Result<Vec<CommitLogFileMetadata>>",
        "fncreate_mapped_files_parallel(&self,metadata:&[CommitLogFileMetadata],)->io::Result<(Vec<Arc<DefaultMappedFile>>,LoadStatistics)>",
        "fncreate_mapped_files_sequential(&self,metadata:&[CommitLogFileMetadata],)->io::Result<(Vec<Arc<DefaultMappedFile>>,LoadStatistics)>",
        "fncreate_mapped_file(&self,meta:&CommitLogFileMetadata,idx:usize,file_count:usize,)->io::Result<DefaultMappedFile>",
    ]
    if any(signature not in normalized for signature in expected_private_signatures):
        violations.append("Store canonical CommitLog metadata flow changed")
    return violations


class StoreLocalContractTests(unittest.TestCase):
    def assert_local_crate_exists(self) -> None:
        self.assertTrue(LOCAL_CRATE.is_dir(), "canonical rocketmq-store-local crate is missing")

    def test_reexport_scanner_ignores_comments_and_strings(self) -> None:
        source = '''
// pub use rocketmq_store_local::mapped_file::DirectIoBuffer;
/* pub use rocketmq_store_local::mapped_file::MappedFileError; */
const TEXT: &str = "pub use rocketmq_store_local::mapped_file::FlushStrategy;";
const RAW: &str = r#"pub use rocketmq_store_local::mapped_file::MappedBuffer;"#;
pub use rocketmq_store_local::mapped_file::MappedFileMetrics;
'''
        self.assertEqual({"MappedFileMetrics"}, active_facade_reexports(source))

    def test_commit_log_reexport_scanner_ignores_comments_and_strings(self) -> None:
        source = '''
// pub use rocketmq_store_local::commit_log::load::LoadStatistics;
/* pub use rocketmq_store_local::commit_log::recovery::RecoveryStatistics; */
const TEXT: &str = "pub use rocketmq_store_local::commit_log::load::RecoveryMmapAdvice;";
pub use rocketmq_store_local::commit_log::recovery::AbnormalRecoveryWindow;
'''
        self.assertEqual(
            {"AbnormalRecoveryWindow": "recovery"},
            active_commit_log_facade_reexports(source),
        )

    def test_commit_log_record_contract_rejects_dynamic_port_and_masks_inactive_text(self) -> None:
        valid = "pub struct CommitLogFrameCursor<S: CommitLogFrameSource> { source: S }"
        dynamic = "pub struct Bad { source: Box<dyn CommitLogFrameSource> }"
        masked = r'''
// dyn CommitLogFrameSource
/* const PARSE_BATCH_SIZE: usize = 1; */
const TEXT: &str = "struct BatchMessageIterator";
const RAW: &str = r#"fn MIN_MESSAGE_SIZE() {}"#;
'''
        self.assertEqual([], commit_log_record_boundary_violations(valid))
        self.assertEqual(
            ["dynamic CommitLogFrameSource"],
            commit_log_record_boundary_violations(dynamic),
        )
        self.assertEqual([], commit_log_record_boundary_violations(masked))

    def test_commit_log_record_contract_rejects_qualified_dyn_and_alias_imports(self) -> None:
        qualified_dynamic = (
            "pub struct Bad { source: Box<dyn "
            "rocketmq_store_local::commit_log::record::CommitLogFrameSource> }"
        )
        aliased_dynamic = '''
use rocketmq_store_local::commit_log::record::CommitLogFrameSource as FrameSource;
pub struct Bad { source: Box<dyn FrameSource> }
'''
        self.assertEqual(
            ["dynamic CommitLogFrameSource"],
            commit_log_record_boundary_violations(qualified_dynamic),
        )
        self.assertIn(
            "forbidden alias/brace import",
            commit_log_record_boundary_violations(aliased_dynamic),
        )

    def test_store_record_facade_contract_rejects_alias_brace_glob_and_algorithm_mutations(self) -> None:
        self.assertEqual(
            [],
            store_record_facade_violations(
                VALID_COMMIT_LOG_RECORD_FACADE,
                VALID_RECOVERY_RECORD_FACADE,
            ),
        )
        for mutated_commit_log in [
            "pub use rocketmq_store_local::commit_log::record::MESSAGE_MAGIC_CODE as CODE;",
            "pub use rocketmq_store_local::commit_log::record::{MESSAGE_MAGIC_CODE, BLANK_MAGIC_CODE};",
            "pub use rocketmq_store_local::commit_log::record::*;",
        ]:
            self.assertNotEqual(
                [],
                store_record_facade_violations(
                    mutated_commit_log,
                    VALID_RECOVERY_RECORD_FACADE,
                ),
            )
        copied_recovery = VALID_RECOVERY_RECORD_FACADE + "\nconst PARSE_BATCH_SIZE: usize = 65536;"
        self.assertIn(
            "legacy iterator constants copied",
            store_record_facade_violations(
                VALID_COMMIT_LOG_RECORD_FACADE,
                copied_recovery,
            ),
        )

    def test_store_record_wrapper_contract_rejects_method_and_import_mutations(self) -> None:
        wrong_new_signature = VALID_RECOVERY_RECORD_FACADE.replace(
            "new(mapped_file: &'a Arc<DefaultMappedFile>)",
            "new(mapped_file: &Arc<DefaultMappedFile>)",
        )
        copied_next_algorithm = VALID_RECOVERY_RECORD_FACADE.replace(
            "self.inner.next_message()",
            '''
let mut peek = self.buffer.clone();
let total_size = peek.get_i32();
if total_size <= 0 || !self.refill_buffer() { return None; }
self.buffer.copy_to_bytes(total_size as usize)
''',
        )
        hard_coded_offset = VALID_RECOVERY_RECORD_FACADE.replace("self.inner.current_offset()", "999")
        brace_recovery_import = VALID_RECOVERY_RECORD_FACADE.replace(
            "pub use rocketmq_store_local::commit_log::record::is_blank_message;",
            "pub use rocketmq_store_local::commit_log::record::{is_blank_message};",
        )
        for mutation in [
            wrong_new_signature,
            copied_next_algorithm,
            hard_coded_offset,
            brace_recovery_import,
        ]:
            self.assertNotEqual(
                [],
                store_record_facade_violations(
                    VALID_COMMIT_LOG_RECORD_FACADE,
                    mutation,
                ),
            )

    def test_commit_log_record_owner_scanner_rejects_duplicate_constants_functions_and_aliases(self) -> None:
        canonical = Path("record.rs")
        duplicate = Path("duplicate.rs")
        masked = Path("masked.rs")
        sources = {
            canonical: "pub const MESSAGE_MAGIC_CODE: i32 = -626843481; pub fn is_blank_message() {}",
            duplicate: "const MESSAGE_MAGIC_CODE: i32 = 1; use x::other as is_blank_message;",
            masked: '// const MESSAGE_MAGIC_CODE: i32 = 2; const TEXT: &str = "fn is_blank_message";',
        }
        self.assertEqual(
            [(duplicate, "const"), (canonical, "const")],
            commit_log_record_owner_occurrences(sources, "MESSAGE_MAGIC_CODE"),
        )
        self.assertEqual(
            [(duplicate, "alias"), (canonical, "fn")],
            commit_log_record_owner_occurrences(sources, "is_blank_message"),
        )

    def test_commit_log_record_parser_contract_rejects_mutable_buf_dynamic_and_import_mutations(self) -> None:
        valid = '''
use bytes::Bytes;
pub trait CommitLogRecordChecksum {}
pub fn decode_commit_log_record<C: CommitLogRecordChecksum>(input: &Bytes) {}
'''
        self.assertEqual([], commit_log_record_parser_boundary_violations(valid))
        mutations = [
            valid.replace("input: &Bytes", "input: &mut Bytes"),
            valid + "fn raw(mut input: Bytes) { input.get_i32(); }",
            valid + "struct Bad { checksum: Box<dyn CommitLogRecordChecksum> }",
            valid.replace("use bytes::Bytes;", "use bytes::{Bytes};"),
            valid.replace("use bytes::Bytes;", "use bytes::*;"),
            valid.replace("use bytes::Bytes;", "use bytes::Bytes as RawBytes;"),
            valid + "enum Outcome { Message(Box<CommitLogRecord>) }",
        ]
        for mutation in mutations:
            self.assertNotEqual([], commit_log_record_parser_boundary_violations(mutation))

    def test_commit_log_record_parser_contract_rejects_blank_boundary_removal(self) -> None:
        valid = '''
use bytes::Bytes;
pub trait CommitLogRecordChecksum {}
pub fn decode_commit_log_record<C: CommitLogRecordChecksum>(input: &Bytes) {
    let declared_len = 8usize;
    if input.len() < declared_len { return; }
    let magic_code = BLANK_MAGIC_CODE;
    if magic_code == BLANK_MAGIC_CODE { return; }
}
'''
        self.assertEqual([], commit_log_record_parser_boundary_violations(valid))
        mutation = valid.replace("if input.len() < declared_len { return; }", "")
        self.assertNotEqual([], commit_log_record_parser_boundary_violations(mutation))

    def test_commit_log_record_parser_contract_rejects_direct_heap_allocation(self) -> None:
        mutation = '''
use bytes::Bytes;
pub fn decode_commit_log_record<C>(input: &Bytes) {}
struct Heap(Box<CommitLogRecord>);
'''
        self.assertNotEqual([], commit_log_record_parser_boundary_violations(mutation))

    def test_commit_log_record_parser_contract_rejects_qualified_heap_allocation(self) -> None:
        mutation = '''
use bytes::Bytes;
pub fn decode_commit_log_record<C>(input: &Bytes) {}
struct Heap(std::boxed::Box<CommitLogRecord>);
'''
        self.assertNotEqual([], commit_log_record_parser_boundary_violations(mutation))

    def test_commit_log_record_parser_contract_rejects_heap_type_alias(self) -> None:
        mutation = '''
use bytes::Bytes;
pub fn decode_commit_log_record<C>(input: &Bytes) {}
type HeapRecord = Box<CommitLogRecord>;
enum Outcome { Message(HeapRecord) }
'''
        self.assertNotEqual([], commit_log_record_parser_boundary_violations(mutation))

    def test_commit_log_record_parser_heap_contract_masks_comments_and_strings(self) -> None:
        masked = r'''
use bytes::Bytes;
pub fn decode_commit_log_record<C>(input: &Bytes) {}
// Box<CommitLogRecord>
/* type HeapRecord = std::boxed::Box<CommitLogRecord>; */
const TEXT: &str = "Box<CommitLogRecord>";
const RAW: &str = r#"type HeapRecord = Box<CommitLogRecord>;"#;
'''
        self.assertEqual([], commit_log_record_parser_boundary_violations(masked))

    def test_normal_recovery_state_contract_rejects_semantic_and_ownership_mutations(self) -> None:
        source = (LOCAL_CRATE / "src" / "commit_log" / "recovery.rs").read_text(encoding="utf-8")
        self.assertEqual([], normal_recovery_state_boundary_violations(source))
        mutations = [
            source.replace(
                "(NormalRecoveryAction::ContinueRecord, start_offset, end_offset)",
                "(NormalRecoveryAction::ContinueRecord, end_offset, end_offset)",
                1,
            ),
            source[: source.index("pub enum NormalRecoveryEvent")]
            + source[source.index("pub enum NormalRecoveryEvent") :].replace(
                "SourceEnded,", "SourceEnded { kind: u8 },", 1
            ),
            source.replace(
                "let (action, next_last_valid, next_truncate) = match event {",
                "let controller = 0;\n        let (action, next_last_valid, next_truncate) = match event {",
                1,
            ),
            source[: source.index("impl NormalRecoveryState")]
            + source[source.index("impl NormalRecoveryState") :].replace(
                "segment_base.checked_add(relative_start)", "segment_base + relative_start", 1
            ),
            source.replace("use tracing::info;", "use tracing::info as recovery_info;", 1),
        ]
        for mutation in mutations:
            self.assertNotEqual([], normal_recovery_state_boundary_violations(mutation))

    def test_abnormal_recovery_state_contract_rejects_matrix_and_ownership_mutations(self) -> None:
        source = (LOCAL_CRATE / "src" / "commit_log" / "recovery.rs").read_text(encoding="utf-8")
        self.assertEqual([], abnormal_recovery_state_boundary_violations(source))
        mutations = [
            source.replace("confirm_candidate <= confirm_offset", "confirm_candidate < confirm_offset", 1),
            source.replace(
                "self.policy == AbnormalRecoveryPolicy::Optimized\n"
                "                        || matches!(dispatch_gate, AbnormalRecoveryDispatchGate::ConfirmBounded { .. })",
                "matches!(dispatch_gate, AbnormalRecoveryDispatchGate::ConfirmBounded { .. })",
                1,
            ),
            source.replace(
                "|| matches!(dispatch_gate, AbnormalRecoveryDispatchGate::ConfirmBounded { .. })",
                "|| true",
                1,
            ),
            source.replace(
                "AbnormalRecoveryPolicy::Standard => AbnormalRecoveryAction::StopRecovery",
                "AbnormalRecoveryPolicy::Standard => AbnormalRecoveryAction::ContinueNextSegment",
                1,
            ),
            source.replace(
                "policy: AbnormalRecoveryPolicy,",
                "policy: AbnormalRecoveryPolicy,\n    controller: bool,",
                1,
            ),
            source.replace("validated_size: u64,", "input_size: u64,", 1),
            source.replace(
                "let (action, next_last_valid, next_confirm_valid, next_truncate) = match event {",
                "let stats = 0;\n        let (action, next_last_valid, next_confirm_valid, next_truncate) = match event {",
                1,
            ),
            source.replace("segment_base.checked_add(relative_start)", "segment_base + relative_start", 1),
            source.replace("use tracing::info;", "use tracing::info as recovery_info;", 1),
        ]
        for mutation_index, mutation in enumerate(mutations):
            with self.subTest(mutation_index=mutation_index):
                self.assertNotEqual(source, mutation)
                self.assertNotEqual([], abnormal_recovery_state_boundary_violations(mutation))

    def test_store_normal_recovery_adapters_reject_branch_bypass_and_abnormal_mutations(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        self.assertEqual([], store_normal_recovery_adapter_violations(source))
        mutations = [
            source.replace("NormalRecoveryEvent::Blank", "NormalRecoveryEvent::SourceEnded", 1),
            source.replace("NormalRecoveryEvent::InvalidRecord", "NormalRecoveryEvent::Blank", 1),
            source.replace("NormalRecoveryEvent::SourceEnded", "NormalRecoveryEvent::InvalidRecord", 1),
            source.replace("normal_recovery.apply", "normal_recovery_bypass.apply", 1),
            source.replace("NormalRecoveryPolicy::Standard", "NormalRecoveryPolicy::Optimized", 1),
            source.replace(
                "let do_dispatch = false;",
                "let mut last_valid_copy = 0;\n            match NormalRecoveryPolicy::Standard {"
                " NormalRecoveryPolicy::Standard => last_valid_copy = 1, _ => {} }\n"
                "            let do_dispatch = false;",
                1,
            ),
            source.replace(
                "let do_dispatch = false;",
                "let mut truncate_offset_copy = 0;\n            truncate_offset_copy = 1;\n"
                "            let do_dispatch = false;",
                1,
            ),
            source.replace("match normal_recovery.apply", "let _ = normal_recovery.apply", 1),
            source.replace(
                "pub async fn recover_abnormally_optimized",
                "pub async fn recover_abnormally_optimized_changed",
                1,
            ),
        ]
        for mutation_index, mutation in enumerate(mutations):
            with self.subTest(mutation_index=mutation_index):
                self.assertNotEqual([], store_normal_recovery_adapter_violations(mutation))

    def test_store_normal_recovery_contract_rejects_empty_message_action(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        mutation = source.replace(
            """Ok(NormalRecoveryAction::ContinueNextSegment) => {
                            record_closed_segment = true;
                            break;
                        }""",
            "Ok(NormalRecoveryAction::ContinueNextSegment) => {}",
            1,
        )
        self.assertNotEqual(source, mutation)
        self.assertNotEqual([], store_normal_recovery_adapter_violations(mutation))

    def test_store_normal_recovery_contract_rejects_plain_current_position_addition(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        mutation = source.replace("current_pos.checked_add(size)", "Some(current_pos + size)", 1)
        self.assertNotEqual(source, mutation)
        self.assertNotEqual([], store_normal_recovery_adapter_violations(mutation))

    def test_store_normal_recovery_contract_rejects_handmade_final_summary(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        mutation = source.replace(
            "let summary = normal_recovery.summary();",
            """let summary = rocketmq_store_local::commit_log::recovery::NormalRecoverySummary {
            last_valid_offset: 0,
            truncate_offset: 0,
        };""",
            1,
        )
        self.assertNotEqual(source, mutation)
        self.assertNotEqual([], store_normal_recovery_adapter_violations(mutation))

    def test_store_abnormal_recovery_adapters_use_local_state(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        self.assertEqual([], store_abnormal_recovery_adapter_violations(source))

    def test_store_abnormal_recovery_contract_rejects_late_process_message(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        optimized_start = source.index("pub async fn recover_abnormally_optimized")
        prefix = source[:optimized_start]
        optimized = source[optimized_start:]
        process_message = (
            "                let mut dispatch_request = "
            "recovery_ctx.process_message(&mut msg_bytes, absolute_offset);\n\n"
        )
        self.assertIn(process_message, optimized)
        mutation = optimized.replace(process_message, "", 1).replace(
            "                    let validated_size = match u64::try_from(dispatch_request.msg_size) {",
            process_message
            + "                    let validated_size = match u64::try_from(dispatch_request.msg_size) {",
            1,
        )
        self.assertNotEqual(source, prefix + mutation)
        self.assertNotEqual([], store_abnormal_recovery_adapter_violations(prefix + mutation))

    def test_store_abnormal_recovery_contract_rejects_dispatch_only_file_processed(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        optimized_start = source.index("pub async fn recover_abnormally_optimized")
        prefix = source[:optimized_start]
        optimized = source[optimized_start:]
        dispatch = (
            "                        Ok(AbnormalRecoveryAction::DispatchMessage) => {\n"
            "                            self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, false);\n"
            "                        }"
        )
        dispatch_only = dispatch.replace(
            "                            self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, false);",
            "                            self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, false);\n"
            "                            file_processed = true;",
            1,
        )
        mutation = optimized.replace(
            "                    file_processed = true;\n",
            "",
            1,
        ).replace(dispatch, dispatch_only, 1)
        self.assertNotEqual(source, prefix + mutation)
        self.assertNotEqual([], store_abnormal_recovery_adapter_violations(prefix + mutation))

    def test_store_abnormal_recovery_contract_rejects_review_mutations(self) -> None:
        source = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        self.assertEqual([], store_abnormal_recovery_adapter_violations(source))
        abnormal_start = source.index("pub async fn recover_abnormally_optimized")

        def mutate_abnormal(old: str, new: str) -> str:
            return source[:abnormal_start] + source[abnormal_start:].replace(old, new, 1)

        mutations = [
            mutate_abnormal("self.get_confirm_offset().max(0)", "initial_offset"),
            source.replace(
                "abnormal_confirm_candidate_end(dispatch_request.commit_log_offset, msg_size)",
                "abnormal_confirm_candidate_end(dispatch_request.commit_log_offset, validated_size)",
                1,
            ),
            source.replace(
                "Ok(AbnormalRecoveryAction::SkipMessageDispatch) => {}",
                "Ok(AbnormalRecoveryAction::SkipMessageDispatch) => {\n                            self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, false);\n                        }",
                1,
            ),
            source.replace("let initial_offset = if index == 0", "let initial_offset = if false", 1),
            source.replace(
                "let summary = abnormal_recovery.summary();",
                "let summary = rocketmq_store_local::commit_log::recovery::AbnormalRecoverySummary { last_valid_offset: 0, confirm_valid_offset: 0, truncate_offset: 0 };",
                1,
            ),
            source.replace(
                "message_store.get_min_phy_offset(), confirm_valid_offset",
                "message_store.get_min_phy_offset(), last_valid_offset",
                1,
            ),
            mutate_abnormal("self.set_confirm_offset(last_valid_offset)", "self.set_confirm_offset(process_offset)"),
            mutate_abnormal("summary.truncate_offset", "summary.last_valid_offset"),
            mutate_abnormal(
                "self.on_commit_log_dispatch(&mut dispatch_request, do_dispatch, true, true);",
                "self.dispatcher.dispatch(&dispatch_request);",
            ),
            mutate_abnormal("file_processed = true;", "file_processed = false;"),
            source.replace("current_pos.checked_add(input_size)", "current_pos + input_size", 1),
            source.replace("abnormal_confirm_candidate_end", "abnormal_confirm_candidate_end_bypass", 1),
            source.replace("AbnormalRecoveryPolicy::Standard", "AbnormalRecoveryPolicy::Optimized", 1),
        ]
        for mutation_index, mutation in enumerate(mutations):
            with self.subTest(mutation_index=mutation_index):
                self.assertNotEqual(source, mutation)
                self.assertNotEqual([], store_abnormal_recovery_adapter_violations(mutation))

    def test_store_record_parser_wrapper_contract_rejects_copy_and_import_mutations(self) -> None:
        root = "pub mod commit_log;"
        wrapper = VALID_STORE_RECORD_PARSER_WRAPPER
        self.assertEqual([], store_record_parser_wrapper_violations(root, wrapper))
        mutations = [
            (root + "\nmod commit_log_record_parser;", wrapper),
            (root, wrapper.replace("bytes.advance(8);", "bytes.get_i32();")),
            (root, wrapper.replace(
                "use rocketmq_store_local::commit_log::record_parser::CommitLogRecordOutcome;",
                "use rocketmq_store_local::commit_log::record_parser::*;",
            )),
        ]
        for mutated_root, mutated_wrapper in mutations:
            self.assertNotEqual(
                [],
                store_record_parser_wrapper_violations(mutated_root, mutated_wrapper),
            )

    def test_store_record_parser_wrapper_contract_rejects_signature_and_decode_mutations(self) -> None:
        root = "pub mod commit_log;"
        mutations = [
            VALID_STORE_RECORD_PARSER_WRAPPER.replace("check_crc: bool", "check_crc: i32"),
            VALID_STORE_RECORD_PARSER_WRAPPER.replace(
                "    let _ = decode_commit_log_record(bytes, body_mode, &checksum);\n",
                "",
            ),
        ]
        for mutation in mutations:
            self.assertNotEqual([], store_record_parser_wrapper_violations(root, mutation))

    def test_store_record_parser_wrapper_contract_rejects_manual_raw_parsing_and_advance_shape_mutations(self) -> None:
        root = "pub mod commit_log;"
        mutations = [
            VALID_STORE_RECORD_PARSER_WRAPPER.replace(
                "    let _ = decode_commit_log_record(bytes, body_mode, &checksum);",
                "    let _ = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);\n"
                "    let _ = decode_commit_log_record(bytes, body_mode, &checksum);",
            ),
            VALID_STORE_RECORD_PARSER_WRAPPER.replace(
                "bytes.advance(8);",
                "bytes.advance(total_size as usize);",
            ),
        ]
        for mutation in mutations:
            self.assertNotEqual([], store_record_parser_wrapper_violations(root, mutation))

    def test_tracing_target_scanner_ignores_comments_and_strings(self) -> None:
        source = r'''
// info!(target: "commented", "ignored");
/* info!(target: "block", "ignored"); */
const TEXT: &str = "info!(target: \"string\", \"ignored\")";
const RAW: &str = r#"info!(target: "raw", "ignored")"#;
info!(target: "rocketmq_store::log_file::commit_log_loader", "active");
info!("missing target");
'''
        self.assertEqual(
            ["rocketmq_store::log_file::commit_log_loader", None],
            active_tracing_info_targets(source),
        )

    def test_tokio_uring_dependency_must_not_also_be_top_level(self) -> None:
        manifest = {
            "dependencies": {"tokio-uring": {"version": "0.5", "optional": True}},
            "target": {
                'cfg(target_os = "linux")': {
                    "dependencies": {"tokio-uring": {"version": "0.5", "optional": True}}
                }
            },
        }
        self.assertFalse(has_linux_only_optional_tokio_uring(manifest))

    def test_tokio_uring_dependency_rejects_windows_target_and_top_level_build_dependency(self) -> None:
        linux_dependency = {"tokio-uring": {"version": "0.5", "optional": True}}
        windows_target = {
            "target": {
                'cfg(target_os = "linux")': {"dependencies": linux_dependency},
                'cfg(target_os = "windows")': {"dependencies": linux_dependency},
            }
        }
        top_level_build = {
            "build-dependencies": linux_dependency,
            "target": {'cfg(target_os = "linux")': {"dependencies": linux_dependency}},
        }
        self.assertFalse(has_linux_only_optional_tokio_uring(windows_target))
        self.assertFalse(has_linux_only_optional_tokio_uring(top_level_build))

    def test_libc_dependency_must_be_one_unix_normal_dependency(self) -> None:
        valid = {"target": {"cfg(unix)": {"dependencies": {"libc": "0.2.186"}}}}
        top_level = {
            "dependencies": {"libc": "0.2.186"},
            "target": {"cfg(unix)": {"dependencies": {"libc": "0.2.186"}}},
        }
        build_only = {"target": {"cfg(unix)": {"build-dependencies": {"libc": "0.2.186"}}}}
        self.assertTrue(has_unix_only_normal_libc(valid))
        self.assertFalse(has_unix_only_normal_libc(top_level))
        self.assertFalse(has_unix_only_normal_libc(build_only))

    def test_canonical_function_definition_scanner_detects_duplicate_active_definitions(self) -> None:
        first = Path("first.rs")
        second = Path("second.rs")
        commented = Path("commented.rs")
        sources = {
            first: "pub fn io_uring_backend_status() {}",
            second: "pub fn io_uring_backend_status() {}",
            commented: "// pub fn io_uring_backend_status() {}",
        }
        self.assertEqual(
            [first, second],
            canonical_definition_paths(sources, "io_uring_backend_status", "fn"),
        )

    def test_kernel_scanners_ignore_comments_and_strings(self) -> None:
        facade = '''
// pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResource;
const TEXT: &str = "pub use rocketmq_store_local::mapped_file::kernel::MappedFileProgress;";
pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResourceBase;
'''
        owner = '''
pub struct DefaultMappedFile {
    progress: MappedFileProgress,
    // wrote_position: AtomicI32,
    text: &'static str,
}
const TEXT: &str = "struct DefaultMappedFile { flushed_position: AtomicI32 }";
'''
        self.assertEqual({"ReferenceResourceBase"}, active_kernel_reexports(facade))
        self.assertEqual(
            ["pub(crate) use ReferenceResourceBase"],
            active_kernel_use_statements(facade),
        )
        self.assertIn("progress: MappedFileProgress", active_struct_body(owner, "DefaultMappedFile"))
        self.assertNotIn("wrote_position", active_struct_body(owner, "DefaultMappedFile"))
        self.assertEqual([], default_mapped_file_progress_violations(owner))

    def test_file_scanners_ignore_comments_and_strings(self) -> None:
        facade = '''
// pub use rocketmq_store_local::mapped_file::file::FilePreallocateOutcome;
const TEXT: &str = "pub use rocketmq_store_local::mapped_file::file::preallocate_file;";
pub use rocketmq_store_local::mapped_file::file::classify_file_preallocate_result;
'''
        owner = '''
pub struct DefaultMappedFile {
    storage: MappedFileStorage,
    // file: File,
    text: &'static str,
}
const TEXT: &str = "struct DefaultMappedFile { file_from_offset: u64 }";
'''
        self.assertEqual(
            ["pub use classify_file_preallocate_result"],
            active_file_use_statements(facade),
        )
        self.assertEqual([], default_mapped_file_storage_violations(owner))

    def test_file_owner_scanner_rejects_type_alias_and_duplicate_owner(self) -> None:
        canonical = Path("canonical.rs")
        duplicate = Path("duplicate.rs")
        alias = Path("alias.rs")
        self.assertEqual(
            [(alias, "type"), (canonical, "struct"), (duplicate, "struct")],
            file_item_owner_occurrences(
                {
                    canonical: "pub struct MappedFileStorage { file: File }",
                    duplicate: "struct MappedFileStorage;",
                    alias: "pub type MappedFileStorage = usize;",
                },
                "MappedFileStorage",
            ),
        )

    def test_file_reexport_scanner_exposes_alias_brace_and_glob_bypasses(self) -> None:
        source = '''
pub use rocketmq_store_local::mapped_file::file::FilePreallocateOutcome as Outcome;
pub use rocketmq_store_local::mapped_file::file::{preallocate_file, PREALLOCATE_UNSUPPORTED_ERRNO};
pub use rocketmq_store_local::mapped_file::file::*;
'''
        self.assertEqual(
            {
                "pub use FilePreallocateOutcome as Outcome",
                "pub use {preallocate_file, PREALLOCATE_UNSUPPORTED_ERRNO}",
                "pub use *",
            },
            set(active_file_use_statements(source)),
        )

    def test_default_mapped_file_storage_scanner_rejects_duplicate_and_legacy_fields(self) -> None:
        source = '''
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow: MappedFileStorage,
    file: File,
    file_from_offset: u64,
}
'''
        violations = default_mapped_file_storage_violations(source)
        self.assertIn("legacy storage field: file", violations)
        self.assertIn("legacy storage field: file_from_offset", violations)
        self.assertIn("MappedFileStorage fields must be exactly: storage", violations)

    def test_default_storage_scanner_rejects_renamed_fully_qualified_file_field(self) -> None:
        source = '''
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow_file: std::fs::File,
}
'''
        self.assertIn(
            "direct file owner field: shadow_file",
            default_mapped_file_storage_violations(source),
        )

    def test_default_storage_scanner_rejects_renamed_fully_qualified_path_field(self) -> None:
        source = '''
struct DefaultMappedFile {
    storage: MappedFileStorage,
    canonical_path: std::path::PathBuf,
}
'''
        self.assertIn(
            "direct path owner field: canonical_path",
            default_mapped_file_storage_violations(source),
        )

    def test_default_storage_scanner_rejects_renamed_plain_offset_field(self) -> None:
        source = '''
struct DefaultMappedFile {
    storage: MappedFileStorage,
    segment_offset: u64,
}
'''
        self.assertIn(
            "direct offset owner field: segment_offset",
            default_mapped_file_storage_violations(source),
        )

    def test_default_storage_scanner_rejects_import_as_file_alias(self) -> None:
        source = '''
use std::fs::File as SegmentHandle;
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow_file: SegmentHandle,
}
'''
        self.assertTrue(
            any(
                violation.startswith("forbidden alias/brace use:")
                for violation in default_mapped_file_storage_violations(source)
            )
        )

    def test_default_storage_scanner_rejects_recursive_type_alias(self) -> None:
        source = '''
type RawHandle = std::fs::File;
type SegmentHandle = RawHandle;
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow_file: SegmentHandle,
}
'''
        self.assertTrue(
            any(
                violation.startswith("forbidden type alias:")
                for violation in default_mapped_file_storage_violations(source)
            )
        )

    def test_default_storage_scanner_rejects_brace_import_alias_bypass(self) -> None:
        source = '''
use std::fs::{File as SegmentHandle, Metadata};
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow_file: SegmentHandle,
}
'''
        self.assertTrue(
            any(
                violation.startswith("forbidden alias/brace use:")
                for violation in default_mapped_file_storage_violations(source)
            )
        )

    def test_default_storage_scanner_rejects_generic_default_type_alias_bypass(self) -> None:
        source = '''
type SegmentHandle<T = std::fs::File> = T;
struct DefaultMappedFile {
    storage: MappedFileStorage,
    shadow_file: SegmentHandle,
}
'''
        self.assertTrue(
            any(
                violation.startswith("forbidden type alias:")
                for violation in default_mapped_file_storage_violations(source)
            )
        )

    def test_default_storage_scanner_accepts_atomic_u64_progress_fields(self) -> None:
        source = '''
use crate::utils::ffi::mlock as lock_memory;
use crate::utils::ffi::munlock as unlock_memory;
use windows::Win32::System::Memory::{
    VirtualQuery, MEMORY_BASIC_INFORMATION, MEM_COMMIT,
};
type Target = [u8];
struct DefaultMappedFile {
    storage: MappedFileStorage,
    lazy_mmap_operations: AtomicU64,
    swap_map_time: std::sync::atomic::AtomicU64,
}
'''
        self.assertEqual([], default_mapped_file_storage_violations(source))

    def test_mapping_owner_scanner_rejects_renamed_fields_and_type_aliases(self) -> None:
        renamed = '''
pub struct MappedFileMapping<M> {
    cell: OnceLock<M>,
    init_lock: Mutex<()>,
    lazy_enabled: bool,
    map_operations: AtomicU64,
    map_failures: AtomicU64,
    total_millis: AtomicU64,
    last_millis: AtomicU64,
}
'''
        aliased = '''
type MappingCell<T> = OnceLock<T>;
pub struct MappedFileMapping<M> {
    value: MappingCell<M>,
    init_lock: Mutex<()>,
    lazy_enabled: bool,
    map_operations: AtomicU64,
    map_failures: AtomicU64,
    total_millis: AtomicU64,
    last_millis: AtomicU64,
}
'''
        self.assertNotEqual([], mapped_file_mapping_owner_violations(renamed))
        self.assertNotEqual([], mapped_file_mapping_owner_violations(aliased))

    def test_struct_field_scanner_reads_generic_owner(self) -> None:
        source = "pub struct MappedFileMapping<M> { value: OnceLock<M>, enabled: bool }"
        self.assertEqual(
            [("value", "OnceLock<M>"), ("enabled", "bool")],
            active_struct_fields(source, "MappedFileMapping"),
        )

    def test_mapping_owner_scanner_ignores_comments_and_strings_but_rejects_duplicates(self) -> None:
        canonical = Path("canonical.rs")
        duplicate = Path("duplicate.rs")
        commented = Path("commented.rs")
        string = Path("string.rs")
        sources = {
            canonical: "pub struct MappedFileMapping<M> { value: OnceLock<M> }",
            duplicate: "struct MappedFileMapping<T> { value: T }",
            commented: "// pub struct MappedFileMapping<M> { value: M }",
            string: 'const TEXT: &str = "pub struct MappedFileMapping<M> {}";',
        }
        self.assertEqual(
            [(canonical, "struct"), (duplicate, "struct")],
            file_item_owner_occurrences(sources, "MappedFileMapping"),
        )

    def test_default_mapping_scanner_rejects_renamed_duplicate_and_aliased_owners(self) -> None:
        renamed = '''
struct DefaultMappedFile {
    mapping: MappedFileMapping<ArcMut<MmapMut>>,
    first_create_in_queue: bool,
    shadow_enabled: bool,
    shadow_operations: AtomicU64,
    swap_map_time: AtomicU64,
}
'''
        duplicate = '''
struct DefaultMappedFile {
    mapping: MappedFileMapping<ArcMut<MmapMut>>,
    shadow: MappedFileMapping<ArcMut<MmapMut>>,
    first_create_in_queue: bool,
    swap_map_time: AtomicU64,
}
'''
        aliased = '''
use rocketmq_store_local::mapped_file::mapping::MappedFileMapping as Mapping;
struct DefaultMappedFile {
    mapping: Mapping<ArcMut<MmapMut>>,
    first_create_in_queue: bool,
    swap_map_time: AtomicU64,
}
'''
        self.assertNotEqual([], default_mapped_file_mapping_violations(renamed))
        self.assertNotEqual([], default_mapped_file_mapping_violations(duplicate))
        self.assertNotEqual([], default_mapped_file_mapping_violations(aliased))

    def test_mapping_reexport_scanner_rejects_alias_brace_and_glob_bypasses(self) -> None:
        source = '''
pub use rocketmq_store_local::mapped_file::mapping::LazyMmapStats as Stats;
pub use rocketmq_store_local::mapped_file::mapping::{LazyMmapStats, MappedFileMapping};
pub use rocketmq_store_local::mapped_file::mapping::*;
'''
        self.assertEqual(
            {
                "pub use LazyMmapStats as Stats",
                "pub use {LazyMmapStats, MappedFileMapping}",
                "pub use *",
            },
            set(active_mapping_use_statements(source)),
        )

    def test_storage_owner_scanner_rejects_extra_size_duplicate_and_aliased_field_types(self) -> None:
        source = '''
pub struct MappedFileStorage {
    file: std::fs::File,
    path: CanonicalPath,
    file_from_offset: Offset,
    file_size: u64,
}
'''
        self.assertNotEqual([], mapped_file_storage_owner_violations(source))

    def test_kernel_owner_scanner_rejects_type_alias_bypass(self) -> None:
        alias = Path("alias.rs")
        self.assertEqual(
            [(alias, "type")],
            kernel_item_owner_occurrences(
                {alias: "pub type MappedFileProgress = usize;"},
                "MappedFileProgress",
            ),
        )

    def test_kernel_owner_scanner_rejects_private_and_crate_visible_duplicates(self) -> None:
        private = Path("private.rs")
        crate_visible = Path("crate_visible.rs")
        self.assertEqual(
            [(private, "struct"), (crate_visible, "trait")],
            kernel_item_owner_occurrences(
                {
                    private: "struct ReferenceResource {}",
                    crate_visible: "pub(crate) trait ReferenceResource {}",
                },
                "ReferenceResource",
            ),
        )

    def test_kernel_use_scanner_rejects_alias_and_glob_bypasses(self) -> None:
        facade = Path("facade.rs")
        source = """
pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResource as Shadow;
pub use rocketmq_store_local::mapped_file::kernel::*;
"""
        self.assertEqual(
            {
                "pub(crate) use ReferenceResource as Shadow",
                "pub use *",
            },
            set(active_kernel_use_statements(source)),
        )
        self.assertEqual(
            [
                (
                    facade,
                    "pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResource as Shadow",
                ),
                (
                    facade,
                    "pub use rocketmq_store_local::mapped_file::kernel::*",
                ),
            ],
            kernel_facade_boundary_uses({facade: source}),
        )

    def test_kernel_facade_scanner_rejects_module_alias_reexport(self) -> None:
        facade = Path("module_alias.rs")
        source = """
use rocketmq_store_local::mapped_file::kernel as local_kernel;
pub(crate) use local_kernel::MappedFileProgress;
"""
        self.assertEqual(
            [
                (
                    facade,
                    "use rocketmq_store_local::mapped_file::kernel as local_kernel",
                ),
                (facade, "pub(crate) use local_kernel::MappedFileProgress"),
            ],
            kernel_facade_boundary_uses({facade: source}),
        )

    def test_kernel_facade_scanner_rejects_crate_alias_glob_reexport(self) -> None:
        facade = Path("crate_alias.rs")
        source = """
use rocketmq_store_local as local;
pub use local::mapped_file::kernel::*;
"""
        self.assertEqual(
            [
                (facade, "use rocketmq_store_local as local"),
                (facade, "pub use local::mapped_file::kernel::*"),
            ],
            kernel_facade_boundary_uses({facade: source}),
        )

    def test_store_policy_rejects_rustfmt_root_child_alias(self) -> None:
        facade = Path("root_child_alias.rs")
        source = """
use rocketmq_store_local::{
    mapped_file::kernel as local_kernel,
};
pub(crate) use local_kernel::MappedFileProgress;
"""
        violations = kernel_facade_boundary_uses({facade: source})
        self.assertEqual(2, len(violations))
        self.assertTrue(any("rocketmq_store_local::{" in statement for _, statement in violations))

    def test_store_policy_rejects_rustfmt_mapped_file_child_kernel_alias(self) -> None:
        facade = Path("mapped_file_child_alias.rs")
        source = """
use rocketmq_store_local::mapped_file::{
    kernel as local_kernel,
};
pub(crate) use local_kernel::MappedFileProgress;
"""
        violations = kernel_facade_boundary_uses({facade: source})
        self.assertEqual(2, len(violations))
        self.assertTrue(any("mapped_file::{" in statement for _, statement in violations))

    def test_store_policy_rejects_rustfmt_multi_member_brace_alias(self) -> None:
        facade = Path("multi_member_alias.rs")
        source = """
use rocketmq_store_local::{
    mapped_file::kernel as local_kernel,
    commit_log as local_commit_log,
};
pub(crate) use local_kernel::*;
"""
        self.assertEqual(2, len(kernel_facade_boundary_uses({facade: source})))

    def test_store_policy_rejects_extern_crate_alias(self) -> None:
        facade = Path("extern_alias.rs")
        source = "extern crate rocketmq_store_local as local;"
        self.assertEqual(
            [(facade, "extern crate rocketmq_store_local as local")],
            kernel_facade_boundary_uses({facade: source}),
        )

    def test_store_policy_rejects_every_public_glob(self) -> None:
        facade = Path("public_glob.rs")
        source = "pub(crate) use any_alias::any_module::*;"
        self.assertEqual(
            [(facade, "pub(crate) use any_alias::any_module::*")],
            kernel_facade_boundary_uses({facade: source}),
        )

    def test_kernel_owner_scanner_rejects_enum_and_union_duplicates(self) -> None:
        enum_owner = Path("enum.rs")
        union_owner = Path("union.rs")
        self.assertEqual(
            [(enum_owner, "enum"), (union_owner, "union")],
            kernel_item_owner_occurrences(
                {
                    enum_owner: "pub enum MappedFileProgress { Empty }",
                    union_owner: "union MappedFileProgress { value: usize }",
                },
                "MappedFileProgress",
            ),
        )

    def test_default_mapped_file_scanner_rejects_old_field_with_new_type(self) -> None:
        source = """
struct DefaultMappedFile {
    progress: MappedFileProgress,
    wrote_position: ProgressAtomic,
}
"""
        self.assertIn(
            "legacy progress field: wrote_position",
            default_mapped_file_progress_violations(source),
        )

    def test_default_mapped_file_scanner_rejects_second_progress_kernel_field(self) -> None:
        source = """
struct DefaultMappedFile {
    progress: MappedFileProgress,
    shadow: MappedFileProgress,
}
"""
        self.assertIn(
            "MappedFileProgress fields must be exactly: progress",
            default_mapped_file_progress_violations(source),
        )

    def test_workspace_and_feature_ownership_are_exact(self) -> None:
        self.assert_local_crate_exists()
        root_manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertIn("rocketmq-store-local", root_manifest["workspace"]["members"])
        workspace_dependency = root_manifest["workspace"]["dependencies"]["rocketmq-store-local"]
        self.assertFalse(workspace_dependency["default-features"])

        local_manifest = tomllib.loads((LOCAL_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual(
            {
                "default": [],
                "fast-load": [],
                "safe-load": [],
                "io_uring": ["dep:tokio-uring"],
            },
            local_manifest["features"],
        )
        self.assertNotIn("tokio-uring", local_manifest.get("dependencies", {}))
        self.assertTrue(has_linux_only_optional_tokio_uring(local_manifest))
        self.assertTrue(has_unix_only_normal_libc(local_manifest))
        store_manifest = tomllib.loads((STORE_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        self.assertEqual(["local_file_store", "fast-load"], store_manifest["features"]["default"])
        self.assertEqual(["rocketmq-store-local/fast-load"], store_manifest["features"]["fast-load"])
        self.assertEqual(["rocketmq-store-local/safe-load"], store_manifest["features"]["safe-load"])
        self.assertEqual(["rocketmq-store-local/io_uring"], store_manifest["features"]["io_uring"])
        self.assertIn("rocketmq-store-local", store_manifest["dependencies"])
        self.assertNotIn("tokio-uring", store_manifest.get("dependencies", {}))
        self.assertNotIn("tokio-uring", store_manifest.get("target", {}).get("cfg(target_os = \"linux\")", {}).get("dependencies", {}))

    def test_local_manifest_and_sources_have_no_forbidden_owner_edges(self) -> None:
        self.assert_local_crate_exists()
        manifest = tomllib.loads((LOCAL_CRATE / "Cargo.toml").read_text(encoding="utf-8"))
        dependencies = {
            alias
            for table in dependency_tables(manifest)
            for alias in table
        }
        self.assertTrue(FORBIDDEN_DEPENDENCIES.isdisjoint(dependencies), dependencies)

        findings: list[str] = []
        for path in sorted((LOCAL_CRATE / "src").rglob("*.rs")):
            source = path.read_text(encoding="utf-8")
            source = re.sub(r"/\*.*?\*/", " ", source, flags=re.DOTALL)
            source = re.sub(r"//[^\r\n]*", " ", source)
            source = re.sub(r'(?s)(?:br|r|b)?(?:#+)?".*?"(?:#+)?', " ", source)
            for token in FORBIDDEN_SOURCE_TOKENS:
                if re.search(rf"\b{re.escape(token)}\b", source):
                    findings.append(f"{path.relative_to(ROOT)}: {token}")
        self.assertEqual([], findings)

    def test_six_leaf_files_have_one_canonical_definition_and_facade_reexports(self) -> None:
        self.assert_local_crate_exists()
        canonical_dir = LOCAL_CRATE / "src" / "mapped_file"
        facade_dir = STORE_CRATE / "src" / "log_file" / "mapped_file"
        self.assertEqual(
            LEAF_FILES | {"file.rs", "kernel.rs", "mapping.rs"},
            {path.name for path in canonical_dir.glob("*.rs")},
        )
        self.assertTrue(all(not (facade_dir / name).exists() for name in LEAF_FILES))

        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        for item, (item_kind, expected_file) in CANONICAL_ITEMS.items():
            definitions = canonical_definition_paths(rust_sources, item, item_kind)
            self.assertEqual([canonical_dir / expected_file], definitions, item)

        facade = (STORE_CRATE / "src" / "log_file" / "mapped_file.rs").read_text(encoding="utf-8")
        reexports = active_facade_reexports(facade)
        self.assertTrue(FACADE_ROOT_ITEMS.issubset(reexports), FACADE_ROOT_ITEMS - reexports)
        self.assertIn("io_uring_impl", reexports)

    def test_mapped_file_kernel_has_one_owner_exact_facades_and_no_store_state_copy(self) -> None:
        canonical_file = LOCAL_CRATE / "src" / "mapped_file" / "kernel.rs"
        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        for item, item_kind in KERNEL_ITEMS.items():
            self.assertEqual(
                [(canonical_file, item_kind)],
                kernel_item_owner_occurrences(rust_sources, item),
                item,
            )

        facade_dir = STORE_CRATE / "src" / "log_file" / "mapped_file"
        reference_facade = (facade_dir / "reference_resource.rs").read_text(encoding="utf-8")
        counter_facade = (facade_dir / "reference_resource_counter.rs").read_text(encoding="utf-8")
        self.assertEqual(
            ["pub(crate) use ReferenceResource"],
            active_kernel_use_statements(reference_facade),
        )
        self.assertEqual(
            [
                "pub(crate) use ReferenceResourceBase",
                "pub(crate) use ReferenceResourceCounter",
            ],
            active_kernel_use_statements(counter_facade),
        )
        store_sources = {
            path: path.read_text(encoding="utf-8")
            for path in STORE_CRATE.glob("src/**/*.rs")
        }
        self.assertEqual(
            [
                (
                    facade_dir / "reference_resource.rs",
                    "pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResource",
                ),
                (
                    facade_dir / "reference_resource_counter.rs",
                    "pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResourceBase",
                ),
                (
                    facade_dir / "reference_resource_counter.rs",
                    "pub(crate) use rocketmq_store_local::mapped_file::kernel::ReferenceResourceCounter",
                ),
            ],
            kernel_facade_boundary_uses(store_sources),
        )

        default_mapped_file = (
            facade_dir / "default_mapped_file_impl.rs"
        ).read_text(encoding="utf-8")
        self.assertEqual(
            [],
            default_mapped_file_progress_violations(default_mapped_file),
        )

    def test_mapped_file_storage_has_one_owner_exact_platform_reexports_and_no_store_state_copy(self) -> None:
        canonical_file = LOCAL_CRATE / "src" / "mapped_file" / "file.rs"
        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        for item, item_kind in FILE_ITEMS.items():
            self.assertEqual(
                [(canonical_file, item_kind)],
                file_item_owner_occurrences(rust_sources, item),
                item,
            )
        canonical_source = canonical_file.read_text(encoding="utf-8")
        self.assertEqual([], mapped_file_storage_owner_violations(canonical_source))
        self.assertEqual(
            [canonical_file],
            canonical_definition_paths(
                {canonical_file: canonical_source},
                "parse_file_from_offset",
                "fn",
            ),
        )
        self.assertEqual(
            [canonical_file],
            canonical_definition_paths(
                {canonical_file: canonical_source},
                "try_parse_file_from_offset",
                "fn",
            ),
        )

        platform = (STORE_CRATE / "src" / "platform.rs").read_text(encoding="utf-8")
        self.assertEqual(
            {f"pub use {item}" for item in FILE_PLATFORM_REEXPORTS},
            set(active_file_use_statements(platform)),
        )

        default_mapped_file = (
            STORE_CRATE / "src" / "log_file" / "mapped_file" / "default_mapped_file_impl.rs"
        ).read_text(encoding="utf-8")
        self.assertEqual([], default_mapped_file_storage_violations(default_mapped_file))
        active_default = active_rust_source(default_mapped_file)
        self.assertRegex(
            active_default,
            r"pub\s+fn\s+parse_file_from_offset\s*\(file_name:\s*&Path\)\s*->\s*u64\s*\{\s*"
            r"rocketmq_store_local::mapped_file::file::parse_file_from_offset\(file_name\)\s*\}",
        )
        self.assertRegex(
            active_default,
            r"pub\s+fn\s+try_parse_file_from_offset\s*\(file_name:\s*&Path\)\s*->\s*io::Result<u64>\s*\{\s*"
            r"rocketmq_store_local::mapped_file::file::try_parse_file_from_offset\(file_name\)\s*\}",
        )

    def test_mapped_file_mapping_has_one_local_owner_and_exact_store_composition(self) -> None:
        canonical_file = LOCAL_CRATE / "src" / "mapped_file" / "mapping.rs"
        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        for item, item_kind in MAPPING_ITEMS.items():
            self.assertEqual(
                [(canonical_file, item_kind)],
                file_item_owner_occurrences(rust_sources, item),
                item,
            )

        canonical_source = canonical_file.read_text(encoding="utf-8")
        self.assertEqual([], mapped_file_mapping_owner_violations(canonical_source))
        self.assertNotRegex(active_rust_source(canonical_source), r"\bArcMut\b")

        default_mapped_file = (
            STORE_CRATE / "src" / "log_file" / "mapped_file" / "default_mapped_file_impl.rs"
        ).read_text(encoding="utf-8")
        self.assertEqual([], default_mapped_file_mapping_violations(default_mapped_file))
        self.assertEqual([], legacy_mapping_getter_signature_violations(default_mapped_file))
        self.assertEqual(
            ["pub use LazyMmapStats", "use MappedFileMapping"],
            active_mapping_use_statements(default_mapped_file),
        )

    def test_commit_log_planning_items_have_one_canonical_definition_and_exact_facade_reexports(self) -> None:
        self.assert_local_crate_exists()
        canonical_dir = LOCAL_CRATE / "src" / "commit_log"
        self.assertEqual(
            {"load.rs", "recovery.rs", "record.rs", "record_parser.rs"},
            {path.name for path in canonical_dir.glob("*.rs")},
        )

        log_file_root = (STORE_CRATE / "src" / "log_file.rs").read_text(encoding="utf-8")
        self.assertIn("pub(crate) mod commit_log_loader;", active_rust_source(log_file_root))
        self.assertIn("pub mod commit_log_recovery;", active_rust_source(log_file_root))

        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        storage_boundary_sources = {
            path: source
            for path, source in rust_sources.items()
            if LOCAL_CRATE in path.parents or STORE_CRATE in path.parents
        }
        for item, (item_kind, expected_file) in COMMIT_LOG_CANONICAL_ITEMS.items():
            sources = storage_boundary_sources if expected_file == "record.rs" else rust_sources
            definitions = canonical_definition_paths(sources, item, item_kind)
            self.assertEqual([canonical_dir / expected_file], definitions, item)

        parser_file = canonical_dir / "record_parser.rs"
        parser_source = parser_file.read_text(encoding="utf-8")
        for item, item_kind in COMMIT_LOG_RECORD_PARSER_ITEMS.items():
            self.assertEqual(
                [(parser_file, item_kind)],
                commit_log_record_owner_occurrences(storage_boundary_sources, item),
                item,
            )
        self.assertEqual([], commit_log_record_parser_boundary_violations(parser_source))

        recovery_file = canonical_dir / "recovery.rs"
        recovery_source = recovery_file.read_text(encoding="utf-8")
        for item, item_kind in NORMAL_RECOVERY_ITEMS.items():
            self.assertEqual(
                [(recovery_file, item_kind)],
                commit_log_record_owner_occurrences(storage_boundary_sources, item),
                item,
            )
        self.assertEqual([], normal_recovery_state_boundary_violations(recovery_source))

        self.assertFalse((STORE_CRATE / "src" / "log_file" / "commit_log_record_parser.rs").exists())
        commit_log = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        self.assertEqual(
            [],
            store_record_parser_wrapper_violations(log_file_root, commit_log),
        )

        facade_dir = STORE_CRATE / "src" / "log_file"
        for facade_file, expected_items in COMMIT_LOG_FACADE_ITEMS.items():
            facade = (facade_dir / facade_file).read_text(encoding="utf-8")
            self.assertEqual(expected_items, active_commit_log_facade_reexports(facade), facade_file)

    def test_commit_log_record_has_one_owner_exact_facades_and_wrapper_only_legacy_iterator(self) -> None:
        canonical_file = LOCAL_CRATE / "src" / "commit_log" / "record.rs"
        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for crate in (LOCAL_CRATE, STORE_CRATE)
            for path in crate.glob("src/**/*.rs")
        }
        for item in [
            "MESSAGE_MAGIC_CODE",
            "BLANK_MAGIC_CODE",
            "is_blank_message",
            "CommitLogFrameSource",
            "CommitLogFrameCursor",
        ]:
            self.assertEqual(
                [(canonical_file, COMMIT_LOG_CANONICAL_ITEMS[item][0])],
                commit_log_record_owner_occurrences(rust_sources, item),
                item,
            )

        canonical_source = canonical_file.read_text(encoding="utf-8")
        self.assertEqual([], commit_log_record_boundary_violations(canonical_source))
        self.assertIn("MESSAGE_MAGIC_CODE_V2", active_rust_source(canonical_source))

        commit_log = (STORE_CRATE / "src" / "log_file" / "commit_log.rs").read_text(encoding="utf-8")
        recovery = (STORE_CRATE / "src" / "log_file" / "commit_log_recovery.rs").read_text(encoding="utf-8")
        self.assertEqual([], store_record_facade_violations(commit_log, recovery))

    def test_commit_log_file_validation_has_one_local_owner_and_store_adapter_only(self) -> None:
        canonical_file = LOCAL_CRATE / "src" / "commit_log" / "load.rs"
        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for crate in (LOCAL_CRATE, STORE_CRATE)
            for path in crate.glob("src/**/*.rs")
        }
        for item, item_kind in COMMIT_LOG_LOAD_OWNER_ITEMS.items():
            self.assertEqual(
                [(canonical_file, item_kind)],
                file_item_owner_occurrences(rust_sources, item),
                item,
            )

        canonical_source = canonical_file.read_text(encoding="utf-8")
        self.assertEqual([], commit_log_file_validation_owner_violations(canonical_source))

        loader_source = (
            STORE_CRATE / "src" / "log_file" / "commit_log_loader.rs"
        ).read_text(encoding="utf-8")
        self.assertEqual([], store_commit_log_file_validation_violations(loader_source))
        self.assertNotIn(
            "CommitLogFileMetadata",
            active_commit_log_facade_reexports(loader_source),
        )
        self.assertNotIn(
            "CommitLogFileLoadDecision",
            active_commit_log_facade_reexports(loader_source),
        )
        self.assertNotIn(
            "CommitLogFileValidationError",
            active_commit_log_facade_reexports(loader_source),
        )

    def test_commit_log_file_validation_contract_rejects_owner_mutations(self) -> None:
        source = (LOCAL_CRATE / "src" / "commit_log" / "load.rs").read_text(encoding="utf-8")
        self.assertEqual([], commit_log_file_validation_owner_violations(source))
        mutations = [
            source.replace("metadata.size == 0 && is_last", "metadata.size == 0 && !is_last", 1),
            source.replace(
                "CommitLogFileLoadDecision::RemoveEmptyLast",
                "CommitLogFileLoadDecision::Load",
                1,
            ),
            source.replace("use std::path::PathBuf;", "use std::{fs, path::PathBuf};", 1),
            source.replace("please check it manually", "check it later", 1),
            source.replace("pub actual: u64,", "actual: u64,", 1),
        ]
        for mutation_index, mutation in enumerate(mutations):
            with self.subTest(mutation_index=mutation_index):
                self.assertNotEqual(source, mutation)
                self.assertNotEqual([], commit_log_file_validation_owner_violations(mutation))

    def test_store_commit_log_file_validation_contract_rejects_review_mutations(self) -> None:
        path = STORE_CRATE / "src" / "log_file" / "commit_log_loader.rs"
        source = path.read_text(encoding="utf-8")
        self.assertEqual([], store_commit_log_file_validation_violations(source))
        mutations = [
            source.replace("idx == last_file_idx", "idx != last_file_idx", 1),
            source.replace(
                "match validate_commit_log_file(&metadata, expected_size, idx == last_file_idx)",
                "match Ok(CommitLogFileLoadDecision::Load)",
                1,
            ),
            source.replace("let size = file_metadata.len();", "let size = file_metadata.len(); if size == 0 {}", 1),
            source.replace(
                "Ok(CommitLogFileLoadDecision::Load) => Ok(Some(metadata))",
                "Ok(CommitLogFileLoadDecision::Load) => Ok(None)",
                1,
            ),
            source.replace(
                "if let Err(error) = fs::remove_file(path)",
                "if let Err(error) = fs::remove_file(path).map_err(io::Error::other)?",
                1,
            ),
            source.replace("io::ErrorKind::InvalidData", "io::ErrorKind::Other", 1),
            source.replace(
                "io::ErrorKind::InvalidData, error",
                'io::ErrorKind::InvalidData, format!("validation: {error}")',
                1,
            ),
            source.replace(
                "match validate_commit_log_file(&metadata, expected_size, idx == last_file_idx)",
                "match validation",
                1,
            ).replace(
                "let results: Result<Vec<_>, _> = paths",
                "let validation = validate_commit_log_file(&metadata, expected_size, false);\n"
                "        let results: Result<Vec<_>, _> = paths",
                1,
            ),
            source.replace(".par_iter()\n            .enumerate()", ".par_iter()", 1),
            source.replace(".into_iter().flatten().collect()", ".into_iter().filter_map(|item| item).collect()", 1),
            source.replace(
                "use rocketmq_store_local::commit_log::load::CommitLogFileMetadata;",
                "use rocketmq_store_local::commit_log::load::{CommitLogFileMetadata};",
                1,
            ),
            source.replace(
                "use rocketmq_store_local::commit_log::load::CommitLogFileMetadata;",
                "use rocketmq_store_local::commit_log::load::CommitLogFileMetadata as Metadata;",
                1,
            ),
            source.replace(
                "use rocketmq_store_local::commit_log::load::CommitLogFileMetadata;",
                "use rocketmq_store_local::commit_log::load::*;",
                1,
            ),
            source.replace(
                "pub fn new(store_path: String, mapped_file_size: u64, enable_parallel: bool) -> Self",
                "pub fn new(store_path: String, mapped_file_size: u64, enable_parallel: usize) -> Self",
                1,
            ),
            source.replace("Ok((mapped_files, stats))", "stats.files_removed += 1; Ok((mapped_files, stats))", 1),
        ]
        for mutation_index, mutation in enumerate(mutations):
            with self.subTest(mutation_index=mutation_index):
                self.assertNotEqual(source, mutation)
                self.assertNotEqual([], store_commit_log_file_validation_violations(mutation))

    def test_commit_log_summary_logging_preserves_legacy_targets(self) -> None:
        expected_targets = {
            "load.rs": ["rocketmq_store::log_file::commit_log_loader"],
            "recovery.rs": ["rocketmq_store::log_file::commit_log_recovery"],
        }
        canonical_dir = LOCAL_CRATE / "src" / "commit_log"
        for source_file, expected in expected_targets.items():
            source = (canonical_dir / source_file).read_text(encoding="utf-8")
            self.assertEqual(expected, active_tracing_info_targets(source), source_file)


if __name__ == "__main__":
    unittest.main()
