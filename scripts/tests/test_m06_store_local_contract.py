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
            r"pub\s+use\s+rocketmq_store_local::commit_log::(load|recovery)::([A-Za-z_][A-Za-z0-9_]*)\s*;",
            source,
        )
    }


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
        self.assertEqual({"load.rs", "recovery.rs"}, {path.name for path in canonical_dir.glob("*.rs")})

        log_file_root = (STORE_CRATE / "src" / "log_file.rs").read_text(encoding="utf-8")
        self.assertIn("pub(crate) mod commit_log_loader;", active_rust_source(log_file_root))
        self.assertIn("pub mod commit_log_recovery;", active_rust_source(log_file_root))

        rust_sources = {
            path: path.read_text(encoding="utf-8")
            for path in ROOT.glob("rocketmq-*/src/**/*.rs")
        }
        for item, (item_kind, expected_file) in COMMIT_LOG_CANONICAL_ITEMS.items():
            definitions = canonical_definition_paths(rust_sources, item, item_kind)
            self.assertEqual([canonical_dir / expected_file], definitions, item)

        facade_dir = STORE_CRATE / "src" / "log_file"
        for facade_file, expected_items in COMMIT_LOG_FACADE_ITEMS.items():
            facade = (facade_dir / facade_file).read_text(encoding="utf-8")
            self.assertEqual(expected_items, active_commit_log_facade_reexports(facade), facade_file)

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
