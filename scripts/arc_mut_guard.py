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

"""Freeze unsafe shared-mutation entry points while the ownership migration runs.

The scanner is deliberately dependency-free so the same command runs in Windows and
Unix CI.  It tokenizes Rust (instead of grepping text), follows local ``use`` and
``type`` aliases to a fixed point, and fails closed when lexical input is malformed.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import re
import sys
import tempfile
import tomllib
from collections import Counter
from pathlib import Path
from typing import NamedTuple


CANONICAL = {"ArcMut", "WeakArcMut", "SyncUnsafeCellWrapper"}
REQUIRED_FIELDS = {
    "identity", "path", "symbol", "kind", "category", "owner", "reason",
    "remove_by", "adr", "occurrences",
}
MILESTONES = {f"M{i:02d}": i for i in range(1, 13)}
APPROVED_ADRS = {"ADR-002", "ADR-013"}
PLACEHOLDERS = {"unassigned", "requires_triage", "tbd", "todo", "unknown", "none", "n/a", "never", "permanent"}
RAW_STRING = re.compile(r"(?:br|r)(#{0,255})\"")


class LexError(ValueError):
    pass


class BaselineError(ValueError):
    pass


class Token(NamedTuple):
    value: str
    offset: int


class Finding(NamedTuple):
    identity: str
    path: str
    symbol: str
    kind: str
    category: str
    occurrences: tuple[dict, ...]


class Issue(NamedTuple):
    code: str
    detail: str


class ModuleInfo(NamedTuple):
    module: tuple[str, ...]
    crate_root: tuple[str, ...]


class UseDecl(NamedTuple):
    path: tuple[str, ...]
    alias: str
    visibility: str
    start: int
    end: int
    leaf_index: int
    alias_index: int | None


class RepositoryContext(NamedTuple):
    unsafe_paths: set[tuple[str, ...]]
    use_kinds_by_file: dict[str, dict[int, str]]
    alias_indices_by_file: dict[str, set[int]]
    modules_by_file: dict[str, tuple[ModuleInfo, ...]]
    crate_aliases: dict[str, tuple[str, ...]]
    declarations: set[tuple[str, ...]]
    visibility: dict[tuple[str, ...], str]
    shared_wrappers_by_file: dict[str, dict[int, str]]


def _skip_quoted(source: str, start: int, quote: str) -> int:
    i = start + 1
    while i < len(source):
        if source[i] == "\\":
            i += 2
        elif source[i] == quote:
            return i + 1
        else:
            i += 1
    raise LexError(f"unterminated {quote} literal at byte {start}")


def _skip_character(source: str, start: int) -> int | None:
    """Return the end of a character literal, or None when this is a lifetime."""
    n = len(source)
    if start + 1 >= n:
        raise LexError(f"unterminated character literal at byte {start}")
    i = start + 1
    if source[i] == "\\":
        i += 1
        if i >= n:
            raise LexError(f"unterminated character escape at byte {start}")
        if source[i] == "x":
            if i + 2 >= n or not all(ch in "0123456789abcdefABCDEF" for ch in source[i + 1:i + 3]):
                raise LexError(f"invalid hex character escape at byte {start}")
            i += 3
        elif source[i] == "u":
            if i + 1 >= n or source[i + 1] != "{":
                raise LexError(f"invalid unicode character escape at byte {start}")
            closing = source.find("}", i + 2)
            digits = source[i + 2:closing] if closing >= 0 else ""
            if closing < 0 or not 1 <= len(digits) <= 6 or not all(ch in "0123456789abcdefABCDEF" for ch in digits):
                raise LexError(f"invalid unicode character escape at byte {start}")
            i = closing + 1
        elif source[i] in "nrt0\\'\"":
            i += 1
        else:
            raise LexError(f"invalid character escape at byte {start}")
        if i >= n or source[i] != "'":
            raise LexError(f"unterminated character literal at byte {start}")
        return i + 1
    if i + 1 < n and source[i + 1] == "'":
        return i + 2
    if source[i] in "\r\n'":
        raise LexError(f"invalid character literal at byte {start}")
    # Apostrophe + identifier is a lifetime unless expression punctuation makes
    # it an unmistakably unterminated character literal (for example `= 'x;`).
    if source[i] == "_" or source[i].isalpha():
        j = i + 1
        while j < n and (source[j] == "_" or source[j].isalnum()):
            j += 1
        previous = start - 1
        while previous >= 0 and source[previous].isspace():
            previous -= 1
        if previous >= 0 and source[previous] == "=":
            raise LexError(f"unterminated character literal at byte {start}")
        return None
    raise LexError(f"invalid character literal at byte {start}")


def tokenize(source: str) -> list[Token]:
    """Return Rust identifiers/punctuation, excluding comments and literals."""
    result: list[Token] = []
    i = 0
    n = len(source)
    while i < n:
        if source.startswith("//", i):
            end = source.find("\n", i + 2)
            i = n if end < 0 else end + 1
            continue
        if source.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if source.startswith("/*", j):
                    depth += 1; j += 2
                elif source.startswith("*/", j):
                    depth -= 1; j += 2
                else:
                    j += 1
            if depth:
                raise LexError(f"unterminated block comment at byte {i}")
            i = j
            continue
        # raw strings: r"", r#""#, br#""#
        raw = RAW_STRING.match(source, i)
        if raw:
            hashes = raw.group(1)
            end_marker = '"' + hashes
            content_start = raw.end()
            end = source.find(end_marker, content_start)
            if end < 0:
                raise LexError(f"unterminated raw string at byte {i}")
            i = end + len(end_marker)
            continue
        if source.startswith('b"', i):
            i = _skip_quoted(source, i + 1, '"')
            continue
        if source[i] == '"':
            i = _skip_quoted(source, i, '"')
            continue
        if source.startswith("b'", i):
            end = _skip_character(source, i + 1)
            if end is None:
                raise LexError(f"byte lifetime is invalid at byte {i}")
            i = end
            continue
        if source[i] == "'":
            end = _skip_character(source, i)
            if end is not None:
                i = end
                continue
            result.append(Token("'", i)); i += 1
            continue
        ch = source[i]
        if ch.isspace():
            i += 1
        elif ch == "_" or ch.isalpha():
            j = i + 1
            while j < n and (source[j] == "_" or source[j].isalnum()):
                j += 1
            result.append(Token(source[i:j], i)); i = j
        elif source.startswith("::", i) or source.startswith("->", i) or source.startswith("=>", i):
            result.append(Token(source[i:i + 2], i)); i += 2
        else:
            result.append(Token(ch, i)); i += 1
    return result


def _file_category(path: str) -> str:
    normalized = path.replace("\\", "/").lower()
    if normalized in {"rocketmq/src/arc_mut.rs", "rocketmq/src/lib.rs"}:
        return "compatibility"
    parts = normalized.split("/")
    if any(part in {"tests", "benches", "examples"} for part in parts) or normalized.endswith("_test.rs"):
        return "test"
    return "production"


def _cfg_test_ranges(values: list[str]) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for i in range(len(values) - 5):
        if values[i:i + 6] != ["#", "[", "cfg", "(", "test", ")"]:
            continue
        try:
            opening = values.index("{", i + 6)
        except ValueError:
            continue
        depth = 0
        for end in range(opening, len(values)):
            if values[end] == "{": depth += 1
            elif values[end] == "}":
                depth -= 1
                if depth == 0:
                    ranges.append((i, end)); break
    return ranges


def _is_identifier(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value))


def _parse_use_declarations(values: list[str], use_index: int, visibility: str) -> list[UseDecl]:
    end = use_index + 1
    while end < len(values) and values[end] != ";":
        end += 1
    declarations: list[UseDecl] = []

    def parse_tree(position: int, prefix: tuple[str, ...]) -> int:
        if position < end and values[position] == "*":
            declarations.append(UseDecl(prefix, "*", visibility, use_index, end, position, None))
            return position + 1
        segments: list[str] = []
        indices: list[int] = []
        while position < end and _is_identifier(values[position]):
            segments.append(values[position]); indices.append(position); position += 1
            if position < end and values[position] == "::":
                position += 1
                if position < end and values[position] == "{":
                    return parse_group(position + 1, prefix + tuple(segments))
                if position < end and values[position] == "*":
                    declarations.append(UseDecl(prefix + tuple(segments), "*", visibility, use_index, end, position, None))
                    return position + 1
                continue
            break
        if not segments:
            return position + 1
        full = prefix + tuple(segments)
        if segments[0] == "self":
            full = prefix + tuple(segments[1:])
        alias_index: int | None = None
        alias = full[-1] if full else prefix[-1]
        if position + 1 < end and values[position] == "as" and _is_identifier(values[position + 1]):
            alias_index = position + 1; alias = values[alias_index]; position += 2
        declarations.append(UseDecl(full, alias, visibility, use_index, end, indices[-1], alias_index))
        return position

    def parse_group(position: int, prefix: tuple[str, ...]) -> int:
        while position < end and values[position] != "}":
            if values[position] == ",":
                position += 1; continue
            position = parse_tree(position, prefix)
        return position + 1 if position < end else position

    parse_tree(use_index + 1, ())
    return declarations


def _visibility_before(values: list[str], index: int) -> str:
    position = index - 1
    while position >= 0 and values[position] not in {";", "{", "}"}:
        if values[position] == "pub":
            suffix = values[position + 1:index]
            if suffix[:3] == ["(", "crate", ")"]: return "crate"
            if suffix[:3] == ["(", "super", ")"]: return "super"
            return "pub"
        position -= 1
    return "private"


def _pruned_walk(root: Path):
    for directory, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(
            d for d in dirnames
            if d not in {"target", ".git", "node_modules"}
            and not d.startswith("target-")
            and not d.startswith(".")
        )
        yield Path(directory), filenames


def _crate_layout(root: Path) -> tuple[dict[Path, tuple[str, ...]], dict[str, tuple[str, ...]]]:
    manifests: dict[Path, tuple[str, ...]] = {}
    aliases: dict[str, tuple[str, ...]] = {}
    for directory, filenames in _pruned_walk(root):
        if "Cargo.toml" not in filenames:
            continue
        try:
            manifest = tomllib.loads((directory / "Cargo.toml").read_text(encoding="utf-8"))
        except (OSError, UnicodeError, tomllib.TOMLDecodeError):
            continue
        package = manifest.get("package")
        if not isinstance(package, dict) or not isinstance(package.get("name"), str):
            continue
        relative = directory.relative_to(root).as_posix()
        crate_root = ("@crate", relative)
        manifests[directory.resolve()] = crate_root
        names = {package["name"].replace("-", "_")}
        library = manifest.get("lib")
        if isinstance(library, dict) and isinstance(library.get("name"), str):
            names.add(library["name"])
        for name in names:
            aliases[name] = crate_root
    if not manifests:
        manifests[root.resolve()] = ("@crate", ".")
    return manifests, aliases


def _module_info(path: Path, root: Path, manifests: dict[Path, tuple[str, ...]]) -> ModuleInfo:
    resolved = path.resolve()
    candidates = [(directory, crate_root) for directory, crate_root in manifests.items() if resolved.is_relative_to(directory)]
    directory, crate_root = max(candidates, key=lambda item: len(item[0].parts)) if candidates else (root, ("@crate", "."))
    relative = path.relative_to(directory)
    parts = list(relative.parts)
    if "src" in parts:
        src_index = parts.index("src")
        if src_index > 0 and directory == root:
            crate_root = ("@crate", Path(*parts[:src_index]).as_posix())
        parts = parts[src_index + 1:]
    stem = Path(parts[-1]).stem if parts else "lib"
    module_parts = parts[:-1]
    if stem not in {"lib", "main", "mod"}:
        module_parts.append(stem)
    return ModuleInfo(crate_root + tuple(module_parts), crate_root)


def _inline_module_infos(values: list[str], base: ModuleInfo) -> tuple[ModuleInfo, ...]:
    infos = [base] * len(values)
    for index, value in enumerate(values):
        if value != "mod" or index + 1 >= len(values) or not _is_identifier(values[index + 1]):
            continue
        opening = index + 2
        while opening < len(values) and values[opening] not in {"{", ";"}: opening += 1
        if opening >= len(values) or values[opening] != "{": continue
        closing = _matching_brace(values, opening)
        if closing is None: continue
        parent = infos[index]
        nested = ModuleInfo(parent.module + (values[index + 1],), parent.crate_root)
        for position in range(opening + 1, closing): infos[position] = nested
    return tuple(infos)


def _is_prefix(prefix: tuple[str, ...], value: tuple[str, ...]) -> bool:
    return len(value) >= len(prefix) and value[:len(prefix)] == prefix


def _can_access(
    symbol: tuple[str, ...],
    consumer: ModuleInfo,
    visibility: dict[tuple[str, ...], str],
) -> bool:
    rule = visibility.get(symbol, "pub")
    defining_module = symbol[:-1]
    if rule == "pub": return True
    if rule == "crate": return symbol[:2] == consumer.crate_root
    if rule == "super":
        scope = defining_module[:-1] if len(defining_module) > len(consumer.crate_root) else defining_module
        return _is_prefix(scope, consumer.module)
    return _is_prefix(defining_module, consumer.module)


def _lookup_simple(
    name: str,
    info: ModuleInfo,
    declarations: set[tuple[str, ...]],
) -> tuple[str, ...] | None:
    module = info.module
    while len(module) >= len(info.crate_root):
        candidate = module + (name,)
        if candidate in declarations: return candidate
        if len(module) == len(info.crate_root): break
        module = module[:-1]
    return None


def _resolve_consumer_path(
    path: tuple[str, ...],
    info: ModuleInfo,
    crate_aliases: dict[str, tuple[str, ...]],
    declarations: set[tuple[str, ...]],
) -> tuple[str, ...]:
    if not path or path[0] in {"crate", "self", "super"} or path[0] in crate_aliases:
        return _resolve_path(path, info, crate_aliases)
    module = info.module
    while len(module) >= len(info.crate_root):
        candidate = module + path
        if candidate in declarations: return candidate
        if len(module) == len(info.crate_root): break
        module = module[:-1]
    return info.crate_root + path


def _resolve_path(path: tuple[str, ...], info: ModuleInfo, crate_aliases: dict[str, tuple[str, ...]]) -> tuple[str, ...]:
    if not path:
        return info.module
    if path[0] == "crate":
        return info.crate_root + path[1:]
    if path[0] == "self":
        return info.module + path[1:]
    if path[0] == "super":
        module = list(info.module)
        position = 0
        while position < len(path) and path[position] == "super":
            if len(module) > len(info.crate_root): module.pop()
            position += 1
        return tuple(module) + path[position:]
    if path[0] in crate_aliases:
        return crate_aliases[path[0]] + path[1:]
    return info.crate_root + path


def _path_at(values: list[str], index: int) -> tuple[str, ...]:
    start = index
    while start >= 2 and values[start - 1] == "::" and _is_identifier(values[start - 2]):
        start -= 2
    return tuple(values[position] for position in range(start, index + 1, 2))


def _repository_context(root: Path, paths: list[Path]) -> RepositoryContext:
    manifests, crate_aliases = _crate_layout(root)
    units: dict[str, tuple[list[Token], list[str], tuple[ModuleInfo, ...]]] = {}
    uses: dict[str, list[UseDecl]] = {}
    types: dict[str, list[tuple[int, int, str, str]]] = {}
    edges: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
    declarations: set[tuple[str, ...]] = set()
    visibility: dict[tuple[str, ...], str] = {}
    symbols_by_module: dict[tuple[str, ...], set[tuple[str, ...]]] = {}
    canonical_nodes = {("@canonical", name) for name in CANONICAL}
    visibility_rank = {"private": 0, "super": 1, "crate": 2, "pub": 3}

    def declare(symbol: tuple[str, ...], rule: str) -> None:
        declarations.add(symbol)
        symbols_by_module.setdefault(symbol[:-1], set()).add(symbol)
        previous = visibility.get(symbol)
        if previous is None or visibility_rank[rule] > visibility_rank[previous]: visibility[symbol] = rule

    def targets_for(path_value: tuple[str, ...], info: ModuleInfo) -> set[tuple[str, ...]]:
        if path_value and path_value[-1] in CANONICAL:
            return {("@canonical", path_value[-1])}
        target = _resolve_consumer_path(path_value, info, crate_aliases, declarations)
        return {target} if target not in declarations or _can_access(target, info, visibility) else set()

    # Pass 1 establishes lexical modules and every declaration, including safe
    # declarations that shadow a hazardous ancestor.
    for path in paths:
        relative = path.relative_to(root).as_posix()
        tokens = tokenize(path.read_text(encoding="utf-8")); values = [token.value for token in tokens]
        base = _module_info(path, root, manifests); module_infos = _inline_module_infos(values, base)
        units[relative] = (tokens, values, module_infos)
        file_uses: list[UseDecl] = []
        for index, value in enumerate(values):
            if value != "use": continue
            file_uses.extend(_parse_use_declarations(values, index, _visibility_before(values, index)))
        uses[relative] = file_uses
        for declaration in file_uses:
            if declaration.alias != "*":
                info = module_infos[declaration.start]
                declare(info.module + (declaration.alias,), declaration.visibility)

        file_types: list[tuple[int, int, str, str]] = []
        for index, value in enumerate(values):
            if value != "type" or index + 1 >= len(values) or not _is_identifier(values[index + 1]):
                continue
            name = values[index + 1]
            end = index + 2
            while end < len(values) and values[end] != ";": end += 1
            try: equals = values.index("=", index + 2, end)
            except ValueError: continue
            rule = _visibility_before(values, index)
            declare(module_infos[index].module + (name,), rule)
            file_types.append((index, equals, name, rule))
        types[relative] = file_types
        for index, value in enumerate(values):
            if value in {"struct", "enum", "trait", "union"} and index + 1 < len(values) and _is_identifier(values[index + 1]):
                declare(module_infos[index].module + (values[index + 1],), _visibility_before(values, index))

    # Pass 2 resolves normal aliases against the complete declaration table.
    explicit_declarations = set(declarations)
    glob_declarations: list[tuple[str, UseDecl, ModuleInfo, tuple[str, ...]]] = []
    for relative, (_, values, module_infos) in units.items():
        for declaration in uses[relative]:
            info = module_infos[declaration.start]
            if declaration.alias == "*":
                source = _resolve_consumer_path(declaration.path, info, crate_aliases, declarations)
                glob_declarations.append((relative, declaration, info, source)); continue
            destination = info.module + (declaration.alias,)
            edges.setdefault(destination, set()).update(targets_for(declaration.path, info))
        for index, equals, name, _ in types[relative]:
            info = module_infos[index]; destination = info.module + (name,)
            end = equals + 1
            while end < len(values) and values[end] != ";": end += 1
            targets: set[tuple[str, ...]] = set()
            for target_index in range(equals + 1, end):
                if not _is_identifier(values[target_index]): continue
                if target_index + 1 < end and values[target_index + 1] == "::": continue
                targets.update(targets_for(_path_at(values, target_index), info))
            edges.setdefault(destination, set()).update(targets)

    # Complete the normal symbol graph first. Glob expansion is independent of
    # hazard status: this lets later wrapper evidence resolve through direct or
    # chained globs without weakening explicit-declaration shadowing.
    glob_imports: dict[tuple[str, int], set[str]] = {}
    graph_changed = True
    while graph_changed:
        graph_changed = False
        for relative, declaration, info, source in glob_declarations:
            imported = glob_imports.setdefault((relative, declaration.start), set())
            for target in tuple(symbols_by_module.get(source, ())):
                if not _can_access(target, info, visibility): continue
                name = target[-1]; destination = info.module + (name,)
                if destination in explicit_declarations: continue
                if destination not in declarations:
                    declare(destination, declaration.visibility); graph_changed = True
                targets = edges.setdefault(destination, set())
                if target not in targets:
                    targets.add(target); graph_changed = True
                imported.add(name)

    wrapper_candidates: dict[tuple[str, ...], tuple[str, str, int, bool, bool]] = {}
    unsafe_sync_targets: set[tuple[str, ...]] = set()
    safe_escape_targets: set[tuple[str, ...]] = set()
    for relative, (_, values, module_infos) in units.items():
        for node, (name, index, direct_cell, shared_field) in _cell_structure_candidates(values, module_infos).items():
            wrapper_candidates[node] = (relative, name, index, direct_cell, shared_field)
        sync_targets, escape_targets = _impl_wrapper_evidence(
            values, module_infos, crate_aliases, declarations
        )
        unsafe_sync_targets.update(sync_targets); safe_escape_targets.update(escape_targets)

    wrapper_nodes: set[tuple[str, ...]] = set()
    shared_wrappers_by_file: dict[str, dict[int, str]] = {}
    sync_closure = _edge_closure(unsafe_sync_targets, edges)
    escape_closure = _edge_closure(safe_escape_targets, edges)
    for node, (relative, name, index, direct_cell, shared_field) in wrapper_candidates.items():
        has_sync = node in sync_closure
        has_escape = node in escape_closure
        if (direct_cell and has_sync) or (shared_field and has_escape):
            wrapper_nodes.add(node)
            shared_wrappers_by_file.setdefault(relative, {})[index] = name

    unsafe = set(canonical_nodes) | wrapper_nodes
    changed = True
    while changed:
        changed = False
        for destination, targets in edges.items():
            if destination not in unsafe and targets.intersection(unsafe):
                unsafe.add(destination); changed = True

    use_kinds: dict[str, dict[int, str]] = {}
    alias_indices: dict[str, set[int]] = {}
    modules_by_file: dict[str, tuple[ModuleInfo, ...]] = {}
    for relative, (_, _, module_infos) in units.items():
        modules_by_file[relative] = module_infos
        kinds: dict[int, str] = {}; indices: set[int] = set()
        for declaration in uses[relative]:
            if declaration.alias == "*":
                info = module_infos[declaration.start]
                names = glob_imports.get((relative, declaration.start), set())
                if not any(info.module + (name,) in unsafe for name in names): continue
            else:
                info = module_infos[declaration.start]
                if info.module + (declaration.alias,) not in unsafe: continue
            kind = "reexport" if declaration.visibility != "private" else "import"
            kinds[declaration.leaf_index] = kind
            if declaration.alias_index is not None:
                kinds[declaration.alias_index] = kind; indices.add(declaration.alias_index)
        use_kinds[relative] = kinds
        alias_indices[relative] = indices
    return RepositoryContext(
        unsafe, use_kinds, alias_indices, modules_by_file, crate_aliases,
        declarations, visibility, shared_wrappers_by_file,
    )


def _entry_identity(path: str, symbol: str, kind: str, category: str) -> str:
    # Whitespace and physical line numbers never enter the identity.  Path and token
    # context intentionally do: moving or disguising debt becomes NEW + STALE.
    material = "\0".join([path.replace("\\", "/"), symbol, kind, category])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:24]


def _item(values: list[str], index: int) -> str:
    for i in range(index - 1, -1, -1):
        if values[i] in {"fn", "struct", "enum", "trait", "mod", "impl"}:
            tail = [v for v in values[i + 1:min(index + 1, i + 15)] if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v)]
            return f"{values[i]} {tail[0]}" if tail else values[i]
    return "<module>"


def _matching_pair(values: list[str], opening: int, left: str, right: str) -> int | None:
    depth = 0
    for index in range(opening, len(values)):
        if values[index] == left: depth += 1
        elif values[index] == right:
            depth -= 1
            if depth == 0: return index
    return None


def _matching_brace(values: list[str], opening: int) -> int | None:
    return _matching_pair(values, opening, "{", "}")


def _cell_structure_candidates(
    values: list[str],
    modules: tuple[ModuleInfo, ...],
) -> dict[tuple[str, ...], tuple[str, int, bool, bool]]:
    candidates: dict[tuple[str, ...], tuple[str, int, bool, bool]] = {}
    for index, value in enumerate(values):
        if value != "struct" or index + 1 >= len(values) or not _is_identifier(values[index + 1]): continue
        opening = index + 2; angle_depth = 0
        while opening < len(values):
            if values[opening] == "<": angle_depth += 1
            elif values[opening] == ">": angle_depth = max(0, angle_depth - 1)
            elif angle_depth == 0 and values[opening] in {"{", "(", ";"}: break
            opening += 1
        if opening >= len(values) or values[opening] == ";": continue
        left, right = ("{", "}") if values[opening] == "{" else ("(", ")")
        closing = _matching_pair(values, opening, left, right)
        if closing is None: continue
        body = values[opening + 1:closing]
        direct_cell = any(token in {"UnsafeCell", "SyncUnsafeCell"} for token in body)
        shared_field = False; field: list[str] = []; depth = 0
        for token in body + [","]:
            if token in {"<", "(", "["}: depth += 1
            elif token in {">", ")", "]"}: depth = max(0, depth - 1)
            if token == "," and depth == 0:
                has_cell = any(item in {"UnsafeCell", "SyncUnsafeCell"} for item in field)
                has_owner = "Arc" in field or any(item in CANONICAL for item in field)
                shared_field = shared_field or (has_cell and has_owner) or any(item in CANONICAL for item in field)
                field = []
            else: field.append(token)
        if direct_cell or shared_field:
            name = values[index + 1]
            candidates[modules[index].module + (name,)] = (name, index, direct_cell, shared_field)
    return candidates


def _impl_self_target(
    values: list[str],
    index: int,
    opening: int,
    info: ModuleInfo,
    crate_aliases: dict[str, tuple[str, ...]],
    declarations: set[tuple[str, ...]],
) -> tuple[str, ...] | None:
    header = values[index + 1:opening]
    if "for" in header:
        header = header[len(header) - 1 - header[::-1].index("for") + 1:]
    elif header and header[0] == "<":
        depth = 0; end = 0
        for end, token in enumerate(header):
            if token == "<": depth += 1
            elif token == ">":
                depth -= 1
                if depth == 0: break
        header = header[end + 1:]
    path_tokens: list[str] = []
    for token in header:
        if token in {"<", "where", "(", "["}: break
        if _is_identifier(token) or token == "::": path_tokens.append(token)
    segments = tuple(token for token in path_tokens if token != "::")
    if not segments: return None
    return _resolve_consumer_path(segments, info, crate_aliases, declarations)


def _impl_wrapper_evidence(
    values: list[str],
    modules: tuple[ModuleInfo, ...],
    crate_aliases: dict[str, tuple[str, ...]],
    declarations: set[tuple[str, ...]],
) -> tuple[set[tuple[str, ...]], set[tuple[str, ...]]]:
    unsafe_sync: set[tuple[str, ...]] = set(); safe_escape: set[tuple[str, ...]] = set()
    for index, value in enumerate(values):
        if value != "impl": continue
        try: opening = values.index("{", index + 1)
        except ValueError: continue
        closing = _matching_brace(values, opening)
        if closing is None: continue
        target = _impl_self_target(values, index, opening, modules[index], crate_aliases, declarations)
        if target is None: continue
        header = values[max(0, index - 1):opening]
        if index > 0 and values[index - 1] == "unsafe" and "Sync" in header and "for" in header:
            unsafe_sync.add(target)
        body = values[opening + 1:closing]
        for fn_index, token in enumerate(body):
            if token != "fn": continue
            signature_end = fn_index
            while signature_end < len(body) and body[signature_end] not in {"{", ";"}: signature_end += 1
            signature = body[fn_index:signature_end]
            has_shared_receiver = any(signature[pos:pos + 2] == ["&", "self"] for pos in range(len(signature) - 1))
            if has_shared_receiver and "->" in signature:
                arrow = signature.index("->")
                if "&" in signature[arrow + 1:] and "mut" in signature[arrow + 1:]:
                    safe_escape.add(target); break
    return unsafe_sync, safe_escape


def _edge_closure(
    starts: set[tuple[str, ...]],
    edges: dict[tuple[str, ...], set[tuple[str, ...]]],
) -> set[tuple[str, ...]]:
    pending = list(starts); seen: set[tuple[str, ...]] = set()
    while pending:
        current = pending.pop()
        if current in seen: continue
        seen.add(current); pending.extend(edges.get(current, ()))
    return seen


def scan_file(path: Path, root: Path, repository: RepositoryContext | None = None) -> list[Finding]:
    relative = path.relative_to(root).as_posix()
    source = path.read_text(encoding="utf-8")
    tokens = tokenize(source)
    values = [t.value for t in tokens]
    use_kinds: dict[int, str] = {}
    alias_indices: set[int] = set()
    if repository is None:
        repository = _repository_context(root, [path])
    use_kinds = repository.use_kinds_by_file.get(relative, {})
    alias_indices = repository.alias_indices_by_file.get(relative, set())
    file_category = _file_category(relative)
    test_ranges = _cfg_test_ranges(values)
    line_starts = [0] + [match.end() for match in re.finditer("\n", source)]
    raw: list[tuple[str, str, list[str], int]] = []
    previous_boundary: list[int] = []
    boundary = 0
    for i, value in enumerate(values):
        if value in {";", "{", "}"}: boundary = i + 1
        previous_boundary.append(boundary)
    next_boundary = [len(values) - 1] * len(values)
    boundary = len(values) - 1
    for i in range(len(values) - 1, -1, -1):
        if values[i] in {";", "{", "}"}: boundary = i - 1
        next_boundary[i] = max(i, boundary)

    shared_unsafe_cell_types = repository.shared_wrappers_by_file.get(relative, {})
    for index, name in shared_unsafe_cell_types.items():
        raw.append((name, "shared_unsafe_cell_wrapper", values[max(0, index - 4):index + 20], index))

    for i, value in enumerate(values):
        start, end = previous_boundary[i], next_boundary[i]
        stmt = values[start:end + 1]
        if value == "mut_from_ref":
            kind = "mut_from_ref_definition" if "fn" in values[max(0, i - 4):i] else "mut_from_ref_call"
            raw.append((value, kind, values[max(0, i - 12):min(len(values), i + 13)], i))
            continue
        hazardous = value in CANONICAL or i in use_kinds
        if _is_identifier(value) and not hazardous:
            path_value = _path_at(values, i)
            module_infos = repository.modules_by_file.get(relative)
            if module_infos:
                info = module_infos[i]
                if len(path_value) == 1:
                    target = _lookup_simple(value, info, repository.declarations)
                else:
                    target = _resolve_consumer_path(path_value, info, repository.crate_aliases, repository.declarations)
                hazardous = bool(
                    target in repository.unsafe_paths
                    and target is not None
                    and _can_access(target, info, repository.visibility)
                )
        if not hazardous:
            continue
        if i in use_kinds:
            kind = use_kinds[i]
        elif i and values[i - 1] == "as" and i not in alias_indices:
            continue
        elif "impl" in stmt and "AsMut" in stmt and "for" in stmt:
            kind = "dangerous_as_mut_impl"
        elif "impl" in stmt and "DerefMut" in stmt and "for" in stmt:
            kind = "dangerous_deref_mut_impl"
        elif "use" in stmt:
            kind = "reexport" if "pub" in stmt[:stmt.index("use") + 1] else "import"
        elif "type" in stmt and "=" in stmt:
            kind = "alias"
        elif i + 2 < len(values) and values[i + 1] == "::" and values[i + 2] in {"new", "default", "from", "clone"}:
            kind = "constructor"
        else:
            kind = "type_reference"
        raw.append((value, kind, values[max(0, i - 12):min(len(values), i + 13)], i))

    grouped: dict[tuple[str, str, str], list[tuple[list[str], int]]] = {}
    for symbol, kind, context, index in raw:
        category = "test" if any(start <= index <= end for start, end in test_ranges) else file_category
        grouped.setdefault((symbol, kind, category), []).append((context, index))
    findings: list[Finding] = []
    for (symbol, kind, category), items in sorted(grouped.items()):
        identity = _entry_identity(relative, symbol, kind, category)
        duplicate_ordinals: Counter[str] = Counter()
        occurrences: list[dict] = []
        for context, index in items:
            fingerprint = hashlib.sha256(" ".join(context).encode("utf-8")).hexdigest()[:24]
            ordinal = duplicate_ordinals[fingerprint]; duplicate_ordinals[fingerprint] += 1
            occurrence_id = hashlib.sha256(f"{identity}\0{fingerprint}\0{ordinal}".encode("utf-8")).hexdigest()[:24]
            occurrences.append({
                "id": occurrence_id,
                "fingerprint": fingerprint,
                "item": _item(values, index),
                "line": bisect.bisect_right(line_starts, tokens[index].offset),
            })
        findings.append(Finding(identity, relative, symbol, kind, category, tuple(occurrences)))
    return findings


def scan_tree(root: Path) -> list[Finding]:
    findings: list[Finding] = []
    paths: list[Path] = []
    for directory, filenames in _pruned_walk(root):
        paths.extend(directory / name for name in filenames if name.endswith(".rs"))
    repository = _repository_context(root, sorted(paths))
    for path in sorted(paths):
        findings.extend(scan_file(path, root, repository))
    return findings


def validate_baseline(data: dict) -> None:
    if data.get("schema_version") != 1 or not isinstance(data.get("entries"), list):
        raise BaselineError("baseline schema_version must be 1 and entries must be an array")
    if data.get("current_milestone") not in MILESTONES:
        raise BaselineError("current_milestone must be one of M01 through M12")
    identities: set[str] = set()
    for number, entry in enumerate(data["entries"], 1):
        missing = REQUIRED_FIELDS - set(entry)
        if missing:
            raise BaselineError(f"entry {number} missing fields: {', '.join(sorted(missing))}")
        for field in REQUIRED_FIELDS - {"occurrences"}:
            if not isinstance(entry[field], str) or not entry[field].strip():
                raise BaselineError(f"entry {number} has empty {field}")
        owner = entry["owner"].strip().lower()
        reason = entry["reason"].strip().lower()
        if owner in PLACEHOLDERS or len(owner) < 3:
            raise BaselineError(f"entry {number} has placeholder owner")
        if reason in PLACEHOLDERS or any(marker in reason for marker in {"requires_triage", "tbd", "todo"}) or len(reason) < 20:
            raise BaselineError(f"entry {number} has placeholder or non-specific reason")
        if entry["remove_by"] not in MILESTONES:
            raise BaselineError(f"entry {number} remove_by must be one of M01 through M12")
        if entry["adr"] not in APPROVED_ADRS or not re.fullmatch(r"ADR-[0-9]{3}", entry["adr"]):
            raise BaselineError(f"entry {number} has unapproved ADR")
        if not isinstance(entry["occurrences"], list) or not entry["occurrences"]:
            raise BaselineError(f"entry {number} occurrences must be a non-empty array")
        occurrence_ids: set[str] = set()
        for occurrence in entry["occurrences"]:
            if set(occurrence) != {"id", "fingerprint", "item", "line"}:
                raise BaselineError(f"entry {number} has malformed occurrence")
            if not all(isinstance(occurrence[k], str) and occurrence[k] for k in ("id", "fingerprint", "item")):
                raise BaselineError(f"entry {number} has empty occurrence identity")
            if not isinstance(occurrence["line"], int) or occurrence["line"] < 1:
                raise BaselineError(f"entry {number} has invalid occurrence line")
            if occurrence["id"] in occurrence_ids:
                raise BaselineError(f"entry {number} has duplicate occurrence id")
            occurrence_ids.add(occurrence["id"])
        if entry["identity"] in identities:
            raise BaselineError(f"duplicate identity {entry['identity']}")
        identities.add(entry["identity"])


def compare_findings(actual: list[Finding], baseline: dict, milestone: str) -> list[Issue]:
    validate_baseline(baseline)
    if milestone not in MILESTONES:
        raise BaselineError("current milestone must be one of M01 through M12")
    expected = {e["identity"]: e for e in baseline["entries"]}
    observed = {f.identity: f for f in actual}
    issues: list[Issue] = []
    for identity, finding in observed.items():
        if identity not in expected:
            issues.append(Issue("NEW", f"{finding.path}: {finding.kind} {finding.symbol}"))
            continue
        entry = expected[identity]
        actual_tuple = (finding.path, finding.symbol, finding.kind, finding.category)
        expected_tuple = tuple(entry[k] for k in ("path", "symbol", "kind", "category"))
        if actual_tuple != expected_tuple:
            issues.append(Issue("CHANGED", identity))
        actual_occurrences = {o["id"]: o for o in finding.occurrences}
        expected_occurrences = {o["id"]: o for o in entry["occurrences"]}
        for occurrence_id, occurrence in actual_occurrences.items():
            if occurrence_id not in expected_occurrences:
                issues.append(Issue("NEW", f"{finding.path}: {finding.kind} {finding.symbol} occurrence={occurrence_id}"))
            elif any(occurrence[k] != expected_occurrences[occurrence_id][k] for k in ("fingerprint", "item")):
                issues.append(Issue("CHANGED", f"{identity} occurrence={occurrence_id}"))
        for occurrence_id in expected_occurrences.keys() - actual_occurrences.keys():
            issues.append(Issue("STALE", f"{entry['path']}: occurrence={occurrence_id}"))
    for identity, entry in expected.items():
        if identity not in observed:
            issues.append(Issue("STALE", f"{entry['path']}: {entry['kind']} {entry['symbol']}"))
        if MILESTONES[entry["remove_by"]] < MILESTONES[milestone]:
            issues.append(Issue("EXPIRED", f"{identity} remove_by={entry['remove_by']}"))
    return issues


def validate_relocation_approvals(approvals: dict | None) -> dict:
    approvals = approvals or {}
    for key, approval in approvals.items():
        if not isinstance(key, tuple) or len(key) != 2 or not all(isinstance(value, str) for value in key):
            raise BaselineError("relocation approval keys must be (identity, replacement occurrence id)")
        if set(approval) != {"from", "reason", "adr"}:
            raise BaselineError(f"relocation approval {key} has invalid fields")
        if not all(isinstance(approval[field], str) and approval[field].strip() for field in approval):
            raise BaselineError(f"relocation approval {key} fields must be non-empty strings")
        if approval["adr"] not in APPROVED_ADRS:
            raise BaselineError(f"relocation approval {key} references an unapproved ADR")
        if approval["reason"].strip().lower() in PLACEHOLDERS:
            raise BaselineError(f"relocation approval {key} has a placeholder reason")
    return approvals


def validate_identity_relocations(relocations: dict | None) -> dict:
    relocations = relocations or {}
    for target_identity, relocation in relocations.items():
        if not isinstance(target_identity, str) or not target_identity:
            raise BaselineError("identity relocation target must be a non-empty string")
        if set(relocation) != {"from", "reason", "adr"}:
            raise BaselineError(f"identity relocation {target_identity} has invalid fields")
        if not all(isinstance(relocation[field], str) and relocation[field].strip() for field in relocation):
            raise BaselineError(f"identity relocation {target_identity} fields must be non-empty strings")
        if relocation["from"] == target_identity:
            raise BaselineError(f"identity relocation {target_identity} must change identity")
        if relocation["adr"] not in APPROVED_ADRS:
            raise BaselineError(f"identity relocation {target_identity} references an unapproved ADR")
        if relocation["reason"].strip().lower() in PLACEHOLDERS:
            raise BaselineError(f"identity relocation {target_identity} has a placeholder reason")
    return relocations


def compare_baselines(
    old: dict,
    new: dict,
    relocation_approvals: dict | None = None,
    identity_relocations: dict | None = None,
) -> list[Issue]:
    validate_baseline(old); validate_baseline(new)
    relocation_approvals = validate_relocation_approvals(relocation_approvals)
    identity_relocations = validate_identity_relocations(identity_relocations)
    old_entries = {e["identity"]: e for e in old["entries"]}
    new_entries = {e["identity"]: e for e in new["entries"]}
    issues: list[Issue] = []
    consumed_relocations: set[tuple[str, str]] = set()
    consumed_identity_relocations: set[str] = set()
    for entry in new["entries"]:
        previous = old_entries.get(entry["identity"])
        if previous is None:
            identity_relocation = identity_relocations.get(entry["identity"])
            previous = old_entries.get(identity_relocation["from"]) if identity_relocation else None
            if previous is None:
                issues.append(Issue("EXPANDED", entry["identity"])); continue
            consumed_identity_relocations.add(entry["identity"])
            for field in ("category", "owner", "reason", "adr"):
                if entry[field] != previous[field]:
                    issues.append(Issue("CHANGED", f"{entry['identity']} {field}"))
            old_m = MILESTONES.get(previous["remove_by"])
            new_m = MILESTONES.get(entry["remove_by"])
            if old_m is None or new_m is None or new_m > old_m:
                issues.append(Issue("DEADLINE_EXTENDED", entry["identity"]))
            continue
        for field in ("path", "symbol", "kind", "category", "owner", "reason", "adr"):
            if entry[field] != previous[field]: issues.append(Issue("CHANGED", f"{entry['identity']} {field}"))
        old_occurrences = {o["id"]: o for o in previous["occurrences"]}
        new_occurrence_ids = {o["id"] for o in entry["occurrences"]}
        for occurrence in entry["occurrences"]:
            if occurrence["id"] not in old_occurrences:
                relocation_key = (entry["identity"], occurrence["id"])
                approval = relocation_approvals.get(relocation_key)
                source = old_occurrences.get(approval["from"]) if approval else None
                if source is None:
                    issues.append(Issue("EXPANDED", f"{entry['identity']} occurrence={occurrence['id']}"))
                elif approval["from"] in new_occurrence_ids:
                    issues.append(Issue("RELOCATION_NOT_MOVE", f"{entry['identity']} occurrence={occurrence['id']}"))
                elif source["item"] != occurrence["item"]:
                    issues.append(Issue("RELOCATION_ITEM_CHANGED", f"{entry['identity']} occurrence={occurrence['id']}"))
                else:
                    consumed_relocations.add(relocation_key)
            elif any(occurrence[k] != old_occurrences[occurrence["id"]][k] for k in ("fingerprint", "item")):
                relocation_key = (entry["identity"], occurrence["id"])
                approval = relocation_approvals.get(relocation_key)
                source = old_occurrences[occurrence["id"]]
                if (
                    approval is not None
                    and approval["from"] == occurrence["id"]
                    and source["item"] == occurrence["item"]
                ):
                    consumed_relocations.add(relocation_key)
                else:
                    issues.append(Issue("CHANGED", f"{entry['identity']} occurrence={occurrence['id']}"))
        old_m = MILESTONES.get(previous["remove_by"])
        new_m = MILESTONES.get(entry["remove_by"])
        if old_m is None or new_m is None or new_m > old_m:
            issues.append(Issue("DEADLINE_EXTENDED", entry["identity"]))
    for relocation_key in relocation_approvals.keys() - consumed_relocations:
        issues.append(Issue("UNUSED_RELOCATION", f"{relocation_key[0]} occurrence={relocation_key[1]}"))
    for target_identity in identity_relocations.keys() - consumed_identity_relocations:
        issues.append(Issue("UNUSED_IDENTITY_RELOCATION", target_identity))
    identity_sources = {relocation["from"] for relocation in identity_relocations.values()}
    for source_identity in identity_sources:
        if source_identity in new_entries:
            issues.append(Issue("IDENTITY_RELOCATION_NOT_MOVE", source_identity))
            continue
        source = old_entries.get(source_identity)
        if source is None:
            issues.append(Issue("IDENTITY_RELOCATION_SOURCE_MISSING", source_identity))
            continue
        targets = [
            new_entries[target_identity]
            for target_identity, relocation in identity_relocations.items()
            if relocation["from"] == source_identity and target_identity in new_entries
        ]
        old_occurrences = len(source["occurrences"])
        new_occurrences = sum(len(target["occurrences"]) for target in targets)
        if new_occurrences > old_occurrences:
            issues.append(
                Issue(
                    "IDENTITY_RELOCATION_EXPANDED",
                    f"{source_identity} occurrences={old_occurrences}->{new_occurrences}",
                )
            )
    return issues


def promote_findings(
    findings: list[Finding],
    baseline: dict,
    milestone: str,
    relocation_approvals: dict | None = None,
    identity_relocations: dict | None = None,
) -> dict:
    """Create a refreshed baseline only when governed debt strictly does not expand."""
    validate_baseline(baseline)
    if milestone not in MILESTONES:
        raise BaselineError("current milestone must be one of M01 through M12")
    previous = {entry["identity"]: entry for entry in baseline["entries"]}
    identity_relocations = validate_identity_relocations(identity_relocations)
    previous_occurrences = sum(len(entry["occurrences"]) for entry in previous.values())
    current_occurrences = sum(len(finding.occurrences) for finding in findings)
    if current_occurrences > previous_occurrences:
        raise BaselineError(
            f"ArcMut occurrence debt expanded from {previous_occurrences} to {current_occurrences}"
        )

    entries = []
    for finding in findings:
        old_entry = previous.get(finding.identity)
        if old_entry is None and finding.identity in identity_relocations:
            old_entry = previous.get(identity_relocations[finding.identity]["from"])
        if old_entry is None:
            raise BaselineError(f"ArcMut identity debt expanded: {finding.identity}")
        entries.append({
            **finding._asdict(),
            "occurrences": list(finding.occurrences),
            "owner": old_entry["owner"],
            "reason": old_entry["reason"],
            "remove_by": old_entry["remove_by"],
            "adr": old_entry["adr"],
        })

    promoted = {"schema_version": 1, "current_milestone": milestone, "entries": entries}
    validate_baseline(promoted)
    issues = compare_baselines(baseline, promoted, relocation_approvals, identity_relocations)
    if issues:
        details = "; ".join(f"{issue.code}: {issue.detail}" for issue in issues)
        raise BaselineError(f"baseline promotion is not monotonic: {details}")
    return promoted


def prune_resolved_findings(findings: list[Finding], baseline: dict) -> dict:
    """Remove only baseline identities that no longer exist in the source tree.

    Retained entries stay byte-for-byte equivalent at the data-model level, so
    source moves, fingerprint drift, and occurrence expansion remain visible to
    the normal comparison instead of being accepted as part of debt reduction.
    """
    validate_baseline(baseline)
    observed_identities = {finding.identity for finding in findings}
    pruned = {
        "schema_version": baseline["schema_version"],
        "current_milestone": baseline["current_milestone"],
        "entries": [
            entry
            for entry in baseline["entries"]
            if entry["identity"] in observed_identities
        ],
    }
    validate_baseline(pruned)
    issues = compare_baselines(baseline, pruned)
    if issues:
        details = "; ".join(f"{issue.code}: {issue.detail}" for issue in issues)
        raise BaselineError(f"resolved baseline pruning is not monotonic: {details}")
    return pruned


def apply_reviewed_reductions(
    findings: list[Finding],
    baseline: dict,
    relocation_approvals: dict | None = None,
) -> dict:
    """Apply deletions and reviewed relocations without accepting other drift."""
    validate_baseline(baseline)
    approvals = validate_relocation_approvals(relocation_approvals)
    observed = {finding.identity: finding for finding in findings}
    entries = []

    for entry in baseline["entries"]:
        finding = observed.get(entry["identity"])
        if finding is None:
            continue

        old_occurrences = {occurrence["id"]: occurrence for occurrence in entry["occurrences"]}
        current_occurrences = {occurrence["id"]: occurrence for occurrence in finding.occurrences}
        accepted_current_ids: set[str] = set()
        consumed_old_ids: set[str] = set()
        next_occurrences = []

        for occurrence in finding.occurrences:
            previous = old_occurrences.get(occurrence["id"])
            approval = approvals.get((entry["identity"], occurrence["id"]))
            if previous is not None:
                unchanged = all(previous[field] == occurrence[field] for field in ("fingerprint", "item"))
                if unchanged:
                    next_occurrences.append(occurrence)
                    accepted_current_ids.add(occurrence["id"])
                elif approval is not None and approval["from"] == occurrence["id"] and previous["item"] == occurrence["item"]:
                    next_occurrences.append(occurrence)
                    accepted_current_ids.add(occurrence["id"])
                    consumed_old_ids.add(previous["id"])
                continue

            source = old_occurrences.get(approval["from"]) if approval else None
            if (
                source is not None
                and approval["from"] not in current_occurrences
                and source["item"] == occurrence["item"]
            ):
                next_occurrences.append(occurrence)
                accepted_current_ids.add(occurrence["id"])
                consumed_old_ids.add(source["id"])

        unaccepted_current_items = {
            occurrence["item"]
            for occurrence in finding.occurrences
            if occurrence["id"] not in accepted_current_ids
        }
        has_unaccepted_current = bool(unaccepted_current_items)
        identity_has_approvals = any(identity == entry["identity"] for identity, _ in approvals)
        for occurrence in entry["occurrences"]:
            if occurrence["id"] in consumed_old_ids or occurrence["id"] in accepted_current_ids:
                continue
            if (
                occurrence["id"] in current_occurrences
                or occurrence["item"] in unaccepted_current_items
            ):
                next_occurrences.append(occurrence)

        if has_unaccepted_current and not identity_has_approvals:
            next_occurrences = list(entry["occurrences"])
        if not next_occurrences:
            continue
        entries.append({**entry, "occurrences": next_occurrences})

    reduced = {
        "schema_version": baseline["schema_version"],
        "current_milestone": baseline["current_milestone"],
        "entries": entries,
    }
    validate_baseline(reduced)
    issues = compare_baselines(baseline, reduced, approvals)
    if issues:
        details = "; ".join(f"{issue.code}: {issue.detail}" for issue in issues)
        raise BaselineError(f"reviewed baseline reduction is not monotonic: {details}")
    return reduced


def _write_below_target(data: dict, output: Path, root: Path, option: str) -> None:
    target = (root / "target").resolve()
    try:
        output.resolve().relative_to(target)
    except ValueError as exc:
        raise BaselineError(f"{option} output must be below <root>/target") from exc
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def _bootstrap(findings: list[Finding], output: Path, root: Path) -> None:
    entries = [
        {**f._asdict(), "owner": "UNASSIGNED", "reason": "REQUIRES_TRIAGE", "remove_by": "M03", "adr": "ADR-002"}
        for f in findings
    ]
    _write_below_target(
        {"schema_version": 1, "current_milestone": "M01", "entries": entries},
        output,
        root,
        "--bootstrap",
    )


def run_fixtures() -> int:
    cases = {
        "comment": ("// ArcMut", False), "string": ('"ArcMut"', False),
        "raw": ('r#"ArcMut"#', False), "char": ("'A'", False),
        "type": ("fn f(x: ArcMut<T>){}", True), "ctor": ("ArcMut::new(1);", True),
        "weak": ("fn f(x: WeakArcMut<T>){}", True), "wrapper": ("SyncUnsafeCellWrapper::new(1);", True),
        "import": ("use x::ArcMut;", True), "alias": ("use x::ArcMut as S; fn f(x:S<T>){}", True),
        "type_alias": ("type S<T>=ArcMut<T>;", True), "reexport": ("pub use x::ArcMut;", True),
        "mut_call": ("x.mut_from_ref();", True), "mut_def": ("fn mut_from_ref(&self)->&mut T{todo!()}", True),
        "asmut": ("impl<T> AsMut<T> for ArcMut<T>{}", True), "derefmut": ("impl<T> DerefMut for ArcMut<T>{}", True),
        "unsafe_cell": ("struct S<T>{x:UnsafeCell<T>} unsafe impl<T> Sync for S<T>{}", True),
        "prod": ("fn f(x: ArcMut<T>){}", True), "test": ("fn f(x: ArcMut<T>){}", True),
        "compat": ("fn f(x: ArcMut<T>){}", True), "nested_comment": ("/* /* ArcMut */ */", False),
        "byte_string": ('b"ArcMut"', False), "raw_byte": ('br#"ArcMut"#', False),
        "rename": ("use x::ArcMut as Harmless; fn f(x:Harmless<T>){}", True),
    }
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        for name, (source, expected) in cases.items():
            sub = "src/compat" if name == "compat" else "tests" if name == "test" else "src"
            path = root / name / sub / "case.rs"; path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(source, encoding="utf-8")
            found = bool(scan_file(path, root))
            if found != expected:
                print(f"FIXTURE_FAILED {name} expected={expected} actual={found}"); return 1
    print(f"FIXTURES_OK cases={len(cases)}")
    return 0


def _load(path: Path) -> dict:
    try:
        data = json.loads(path.read_text(encoding="utf-8")); validate_baseline(data); return data
    except (OSError, json.JSONDecodeError, BaselineError) as exc:
        raise BaselineError(f"cannot load {path}: {exc}") from exc


def _load_relocation_approvals(path: Path | None) -> tuple[dict, dict]:
    if path is None:
        return {}, {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BaselineError(f"cannot load {path}: {exc}") from exc
    if set(data) not in (
        {"schema_version", "adr", "relocations"},
        {"schema_version", "adr", "relocations", "identity_relocations"},
    ) or data["schema_version"] != 1:
        raise BaselineError("relocation approvals must use schema version 1 and exact top-level fields")
    if data["adr"] not in APPROVED_ADRS:
        raise BaselineError("relocation approvals reference an unapproved ADR")
    if not isinstance(data["relocations"], list):
        raise BaselineError("relocations must be a list")
    approvals = {}
    for number, relocation in enumerate(data["relocations"], start=1):
        if set(relocation) != {"identity", "from", "to", "reason"}:
            raise BaselineError(f"relocation {number} has invalid fields")
        key = (relocation["identity"], relocation["to"])
        if key in approvals:
            raise BaselineError(f"duplicate relocation approval {key}")
        approvals[key] = {
            "from": relocation["from"],
            "reason": relocation["reason"],
            "adr": data["adr"],
        }
    identity_relocations = {}
    raw_identity_relocations = data.get("identity_relocations", [])
    if not isinstance(raw_identity_relocations, list):
        raise BaselineError("identity_relocations must be a list")
    for number, relocation in enumerate(raw_identity_relocations, start=1):
        if set(relocation) != {"from", "to", "reason"}:
            raise BaselineError(f"identity relocation {number} has invalid fields")
        target_identity = relocation["to"]
        if target_identity in identity_relocations:
            raise BaselineError(f"duplicate identity relocation approval {target_identity}")
        identity_relocations[target_identity] = {
            "from": relocation["from"],
            "reason": relocation["reason"],
            "adr": data["adr"],
        }
    return (
        validate_relocation_approvals(approvals),
        validate_identity_relocations(identity_relocations),
    )


def resolve_current_milestone(override: str | None, baseline: dict) -> str:
    milestone = override or baseline["current_milestone"]
    if milestone not in MILESTONES:
        raise BaselineError("current milestone must be one of M01 through M12")
    return milestone


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--baseline", type=Path, default=Path(__file__).with_name("arc-mut-baseline.json"))
    parser.add_argument("--current-milestone")
    parser.add_argument("--bootstrap", type=Path)
    parser.add_argument("--promote-baseline", type=Path)
    parser.add_argument("--prune-resolved", type=Path)
    parser.add_argument("--apply-reviewed-reductions", type=Path)
    parser.add_argument("--compare-baseline", type=Path)
    parser.add_argument("--relocation-approvals", type=Path)
    parser.add_argument("--fixtures", action="store_true")
    args = parser.parse_args(argv)
    try:
        if args.fixtures: return run_fixtures()
        relocation_approvals, identity_relocations = _load_relocation_approvals(args.relocation_approvals)
        if args.compare_baseline:
            issues = compare_baselines(
                _load(args.baseline),
                _load(args.compare_baseline),
                relocation_approvals,
                identity_relocations,
            )
        else:
            findings = scan_tree(args.root.resolve())
            if args.bootstrap:
                _bootstrap(findings, args.bootstrap, args.root.resolve())
                print(f"BOOTSTRAP_WRITTEN {args.bootstrap} entries={len(findings)}"); return 0
            if args.prune_resolved:
                baseline = _load(args.baseline)
                pruned = prune_resolved_findings(findings, baseline)
                _write_below_target(pruned, args.prune_resolved, args.root.resolve(), "--prune-resolved")
                old_occurrences = sum(len(entry["occurrences"]) for entry in baseline["entries"])
                new_occurrences = sum(len(entry["occurrences"]) for entry in pruned["entries"])
                print(
                    f"BASELINE_PRUNED {args.prune_resolved} "
                    f"entries={len(baseline['entries'])}->{len(pruned['entries'])} "
                    f"occurrences={old_occurrences}->{new_occurrences}"
                )
                return 0
            if args.apply_reviewed_reductions:
                baseline = _load(args.baseline)
                reduced = apply_reviewed_reductions(findings, baseline, relocation_approvals)
                _write_below_target(
                    reduced,
                    args.apply_reviewed_reductions,
                    args.root.resolve(),
                    "--apply-reviewed-reductions",
                )
                old_occurrences = sum(len(entry["occurrences"]) for entry in baseline["entries"])
                new_occurrences = sum(len(entry["occurrences"]) for entry in reduced["entries"])
                print(
                    f"BASELINE_REDUCED {args.apply_reviewed_reductions} "
                    f"entries={len(baseline['entries'])}->{len(reduced['entries'])} "
                    f"occurrences={old_occurrences}->{new_occurrences}"
                )
                return 0
            if args.promote_baseline:
                baseline = _load(args.baseline)
                milestone = resolve_current_milestone(args.current_milestone, baseline)
                promoted = promote_findings(
                    findings,
                    baseline,
                    milestone,
                    relocation_approvals,
                    identity_relocations,
                )
                _write_below_target(
                    promoted, args.promote_baseline, args.root.resolve(), "--promote-baseline"
                )
                occurrences = sum(len(entry["occurrences"]) for entry in promoted["entries"])
                print(
                    f"BASELINE_PROMOTED {args.promote_baseline} "
                    f"entries={len(promoted['entries'])} occurrences={occurrences}"
                )
                return 0
            baseline = _load(args.baseline)
            milestone = resolve_current_milestone(args.current_milestone, baseline)
            issues = compare_findings(findings, baseline, milestone)
        for issue in issues: print(f"{issue.code}: {issue.detail}")
        if issues: print(f"ARC_MUT_GUARD_FAILED violations={len(issues)}"); return 1
        print("ARC_MUT_GUARD_OK")
        return 0
    except (LexError, BaselineError, OSError, UnicodeError) as exc:
        print(f"ARC_MUT_GUARD_ERROR: {exc}", file=sys.stderr); return 2


if __name__ == "__main__":
    raise SystemExit(main())
