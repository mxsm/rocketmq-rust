#!/usr/bin/env python3
#
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

"""Discover the Cargo production sources scanned by the D-06 hygiene guard."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import environment_write_guard as rust_source


EXCLUDED_ROOTS = {"benches", "examples", "fixtures", "fixture", "fuzz", "target", "tests"}
EXTERNAL_MODULE = re.compile(
    r"(?P<attributes>(?:#\s*\[[^]]*\]\s*)*)"
    r"(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*;",
    re.MULTILINE,
)
CFG_ATTRIBUTE = re.compile(r"#\s*\[\s*cfg\s*\((?P<body>.*?)\)\s*\]", re.DOTALL)
PATH_ATTRIBUTE = re.compile(r'#\s*\[\s*path\s*=\s*"(?P<path>[^"\r\n]+)"\s*\]')


@dataclass(frozen=True)
class DiscoveryFinding:
    path: str
    line: int
    reason: str


@dataclass(frozen=True)
class PendingSource:
    path: Path
    module_dir: Path


def _relative(root: Path, path: Path) -> str:
    return path.resolve().relative_to(root).as_posix()


def _excluded(root: Path, path: Path) -> bool:
    try:
        parts = path.resolve().relative_to(root).parts
    except ValueError:
        return True
    return bool(EXCLUDED_ROOTS.intersection(part.lower() for part in parts))


def _target_module_dir(path: Path) -> Path:
    return path.parent if path.name == "mod.rs" else path.parent / path.stem


def _register_root(
    root: Path,
    manifest: Path,
    candidate: Path,
    target: str,
    required: bool,
    pending: list[PendingSource],
    findings: list[DiscoveryFinding],
) -> None:
    resolved = candidate.resolve()
    relative_manifest = _relative(root, manifest)
    if not resolved.is_relative_to(root):
        findings.append(DiscoveryFinding(relative_manifest, 1, f"Cargo {target} escapes repository root"))
    elif resolved.is_file():
        pending.append(PendingSource(resolved, resolved.parent))
    elif required:
        findings.append(DiscoveryFinding(relative_manifest, 1, f"missing Cargo {target} target"))


def _cargo_roots(
    root: Path, root_filter: Callable[[str], bool] | None
) -> tuple[list[PendingSource], list[DiscoveryFinding], bool]:
    pending: list[PendingSource] = []
    findings: list[DiscoveryFinding] = []
    saw_package = False
    for manifest in root.rglob("Cargo.toml"):
        if _excluded(root, manifest):
            continue
        relative_manifest = _relative(root, manifest)
        if root_filter is not None and not root_filter(relative_manifest):
            continue
        try:
            document = tomllib.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            findings.append(DiscoveryFinding(relative_manifest, 1, f"unreadable Cargo manifest: {error}"))
            continue
        package = document.get("package")
        if not isinstance(package, dict):
            continue
        saw_package = True
        package_root = manifest.parent
        package_name = package.get("name")
        if not isinstance(package_name, str) or not package_name:
            findings.append(DiscoveryFinding(relative_manifest, 1, "Cargo package requires a name"))
            package_name = package_root.name

        library = document.get("lib")
        if isinstance(library, dict):
            library_path = library.get("path", "src/lib.rs")
            if not isinstance(library_path, str):
                findings.append(DiscoveryFinding(relative_manifest, 1, "Cargo library path must be a string"))
            else:
                _register_root(root, manifest, package_root / library_path, "library", True, pending, findings)
        elif package.get("autolib", True) is not False:
            _register_root(root, manifest, package_root / "src/lib.rs", "library", False, pending, findings)

        explicit_paths: set[Path] = set()
        explicit_names: set[str] = set()
        binaries = document.get("bin", [])
        if isinstance(binaries, list):
            for binary in binaries:
                if not isinstance(binary, dict):
                    findings.append(DiscoveryFinding(relative_manifest, 1, "Cargo binary target must be a table"))
                    continue
                name = binary.get("name")
                path_value = binary.get("path")
                if path_value is None and isinstance(name, str) and name:
                    main = package_root / "src/main.rs"
                    flat = package_root / f"src/bin/{name}.rs"
                    nested = package_root / f"src/bin/{name}/main.rs"
                    if name == package_name and main.is_file():
                        path_value = str(main.relative_to(package_root))
                    else:
                        choices = [path for path in (flat, nested) if path.is_file()]
                        if len(choices) > 1:
                            findings.append(
                                DiscoveryFinding(relative_manifest, 1, f"ambiguous Cargo binary target {name}")
                            )
                            continue
                        path_value = str((choices[0] if choices else flat).relative_to(package_root))
                if not isinstance(path_value, str):
                    findings.append(DiscoveryFinding(relative_manifest, 1, "Cargo binary path must be a string"))
                    continue
                target = (package_root / path_value).resolve()
                explicit_paths.add(target)
                if isinstance(name, str):
                    explicit_names.add(name)
                _register_root(root, manifest, target, "binary", True, pending, findings)

        if package.get("autobins", True) is not False:
            automatic = [(package_root / "src/main.rs", package_name)]
            automatic.extend((path, path.stem) for path in (package_root / "src/bin").glob("*.rs"))
            automatic.extend((path, path.parent.name) for path in (package_root / "src/bin").glob("*/main.rs"))
            by_name: dict[str, list[Path]] = {}
            for path, name in automatic:
                if path.is_file() and path.resolve() not in explicit_paths and name not in explicit_names:
                    by_name.setdefault(name, []).append(path)
            for name, paths in by_name.items():
                if len(paths) > 1:
                    findings.append(
                        DiscoveryFinding(relative_manifest, 1, f"duplicate automatic binary target {name}")
                    )
                    continue
                _register_root(root, manifest, paths[0], "automatic binary", False, pending, findings)

        build = package.get("build")
        if build is not False:
            build_path = build if isinstance(build, str) else "build.rs"
            if build is not None and build is not True and not isinstance(build, str):
                findings.append(DiscoveryFinding(relative_manifest, 1, "Cargo build path must be a string or boolean"))
            else:
                _register_root(
                    root,
                    manifest,
                    package_root / build_path,
                    "build script",
                    build is True or isinstance(build, str),
                    pending,
                    findings,
                )
    return pending, findings, saw_package


def production_sources(
    root: Path,
    cfg_requires_test: Callable[[str], bool],
    root_filter: Callable[[str], bool] | None = None,
) -> tuple[list[Path], list[DiscoveryFinding]]:
    """Follow literal production module edges from Cargo lib/bin/build roots."""

    root = root.resolve()
    pending, findings, saw_package = _cargo_roots(root, root_filter)
    if not pending and not saw_package:
        fallback_roots = [
            path.resolve()
            for pattern in ("*/src/lib.rs", "*/src/main.rs")
            for path in root.glob(pattern)
            if not _excluded(root, path)
        ]
        if fallback_roots:
            pending.extend(PendingSource(path, path.parent) for path in fallback_roots)
        else:
            return rust_source.production_sources(root), findings

    visited: set[Path] = set()
    while pending:
        current = pending.pop()
        if current.path in visited:
            continue
        visited.add(current.path)
        try:
            source = current.path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError) as error:
            findings.append(DiscoveryFinding(_relative(root, current.path), 1, f"unreadable Rust source: {error}"))
            continue
        masked = rust_source.mask_comments_and_literals(source)
        relative = _relative(root, current.path)
        for declaration in EXTERNAL_MODULE.finditer(masked):
            attributes = source[declaration.start("attributes") : declaration.end("attributes")]
            if any(cfg_requires_test(match.group("body")) for match in CFG_ATTRIBUTE.finditer(attributes)):
                continue
            name = declaration.group("name")
            if re.search(r"#\s*\[\s*cfg_attr\b[^]]*\bpath\s*=", attributes):
                findings.append(
                    DiscoveryFinding(
                        relative,
                        source.count("\n", 0, declaration.start()) + 1,
                        f"unsupported configured path for module {name}",
                    )
                )
                continue
            path_match = PATH_ATTRIBUTE.search(attributes)
            if re.search(r"#\s*\[\s*path\b", attributes) and path_match is None:
                findings.append(
                    DiscoveryFinding(relative, source.count("\n", 0, declaration.start()) + 1, f"invalid path for module {name}")
                )
                continue
            candidates = (
                [current.path.parent / path_match.group("path")]
                if path_match is not None
                else [current.module_dir / f"{name}.rs", current.module_dir / name / "mod.rs"]
            )
            resolved = [candidate.resolve() for candidate in candidates if candidate.resolve().is_file()]
            line = source.count("\n", 0, declaration.start()) + 1
            if any(not candidate.resolve().is_relative_to(root) for candidate in candidates):
                findings.append(DiscoveryFinding(relative, line, f"module {name} escapes repository root"))
            elif len(resolved) != 1:
                reason = "missing" if not resolved else "ambiguous"
                findings.append(DiscoveryFinding(relative, line, f"{reason} production module {name}"))
            else:
                pending.append(PendingSource(resolved[0], _target_module_dir(resolved[0])))
    return sorted(visited), findings
