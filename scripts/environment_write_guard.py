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

"""Reject process-environment mutation from production Rust source."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import NamedTuple


ROOT = Path(__file__).resolve().parents[1]
CALL = re.compile(r"\b(?:std\s*::\s*)?env\s*::\s*(set_var|remove_var)\s*\(")
CFG_TEST_MODULE = re.compile(
    r"#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+[A-Za-z_][A-Za-z0-9_]*\s*\{",
    re.MULTILINE,
)


class Finding(NamedTuple):
    path: str
    line: int
    operation: str


def mask_comments_and_literals(source: str) -> str:
    """Replace Rust comments and literal bodies while preserving offsets/newlines."""

    masked = list(source)
    index = 0
    block_depth = 0
    state = "code"
    raw_hashes = 0

    def blank(position: int) -> None:
        if masked[position] not in "\r\n":
            masked[position] = " "

    while index < len(source):
        if state == "line_comment":
            if source[index] in "\r\n":
                state = "code"
            else:
                blank(index)
            index += 1
            continue

        if state == "block_comment":
            if source.startswith("/*", index):
                blank(index)
                blank(index + 1)
                block_depth += 1
                index += 2
            elif source.startswith("*/", index):
                blank(index)
                blank(index + 1)
                block_depth -= 1
                index += 2
                if block_depth == 0:
                    state = "code"
            else:
                blank(index)
                index += 1
            continue

        if state == "string":
            blank(index)
            if source[index] == "\\" and index + 1 < len(source):
                index += 1
                blank(index)
            elif source[index] == '"':
                state = "code"
            index += 1
            continue

        if state == "character":
            blank(index)
            if source[index] == "\\" and index + 1 < len(source):
                index += 1
                blank(index)
            elif source[index] == "'":
                state = "code"
            index += 1
            continue

        if state == "raw":
            terminator = '"' + ("#" * raw_hashes)
            if source.startswith(terminator, index):
                for offset in range(len(terminator)):
                    blank(index + offset)
                index += len(terminator)
                state = "code"
            else:
                blank(index)
                index += 1
            continue

        if source.startswith("//", index):
            blank(index)
            blank(index + 1)
            index += 2
            state = "line_comment"
        elif source.startswith("/*", index):
            blank(index)
            blank(index + 1)
            index += 2
            block_depth = 1
            state = "block_comment"
        elif source[index] == '"':
            blank(index)
            index += 1
            state = "string"
        elif source[index] == "'":
            lifetime = re.match(r"'[A-Za-z_][A-Za-z0-9_]*", source[index:])
            lifetime_end = index + len(lifetime.group(0)) if lifetime else index
            if lifetime and (lifetime_end >= len(source) or source[lifetime_end] != "'"):
                index += 1
            else:
                blank(index)
                index += 1
                state = "character"
        elif source[index] in {"r", "b"}:
            match = re.match(r"(?:br|r)(#{0,255})\"", source[index:])
            if match:
                raw_hashes = len(match.group(1))
                for offset in range(len(match.group(0))):
                    blank(index + offset)
                index += len(match.group(0))
                state = "raw"
            else:
                index += 1
        else:
            index += 1

    return "".join(masked)


def test_module_ranges(masked: str) -> list[tuple[int, int]]:
    ranges: list[tuple[int, int]] = []
    for match in CFG_TEST_MODULE.finditer(masked):
        opening = masked.find("{", match.start(), match.end())
        depth = 0
        for index in range(opening, len(masked)):
            if masked[index] == "{":
                depth += 1
            elif masked[index] == "}":
                depth -= 1
                if depth == 0:
                    ranges.append((match.start(), index + 1))
                    break
    return ranges


def scan_source(source: str, relative_path: str) -> list[Finding]:
    masked = mask_comments_and_literals(source)
    excluded = test_module_ranges(masked)
    findings: list[Finding] = []
    for match in CALL.finditer(masked):
        if any(start <= match.start() < end for start, end in excluded):
            continue
        findings.append(
            Finding(
                relative_path,
                source.count("\n", 0, match.start()) + 1,
                match.group(1),
            )
        )
    return findings


def production_sources(root: Path) -> list[Path]:
    sources: list[Path] = []
    for path in root.rglob("*.rs"):
        relative = path.relative_to(root)
        parts = relative.parts
        if "target" in parts or "fuzz" in parts:
            continue
        if "src" not in parts or path.name == "build.rs":
            continue
        sources.append(path)
    return sorted(sources)


def scan_tree(root: Path) -> list[Finding]:
    findings: list[Finding] = []
    for path in production_sources(root):
        relative = path.relative_to(root).as_posix()
        findings.extend(scan_source(path.read_text(encoding="utf-8"), relative))
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()

    findings = scan_tree(args.root.resolve())
    if findings:
        for finding in findings:
            print(
                f"{finding.path}:{finding.line}: production environment write "
                f"via env::{finding.operation}",
                file=sys.stderr,
            )
        print(f"ENVIRONMENT_WRITE_GUARD_FAILED findings={len(findings)}", file=sys.stderr)
        return 1

    print("ENVIRONMENT_WRITE_GUARD_OK production_writes=0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
