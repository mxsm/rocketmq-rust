#!/usr/bin/env python3
"""Detect local filesystem paths and non-English text in a GitHub issue draft."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    (
        "windows drive path",
        re.compile(r"(?<![A-Za-z0-9_])(?:[A-Za-z]:[\\/][^\s`'\"<>|]+)"),
    ),
    (
        "windows UNC path",
        re.compile(r"(?<![A-Za-z0-9_])(?:\\\\[^\s\\/:*?\"<>|]+\\[^\s`'\"<>|]+)"),
    ),
    (
        "unix user home path",
        re.compile(r"(?<![A-Za-z0-9_])/(?:Users|home)/[A-Za-z0-9._-]+(?:/[^\s`'\"<>]+)*"),
    ),
    (
        "wsl mount path",
        re.compile(r"(?<![A-Za-z0-9_])/mnt/[A-Za-z]/[^\s`'\"<>]+"),
    ),
    (
        "tilde home path",
        re.compile(r"(?<![A-Za-z0-9_])~[\\/][^\s`'\"<>]+"),
    ),
    (
        "file uri",
        re.compile(r"file://[^\s`'\"<>]+", re.IGNORECASE),
    ),
    (
        "home environment variable",
        re.compile(
            r"(?:%USERPROFILE%|%HOMEPATH%|\$HOME|\$\{HOME\}|\$env:USERPROFILE)",
            re.IGNORECASE,
        ),
    ),
]

ALLOWED_NON_ASCII = frozenset("\U0001F41B\U0001F4DD\u2728\U0001F680\u267B\uFE0F\U0001F9EA\ufeff")


TITLE_MARKER_RE = re.compile(
    r"\b(?:task|phase|stage|step|part|milestone)\s*[-_:]?\s*(?:\d+|[ivxlcdm]+)\b",
    re.IGNORECASE,
)


def read_input(path_arg: str) -> tuple[str, str]:
    if path_arg == "-":
        return "<stdin>", sys.stdin.read()

    path = Path(path_arg)
    return str(path), path.read_text(encoding="utf-8")


def scan_text(text: str) -> list[tuple[int, str, str]]:
    findings: list[tuple[int, str, str]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for name, pattern in PATTERNS:
            for match in pattern.finditer(line):
                findings.append((line_no, name, match.group(0)))
    return findings


def find_non_english_text(text: str) -> list[tuple[int, str, str]]:
    findings: list[tuple[int, str, str]] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for char in line:
            if ord(char) < 128 or char in ALLOWED_NON_ASCII:
                continue
            findings.append((line_no, "non-English character", f"U+{ord(char):04X} {char!r}"))
    return findings


def find_title_markers(title: str) -> list[str]:
    return [match.group(0) for match in TITLE_MARKER_RE.finditer(title)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fail if a GitHub issue draft contains local filesystem paths or non-English text.",
    )
    parser.add_argument(
        "--title",
        default="",
        help="Optional issue title to include in the audit.",
    )
    parser.add_argument(
        "draft",
        help="Issue draft markdown file, or '-' to read from standard input.",
    )
    args = parser.parse_args()

    source, text = read_input(args.draft)
    audited_text = f"{args.title}\n{text}" if args.title else text
    findings = scan_text(audited_text)
    english_findings = find_non_english_text(audited_text)
    title_marker_findings = find_title_markers(args.title) if args.title else []

    if not findings and not english_findings and not title_marker_findings:
        print(f"OK: no local paths, title markers, or non-English text detected in {source}")
        return 0

    if findings:
        print(f"Local path findings in {source}:", file=sys.stderr)
        for line_no, name, value in findings:
            print(f"  line {line_no}: {name}: {value}", file=sys.stderr)
    if english_findings:
        print(f"Non-English text findings in {source}:", file=sys.stderr)
        for line_no, name, value in english_findings:
            print(f"  line {line_no}: {name}: {value}", file=sys.stderr)
    if title_marker_findings:
        print(f"Sequencing-marker title findings in {source}:", file=sys.stderr)
        for value in title_marker_findings:
            print(f"  title: {value}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
