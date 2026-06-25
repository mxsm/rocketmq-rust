#!/usr/bin/env python3
"""Validate RocketMQ Rust pull request title and body content."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


TITLE_RE = re.compile(
    r"^\[ISSUE #(?P<issue>\d+)\](?P<emoji>🐛|📝|✨|🚀|♻️|🧪)(?P<summary>\S(?:.*\S)?)$",
)

LOCAL_PATH_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
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

REQUIRED_HEADINGS = [
    "### Which Issue(s) This PR Fixes(Closes)",
    "### Brief Description",
    "### How Did You Test This Change?",
]


def find_local_paths(text: str) -> list[str]:
    findings: list[str] = []
    for line_no, line in enumerate(text.splitlines(), start=1):
        for name, pattern in LOCAL_PATH_PATTERNS:
            for match in pattern.finditer(line):
                findings.append(f"line {line_no}: {name}: {match.group(0)}")
    return findings


def validate_heading_order(body: str) -> list[str]:
    errors: list[str] = []
    positions: list[int] = []
    for heading in REQUIRED_HEADINGS:
        position = body.find(heading)
        if position < 0:
            errors.append(f"missing heading: {heading}")
        positions.append(position)

    present_positions = [position for position in positions if position >= 0]
    if len(present_positions) == len(REQUIRED_HEADINGS) and present_positions != sorted(
        present_positions,
    ):
        errors.append("required headings are not in template order")
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate RocketMQ Rust PR title and body content.",
    )
    parser.add_argument("--title", required=True, help="PR title to validate.")
    parser.add_argument("--body", required=True, help="Path to PR body markdown.")
    args = parser.parse_args()

    body = Path(args.body).read_text(encoding="utf-8")
    errors: list[str] = []

    title_match = TITLE_RE.match(args.title)
    if title_match is None:
        errors.append("title must match: [ISSUE #issue_id]<emoji><summary>")
        title_issue = None
    else:
        title_issue = title_match.group("issue")
        if not title_match.group("summary").strip():
            errors.append("title summary must not be empty")

    errors.extend(validate_heading_order(body))

    if "#issue_id" in body:
        errors.append("body still contains #issue_id placeholder")

    fixes_matches = re.findall(r"(?m)^-\s+Fixes\s+#(\d+)\s*$", body)
    if not fixes_matches:
        errors.append("body must contain a '- Fixes #issue_id' line with a numeric issue id")
    elif title_issue is not None and title_issue not in fixes_matches:
        errors.append(
            f"title issue #{title_issue} does not match body Fixes issue(s): "
            + ", ".join(f"#{issue}" for issue in fixes_matches),
        )

    for finding in find_local_paths(args.title + "\n" + body):
        errors.append(f"local path leak: {finding}")

    if errors:
        print("PR content validation failed:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    print("OK: PR title and body passed validation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
