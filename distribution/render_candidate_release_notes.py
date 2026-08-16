#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import argparse
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, load_candidate, load_layout
from release_state import read_json, resolve_within


TEMPLATE = ROOT / "rocketmq-doc" / "en" / "release" / "1.0" / "release-notes-template.md"
IDENTITY = ROOT / "distribution" / "release-identity.json"


def _known_issues(values: list[Any]) -> str:
    if not values:
        return "- None recorded for this candidate."
    rendered: list[str] = []
    for value in values:
        if isinstance(value, str):
            rendered.append(f"- {value}")
        elif isinstance(value, dict):
            identifier = value.get("id", "unidentified")
            summary = value.get("summary") or value.get("description") or "No summary"
            rendered.append(f"- `{identifier}`: {summary}")
        else:
            raise ArchiveError("candidate known_issues contains an unsupported entry")
    return "\n".join(rendered)


def render_notes(candidate: dict[str, Any]) -> str:
    template = TEMPLATE.read_text(encoding="utf-8")
    identity = read_json(IDENTITY)
    layout = load_layout()
    components = "\n".join(
        f"- `{entry['id']}`: `{entry['binary']}` ({entry['kind']})"
        for entry in layout["binaries"]
    )
    exclusions = "\n".join(
        f"- {value}: not supported by this core distribution."
        for value in layout["excluded_capabilities"]
    )
    replacements = {
        "{{version}}": candidate["version"],
        "{{candidate_id}}": candidate["candidate_id"],
        "{{run_id}}": candidate["run_id"],
        "{{attempt}}": str(candidate["attempt"]),
        "{{approver}}": identity["approval"]["approver"],
        "{{included_components}}": components,
        "{{excluded_capabilities}}": exclusions,
        "{{known_issues}}": _known_issues(candidate["known_issues"]),
    }
    for marker, value in replacements.items():
        template = template.replace(marker, value)
    if "{{" in template or "}}" in template:
        raise ArchiveError("release notes template has unresolved markers")
    return template.rstrip() + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        _manifest, candidate, root = load_candidate(args.candidate_manifest)
        output = args.output or root / "common-input-source" / "RELEASE_NOTES.md"
        output = resolve_within(root, output, "release notes output")
        if output.exists():
            raise ArchiveError(f"release notes already exist: {output}")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(render_notes(candidate), encoding="utf-8", newline="\n")
        print(f"CANDIDATE_RELEASE_NOTES_OK output={output}")
        return 0
    except (ArchiveError, OSError) as error:
        print(f"CANDIDATE_RELEASE_NOTES_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
