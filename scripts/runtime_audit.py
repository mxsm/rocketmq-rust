#!/usr/bin/env python3
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

"""Report runtime ownership sites for review without source baselines."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))

import core_release_scope
import environment_write_guard as rust_source
import rust_hygiene_guard as hygiene
import rust_production_sources


ROOT = Path(__file__).resolve().parents[1]
PATTERNS = {
    "task-spawn": re.compile(r"\btokio\s*::\s*(?:task\s*::\s*)?spawn\s*\("),
    "thread-spawn": re.compile(r"\b(?:std\s*::\s*)?thread\s*::\s*spawn\s*\("),
    "runtime-creation": re.compile(r"\b(?:Runtime\s*::\s*new|Builder\s*::\s*new_(?:multi_thread|current_thread))\s*\("),
    "blocking": re.compile(r"\b(?:spawn_blocking|block_on|block_in_place|blocking_recv|blocking_send)\s*\("),
    "scheduler": re.compile(r"\b(?:interval|interval_at)\s*\("),
    "shutdown": re.compile(r"\b(?:cancel|cancelled|cancel_and_wait|shutdown|abort)\s*\("),
}


def scan_source(source: str, relative: str) -> list[dict[str, object]]:
    masked = rust_source.mask_comments_and_literals(source)
    tests = hygiene.cfg_test_item_ranges(masked, source)
    sites = []
    for kind, pattern in PATTERNS.items():
        for match in pattern.finditer(masked):
            if not hygiene.is_test_only(match.start(), tests):
                sites.append({"path": relative, "line": source.count("\n", 0, match.start()) + 1, "kind": kind})
    return sorted(sites, key=lambda site: (site["line"], site["kind"]))


def audit(root: Path, scope: str = "all") -> list[dict[str, object]]:
    root = root.resolve()
    scope_document = core_release_scope.load_scope(root / "scripts/core-release-scope.json") if scope == "core-release" else None
    root_filter = (
        (lambda relative: core_release_scope.path_in_scope(relative, scope, scope_document))
        if scope_document is not None else None
    )
    sources, findings = rust_production_sources.production_sources(root, hygiene.cfg_requires_test, root_filter)
    if findings:
        raise ValueError("; ".join(f"{f.path}:{f.line}: {f.reason}" for f in findings))
    return [
        site
        for path in sources
        for site in scan_source(path.read_text(encoding="utf-8"), path.relative_to(root).as_posix())
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=Path("target/runtime-audit"))
    parser.add_argument("--scope", choices=("core-release", "all"), default="all")
    args = parser.parse_args()
    root = args.root.resolve()
    output = args.output if args.output.is_absolute() else root / args.output
    try:
        sites = audit(root, args.scope)
        output.mkdir(parents=True, exist_ok=True)
        report = {"scope": args.scope, "counts": dict(Counter(site["kind"] for site in sites)), "sites": sites}
        (output / "runtime-sites.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        lines = [
            "# Runtime ownership review",
            "",
            "These source matches guide review; they do not classify a site as safe or unsafe.",
            "Check lifecycle ownership, bounded work, cancellation, and shutdown with the relevant Rust tests.",
            "",
            "| Kind | Source | Line |",
            "| --- | --- | ---: |",
            *(f"| {site['kind']} | {site['path']} | {site['line']} |" for site in sites),
            "",
        ]
        (output / "README.md").write_text("\n".join(lines), encoding="utf-8")
    except (OSError, ValueError) as error:
        print(f"RUNTIME_AUDIT_FAILED {error}", file=sys.stderr)
        return 1
    print(f"RUNTIME_AUDIT_OK sites={len(sites)} output={output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
