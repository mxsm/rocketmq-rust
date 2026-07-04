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
import dataclasses
import re
import sys
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
RUST_SUFFIX = ".rs"


@dataclasses.dataclass(frozen=True)
class Finding:
    path: Path
    line: int
    message: str

    def render(self) -> str:
        rel = self.path.relative_to(ROOT).as_posix()
        return f"{rel}:{self.line}: {self.message}"


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def rust_files_under(*parts: str) -> list[Path]:
    root = ROOT.joinpath(*parts)
    if not root.exists():
        return []
    return sorted(path for path in root.rglob(f"*{RUST_SUFFIX}") if "target" not in path.parts)


def is_test_context(path: Path, line_number: int) -> bool:
    """Best-effort detector for unit-test modules inside Rust source files."""
    rel_parts = path.relative_to(ROOT).parts
    if "tests" in rel_parts or "benches" in rel_parts or "examples" in rel_parts:
        return True
    if "src" in rel_parts and "bin" in rel_parts:
        return True

    depth = 0
    test_module_depth: int | None = None
    pending_test_cfg = False
    for current_line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if stripped == "#[cfg(test)]":
            pending_test_cfg = True
        elif pending_test_cfg and re.match(r"(pub\([^)]*\)\s+|pub\s+)?mod\s+tests\b", stripped):
            test_module_depth = depth + stripped.count("{") - stripped.count("}")
            pending_test_cfg = False
        elif stripped and not stripped.startswith("#"):
            pending_test_cfg = False

        if current_line_number == line_number:
            return test_module_depth is not None

        depth += line.count("{") - line.count("}")
        if test_module_depth is not None and depth < test_module_depth:
            test_module_depth = None
    return False


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


def check_error_and_common_public_surface() -> list[Finding]:
    forbidden = {
        "RocketmqError": "legacy RocketmqError must not re-enter core public error code",
        "RocketMqError": "legacy RocketMqError spelling must not re-enter core public error code",
        "rocketmq_error::Result": "old rocketmq_error::Result alias must not be used",
        "anyhow::Result": "rocketmq-error/common must not expose public anyhow Result",
        "anyhow::Error": "rocketmq-error/common must not expose anyhow Error",
        "pub type Result": "rocketmq-error/common must use typed crate-local aliases, not generic public Result",
    }
    return scan_forbidden_terms(
        [*rust_files_under("rocketmq-error", "src"), *rust_files_under("rocketmq-common", "src")],
        forbidden,
    )


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


def check_required_mapping_adapters() -> list[Finding]:
    checks = {
        ROOT / "rocketmq-remoting" / "src" / "error_response.rs": [
            "error.spec().remoting.code.as_i32()",
            "request_code_not_supported_with_remark",
            "internal_error_with_opaque",
        ],
        ROOT / "rocketmq-proxy" / "src" / "status.rs": [
            "error.spec().grpc",
            "grpc_payload_to_code",
            "grpc_status_to_tonic_code",
        ],
    }
    findings: list[Finding] = []
    for path, needles in checks.items():
        if not path.exists():
            findings.append(Finding(path, 1, "required error boundary adapter file is missing"))
            continue
        text = read_text(path)
        for needle in needles:
            if needle not in text:
                findings.append(Finding(path, 1, f"required mapping adapter token missing: {needle}"))
    return findings


def check_redaction_guards() -> list[Finding]:
    required_tokens = {
        ROOT / "rocketmq-error" / "src" / "context.rs": [
            'pub const REDACTED: &str = "<redacted>";',
            'f.write_str("Sensitive(<redacted>)")',
        ],
        ROOT / "rocketmq-error" / "tests" / "error_context_redaction.rs": [
            "secret_key=<redacted>",
            "token=<redacted>",
            "error_context_redacts_sensitive_fields",
        ],
        ROOT / "rocketmq-common" / "src" / "common" / "base" / "plain_access_config.rs": [
            "debug_and_display_redact_secret_key",
            "<redacted>",
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

    sensitive_field_pattern = re.compile(r'\.field\(".*(secret|password|token|signature).*",', re.I)
    for path in [
        *rust_files_under("rocketmq-error", "src"),
        *rust_files_under("rocketmq-common", "src"),
        *rust_files_under("rocketmq-client", "src"),
        *rust_files_under("rocketmq-remoting", "src"),
    ]:
        for line_number, line in enumerate(read_text(path).splitlines(), start=1):
            if is_test_context(path, line_number):
                continue
            if (
                sensitive_field_pattern.search(line)
                and "<redacted>" not in line
                and "REDACTED" not in line
                and "redacted_value" not in line
            ):
                findings.append(Finding(path, line_number, "sensitive Debug field must be explicitly redacted"))
    return findings


def run() -> int:
    checks = [
        ("core public surface", check_error_and_common_public_surface),
        ("processor boundary mappings", check_processor_boundary_mappings),
        ("required mapping adapters", check_required_mapping_adapters),
        ("redaction guards", check_redaction_guards),
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
