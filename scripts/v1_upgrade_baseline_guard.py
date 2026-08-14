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

"""Validate immutable v0.9.0 inputs used by the 1.0 upgrade tests."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
BASELINE_PATH = ROOT / "scripts" / "v1-upgrade-baseline.json"
EXPECTED_FIXTURES = frozenset({"localfile-store", "broker-metadata"})
EXPECTED_TRANSITIONS = frozenset({"multipath-commitlog", "extended-timer", "persistent-pop-profile"})
FORBIDDEN_FIELD_PARTS = ("sha", "hash", "digest", "checksum")


class BaselineInputError(ValueError):
    """Raised when the upgrade baseline cannot be loaded."""


@dataclass(frozen=True, order=True)
class BaselineFinding:
    code: str
    path: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return asdict(self)

    def render(self) -> str:
        return f"V1_UPGRADE_BASELINE_FINDING code={self.code} path={self.path} detail={self.detail}"


def load_baseline(path: Path = BASELINE_PATH) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BaselineInputError(f"cannot load {path}: {error}") from error
    if not isinstance(value, dict):
        raise BaselineInputError(f"{path} must contain a JSON object")
    return value


def _finding(findings: list[BaselineFinding], code: str, path: str, detail: str) -> None:
    findings.append(BaselineFinding(code, path, detail))


def _relative_path(value: object) -> bool:
    if not isinstance(value, str) or not value or "\\" in value or ":" in value:
        return False
    path = Path(value)
    return not path.is_absolute() and ".." not in path.parts


def _non_empty_strings(value: object) -> bool:
    return isinstance(value, list) and bool(value) and all(isinstance(item, str) and item for item in value)


def _validate_no_content_digests(value: object, path: str, findings: list[BaselineFinding]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(part in normalized for part in FORBIDDEN_FIELD_PARTS):
                _finding(findings, "content-digest-field-forbidden", f"{path}.{key}", "content digests are not gates")
            _validate_no_content_digests(child, f"{path}.{key}", findings)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_no_content_digests(child, f"{path}[{index}]", findings)


def _validate_fixture(
    fixture: dict[str, Any],
    *,
    root: Path,
    findings: list[BaselineFinding],
) -> None:
    fixture_id = fixture.get("fixture_id")
    path = str(fixture_id or "fixtures")
    if fixture.get("writer_version") != "0.9.0":
        _finding(findings, "writer-version-invalid", path, repr(fixture.get("writer_version")))
    fixture_root = fixture.get("root")
    if not _relative_path(fixture_root) or not str(fixture_root).endswith("/upgrade/v0.9.0"):
        _finding(findings, "fixture-root-invalid", path, repr(fixture_root))

    files = fixture.get("files")
    if not _non_empty_strings(files):
        _finding(findings, "fixture-files-invalid", path, repr(files))
    else:
        declared: set[str] = set()
        for item in files:
            if not _relative_path(item):
                _finding(findings, "fixture-path-invalid", path, repr(item))
                continue
            declared.add(item)
            if not (root / item).is_file():
                _finding(findings, "fixture-file-missing", path, item)
        if _relative_path(fixture_root) and (root / fixture_root).is_dir():
            actual = {
                file.relative_to(root).as_posix()
                for file in (root / fixture_root).rglob("*")
                if file.is_file()
            }
            if declared != actual:
                _finding(findings, "fixture-inventory-mismatch", path, f"declared={len(declared)} actual={len(actual)}")

    expected = fixture.get("expected_records")
    if not isinstance(expected, dict) or not expected:
        _finding(findings, "expected-records-missing", path, "expected_records must be a non-empty object")
    expected_path = fixture.get("expected_records_file")
    if not _relative_path(expected_path) or not (root / str(expected_path)).is_file():
        _finding(findings, "expected-records-file-invalid", path, repr(expected_path))
    else:
        try:
            file_expected = json.loads((root / str(expected_path)).read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            _finding(findings, "expected-records-file-invalid", path, str(error))
        else:
            if file_expected != expected:
                _finding(findings, "expected-records-mismatch", path, "inline and fixture records differ")
    if not _non_empty_strings(fixture.get("operations")):
        _finding(findings, "fixture-operations-missing", path, "operations")


def validate_baseline(
    baseline: dict[str, Any],
    *,
    root: Path = ROOT,
) -> list[BaselineFinding]:
    findings: list[BaselineFinding] = []
    if baseline.get("schema_version") != 1 or baseline.get("release_line") != "1.0":
        _finding(findings, "baseline-schema-invalid", "baseline", "schema_version=1 release_line=1.0 required")

    upgrade_from = baseline.get("upgrade_from")
    if not isinstance(upgrade_from, dict) or upgrade_from.get("version") != "0.9.0" or upgrade_from.get("tag") != "v0.9.0":
        _finding(findings, "writer-version-invalid", "upgrade_from", repr(upgrade_from))
    elif upgrade_from.get("writer") != "rocketmq-rust" or not upgrade_from.get("platform"):
        _finding(findings, "writer-metadata-invalid", "upgrade_from", repr(upgrade_from))

    generation = baseline.get("generation")
    if not isinstance(generation, dict):
        _finding(findings, "generation-invalid", "generation", "object required")
    else:
        commands = generation.get("commands")
        if (
            not _non_empty_strings(commands)
            or not all("v0.9.0" in command for command in commands)
            or any("cargo run --release" in command for command in commands)
        ):
            _finding(findings, "generation-command-invalid", "generation", repr(commands))
        if generation.get("regeneration_policy") != "read-only-test-input":
            _finding(findings, "regeneration-policy-invalid", "generation", repr(generation.get("regeneration_policy")))
        if not isinstance(generation.get("configuration"), dict) or not generation["configuration"]:
            _finding(findings, "generation-config-missing", "generation", "configuration")
        if not isinstance(generation.get("owner"), str) or not generation["owner"]:
            _finding(findings, "generation-owner-missing", "generation", "owner")

    fixtures = baseline.get("fixtures")
    if not isinstance(fixtures, list) or any(not isinstance(item, dict) for item in fixtures):
        _finding(findings, "fixtures-invalid", "fixtures", "object list required")
    else:
        fixture_ids = [item.get("fixture_id") for item in fixtures]
        if set(fixture_ids) != EXPECTED_FIXTURES or len(fixture_ids) != len(set(fixture_ids)):
            _finding(findings, "fixture-set-invalid", "fixtures", repr(fixture_ids))
        for fixture in fixtures:
            _validate_fixture(fixture, root=root, findings=findings)

    transitions = baseline.get("format_transitions")
    if not isinstance(transitions, list) or any(not isinstance(item, dict) for item in transitions):
        _finding(findings, "format-transitions-invalid", "format_transitions", "object list required")
    else:
        names = [item.get("format") for item in transitions]
        if set(names) != EXPECTED_TRANSITIONS or len(names) != len(set(names)):
            _finding(findings, "format-transition-set-invalid", "format_transitions", repr(names))
        for transition in transitions:
            path = str(transition.get("format") or "format_transitions")
            if transition.get("baseline_state") != "legacy-absent" or transition.get("upgrade") != "initialize-current-format":
                _finding(findings, "upgrade-contract-invalid", path, repr(transition))
            if transition.get("downgrade") != "reject-by-1.0-preflight" or transition.get("old_reader_awareness") != "not-assumed":
                _finding(findings, "downgrade-contract-unsafe", path, repr(transition))

    change_control = baseline.get("change_control")
    if (
        not isinstance(change_control, dict)
        or any(not isinstance(change_control.get(key), str) or not change_control[key] for key in ("store_reviewer", "broker_reviewer", "reason"))
        or change_control.get("store_reviewer") == change_control.get("broker_reviewer")
    ):
        _finding(findings, "change-control-invalid", "change_control", repr(change_control))

    _validate_no_content_digests(baseline, "baseline", findings)
    return sorted(set(findings))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, default=BASELINE_PATH)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        baseline = load_baseline(args.baseline)
    except BaselineInputError as error:
        print(f"V1_UPGRADE_BASELINE_INPUT_ERROR {error}", file=sys.stderr)
        return 2
    findings = validate_baseline(baseline)
    if findings:
        for finding in findings:
            print(finding.render())
        print(f"V1_UPGRADE_BASELINE_FAILED findings={len(findings)}", file=sys.stderr)
        return 1
    print(
        "V1_UPGRADE_BASELINE_OK "
        f"writer={baseline['upgrade_from']['version']} fixtures={len(baseline['fixtures'])} "
        f"transitions={len(baseline['format_transitions'])}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
