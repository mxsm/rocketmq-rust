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

"""Assemble and validate a commit-bound production qualification evidence bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Any


CATEGORIES = (
    "performance",
    "ha_soak_rpo_rto",
    "disaster_recovery",
    "n_minus_one_rolling_upgrade",
    "five_image_supply_chain",
)
CATEGORY_SOURCES = {
    "performance": "architecture-performance-evidence",
    "ha_soak_rpo_rto": "architecture-slo-evidence",
    "disaster_recovery": "architecture-disaster-recovery-evidence",
    "n_minus_one_rolling_upgrade": "architecture-n-minus-one-rolling-upgrade-evidence",
    "five_image_supply_chain": "workflow://mxsm/rocketmq-rust/service-image-publish",
}
STATUSES = {"pass", "fail", "not-run"}
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
MANIFEST_FIELDS = {"schema_version", "candidate_commit", "status", "evidence"}
EVIDENCE_FIELDS = {
    "category",
    "status",
    "source",
    "fixture",
    "record_path",
    "sha256",
    "artifacts",
    "finding",
}
ARTIFACT_FIELDS = {"path", "sha256"}


class BundleError(ValueError):
    """Raised when a caller provides an unsafe or ambiguous bundle input."""


def paths_alias(left: Path, right: Path) -> bool:
    """Return whether two paths name the same target or existing file."""
    if left.resolve(strict=False) == right.resolve(strict=False):
        return True
    try:
        return left.samefile(right)
    except FileNotFoundError:
        return False


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    """Atomically replace JSON through a unique temporary sibling."""
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(value, indent=2, sort_keys=True) + "\n"
    descriptor, temporary_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        text=True,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as output:
            descriptor = -1
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
        raise


def safe_relative_path(root: Path, value: str, label: str) -> tuple[Path, str]:
    if not isinstance(value, str) or not value:
        raise BundleError(f"{label} must be a non-empty relative path")
    relative = Path(value)
    if relative.is_absolute() or relative.drive or ".." in relative.parts:
        raise BundleError(f"{label} must stay below the evidence root")
    resolved = (root / relative).resolve(strict=False)
    try:
        resolved.relative_to(root)
    except ValueError as error:
        raise BundleError(f"{label} must stay below the evidence root") from error
    return resolved, relative.as_posix()


def missing_item(category: str, record_path: str | None = None) -> dict[str, Any]:
    finding = "evidence record was not supplied" if record_path is None else "evidence record is missing"
    return {
        "category": category,
        "status": "not-run",
        "source": None,
        "fixture": None,
        "record_path": record_path,
        "sha256": None,
        "artifacts": [],
        "finding": finding,
    }


def inspect_artifacts(
    record: dict[str, Any],
    record_file: Path,
    status: object,
) -> tuple[list[dict[str, str]], set[Path], list[str]]:
    declared = record.get("artifacts")
    if declared is None:
        if status == "pass":
            return [], set(), ["pass evidence record must contain non-empty artifacts"]
        return [], set(), []
    if not isinstance(declared, list):
        return [], set(), ["evidence record artifacts must be a list"]
    if status == "pass" and not declared:
        return [], set(), ["pass evidence record must contain non-empty artifacts"]

    normalized: list[dict[str, str]] = []
    protected: set[Path] = set()
    findings: list[str] = []
    resolved_record = record_file.resolve(strict=False)
    record_parent = resolved_record.parent
    for index, artifact in enumerate(declared):
        label = f"artifacts[{index}]"
        if not isinstance(artifact, dict) or set(artifact) != ARTIFACT_FIELDS:
            findings.append(f"{label} must contain only path and sha256")
            continue
        relative = artifact.get("path")
        expected_hash = artifact.get("sha256")
        if not isinstance(relative, str) or not relative:
            findings.append(f"{label}.path must be a non-empty relative path")
            continue
        try:
            artifact_file, normalized_path = safe_relative_path(record_parent, relative, f"{label}.path")
        except BundleError as error:
            findings.append(str(error))
            continue
        if artifact_file == resolved_record:
            findings.append(f"{label}.path must not reference the evidence record")
            continue
        if artifact_file in protected:
            findings.append(f"duplicate artifact path: {normalized_path}")
            continue
        protected.add(artifact_file)
        normalized.append({"path": normalized_path, "sha256": str(expected_hash)})
        if not isinstance(expected_hash, str) or SHA256_RE.fullmatch(expected_hash) is None:
            findings.append(f"{label}.sha256 must be a lowercase SHA-256 digest")
            continue
        if not artifact_file.is_file():
            findings.append(f"missing artifact: {normalized_path}")
            continue
        try:
            actual_hash = hashlib.sha256(artifact_file.read_bytes()).hexdigest()
        except OSError:
            findings.append(f"artifact cannot be read: {normalized_path}")
            continue
        if actual_hash != expected_hash:
            findings.append(f"artifact hash mismatch: {normalized_path}")
    return normalized, protected, findings


def inspect_record(
    candidate: str,
    category: str,
    path: Path,
    record_path: str,
) -> tuple[dict[str, Any], set[Path]]:
    if not path.is_file():
        return missing_item(category, record_path), set()

    item: dict[str, Any] = {
        "category": category,
        "status": "fail",
        "source": None,
        "fixture": None,
        "record_path": record_path,
        "sha256": None,
        "artifacts": [],
        "finding": None,
    }
    try:
        encoded = path.read_bytes()
    except OSError:
        item["finding"] = "evidence record cannot be read"
        return item, set()
    item["sha256"] = hashlib.sha256(encoded).hexdigest()
    try:
        record = json.loads(encoded.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        item["finding"] = "evidence record is not valid UTF-8 JSON"
        return item, set()
    if not isinstance(record, dict):
        item["finding"] = "evidence record must be a JSON object"
        return item, set()

    findings: list[str] = []
    source = record.get("source")
    item["source"] = source if isinstance(source, str) and source.strip() else None
    fixture = record.get("fixture")
    item["fixture"] = fixture if type(fixture) is bool else None
    if record.get("schema_version") != 1:
        findings.append("evidence record schema_version must be 1")
    if record.get("category") != category:
        findings.append(f"evidence record category must be {category}")
    if source != CATEGORY_SOURCES[category]:
        findings.append(f"evidence record source must be {CATEGORY_SOURCES[category]}")
    if type(fixture) is not bool:
        findings.append("evidence record fixture must be a boolean")
    if record.get("candidate_commit") != candidate:
        findings.append("evidence record candidate_commit does not match the bundle")
    status = record.get("status")
    if not isinstance(status, str) or status not in STATUSES:
        findings.append("evidence record status must be pass, fail, or not-run")
    if fixture is True and status == "pass":
        findings.append("fixture evidence record cannot claim pass")

    artifacts, protected, artifact_findings = inspect_artifacts(record, path, status)
    item["artifacts"] = artifacts
    findings.extend(artifact_findings)
    if not findings:
        item["status"] = status
    item["finding"] = "; ".join(findings) or None
    return item, protected


def assemble_record(candidate: str, category: str, path: Path, record_path: str) -> dict[str, Any]:
    item, _ = inspect_record(candidate, category, path, record_path)
    return item


def overall_status(items: list[dict[str, Any]]) -> str:
    statuses = {item["status"] for item in items}
    if "fail" in statuses:
        return "fail"
    if statuses == {"pass"}:
        return "pass"
    return "not-run"


def parse_evidence_arguments(values: list[str]) -> dict[str, str]:
    evidence: dict[str, str] = {}
    for value in values:
        category, separator, record_path = value.partition("=")
        if not separator or category not in CATEGORIES or not record_path:
            allowed = ", ".join(CATEGORIES)
            raise BundleError(f"--evidence must be CATEGORY=PATH; categories: {allowed}")
        if category in evidence:
            raise BundleError(f"duplicate evidence category: {category}")
        evidence[category] = record_path
    return evidence


def assemble_with_protected_paths(
    candidate: str,
    evidence_root: Path,
    evidence: dict[str, str],
) -> tuple[dict[str, Any], set[Path]]:
    if COMMIT_RE.fullmatch(candidate) is None:
        raise BundleError("candidate must be a full lowercase 40-character Git SHA")
    root = evidence_root.resolve(strict=True)
    if not root.is_dir():
        raise BundleError("evidence root must be a directory")

    items: list[dict[str, Any]] = []
    protected: set[Path] = set()
    for category in CATEGORIES:
        supplied = evidence.get(category)
        if supplied is None:
            items.append(missing_item(category))
            continue
        path, normalized = safe_relative_path(root, supplied, f"evidence path for {category}")
        protected.add(path)
        item, artifacts = inspect_record(candidate, category, path, normalized)
        items.append(item)
        protected.update(artifacts)
    manifest = {
        "schema_version": 1,
        "candidate_commit": candidate,
        "status": overall_status(items),
        "evidence": items,
    }
    return manifest, protected


def assemble(candidate: str, evidence_root: Path, evidence: dict[str, str]) -> dict[str, Any]:
    manifest, _ = assemble_with_protected_paths(candidate, evidence_root, evidence)
    return manifest


def validate_manifest(value: Any, evidence_root: Path) -> list[str]:
    if not isinstance(value, dict):
        return ["manifest must be a JSON object"]
    if set(value) != MANIFEST_FIELDS:
        return ["manifest fields must be schema_version, candidate_commit, status, and evidence"]

    findings: list[str] = []
    candidate = value.get("candidate_commit")
    if value.get("schema_version") != 1:
        findings.append("manifest schema_version must be 1")
    if not isinstance(candidate, str) or COMMIT_RE.fullmatch(candidate) is None:
        findings.append("manifest candidate_commit must be a full lowercase Git SHA")
        return findings
    manifest_status = value.get("status")
    if not isinstance(manifest_status, str) or manifest_status not in STATUSES:
        findings.append("manifest status must be pass, fail, or not-run")

    items = value.get("evidence")
    if not isinstance(items, list) or len(items) != len(CATEGORIES):
        findings.append("manifest evidence must contain the five required categories")
        return findings

    root = evidence_root.resolve(strict=True)
    actual_categories = [item.get("category") if isinstance(item, dict) else None for item in items]
    if actual_categories != list(CATEGORIES):
        findings.append("manifest evidence categories or ordering are invalid")

    expected_items: list[dict[str, Any]] = []
    for index, (category, item) in enumerate(zip(CATEGORIES, items, strict=True)):
        if not isinstance(item, dict) or set(item) != EVIDENCE_FIELDS:
            findings.append(f"evidence[{index}] has invalid fields")
            continue
        record_path = item.get("record_path")
        if record_path is None:
            expected = missing_item(category)
        elif isinstance(record_path, str):
            try:
                path, normalized = safe_relative_path(root, record_path, f"evidence[{index}].record_path")
            except BundleError as error:
                findings.append(str(error))
                continue
            expected, _ = inspect_record(candidate, category, path, normalized)
        else:
            findings.append(f"evidence[{index}].record_path must be a string or null")
            continue
        expected_items.append(expected)
        if item != expected:
            findings.append(f"evidence[{index}] does not match its source record")

    if len(expected_items) == len(CATEGORIES):
        expected_status = overall_status(expected_items)
        if value.get("status") != expected_status:
            findings.append(f"manifest status must be {expected_status}")
    return findings


def load_manifest(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BundleError(f"cannot read manifest JSON: {error}") from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    assemble_parser = subparsers.add_parser("assemble", help="assemble a truthful evidence manifest")
    assemble_parser.add_argument("--candidate", required=True)
    assemble_parser.add_argument("--evidence-root", type=Path, default=Path.cwd())
    assemble_parser.add_argument("--evidence", action="append", default=[], metavar="CATEGORY=PATH")
    assemble_parser.add_argument("--output", required=True, help="relative path below --evidence-root")

    validate_parser = subparsers.add_parser("validate", help="validate a manifest and its referenced records")
    validate_parser.add_argument("--evidence-root", type=Path, default=Path.cwd())
    validate_parser.add_argument("--manifest", required=True, help="relative path below --evidence-root")
    validate_parser.add_argument("--require-pass", action="store_true")
    validate_parser.add_argument(
        "--require-category-pass",
        action="append",
        choices=CATEGORIES,
        default=[],
        metavar="CATEGORY",
        help="require a category to pass without requiring the other categories",
    )
    validate_parser.add_argument("--candidate", help="expected full candidate SHA for promotion")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        root = args.evidence_root.resolve(strict=True)
        if not root.is_dir():
            raise BundleError("evidence root must be a directory")
        if args.command == "assemble":
            evidence = parse_evidence_arguments(args.evidence)
            output, _ = safe_relative_path(root, args.output, "output path")
            manifest, protected = assemble_with_protected_paths(args.candidate, root, evidence)
            if any(paths_alias(output, path) for path in protected):
                raise BundleError("output path must not overwrite an evidence record or artifact")
            atomic_write_json(output, manifest)
            print(f"ARCHITECTURE_EVIDENCE_BUNDLE_WRITTEN status={manifest['status']} path={output}")
            return 0

        manifest_path, _ = safe_relative_path(root, args.manifest, "manifest path")
        manifest = load_manifest(manifest_path)
        findings = validate_manifest(manifest, root)
        if args.candidate is not None:
            if COMMIT_RE.fullmatch(args.candidate) is None:
                findings.append("expected candidate must be a full lowercase 40-character Git SHA")
            elif isinstance(manifest, dict) and manifest.get("candidate_commit") != args.candidate:
                findings.append("manifest candidate_commit does not match the expected candidate")
        if args.require_pass:
            if args.candidate is None:
                findings.append("--require-pass requires --candidate")
            if isinstance(manifest, dict) and manifest.get("status") != "pass":
                findings.append("production qualification requires manifest status pass")
        if args.require_category_pass:
            if args.candidate is None:
                findings.append("--require-category-pass requires --candidate")
            if isinstance(manifest, dict) and isinstance(manifest.get("evidence"), list):
                statuses = {
                    item.get("category"): item.get("status")
                    for item in manifest["evidence"]
                    if isinstance(item, dict)
                }
                for category in dict.fromkeys(args.require_category_pass):
                    if statuses.get(category) != "pass":
                        findings.append(f"category qualification requires {category} status pass")
        if findings:
            for finding in findings:
                print(f"ARCHITECTURE_EVIDENCE_BUNDLE_FINDING {finding}", file=sys.stderr)
            return 1
        print(f"ARCHITECTURE_EVIDENCE_BUNDLE_OK status={manifest['status']}")
        return 0
    except (BundleError, OSError) as error:
        print(f"ARCHITECTURE_EVIDENCE_BUNDLE_ERROR {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
