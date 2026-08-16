#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Apply and validate the semantic Phase 6 capability freeze."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DISTRIBUTION = ROOT / "distribution"
if str(DISTRIBUTION) not in sys.path:
    sys.path.insert(0, str(DISTRIBUTION))

from release_state import ensure_no_digest_fields


MANIFEST = ROOT / "scripts" / "v1-capability-manifest.json"
FREEZE = ROOT / "scripts" / "v1-capability-freeze.json"
ACTIVE_IDS = frozenset(
    {f"F-{number:02d}" for number in range(1, 19)}
    | {f"G-{number:02d}" for number in range(1, 7)}
)
DEFERRED_IDS = frozenset({"G-07", "G-08"})
FREEZE_ARTIFACT = "scripts/v1-capability-freeze.json"


class FreezeError(ValueError):
    """Raised when the capability freeze cannot be applied safely."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FreezeError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise FreezeError(f"{path} must contain a JSON object")
    ensure_no_digest_fields(value)
    return value


def validate_freeze(
    manifest: dict[str, Any], contract: dict[str, Any], *, root: Path = ROOT
) -> list[str]:
    findings: list[str] = []
    active_contract = contract.get("active_capabilities")
    if contract.get("schema_version") != 1 or contract.get("release_line") != "1.0":
        findings.append("freeze schema/release line is invalid")
    if contract.get("evidence_basis") != "merged-focused-validation":
        findings.append("freeze evidence basis is not approved")
    if contract.get("remote_publication") != "not-executed":
        findings.append("freeze must not claim or execute remote publication")
    if not isinstance(active_contract, dict) or set(active_contract) != ACTIVE_IDS:
        findings.append("active capability freeze denominator is not F-01..F-18 plus G-01..G-06")
        active_contract = active_contract if isinstance(active_contract, dict) else {}
    run_id = contract.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        findings.append("freeze run ID is missing")
    capabilities = manifest.get("capabilities")
    records = {
        item.get("capability_id"): item
        for item in capabilities
        if isinstance(capabilities, list) and isinstance(item, dict)
    } if isinstance(capabilities, list) else {}
    for capability_id in sorted(ACTIVE_IDS):
        record = records.get(capability_id)
        frozen = active_contract.get(capability_id)
        if not isinstance(record, dict) or not isinstance(frozen, dict):
            findings.append(f"{capability_id} freeze record is missing")
            continue
        expected_completion = frozen.get("completion_status")
        allowed_completion = (
            "alternative-equivalent"
            if record.get("compatibility_mode") == "rust-native"
            else "equivalent"
        )
        if expected_completion != allowed_completion:
            findings.append(f"{capability_id} completion decision is inconsistent with its mode")
        if record.get("completion_status") != expected_completion:
            findings.append(f"{capability_id} completion status is not frozen")
        if record.get("implementation_status") != "implemented":
            findings.append(f"{capability_id} implementation status is not implemented")
        expected_evidence = (
            "functional-system"
            if record.get("compatibility_mode") == "rust-native"
            else "interop"
            if record.get("compatibility_mode") == "wire"
            else "component"
        )
        if record.get("evidence_status") != expected_evidence:
            findings.append(f"{capability_id} evidence status is not {expected_evidence}")
        expected_artifact = [{"path": FREEZE_ARTIFACT, "run_id": run_id}]
        if record.get("artifacts") != expected_artifact:
            findings.append(f"{capability_id} evidence artifact is not bound to this freeze")
        pull_requests = frozen.get("implementation_prs")
        if (
            not isinstance(pull_requests, list)
            or not pull_requests
            or any(not isinstance(number, int) or isinstance(number, bool) or number < 1 for number in pull_requests)
        ):
            findings.append(f"{capability_id} implementation PR evidence is missing")
        if not record.get("test_ids") or len(record.get("test_ids", [])) != len(record.get("commands", [])):
            findings.append(f"{capability_id} executable test denominator is incomplete")
    deferred = contract.get("deferred_capabilities")
    if not isinstance(deferred, dict) or set(deferred) != DEFERRED_IDS:
        findings.append("deferred capability denominator is not G-07/G-08")
    for capability_id in sorted(DEFERRED_IDS):
        record = records.get(capability_id)
        if (
            not isinstance(record, dict)
            or record.get("completion_status") != "deferred-by-scope"
            or record.get("evidence_status") != "none"
            or record.get("artifacts") != []
            or record.get("deferred_reference") != (deferred or {}).get(capability_id)
        ):
            findings.append(f"{capability_id} is not an explicit unevidenced deferral")
    admin = contract.get("admin_operation_denominator")
    if admin != {"raw": 96, "excluded": 2, "active": 94}:
        findings.append("Admin operation denominator is not raw=96/excluded=2/active=94")
    proxy = contract.get("proxy_route_policy")
    if not isinstance(proxy, dict) or proxy.get("active_to_unsupported_allowed") is not False:
        findings.append("Proxy active routes may not be classified Unsupported")
    expected_exclusions = {
        "OpenMessaging",
        "BrokerContainer",
        "DLedger CommitLog",
        "Java Controller internal protocols",
    }
    if set(contract.get("excluded_capabilities", [])) != expected_exclusions:
        findings.append("freeze exclusion denominator drifted")
    if not (root / FREEZE_ARTIFACT).is_file():
        findings.append("freeze evidence artifact does not exist")
    ensure_no_digest_fields(manifest)
    ensure_no_digest_fields(contract)
    return sorted(set(findings))


def _replace_once(section: str, pattern: str, replacement: str, capability_id: str) -> str:
    updated, count = re.subn(pattern, replacement, section, count=1, flags=re.DOTALL)
    if count != 1:
        raise FreezeError(f"cannot update {capability_id}: pattern {pattern!r} matched {count} times")
    return updated


def apply_freeze(manifest_path: Path = MANIFEST, freeze_path: Path = FREEZE) -> None:
    manifest = _read(manifest_path)
    contract = _read(freeze_path)
    active = contract.get("active_capabilities")
    if not isinstance(active, dict) or set(active) != ACTIVE_IDS:
        raise FreezeError("refusing to apply an incomplete active capability freeze")
    text = manifest_path.read_text(encoding="utf-8")
    starts = {
        match.group(1): match.start()
        for match in re.finditer(r'^    \{\r?\n      "capability_id": "([FG]-\d{2})"', text, re.MULTILINE)
    }
    exclusion_start = text.find('  "exclusions": [')
    if set(starts) != ACTIVE_IDS | DEFERRED_IDS or exclusion_start < 0:
        raise FreezeError("manifest capability object boundaries are not closed")
    ordered = sorted(starts.items(), key=lambda item: item[1])
    boundaries = {
        capability_id: (start, ordered[index + 1][1] if index + 1 < len(ordered) else exclusion_start)
        for index, (capability_id, start) in enumerate(ordered)
    }
    run_id = contract["run_id"]
    for capability_id in sorted(ACTIVE_IDS, key=lambda item: boundaries[item][0], reverse=True):
        start, end = boundaries[capability_id]
        section = text[start:end]
        completion = active[capability_id]["completion_status"]
        capability = next(
            item for item in manifest["capabilities"] if item.get("capability_id") == capability_id
        )
        evidence = (
            "functional-system"
            if capability.get("compatibility_mode") == "rust-native"
            else "interop"
            if capability.get("compatibility_mode") == "wire"
            else "component"
        )
        section = _replace_once(
            section,
            r'"implementation_status": "[^"]+"',
            '"implementation_status": "implemented"',
            capability_id,
        )
        section = _replace_once(
            section,
            r'"evidence_status": "[^"]+"',
            f'"evidence_status": "{evidence}"',
            capability_id,
        )
        artifact = json.dumps([{"path": FREEZE_ARTIFACT, "run_id": run_id}])
        section = _replace_once(
            section,
            r'"artifacts": .*?(?=,\r?\n      "ownership")',
            f'"artifacts": {artifact}',
            capability_id,
        )
        section = _replace_once(
            section,
            r'"completion_status": "[^"]+"',
            f'"completion_status": "{completion}"',
            capability_id,
        )
        text = text[:start] + section + text[end:]
    temporary = manifest_path.with_name(f".{manifest_path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    try:
        temporary.write_text(text, encoding="utf-8", newline="\n")
        os.replace(temporary, manifest_path)
    finally:
        temporary.unlink(missing_ok=True)
    updated = _read(manifest_path)
    findings = validate_freeze(updated, contract, root=ROOT)
    if findings:
        raise FreezeError("applied freeze is invalid: " + "; ".join(findings))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--apply", action="store_true")
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    parser.add_argument("--freeze", type=Path, default=FREEZE)
    args = parser.parse_args(argv)
    try:
        if args.apply:
            apply_freeze(args.manifest, args.freeze)
        manifest = _read(args.manifest)
        contract = _read(args.freeze)
        findings = validate_freeze(manifest, contract, root=ROOT)
    except (FreezeError, ValueError) as error:
        print(f"V1_CAPABILITY_FREEZE_FAILED detail={error}", file=sys.stderr)
        return 1
    if findings:
        print(f"V1_CAPABILITY_FREEZE_FAILED findings={len(findings)}")
        for finding in findings:
            print(f"V1_CAPABILITY_FREEZE_FINDING detail={finding}")
        return 1
    print(f"V1_CAPABILITY_FREEZE_OK active={len(ACTIVE_IDS)} deferred={len(DEFERRED_IDS)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
