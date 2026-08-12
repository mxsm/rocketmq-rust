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

"""Build and verify fail-closed message-path release evidence bundles."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "scripts" / "message-path-release-evidence-schema.json"
GIT_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
REQUIRED_RELEASE_DIRECTORIES = {"environment", "ab", "fault", "rpo", "soak", "qualification"}
INVENTORY_PATH = Path("qualification/artifact-inventory.json")
CHECKSUM_PATH = Path("qualification/bundle-sha256.txt")
SIGNATURE_PATH = Path("qualification/bundle-sha256.txt.minisig")
FORBIDDEN_PATH_TOKENS = {
    "secret",
    "secrets",
    "token",
    "tokens",
    "credential",
    "credentials",
    "password",
    "passwd",
    "private-key",
    "private_key",
}


class ReleaseError(RuntimeError):
    """Raised when release evidence is incomplete, mutable, or inconsistent."""


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReleaseError(f"cannot read JSON {path}: {error}") from error
    if not isinstance(value, dict):
        raise ReleaseError(f"JSON document must be an object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ReleaseError(message)


def required_object(value: Any, context: str) -> dict[str, Any]:
    require(isinstance(value, dict), f"{context} must be an object")
    return value


def validate_schema_contract(schema: dict[str, Any]) -> None:
    require(schema.get("$schema") == "https://json-schema.org/draft/2020-12/schema", "release schema draft differs")
    definitions = required_object(schema.get("$defs"), "release schema $defs")
    for name in ("rollback_evidence", "artifact_inventory"):
        definition = required_object(definitions.get(name), f"release schema $defs.{name}")
        require(definition.get("additionalProperties") is False, f"release schema {name} must be closed")
        require(isinstance(definition.get("required"), list) and definition["required"], f"release schema {name} required fields missing")


def validate_transition_proof(
    proof: dict[str, Any],
    checkpoint: dict[str, Any],
    target_state: dict[str, Any],
    direction: str,
) -> None:
    require(proof.get("schema_version") == 1, f"{direction} proof schema is invalid")
    require(proof.get("checkpoint_set_id") == checkpoint.get("checkpointSetId"), f"{direction} proof checkpoint differs")
    require(proof.get("target_release_id") == target_state.get("release_id"), f"{direction} proof target differs")
    require(proof.get("generation") == checkpoint.get("generation"), f"{direction} proof generation differs")
    require(proof.get("fencing_token") == checkpoint.get("fencingToken"), f"{direction} proof fence differs")
    for field in (
        "acknowledged_messages_preserved",
        "consumer_offsets_preserved",
        "wal_retained",
        "persistent_volumes_reused",
    ):
        require(proof.get(field) is True, f"{direction} proof did not establish {field}")
    stores = checkpoint.get("stores")
    require(isinstance(stores, list) and stores, f"{direction} checkpoint stores are missing")
    expected_ids = sorted(str(item.get("artifact", {}).get("checkpointId", "")) for item in stores)
    actual_ids = sorted(str(item) for item in proof.get("store_checkpoint_ids", []))
    require("" not in expected_ids and actual_ids == expected_ids, f"{direction} proof Store checkpoints differ")
    try:
        dt.datetime.fromisoformat(str(proof.get("verified_at", "")).replace("Z", "+00:00"))
    except ValueError as error:
        raise ReleaseError(f"{direction} proof verified_at is invalid") from error


def copy_bound_artifact(source: Path, destination_root: Path, name: str) -> dict[str, Any]:
    require(source.is_file(), f"bound artifact is missing: {source}")
    destination = destination_root / name
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.resolve() != destination.resolve():
        shutil.copy2(source, destination)
    return {"path": destination.relative_to(destination_root.parent).as_posix(), "sha256": sha256_file(destination)}


def build_rollback_evidence(args: argparse.Namespace) -> dict[str, Any]:
    candidate = read_json(args.candidate_measurement)
    baseline_state = read_json(args.baseline_state)
    candidate_state = read_json(args.candidate_state)
    rollback_checkpoint = read_json(args.rollback_checkpoint)
    forward_checkpoint = read_json(args.forward_checkpoint)
    rollback_proof = read_json(args.rollback_proof)
    forward_proof = read_json(args.forward_proof)
    subject = required_object(candidate.get("subject"), "candidate subject")
    target = required_object(candidate.get("target"), "candidate target")
    commit = str(subject.get("commit", ""))
    require(GIT_SHA_RE.fullmatch(commit) is not None, "candidate commit is invalid")
    require(subject.get("role") == "candidate", "measurement subject must be candidate")
    require(candidate.get("measurement_qualified") is True and candidate.get("status") == "pass", "candidate measurement is not qualified")
    require(candidate_state.get("source_commit") == commit, "candidate ReleaseState commit differs")
    require(candidate_state.get("identity", {}).get("commit") == commit, "candidate ReleaseState identity differs")
    require(candidate_state.get("identity", {}).get("config_digest") == target.get("effective_config_sha256"), "candidate ReleaseState config differs")
    require(baseline_state.get("source_commit") != commit, "baseline and candidate commits must differ")
    require(baseline_state.get("release_id") != candidate_state.get("release_id"), "baseline and candidate releases must differ")
    require(baseline_state.get("storage_generation") == candidate_state.get("storage_generation"), "storage generations differ")
    require(rollback_checkpoint.get("releaseId") == candidate_state.get("release_id"), "rollback checkpoint source differs")
    require(forward_checkpoint.get("releaseId") == baseline_state.get("release_id"), "forward checkpoint source differs")
    validate_transition_proof(rollback_proof, rollback_checkpoint, baseline_state, "rollback")
    validate_transition_proof(forward_proof, forward_checkpoint, candidate_state, "forward")

    rollback_log = args.rollback_log.read_text(encoding="utf-8")
    forward_log = args.forward_log.read_text(encoding="utf-8")
    rollback_marker = f"direction=Rollback target_release_id={baseline_state['release_id']}"
    forward_marker = f"direction=Forward target_release_id={candidate_state['release_id']}"
    require("RELEASE_ROLLBACK_OK" in rollback_log and rollback_marker in rollback_log, "rollback completion marker is missing")
    require("RELEASE_ROLLBACK_OK" in forward_log and forward_marker in forward_log, "forward completion marker is missing")

    output_root = args.output.parent
    artifact_root = output_root / "artifacts"
    bindings = (
        (args.baseline_state, "baseline-release-state.json"),
        (args.candidate_state, "candidate-release-state.json"),
        (args.rollback_checkpoint, "rollback-checkpoint-set.json"),
        (args.forward_checkpoint, "forward-checkpoint-set.json"),
        (args.rollback_proof, "rollback-preservation-proof.json"),
        (args.forward_proof, "forward-preservation-proof.json"),
        (args.rollback_log, "rollback.log"),
        (args.forward_log, "forward.log"),
    )
    artifacts = [copy_bound_artifact(source, artifact_root, name) for source, name in bindings]
    assertions = {
        "acknowledged_messages_preserved": True,
        "consumer_offsets_preserved": True,
        "wal_retained": True,
        "persistent_volumes_reused": True,
        "storage_generation_unchanged": True,
        "candidate_restored": True,
    }
    evidence = {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_rollback_evidence",
        "generated_at": utc_now(),
        "status": "pass",
        "rehearsal_qualified": True,
        "dynamic_execution": True,
        "fixture": False,
        "candidate_commit": commit,
        "candidate_measurement_sha256": "sha256:" + sha256_file(args.candidate_measurement),
        "deployment_digest": subject.get("deployment_digest"),
        "target_id": target.get("target_id"),
        "cluster_uid": target.get("cluster_uid"),
        "effective_config_sha256": target.get("effective_config_sha256"),
        "durability_contract": candidate.get("durability_contract"),
        "baseline_release_id": baseline_state.get("release_id"),
        "candidate_release_id": candidate_state.get("release_id"),
        "steps": [
            {
                "direction": "rollback",
                "status": "pass",
                "target_release_id": baseline_state.get("release_id"),
                "checkpoint_set_id": rollback_checkpoint.get("checkpointSetId"),
                "verified_at": rollback_proof.get("verified_at"),
            },
            {
                "direction": "forward",
                "status": "pass",
                "target_release_id": candidate_state.get("release_id"),
                "checkpoint_set_id": forward_checkpoint.get("checkpointSetId"),
                "verified_at": forward_proof.get("verified_at"),
            },
        ],
        "assertions": assertions,
        "artifacts": sorted(artifacts, key=lambda item: item["path"]),
    }
    for field in ("candidate_measurement_sha256", "deployment_digest", "effective_config_sha256"):
        require(DIGEST_RE.fullmatch(str(evidence.get(field, ""))) is not None, f"rollback {field} is invalid")
    for field in ("target_id", "cluster_uid", "durability_contract"):
        require(isinstance(evidence.get(field), str) and evidence[field], f"rollback {field} is missing")
    write_json(args.output, evidence)
    return evidence


def forbidden_evidence_path(relative: Path) -> bool:
    lowered = relative.as_posix().lower()
    tokens = set(re.split(r"[^a-z0-9_]+", lowered))
    return bool(tokens & FORBIDDEN_PATH_TOKENS) or lowered.endswith((".key", ".pem", ".p12", ".pfx"))


def evidence_files(root: Path) -> list[Path]:
    excluded = {INVENTORY_PATH.as_posix(), CHECKSUM_PATH.as_posix(), SIGNATURE_PATH.as_posix()}
    result: list[Path] = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if path.is_symlink():
            raise ReleaseError(f"evidence bundle contains a symbolic link: {relative.as_posix()}")
        if path.is_file() and relative.as_posix() not in excluded:
            if forbidden_evidence_path(relative):
                raise ReleaseError(f"evidence bundle contains a forbidden secret-like path: {relative.as_posix()}")
            result.append(path)
    return result


def inventory_entries(root: Path) -> list[dict[str, Any]]:
    return [
        {
            "path": path.relative_to(root).as_posix(),
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        for path in evidence_files(root)
    ]


def verify_release_documents(root: Path, qualification_relative: Path, rollback_relative: Path) -> tuple[str, dict[str, Any]]:
    qualification = read_json(root / qualification_relative)
    rollback = read_json(root / rollback_relative)
    require(qualification.get("artifact_kind") == "rocketmq_message_path_qualification_report", "qualification report kind differs")
    require(qualification.get("status") == "pass" and qualification.get("release_qualified") is True, "qualification report is NO-GO")
    require(rollback.get("artifact_kind") == "rocketmq_message_path_rollback_evidence", "rollback report kind differs")
    require(rollback.get("status") == "pass" and rollback.get("rehearsal_qualified") is True, "rollback rehearsal is not qualified")
    commit = str(qualification.get("candidate_commit", ""))
    require(GIT_SHA_RE.fullmatch(commit) is not None, "qualification candidate commit is invalid")
    require(rollback.get("candidate_commit") == commit, "rollback and qualification candidate commits differ")
    return commit, qualification


def package_bundle(args: argparse.Namespace) -> dict[str, Any]:
    source = args.source_root.resolve()
    archive = args.archive_output.resolve()
    require(source.is_dir(), f"release source root is missing: {source}")
    require(not archive.exists(), f"archive output already exists: {archive}")
    require(source not in archive.parents and archive not in source.parents, "source and archive paths overlap")
    root_entries = {path.name for path in source.iterdir()}
    require(root_entries == REQUIRED_RELEASE_DIRECTORIES, "release source root must contain exactly the canonical directories")
    evidence_files(source)
    shutil.copytree(source, archive, copy_function=shutil.copy2)
    commit, qualification = verify_release_documents(archive, args.qualification_report, args.rollback_evidence)
    artifacts = inventory_entries(archive)
    require(artifacts, "release bundle contains no evidence")
    bundle_digest = canonical_sha256(artifacts)
    inventory = {
        "schema_version": 1,
        "artifact_kind": "rocketmq_message_path_artifact_inventory",
        "generated_at": utc_now(),
        "candidate_commit": commit,
        "release_qualified": True,
        "bundle_sha256": bundle_digest,
        "qualification_report_sha256": sha256_file(archive / args.qualification_report),
        "artifact_count": len(artifacts),
        "artifacts": artifacts,
        "subject": qualification.get("subject"),
        "target": qualification.get("target"),
        "durability_contract": qualification.get("durability_contract"),
    }
    write_json(archive / INVENTORY_PATH, inventory)
    (archive / CHECKSUM_PATH).write_text(f"{bundle_digest}  rocketmq-message-path-release-bundle\n", encoding="utf-8")
    if args.minisign_secret_key is not None:
        command = [
            "minisign",
            "-S",
            "-s",
            str(args.minisign_secret_key.resolve()),
            "-m",
            str((archive / CHECKSUM_PATH).resolve()),
            "-x",
            str((archive / SIGNATURE_PATH).resolve()),
        ]
        result = subprocess.run(command, capture_output=True, text=True, check=False, shell=False)
        require(result.returncode == 0, f"minisign failed: {result.stderr.strip()}")
    if args.read_only:
        for path in archive.rglob("*"):
            if path.is_file():
                path.chmod(0o444)
    return inventory


def verify_bundle(args: argparse.Namespace) -> dict[str, Any]:
    root = args.bundle.resolve()
    inventory = read_json(root / INVENTORY_PATH)
    require(inventory.get("artifact_kind") == "rocketmq_message_path_artifact_inventory", "artifact inventory kind differs")
    artifacts = inventory.get("artifacts")
    require(isinstance(artifacts, list) and artifacts, "artifact inventory is empty")
    expected_paths = {str(item.get("path", "")) for item in artifacts if isinstance(item, dict)}
    actual = inventory_entries(root)
    require({item["path"] for item in actual} == expected_paths, "bundle files differ from the inventory")
    require(actual == artifacts, "bundle artifact hash or size differs")
    require(canonical_sha256(actual) == inventory.get("bundle_sha256"), "bundle aggregate SHA-256 differs")
    checksum = (root / CHECKSUM_PATH).read_text(encoding="utf-8").strip()
    require(checksum == f"{inventory['bundle_sha256']}  rocketmq-message-path-release-bundle", "bundle checksum differs")
    verify_release_documents(root, args.qualification_report, args.rollback_evidence)
    if args.minisign_public_key is not None:
        require((root / SIGNATURE_PATH).is_file(), "bundle minisign signature is missing")
        result = subprocess.run(
            [
                "minisign",
                "-V",
                "-p",
                str(args.minisign_public_key.resolve()),
                "-m",
                str((root / CHECKSUM_PATH).resolve()),
                "-x",
                str((root / SIGNATURE_PATH).resolve()),
            ],
            capture_output=True,
            text=True,
            check=False,
            shell=False,
        )
        require(result.returncode == 0, f"minisign verification failed: {result.stderr.strip()}")
    return inventory


def add_release_document_paths(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--qualification-report",
        type=Path,
        default=Path("qualification/qualification-report.json"),
    )
    parser.add_argument(
        "--rollback-evidence",
        type=Path,
        default=Path("qualification/rollback/rollback-evidence.json"),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("validate-schema")
    rollback = commands.add_parser("build-rollback-evidence")
    for name in (
        "candidate-measurement",
        "baseline-state",
        "candidate-state",
        "rollback-checkpoint",
        "forward-checkpoint",
        "rollback-proof",
        "forward-proof",
        "rollback-log",
        "forward-log",
        "output",
    ):
        rollback.add_argument("--" + name, type=Path, required=True)
    package = commands.add_parser("package")
    package.add_argument("--source-root", type=Path, required=True)
    package.add_argument("--archive-output", type=Path, required=True)
    package.add_argument("--minisign-secret-key", type=Path)
    package.add_argument("--read-only", action="store_true")
    add_release_document_paths(package)
    verify = commands.add_parser("verify")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--minisign-public-key", type=Path)
    add_release_document_paths(verify)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        schema = read_json(args.schema)
        validate_schema_contract(schema)
        if args.command == "validate-schema":
            print("MESSAGE_PATH_RELEASE_SCHEMA_OK")
            return 0
        if args.command == "build-rollback-evidence":
            evidence = build_rollback_evidence(args)
            print(json.dumps({"status": evidence["status"], "output": str(args.output.resolve())}, sort_keys=True))
            return 0
        if args.command == "package":
            inventory = package_bundle(args)
            print(json.dumps({"status": "pass", "bundle_sha256": inventory["bundle_sha256"]}, sort_keys=True))
            return 0
        inventory = verify_bundle(args)
        print(json.dumps({"status": "pass", "bundle_sha256": inventory["bundle_sha256"]}, sort_keys=True))
        return 0
    except (ReleaseError, OSError) as error:
        print(f"MESSAGE_PATH_RELEASE_ERROR: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
