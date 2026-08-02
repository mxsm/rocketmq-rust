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

"""Verify a signed five-image publication before exporting its image map."""

from __future__ import annotations

import argparse
import base64
import binascii
import json
import re
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path, PurePosixPath
from typing import Any


SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")
SOURCE = "workflow://mxsm/rocketmq-rust/service-image-publish"
CATEGORY = "five_image_supply_chain"
CERTIFICATE_IDENTITY_REGEXP = (
    r"^https://github\.com/mxsm/rocketmq-rust/\.github/workflows/"
    r"service-image-publish\.yml@refs/(heads/main|tags/[A-Za-z0-9._-]+)$"
)
CERTIFICATE_OIDC_ISSUER = "https://token.actions.githubusercontent.com"
PREDICATE_TYPE = (
    "https://github.com/mxsm/rocketmq-rust/attestations/"
    "service-image-evidence/v1"
)
STATEMENT_TYPE = "https://in-toto.io/Statement/v1"
DSSE_PAYLOAD_TYPE = "application/vnd.in-toto+json"
TOOLS = {"syft": "v1.48.0", "trivy": "v0.72.0", "cosign": "v3.1.2"}
POLICY = {
    "critical_vulnerability_severity": "CRITICAL",
    "maximum_critical_findings": 0,
    "ignore_unfixed": False,
}
PUBLICATION_FIELDS = {
    "schema_version",
    "category",
    "fixture",
    "candidate_commit",
    "status",
    "source",
    "candidate",
    "workflow_run",
    "tools",
    "policy",
    "images",
    "services",
    "artifacts",
}
SERVICE_FIELDS = {
    "service",
    "image",
    "sbom",
    "vulnerability_scan",
    "signature",
    "attestation",
    "promotion",
}
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
WORKFLOW_RUN_RE = re.compile(
    r"^https://github\.com/mxsm/rocketmq-rust/actions/runs/[1-9][0-9]*$"
)
ARTIFACT_PATH_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
IMAGE_RE = re.compile(
    r"^ghcr\.io/mxsm/rocketmq-rust/"
    r"(?P<service>broker|namesrv|controller|proxy|mcp)@"
    r"(?P<digest>sha256:[0-9a-f]{64})$"
)


class VerificationError(ValueError):
    """Raised when publication evidence is incomplete or inconsistent."""


CosignRunner = Callable[[Sequence[str]], str]


def require(condition: bool, message: str) -> None:
    if not condition:
        raise VerificationError(message)


def require_sha256(value: object, label: str) -> str:
    require(isinstance(value, str) and SHA256_RE.fullmatch(value) is not None, f"{label} must be sha256")
    return str(value)


def require_record(value: object, fields: set[str], label: str) -> dict[str, Any]:
    require(type(value) is dict and set(value) == fields, f"{label} fields must match the exact schema")
    assert isinstance(value, dict)
    return value


def require_safe_artifact_path(value: object, label: str) -> str:
    require(isinstance(value, str) and bool(value), f"{label} must be non-empty")
    path = str(value)
    require("\\" not in path, f"{label} must use forward slashes")
    require(ARTIFACT_PATH_RE.fullmatch(path) is not None, f"{label} contains unsafe characters")
    parts = path.split("/")
    require(all(part not in ("", ".", "..") for part in parts), f"{label} must be normalized and relative")
    normalized = PurePosixPath(*parts)
    require(not normalized.is_absolute() and normalized.as_posix() == path, f"{label} must be normalized and relative")
    return path


def require_artifact_reference(
    artifacts: dict[str, str], path: object, sha256: object, label: str
) -> tuple[str, str]:
    normalized_path = require_safe_artifact_path(path, f"{label} path")
    normalized_sha256 = require_sha256(sha256, f"{label} hash")
    require(
        artifacts.get(normalized_path) == normalized_sha256,
        f"{label} path/hash is missing from the explicit artifact manifest",
    )
    return normalized_path, normalized_sha256


def json_exact_equal(actual: object, expected: object) -> bool:
    if type(actual) is not type(expected):
        return False
    if type(expected) is dict:
        assert isinstance(actual, dict) and isinstance(expected, dict)
        return set(actual) == set(expected) and all(
            json_exact_equal(actual[key], expected[key]) for key in expected
        )
    if type(expected) is list:
        assert isinstance(actual, list) and isinstance(expected, list)
        return len(actual) == len(expected) and all(
            json_exact_equal(actual_item, expected_item)
            for actual_item, expected_item in zip(actual, expected, strict=True)
        )
    return actual == expected


def validate_publication(
    publication: object, candidate: str
) -> tuple[
    dict[str, str],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    require(COMMIT_RE.fullmatch(candidate) is not None, "candidate must be a full lowercase Git SHA")
    publication = require_record(publication, PUBLICATION_FIELDS, "publication")
    require(type(publication.get("schema_version")) is int and publication["schema_version"] == 1, "publication schema_version must be integer 1")
    require(publication.get("status") == "pass", "publication status must be pass")
    require(publication.get("source") == SOURCE, "publication source is not the trusted publishing workflow")
    require(publication.get("category") == CATEGORY, "publication category must be five_image_supply_chain")
    require(publication.get("fixture") is False, "publication fixture must be false")
    require(publication.get("candidate_commit") == candidate, "publication candidate_commit differs from checkout")

    candidate_record = publication.get("candidate")
    candidate_record = require_record(
        candidate_record,
        {"source_commit", "source_version", "source_kind", "immutable_tag"},
        "publication candidate",
    )
    require(candidate_record.get("source_commit") == candidate, "publication candidate.source_commit differs from checkout")
    source_version = candidate_record.get("source_version")
    require(isinstance(source_version, str) and VERSION_RE.fullmatch(source_version) is not None, "publication source_version must be a semantic version")
    source_kind = candidate_record.get("source_kind")
    require(
        type(source_kind) is str
        and source_kind in {"protected-main-commit", "published-release-tag"},
        "publication source_kind is invalid",
    )
    immutable_tag = f"{source_version}-{candidate[:12]}"
    require(candidate_record.get("immutable_tag") == immutable_tag, "publication immutable_tag differs from candidate")
    workflow_run = publication.get("workflow_run")
    require(
        isinstance(workflow_run, str) and WORKFLOW_RUN_RE.fullmatch(workflow_run) is not None,
        "publication workflow_run is invalid",
    )

    tools = require_record(publication.get("tools"), set(TOOLS), "publication tools")
    require(json_exact_equal(tools, TOOLS), "publication tool versions do not match the trusted toolchain")
    policy = require_record(publication.get("policy"), set(POLICY), "publication policy")
    require(json_exact_equal(policy, POLICY), "publication policy does not match the zero-CRITICAL policy")

    artifacts = publication.get("artifacts")
    require(type(artifacts) is list and bool(artifacts), "publication artifacts must be a non-empty list")
    artifact_map: dict[str, str] = {}
    for index, artifact in enumerate(artifacts if isinstance(artifacts, list) else []):
        artifact = require_record(artifact, {"path", "sha256"}, f"publication artifact {index}")
        path = require_safe_artifact_path(artifact.get("path"), f"publication artifact {index} path")
        require(path not in artifact_map, f"publication artifact path is duplicated: {path}")
        artifact_map[path] = require_sha256(artifact.get("sha256"), f"publication artifact {path} hash")

    images = publication.get("images")
    require(type(images) is dict and set(images) == set(SERVICES), "publication images must contain exactly five services")
    assert isinstance(images, dict)
    normalized_images: dict[str, str] = {}
    image_digests: set[str] = set()
    for service in SERVICES:
        reference = images.get(service)
        match = IMAGE_RE.fullmatch(reference) if isinstance(reference, str) else None
        require(match is not None and match.group("service") == service, f"publication images.{service} is invalid")
        assert match is not None
        digest = match.group("digest")
        require(digest not in image_digests, f"publication image digest is duplicated: {digest}")
        image_digests.add(digest)
        normalized_images[service] = str(reference)

    services = publication.get("services")
    require(type(services) is list and len(services) == len(SERVICES), "publication services must contain exactly five entries")
    service_records: dict[str, dict[str, Any]] = {}
    for item in services if isinstance(services, list) else []:
        item = require_record(item, SERVICE_FIELDS, "publication service entry")
        service = item.get("service")
        require(type(service) is str and service in SERVICES, "publication service name is invalid")
        assert isinstance(service, str)
        require(service not in service_records, f"publication service is duplicated: {service}")
        service_records[service] = item
    require(set(service_records) == set(SERVICES), "publication services must identify exactly five services")

    canonical_predicates: dict[str, dict[str, Any]] = {}
    staging_tags: set[str] = set()
    for service in SERVICES:
        item = service_records[service]
        image = require_record(
            item.get("image"),
            {"repository", "tag", "staging_tag", "digest", "digest_reference"},
            f"publication {service} image",
        )
        reference = normalized_images[service]
        match = IMAGE_RE.fullmatch(reference)
        assert match is not None
        digest = match.group("digest")
        repository = f"ghcr.io/mxsm/rocketmq-rust/{service}"
        require(image.get("repository") == repository, f"publication {service} repository mismatch")
        require(image.get("tag") == immutable_tag, f"publication {service} immutable tag mismatch")
        staging_tag = image.get("staging_tag")
        require(
            isinstance(staging_tag, str)
            and re.fullmatch(rf"staging-{candidate[:12]}-[1-9][0-9]*-[1-9][0-9]*", staging_tag) is not None,
            f"publication {service} staging tag is invalid",
        )
        staging_tags.add(str(staging_tag))
        require(image.get("digest") == digest, f"publication {service} digest mismatch")
        require(image.get("digest_reference") == reference, f"publication {service} digest_reference mismatch")
        sbom = require_record(
            item.get("sbom"), {"path", "format", "sha256"}, f"publication {service} SBOM"
        )
        require(sbom.get("path") == f"sbom/{service}.cdx.json", f"publication {service} SBOM path mismatch")
        require(sbom.get("format") == "cyclonedx-json", f"publication {service} SBOM format mismatch")
        _, sbom_sha256 = require_artifact_reference(
            artifact_map, sbom.get("path"), sbom.get("sha256"), f"publication {service} SBOM"
        )

        vulnerability = require_record(
            item.get("vulnerability_scan"),
            {"path", "scanner", "severity", "ignore_unfixed", "critical_findings", "sha256"},
            f"publication {service} vulnerability scan",
        )
        require(vulnerability.get("path") == f"vulnerabilities/{service}.trivy.json", f"publication {service} vulnerability path mismatch")
        require(vulnerability.get("scanner") == "trivy", f"publication {service} vulnerability scanner mismatch")
        require(vulnerability.get("severity") == "CRITICAL", f"publication {service} vulnerability severity mismatch")
        require(vulnerability.get("ignore_unfixed") is False, f"publication {service} ignore_unfixed must be false")
        require(
            type(vulnerability.get("critical_findings")) is int
            and vulnerability["critical_findings"] == 0,
            f"publication {service} critical_findings must be integer zero",
        )
        _, vulnerability_sha256 = require_artifact_reference(
            artifact_map,
            vulnerability.get("path"),
            vulnerability.get("sha256"),
            f"publication {service} vulnerability scan",
        )

        signature = require_record(
            item.get("signature"),
            {"format", "bundle_path", "bundle_sha256", "verification_path", "verification_sha256"},
            f"publication {service} signature",
        )
        require(signature.get("format") == "sigstore-keyless", f"publication {service} signature format mismatch")
        require(signature.get("bundle_path") == f"signatures/{service}.bundle.json", f"publication {service} signature bundle path mismatch")
        require(signature.get("verification_path") == f"signatures/{service}.verification.json", f"publication {service} signature verification path mismatch")
        require_artifact_reference(
            artifact_map,
            signature.get("bundle_path"),
            signature.get("bundle_sha256"),
            f"publication {service} signature bundle",
        )
        require_artifact_reference(
            artifact_map,
            signature.get("verification_path"),
            signature.get("verification_sha256"),
            f"publication {service} signature verification",
        )

        attestation = require_record(
            item.get("attestation"),
            {
                "predicate_type",
                "predicate_path",
                "predicate_sha256",
                "bundle_path",
                "bundle_sha256",
                "verification_path",
                "verification_sha256",
                "matched_statement_path",
                "matched_statement_sha256",
            },
            f"publication {service} attestation",
        )
        require(attestation.get("predicate_type") == PREDICATE_TYPE, f"publication {service} predicate type mismatch")
        for field, expected_path in (
            ("predicate", f"attestations/{service}.predicate.json"),
            ("bundle", f"attestations/{service}.bundle.json"),
            ("verification", f"attestations/{service}.verification.jsonl"),
            ("matched_statement", f"attestations/{service}.matched-statement.json"),
        ):
            require(attestation.get(f"{field}_path") == expected_path, f"publication {service} attestation {field} path mismatch")
            require_artifact_reference(
                artifact_map,
                attestation.get(f"{field}_path"),
                attestation.get(f"{field}_sha256"),
                f"publication {service} attestation {field}",
            )

        promotion = require_record(
            item.get("promotion"),
            {"immutable_reference", "action", "raw_manifest_path", "raw_manifest_sha256"},
            f"publication {service} promotion",
        )
        require(promotion.get("immutable_reference") == f"{repository}:{immutable_tag}", f"publication {service} promotion reference mismatch")
        require(type(promotion.get("action")) is str and promotion["action"] in {"created", "reused"}, f"publication {service} promotion action is invalid")
        require(promotion.get("raw_manifest_path") == f"promotion/{service}.manifest.raw.json", f"publication {service} promotion manifest path mismatch")
        require(promotion.get("raw_manifest_sha256") == digest.removeprefix("sha256:"), f"publication {service} promotion manifest digest mismatch")
        require_artifact_reference(
            artifact_map,
            promotion.get("raw_manifest_path"),
            promotion.get("raw_manifest_sha256"),
            f"publication {service} promotion manifest",
        )

        canonical_predicates[service] = {
            "schema_version": 1,
            "source_commit": candidate,
            "source_version": source_version,
            "service": service,
            "image": {"reference": reference, "digest": digest},
            "sbom": {"format": "cyclonedx-json", "sha256": sbom_sha256},
            "vulnerability_scan": {
                "scanner": "trivy",
                "severity": "CRITICAL",
                "ignore_unfixed": False,
                "critical_findings": 0,
                "sha256": vulnerability_sha256,
            },
        }

    require(len(staging_tags) == 1, "publication services must share one staging tag")
    return normalized_images, service_records, canonical_predicates


def run_cosign(arguments: Sequence[str]) -> str:
    completed = subprocess.run(
        ["cosign", *arguments],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
        shell=False,
        timeout=180,
    )
    if completed.returncode != 0:
        command = " ".join(arguments[:2])
        raise VerificationError(f"cosign {command} failed: {completed.stderr.strip()}")
    return completed.stdout


def require_json_output(value: str, label: str) -> None:
    require(bool(value.strip()), f"{label} returned no JSON")
    decoder = json.JSONDecoder()
    offset = 0
    documents = 0
    while offset < len(value):
        while offset < len(value) and value[offset].isspace():
            offset += 1
        if offset == len(value):
            break
        try:
            _, offset = decoder.raw_decode(value, offset)
        except json.JSONDecodeError as error:
            raise VerificationError(f"{label} returned invalid JSON: {error}") from error
        documents += 1
    require(documents > 0, f"{label} returned no JSON documents")


def decode_attestation_statements(value: str) -> list[dict[str, Any]]:
    statements: list[dict[str, Any]] = []
    for line_number, line in enumerate(value.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            envelope = json.loads(line)
        except json.JSONDecodeError as error:
            raise VerificationError(f"attestation JSONL line {line_number} is invalid: {error}") from error
        require(isinstance(envelope, dict), f"attestation JSONL line {line_number} must be a DSSE object")
        require(
            envelope.get("payloadType") == DSSE_PAYLOAD_TYPE,
            f"attestation JSONL line {line_number} has an unexpected DSSE payloadType",
        )
        payload = envelope.get("payload")
        require(isinstance(payload, str) and bool(payload), f"attestation JSONL line {line_number} has no DSSE payload")
        try:
            padded_payload = payload + "=" * (-len(payload) % 4)
            decoded = base64.b64decode(padded_payload, validate=True).decode("utf-8")
            statement = json.loads(decoded)
        except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as error:
            raise VerificationError(f"attestation JSONL line {line_number} has an invalid DSSE payload: {error}") from error
        require(isinstance(statement, dict), f"attestation JSONL line {line_number} payload must be an object")
        statements.append(statement)
    require(bool(statements), "cosign verify-attestation returned no DSSE statements")
    return statements


def statement_matches(
    statement: dict[str, Any],
    *,
    repository: str,
    digest: str,
    predicate: dict[str, Any],
) -> bool:
    if type(statement) is not dict or set(statement) != {
        "_type",
        "subject",
        "predicateType",
        "predicate",
    }:
        return False
    subjects = statement.get("subject")
    subject_matches = (
        type(subjects) is list
        and len(subjects) == 1
        and type(subjects[0]) is dict
        and set(subjects[0]) == {"name", "digest"}
        and subjects[0].get("name") == repository
        and type(subjects[0].get("digest")) is dict
        and set(subjects[0]["digest"]) == {"sha256"}
        and subjects[0]["digest"].get("sha256") == digest.removeprefix("sha256:")
    )
    return bool(
        statement.get("_type") == STATEMENT_TYPE
        and statement.get("predicateType") == PREDICATE_TYPE
        and subject_matches
        and json_exact_equal(statement.get("predicate"), predicate)
    )


def verify_publication(publication: object, candidate: str, runner: CosignRunner = run_cosign) -> dict[str, str]:
    images, service_records, predicates = validate_publication(publication, candidate)

    for service in SERVICES:
        reference = images[service]
        digest = reference.rsplit("@", 1)[1]
        item = service_records[service]
        image = item["image"]
        assert isinstance(image, dict)
        signature_output = runner(
            (
                "verify",
                "--certificate-identity-regexp",
                CERTIFICATE_IDENTITY_REGEXP,
                "--certificate-oidc-issuer",
                CERTIFICATE_OIDC_ISSUER,
                "-a",
                f"org.opencontainers.image.revision={candidate}",
                "-a",
                f"io.rocketmq.service={service}",
                "--output",
                "json",
                reference,
            )
        )
        require_json_output(signature_output, f"cosign verify for {service}")

        attestation_output = runner(
            (
                "verify-attestation",
                "--certificate-identity-regexp",
                CERTIFICATE_IDENTITY_REGEXP,
                "--certificate-oidc-issuer",
                CERTIFICATE_OIDC_ISSUER,
                "--type",
                PREDICATE_TYPE,
                "--output",
                "json",
                reference,
            )
        )
        statements = decode_attestation_statements(attestation_output)
        require(
            any(
                statement_matches(
                    statement,
                    repository=str(image["repository"]),
                    digest=digest,
                    predicate=predicates[service],
                )
                for statement in statements
            ),
            f"no verified Statement/v1 matches {service}, its unique subject, and canonical predicate",
        )
    return images


def write_image_map(path: Path, images: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(images, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publication", type=Path, required=True)
    parser.add_argument("--candidate", required=True)
    parser.add_argument("--output-image-map", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        publication = json.loads(args.publication.read_text(encoding="utf-8"))
        images = verify_publication(publication, args.candidate)
        write_image_map(args.output_image_map, images)
    except (OSError, json.JSONDecodeError, VerificationError) as error:
        print(f"SERVICE_IMAGE_PUBLICATION_VERIFICATION_FAILED {error}", file=sys.stderr)
        return 1
    print(
        "SERVICE_IMAGE_PUBLICATION_VERIFIED "
        f"candidate={args.candidate} services={len(images)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
