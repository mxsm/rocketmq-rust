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

"""Focused tests for signed service-image publication consumption."""

from __future__ import annotations

import base64
import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest import mock


SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

import verify_service_image_publication as verifier  # noqa: E402


CANDIDATE = "0123456789abcdef0123456789abcdef01234567"
VERSION = "0.7.0"
DELETE = object()


def digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def sha256(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def build_publication() -> dict[str, Any]:
    images: dict[str, str] = {}
    services: list[dict[str, Any]] = []
    artifacts: list[dict[str, str]] = []

    def record_artifact(path: str, value: str | None = None) -> str:
        artifact_sha256 = value or sha256(path)
        artifacts.append({"path": path, "sha256": artifact_sha256})
        return artifact_sha256

    immutable_tag = f"{VERSION}-{CANDIDATE[:12]}"
    staging_tag = f"staging-{CANDIDATE[:12]}-123-1"
    for service in verifier.SERVICES:
        image_digest = digest(service)
        repository = f"ghcr.io/mxsm/rocketmq-rust/{service}"
        reference = f"{repository}@{image_digest}"
        sbom_path = f"sbom/{service}.cdx.json"
        vulnerability_path = f"vulnerabilities/{service}.trivy.json"
        signature_bundle_path = f"signatures/{service}.bundle.json"
        signature_verification_path = f"signatures/{service}.verification.json"
        predicate_path = f"attestations/{service}.predicate.json"
        attestation_bundle_path = f"attestations/{service}.bundle.json"
        attestation_verification_path = f"attestations/{service}.verification.jsonl"
        matched_statement_path = f"attestations/{service}.matched-statement.json"
        raw_manifest_path = f"promotion/{service}.manifest.raw.json"
        sbom_sha256 = record_artifact(sbom_path)
        vulnerability_sha256 = record_artifact(vulnerability_path)
        signature_bundle_sha256 = record_artifact(signature_bundle_path)
        signature_verification_sha256 = record_artifact(signature_verification_path)
        predicate_sha256 = record_artifact(predicate_path)
        attestation_bundle_sha256 = record_artifact(attestation_bundle_path)
        attestation_verification_sha256 = record_artifact(attestation_verification_path)
        matched_statement_sha256 = record_artifact(matched_statement_path)
        raw_manifest_sha256 = record_artifact(
            raw_manifest_path, image_digest.removeprefix("sha256:")
        )
        images[service] = reference
        services.append(
            {
                "service": service,
                "image": {
                    "repository": repository,
                    "tag": immutable_tag,
                    "staging_tag": staging_tag,
                    "digest": image_digest,
                    "digest_reference": reference,
                },
                "sbom": {
                    "path": sbom_path,
                    "format": "cyclonedx-json",
                    "sha256": sbom_sha256,
                },
                "vulnerability_scan": {
                    "path": vulnerability_path,
                    "scanner": "trivy",
                    "severity": "CRITICAL",
                    "ignore_unfixed": False,
                    "critical_findings": 0,
                    "sha256": vulnerability_sha256,
                },
                "signature": {
                    "format": "sigstore-keyless",
                    "bundle_path": signature_bundle_path,
                    "bundle_sha256": signature_bundle_sha256,
                    "verification_path": signature_verification_path,
                    "verification_sha256": signature_verification_sha256,
                },
                "attestation": {
                    "predicate_type": verifier.PREDICATE_TYPE,
                    "predicate_path": predicate_path,
                    "predicate_sha256": predicate_sha256,
                    "bundle_path": attestation_bundle_path,
                    "bundle_sha256": attestation_bundle_sha256,
                    "verification_path": attestation_verification_path,
                    "verification_sha256": attestation_verification_sha256,
                    "matched_statement_path": matched_statement_path,
                    "matched_statement_sha256": matched_statement_sha256,
                },
                "promotion": {
                    "immutable_reference": f"{repository}:{immutable_tag}",
                    "action": "created",
                    "raw_manifest_path": raw_manifest_path,
                    "raw_manifest_sha256": raw_manifest_sha256,
                },
            }
        )
    return {
        "schema_version": 1,
        "candidate_commit": CANDIDATE,
        "status": "pass",
        "source": verifier.SOURCE,
        "category": verifier.CATEGORY,
        "fixture": False,
        "candidate": {
            "source_commit": CANDIDATE,
            "source_version": VERSION,
            "source_kind": "protected-main-commit",
            "immutable_tag": immutable_tag,
        },
        "workflow_run": "https://github.com/mxsm/rocketmq-rust/actions/runs/123",
        "tools": dict(verifier.TOOLS),
        "policy": dict(verifier.POLICY),
        "images": images,
        "services": services,
        "artifacts": artifacts,
    }


def attestation_line(publication: dict[str, Any], service: str, **overrides: Any) -> str:
    entry = next(item for item in publication["services"] if item["service"] == service)
    reference = publication["images"][service]
    image_digest = reference.rsplit("@sha256:", 1)[1]
    statement: dict[str, Any] = {
        "_type": verifier.STATEMENT_TYPE,
        "subject": [
            {
                "name": entry["image"]["repository"],
                "digest": {"sha256": image_digest},
            }
        ],
        "predicateType": verifier.PREDICATE_TYPE,
        "predicate": {
            "schema_version": 1,
            "source_commit": CANDIDATE,
            "source_version": VERSION,
            "service": service,
            "image": {"reference": reference, "digest": f"sha256:{image_digest}"},
            "sbom": {
                "format": "cyclonedx-json",
                "sha256": entry["sbom"]["sha256"],
            },
            "vulnerability_scan": {
                "scanner": "trivy",
                "severity": "CRITICAL",
                "ignore_unfixed": False,
                "critical_findings": 0,
                "sha256": entry["vulnerability_scan"]["sha256"],
            },
        },
    }
    for path, value in overrides.items():
        target: Any = statement
        segments = path.split("__")
        for segment in segments[:-1]:
            target = target[segment]
        if value is DELETE:
            del target[segments[-1]]
        else:
            target[segments[-1]] = value
    payload = base64.b64encode(json.dumps(statement, separators=(",", ":")).encode()).decode()
    return json.dumps(
        {
            "payloadType": verifier.DSSE_PAYLOAD_TYPE,
            "payload": payload,
            "signatures": [],
        }
    )


class ServiceImagePublicationVerifierTests(unittest.TestCase):
    def test_valid_publication_verifies_all_services_and_annotations(self) -> None:
        publication = build_publication()
        calls: list[tuple[str, ...]] = []

        def runner(arguments) -> str:  # type: ignore[no-untyped-def]
            call = tuple(arguments)
            calls.append(call)
            service = call[-1].split("/")[-1].split("@")[0]
            if call[0] == "verify":
                return json.dumps([{"verified": True}])
            return attestation_line(publication, service) + "\n"

        images = verifier.verify_publication(publication, CANDIDATE, runner)

        self.assertEqual(publication["images"], images)
        self.assertEqual(10, len(calls))
        for service in verifier.SERVICES:
            signature = next(call for call in calls if call[0] == "verify" and call[-1].endswith(digest(service)))
            self.assertIn(verifier.CERTIFICATE_IDENTITY_REGEXP, signature)
            self.assertIn(verifier.CERTIFICATE_OIDC_ISSUER, signature)
            self.assertIn(f"org.opencontainers.image.revision={CANDIDATE}", signature)
            self.assertIn(f"io.rocketmq.service={service}", signature)
            attestation = next(
                call for call in calls if call[0] == "verify-attestation" and call[-1].endswith(digest(service))
            )
            self.assertIn(verifier.PREDICATE_TYPE, attestation)

    def test_publication_identity_fields_fail_closed(self) -> None:
        mutations = (
            ("source", "workflow://example.invalid/publish", "source"),
            ("category", "performance", "category"),
            ("fixture", True, "fixture"),
            ("status", "not-run", "status"),
            ("candidate_commit", "f" * 40, "candidate_commit"),
        )
        for field, value, finding in mutations:
            with self.subTest(field=field):
                publication = build_publication()
                publication[field] = value
                with self.assertRaisesRegex(verifier.VerificationError, finding):
                    verifier.validate_publication(publication, CANDIDATE)

    def test_image_and_service_digest_mismatches_fail_closed(self) -> None:
        publication = build_publication()
        publication["services"][0]["image"]["digest"] = digest("different")
        with self.assertRaisesRegex(verifier.VerificationError, "digest mismatch"):
            verifier.validate_publication(publication, CANDIDATE)

        publication = build_publication()
        publication["images"].pop("mcp")
        with self.assertRaisesRegex(verifier.VerificationError, "exactly five services"):
            verifier.validate_publication(publication, CANDIDATE)

    def test_candidate_toolchain_and_policy_are_exact(self) -> None:
        mutations = (
            (lambda publication: publication["candidate"].update(source_kind="branch"), "source_kind"),
            (lambda publication: publication["candidate"].update(immutable_tag="latest"), "immutable_tag"),
            (lambda publication: publication["tools"].update(syft="latest"), "tool versions"),
            (
                lambda publication: publication["policy"].update(
                    maximum_critical_findings=False
                ),
                "zero-CRITICAL policy",
            ),
        )
        for mutation, finding in mutations:
            with self.subTest(finding=finding):
                publication = build_publication()
                mutation(publication)
                with self.assertRaisesRegex(verifier.VerificationError, finding):
                    verifier.validate_publication(publication, CANDIDATE)

    def test_artifact_paths_presence_and_hashes_are_exact(self) -> None:
        mutations = (
            (
                lambda publication: publication["artifacts"][0].update(
                    path="../sbom/broker.cdx.json"
                ),
                "normalized and relative",
            ),
            (lambda publication: publication["artifacts"].pop(0), "explicit artifact manifest"),
            (
                lambda publication: publication["artifacts"][0].update(
                    sha256="f" * 64
                ),
                "explicit artifact manifest",
            ),
        )
        for mutation, finding in mutations:
            with self.subTest(finding=finding):
                publication = build_publication()
                mutation(publication)
                with self.assertRaisesRegex(verifier.VerificationError, finding):
                    verifier.validate_publication(publication, CANDIDATE)

    def test_service_image_digests_must_be_unique(self) -> None:
        publication = build_publication()
        broker_digest = publication["images"]["broker"].rsplit("@", 1)[1]
        publication["images"]["namesrv"] = (
            f"ghcr.io/mxsm/rocketmq-rust/namesrv@{broker_digest}"
        )
        with self.assertRaisesRegex(verifier.VerificationError, "digest is duplicated"):
            verifier.validate_publication(publication, CANDIDATE)

    def test_attestation_candidate_or_digest_mismatch_is_rejected(self) -> None:
        for override, value in (
            ("predicate__source_commit", "f" * 40),
            ("predicate__image__digest", digest("different")),
            ("predicateType", "https://example.invalid/predicate"),
            ("subject", [{"digest": {"sha256": sha256("different")}}]),
            ("_type", "https://in-toto.io/Statement/v0.1"),
            ("predicate__sbom__format", DELETE),
            ("predicate__vulnerability_scan__critical_findings", False),
            ("extra", "untrusted"),
        ):
            with self.subTest(override=override):
                publication = build_publication()

                def runner(arguments) -> str:  # type: ignore[no-untyped-def]
                    service = arguments[-1].split("/")[-1].split("@")[0]
                    if arguments[0] == "verify":
                        return '{"verified":true}\n'
                    return attestation_line(publication, service, **{override: value}) + "\n"

                with self.assertRaisesRegex(verifier.VerificationError, "no verified Statement/v1 matches"):
                    verifier.verify_publication(publication, CANDIDATE, runner)

    def test_every_dsse_jsonl_line_must_decode(self) -> None:
        publication = build_publication()
        service = verifier.SERVICES[0]
        valid = attestation_line(publication, service)
        with self.assertRaisesRegex(verifier.VerificationError, "line 2"):
            verifier.decode_attestation_statements(valid + "\n" + '{"payload":"not base64"}\n')

        envelope = json.loads(valid)
        envelope["payloadType"] = "application/json"
        with self.assertRaisesRegex(verifier.VerificationError, "payloadType"):
            verifier.decode_attestation_statements(json.dumps(envelope) + "\n")

    def test_failed_verification_preserves_existing_output(self) -> None:
        with tempfile.TemporaryDirectory(prefix="publication-output-") as temporary:
            root = Path(temporary)
            publication_path = root / "publication.json"
            output_path = root / "candidate-images.json"
            publication_path.write_text(
                json.dumps(build_publication()), encoding="utf-8"
            )
            output_path.write_text("existing-output\n", encoding="utf-8")
            with mock.patch.object(
                verifier,
                "verify_publication",
                side_effect=verifier.VerificationError("deliberate failure"),
            ):
                result = verifier.main(
                    (
                        "--publication",
                        str(publication_path),
                        "--candidate",
                        CANDIDATE,
                        "--output-image-map",
                        str(output_path),
                    )
                )
            self.assertEqual(1, result)
            self.assertEqual("existing-output\n", output_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
