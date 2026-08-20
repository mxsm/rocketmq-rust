# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock
import zipfile


ROOT = Path(__file__).resolve().parents[2]
BUILDER = ROOT / "distribution" / "build_publication_handoff.py"
VERIFIER = ROOT / "distribution" / "verify_publication_handoff.py"
SOURCE_MAP = ROOT / "distribution" / "publication-handoff-source-map.json"


def load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PublicationHandoffTests(unittest.TestCase):
    def setUp(self) -> None:
        for path in (BUILDER, VERIFIER, SOURCE_MAP):
            self.assertTrue(path.is_file(), f"publication handoff component must exist: {path.name}")
        self.builder = load_module("build_publication_handoff_test", BUILDER)
        self.verifier = load_module("verify_publication_handoff_test", VERIFIER)

    def test_builds_and_semantically_verifies_local_only_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            report = self.verifier.verify_handoff(
                draft,
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                mode="draft-pre-ready",
                result_id="H02-DRAFT-SEMANTIC",
            )

            self.assertEqual("passed", report["status"])
            self.assertIn("phase", report)
            self.assertIn("gate_stage", report)
            self.assertEqual(6, report["phase"])
            self.assertEqual("final-handoff", report["gate_stage"])
            self.assertEqual("not-executed", report["remote_publication"]["status"])
            self.assertFalse((draft / "PUBLICATION_READY.json").exists())
            handoff = json.loads((draft / "PUBLICATION_HANDOFF.json").read_text(encoding="utf-8"))
            self.assertEqual("unofficial-community", handoff["distribution_identity"])
            self.assertEqual("ghcr.io/mxsm/rocketmq-rust", handoff["future_publication"]["oci_namespace"])
            self.assertFalse(handoff["future_publication"]["executed"])
            self.assertNotIn("sha256", json.dumps(handoff).lower())

    def test_same_size_handoff_copy_tamper_is_rejected_by_stream_compare(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"], fixture["candidate_root"], fixture["source_root"], fixture["output_root"], SOURCE_MAP
            )
            legal = draft / "legal" / "LICENSE-APACHE"
            replacement = b"X" * legal.stat().st_size
            legal.write_bytes(replacement)
            with self.assertRaisesRegex(ValueError, "byte content differs"):
                self.verifier.verify_handoff(
                    draft,
                    fixture["candidate_manifest"],
                    fixture["candidate_root"],
                    fixture["source_root"],
                    mode="draft-pre-ready",
                    result_id="H02-DRAFT-SEMANTIC",
                )

    def test_nested_private_key_and_remote_enabled_candidate_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory), nested_secret=True)
            draft = self.builder.build_draft(
                fixture["candidate_manifest"], fixture["candidate_root"], fixture["source_root"], fixture["output_root"], SOURCE_MAP
            )
            with self.assertRaisesRegex(ValueError, "secret material"):
                self.verifier.verify_handoff(
                    draft,
                    fixture["candidate_manifest"],
                    fixture["candidate_root"],
                    fixture["source_root"],
                    mode="draft-pre-ready",
                    result_id="H02-DRAFT-SEMANTIC",
                )

        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            no_remote = fixture["candidate_root"] / "NO_REMOTE_PUBLICATION.json"
            value = json.loads(no_remote.read_text(encoding="utf-8"))
            value["remote_publication"]["status"] = "violation-detected"
            no_remote.write_text(json.dumps(value), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "not-executed"):
                self.builder.build_draft(
                    fixture["candidate_manifest"],
                    fixture["candidate_root"],
                    fixture["source_root"],
                    fixture["output_root"],
                    SOURCE_MAP,
                )

    def test_finalize_is_atomic_and_does_not_create_ready_marker(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"], fixture["candidate_root"], fixture["source_root"], fixture["output_root"], SOURCE_MAP
            )
            final = self.builder.finalize_draft(draft)
            self.assertTrue(final.is_dir())
            self.assertFalse(draft.exists())
            self.assertFalse((final / "PUBLICATION_READY.json").exists())
            with self.assertRaisesRegex(ValueError, "already exists"):
                self.builder.finalize_draft(final.with_name(f".{final.name}.staging"))

    def test_final_cli_requires_an_explicit_read_only_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            final = self.builder.finalize_draft(draft)
            output = Path(directory) / "H04-FINAL-SEMANTIC.json"

            exit_code = self.verifier.main(
                [
                    "--handoff",
                    str(final),
                    "--candidate-manifest",
                    str(fixture["candidate_manifest"]),
                    "--candidate-root",
                    str(fixture["candidate_root"]),
                    "--source-root",
                    str(fixture["source_root"]),
                    "--final-pre-ready",
                    "--result-id",
                    "H04-FINAL-SEMANTIC",
                    "--output",
                    str(output),
                ]
            )

            self.assertEqual(1, exit_code)
            self.assertFalse(output.exists())

    def test_final_read_only_rejects_an_output_inside_the_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            final = self.builder.finalize_draft(draft)
            output = final / "evidence" / "H04-FINAL-SEMANTIC.json"

            exit_code = self.verifier.main(
                [
                    "--handoff",
                    str(final),
                    "--candidate-manifest",
                    str(fixture["candidate_manifest"]),
                    "--candidate-root",
                    str(fixture["candidate_root"]),
                    "--source-root",
                    str(fixture["source_root"]),
                    "--final-pre-ready",
                    "--final-read-only",
                    "--result-id",
                    "H04-FINAL-SEMANTIC",
                    "--output",
                    str(output),
                ]
            )

            self.assertEqual(1, exit_code)
            self.assertFalse(output.exists())

    def test_final_read_only_detects_same_size_mutation_during_verification(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            final = self.builder.finalize_draft(draft)
            output = Path(directory) / "H04-FINAL-SEMANTIC.json"
            original = self.verifier._verify_package_semantics

            def mutate_after_semantic_scan(*args, **kwargs):
                result = original(*args, **kwargs)
                notice = final / "legal" / "NOTICE"
                notice.write_bytes(b"X" * notice.stat().st_size)
                return result

            with mock.patch.object(
                self.verifier,
                "_verify_package_semantics",
                side_effect=mutate_after_semantic_scan,
            ):
                exit_code = self.verifier.main(
                    [
                        "--handoff",
                        str(final),
                        "--candidate-manifest",
                        str(fixture["candidate_manifest"]),
                        "--candidate-root",
                        str(fixture["candidate_root"]),
                        "--source-root",
                        str(fixture["source_root"]),
                        "--final-pre-ready",
                        "--final-read-only",
                        "--result-id",
                        "H04-FINAL-SEMANTIC",
                        "--output",
                        str(output),
                    ]
                )

            self.assertEqual(1, exit_code)
            self.assertFalse(output.exists())

    def test_final_read_only_external_output_records_the_verified_contract(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            final = self.builder.finalize_draft(draft)
            output = Path(directory) / "candidate-evidence" / "H04-FINAL-SEMANTIC.json"

            exit_code = self.verifier.main(
                [
                    "--handoff",
                    str(final),
                    "--candidate-manifest",
                    str(fixture["candidate_manifest"]),
                    "--candidate-root",
                    str(fixture["candidate_root"]),
                    "--source-root",
                    str(fixture["source_root"]),
                    "--final-pre-ready",
                    "--final-read-only",
                    "--result-id",
                    "H04-FINAL-SEMANTIC",
                    "--output",
                    str(output),
                ]
            )

            self.assertEqual(0, exit_code)
            result = json.loads(output.read_text(encoding="utf-8"))
            self.assertIn("read_only_verified", result)
            self.assertIs(result["read_only_verified"], True)

    def test_refresh_evidence_closes_the_three_platform_cut(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"], fixture["candidate_root"], fixture["source_root"], fixture["output_root"], SOURCE_MAP
            )
            evidence_path = fixture["candidate_root"] / "EVIDENCE_INDEX.json"
            evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
            evidence["result_ids"].extend(["H01-LINUX", "H01-WINDOWS", "H01-MACOS"])
            evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

            self.builder.refresh_evidence(
                draft,
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                evidence_path,
                fixture["candidate_root"] / "NO_REMOTE_PUBLICATION.json",
            )

            self.assertEqual(
                "passed",
                json.loads((draft / "evidence" / "SECRET_SCAN.json").read_text(encoding="utf-8"))["status"],
            )
            report = self.verifier.verify_handoff(
                draft,
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                mode="draft-pre-ready",
                result_id="H02-DRAFT-SEMANTIC",
            )
            self.assertEqual("passed", report["status"])

    def test_platform_verification_executes_manifest_archive_binaries(self) -> None:
        """A missing archive smoke call must not be reported as a passed H01 result."""

        with tempfile.TemporaryDirectory() as directory:
            fixture = self._fixture(Path(directory))
            draft = self.builder.build_draft(
                fixture["candidate_manifest"],
                fixture["candidate_root"],
                fixture["source_root"],
                fixture["output_root"],
                SOURCE_MAP,
            )
            target = "x86_64-unknown-linux-gnu"
            layout = json.loads(
                (ROOT / "distribution" / "release-layout.json").read_text(encoding="utf-8")
            )
            binaries = {
                entry.get("archive_binary", entry["binary"]): entry for entry in layout["binaries"]
            }

            def version_result(command, **_kwargs):
                binary = binaries[Path(command[0]).name]
                return mock.Mock(
                    returncode=0,
                    stdout=(
                        f"component={binary['id']}\n"
                        "version=1.0.0\n"
                        f"artifact_id=final-1.{target}.{binary['id']}\n"
                        f"requested_features={','.join(binary['requested_features'])}\n"
                        f"effective_features={','.join(binary['effective_features'])}\n"
                    ),
                    stderr="",
                )

            with mock.patch("subprocess.run", side_effect=version_result):
                report = self.verifier.verify_handoff(
                    draft,
                    fixture["candidate_manifest"],
                    fixture["candidate_root"],
                    fixture["source_root"],
                    mode="draft-pre-ready",
                    result_id="H01-LINUX",
                    platform="linux",
                    worker_id="handoff-linux",
                )

            self.assertIn("archive_smoke_results", report)
            self.assertEqual(
                ["admin", "broker", "controller", "namesrv", "proxy", "store-inspect"],
                sorted(result["component"] for result in report["archive_smoke_results"]),
            )
            self.assertTrue(
                all(result["exit_code"] == 0 for result in report["archive_smoke_results"])
            )
            self.assertTrue(
                all("version=1.0.0" in result["stdout"] for result in report["archive_smoke_results"])
            )

    @staticmethod
    def _add_tar(path: Path, files: dict[str, bytes]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with tarfile.open(path, "w:gz") as archive:
            for name, payload in files.items():
                info = tarfile.TarInfo(name)
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))

    @classmethod
    def _fixture(cls, root: Path, *, nested_secret: bool = False):
        candidate_root = root / "candidate"
        source_root = root / "source"
        output_root = root / "handoff"
        candidate_root.mkdir()
        source_root.mkdir()

        package_files = {
            "rocketmq-error-1.0.0/Cargo.toml": b'[package]\nname="rocketmq-error"\nversion="1.0.0"\n',
            "rocketmq-error-1.0.0/LICENSE-APACHE": b"Apache License\n",
            "rocketmq-error-1.0.0/NOTICE": b"RocketMQ Rust Community Distribution\n",
        }
        cls._add_tar(candidate_root / "crate-packages" / "rocketmq-error-1.0.0.crate", package_files)
        (candidate_root / "crate-packages" / "PACKAGE_PLAN.json").write_text(
            json.dumps({"schema_version": 1, "remote_publication": "not-executed"}), encoding="utf-8"
        )

        cls._add_release_archives(candidate_root, nested_secret=nested_secret)

        for service in ("namesrv", "broker", "controller", "proxy"):
            oci = candidate_root / "oci" / service
            oci.mkdir(parents=True)
            (oci / "oci-layout").write_text(json.dumps({"imageLayoutVersion": "1.0.0"}), encoding="utf-8")
            (oci / "index.json").write_text(json.dumps({"schemaVersion": 2, "manifests": []}), encoding="utf-8")

        cls._add_tar(
            candidate_root / "helm" / "rocketmq-rust-core-1.0.0.tgz",
            {"rocketmq-rust-core/Chart.yaml": b"apiVersion: v2\nname: rocketmq-rust-core\nversion: 1.0.0\n"},
        )
        for directory, name, content in (
            ("sbom", "RELEASE_SBOM.json", '{"schema_version":1}\n'),
            ("provenance", "RELEASE_PROVENANCE.json", '{"schema_version":1,"local_only":true}\n'),
            ("common-input-source", "RELEASE_NOTES.md", "# RocketMQ Rust 1.0.0 candidate\n"),
        ):
            path = candidate_root / directory / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")

        no_remote = {
            "schema_version": 1,
            "candidate_id": "final-1",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "remote_publication": {"status": "not-executed"},
            "remote_publication_workflow_dispatches": [],
            "publishing_credentials_provided": False,
            "publishing_credential_names": [],
        }
        (candidate_root / "NO_REMOTE_PUBLICATION.json").write_text(json.dumps(no_remote), encoding="utf-8")
        evidence = {
            "schema_version": 1,
            "candidate_id": "final-1",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "status": "passed",
            "result_ids": ["P01", "M01", "L01", "A01", "U01", "S01", "S02", "S03"],
        }
        (candidate_root / "EVIDENCE_INDEX.json").write_text(json.dumps(evidence), encoding="utf-8")
        artifact = {
            "schema_version": 1,
            "candidate_id": "final-1",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "targets": ["x86_64-unknown-linux-gnu", "x86_64-pc-windows-msvc", "x86_64-apple-darwin"],
            "artifacts": [],
            "remote_publication": "not-executed",
        }
        (candidate_root / "ARTIFACT_INDEX.json").write_text(json.dumps(artifact), encoding="utf-8")

        upgrade = source_root / "rocketmq-doc" / "en" / "release" / "1.0" / "upgrade-and-rollback.md"
        handoff_doc = source_root / "rocketmq-doc" / "en" / "release" / "1.0" / "publication-handoff.md"
        upgrade.parent.mkdir(parents=True)
        upgrade.write_text("# Upgrade and rollback\n", encoding="utf-8")
        handoff_doc.write_text("# Publication handoff\nThis does not mean 1.0.0 is published.\n", encoding="utf-8")
        (source_root / "LICENSE-APACHE").write_text("Apache License\n", encoding="utf-8")
        (source_root / "NOTICE").write_text(
            "RocketMQ Rust Community Distribution\n", encoding="utf-8"
        )

        series = root / "RELEASE_SERIES.json"
        series.write_text("{}", encoding="utf-8")
        manifest = candidate_root / "CANDIDATE_RUN.json"
        candidate = {
            "schema_version": 1,
            "candidate_id": "final-1",
            "candidate_kind": "final",
            "version": "1.0.0",
            "run_id": "run-1",
            "attempt": 1,
            "ordinal": 3,
            "candidate_root": str(candidate_root),
            "series_manifest": str(series),
            "series_id": "community-v1",
            "series_generation": 3,
            "parent_manifest": str(root / "parent.json"),
            "state": "ga-candidate-ready",
            "sealed": False,
            "outcome": "success",
            "rejection_reason": None,
            "known_issues": [
                {
                    "issue_id": "KNOWN-1",
                    "severity": "Medium",
                    "impact": "Documented limitation",
                    "workaround": "Use the documented local path",
                    "owner": "mxsm",
                    "target_version": "1.0.1",
                    "approval_status": "approved",
                    "approver": "mxsm",
                    "waiver_expiry": "2999-08-16",
                    "resolution_status": "open",
                }
            ],
            "generation": 4,
            "build_source_bundle": None,
            "source_snapshot": None,
            "artifact_index": "ARTIFACT_INDEX.json",
            "evidence_index": "EVIDENCE_INDEX.json",
            "event_index": None,
            "execution_context_index": None,
            "creation_operation_id": "fixture",
            "created_at": "2026-08-16T00:00:00Z",
            "updated_at": "2026-08-16T00:00:00Z",
        }
        manifest.write_text(json.dumps(candidate), encoding="utf-8")
        return {
            "candidate_manifest": manifest,
            "candidate_root": candidate_root,
            "source_root": source_root,
            "output_root": output_root,
        }

    @classmethod
    def _add_release_archives(cls, candidate_root: Path, *, nested_secret: bool) -> None:
        layout = json.loads(
            (ROOT / "distribution" / "release-layout.json").read_text(encoding="utf-8")
        )
        package_root = "rocketmq-rust-1.0.0"
        for target, target_spec in layout["targets"].items():
            suffix = target_spec["executable_suffix"]
            files: dict[str, bytes] = {}
            binary_records = []
            for binary in layout["binaries"]:
                name = binary.get("archive_binary", binary["binary"])
                files[f"bin/{name}{suffix}"] = b"fixture executable\n"
                binary_records.append(
                    {
                        "component": binary["id"],
                        "requested_features": binary["requested_features"],
                        "effective_features": binary["effective_features"],
                        "required_dependencies": binary.get("required_dependencies", []),
                    }
                )
            for service in layout["configs"]:
                files[f"conf/{service}.toml"] = b"[service]\n"
            if nested_secret and target == "x86_64-unknown-linux-gnu":
                files["private.pem"] = b"-----BEGIN PRIVATE KEY-----\nsecret\n"
            inventory = [
                {"path": path, "type": "directory", "size": 0}
                for path in ("bin", "conf")
            ] + [
                {"path": path, "type": "file", "size": len(content)}
                for path, content in files.items()
            ]
            inventory.sort(key=lambda item: item["path"])
            archive_name = f"rocketmq-rust-1.0.0-{target}"
            archive_path = candidate_root / "archives" / (
                f"{archive_name}.zip" if target_spec["archive_format"] == "zip" else f"{archive_name}.tar.gz"
            )
            payload = {f"{package_root}/{path}": content for path, content in files.items()}
            if target_spec["archive_format"] == "zip":
                with zipfile.ZipFile(archive_path, "w") as archive:
                    for path, content in payload.items():
                        archive.writestr(path, content)
            else:
                cls._add_tar(archive_path, payload)
            manifest = {
                "schema_version": 1,
                "candidate_id": "final-1",
                "version": "1.0.0",
                "run_id": "run-1",
                "attempt": 1,
                "target": target,
                "artifact_id": f"final-1.{target}.archive",
                "archive": f"archives/{archive_path.name}",
                "files": inventory,
                "binaries": binary_records,
                "remote_publication": "not-executed",
            }
            (candidate_root / "archives" / f"{archive_name}.manifest.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )


if __name__ == "__main__":
    unittest.main()
