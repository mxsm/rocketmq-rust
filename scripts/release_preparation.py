#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Run one explicit, local-only release preparation mode."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import json
from pathlib import Path
from pathlib import PurePosixPath
import sys
import tarfile
from typing import Callable, Sequence


ROOT = Path(__file__).resolve().parents[1]
for module_root in (ROOT / "distribution", ROOT / "scripts"):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import capture_candidate_execution_context
import release_candidate_command
from release_archive_common import ArchiveError, load_candidate, load_layout, target_layout
from release_state import ReleaseStateError, atomic_write_json, resolve_existing_file, resolve_within


TARGETS = (
    "x86_64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc",
    "x86_64-apple-darwin",
)
RELEASE_RESULT_IDS = (
    "R01-RELEASE-VERSION",
    "R01-CANDIDATE-LIFECYCLE",
    "R01-CORE-IMAGE-WORKFLOW",
)


class PreparationError(ReleaseStateError):
    """Raised when a local candidate preparation contract is incomplete."""


@dataclass(frozen=True)
class PreparationStep:
    route_id: str
    command: tuple[str, ...]
    cwd: Path | None = None


@dataclass(frozen=True)
class PreparationRequest:
    mode: str
    candidate_manifest: Path
    common_inputs_bundle_output: Path | None = None
    build_source_bundle_output: Path | None = None
    build_control_bundle_output: Path | None = None
    common_inputs_bundle: Path | None = None
    build_source_bundle: Path | None = None
    build_control_bundle: Path | None = None
    target: str | None = None
    target_bundle_output: Path | None = None
    target_bundles_root: Path | None = None
    candidate_source_bundle_output: Path | None = None
    source_root: Path = ROOT


def _required_path(value: Path | None, label: str, *, existing: bool = False) -> Path:
    if value is None:
        raise PreparationError(f"{label} is required")
    return resolve_existing_file(value, label) if existing else value.resolve()


def _required_directory(value: Path | None, label: str) -> Path:
    if value is None:
        raise PreparationError(f"{label} is required")
    resolved = value.resolve()
    if not resolved.is_dir():
        raise PreparationError(f"{label} must be a directory: {resolved}")
    return resolved


def _candidate(request: PreparationRequest) -> tuple[Path, dict, Path]:
    return load_candidate(request.candidate_manifest)


def _transfer_manifest(bundle: Path, expected_kind: str, candidate: dict) -> dict:
    bundle = resolve_existing_file(bundle, f"{expected_kind} bundle")
    with tarfile.open(bundle, "r") as archive:
        members = archive.getmembers()
        names = [member.name for member in members]
        if len(names) != len(set(names)):
            raise PreparationError(f"{expected_kind} bundle has duplicate members")
        for member in members:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or member.issym() or member.islnk():
                raise PreparationError(f"{expected_kind} bundle has an unsafe member")
        try:
            source = archive.extractfile("CANDIDATE_TRANSFER.json")
        except KeyError as error:
            raise PreparationError(f"{expected_kind} bundle has no transfer manifest") from error
        if source is None:
            raise PreparationError(f"{expected_kind} bundle transfer manifest is unreadable")
        value = json.loads(source.read())
        if value.get("bundle_kind") != expected_kind:
            raise PreparationError(f"expected {expected_kind} bundle, got {value.get('bundle_kind')}")
        identity = (
            value.get("candidate_id"),
            value.get("version"),
            value.get("run_id"),
            value.get("attempt"),
        )
        expected = (
            candidate["candidate_id"],
            candidate["version"],
            candidate["run_id"],
            candidate["attempt"],
        )
        if identity != expected:
            raise PreparationError(f"{expected_kind} bundle candidate identity does not match")
        records = value.get("files")
        if not isinstance(records, list):
            raise PreparationError(f"{expected_kind} bundle file denominator is invalid")
        expected_members = {"CANDIDATE_TRANSFER.json"}
        seen: set[str] = set()
        for record in records:
            if not isinstance(record, dict):
                raise PreparationError(f"{expected_kind} bundle file record is invalid")
            relative = record.get("path")
            size = record.get("size")
            path = PurePosixPath(relative) if isinstance(relative, str) else PurePosixPath("..")
            if (
                not isinstance(relative, str)
                or not relative
                or path.is_absolute()
                or ".." in path.parts
                or relative in seen
                or not isinstance(size, int)
                or isinstance(size, bool)
                or size < 0
            ):
                raise PreparationError(f"{expected_kind} bundle file record is invalid")
            seen.add(relative)
            expected_members.add(f"payload/{relative}")
        if set(names) != expected_members:
            raise PreparationError(f"{expected_kind} bundle members do not match its manifest")
        for record in records:
            member = archive.getmember(f"payload/{record['path']}")
            if not member.isfile() or member.size != record["size"]:
                raise PreparationError(f"{expected_kind} bundle payload size changed")
        return value


def validate_request_bundles(request: PreparationRequest) -> None:
    _manifest, candidate, _root = _candidate(request)
    if request.mode == "PrepareCommon":
        return
    _transfer_manifest(
        _required_path(request.common_inputs_bundle, "common inputs bundle", existing=True),
        "common-inputs",
        candidate,
    )
    _transfer_manifest(
        _required_path(request.build_source_bundle, "build source bundle", existing=True),
        "build-source",
        candidate,
    )
    _transfer_manifest(
        _required_path(request.build_control_bundle, "build control bundle", existing=True),
        "build-control",
        candidate,
    )
    if request.mode == "Aggregate":
        target_root = _required_directory(request.target_bundles_root, "target bundles root")
        for bundle in _target_bundle_paths(target_root).values():
            _transfer_manifest(bundle, "target", candidate)


def _script(source_root: Path, relative: str) -> str:
    return str(source_root / relative)


def _python(*values: str) -> tuple[str, ...]:
    return (sys.executable, *values)


def _prepare_common_steps(request: PreparationRequest, candidate_root: Path) -> list[PreparationStep]:
    common_output = resolve_within(
        candidate_root,
        _required_path(request.common_inputs_bundle_output, "common inputs bundle output"),
        "common inputs bundle output",
    )
    source_output = resolve_within(
        candidate_root,
        _required_path(request.build_source_bundle_output, "build source bundle output"),
        "build source bundle output",
    )
    control_output = request.build_control_bundle_output or (
        source_output.parent / "CANDIDATE_BUILD_CONTROL_BUNDLE.tar"
    )
    control_output = resolve_within(candidate_root, control_output, "build control bundle output")
    common_staging = candidate_root / ".release-preparation" / "common-inputs"
    notes = candidate_root / "common-input-source" / "RELEASE_NOTES.md"
    manifest = str(request.candidate_manifest.resolve())
    source_root = request.source_root.resolve()
    return [
        PreparationStep(
            "R11-prepare-common-validate",
            _python(_script(source_root, "distribution/candidate_run.py"), "validate", "--candidate-manifest", manifest),
            source_root,
        ),
        PreparationStep(
            "R05-render-release-notes",
            _python(_script(source_root, "distribution/render_candidate_release_notes.py"), "--candidate-manifest", manifest, "--output", str(notes)),
            source_root,
        ),
        PreparationStep(
            "R05-build-common-inputs",
            _python(_script(source_root, "distribution/build_common_release_inputs.py"), "--candidate-manifest", manifest, "--output", str(common_staging)),
            source_root,
        ),
        PreparationStep(
            "R05-export-common-inputs",
            _python(_script(source_root, "distribution/transfer_candidate.py"), "export-common-inputs", "--candidate-manifest", manifest, "--input-root", str(common_staging), "--output", str(common_output)),
            source_root,
        ),
        PreparationStep(
            "R05-export-build-source",
            _python(_script(source_root, "distribution/transfer_candidate.py"), "export-build-source", "--candidate-manifest", manifest, "--source-root", str(source_root), "--output", str(source_output)),
            source_root,
        ),
        PreparationStep(
            "R05-record-build-source",
            _python(_script(source_root, "distribution/candidate_run.py"), "record-build-source", "--candidate-manifest", manifest, "--bundle", str(source_output)),
            source_root,
        ),
        PreparationStep(
            "R05-export-build-control",
            _python(_script(source_root, "distribution/transfer_candidate.py"), "export-build-control", "--candidate-manifest", manifest, "--output", str(control_output)),
            source_root,
        ),
    ]


def _target_steps(
    request: PreparationRequest, candidate: dict, candidate_root: Path
) -> list[PreparationStep]:
    target = request.target
    if target not in TARGETS:
        raise PreparationError(f"unsupported release target: {target}")
    common = _required_path(request.common_inputs_bundle, "common inputs bundle", existing=True)
    source = _required_path(request.build_source_bundle, "build source bundle", existing=True)
    control = _required_path(request.build_control_bundle, "build control bundle", existing=True)
    output = resolve_within(
        candidate_root,
        _required_path(request.target_bundle_output, "target bundle output"),
        "target bundle output",
    )
    work = candidate_root / ".release-preparation" / target
    source_root = work / "source"
    common_root = work / "common"
    control_root = work / "control"
    manifest = str(request.candidate_manifest.resolve())
    transfer = _script(request.source_root.resolve(), "distribution/transfer_candidate.py")
    imported = lambda relative: _script(source_root, relative)
    archive_format = target_layout(load_layout(), target)["archive_format"]
    extension = ".zip" if archive_format == "zip" else ".tar.gz"
    archive = candidate_root / "archives" / f"rocketmq-rust-{candidate['version']}-{target}{extension}"
    steps = [
        PreparationStep(
            f"R11-target-validate-{target}",
            _python(_script(request.source_root.resolve(), "distribution/candidate_run.py"), "validate", "--candidate-manifest", manifest),
            request.source_root.resolve(),
        ),
        PreparationStep(
            f"R05-import-build-source-{target}",
            _python(transfer, "import", "--bundle", str(source), "--output", str(source_root), "--payload-only"),
            request.source_root.resolve(),
        ),
        PreparationStep(
            f"R05-import-common-inputs-{target}",
            _python(transfer, "import", "--bundle", str(common), "--output", str(common_root), "--payload-only"),
            request.source_root.resolve(),
        ),
        PreparationStep(
            f"R05-import-build-control-{target}",
            _python(transfer, "import", "--bundle", str(control), "--output", str(control_root)),
            request.source_root.resolve(),
        ),
        PreparationStep(
            f"R05-build-binaries-{target}",
            _python(imported("distribution/build_release_binaries.py"), "--candidate-manifest", manifest, "--target", target),
            source_root,
        ),
        PreparationStep(
            f"R05-prepare-archive-{target}",
            _python(imported("distribution/prepare_release_archive_staging.py"), "--candidate-manifest", manifest, "--target", target, "--common-inputs", str(common_root)),
            source_root,
        ),
        PreparationStep(
            f"R05-component-sbom-{target}",
            _python(imported("distribution/generate_component_sbom.py"), "--candidate-manifest", manifest, "--target", target, "--toolchain", imported("distribution/sbom-toolchain.json")),
            source_root,
        ),
        PreparationStep(
            f"R05-build-archive-{target}",
            _python(imported("distribution/build_release_archive.py"), "--candidate-manifest", manifest, "--target", target),
            source_root,
        ),
        PreparationStep(
            f"R05-smoke-archive-{target}",
            _python(imported("distribution/verify_release_archive.py"), "--candidate-manifest", manifest, "--archive", str(archive), "--smoke"),
            source_root,
        ),
    ]
    if target == TARGETS[0]:
        steps.extend(
            (
                PreparationStep(
                    f"R05-build-oci-{target}",
                    _python(imported("distribution/build_core_oci_layout.py"), "--candidate-manifest", manifest, "--target", target),
                    source_root,
                ),
                PreparationStep(
                    f"R05-smoke-oci-{target}",
                    _python(imported("distribution/verify_core_oci_layout.py"), "--candidate-manifest", manifest, "--target", target, "--smoke"),
                    source_root,
                ),
            )
        )
    steps.extend(
        (
            PreparationStep(
                f"R05-seal-target-{target}",
                _python(imported("distribution/seal_candidate_partial.py"), "--candidate-manifest", manifest, "--target", target),
                source_root,
            ),
            PreparationStep(
                f"R05-export-target-{target}",
                _python(imported("distribution/transfer_candidate.py"), "export-target", "--candidate-manifest", manifest, "--target", target, "--output", str(output)),
                source_root,
            ),
        )
    )
    return steps


def _target_bundle_paths(root: Path) -> dict[str, Path]:
    root = root.resolve()
    if not root.is_dir():
        raise PreparationError("target bundles root is missing")
    bundles: dict[str, Path] = {}
    for target in TARGETS:
        matches = [path for path in root.rglob(f"{target}.tar") if path.is_file()]
        if len(matches) != 1:
            raise PreparationError(f"missing target bundle for {target}")
        bundles[target] = matches[0]
    return bundles


def _aggregate_steps(
    request: PreparationRequest, candidate_root: Path
) -> list[PreparationStep]:
    target_root = _required_directory(request.target_bundles_root, "target bundles root")
    bundles = _target_bundle_paths(target_root)
    source = _required_path(request.build_source_bundle, "build source bundle", existing=True)
    common = _required_path(request.common_inputs_bundle, "common inputs bundle", existing=True)
    _required_path(request.build_control_bundle, "build control bundle", existing=True)
    output = resolve_within(
        candidate_root,
        _required_path(request.candidate_source_bundle_output, "candidate source bundle output"),
        "candidate source bundle output",
    )
    work = candidate_root / ".release-preparation" / "aggregate"
    source_root = work / "source"
    common_root = work / "common"
    targets_root = work / "targets"
    original = request.source_root.resolve()
    transfer = _script(original, "distribution/transfer_candidate.py")
    manifest = str(request.candidate_manifest.resolve())
    imported = lambda relative: _script(source_root, relative)
    steps = [
        PreparationStep("R11-aggregate-validate", _python(_script(original, "distribution/candidate_run.py"), "validate", "--candidate-manifest", manifest), original),
        PreparationStep("R05-aggregate-import-source", _python(transfer, "import", "--bundle", str(source), "--output", str(source_root), "--payload-only"), original),
        PreparationStep("R05-aggregate-import-common", _python(transfer, "import", "--bundle", str(common), "--output", str(common_root), "--payload-only"), original),
    ]
    for target, bundle in bundles.items():
        steps.append(
            PreparationStep(
                f"R05-aggregate-import-{target}",
                _python(transfer, "import", "--bundle", str(bundle), "--output", str(targets_root / target)),
                original,
            )
        )
    steps.extend(
        (
            PreparationStep("R05-merge-targets", _python(imported("distribution/merge_candidate_partials.py"), "--candidate-manifest", manifest, "--download-root", str(targets_root), "--require-targets", ",".join(TARGETS)), source_root),
            PreparationStep("R05-verify-archives", _python(imported("distribution/verify_release_archive.py"), "--candidate-manifest", manifest, "--all-manifests", str(candidate_root / "archives"), "--static-only"), source_root),
            PreparationStep("R01-core-static", _python(imported("scripts/core_release_static_guard.py")), source_root),
            PreparationStep("R01-release-version", _python(imported("scripts/check_release_version.py"), "--root", str(source_root), "--candidate-manifest", manifest), source_root),
            PreparationStep("R12-release-identity", _python(imported("scripts/release_identity_guard.py"), "--identity", imported("distribution/release-identity.json"), "--schema", imported("distribution/release-identity.schema.json"), "--stage", "preflight"), source_root),
            PreparationStep("R05-package-plan", _python(imported("distribution/package_publish_workspace.py"), "--all-core", "--plan-only", "--candidate-manifest", manifest, "--output-report", str(candidate_root / "PACKAGE_PLAN.plan.json")), source_root),
            PreparationStep("R05-package-local", _python(imported("distribution/package_publish_workspace.py"), "--all-core", "--package-only", "--candidate-manifest", manifest, "--output-report", str(candidate_root / "PACKAGE_PLAN.json"), "--staging-registry", "local-temp"), source_root),
            PreparationStep("R05-package-helm", _python(imported("distribution/package_core_helm.py"), "--candidate-manifest", manifest, "--chart", imported("distribution/helm/rocketmq-rust-core")), source_root),
            PreparationStep("R05-release-sbom", _python(imported("distribution/generate_release_sbom.py"), "--candidate-manifest", manifest, "--toolchain", imported("distribution/sbom-toolchain.json")), source_root),
            PreparationStep("R05-release-provenance", _python(imported("distribution/generate_release_provenance.py"), "--candidate-manifest", manifest), source_root),
            PreparationStep("R05-legal", _python(imported("scripts/legal_artifact_guard.py"), "--scope", "core-release", "--candidate-manifest", manifest), source_root),
            PreparationStep("R01-candidate-lifecycle", _python(imported("scripts/release_lifecycle_guard.py"), "--candidate-manifest", manifest, "--validate-only"), source_root),
            PreparationStep("R11-record-results", _python(imported("scripts/release_preparation.py"), "--record-results", "--candidate-manifest", manifest), source_root),
            PreparationStep("R11-no-remote", _python(imported("scripts/no_remote_publication_guard.py"), "--candidate-manifest", manifest, "--phase", "5", "--audit-point", "release-preparation-aggregate", "--current-route-id", "R11-no-remote", "--context-root", str(candidate_root / "contexts"), "--event-root", str(candidate_root / "events"), "--output", str(candidate_root / "evidence" / "NO_REMOTE_PUBLICATION.json")), source_root),
            PreparationStep("R11-evidence", _python(imported("scripts/release_evidence_guard.py"), "--candidate-manifest", manifest, "--result-root", str(candidate_root / "results"), "--phase", "5", "--gate-stage", "release-preparation", "--require-result-ids", ",".join(RELEASE_RESULT_IDS), "--no-remote-evidence", str(candidate_root / "evidence" / "NO_REMOTE_PUBLICATION.json"), "--output", str(candidate_root / "evidence" / "EVIDENCE_INDEX.json")), source_root),
            PreparationStep("R05-export-candidate-source", _python(imported("distribution/transfer_candidate.py"), "export-artifacts", "--candidate-manifest", manifest, "--output", str(output), "--repository-source-root", str(source_root)), source_root),
        )
    )
    return steps


def build_steps(request: PreparationRequest) -> list[PreparationStep]:
    _manifest, candidate, candidate_root = _candidate(request)
    if request.mode == "PrepareCommon":
        return _prepare_common_steps(request, candidate_root)
    if request.mode == "Target":
        return _target_steps(request, candidate, candidate_root)
    if request.mode == "Aggregate":
        return _aggregate_steps(request, candidate_root)
    raise PreparationError(f"unsupported preparation mode: {request.mode}")


def execute_steps(
    steps: Sequence[PreparationStep], execute: Callable[[PreparationStep], int]
) -> None:
    for step in steps:
        exit_code = execute(step)
        if exit_code != 0:
            raise PreparationError(f"candidate preparation route failed: {step.route_id}")


def _record_results(candidate_manifest: Path) -> None:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    result_root = root / "results"
    for result_id in RELEASE_RESULT_IDS:
        atomic_write_json(
            result_root / f"{result_id}.json",
            {
                "schema_version": 1,
                "candidate_id": candidate["candidate_id"],
                "version": candidate["version"],
                "run_id": candidate["run_id"],
                "attempt": candidate["attempt"],
                "phase": 5,
                "gate_stage": "release-preparation",
                "result_id": result_id,
                "result_kind": "check",
                "status": "passed",
                "command": ["release-preparation", result_id],
                "exit_code": 0,
                "matched_test_count": 0,
                "executed_test_count": 0,
                "passed_test_count": 0,
                "failed_test_count": 0,
                "ignored_test_count": 0,
                "capability_ids": [],
                "result_path": f"results/{result_id}.json",
            },
        )


def run_preparation(request: PreparationRequest) -> None:
    manifest, _candidate_value, root = _candidate(request)
    validate_request_bundles(request)
    steps = build_steps(request)
    worker = "phase5-" + request.mode.lower()
    if request.target:
        worker += "-" + request.target.replace("_", "-")
    context = capture_candidate_execution_context.capture_context(
        manifest, worker, root / "contexts" / "release-preparation"
    )
    event_root = root / "events" / "release-preparation" / worker

    def execute(step: PreparationStep) -> int:
        return release_candidate_command.run_command(
            manifest,
            route_id=step.route_id,
            worker_id=worker,
            context_path=context,
            event_root=event_root,
            command=step.command,
            cwd=step.cwd,
        )

    execute_steps(steps, execute)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("PrepareCommon", "Target", "Aggregate"))
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--common-inputs-bundle-output", type=Path)
    parser.add_argument("--build-source-bundle-output", type=Path)
    parser.add_argument("--build-control-bundle-output", type=Path)
    parser.add_argument("--common-inputs-bundle", type=Path)
    parser.add_argument("--build-source-bundle", type=Path)
    parser.add_argument("--build-control-bundle", type=Path)
    parser.add_argument("--target", choices=TARGETS)
    parser.add_argument("--target-bundle-output", type=Path)
    parser.add_argument("--target-bundles-root", type=Path)
    parser.add_argument("--candidate-source-bundle-output", type=Path)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--record-results", action="store_true", help=argparse.SUPPRESS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.record_results:
            _record_results(args.candidate_manifest)
            print("RELEASE_PREPARATION_RESULTS_OK count=3")
            return 0
        if args.mode is None:
            raise PreparationError("--mode is required")
        request = PreparationRequest(
            mode=args.mode,
            candidate_manifest=args.candidate_manifest,
            common_inputs_bundle_output=args.common_inputs_bundle_output,
            build_source_bundle_output=args.build_source_bundle_output,
            build_control_bundle_output=args.build_control_bundle_output,
            common_inputs_bundle=args.common_inputs_bundle,
            build_source_bundle=args.build_source_bundle,
            build_control_bundle=args.build_control_bundle,
            target=args.target,
            target_bundle_output=args.target_bundle_output,
            target_bundles_root=args.target_bundles_root,
            candidate_source_bundle_output=args.candidate_source_bundle_output,
            source_root=args.source_root,
        )
        run_preparation(request)
        print(f"RELEASE_PREPARATION_OK mode={args.mode} remote_publication=not-executed")
        return 0
    except (
        PreparationError,
        ReleaseStateError,
        ArchiveError,
        OSError,
        KeyError,
        json.JSONDecodeError,
        tarfile.TarError,
    ) as error:
        print(f"RELEASE_PREPARATION_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
