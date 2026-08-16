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

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
for module_root in (ROOT / "distribution", SCRIPTS):
    if str(module_root) not in sys.path:
        sys.path.insert(0, str(module_root))

import capture_candidate_execution_context
import core_release_scope
import release_candidate_command
import release_identity_guard
import stage_publishable_crate
from release_state import (
    ReleaseStateError,
    atomic_write_json,
    read_json,
    resolve_existing_file,
    utc_now,
    validate_candidate,
)


class PlannerError(ReleaseStateError):
    """Raised when the core crate package graph cannot be prepared safely."""


PACKAGE_POLICY = ROOT / "distribution" / "release-package-policy.json"
LEGAL_POLICY = ROOT / "distribution" / "legal-policy.json"


def collect_metadata(root: Path = ROOT) -> dict[str, Any]:
    completed = subprocess.run(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        raise PlannerError(f"cargo metadata failed: {completed.stderr.strip()}")
    try:
        value = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        raise PlannerError(f"cargo metadata returned invalid JSON: {error}") from error
    if not isinstance(value, dict):
        raise PlannerError("cargo metadata did not return an object")
    return value


def _workspace_packages(metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    packages = metadata.get("packages")
    members = metadata.get("workspace_members")
    if not isinstance(packages, list) or not isinstance(members, list):
        raise PlannerError("cargo metadata has no packages/workspace_members")
    by_id = {
        package.get("id"): package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    result: dict[str, dict[str, Any]] = {}
    for package_id in members:
        package = by_id.get(package_id)
        if not isinstance(package, dict) or not isinstance(package.get("name"), str):
            raise PlannerError(f"workspace member is absent from metadata: {package_id}")
        if package["name"] in result:
            raise PlannerError(f"workspace package name is duplicated: {package['name']}")
        result[package["name"]] = package
    return result


def _stable_topological_order(dependencies: dict[str, set[str]]) -> list[str]:
    remaining = {name: set(values) for name, values in dependencies.items()}
    ordered: list[str] = []
    while remaining:
        ready = sorted(name for name, values in remaining.items() if not values)
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise PlannerError(f"core registry-publish dependency cycle detected: {cycle}")
        for name in ready:
            ordered.append(name)
            remaining.pop(name)
        for values in remaining.values():
            values.difference_update(ready)
    return ordered


def build_plan(
    metadata: dict[str, Any],
    scope: dict[str, Any],
    *,
    selector: str | None,
) -> dict[str, Any]:
    entries = scope.get("core_packages")
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        raise PlannerError("core release scope has no core_packages list")
    scoped = {
        entry.get("name"): entry
        for entry in entries
        if isinstance(entry.get("name"), str)
    }
    if len(scoped) != len(entries):
        raise PlannerError("core release scope contains duplicate or invalid package names")
    workspace = _workspace_packages(metadata)
    missing = sorted(set(scoped) - set(workspace))
    if missing:
        raise PlannerError(f"core packages are missing from Cargo metadata: {', '.join(missing)}")
    excluded_entries = scope.get("workspace_exclusions", [])
    if not isinstance(excluded_entries, list) or any(
        not isinstance(entry, dict) or not isinstance(entry.get("name"), str)
        for entry in excluded_entries
    ):
        raise PlannerError("core release scope has an invalid workspace_exclusions list")
    excluded = {entry["name"] for entry in excluded_entries}
    unclassified = sorted(set(workspace) - set(scoped) - excluded)
    if unclassified:
        raise PlannerError(
            "workspace packages are not classified by core release scope: "
            + ", ".join(unclassified)
        )
    if selector is not None:
        selected_entry = scoped.get(selector)
        if selected_entry is None:
            raise PlannerError(f"{selector!r} is not a core package")
        classification = selected_entry.get("classification")
        if classification != "registry-publish":
            raise PlannerError(
                f"core package {selector!r} is {classification!r}, not registry-publish"
            )

    publishable = {
        name
        for name, entry in scoped.items()
        if entry.get("classification") == "registry-publish"
    }
    dependencies: dict[str, set[str]] = {}
    for name in publishable:
        package_dependencies = workspace[name].get("dependencies", [])
        if not isinstance(package_dependencies, list):
            raise PlannerError(f"metadata dependencies are invalid for {name}")
        internal: set[str] = set()
        for dependency in package_dependencies:
            if not isinstance(dependency, dict) or not isinstance(dependency.get("name"), str):
                raise PlannerError(f"metadata dependency is invalid for {name}")
            dependency_name = dependency["name"]
            if dependency_name not in scoped:
                if dependency.get("path") is not None:
                    raise PlannerError(
                        f"registry-publish package {name} has unclassified workspace dependency "
                        f"{dependency_name}"
                    )
                continue
            dependency_classification = scoped[dependency_name].get("classification")
            if dependency_classification != "registry-publish":
                raise PlannerError(
                    f"registry-publish package {name} depends on {dependency_name} "
                    f"classified as {dependency_classification!r}"
                )
            if dependency_name != name:
                internal.add(dependency_name)
        dependencies[name] = internal

    if selector is None:
        selected = set(publishable)
    else:
        selected = set()
        pending = [selector]
        while pending:
            name = pending.pop()
            if name in selected:
                continue
            selected.add(name)
            pending.extend(sorted(dependencies[name] - selected))
    selected_dependencies = {
        name: dependencies[name] & selected for name in selected
    }
    order = _stable_topological_order(selected_dependencies)
    workspace_root_value = metadata.get("workspace_root")
    workspace_root = (
        Path(workspace_root_value).resolve()
        if isinstance(workspace_root_value, str)
        else ROOT
    )
    packages: list[dict[str, Any]] = []
    for index, name in enumerate(order, start=1):
        package = workspace[name]
        manifest = package.get("manifest_path")
        if not isinstance(manifest, str):
            raise PlannerError(f"metadata manifest_path is invalid for {name}")
        try:
            relative_manifest = Path(manifest).resolve().relative_to(workspace_root).as_posix()
        except ValueError as error:
            raise PlannerError(f"manifest for {name} escapes the workspace") from error
        packages.append(
            {
                "name": name,
                "classification": "registry-publish",
                "manifest": relative_manifest,
                "version": package.get("version"),
                "order": index,
                "internal_dependencies": sorted(selected_dependencies[name]),
                "operation_type": "registry-package",
                "target_registry": "crates.io",
            }
        )
    skipped: list[dict[str, str]] = []
    for name in sorted(scoped):
        if name in selected:
            continue
        classification = str(scoped[name].get("classification"))
        reason = (
            f"classification={classification}"
            if classification != "registry-publish"
            else "not-selected"
        )
        skipped.append({"name": name, "classification": classification, "reason": reason})
    return {"packages": packages, "skipped_packages": skipped}


def _candidate(manifest: Path) -> tuple[Path, dict[str, Any]]:
    manifest = resolve_existing_file(manifest, "candidate_manifest")
    candidate = read_json(manifest)
    validate_candidate(candidate)
    if candidate["sealed"]:
        raise PlannerError("sealed candidates cannot create package reports")
    candidate_root = Path(candidate["candidate_root"]).resolve()
    if candidate_root != manifest.parent.resolve():
        raise PlannerError("candidate manifest does not live at its candidate root")
    return manifest, candidate


def _within_candidate(path: Path, candidate_root: Path, label: str) -> Path:
    resolved = path.resolve()
    try:
        resolved.relative_to(candidate_root)
    except ValueError as error:
        raise PlannerError(f"{label} must stay within the candidate root") from error
    return resolved


def _validate_preflight() -> None:
    identity = release_identity_guard.read_json(release_identity_guard.DEFAULT_IDENTITY)
    schema = release_identity_guard.read_json(release_identity_guard.DEFAULT_SCHEMA)
    findings = release_identity_guard.validate_identity(identity, schema, root=ROOT)
    if findings:
        raise PlannerError("release identity preflight failed: " + "; ".join(findings))


def _policy(path: Path, label: str) -> dict[str, Any]:
    value = read_json(path)
    if not isinstance(value, dict) or value.get("schema_version") != 1:
        raise PlannerError(f"{label} must use schema_version 1")
    return value


def _validate_package_policy(
    policy: dict[str, Any], plan: dict[str, Any], *, all_core: bool
) -> None:
    if policy.get("remote_publication") != "not-executed":
        raise PlannerError("package policy must prohibit remote publication")
    if policy.get("inventory_source") != "cargo metadata --locked --format-version 1 --no-deps":
        raise PlannerError("package policy must use locked Cargo metadata")
    if all_core:
        expected_publish = policy.get("expected_registry_publish_count")
        expected_binary = policy.get("expected_binary_only_count")
        actual_binary = sum(
            entry.get("classification") == "binary-only"
            for entry in plan["skipped_packages"]
        )
        if len(plan["packages"]) != expected_publish:
            raise PlannerError(
                f"registry-publish count drifted: expected {expected_publish}, "
                f"found {len(plan['packages'])}"
            )
        if actual_binary != expected_binary:
            raise PlannerError(
                f"binary-only count drifted: expected {expected_binary}, found {actual_binary}"
            )


def _verification_package(policy: dict[str, Any], packages: list[dict[str, Any]]) -> str:
    selected = {entry["name"] for entry in packages}
    preferred = policy.get("preferred_verification_packages")
    if not isinstance(preferred, list) or any(not isinstance(value, str) for value in preferred):
        raise PlannerError("package policy preferred_verification_packages must be a string list")
    for name in preferred:
        if name in selected:
            return name
    return packages[-1]["name"]


def _execute(args: argparse.Namespace) -> int:
    manifest, candidate = _candidate(args.candidate_manifest)
    candidate_root = manifest.parent.resolve()
    output = _within_candidate(args.output_report, candidate_root, "output report")
    if args.package_only and args.staging_registry != "local-temp":
        raise PlannerError("--package-only requires --staging-registry local-temp")
    _validate_preflight()
    scope, scope_findings = core_release_scope.validate_repository(root=ROOT)
    blocking = [finding for finding in scope_findings if finding.scope == "core"]
    if blocking:
        raise PlannerError(
            "core release scope is invalid: "
            + "; ".join(item.render() for item in blocking)
        )
    metadata = collect_metadata(ROOT)
    plan = build_plan(metadata, scope, selector=args.project)
    package_policy = _policy(PACKAGE_POLICY, "package policy")
    _validate_package_policy(package_policy, plan, all_core=args.project is None)
    versions = {entry["version"] for entry in plan["packages"]}
    if versions != {candidate["version"]}:
        raise PlannerError(
            f"candidate version {candidate['version']} does not match package versions "
            f"{sorted(versions)}"
        )
    mode = "package-only" if args.package_only else "plan-only"
    staged_packages: list[dict[str, Any]] = []
    registry_validation: dict[str, Any] | None = None
    if mode == "package-only":
        legal_policy = _policy(LEGAL_POLICY, "legal policy")
        try:
            staged = stage_publishable_crate.stage_workspace_crates(
                ROOT,
                candidate_root,
                packages=plan["packages"],
                legal_policy=legal_policy,
            )
            registry_root = candidate_root / "package-check-registry"
            stage_publishable_crate.create_local_registry(registry_root, staged, metadata)
            verification_name = _verification_package(package_policy, plan["packages"])
            verification_version = next(
                entry["version"] for entry in plan["packages"] if entry["name"] == verification_name
            )
            registry_validation = stage_publishable_crate.verify_local_registry(
                registry_root,
                package_name=verification_name,
                version=verification_version,
                work_root=candidate_root / "package-check-consumer",
            )
        except stage_publishable_crate.StagingError as error:
            raise PlannerError(str(error)) from error
        staged_by_name = {entry["name"]: entry for entry in staged}
        for entry in plan["packages"]:
            staged_entry = staged_by_name[entry["name"]]
            crate_path = Path(staged_entry["crate_path"]).resolve()
            staged_packages.append(
                {
                    "name": entry["name"],
                    "version": entry["version"],
                    "order": entry["order"],
                    "crate_path": crate_path.relative_to(candidate_root).as_posix(),
                    "legal_files": staged_entry["legal_files"],
                    "local_package_command": staged_entry["command"],
                    "exit_code": staged_entry["exit_code"],
                    "status": "packaged-locally",
                }
            )
    value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "mode": mode,
        "selector": args.project or "all-core",
        "registry_publish_count": len(plan["packages"]),
        "packages": plan["packages"],
        "skipped_packages": plan["skipped_packages"],
        "staged_packages": staged_packages,
        "local_registry_validation": registry_validation,
        "remote_publication": {"status": "not-executed"},
        "generated_at": utc_now(),
    }
    atomic_write_json(output, value)
    print(f"CORE_PACKAGE_PLAN_OK packages={len(plan['packages'])} report={output}")
    return 0


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(
        description="Plan or locally package the core release crate graph."
    )
    selector = value.add_mutually_exclusive_group(required=True)
    selector.add_argument("--all-core", action="store_true")
    selector.add_argument("--project")
    mode = value.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan-only", action="store_true")
    mode.add_argument("--package-only", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="compatibility alias for --plan-only")
    value.add_argument("--candidate-manifest", type=Path, required=True)
    value.add_argument("--output-report", type=Path, required=True)
    value.add_argument("--staging-registry", choices=("local-temp",))
    value.add_argument("--wrapped", action="store_true", help=argparse.SUPPRESS)
    return value


def main(argv: list[str] | None = None) -> int:
    raw = list(sys.argv[1:] if argv is None else argv)
    args = parser().parse_args(raw)
    if args.dry_run:
        args.plan_only = True
    try:
        if args.package_only and args.staging_registry != "local-temp":
            raise PlannerError("--package-only requires --staging-registry local-temp")
        if args.wrapped:
            return _execute(args)
        manifest, candidate = _candidate(args.candidate_manifest)
        candidate_root = manifest.parent.resolve()
        selector = args.project or "all-core"
        mode = "package" if args.package_only else "plan"
        route_id = f"R04-{mode}-{selector}".replace("_", "-")
        worker_id = f"package-{mode}"
        context = capture_candidate_execution_context.capture_context(
            manifest, worker_id, candidate_root / "contexts"
        )
        child = [
            sys.executable,
            str(Path(__file__).resolve()),
            *[item for item in raw if item != "--wrapped"],
            "--wrapped",
        ]
        return release_candidate_command.run_command(
            manifest,
            route_id=route_id,
            worker_id=worker_id,
            context_path=context,
            event_root=candidate_root / "events",
            command=child,
        )
    except ReleaseStateError as error:
        print(f"CORE_PACKAGE_PLAN_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
