#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Generate external CycloneDX SBOMs for the complete local candidate set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from build_core_oci_layout import SERVICES
from release_archive_common import (
    ArchiveError,
    candidate_relative,
    load_candidate,
    load_layout,
    resolve_candidate_path,
    write_json,
)
from release_artifact_index import register_artifacts
from release_state import read_json, resolve_existing_file


def _cargo_metadata() -> dict[str, Any]:
    completed = subprocess.run(
        ["cargo", "metadata", "--locked", "--offline", "--format-version", "1"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ArchiveError(f"cargo metadata failed: {completed.stderr.strip()}")
    return json.loads(completed.stdout)


def _component(package: dict[str, Any], *, scope: str) -> dict[str, Any]:
    license_expression = package.get("license")
    if not isinstance(license_expression, str) or not license_expression:
        raise ArchiveError(f"dependency has no license metadata: {package.get('name')}")
    return {
        "type": "library",
        "name": package["name"],
        "version": package["version"],
        "bom-ref": f"{package['name']}@{package['version']}",
        "licenses": [{"expression": license_expression}],
        "properties": [{"name": "rocketmq:dependency-scope", "value": scope}],
    }


def _package_sbom(
    candidate: dict[str, Any],
    package_name: str,
    version: str,
    metadata: dict[str, Any],
    toolchain: dict[str, Any],
) -> dict[str, Any]:
    packages = {package["id"]: package for package in metadata.get("packages", [])}
    roots = [
        package_id
        for package_id, package in packages.items()
        if package.get("name") == package_name and package.get("version") == version
    ]
    if len(roots) != 1:
        raise ArchiveError(f"Cargo metadata does not select one package: {package_name} {version}")
    nodes = {node["id"]: node for node in metadata.get("resolve", {}).get("nodes", [])}
    root_id = roots[0]
    root_node = nodes.get(root_id)
    if root_node is None:
        raise ArchiveError(f"Cargo resolve graph has no package node: {package_name}")
    direct = {dependency["pkg"] for dependency in root_node.get("deps", [])}
    visited: set[str] = set()
    pending = list(direct)
    while pending:
        package_id = pending.pop()
        if package_id in visited:
            continue
        visited.add(package_id)
        node = nodes.get(package_id)
        if node:
            pending.extend(dependency["pkg"] for dependency in node.get("deps", []))
    components = [
        _component(packages[package_id], scope="direct" if package_id in direct else "transitive")
        for package_id in sorted(visited)
        if package_id in packages
    ]
    root_package = packages[root_id]
    return {
        "bomFormat": toolchain["format"],
        "specVersion": toolchain["spec_version"],
        "version": 1,
        "metadata": {
            "component": _component(root_package, scope="root"),
            "properties": [
                {"name": "rocketmq:candidate-id", "value": candidate["candidate_id"]},
                {"name": "rocketmq:generator", "value": toolchain["generator"]},
                {"name": "rocketmq:generator-version", "value": toolchain["generator_version"]},
                {"name": "rocketmq:remote-publication", "value": "not-executed"},
            ],
        },
        "components": components,
        "dependencies": [
            {
                "ref": f"{package_name}@{version}",
                "dependsOn": [f"{packages[value]['name']}@{packages[value]['version']}" for value in sorted(direct)],
            }
        ],
    }


def _application_sbom(
    candidate: dict[str, Any],
    *,
    name: str,
    components: list[dict[str, Any]],
    toolchain: dict[str, Any],
    ownership: str,
) -> dict[str, Any]:
    return {
        "bomFormat": toolchain["format"],
        "specVersion": toolchain["spec_version"],
        "version": 1,
        "metadata": {
            "component": {
                "type": "application",
                "name": name,
                "version": candidate["version"],
                "licenses": [{"expression": "Apache-2.0"}],
            },
            "properties": [
                {"name": "rocketmq:candidate-id", "value": candidate["candidate_id"]},
                {"name": "rocketmq:ownership", "value": ownership},
                {"name": "rocketmq:generator", "value": toolchain["generator"]},
                {"name": "rocketmq:generator-version", "value": toolchain["generator_version"]},
                {"name": "rocketmq:remote-publication", "value": "not-executed"},
            ],
        },
        "components": components,
    }


def generate(candidate_manifest: Path, toolchain_path: Path) -> Path:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    toolchain = read_json(resolve_existing_file(toolchain_path, "SBOM toolchain"))
    if toolchain.get("generator_version") != "1.0.0" or "latest" in json.dumps(toolchain).lower():
        raise ArchiveError("SBOM toolchain version is not frozen")
    plan = read_json(resolve_existing_file(root / "PACKAGE_PLAN.json", "package-only report"))
    staged = plan.get("staged_packages", [])
    if plan.get("mode") != "package-only" or plan.get("registry_publish_count") != 24 or len(staged) != 24:
        raise ArchiveError("package-only SBOM denominator must contain 24 staged crates")
    metadata = _cargo_metadata()
    outputs: list[dict[str, str]] = []
    output_root = root / "sbom"
    if output_root.exists():
        raise ArchiveError(f"external SBOM root already exists: {output_root}")
    for package in staged:
        crate = resolve_existing_file(root / package["crate_path"], f"{package['name']} crate")
        sbom = _package_sbom(candidate, package["name"], package["version"], metadata, toolchain)
        output = output_root / "crates" / f"{package['name']}.cdx.json"
        write_json(output, sbom)
        outputs.append({"kind": "crate", "owner": package["name"], "input": candidate_relative(root, crate, "crate"), "path": candidate_relative(root, output, "crate SBOM")})
    release_layout = load_layout()
    for target in release_layout["targets"]:
        manifest = root / "archives" / f"rocketmq-rust-{candidate['version']}-{target}.manifest.json"
        archive_manifest = read_json(resolve_existing_file(manifest, f"{target} archive manifest"))
        archive = resolve_candidate_path(root, archive_manifest["archive"], f"{target} archive")
        if not archive.is_file():
            raise ArchiveError(f"archive is missing: {target}")
        components = [
            {
                "type": "application",
                "name": entry["component"],
                "version": candidate["version"],
                "licenses": [{"expression": "Apache-2.0"}],
                "properties": [
                    {"name": "rocketmq:requested-features", "value": ",".join(entry["requested_features"])},
                    {"name": "rocketmq:effective-features", "value": ",".join(entry["effective_features"])},
                ],
            }
            for entry in archive_manifest["binaries"]
        ]
        output = output_root / "archives" / target / "archive.cdx.json"
        write_json(
            output,
            _application_sbom(
                candidate,
                name=f"rocketmq-rust-{target}",
                components=components,
                toolchain=toolchain,
                ownership=target,
            ),
        )
        outputs.append({"kind": "archive", "owner": target, "input": candidate_relative(root, archive, "archive"), "path": candidate_relative(root, output, "archive SBOM")})
    for service in SERVICES:
        manifest = root / "oci-layout" / service / "OCI_CANDIDATE_MANIFEST.json"
        image = read_json(resolve_existing_file(manifest, f"{service} OCI manifest"))
        if image.get("remote_publication") != "not-executed" or image.get("version") != candidate["version"]:
            raise ArchiveError(f"OCI SBOM input is inconsistent: {service}")
        components = [
            {
                "type": "application",
                "name": service,
                "version": candidate["version"],
                "licenses": [{"expression": "Apache-2.0"}],
                "properties": [
                    {"name": "rocketmq:artifact-id", "value": image["artifact_id"]},
                    {"name": "rocketmq:service", "value": service},
                ],
            }
        ]
        output = output_root / "oci" / service / "image.cdx.json"
        write_json(output, _application_sbom(candidate, name=f"rocketmq-rust-{service}", components=components, toolchain=toolchain, ownership=service))
        outputs.append({"kind": "oci", "owner": service, "input": candidate_relative(root, manifest, "OCI manifest"), "path": candidate_relative(root, output, "OCI SBOM")})
    if len(outputs) != 31:
        raise ArchiveError(f"external SBOM denominator changed: {len(outputs)}")
    index = output_root / "SBOM_INDEX.json"
    write_json(
        index,
        {
            "schema_version": 1,
            "candidate_id": candidate["candidate_id"],
            "version": candidate["version"],
            "outputs": outputs,
            "remote_publication": "not-executed",
        },
    )
    register_artifacts(candidate_manifest, [{"id": "release-sbom-index", "kind": "sbom-index", "path": index}])
    return index


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--toolchain", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        output = generate(args.candidate_manifest, args.toolchain)
        print(f"RELEASE_SBOM_OK outputs=31 index={output} remote_publication=not-executed")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError) as error:
        print(f"RELEASE_SBOM_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
