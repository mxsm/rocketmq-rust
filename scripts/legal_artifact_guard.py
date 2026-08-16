#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Verify legal metadata in every local core candidate crate, archive, and OCI layer."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tarfile
import zipfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from build_core_oci_layout import SERVICES
from release_archive_common import ArchiveError, load_candidate, load_layout, resolve_candidate_path
from release_state import read_json, resolve_existing_file


LEGAL = {"LICENSE-APACHE", "NOTICE"}


def _crate_legal(crate: Path, name: str, version: str) -> bool:
    with tarfile.open(crate, "r:gz") as archive:
        names = set(archive.getnames())
    root = f"{name}-{version}"
    return all(f"{root}/{legal}" in names for legal in LEGAL)


def _archive_legal(archive: Path) -> bool:
    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as value:
            names = set(value.namelist())
    else:
        with tarfile.open(archive, "r:gz") as value:
            names = set(value.getnames())
    return all(any(name.endswith(f"/{legal}") for name in names) for legal in LEGAL)


def _oci_legal(layout: Path) -> bool:
    # OCI descriptor names are required to locate blobs. They are structural
    # protocol fields and are never used as this release guard's pass/fail hash.
    index = read_json(resolve_existing_file(layout / "index.json", "OCI index"))
    descriptor = index.get("manifests", [None])[0]
    if not isinstance(descriptor, dict):
        return False
    manifest_digest = descriptor.get("digest", "").partition(":")[2]
    manifest = read_json(resolve_existing_file(layout / "blobs" / "sha256" / manifest_digest, "OCI manifest blob"))
    layer = manifest.get("layers", [None])[0]
    if not isinstance(layer, dict):
        return False
    layer_digest = layer.get("digest", "").partition(":")[2]
    with tarfile.open(resolve_existing_file(layout / "blobs" / "sha256" / layer_digest, "OCI layer"), "r") as archive:
        names = set(archive.getnames())
    return LEGAL.issubset(names)


def audit(candidate_manifest: Path) -> list[str]:
    findings: list[str] = []
    _manifest, candidate, root = load_candidate(candidate_manifest)
    plan = read_json(resolve_existing_file(root / "PACKAGE_PLAN.json", "package-only report"))
    staged = plan.get("staged_packages", [])
    if plan.get("registry_publish_count") != 24 or len(staged) != 24:
        findings.append("crate legal denominator must contain exactly 24 staged packages")
    for package in staged:
        try:
            crate = resolve_candidate_path(root, package["crate_path"], f"{package.get('name')} crate")
            if not crate.is_file() or not _crate_legal(crate, package["name"], package["version"]):
                findings.append(f"crate legal files are incomplete: {package.get('name')}")
        except (ArchiveError, OSError, KeyError, tarfile.TarError) as error:
            findings.append(f"crate legal inspection failed: {package.get('name')}: {error}")
    layout = load_layout()
    for target in layout["targets"]:
        manifest = root / "archives" / f"rocketmq-rust-{candidate['version']}-{target}.manifest.json"
        try:
            value = read_json(resolve_existing_file(manifest, f"{target} archive manifest"))
            archive = resolve_candidate_path(root, value["archive"], f"{target} archive")
            if not archive.is_file() or not _archive_legal(archive):
                findings.append(f"archive legal files are incomplete: {target}")
        except (ArchiveError, OSError, KeyError, tarfile.TarError, zipfile.BadZipFile) as error:
            findings.append(f"archive legal inspection failed: {target}: {error}")
    for service in SERVICES:
        try:
            directory = resolve_candidate_path(root, f"oci-layout/{service}", f"{service} OCI layout")
            if not directory.is_dir() or not _oci_legal(directory):
                findings.append(f"OCI legal files are incomplete: {service}")
        except (ArchiveError, OSError, KeyError, IndexError, tarfile.TarError) as error:
            findings.append(f"OCI legal inspection failed: {service}: {error}")
    identity = read_json(ROOT / "distribution" / "release-identity.json")
    if identity.get("identity_kind") != "unofficial-community" or identity.get("official_apache_release") is not False:
        findings.append("candidate legal identity is not the approved unofficial community distribution")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", choices=["core-release"], required=True)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        findings = audit(args.candidate_manifest)
    except (ArchiveError, OSError, json.JSONDecodeError) as error:
        findings = [str(error)]
    if findings:
        for finding in findings:
            print(f"LEGAL_ARTIFACT_GUARD_FAILED detail={finding}", file=sys.stderr)
        return 1
    print("LEGAL_ARTIFACT_GUARD_OK crates=24 archives=3 oci=4 distribution=unofficial-community")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
