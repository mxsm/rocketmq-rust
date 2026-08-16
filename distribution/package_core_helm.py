#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Package the core Helm chart locally for one explicit release candidate."""

from __future__ import annotations

import argparse
import gzip
import io
import json
from pathlib import Path
import re
import sys
import tarfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "distribution") not in sys.path:
    sys.path.insert(0, str(ROOT / "distribution"))

from release_archive_common import ArchiveError, candidate_relative, load_candidate, write_json
from release_state import atomic_write_json, read_json, resolve_existing_file


REQUIRED_FILES = {
    "Chart.yaml",
    "values.yaml",
    "values.schema.json",
    "values-dev-single.yaml",
    "values-production-default-ha.yaml",
    "values-production-controller-ha.yaml",
    "values-production-proxy-tls.yaml",
    "templates/_helpers.tpl",
    "templates/configmaps.yaml",
    "templates/workloads.yaml",
    "templates/services.yaml",
    "templates/networkpolicies.yaml",
}


def _render(relative: str, content: bytes, version: str) -> bytes:
    if relative == "Chart.yaml":
        text = content.decode()
        text = re.sub(r"(?m)^version: .+$", f"version: {version}", text)
        text = re.sub(r'(?m)^appVersion: .+$', f'appVersion: "{version}"', text)
        return text.encode()
    if relative == "values.yaml":
        return re.sub(
            rb'(?m)^  candidateVersion: ".+"$',
            f'  candidateVersion: "{version}"'.encode(),
            content,
        )
    return content


def package_chart(candidate_manifest: Path, chart: Path) -> tuple[Path, Path]:
    _manifest, candidate, root = load_candidate(candidate_manifest)
    chart = chart.resolve()
    if not chart.is_dir():
        raise ArchiveError(f"core chart does not exist: {chart}")
    files = {
        path.relative_to(chart).as_posix(): path
        for path in chart.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if set(files) != REQUIRED_FILES:
        raise ArchiveError(
            f"core chart file set changed: missing={sorted(REQUIRED_FILES - set(files))} "
            f"extra={sorted(set(files) - REQUIRED_FILES)}"
        )
    output_root = root / "helm"
    output_root.mkdir(parents=True, exist_ok=True)
    package = output_root / f"rocketmq-rust-core-{candidate['version']}.tgz"
    if package.exists():
        raise ArchiveError(f"core chart package already exists: {package}")
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w") as archive:
            for relative, path in sorted(files.items()):
                content = _render(relative, path.read_bytes(), candidate["version"])
                info = tarfile.TarInfo(f"rocketmq-rust-core/{relative}")
                info.size = len(content)
                info.mode = 0o644
                info.mtime = 0
                archive.addfile(info, io.BytesIO(content))
    package.write_bytes(buffer.getvalue())
    manifest = output_root / f"rocketmq-rust-core-{candidate['version']}.manifest.json"
    manifest_value = {
        "schema_version": 1,
        "candidate_id": candidate["candidate_id"],
        "version": candidate["version"],
        "run_id": candidate["run_id"],
        "attempt": candidate["attempt"],
        "chart": "rocketmq-rust-core",
        "package": candidate_relative(root, package, "core Helm package"),
        "files": sorted(REQUIRED_FILES),
        "remote_publication": "not-executed",
    }
    write_json(manifest, manifest_value)
    artifact_index = root / "ARTIFACT_INDEX.json"
    index = read_json(resolve_existing_file(artifact_index, "candidate artifact index"))
    artifacts = index.get("artifacts")
    if not isinstance(artifacts, list):
        raise ArchiveError("candidate artifact index has no artifacts list")
    identifiers = {entry.get("id") for entry in artifacts if isinstance(entry, dict)}
    for identifier, kind, path in (
        ("helm-core", "helm-package", package),
        ("helm-core-manifest", "helm-manifest", manifest),
    ):
        if identifier in identifiers:
            raise ArchiveError(f"candidate artifact is already registered: {identifier}")
        artifacts.append(
            {
                "id": identifier,
                "kind": kind,
                "path": candidate_relative(root, path, identifier),
            }
        )
    atomic_write_json(artifact_index, index)
    return package, manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-manifest", type=Path, required=True)
    parser.add_argument("--chart", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        package, manifest = package_chart(args.candidate_manifest, args.chart)
        print(f"CORE_HELM_PACKAGE_OK package={package} manifest={manifest} remote_publication=not-executed")
        return 0
    except (ArchiveError, OSError, KeyError, json.JSONDecodeError, tarfile.TarError) as error:
        print(f"CORE_HELM_PACKAGE_FAILED detail={error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
