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

"""Generate and verify the default-feature public API snapshot from rustdoc JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
LIB_KINDS = {"lib", "rlib", "proc-macro"}


class SnapshotError(RuntimeError):
    pass


def run(command: list[str]) -> str:
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        raise SnapshotError(
            f"command failed ({result.returncode}): {' '.join(command)}\n{result.stdout}{result.stderr}"
        )
    return result.stdout.strip()


def workspace_library_targets() -> list[tuple[str, str]]:
    metadata = json.loads(run(["cargo", "metadata", "--format-version", "1", "--no-deps"]))
    members = set(metadata["workspace_members"])
    targets: list[tuple[str, str]] = []
    for package in metadata["packages"]:
        if package["id"] not in members:
            continue
        library_targets = [
            target
            for target in package["targets"]
            if LIB_KINDS.intersection(target["kind"])
        ]
        if len(library_targets) > 1:
            raise SnapshotError(f"{package['name']} has multiple library targets")
        if library_targets:
            targets.append((package["name"], library_targets[0]["name"]))
    return sorted(targets)


def toolchain() -> dict[str, str]:
    return {
        "rustc": run(["rustc", "--version"]),
        "rustdoc": run(["rustdoc", "--version"]),
        "cargo": run(["cargo", "--version"]),
    }


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def public_path_fingerprint(document: dict[str, Any]) -> tuple[int, str]:
    paths = sorted(
        f"{item['kind']} {'::'.join(item['path'])}"
        for item in document["paths"].values()
        if item["crate_id"] == 0
    )
    payload = ("\n".join(paths) + "\n").encode()
    return len(paths), sha256(payload)


def snapshot_package(package: str, target: str, *, refresh: bool = True) -> dict[str, Any]:
    if refresh:
        run(
            [
                "cargo",
                "rustdoc",
                "-p",
                package,
                "--lib",
                "--",
                "-Z",
                "unstable-options",
                "--output-format",
                "json",
            ]
        )
    rustdoc_json = ROOT / "target" / "doc" / f"{target}.json"
    if not rustdoc_json.is_file():
        raise SnapshotError(f"rustdoc JSON was not produced for {package}: {rustdoc_json}")
    raw = rustdoc_json.read_bytes()
    document = json.loads(raw)
    public_path_count, public_path_sha256 = public_path_fingerprint(document)
    return {
        "target": target,
        "crate_version": document.get("crate_version"),
        "public_path_count": public_path_count,
        "public_path_sha256": public_path_sha256,
        "rustdoc_json_sha256": sha256(raw),
    }


def validate_existing_artifacts(targets: list[tuple[str, str]]) -> None:
    dirty_api_inputs = run(
        [
            "git",
            "status",
            "--porcelain",
            "--",
            ":(glob)**/*.rs",
            ":(glob)**/Cargo.toml",
            "Cargo.lock",
            "rust-toolchain.toml",
        ]
    )
    if dirty_api_inputs:
        raise SnapshotError(
            "--from-existing cannot verify dirty Rust/API inputs; refresh after committing or stashing them"
        )

    commit_timestamp = int(run(["git", "show", "-s", "--format=%ct", "HEAD"]))
    missing = []
    stale = []
    for package, target in targets:
        rustdoc_json = ROOT / "target" / "doc" / f"{target}.json"
        if not rustdoc_json.is_file():
            missing.append(package)
        elif rustdoc_json.stat().st_mtime < commit_timestamp:
            stale.append(package)
    if missing or stale:
        raise SnapshotError(
            "--from-existing requires artifacts refreshed after HEAD; "
            f"missing={','.join(missing) or '-'} stale={','.join(stale) or '-'}"
        )


def generate_snapshot(*, refresh: bool = True) -> dict[str, Any]:
    targets = workspace_library_targets()
    if not refresh:
        validate_existing_artifacts(targets)
    packages = {}
    for index, (package, target) in enumerate(targets, start=1):
        print(f"PUBLIC_API_SNAPSHOT_PACKAGE {index}/{len(targets)} {package}", flush=True)
        packages[package] = snapshot_package(package, target, refresh=refresh)
    return {
        "schema_version": SCHEMA_VERSION,
        "source_commit": run(["git", "rev-parse", "HEAD"]),
        "feature_profile": "default",
        "toolchain": toolchain(),
        "packages": packages,
    }


def compare_snapshots(baseline: dict[str, Any], candidate: dict[str, Any]) -> list[dict[str, Any]]:
    differences: list[dict[str, Any]] = []
    if baseline.get("schema_version") != SCHEMA_VERSION:
        differences.append({"kind": "schema", "expected": SCHEMA_VERSION, "actual": baseline.get("schema_version")})
    if baseline.get("feature_profile") != candidate.get("feature_profile"):
        differences.append(
            {
                "kind": "feature-profile",
                "expected": baseline.get("feature_profile"),
                "actual": candidate.get("feature_profile"),
            }
        )
    if baseline.get("toolchain") != candidate.get("toolchain"):
        differences.append(
            {"kind": "toolchain", "expected": baseline.get("toolchain"), "actual": candidate.get("toolchain")}
        )

    baseline_packages = baseline.get("packages", {})
    candidate_packages = candidate.get("packages", {})
    for package in sorted(set(baseline_packages) | set(candidate_packages)):
        before = baseline_packages.get(package)
        after = candidate_packages.get(package)
        if before is None:
            differences.append({"kind": "package-added", "package": package, "classification": "unclassified"})
        elif after is None:
            differences.append({"kind": "package-removed", "package": package, "classification": "breaking"})
        elif before != after:
            differences.append(
                {
                    "kind": "surface-changed",
                    "package": package,
                    "classification": "unclassified",
                    "expected": before,
                    "actual": after,
                }
            )
    return differences


def write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write-baseline", type=Path)
    mode.add_argument("--check", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--from-existing",
        action="store_true",
        help="assemble the snapshot from rustdoc JSON already refreshed for this source commit",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        candidate = generate_snapshot(refresh=not args.from_existing)
        if args.write_baseline:
            write_json(args.write_baseline, candidate)
            print(
                f"PUBLIC_API_BASELINE_WRITTEN packages={len(candidate['packages'])} "
                f"path={args.write_baseline.as_posix()}"
            )
            return 0

        baseline = json.loads(args.check.read_text(encoding="utf-8"))
        differences = compare_snapshots(baseline, candidate)
        report = {
            "schema_version": SCHEMA_VERSION,
            "baseline_commit": baseline.get("source_commit"),
            "candidate_commit": candidate["source_commit"],
            "packages": len(candidate["packages"]),
            "differences": differences,
            "status": "compatible" if not differences else "review-required",
        }
        if args.output:
            write_json(args.output, report)
        if differences:
            print(json.dumps(report, indent=2, sort_keys=True))
            return 1
        print(f"PUBLIC_API_SNAPSHOT_OK packages={len(candidate['packages'])} differences=0")
        return 0
    except (OSError, json.JSONDecodeError, SnapshotError) as error:
        print(f"PUBLIC_API_SNAPSHOT_FAILED {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
