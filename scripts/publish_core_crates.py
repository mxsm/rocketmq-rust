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

"""Verify or publish core crates and Dashboard common, resuming partial releases safely."""

from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.request

import core_release_scope


ROOT = Path(os.environ.get("RELEASE_SOURCE_ROOT", Path(__file__).resolve().parents[1])).resolve()
USER_AGENT = "rocketmq-rust-release (https://github.com/mxsm/rocketmq-rust)"
# Dashboard common is publishable while remaining outside the core architecture boundary.
ADDITIONAL_WORKSPACE_PACKAGES = ("rocketmq-dashboard-common",)


class ReleaseError(ValueError):
    """A release input or registry state is unsuitable for publication."""


def release_packages(root: Path, tag: str) -> tuple[str, list[str]]:
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ReleaseError("release tag must be a stable version such as v1.0.0")
    workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    version = workspace["workspace"]["package"]["version"]
    if tag != f"v{version}":
        raise ReleaseError(f"tag {tag} does not match workspace version {version}")
    scope = core_release_scope.load_scope(root / "scripts/core-release-scope.json")
    metadata = core_release_scope.collect_metadata(root)
    packages = {p["name"]: p for p in metadata["packages"] if p["id"] in metadata["workspace_members"]}
    selected = []
    for entry in scope["core_packages"]:
        if entry["classification"] != "registry-publish":
            continue
        package = packages.get(entry["name"])
        if package is None or package["version"] != version or package["publish"] == []:
            raise ReleaseError(f"invalid publishable package: {entry['name']}")
        selected.append(entry["name"])
    if not selected:
        raise ReleaseError("core release scope contains no registry packages")
    for name in ADDITIONAL_WORKSPACE_PACKAGES:
        package = packages.get(name)
        if package is None or package["version"] != version or package["publish"] == []:
            raise ReleaseError(f"invalid publishable workspace package: {name}")
        if name not in selected:
            selected.append(name)
    return version, selected


def fetch(url: str, *, missing_ok: bool = False) -> bytes | None:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                content = response.read(20 * 1024 * 1024 + 1)
                if len(content) > 20 * 1024 * 1024:
                    raise ReleaseError("registry response exceeds the release download limit")
                return content
        except urllib.error.HTTPError as error:
            error.close()
            if missing_ok and error.code == 404:
                return None
            if error.code not in {429, 500, 502, 503, 504} or attempt == 2:
                raise ReleaseError(f"registry request failed: HTTP {error.code}") from error
        except (urllib.error.URLError, TimeoutError, OSError) as error:
            if attempt == 2:
                raise ReleaseError("registry request failed after three attempts") from error
        time.sleep(2 ** attempt)
    raise ReleaseError("registry request did not complete")


def version_exists(name: str, version: str, commit: str | None) -> bool:
    content = fetch(f"https://crates.io/api/v1/crates/{name}/{version}", missing_ok=True)
    if content is None:
        return False
    published = json.loads(content)["version"]
    if published["yanked"]:
        raise ReleaseError(f"{name} {version} is yanked; a version cannot be republished")
    if commit:
        archive = fetch(f"https://static.crates.io/crates/{name}/{name}-{version}.crate")
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as crate:
            info = crate.extractfile(f"{name}-{version}/.cargo_vcs_info.json")
            if info is None:
                raise ReleaseError(f"cannot verify source commit for existing {name} {version}")
            vcs = json.load(info)
        if vcs["git"]["sha1"] != commit or vcs["git"].get("dirty", False):
            raise ReleaseError(f"existing {name} {version} was published from a different or dirty source")
    return True


def cargo_publish(root: Path, packages: list[str], *, dry_run: bool) -> None:
    command = ["cargo", "publish", "--locked", "--registry", "crates-io"]
    if dry_run:
        command.append("--dry-run")
    for name in packages:
        command.extend(["--package", name])
    subprocess.run(command, cwd=root, check=True)


def execute(root: Path, tag: str, mode: str, commit: str | None, output: Path) -> dict:
    if commit and not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ReleaseError("source commit must be a full lowercase commit SHA")
    if commit:
        actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
        if actual != commit:
            raise ReleaseError("checkout does not match the requested source commit")
    if mode == "publish":
        if not commit:
            raise ReleaseError("publication requires --source-commit")
        if not os.environ.get("CARGO_REGISTRY_TOKEN"):
            raise ReleaseError("CARGO_REGISTRY_TOKEN is required for publication")
    version, packages = release_packages(root, tag)
    result = {"version": version, "source_commit": commit,
              "tooling_commit": os.environ.get("RELEASE_TOOLING_COMMIT"),
              "packages": packages, "mode": mode}
    if mode == "verify":
        cargo_publish(root, packages, dry_run=True)
        result["verified"] = packages
    else:
        # Query the entire release before writing anything; registry failures are not absence.
        existing = [name for name in packages if version_exists(name, version, commit)]
        pending = [name for name in packages if name not in existing]
        result.update(existing=existing, pending=pending)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        if mode == "publish" and pending:
            # Cargo 1.95 handles dependency ordering and waits for registry index availability.
            cargo_publish(root, pending, dry_run=False)
            result["published"] = pending
        elif mode == "publish":
            result["published"] = []
            print(f"All {len(packages)} selected workspace crates already published from this source")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--mode", choices=("plan", "verify", "publish"), default="verify")
    parser.add_argument("--source-commit")
    parser.add_argument("--output", type=Path, default=ROOT / "target/release/crates.json")
    args = parser.parse_args()
    try:
        result = execute(ROOT, args.tag, args.mode, args.source_commit, args.output)
    except (ReleaseError, OSError, KeyError, ValueError, tarfile.TarError, subprocess.CalledProcessError) as error:
        print(f"CRATE_RELEASE_FAILED: {error}", file=sys.stderr)
        return 1
    print(f"CRATE_RELEASE_OK mode={args.mode} version={result['version']} packages={len(result['packages'])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
