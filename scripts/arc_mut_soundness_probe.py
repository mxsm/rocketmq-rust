#!/usr/bin/env python3
# Copyright 2023 The RocketMQ Rust Authors
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

"""Run the bounded ArcMut Miri audit and the safe replacement Loom model."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Sequence


class ProbeFailure(RuntimeError):
    """Raised when a soundness probe produces an unexpected result."""


def combined_output(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}\n{result.stderr}".strip()


def validate_guarded_result(result: subprocess.CompletedProcess[str]) -> None:
    if result.returncode != 0:
        raise ProbeFailure(
            "the guarded backing path failed under Miri:\n"
            f"{combined_output(result)[-4000:]}"
        )


def validate_alias_result(result: subprocess.CompletedProcess[str]) -> None:
    output = combined_output(result)
    if result.returncode == 0:
        raise ProbeFailure("the clone-safe mutable alias probe unexpectedly passed under Miri")
    if "Undefined Behavior" not in output:
        raise ProbeFailure(
            "the alias probe failed without Miri's undefined-behavior diagnostic:\n"
            f"{output[-4000:]}"
        )
    if not any(marker in output for marker in ("borrow stack", "Stacked Borrows", "Unique retag")):
        raise ProbeFailure(
            "the alias probe did not report the expected reference-aliasing evidence:\n"
            f"{output[-4000:]}"
        )


def run(command: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=cwd,
        env=os.environ.copy(),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
        check=False,
    )


def write_probe(root: Path) -> tuple[Path, str]:
    inputs = (root / "rocketmq" / "Cargo.toml").read_bytes()
    inputs += (root / "rocketmq" / "src" / "arc_mut.rs").read_bytes()
    fingerprint = hashlib.sha256(inputs).hexdigest()[:16]
    probe_root = (
        root
        / "target"
        / "architecture-refactor"
        / "M11"
        / "arc-mut-soundness"
        / fingerprint
    )
    source_dir = probe_root / "src"
    source_dir.mkdir(parents=True, exist_ok=True)

    dependency_path = (root / "rocketmq").resolve().as_posix()
    manifest = f"""[package]
name = "arc-mut-miri-probe"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
rocketmq-rust = {{ path = "{dependency_path}" }}

[workspace]
"""
    source = """use rocketmq_rust::ArcMut;

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("guarded") => {
            let value = ArcMut::new(1_u64);
            *value.get_inner().write() += 1;
            assert_eq!(*value.get_inner().read(), 2);
        }
        Some("alias") => {
            let mut left = ArcMut::new(0_u64);
            let mut right = left.clone();
            let left_ref = &mut *left;
            let right_ref = &mut *right;
            *left_ref = 1;
            *right_ref = 2;
            assert_eq!(*left_ref, 2);
        }
        _ => panic!("expected guarded or alias probe"),
    }
}
"""
    (probe_root / "Cargo.toml").write_text(manifest, encoding="utf-8")
    (source_dir / "main.rs").write_text(source, encoding="utf-8")
    return probe_root, fingerprint


def miri_command(manifest: Path, mode: str, locked: bool) -> list[str]:
    command = [
        "cargo",
        "+nightly",
        "miri",
        "run",
        "--manifest-path",
        str(manifest),
    ]
    if locked:
        command.append("--locked")
    command.extend(["--", mode])
    return command


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit ArcMut with Miri and model its guarded replacement with Loom."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="repository root",
    )
    parser.add_argument("--skip-loom", action="store_true")
    args = parser.parse_args()

    root = args.root.resolve()
    probe_root, fingerprint = write_probe(root)
    manifest = probe_root / "Cargo.toml"
    lock_exists = (probe_root / "Cargo.lock").exists()

    guarded_command = miri_command(manifest, "guarded", lock_exists)
    guarded = run(guarded_command, root)
    (probe_root / "guarded.log").write_text(combined_output(guarded), encoding="utf-8")
    validate_guarded_result(guarded)

    alias_command = miri_command(manifest, "alias", True)
    alias = run(alias_command, root)
    (probe_root / "alias.log").write_text(combined_output(alias), encoding="utf-8")
    validate_alias_result(alias)

    loom_command = [
        "cargo",
        "test",
        "-p",
        "rocketmq-rust",
        "--test",
        "arc_mut_replacement_loom",
    ]
    loom: subprocess.CompletedProcess[str] | None = None
    if not args.skip_loom:
        loom = run(loom_command, root)
        (probe_root / "loom.log").write_text(combined_output(loom), encoding="utf-8")
        if loom.returncode != 0:
            raise ProbeFailure(
                "the guarded replacement Loom model failed:\n"
                f"{combined_output(loom)[-4000:]}"
            )

    summary = {
        "schema_version": 1,
        "fingerprint": fingerprint,
        "guarded_miri": {"exit_code": guarded.returncode, "result": "pass"},
        "alias_miri": {
            "exit_code": alias.returncode,
            "result": "expected_undefined_behavior",
        },
        "loom": {
            "exit_code": None if loom is None else loom.returncode,
            "result": "skipped" if loom is None else "pass",
        },
    }
    (probe_root / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(
        "ARC_MUT_SOUNDNESS_PROBE_OK "
        f"fingerprint={fingerprint} guarded=pass alias=expected_ub "
        f"loom={summary['loom']['result']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ProbeFailure as error:
        print(f"ARC_MUT_SOUNDNESS_PROBE_FAILED: {error}", file=sys.stderr)
        raise SystemExit(1)
