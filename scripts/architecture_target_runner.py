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

"""Dispatch the frozen M10 correctness and measurement commands."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Sequence

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
RUNNER_PATH = "scripts/architecture_target_runner.py"

CORRECTNESS_COMMANDS: dict[str, tuple[tuple[str, ...], ...]] = {
    "sync_flush_crash_recovery": (
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-store",
            "--test",
            "architecture_correctness",
            "sync_flush_crash_recovery",
            "--",
            "--exact",
            "--nocapture",
        ),
    ),
    "derived_replay_no_holes": (
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-store",
            "--test",
            "architecture_correctness",
            "derived_replay_no_holes",
            "--",
            "--exact",
            "--nocapture",
        ),
    ),
    "bounded_overload": (
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-transport",
            "--test",
            "client_server_lifecycle",
            "data_overload_rejects_without_closing_and_control_reserve_survives",
            "--",
            "--exact",
            "--nocapture",
        ),
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-transport",
            "--example",
            "architecture_network_performance_collector",
            "tests::scaled_overload_rejects_data_and_preserves_control_plane",
            "--",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ),
    ),
    "no_raw_commitlog_fallback": (
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-store",
            "--lib",
            "kv::compaction_store::tests::recovering_state_fails_closed_without_raw_commit_log_fallback",
            "--",
            "--exact",
            "--nocapture",
        ),
    ),
}

MEASUREMENT_COMMANDS: dict[tuple[str, str], tuple[str, ...]] = {
    ("local-append", "producers-1"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "local-append",
        "producers-1",
    ),
    ("local-append", "producers-8"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "local-append",
        "producers-8",
    ),
    ("local-append", "producers-32"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "local-append",
        "producers-32",
    ),
    ("sync-flush", "concurrency-64"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "sync-flush",
        "concurrency-64",
    ),
    ("local-pull", "batch-32"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "local-pull",
        "batch-32",
    ),
    ("rocks-pull", "batch-32"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--features",
        "rocksdb_store",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "rocks-pull",
        "batch-32",
    ),
    ("tiered-append", "batch-64"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--features",
        "tieredstore",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "tiered-append",
        "batch-64",
    ),
    ("tiered-pull", "cold-32"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--features",
        "tieredstore",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "tiered-pull",
        "cold-32",
    ),
    ("tiered-pull", "warm-32"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-store",
        "--features",
        "tieredstore",
        "--example",
        "architecture_store_performance_collector",
        "--",
        "tiered-pull",
        "warm-32",
    ),
    ("connection-soak", "mixed-tls-churn"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-transport",
        "--example",
        "architecture_network_performance_collector",
        "--",
        "connection-soak",
        "mixed-tls-churn",
    ),
    ("overload", "bounded-rejection"): (
        "cargo",
        "run",
        "--release",
        "--quiet",
        "-p",
        "rocketmq-transport",
        "--example",
        "architecture_network_performance_collector",
        "--",
        "overload",
        "bounded-rejection",
    ),
}


def manifest_command(mode: str, *identifiers: str) -> list[str]:
    """Return the portable command stored in a target sidecar manifest."""

    return ["python", RUNNER_PATH, mode, *identifiers]


def resolve_commands(arguments: Sequence[str]) -> tuple[tuple[str, ...], ...]:
    """Resolve one exact runner invocation or raise ``ValueError``."""

    if len(arguments) == 2 and arguments[0] == "correctness":
        check_id = arguments[1]
        try:
            return CORRECTNESS_COMMANDS[check_id]
        except KeyError as error:
            raise ValueError(f"unknown correctness check: {check_id}") from error
    if len(arguments) == 3 and arguments[0] == "measurement":
        key = (arguments[1], arguments[2])
        try:
            return (MEASUREMENT_COMMANDS[key],)
        except KeyError as error:
            raise ValueError(f"unknown measurement profile/variant: {key[0]}/{key[1]}") from error
    raise ValueError(
        "usage: architecture_target_runner.py correctness <check-id> | "
        "measurement <profile> <variant>"
    )


def run_commands(commands: Sequence[Sequence[str]]) -> int:
    """Run commands without a shell and stop on the first failure."""

    for command in commands:
        completed = subprocess.run(
            list(command),
            cwd=REPOSITORY_ROOT,
            check=False,
        )
        if completed.returncode != 0:
            return completed.returncode if completed.returncode > 0 else 1
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    try:
        commands = resolve_commands(arguments)
    except ValueError as error:
        print(f"ARCHITECTURE_TARGET_RUNNER_FAILED {error}", file=sys.stderr)
        return 2
    return run_commands(commands)


if __name__ == "__main__":
    raise SystemExit(main())
