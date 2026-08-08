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

"""Run one Rust integration-test target and reject empty or all-ignored runs."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence


REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
LISTED_TEST = re.compile(r"^.+: test$", re.MULTILINE)
TEST_SUMMARY = re.compile(
    r"test result: (?:ok|FAILED)\. "
    r"(?P<passed>\d+) passed; "
    r"(?P<failed>\d+) failed; "
    r"(?P<ignored>\d+) ignored;"
)
FORBIDDEN_CARGO_ARG_NAMES = {
    "--all",
    "--all-targets",
    "--bench",
    "--benches",
    "--bin",
    "--bins",
    "--doc",
    "--example",
    "--examples",
    "--exclude",
    "--lib",
    "--manifest-path",
    "--package",
    "--test",
    "--tests",
    "--workspace",
    "-p",
    "--",
}


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True, help="Cargo package name")
    parser.add_argument("--target", required=True, help="Integration-test target name")
    parser.add_argument("--features", help="Comma-separated Cargo feature list")
    parser.add_argument(
        "--cargo-arg",
        action="append",
        default=[],
        help="Additional Cargo argument before the test-harness separator; repeat as needed",
    )
    args = parser.parse_args(argv)
    for cargo_arg in args.cargo_arg:
        name = cargo_arg.split("=", 1)[0]
        if name in FORBIDDEN_CARGO_ARG_NAMES or (cargo_arg.startswith("-p") and cargo_arg != "-p"):
            parser.error(f"--cargo-arg may not select an additional test harness: {cargo_arg}")
    return args


def cargo_test_command(args: argparse.Namespace) -> list[str]:
    command = ["cargo", "test", "-p", args.package, "--test", args.target]
    if args.features:
        command.extend(["--features", args.features])
    command.extend(args.cargo_arg)
    return command


def run(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        cwd=REPOSITORY_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    return result


def fail(message: str) -> int:
    print(f"RUST_TEST_TARGET_FAILED: {message}", file=sys.stderr)
    return 1


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    command = cargo_test_command(args)
    list_result = run([*command, "--", "--list", "--format", "terse"])
    if list_result.returncode != 0:
        return list_result.returncode

    listed_count = len(LISTED_TEST.findall(list_result.stdout))
    if listed_count == 0:
        return fail(f"no tests were listed for {args.package}::{args.target}")

    execute_result = run(command)
    if execute_result.returncode != 0:
        return execute_result.returncode

    summaries = list(TEST_SUMMARY.finditer(execute_result.stdout + execute_result.stderr))
    if not summaries:
        return fail(f"test result summary was not found for {args.package}::{args.target}")

    passed = sum(int(summary.group("passed")) for summary in summaries)
    ignored = sum(int(summary.group("ignored")) for summary in summaries)
    if passed == 0:
        return fail(f"no tests passed for {args.package}::{args.target}; ignored={ignored}")

    print(
        "RUST_TEST_TARGET_OK "
        f"package={args.package} target={args.target} listed={listed_count} "
        f"passed={passed} ignored={ignored}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
