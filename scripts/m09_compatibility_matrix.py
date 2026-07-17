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

"""Run the frozen M09 public-feature, wire, and storage compatibility matrix."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class MatrixEntry:
    id: str
    group: str
    command: tuple[str, ...]


MATRIX = (
    MatrixEntry("protocol-no-default", "feature", ("cargo", "check", "-p", "rocketmq-protocol", "--no-default-features")),
    MatrixEntry("protocol-simd", "feature", ("cargo", "test", "-p", "rocketmq-protocol", "--features", "simd")),
    MatrixEntry("transport-no-default", "feature", ("cargo", "check", "-p", "rocketmq-transport", "--no-default-features")),
    MatrixEntry("transport-default-tls", "feature", ("cargo", "test", "-p", "rocketmq-transport")),
    MatrixEntry(
        "transport-observability",
        "feature",
        ("cargo", "check", "-p", "rocketmq-transport", "--no-default-features", "--features", "observability"),
    ),
    MatrixEntry("transport-all-features", "feature", ("cargo", "check", "-p", "rocketmq-transport", "--all-features")),
    MatrixEntry("store-no-default", "feature", ("cargo", "check", "-p", "rocketmq-store", "--no-default-features")),
    MatrixEntry("store-default", "feature", ("cargo", "check", "-p", "rocketmq-store")),
    MatrixEntry(
        "store-local-file",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "local_file_store"),
    ),
    MatrixEntry(
        "store-fast-load",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "fast-load"),
    ),
    MatrixEntry(
        "store-safe-load",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "safe-load"),
    ),
    MatrixEntry(
        "store-fast-safe-load",
        "feature",
        (
            "cargo",
            "check",
            "-p",
            "rocketmq-store",
            "--no-default-features",
            "--features",
            "fast-load,safe-load",
        ),
    ),
    MatrixEntry(
        "store-io-uring",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "io_uring"),
    ),
    MatrixEntry(
        "store-rocksdb",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "rocksdb_store"),
    ),
    MatrixEntry(
        "store-tiered",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "tieredstore"),
    ),
    MatrixEntry(
        "store-observability",
        "feature",
        ("cargo", "check", "-p", "rocketmq-store", "--no-default-features", "--features", "observability"),
    ),
    MatrixEntry("admin-no-default", "feature", ("cargo", "check", "-p", "rocketmq-admin-core", "--no-default-features")),
    MatrixEntry(
        "admin-client-adapter",
        "feature",
        ("cargo", "check", "-p", "rocketmq-admin-core", "--no-default-features", "--features", "client-adapter"),
    ),
    MatrixEntry("admin-default-legacy", "feature", ("cargo", "test", "-p", "rocketmq-admin-core")),
    MatrixEntry("proxy-no-default", "feature", ("cargo", "check", "-p", "rocketmq-proxy", "--no-default-features")),
    MatrixEntry("proxy-default-r0", "feature", ("cargo", "test", "-p", "rocketmq-proxy")),
    MatrixEntry(
        "proxy-observability",
        "feature",
        ("cargo", "check", "-p", "rocketmq-proxy", "--no-default-features", "--features", "observability"),
    ),
    MatrixEntry(
        "proxy-tiered",
        "feature",
        ("cargo", "check", "-p", "rocketmq-proxy", "--no-default-features", "--features", "tieredstore"),
    ),
    MatrixEntry("proxy-all-features", "feature", ("cargo", "check", "-p", "rocketmq-proxy", "--all-features")),
    MatrixEntry(
        "common-protocol-codec",
        "wire",
        ("cargo", "test", "-p", "rocketmq-common", "--test", "protocol_message_codec_compatibility"),
    ),
    MatrixEntry(
        "remoting-extraction",
        "wire",
        ("cargo", "test", "-p", "rocketmq-remoting", "--test", "protocol_extraction_compatibility"),
    ),
    MatrixEntry(
        "remoting-wire",
        "wire",
        ("cargo", "test", "-p", "rocketmq-remoting", "--test", "protocol_compatibility_tests"),
    ),
    MatrixEntry(
        "remoting-legacy-facades",
        "wire",
        ("cargo", "test", "-p", "rocketmq-remoting", "--test", "m04_compatibility_facades"),
    ),
    MatrixEntry(
        "proxy-canonical-legacy",
        "wire",
        ("cargo", "test", "-p", "rocketmq-proxy", "--test", "core_compatibility"),
    ),
    MatrixEntry(
        "runtime-canonical-legacy",
        "wire",
        ("cargo", "test", "-p", "rocketmq-rust", "--test", "runtime_compatibility"),
    ),
    MatrixEntry(
        "local-cq-20-byte",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store-local", "--test", "consume_queue_record"),
    ),
    MatrixEntry(
        "local-index-codec",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store-local", "--test", "index_codec"),
    ),
    MatrixEntry(
        "store-local-record-facade",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store", "--test", "m06_store_local_record_compatibility"),
    ),
    MatrixEntry(
        "store-local-facade",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store", "--test", "m06_store_local_compatibility"),
    ),
    MatrixEntry(
        "store-commitlog-facade",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store", "--test", "m06_store_local_commitlog_compatibility"),
    ),
    MatrixEntry(
        "store-commitlog-fail-closed",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store", "--test", "m06_commitlog_record_fail_closed"),
    ),
    MatrixEntry(
        "rocksdb-foundation",
        "storage",
        ("cargo", "test", "-p", "rocketmq-store", "--features", "rocksdb_store", "--test", "rocksdb_foundation_tests"),
    ),
    MatrixEntry(
        "rocksdb-semantics",
        "storage",
        (
            "cargo",
            "test",
            "-p",
            "rocketmq-store",
            "--features",
            "rocksdb_store",
            "--test",
            "rocksdb_store_semantics_tests",
        ),
    ),
    MatrixEntry(
        "broker-rocksdb",
        "storage",
        ("cargo", "test", "-p", "rocketmq-broker", "--features", "rocksdb_store", "rocksdb"),
    ),
    MatrixEntry(
        "broker-pop-consumer",
        "storage",
        ("cargo", "test", "-p", "rocketmq-broker", "--features", "rocksdb_store", "pop_consumer"),
    ),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", action="append", choices=("feature", "wire", "storage"))
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def write_report(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    selected_groups = set(args.group or ("feature", "wire", "storage"))
    selected = [entry for entry in MATRIX if entry.group in selected_groups]
    if args.list:
        for entry in selected:
            print(f"{entry.id}\t{entry.group}\t{' '.join(entry.command)}")
        return 0

    results = []
    for index, entry in enumerate(selected, start=1):
        print(f"M09_MATRIX_START {index}/{len(selected)} id={entry.id}", flush=True)
        started = time.monotonic()
        result = subprocess.run(
            entry.command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        elapsed = round(time.monotonic() - started, 3)
        results.append(
            {
                **asdict(entry),
                "command": list(entry.command),
                "duration_seconds": elapsed,
                "exit_code": result.returncode,
            }
        )
        if result.returncode != 0:
            print(result.stdout, end="")
            print(result.stderr, end="", file=sys.stderr)
            report = {"schema_version": SCHEMA_VERSION, "status": "failed", "results": results}
            if args.output:
                write_report(args.output, report)
            print(f"M09_MATRIX_FAILED id={entry.id} exit_code={result.returncode}", file=sys.stderr)
            return result.returncode
        print(f"M09_MATRIX_OK id={entry.id} duration_seconds={elapsed}", flush=True)

    report = {"schema_version": SCHEMA_VERSION, "status": "passed", "results": results}
    if args.output:
        write_report(args.output, report)
    print(f"M09_MATRIX_PASSED commands={len(results)} groups={','.join(sorted(selected_groups))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
