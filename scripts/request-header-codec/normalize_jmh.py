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

"""Normalize JMH JSON output into the cross-runtime performance schema."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any


UNIT_TO_NANOS = {
    "ns/op": 1.0,
    "us/op": 1_000.0,
    "ms/op": 1_000_000.0,
    "s/op": 1_000_000_000.0,
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def flatten_raw(metric: dict[str, Any]) -> tuple[list[float], list[float]]:
    unit = metric.get("scoreUnit")
    if unit not in UNIT_TO_NANOS:
        raise ValueError(f"unsupported JMH score unit: {unit}")
    factor = UNIT_TO_NANOS[unit]
    raw_data = metric.get("rawData")
    if not isinstance(raw_data, list) or not raw_data:
        raise ValueError("JMH primaryMetric.rawData is required")
    forks: list[float] = []
    flattened: list[float] = []
    for fork in raw_data:
        if not isinstance(fork, list) or not fork:
            raise ValueError("JMH rawData must retain every non-empty fork")
        values = [float(value) * factor for value in fork]
        flattened.extend(values)
        forks.append(statistics.median(values))
    return forks, flattened


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--profile", choices=("gate", "diagnostic"), required=True)
    parser.add_argument("--forks", type=int, required=True)
    parser.add_argument("--warmup-iterations", type=int, required=True)
    parser.add_argument("--measurement-iterations", type=int, required=True)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()

    corpus = read_json(args.corpus)
    cases = {case["id"]: case for case in corpus["cases"] if case["gateWeight"] > 0}
    normalized: dict[str, dict[str, Any]] = {}
    for result in read_json(args.raw):
        benchmark = result["benchmark"].rsplit(".", 1)[-1]
        if benchmark == "encodeProductionHeader":
            operation = "encode"
        elif benchmark == "decodeProductionHeader":
            operation = "decode"
        else:
            continue
        fixture_id = result.get("params", {}).get("fixtureId")
        case_id = f"{fixture_id}-{operation}"
        if case_id not in cases:
            raise ValueError(f"JMH produced an unknown corpus case: {case_id}")
        if case_id in normalized:
            raise ValueError(f"duplicate JMH result for {case_id}")
        fork_medians, raw = flatten_raw(result["primaryMetric"])
        gc_metric = result.get("secondaryMetrics", {}).get("gc.alloc.rate.norm", {})
        normalized[case_id] = {
            "id": case_id,
            "operation": operation,
            "serializeType": cases[case_id]["serializeType"],
            "tier": cases[case_id]["tier"],
            "gateWeight": cases[case_id]["gateWeight"],
            "fastGateWeight": cases[case_id].get("fastGateWeight", 0.0),
            "medianLatencyNanos": statistics.median(fork_medians),
            "forkMedianLatencyNanos": fork_medians,
            "rawLatencyNanos": raw,
            "allocatedBytesPerOperation": gc_metric.get("score"),
        }

    missing = sorted(set(cases) - set(normalized))
    if missing and not args.allow_partial:
        raise ValueError(f"JMH corpus mismatch; missing={missing}")
    output = {
        "schemaVersion": 1,
        "runtime": "java",
        "codec": "RocketMQ production request-header codec",
        "role": "java-pinned",
        "commit": args.commit,
        "profile": args.profile,
        "releasable": args.profile == "gate" and not args.allow_partial,
        "jmh": {
            "forks": args.forks,
            "warmupIterations": args.warmup_iterations,
            "measurementIterations": args.measurement_iterations,
        },
        "corpusVersion": corpus["corpusVersion"],
        "corpusSha256": digest(args.corpus),
        "fixtureManifestSha256": digest(args.fixture_manifest),
        "runnerFingerprintSha256": digest(args.runner),
        "runner": read_json(args.runner),
        "sampleCount": len(normalized),
        "missingCases": missing,
        "samples": [normalized[key] for key in sorted(normalized)],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
