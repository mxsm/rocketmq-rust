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

"""Reduce raw Criterion/JMH baseline artifacts to reviewable summary tables."""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def median(values: list[float]) -> float:
    if not values:
        raise ValueError("cannot summarize an empty sample")
    return float(statistics.median(values))


def criterion_cases(root: Path, required_runs: int) -> list[dict[str, Any]]:
    samples: dict[tuple[str, str], list[tuple[str, float, dict[str, Any] | None]]] = defaultdict(list)
    for sample_path in root.glob("rust/sample-*/*/criterion/**/new/sample.json"):
        relative = sample_path.relative_to(root)
        run_id = relative.parts[1]
        target = relative.parts[2]
        benchmark = read_json(sample_path.with_name("benchmark.json"))
        sample = read_json(sample_path)
        iterations = sample["iters"]
        times = sample["times"]
        if len(iterations) != len(times) or not iterations:
            raise ValueError(f"invalid Criterion vectors: {sample_path}")
        latency = median([float(time) / float(count) for count, time in zip(iterations, times, strict=True)])
        samples[(target, benchmark["full_id"])].append((run_id, latency, benchmark.get("throughput")))

    if not samples:
        raise ValueError(f"no Criterion samples found below {root}")

    result = []
    for (target, case_id), process_samples in sorted(samples.items()):
        run_ids = {run_id for run_id, _, _ in process_samples}
        if len(run_ids) != required_runs:
            raise ValueError(
                f"{target}/{case_id} has {len(run_ids)} process samples; expected {required_runs}"
            )
        latencies = [latency for _, latency, _ in process_samples]
        throughput = process_samples[0][2]
        throughput_kind = None
        throughput_per_second = None
        if throughput:
            throughput_kind, amount = next(iter(throughput.items()))
            throughput_per_second = float(amount) * 1_000_000_000.0 / median(latencies)
        result.append(
            {
                "target": target,
                "caseId": case_id,
                "processSamples": len(run_ids),
                "medianNs": median(latencies),
                "minProcessMedianNs": min(latencies),
                "maxProcessMedianNs": max(latencies),
                "operationsPerSecond": 1_000_000_000.0 / median(latencies),
                "throughputKind": throughput_kind,
                "throughputPerSecond": throughput_per_second,
            }
        )
    return result


def allocation_cases(root: Path, required_runs: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    samples: dict[tuple[str, str], list[dict[str, float | None]]] = defaultdict(list)
    footprints: list[dict[str, Any]] = []
    for evidence_path in root.glob("rust/sample-*/*/*.json"):
        if evidence_path.name not in {"allocations.json", "evidence.json"}:
            continue
        target = evidence_path.parent.name
        evidence = read_json(evidence_path)
        if "objectFootprint" in evidence:
            footprints.append(evidence["objectFootprint"])
        for case in evidence.get("cases", []):
            allocations = case["allocations"]
            allocated_bytes = case["allocatedBytes"]
            if isinstance(allocations, list):
                allocations = median([float(value) for value in allocations])
                allocated_bytes = median([float(value) for value in allocated_bytes])
            samples[(target, case["id"])].append(
                {
                    "allocations": float(allocations),
                    "allocatedBytes": float(allocated_bytes),
                    "outputLen": case.get("outputLen"),
                    "outputCapacity": case.get("outputCapacity"),
                }
            )

    result = []
    for (target, case_id), process_samples in sorted(samples.items()):
        if len(process_samples) != required_runs:
            raise ValueError(
                f"allocation evidence {target}/{case_id} has {len(process_samples)} process samples; "
                f"expected {required_runs}"
            )
        result.append(
            {
                "target": target,
                "caseId": case_id,
                "processSamples": len(process_samples),
                "medianAllocations": median([float(sample["allocations"]) for sample in process_samples]),
                "medianAllocatedBytes": median([float(sample["allocatedBytes"]) for sample in process_samples]),
                "outputLen": process_samples[0]["outputLen"],
                "outputCapacity": process_samples[0]["outputCapacity"],
            }
        )

    footprint = {}
    if footprints:
        if len(footprints) != required_runs:
            raise ValueError(f"object footprint has {len(footprints)} samples; expected {required_runs}")
        sizes = {int(item["sizeOfBytes"]) for item in footprints}
        counts = {int(item["objectCount"]) for item in footprints}
        if len(sizes) != 1 or len(counts) != 1:
            raise ValueError("object footprint shape changed between process samples")
        footprint = {
            "sizeOfBytes": sizes.pop(),
            "objectCount": counts.pop(),
            "medianRssDeltaBytes": median([float(item["rssDeltaBytes"]) for item in footprints]),
            "minRssDeltaBytes": min(int(item["rssDeltaBytes"]) for item in footprints),
            "maxRssDeltaBytes": max(int(item["rssDeltaBytes"]) for item in footprints),
        }
    return result, footprint


def java_cases(normalized: Path, expected_cases: int) -> list[dict[str, Any]]:
    document = read_json(normalized)
    samples = document["samples"]
    if len(samples) != expected_cases:
        raise ValueError(f"Java JMH produced {len(samples)} cases; expected {expected_cases}")
    return [
        {
            "caseId": sample["id"],
            "operation": sample["operation"],
            "serializeType": sample["serializeType"],
            "tier": sample["tier"],
            "medianNs": sample["medianLatencyNanos"],
            "allocatedBytesPerOperation": sample["allocatedBytesPerOperation"],
        }
        for sample in sorted(samples, key=lambda sample: sample["id"])
    ]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise ValueError(f"cannot write empty CSV: {path}")
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--artifact-root", type=Path, required=True)
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--java-normalized", type=Path, required=True)
    parser.add_argument("--metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    profile = read_json(args.profile)
    metadata = read_json(args.metadata)
    required_runs = int(profile["rustProcessSamples"])
    expected_cases = int(profile["caseCount"])
    rust = criterion_cases(args.artifact_root, required_runs)
    allocations, footprint = allocation_cases(args.artifact_root, required_runs)
    java = java_cases(args.java_normalized, expected_cases)

    output = args.output
    write_csv(output / "rust-summary.csv", rust)
    write_csv(output / "allocation-summary.csv", allocations)
    write_csv(output / "java-summary.csv", java)
    write_json(
        output / "baseline-summary.json",
        {
            "schemaVersion": 1,
            "baselineId": profile["baselineId"],
            "rustRevision": metadata["rustRevision"],
            "javaRevision": metadata["javaRevision"],
            "rustProcessSamples": required_runs,
            "javaCases": expected_cases,
            "rustCases": len(rust),
            "allocationCases": len(allocations),
            "objectFootprint": footprint,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
