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

"""Normalize Criterion samples into the cross-runtime performance schema."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
from pathlib import Path
from typing import Any


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def case_id_for(sample: Path, known_ids: set[str]) -> str | None:
    for parent in sample.parents:
        if parent.name in known_ids:
            return parent.name
    normalized = str(sample).replace("\\", "/")
    matches = [case_id for case_id in known_ids if f"/{case_id}/" in normalized]
    if len(matches) == 1:
        return matches[0]
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--fixture-manifest", type=Path, required=True)
    parser.add_argument("--runner", type=Path, required=True)
    parser.add_argument("--benchmark-harness", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--role", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--profile", choices=("gate", "diagnostic"), required=True)
    parser.add_argument("--allocations", type=Path)
    parser.add_argument("--baseline-manifest", type=Path)
    parser.add_argument("--allow-partial", action="store_true")
    args = parser.parse_args()

    corpus = read_json(args.corpus)
    cases = {case["id"]: case for case in corpus["cases"] if case["gateWeight"] > 0}
    normalized: dict[str, dict[str, Any]] = {}
    for sample_path in args.raw_dir.rglob("sample.json"):
        if sample_path.parent.name != "new":
            continue
        case_id = case_id_for(sample_path, set(cases))
        if case_id is None:
            continue
        if case_id in normalized:
            raise ValueError(f"duplicate Criterion sample for {case_id}")
        sample = read_json(sample_path)
        iterations = sample.get("iters")
        times = sample.get("times")
        if not isinstance(iterations, list) or not isinstance(times, list) or len(iterations) != len(times):
            raise ValueError(f"invalid Criterion sample vectors: {sample_path}")
        raw = [float(time) / float(count) for count, time in zip(iterations, times) if float(count) > 0]
        if not raw:
            raise ValueError(f"Criterion sample is empty: {sample_path}")
        normalized[case_id] = {
            "id": case_id,
            "operation": cases[case_id]["operation"],
            "serializeType": cases[case_id]["serializeType"],
            "tier": cases[case_id]["tier"],
            "gateWeight": cases[case_id]["gateWeight"],
            "fastGateWeight": cases[case_id].get("fastGateWeight", 0.0),
            "medianLatencyNanos": statistics.median(raw),
            "rawLatencyNanos": raw,
            "rawFile": sample_path.resolve().relative_to(args.raw_dir.resolve()).as_posix(),
        }

    missing = sorted(set(cases) - set(normalized))
    extra = sorted(set(normalized) - set(cases))
    if extra or (missing and not args.allow_partial):
        raise ValueError(f"Criterion corpus mismatch; missing={missing}, extra={extra}")

    allocation_document = None
    if args.allocations:
        allocation_document = read_json(args.allocations)

    output = {
        "schemaVersion": 1,
        "runtime": "rust",
        "codec": "RequestHeaderCodecV2" if args.role in ("historical", "post-p0", "phase1-hardened", "v2-replay") else "RequestHeaderCodecV3",
        "role": args.role,
        "commit": args.commit,
        "profile": args.profile,
        "releasable": args.profile == "gate" and not args.allow_partial,
        "corpusVersion": corpus["corpusVersion"],
        "corpusSha256": digest(args.corpus),
        "fixtureManifestSha256": digest(args.fixture_manifest),
        "runnerFingerprintSha256": digest(args.runner),
        "benchmarkHarnessSha256": digest(args.benchmark_harness),
        "runner": read_json(args.runner),
        "sampleCount": len(normalized),
        "missingCases": missing,
        "samples": [normalized[key] for key in sorted(normalized)],
        "allocationEvidence": allocation_document,
    }
    if args.baseline_manifest:
        output["baselineManifestSha256"] = digest(args.baseline_manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")


if __name__ == "__main__":
    main()
