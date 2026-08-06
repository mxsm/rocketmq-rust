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

"""Evaluate the fail-closed RequestHeaderCodecV3 performance contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import statistics
import sys
from pathlib import Path
from typing import Any


REQUIRED_GATES = {f"PERF-{index:02d}" for index in range(1, 10)}
BOOTSTRAP_SEED = 0x524D_5156_3301
BOOTSTRAP_ROUNDS = 10_000


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_text_digest(path: Path) -> str:
    text = path.read_text(encoding="utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def percentile(values: list[float], probability: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    fraction = position - lower
    return ordered[lower] * (1.0 - fraction) + ordered[upper] * fraction


def weighted_geomean(ratios: dict[str, float], weights: dict[str, float]) -> float:
    selected = {case_id: weight for case_id, weight in weights.items() if weight > 0}
    weight_sum = sum(selected.values())
    if weight_sum <= 0 or set(selected) - set(ratios):
        raise ValueError("weighted geometric mean has missing cases or zero total weight")
    return math.exp(sum((weight / weight_sum) * math.log(ratios[case_id]) for case_id, weight in selected.items()))


def sample_index(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result = {sample["id"]: sample for sample in document.get("samples", [])}
    if len(result) != len(document.get("samples", [])):
        raise ValueError(f"{document.get('role')} contains duplicate sample IDs")
    return result


def bootstrap_lower_bound(
    numerator: dict[str, dict[str, Any]],
    denominator: dict[str, dict[str, Any]],
    weights: dict[str, float],
    seed_offset: int,
) -> float:
    selected = [case_id for case_id, weight in weights.items() if weight > 0]
    randomizer = random.Random(BOOTSTRAP_SEED + seed_offset)
    estimates: list[float] = []
    for _ in range(BOOTSTRAP_ROUNDS):
        ratios: dict[str, float] = {}
        for case_id in selected:
            left = numerator[case_id].get("rawLatencyNanos")
            right = denominator[case_id].get("rawLatencyNanos")
            if not left or not right:
                raise ValueError(f"raw samples required for bootstrap: {case_id}")
            ratios[case_id] = float(randomizer.choice(left)) / float(randomizer.choice(right))
        estimates.append(weighted_geomean(ratios, weights))
    return percentile(estimates, 0.025)


def gate_result(actual: float, operator: str, threshold: float) -> dict[str, Any]:
    if operator == "gte":
        passed = actual >= threshold
    elif operator == "gt":
        passed = actual > threshold
    elif operator == "lte":
        passed = actual <= threshold
    else:
        raise ValueError(f"unsupported gate operator: {operator}")
    return {"pass": passed, "actual": actual, "operator": operator, "threshold": threshold}


def checked_path(root: Path, manifest: dict[str, Any], name: str) -> Path:
    entry = manifest.get("files", {}).get(name)
    if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
        raise ValueError(f"evidence manifest is missing the exact {name} file identity")
    path = (root / entry["path"]).resolve()
    if not path.is_file() or digest(path) != entry["sha256"]:
        raise ValueError(f"evidence file digest mismatch: {name}")
    return path


def checked_file(root: Path, manifest: dict[str, Any], name: str) -> tuple[Path, dict[str, Any]]:
    path = checked_path(root, manifest, name)
    return path, read_json(path)


def validate_gates(gates_path: Path, gates: dict[str, Any]) -> None:
    if gates.get("schemaVersion") != 1 or set(gates.get("gates", {})) != REQUIRED_GATES:
        raise ValueError("perf-gates.json must define exactly PERF-01 through PERF-09")
    expected = {
        "PERF-01": ("gte", "threshold", 1.15),
        "PERF-02": ("gte", "threshold", 1.10),
        "PERF-03": ("gte", "threshold", 1.05),
        "PERF-04": ("gt", "threshold", 1.00),
        "PERF-05": ("gte", "threshold", 0.97),
        "PERF-06": ("lte", "allocationRatio", 0.70),
    }
    for gate_id, (operator, field, value) in expected.items():
        gate = gates["gates"][gate_id]
        if gate.get("operator") != operator or float(gate.get(field, -1)) != value:
            raise ValueError(f"{gate_id} has drifted from the frozen contract")
    if gates["gates"]["PERF-07"] != {
        "operator": "lte",
        "eachMetric": ["textBytesGrowth", "artifactBytesGrowth"],
        "growth": 0.03,
    }:
        raise ValueError("PERF-07 has drifted from the frozen contract")
    if gates["gates"]["PERF-08"] != {
        "operator": "lte",
        "eachMetric": ["cleanBuildGrowth", "processTreePeakWorkingSetGrowth"],
        "cleanBuildGrowth": 0.10,
        "processTreePeakWorkingSetGrowth": 0.10,
    }:
        raise ValueError("PERF-08 has drifted from the frozen contract")
    if gates["gates"]["PERF-09"] != {
        "operator": "lte",
        "incrementalBuildGrowth": 0.05,
    }:
        raise ValueError("PERF-09 has drifted from the frozen contract")
    if gates["gates"]["PERF-04"].get("appliesTo") != ["PERF-01", "PERF-02", "PERF-03"]:
        raise ValueError("PERF-04 must apply independently to PERF-01 through PERF-03")
    recipe = (gates_path.parent.parent.parent / gates["buildRecipe"]["path"]).resolve()
    if not recipe.is_file() or canonical_text_digest(recipe) != gates["buildRecipe"]["sha256"]:
        raise ValueError("perf-gates.json build recipe digest is stale")


def validate_runtime_identity(
    documents: list[dict[str, Any]], corpus: dict[str, Any], runner_sha: str
) -> None:
    expected_cases = {case["id"] for case in corpus["cases"] if case["gateWeight"] > 0}
    identities = {
        (document.get("corpusSha256"), document.get("fixtureManifestSha256"), document.get("runnerFingerprintSha256"))
        for document in documents
    }
    if len(identities) != 1 or next(iter(identities))[2] != runner_sha:
        raise ValueError("runtime evidence was not collected against one corpus, fixture set, and runner")
    for document in documents:
        if document.get("profile") != "gate" or document.get("releasable") is not True:
            raise ValueError(f"{document.get('role')} is diagnostic-only")
        if set(sample_index(document)) != expected_cases:
            raise ValueError(f"{document.get('role')} does not contain the complete production corpus")


def allocation_ratio(v3: dict[str, Any], v2: dict[str, Any], corpus: dict[str, Any]) -> float:
    def allocations(document: dict[str, Any]) -> dict[str, float]:
        evidence = document.get("allocationEvidence")
        if not evidence or int(evidence.get("samplesPerCase", 0)) < 32:
            raise ValueError(f"{document.get('role')} lacks 32 allocation samples per encode case")
        return {
            case["id"]: statistics.median(float(value) for value in case["allocations"])
            for case in evidence["cases"]
        }

    v3_values = allocations(v3)
    v2_values = allocations(v2)
    selected = [
        case
        for case in corpus["cases"]
        if case["operation"] == "encode" and case["serializeType"] == "ROCKETMQ" and case["gateWeight"] > 0
    ]
    total = sum(float(case["gateWeight"]) for case in selected)
    v3_average = sum(float(case["gateWeight"]) * v3_values[case["id"]] for case in selected) / total
    v2_average = sum(float(case["gateWeight"]) * v2_values[case["id"]] for case in selected) / total
    return v3_average / v2_average


def paired_growth(v3: dict[str, Any], v2: dict[str, Any], field: str) -> float:
    left = [float(sample[field]) for sample in v3["samples"]]
    right = [float(sample[field]) for sample in v2["samples"]]
    if len(left) != 8 or len(right) != 8:
        raise ValueError(f"{field} requires exactly eight paired samples")
    return statistics.median(current / baseline - 1.0 for current, baseline in zip(left, right))


def validate_build_pair(v3: dict[str, Any], v2: dict[str, Any], recipe: dict[str, Any]) -> None:
    for document in (v3, v2):
        if document.get("releasable") is not True or document.get("repetitions") != 8:
            raise ValueError("build evidence must contain eight releasable samples")
        if document.get("buildRecipe") != recipe:
            raise ValueError("build evidence recipe identity mismatch")
    identity_fields = (
        "mode",
        "resolvedCommand",
        "normalizedEnvironmentTemplateSha256",
        "toolIdentitySha256",
        "measurementHelperSha256",
    )
    for field in identity_fields:
        if v3.get(field) != v2.get(field):
            raise ValueError(f"V2/V3 build evidence differs in {field}")
    for document in (v3, v2):
        targets = [sample["targetDirectory"] for sample in document["samples"]]
        if len(targets) != len(set(targets)):
            raise ValueError("build evidence reused a target directory")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--gates", type=Path, required=True)
    args = parser.parse_args()

    root = args.evidence.resolve()
    manifest_path = root / "manifest.json"
    manifest = read_json(manifest_path)
    gates = read_json(args.gates)
    validate_gates(args.gates.resolve(), gates)
    if manifest.get("schemaVersion") != 1 or manifest.get("mode") != "release":
        raise ValueError("only a release evidence bundle can produce a performance attestation")
    if manifest.get("gatesSha256") != canonical_text_digest(args.gates):
        raise ValueError("evidence was collected with different performance gates")

    corpus_path, corpus = checked_file(root, manifest, "corpus")
    runner_path, _ = checked_file(root, manifest, "runner")
    rust_benchmark_harness_path = checked_path(root, manifest, "rustBenchmarkHarness")
    _, v3 = checked_file(root, manifest, "v3")
    v2_replay_path, v2 = checked_file(root, manifest, "v2Replay")
    v2_manifest_path, v2_manifest = checked_file(root, manifest, "v2Manifest")
    _, java = checked_file(root, manifest, "java")
    _, v3_clean = checked_file(root, manifest, "v3CleanBuild")
    _, v2_clean = checked_file(root, manifest, "v2CleanBuild")
    _, v3_incremental = checked_file(root, manifest, "v3IncrementalBuild")
    _, v2_incremental = checked_file(root, manifest, "v2IncrementalBuild")

    if gates["baselinePolicy"] != {
        "v2Role": "phase1-hardened",
        "v2ManifestFileName": "v2-phase1.json",
        "requireSameRunReplay": True,
        "rejectV2Roles": ["historical", "post-p0"],
    }:
        raise ValueError("V2 baseline policy drifted")
    if v3.get("role") != "release-candidate" or v2.get("role") != "v2-replay" or java.get("role") != "java-pinned":
        raise ValueError("release bundle contains an invalid runtime role")
    if v2_manifest.get("role") != "phase1-hardened" or v2_manifest.get("commit") != v2.get("commit"):
        raise ValueError("V2 replay does not match the Phase 1 hardened identity")
    if v2.get("baselineManifestSha256") != digest(v2_manifest_path):
        raise ValueError("V2 replay does not reference the frozen baseline manifest")
    if v2_replay_path == v2_manifest_path:
        raise ValueError("same-run V2 replay cannot reuse the historical manifest file")
    rust_benchmark_harness_sha = digest(rust_benchmark_harness_path)
    if {
        v3.get("benchmarkHarnessSha256"),
        v2.get("benchmarkHarnessSha256"),
    } != {rust_benchmark_harness_sha}:
        raise ValueError("V3 and V2 replay did not use the bundled Rust benchmark harness")

    validate_runtime_identity([v3, v2, java], corpus, digest(runner_path))
    v3_samples = sample_index(v3)
    v2_samples = sample_index(v2)
    java_samples = sample_index(java)
    weights = {case["id"]: float(case["gateWeight"]) for case in corpus["cases"]}
    fast_weights = {case["id"]: float(case.get("fastGateWeight", 0.0)) for case in corpus["cases"]}

    v3_over_v2 = {case_id: v2_samples[case_id]["medianLatencyNanos"] / v3_samples[case_id]["medianLatencyNanos"] for case_id in weights}
    v3_over_java = {case_id: java_samples[case_id]["medianLatencyNanos"] / v3_samples[case_id]["medianLatencyNanos"] for case_id in weights}
    perf01 = weighted_geomean(v3_over_v2, weights)
    perf02 = weighted_geomean(v3_over_java, weights)
    perf03 = weighted_geomean(v3_over_java, fast_weights)
    ci01 = bootstrap_lower_bound(v2_samples, v3_samples, weights, 1)
    ci02 = bootstrap_lower_bound(java_samples, v3_samples, weights, 2)
    ci03 = bootstrap_lower_bound(java_samples, v3_samples, fast_weights, 3)
    tier1 = [case["id"] for case in corpus["cases"] if case["tier"] == 1 and case["gateWeight"] > 0]
    perf05 = min(min(v3_over_v2[case_id], v3_over_java[case_id]) for case_id in tier1)
    perf06 = allocation_ratio(v3, v2, corpus)

    recipe_identity = gates["buildRecipe"]
    recipe = {"id": recipe_identity["id"], "sha256": recipe_identity["sha256"]}
    validate_build_pair(v3_clean, v2_clean, recipe)
    validate_build_pair(v3_incremental, v2_incremental, recipe)
    text_growth = paired_growth_nested(v3_clean, v2_clean, "textBytes")
    artifact_growth = paired_growth_nested(v3_clean, v2_clean, "artifactBytes")
    clean_growth = paired_growth(v3_clean, v2_clean, "wallTimeNanos")
    memory_growth = paired_growth(v3_clean, v2_clean, "processTreePeakWorkingSetBytes")
    incremental_growth = paired_growth(v3_incremental, v2_incremental, "wallTimeNanos")

    configured = gates["gates"]
    results = {
        "PERF-01": gate_result(perf01, configured["PERF-01"]["operator"], configured["PERF-01"]["threshold"]),
        "PERF-02": gate_result(perf02, configured["PERF-02"]["operator"], configured["PERF-02"]["threshold"]),
        "PERF-03": gate_result(perf03, configured["PERF-03"]["operator"], configured["PERF-03"]["threshold"]),
        "PERF-04": {
            "pass": all(value > configured["PERF-04"]["threshold"] for value in (ci01, ci02, ci03)),
            "operator": "gt",
            "threshold": configured["PERF-04"]["threshold"],
            "lowerBounds": {"PERF-01": ci01, "PERF-02": ci02, "PERF-03": ci03},
        },
        "PERF-05": gate_result(perf05, configured["PERF-05"]["operator"], configured["PERF-05"]["threshold"]),
        "PERF-06": gate_result(perf06, configured["PERF-06"]["operator"], configured["PERF-06"]["allocationRatio"]),
        "PERF-07": {
            "pass": text_growth <= configured["PERF-07"]["growth"] and artifact_growth <= configured["PERF-07"]["growth"],
            "operator": "lte",
            "threshold": configured["PERF-07"]["growth"],
            "actual": {"textBytesGrowth": text_growth, "artifactBytesGrowth": artifact_growth},
        },
        "PERF-08": {
            "pass": clean_growth <= configured["PERF-08"]["cleanBuildGrowth"] and memory_growth <= configured["PERF-08"]["processTreePeakWorkingSetGrowth"],
            "operator": "lte",
            "actual": {"cleanBuildGrowth": clean_growth, "processTreePeakWorkingSetGrowth": memory_growth},
        },
        "PERF-09": gate_result(incremental_growth, configured["PERF-09"]["operator"], configured["PERF-09"]["incrementalBuildGrowth"]),
    }
    release_pass = all(result["pass"] for result in results.values())
    report = {
        "schemaVersion": 1,
        "releasePass": release_pass,
        "evidenceManifestSha256": digest(manifest_path),
        "gatesSha256": digest(args.gates),
        "corpusSha256": digest(corpus_path),
        "bootstrap": {"seed": BOOTSTRAP_SEED, "rounds": BOOTSTRAP_ROUNDS},
        "gates": results,
    }
    output = root / "performance-attestation.json"
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not release_pass:
        sys.exit(1)


def paired_growth_nested(v3: dict[str, Any], v2: dict[str, Any], field: str) -> float:
    left = [float(sample["artifact"][field]) for sample in v3["samples"]]
    right = [float(sample["artifact"][field]) for sample in v2["samples"]]
    if len(left) != 8 or len(right) != 8:
        raise ValueError(f"artifact {field} requires exactly eight paired samples")
    return statistics.median(current / baseline - 1.0 for current, baseline in zip(left, right))


if __name__ == "__main__":
    main()
