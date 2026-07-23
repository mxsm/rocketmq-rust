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

"""Validate the R24 SLO contract and fail-closed release evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


OBJECTIVE_IDS = (
    "message_delivery_ratio",
    "send_message_p99_millis",
    "consumer_lag_messages",
    "flush_behind_bytes",
    "ha_replication_lag_bytes",
    "collector_outage_roundtrip_seconds",
    "drain_failed_prestop_events",
)
FAULT_SCENARIOS = (
    "rolling_upgrade",
    "node_eviction",
    "collector_outage",
    "disk_pressure",
    "controller_leader_failure",
    "secret_rotation",
    "acknowledged_message_recovery",
)
ROLLBACK_ASSERTIONS = (
    "baseline_images_restored",
    "baseline_chart_restored",
    "pvc_uid_set_preserved",
    "wal_preserved",
    "collector_restored",
    "controller_quorum_restored",
    "unresolved_faults_empty",
)
SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
METRIC_RE = re.compile(r"\brocketmq_[a-z0-9_]+\b")
PLACEHOLDER_HASHES = {character * 64 for character in "0123456789abcdef"}
POLICY_PATH = Path("distribution/config/architecture-production-readiness-policy.json")
REGISTRY_PATH = Path("scripts/telemetry-semantic-registry.json")
RUN_FIELDS = {
    "schema_version",
    "milestone",
    "execution_unit",
    "policy_sha256",
    "candidate_commit",
    "candidate_images",
    "run_id",
    "started_at",
    "finished_at",
    "sample_interval_seconds",
    "samples_expected",
    "samples_observed",
    "missing_sample_ratio",
    "dynamic_execution",
    "fixture",
    "fault_evidence",
    "objectives",
    "rollback_assertions",
    "unresolved_alerts",
    "unresolved_faults",
    "release_artifacts",
    "artifacts",
}


class Guard:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.errors: list[str] = []

    def require(self, condition: bool, message: str) -> None:
        if not condition:
            self.errors.append(message)

    def load_json(self, path: Path, label: str) -> dict[str, Any]:
        if not path.is_file():
            self.errors.append(f"missing required JSON: {label}")
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            self.errors.append(f"invalid JSON in {label}: {error}")
            return {}
        if not isinstance(value, dict):
            self.errors.append(f"{label} must contain a JSON object")
            return {}
        return value

    def read(self, relative: str) -> str:
        path = self.root / relative
        if not path.is_file():
            self.errors.append(f"missing required file: {relative}")
            return ""
        return path.read_text(encoding="utf-8")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def parse_utc(value: object, label: str, guard: Guard) -> datetime | None:
    if not isinstance(value, str):
        guard.errors.append(f"{label} must be an RFC3339 UTC timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        guard.errors.append(f"{label} must be an RFC3339 UTC timestamp")
        return None
    guard.require(
        parsed.tzinfo is not None
        and parsed.utcoffset() == timezone.utc.utcoffset(parsed),
        f"{label} must be UTC",
    )
    return parsed


def registry_metric_ids(registry: dict[str, Any]) -> set[str]:
    return {
        item["id"]
        for item in registry.get("signals", [])
        if isinstance(item, dict)
        and item.get("signal_type") == "metric"
        and isinstance(item.get("id"), str)
    }


def normalized_metric_id(name: str) -> str:
    for suffix in ("_bucket", "_count", "_sum", "_created"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


def validate_policy(
    guard: Guard, policy: dict[str, Any], registry: dict[str, Any]
) -> None:
    guard.require(policy.get("schema_version") == 1, "SLO policy schema must remain 1")
    guard.require(policy.get("milestone") == "M11-12", "SLO milestone must remain M11-12")
    guard.require(policy.get("execution_unit") == "R24", "SLO execution unit must remain R24")
    guard.require(
        policy.get("minimum_soak_seconds") == 21600,
        "production soak must remain at least six hours",
    )
    guard.require(
        policy.get("sample_interval_seconds") == 60,
        "SLO sampling interval must remain 60 seconds",
    )
    guard.require(
        policy.get("maximum_missing_sample_ratio") == 0.01,
        "missing-sample budget must remain one percent",
    )
    guard.require(
        policy.get("required_fault_scenarios") == list(FAULT_SCENARIOS),
        "SLO policy must bind the exact seven M11-11 fault scenarios",
    )

    objectives = policy.get("objectives")
    guard.require(isinstance(objectives, list), "SLO objectives must be a list")
    observed_ids = [
        item.get("id") for item in objectives or [] if isinstance(item, dict)
    ]
    guard.require(
        observed_ids == list(OBJECTIVE_IDS),
        "SLO policy must define the exact ordered seven objectives",
    )
    metric_ids = registry_metric_ids(registry)
    for item in objectives or []:
        if not isinstance(item, dict):
            guard.errors.append("every SLO objective must be an object")
            continue
        objective = item.get("id", "unknown")
        guard.require(
            item.get("operator") in {"gte", "lte"},
            f"{objective}: unsupported comparison operator",
        )
        guard.require(
            isinstance(item.get("target"), (int, float)),
            f"{objective}: numeric target missing",
        )
        guard.require(bool(item.get("unit")), f"{objective}: unit missing")
        guard.require(
            bool(item.get("dashboard_panel")),
            f"{objective}: dashboard panel missing",
        )
        source = item.get("source")
        guard.require(
            source == "fault_evidence" or source in metric_ids,
            f"{objective}: source is absent from the telemetry registry",
        )
        query = item.get("query")
        if source == "fault_evidence":
            guard.require(query is None, f"{objective}: fault evidence must not use a live query")
        else:
            guard.require(
                isinstance(query, str) and bool(query),
                f"{objective}: Prometheus query missing",
            )
            for metric in METRIC_RE.findall(str(query)):
                guard.require(
                    normalized_metric_id(metric) in metric_ids,
                    f"{objective}: query uses unregistered metric {metric}",
                )

    guard.require(
        policy.get("rollback_assertions") == list(ROLLBACK_ASSERTIONS),
        "rollback assertion set drifted",
    )
    evidence = policy.get("evidence", {})
    guard.require(evidence.get("schema_version") == 1, "evidence schema must remain 1")
    guard.require(
        evidence.get("production_requires_dynamic_execution") is True,
        "production evidence must require dynamic execution",
    )
    guard.require(
        evidence.get("production_requires_current_commit") is True,
        "production evidence must bind the current commit",
    )
    guard.require(
        evidence.get("fixture_requires_explicit_opt_in") is True,
        "fixture evidence must require explicit opt-in",
    )


def validate_release_assets(
    guard: Guard, policy: dict[str, Any], registry: dict[str, Any]
) -> None:
    artifacts = policy.get("release_artifacts", {})
    required = {"dashboard", "alerts", "runbook", "fault_policy", "telemetry_registry"}
    guard.require(
        isinstance(artifacts, dict) and set(artifacts) == required,
        "release artifact map drifted",
    )
    if not isinstance(artifacts, dict):
        return
    for relative in artifacts.values():
        guard.require(
            isinstance(relative, str) and (guard.root / relative).is_file(),
            f"release artifact is missing: {relative}",
        )

    dashboard_text = guard.read(str(artifacts.get("dashboard", "")))
    alerts_text = guard.read(str(artifacts.get("alerts", "")))
    runbook = guard.read(str(artifacts.get("runbook", "")))
    try:
        dashboard = json.loads(dashboard_text)
    except json.JSONDecodeError as error:
        guard.errors.append(f"invalid Grafana dashboard JSON: {error}")
        dashboard = {}
    panel_titles = {
        panel.get("title")
        for panel in dashboard.get("panels", [])
        if isinstance(panel, dict)
    }
    for objective in policy.get("objectives", []):
        if not isinstance(objective, dict):
            continue
        guard.require(
            objective.get("dashboard_panel") in panel_titles,
            f"dashboard panel missing for {objective.get('id')}",
        )
        alert = objective.get("alert")
        if alert is not None:
            guard.require(
                isinstance(alert, str) and f"alert: {alert}" in alerts_text,
                f"Prometheus alert missing for {objective.get('id')}",
            )
            guard.require(
                alert in runbook,
                f"runbook does not route alert {alert}",
            )

    registered = registry_metric_ids(registry)
    for metric in sorted(set(METRIC_RE.findall(dashboard_text + "\n" + alerts_text))):
        normalized = normalized_metric_id(metric)
        guard.require(
            normalized in registered,
            f"dashboard/alert uses unregistered metric: {metric}",
        )
    for marker in (
        "dynamic_execution=true",
        "six hours",
        "CommitLog remains the authoritative WAL",
        "Do not delete CommitLog",
        "FailedPreStopHook",
    ):
        guard.require(marker in runbook, f"runbook marker missing: {marker}")

    runner = guard.read("scripts/run-architecture-slo-evidence.ps1")
    workflow = guard.read(".github/workflows/architecture-slo-evidence.yml")
    tests = guard.read("scripts/tests/test_architecture_slo_guard.py")
    for marker in (
        '[ValidateSet("Validate", "Run")]',
        "minimum_soak_seconds",
        "fault_matrix_guard.py",
        "architecture_slo_guard.py",
        "dynamic_execution",
        "candidate_commit",
    ):
        guard.require(marker in runner, f"SLO runner contract marker missing: {marker}")
    guard.require(
        "workflow_dispatch:" in workflow,
        "SLO workflow must require explicit dispatch",
    )
    guard.require(
        "run-architecture-slo-evidence.ps1" in workflow,
        "SLO workflow must execute the real runner",
    )
    guard.require(
        "permissions:\n  contents: read" in workflow,
        "SLO workflow must retain contents:read only",
    )
    guard.require(
        "test_architecture_slo_guard" in workflow and "deliberate" in tests,
        "SLO deliberate-violation tests are not wired",
    )


def validate_image_map(
    guard: Guard, value: object, label: str
) -> dict[str, str]:
    if not isinstance(value, dict):
        guard.errors.append(f"{label} must be an object")
        return {}
    guard.require(set(value) == set(SERVICES), f"{label} must contain five services")
    result: dict[str, str] = {}
    for service in SERVICES:
        reference = value.get(service)
        match = re.fullmatch(r"[^@\s]+@sha256:([0-9a-f]{64})", reference or "")
        guard.require(match is not None, f"{label}.{service} must be image@sha256")
        if match:
            guard.require(
                match.group(1) not in PLACEHOLDER_HASHES,
                f"{label}.{service} uses a placeholder digest",
            )
            result[service] = str(reference)
    return result


def objective_passes(operator: str, observed: float, target: float) -> bool:
    if operator == "gte":
        return observed >= target
    if operator == "lte":
        return observed <= target
    return False


def current_commit(root: Path, guard: Guard) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        guard.errors.append("unable to resolve current Git commit")
        return ""
    return result.stdout.strip()


def validate_fault_snapshot(
    guard: Guard,
    snapshot: dict[str, Any],
    candidate_images: dict[str, str],
    production: bool,
) -> None:
    guard.require(
        snapshot.get("milestone") == "M11-11",
        "fault evidence must come from M11-11",
    )
    guard.require(
        snapshot.get("candidate_images") == candidate_images,
        "fault and SLO evidence candidate image maps differ",
    )
    scenarios = snapshot.get("scenarios")
    scenario_ids = [
        item.get("id") for item in scenarios or [] if isinstance(item, dict)
    ]
    guard.require(
        scenario_ids == list(FAULT_SCENARIOS),
        "fault evidence does not contain the exact seven scenarios",
    )
    assertions = snapshot.get("global_assertions")
    guard.require(
        isinstance(assertions, dict)
        and bool(assertions)
        and all(value is True for value in assertions.values()),
        "fault evidence contains a failed global assertion",
    )
    guard.require(
        snapshot.get("unresolved_faults") == [],
        "fault evidence contains unresolved faults",
    )
    if production:
        guard.require(
            snapshot.get("dynamic_execution") is True
            and snapshot.get("fixture") is False,
            "production SLO evidence requires dynamic non-fixture fault evidence",
        )


def validate_evidence(
    guard: Guard,
    policy: dict[str, Any],
    evidence_dir: Path,
    allow_fixture: bool,
) -> None:
    run = guard.load_json(evidence_dir / "run.json", "run.json")
    if not run:
        return
    guard.require(set(run) == RUN_FIELDS, "run.json fields must match the exact schema")
    guard.require(run.get("schema_version") == 1, "run schema must remain 1")
    guard.require(run.get("milestone") == "M11-12", "run milestone must remain M11-12")
    guard.require(run.get("execution_unit") == "R24", "run execution unit must remain R24")
    guard.require(
        run.get("policy_sha256") == canonical_json_sha256(policy),
        "run policy_sha256 does not match the committed policy",
    )

    fixture = run.get("fixture") is True
    dynamic = run.get("dynamic_execution") is True
    if fixture:
        guard.require(allow_fixture, "fixture evidence requires --allow-fixture")
        guard.require(not dynamic, "fixture evidence must not claim dynamic execution")
    else:
        guard.require(dynamic, "production evidence requires dynamic execution")
    production = not fixture

    commit = run.get("candidate_commit")
    guard.require(
        isinstance(commit, str) and COMMIT_RE.fullmatch(commit) is not None,
        "candidate_commit must be a full Git SHA",
    )
    if production:
        guard.require(
            commit == current_commit(guard.root, guard),
            "production evidence candidate_commit differs from the checked-out commit",
        )

    candidate_images = validate_image_map(
        guard, run.get("candidate_images"), "candidate_images"
    )
    started = parse_utc(run.get("started_at"), "started_at", guard)
    finished = parse_utc(run.get("finished_at"), "finished_at", guard)
    if started and finished:
        guard.require(finished >= started, "finished_at precedes started_at")
        guard.require(
            (finished - started).total_seconds() >= policy["minimum_soak_seconds"],
            "soak duration is shorter than the committed minimum",
        )
    guard.require(
        run.get("sample_interval_seconds") == policy["sample_interval_seconds"],
        "sample interval differs from policy",
    )
    expected = run.get("samples_expected")
    observed = run.get("samples_observed")
    guard.require(
        isinstance(expected, int) and expected >= 360,
        "samples_expected is below the six-hour minimum",
    )
    guard.require(
        isinstance(observed, int) and isinstance(expected, int) and 0 <= observed <= expected,
        "samples_observed is invalid",
    )
    missing = run.get("missing_sample_ratio")
    guard.require(
        isinstance(missing, (int, float))
        and 0 <= missing <= policy["maximum_missing_sample_ratio"],
        "missing-sample ratio exceeds policy",
    )
    if isinstance(expected, int) and expected > 0 and isinstance(observed, int):
        guard.require(
            abs((expected - observed) / expected - float(missing or 0)) < 1e-9,
            "missing-sample ratio does not match sample counts",
        )

    artifact_items = run.get("artifacts")
    guard.require(
        isinstance(artifact_items, list) and bool(artifact_items),
        "artifact index must be non-empty",
    )
    indexed: dict[str, str] = {}
    for item in artifact_items or []:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            guard.errors.append("artifact entries must contain only path and sha256")
            continue
        relative = item.get("path")
        expected_hash = item.get("sha256")
        if not isinstance(relative, str) or not relative:
            guard.errors.append("artifact path must be non-empty")
            continue
        relative_path = Path(relative)
        guard.require(
            not relative_path.is_absolute() and ".." not in relative_path.parts,
            f"unsafe artifact path: {relative}",
        )
        guard.require(relative not in indexed, f"duplicate artifact path: {relative}")
        path = evidence_dir / relative_path
        guard.require(path.is_file(), f"missing artifact: {relative}")
        guard.require(
            isinstance(expected_hash, str)
            and SHA_RE.fullmatch(expected_hash) is not None,
            f"invalid artifact hash: {relative}",
        )
        if path.is_file() and isinstance(expected_hash, str):
            guard.require(
                sha256_file(path) == expected_hash,
                f"artifact hash mismatch: {relative}",
            )
        indexed[relative] = str(expected_hash)

    fault = run.get("fault_evidence")
    guard.require(
        isinstance(fault, dict) and set(fault) == {"path", "sha256"},
        "fault_evidence reference is invalid",
    )
    if isinstance(fault, dict):
        path = fault.get("path")
        guard.require(
            isinstance(path, str) and path in indexed,
            "fault evidence is absent from the artifact index",
        )
        guard.require(
            fault.get("sha256") == indexed.get(str(path)),
            "fault evidence hash differs from the artifact index",
        )
        fault_path = evidence_dir / str(path)
        snapshot = guard.load_json(fault_path, "fault evidence snapshot")
        if snapshot:
            validate_fault_snapshot(guard, snapshot, candidate_images, production)

    policy_objectives = {
        item["id"]: item
        for item in policy.get("objectives", [])
        if isinstance(item, dict) and "id" in item
    }
    objectives = run.get("objectives")
    guard.require(isinstance(objectives, list), "objective evidence must be a list")
    objective_ids = [
        item.get("id") for item in objectives or [] if isinstance(item, dict)
    ]
    guard.require(
        objective_ids == list(OBJECTIVE_IDS),
        "evidence must contain the exact ordered seven objectives",
    )
    for item in objectives or []:
        if not isinstance(item, dict):
            guard.errors.append("objective evidence must be an object")
            continue
        objective = str(item.get("id"))
        expected_objective = policy_objectives.get(objective, {})
        guard.require(
            set(item) == {"id", "status", "observed", "target", "unit", "evidence"},
            f"{objective}: evidence fields drifted",
        )
        guard.require(item.get("status") == "passed", f"{objective}: status must pass")
        guard.require(
            item.get("target") == expected_objective.get("target")
            and item.get("unit") == expected_objective.get("unit"),
            f"{objective}: target or unit differs from policy",
        )
        observed_value = item.get("observed")
        target = expected_objective.get("target")
        guard.require(
            isinstance(observed_value, (int, float))
            and isinstance(target, (int, float))
            and objective_passes(
                str(expected_objective.get("operator")),
                float(observed_value),
                float(target),
            ),
            f"{objective}: observed value violates the objective",
        )
        guard.require(
            isinstance(item.get("evidence"), str)
            and item["evidence"] in indexed,
            f"{objective}: evidence artifact is not indexed",
        )

    rollback = run.get("rollback_assertions")
    guard.require(
        isinstance(rollback, dict) and set(rollback) == set(ROLLBACK_ASSERTIONS),
        "rollback assertion keys drifted",
    )
    if isinstance(rollback, dict):
        for assertion in ROLLBACK_ASSERTIONS:
            guard.require(
                rollback.get(assertion) is True,
                f"rollback assertion failed: {assertion}",
            )
    guard.require(run.get("unresolved_alerts") == [], "unresolved alerts must be empty")
    guard.require(run.get("unresolved_faults") == [], "unresolved faults must be empty")

    release_artifacts = run.get("release_artifacts")
    expected_paths = set(policy.get("release_artifacts", {}).values())
    guard.require(
        isinstance(release_artifacts, dict)
        and set(release_artifacts) == expected_paths,
        "release artifact hash map drifted",
    )
    if isinstance(release_artifacts, dict):
        for relative, expected_hash in release_artifacts.items():
            path = guard.root / relative
            guard.require(
                isinstance(expected_hash, str)
                and path.is_file()
                and sha256_file(path) == expected_hash,
                f"release artifact hash mismatch: {relative}",
            )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--policy-only", action="store_true")
    parser.add_argument("--print-policy-sha256", action="store_true")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--allow-fixture", action="store_true")
    args = parser.parse_args()
    if args.print_policy_sha256 and (args.evidence or args.allow_fixture):
        parser.error("--print-policy-sha256 cannot be combined with evidence options")
    root = args.root.resolve()
    guard = Guard(root)
    policy = guard.load_json(root / POLICY_PATH, str(POLICY_PATH))
    registry = guard.load_json(root / REGISTRY_PATH, str(REGISTRY_PATH))
    validate_policy(guard, policy, registry)
    if not args.print_policy_sha256:
        validate_release_assets(guard, policy, registry)
    if args.evidence:
        evidence_dir = args.evidence if args.evidence.is_absolute() else root / args.evidence
        validate_evidence(
            guard, policy, evidence_dir.resolve(), args.allow_fixture
        )
    elif args.allow_fixture:
        guard.errors.append("--allow-fixture requires --evidence")
    if guard.errors:
        for error in guard.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    if args.print_policy_sha256:
        print(canonical_json_sha256(policy))
        return 0
    mode = "policy" if not args.evidence else "policy and evidence"
    print(f"M11-12 R24 architecture SLO {mode} guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
