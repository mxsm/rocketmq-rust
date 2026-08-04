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

"""Validate the M11-11 fault policy and fail-closed dynamic evidence."""

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


SERVICES = ("broker", "namesrv", "controller", "proxy", "mcp")
SCENARIOS = (
    "rolling_upgrade",
    "node_eviction",
    "nameserver_minority_partition",
    "nameserver_majority_unavailable",
    "collector_outage",
    "disk_pressure",
    "disk_full",
    "slow_disk_fsync_jitter",
    "controller_leader_failure",
    "controller_quorum_loss",
    "network_impairment",
    "ha_replication_lag",
    "snapshot_install_interruption",
    "proxy_slow_broker_overload",
    "secret_rotation",
    "acknowledged_message_recovery",
)
GLOBAL_ASSERTIONS = (
    "all_workloads_ready",
    "all_faults_reverted",
    "controller_quorum_restored",
    "pvc_uid_set_preserved",
    "acknowledged_message_recovered",
    "queue_offset_preserved",
    "commitlog_offset_preserved",
    "drain_completed_within_deadline",
    "slo_budget_satisfied",
    "rollback_verified",
    "unresolved_faults_empty",
)
DIGEST_RE = re.compile(r"^[^@\s]+@sha256:([0-9a-f]{64})$")
SHA_RE = re.compile(r"^[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PLACEHOLDER_HASHES = {character * 64 for character in "0123456789abcdef"}
REQUIRED_RUN_FIELDS = {
    "schema_version",
    "milestone",
    "policy_sha256",
    "run_id",
    "candidate_commit",
    "started_at",
    "finished_at",
    "backend",
    "dynamic_execution",
    "fixture",
    "cluster_profile",
    "tool_versions",
    "chart_sha256",
    "overlay_sha256",
    "baseline_images",
    "candidate_images",
    "global_assertions",
    "unresolved_faults",
    "scenarios",
    "artifacts",
}


class Guard:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.errors: list[str] = []

    def require(self, condition: bool, message: str) -> None:
        if not condition:
            self.errors.append(message)

    def read(self, relative: str) -> str:
        path = self.root / relative
        if not path.is_file():
            self.errors.append(f"missing required file: {relative}")
            return ""
        return path.read_text(encoding="utf-8")

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


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json_sha256(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


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


def parse_utc(value: object, label: str, guard: Guard) -> datetime | None:
    if not isinstance(value, str):
        guard.errors.append(f"{label} must be an RFC3339 UTC timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        guard.errors.append(f"{label} must be an RFC3339 UTC timestamp")
        return None
    guard.require(parsed.tzinfo is not None and parsed.utcoffset() == timezone.utc.utcoffset(parsed), f"{label} must be UTC")
    return parsed


def validate_policy(guard: Guard, policy: dict[str, Any]) -> None:
    guard.require(policy.get("schema_version") == 1, "fault policy schema_version must remain 1")
    guard.require(policy.get("milestone") == "M11-11", "fault policy milestone must remain M11-11")
    guard.require(policy.get("evidence_schema_version") == 1, "evidence schema version must remain 1")
    guard.require(policy.get("backends") == ["kind", "k3d"], "fault policy must define exact Kind/K3d backends")
    guard.require(policy.get("kubernetes_version") == "1.32.2", "fault Kubernetes version must remain 1.32.2")

    cluster = policy.get("cluster", {})
    guard.require(cluster.get("control_plane_nodes") == 1, "fault cluster must have one control-plane node")
    guard.require(cluster.get("worker_nodes") == 3, "fault cluster must have three worker nodes")
    guard.require(cluster.get("broker_replicas") == 3, "fault cluster must have three Broker replicas")
    guard.require(cluster.get("controller_replicas") == 3, "fault cluster must have three Controller replicas")
    guard.require(cluster.get("controller_quorum") == 2, "Controller quorum must remain two")
    guard.require(
        cluster.get("kind_node_image")
        == "kindest/node:v1.32.2@sha256:f226345927d7e348497136874b6d207e0b32cc52154ad8323129352923a3142f",
        "Kind node image/version digest drifted",
    )
    guard.require(cluster.get("storage_classes") == {"kind": "standard", "k3d": "local-path"}, "storage class matrix drifted")

    guard.require(policy.get("tools") == {"kind": "v0.27.0", "k3d": "v5.9.0", "kubectl": "v1.32.2", "helm": "v4.2.3"}, "fault tool versions drifted")
    images = policy.get("images", {})
    guard.require(images.get("required_services") == list(SERVICES), "fault matrix must cover exactly five services")
    guard.require(images.get("fault_driver_target") == "fault-driver", "test-only fault-driver target drifted")
    guard.require(images.get("fault_driver_test_only") is True, "fault driver must remain test-only")
    guard.require(policy.get("global_assertions") == list(GLOBAL_ASSERTIONS), "global fault assertions drifted")

    scenarios = policy.get("scenarios", [])
    guard.require(isinstance(scenarios, list), "scenarios must be a list")
    scenario_ids = [item.get("id") for item in scenarios if isinstance(item, dict)]
    guard.require(
        scenario_ids == list(SCENARIOS),
        f"fault policy must define the exact ordered {len(SCENARIOS)} scenarios",
    )
    for item in scenarios if isinstance(scenarios, list) else []:
        if not isinstance(item, dict):
            guard.errors.append("every fault scenario must be an object")
            continue
        scenario = item.get("id", "unknown")
        guard.require(isinstance(item.get("timeout_seconds"), int) and item["timeout_seconds"] > 0, f"{scenario}: timeout must be positive")
        guard.require(bool(item.get("precondition")), f"{scenario}: precondition contract missing")
        guard.require(bool(item.get("injection")), f"{scenario}: injection contract missing")
        guard.require(bool(item.get("observable")), f"{scenario}: observable contract missing")
        guard.require(
            isinstance(item.get("rpo_seconds"), int) and item["rpo_seconds"] >= 0,
            f"{scenario}: RPO must be a non-negative integer",
        )
        guard.require(
            isinstance(item.get("rto_seconds"), int) and 0 < item["rto_seconds"] <= item["timeout_seconds"],
            f"{scenario}: RTO must be positive and no greater than timeout",
        )
        guard.require(bool(item.get("abort_condition")), f"{scenario}: abort condition missing")
        guard.require(bool(item.get("cleanup")), f"{scenario}: cleanup contract missing")
        guard.require(bool(item.get("cleanup_verification")), f"{scenario}: cleanup verification missing")
        assertions = item.get("required_assertions")
        evidence = item.get("required_evidence")
        guard.require(isinstance(assertions, list) and len(assertions) >= 4, f"{scenario}: insufficient assertions")
        guard.require(isinstance(evidence, list) and len(evidence) >= 4, f"{scenario}: insufficient evidence")
        guard.require(len(set(assertions or [])) == len(assertions or []), f"{scenario}: duplicate assertions")
        guard.require(len(set(evidence or [])) == len(evidence or []), f"{scenario}: duplicate evidence fields")

    evidence = policy.get("evidence", {})
    guard.require(set(evidence.get("required_run_fields", [])) == REQUIRED_RUN_FIELDS, "required run evidence fields drifted")
    guard.require(evidence.get("production_requires_dynamic_execution") is True, "production evidence must require dynamic execution")
    guard.require(evidence.get("fixture_requires_explicit_opt_in") is True, "fixtures must require explicit opt-in")
    rollback = policy.get("rollback", {})
    guard.require(rollback.get("restore_baseline_digests") is True, "rollback must restore baseline digests")
    guard.require(rollback.get("restore_baseline_chart") is True, "rollback must restore the baseline chart")
    guard.require(rollback.get("delete_pvcs") is False, "rollback must not delete PVCs")
    guard.require(rollback.get("delete_wal") is False, "rollback must not delete WAL")
    guard.require(rollback.get("preserve_evidence") is True, "rollback must preserve evidence")


def validate_sources(guard: Guard) -> None:
    runner = guard.read("scripts/kind-architecture-refactor-e2e.ps1")
    secret_generator = guard.read("scripts/new-m11-evidence-secrets.ps1")
    workflow = guard.read(".github/workflows/kubernetes-fault-matrix.yml")
    publication_verifier = guard.read("scripts/verify_service_image_publication.py")
    dockerfile = guard.read("docker/Dockerfile.base")
    tests = guard.read("scripts/tests/test_m11_fault_matrix.py")
    readme = guard.read("distribution/kubernetes/README.md")
    for marker in (
        '[ValidateSet("Validate", "Run")]',
        '[ValidateSet("kind", "k3d")]',
        "dynamic_execution",
        "candidate_commit = $CandidateCommit",
        "rolling_upgrade",
        "node_eviction",
        "nameserver_minority_partition",
        "nameserver_majority_unavailable",
        "collector_outage",
        "disk_pressure",
        "disk_full",
        "slow_disk_fsync_jitter",
        "controller_leader_failure",
        "controller_quorum_loss",
        "network_impairment",
        "ha_replication_lag",
        "snapshot_install_interruption",
        "proxy_slow_broker_overload",
        "secret_rotation",
        "acknowledged_message_recovery",
        "Invoke-Native kubectl @('drain'",
        "node.kubernetes.io/disk-pressure",
        "rocketmq.apache.org/simulated-disk-pressure",
        "$pressureStatusDuring -match 'rocketmq.apache.org/simulated-disk-pressure'",
        "$_.key -in $diskPressureTaintKeys",
        "$taintCleanup.ExitCode -eq 0 -or $taintCleanup.Output -match 'not found'",
        "$simulationTaintCleanup.ExitCode -eq 0 -or $simulationTaintCleanup.Output -match 'not found'",
        "kubelet already removed it",
        "queryMsgByUniqueKey",
        "fault matrix may create only its fixed synthetic topic",
        "'updateTopic'",
        "'RocketmqRust'",
        "queue_offset_preserved",
        "commitlog_offset_preserved",
        "pvc_uid_set_preserved",
        "$evictionProxyUid",
        "$proxyUidsBeforeEviction -notcontains",
        "eviction_api_used = $null -ne $evictionReplacementProxyPod",
        "$pressureProxyUid",
        "$proxyUidsBeforePressure -notcontains",
        "stateless_pod_rescheduled = $null -ne $replacementProxyPod",
        "Get-ControllerLeaderOrdinal",
        "leader_changed = $leaderAfterOrdinal -ne $failureLeaderOrdinal",
        "Set-StatefulSetReplicas 'rocketmq-namesrv' 1",
        "Set-StatefulSetReplicas 'rocketmq-controller' 1",
        "Set-NodeNetworkImpairment $minorityNode @('loss', '100%')",
        "Set-NodeNetworkImpairment $networkNode @('delay', '200ms', '50ms', 'loss', '10%')",
        "disk_full_state_rejects_new_writes_without_losing_acknowledged_data",
        "i=0; : > /tmp/rocketmq/phase06-fsync.pids; while",
        "attempt=0; active=1; while",
        'case "$state" in ""|Z*)',
        "/tmp/rocketmq/phase06-fsync-",
        "sync_master_without_slave_ack_returns_flush_slave_timeout",
        "three_controller_two_broker_controller_mode_failover_and_rejoin",
        "interrupted_snapshot_install_is_rejected_and_full_retry_recovers",
        "unrelated_route_progresses_while_pull_is_blocked",
        "same_consumer_key_is_fifo_without_serializing_distinct_keys",
        "data_saturation_preserves_readiness_capacity_and_releases_all_permits",
        "leaked=0 detached_still_running=0",
        "Invoke-Native docker @('pull', $image)",
        "'org.opencontainers.image.revision'",
        "@($revisions | Sort-Object -Unique)",
        "candidate image revision must equal CandidateCommit",
        "'ROCKETMQ_RELEASE_COMMIT'",
        "'--patch-file', $patchPath",
        "Invoke-Native docker @('tag', $image, $cacheTag)",
        "Invoke-Native kind @('load', 'image-archive', $archivePath, '--name', $ClusterName)",
        "'images', 'tag', $cacheReference, $targetReference",
        "Invoke-Native k3d (@('image', 'import') + $clusterImages",
        "selector: { matchLabels: { app.kubernetes.io/name: otel-collector } }",
        "$jobStatus.failed ?? 0",
        "broker cluster registration was not observed before the synthetic topic deadline",
        "Wait-ReadyWorkerPod -Selector 'rocketmq.apache.org/service=proxy'",
        "Invoke-Native kubectl @('cordon', $minorityNode)",
        "Invoke-Native kubectl @('cordon', $networkNode)",
        "rocketmq.apache.org/service=controller",
        "rocketmq.apache.org/service=broker",
        "type = 'OnDelete'",
        "ControllerLeaderId\\s+([1-3])",
        "Invoke-Native kubectl @('cordon', $leaderNode)",
        "Sort-Object -Descending",
        "Wait-ControllerReplicationCaughtUp",
        "CommittedLogIndex",
        "AppliedLogIndex",
        "$actualScenarioOrder = (($ScenarioRecords | ForEach-Object { $_.id }) -join ',')",
        "Assert-True ($actualScenarioOrder -eq $expectedScenarioOrder)",
        "$null = Wait-ControllerLeadershipStable -Ordinals $survivingOrdinals",
        "Invoke-Native kubectl @('cordon', $failureLeaderNode)",
        "Wait-ControllerLeadershipStable -Ordinals $failureSurvivingOrdinals",
        "$restoredLeadership = Wait-ControllerLeadershipStable",
        "$snapshotLeadershipBefore = Wait-ControllerLeadershipStable",
        "function Test-MessageQuerySucceeded",
        "function Wait-CredentialCutover",
        "credential rotation must converge through hot reload without restarting Broker pods",
        "semantic_query=$($rotationResult.DeniedSucceeded)",
    ):
        guard.require(marker in runner, f"fault runner contract marker missing: {marker}")
    secret_rotation_start = runner.find("$preRotation = Query-AcknowledgedMessage $ack.Id")
    secret_rotation_end = runner.find("$pvcBeforeRestart = Get-PvcUidSet", secret_rotation_start)
    guard.require(
        secret_rotation_start >= 0 and secret_rotation_end > secret_rotation_start,
        "secret rotation block boundaries are missing",
    )
    if secret_rotation_start >= 0 and secret_rotation_end > secret_rotation_start:
        secret_rotation_block = runner[secret_rotation_start:secret_rotation_end]
        guard.require(
            "rollout', 'restart', 'statefulset/rocketmq-broker" not in secret_rotation_block,
            "secret rotation must use Broker hot reload instead of a rollout restart",
        )
    guard.require("Mode -eq \"Validate\"" in runner, "runner must provide a non-dynamic Validate mode")
    guard.require("throw" in runner, "runner must fail closed on missing prerequisites or failed assertions")
    guard.require("FROM builder-base AS fault-driver-builder" in dockerfile, "fault-driver builder target missing")
    guard.require("FROM runtime-base AS fault-driver" in dockerfile, "fault-driver runtime target missing")
    guard.require('io.rocketmq.image.role="fault-driver-test-only"' in dockerfile, "fault driver test-only label missing")
    guard.require("fault_matrix_guard.py" in workflow, "fault workflow must execute evidence guard")
    guard.require("kind-architecture-refactor-e2e.ps1" in workflow, "fault workflow must execute the real runner")
    guard.require("workflow_dispatch:" in workflow, "fault workflow must support explicit dynamic dispatch")
    guard.require(
        "permissions:\n  contents: read\n  packages: read" in workflow,
        "fault workflow must retain least-privilege contents/package reads",
    )
    guard.require("docker login ghcr.io" in workflow, "fault workflow must authenticate immutable GHCR pulls")
    guard.require(
        "CANDIDATE_COMMIT=$(git rev-parse HEAD)" in workflow
        and "-CandidateCommit '${{ steps.bind-candidate.outputs.candidate_commit }}'" in workflow
        and "m11-11-${{ env.EVIDENCE_BACKEND }}-${{ steps.bind-candidate.outputs.candidate_commit }}" in workflow,
        "fault workflow must bind execution and artifact identity to the checked-out candidate commit",
    )
    guard.require(
        "candidate_publication_json:" in workflow
        and "ARCHITECTURE_CANDIDATE_PUBLICATION_JSON" in workflow
        and "candidate_images_json" not in workflow
        and "ARCHITECTURE_CANDIDATE_IMAGES_JSON" not in workflow,
        "fault workflow must accept a signed candidate publication, not a self-reported image map",
    )
    guard.require(
        "sigstore/cosign-installer@6f9f17788090df1f26f669e9d70d6ae9567deba6" in workflow
        and "cosign-release: v3.1.2" in workflow,
        "fault workflow must install pinned Cosign v3.1.2",
    )
    publication_command = "python scripts/verify_service_image_publication.py"
    candidate_map_argument = "-CandidateImageMap target/fault-inputs/candidate-images.json"
    guard.require(
        publication_command in workflow
        and '--publication target/fault-inputs/publication.json' in workflow
        and '--candidate "$CANDIDATE_COMMIT"' in workflow
        and "--output-image-map target/fault-inputs/candidate-images.json" in workflow
        and candidate_map_argument in workflow
        and workflow.index(publication_command) < workflow.index(candidate_map_argument),
        "fault workflow must validate the signed candidate publication before deriving its image map",
    )
    for marker in (
        'SOURCE = "workflow://mxsm/rocketmq-rust/service-image-publish"',
        'CATEGORY = "five_image_supply_chain"',
        'CERTIFICATE_OIDC_ISSUER = "https://token.actions.githubusercontent.com"',
        'service-image-publish\\.yml@refs/(heads/main|tags/',
        '"service-image-evidence/v1"',
        'publication.get("candidate_commit") == candidate',
        'json_exact_equal(tools, TOOLS)',
        'json_exact_equal(policy, POLICY)',
        'path not in artifact_map',
        'artifacts.get(normalized_path) == normalized_sha256',
        'digest not in image_digests',
        'image.get("digest") == digest',
        'image.get("digest_reference") == reference',
        '"org.opencontainers.image.revision=',
        '"io.rocketmq.service=',
        '"verify-attestation"',
        'envelope.get("payloadType") == DSSE_PAYLOAD_TYPE',
        'base64.b64decode(padded_payload, validate=True)',
        'set(statement) != {',
        'statement.get("_type") == STATEMENT_TYPE',
        'statement.get("predicateType") == PREDICATE_TYPE',
        'len(subjects) == 1',
        'subjects[0].get("name") == repository',
        'json_exact_equal(statement.get("predicate"), predicate)',
    ):
        guard.require(
            marker in publication_verifier,
            f"signed service-image publication verifier contract missing: {marker}",
        )
    verification_index = publication_verifier.find("images = verify_publication")
    output_index = publication_verifier.find("write_image_map(", verification_index)
    guard.require(
        verification_index >= 0 and output_index > verification_index,
        "candidate image map must be written only after signed publication verification",
    )
    guard.require(
        "new-m11-evidence-secrets.ps1" in workflow,
        "fault workflow must generate isolated run-scoped evidence credentials",
    )
    guard.require(
        "M11_RUNTIME_SECRET_MANIFEST_B64" not in workflow
        and "M11_ROTATED_RUNTIME_SECRET_MANIFEST_B64" not in workflow,
        "fault workflow must not depend on repository-stored test secret manifests",
    )
    guard.require(
        "Remove-Item -LiteralPath $inputDirectory -Recurse -Force" in workflow
        and "docker logout ghcr.io" in workflow,
        "fault workflow must remove run-scoped credentials and registry sessions",
    )
    for marker in (
        "[Security.Cryptography.RandomNumberGenerator]::Create",
        "M11_EPHEMERAL_SECRET_MANIFESTS_OK",
        "rocketmq-fault-driver-baseline",
        "rocketmq-fault-driver-rotated",
    ):
        guard.require(marker in secret_generator, f"evidence secret generator marker missing: {marker}")
    guard.require("test_m11_fault_matrix" in workflow and "deliberate" in tests, "deliberate-violation tests missing")
    guard.require("dynamic" in readme.lower() and "fault" in readme.lower(), "Kubernetes README must document dynamic fault evidence")


def validate_image_map(guard: Guard, label: str, value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        guard.errors.append(f"{label} must be an object")
        return {}
    guard.require(set(value) == set(SERVICES), f"{label} must contain exactly five services")
    result: dict[str, str] = {}
    for service in SERVICES:
        reference = value.get(service)
        match = DIGEST_RE.fullmatch(reference) if isinstance(reference, str) else None
        guard.require(match is not None, f"{label}.{service} must be an image@sha256 digest")
        if match:
            guard.require(match.group(1) not in PLACEHOLDER_HASHES, f"{label}.{service} uses a placeholder digest")
            result[service] = reference
    return result


def validate_evidence(guard: Guard, policy: dict[str, Any], evidence_dir: Path, allow_fixture: bool) -> None:
    run = guard.load_json(evidence_dir / "run.json", "run.json")
    if not run:
        return
    guard.require(set(run) == REQUIRED_RUN_FIELDS, "run.json fields must match the exact evidence schema")
    guard.require(run.get("schema_version") == 1, "run evidence schema_version must remain 1")
    guard.require(run.get("milestone") == "M11-11", "run milestone must remain M11-11")
    guard.require(
        run.get("policy_sha256") == canonical_json_sha256(policy),
        "run policy_sha256 does not match the canonical committed policy",
    )
    guard.require(isinstance(run.get("run_id"), str) and bool(run["run_id"].strip()), "run_id must be non-empty")
    started = parse_utc(run.get("started_at"), "started_at", guard)
    finished = parse_utc(run.get("finished_at"), "finished_at", guard)
    if started and finished:
        guard.require(finished >= started, "finished_at must not precede started_at")
    guard.require(run.get("backend") in ("kind", "k3d"), "run backend must be kind or k3d")
    fixture = run.get("fixture") is True
    if fixture:
        guard.require(allow_fixture, "fixture evidence requires --allow-fixture")
        guard.require(run.get("dynamic_execution") is False, "fixture evidence must not claim dynamic execution")
    else:
        guard.require(run.get("fixture") is False, "production evidence must set fixture=false")
        guard.require(run.get("dynamic_execution") is True, "production evidence must come from dynamic execution")

    candidate_commit = run.get("candidate_commit")
    guard.require(
        isinstance(candidate_commit, str) and COMMIT_RE.fullmatch(candidate_commit) is not None,
        "candidate_commit must be a full Git SHA",
    )
    if not fixture and isinstance(candidate_commit, str) and COMMIT_RE.fullmatch(candidate_commit):
        guard.require(
            candidate_commit == current_commit(guard.root, guard),
            "production evidence candidate_commit differs from the checked-out commit",
        )

    profile = run.get("cluster_profile")
    guard.require(isinstance(profile, dict), "cluster_profile must be an object")
    if isinstance(profile, dict):
        guard.require(profile.get("control_plane_nodes") == 1, "cluster profile must record one control plane")
        guard.require(profile.get("worker_nodes") == 3, "cluster profile must record three workers")
        guard.require(profile.get("broker_replicas") == 3, "cluster profile must record three Brokers")
        guard.require(profile.get("controller_replicas") == 3, "cluster profile must record three Controllers")
        guard.require(bool(profile.get("storage_class")), "cluster profile storage class missing")
        guard.require(isinstance(profile.get("nodes"), list) and len(profile["nodes"]) == 4, "cluster profile must record four nodes")
    tools = run.get("tool_versions")
    guard.require(isinstance(tools, dict) and set(tools) == {"docker", "kind", "k3d", "kubectl", "helm"}, "tool version evidence incomplete")
    for key in ("chart_sha256", "overlay_sha256"):
        guard.require(isinstance(run.get(key), str) and SHA_RE.fullmatch(run[key]) is not None, f"{key} must be sha256")

    baseline = validate_image_map(guard, "baseline_images", run.get("baseline_images"))
    candidate = validate_image_map(guard, "candidate_images", run.get("candidate_images"))
    if baseline and candidate:
        guard.require(any(baseline[name] != candidate[name] for name in SERVICES), "candidate images must differ from baseline")

    assertions = run.get("global_assertions")
    guard.require(isinstance(assertions, dict) and set(assertions) == set(GLOBAL_ASSERTIONS), "global assertions must match policy")
    if isinstance(assertions, dict):
        for name in GLOBAL_ASSERTIONS:
            guard.require(assertions.get(name) is True, f"global assertion failed: {name}")
    guard.require(run.get("unresolved_faults") == [], "unresolved_faults must be empty")

    policy_scenarios = {item["id"]: item for item in policy.get("scenarios", []) if isinstance(item, dict) and "id" in item}
    scenario_records = run.get("scenarios")
    guard.require(isinstance(scenario_records, list), "scenarios evidence must be a list")
    observed_ids = [item.get("id") for item in scenario_records or [] if isinstance(item, dict)]
    guard.require(
        observed_ids == list(SCENARIOS),
        f"evidence must contain exact ordered {len(SCENARIOS)} scenarios",
    )
    for record in scenario_records or []:
        if not isinstance(record, dict):
            guard.errors.append("scenario evidence must be an object")
            continue
        scenario = record.get("id", "unknown")
        expected = policy_scenarios.get(str(scenario), {})
        expected_assertions = set(expected.get("required_assertions", []))
        actual_assertions = record.get("assertions")
        guard.require(record.get("status") == "passed", f"{scenario}: status must be passed")
        guard.require(isinstance(actual_assertions, dict) and set(actual_assertions) == expected_assertions, f"{scenario}: assertion keys drifted")
        if isinstance(actual_assertions, dict):
            for name in expected_assertions:
                guard.require(actual_assertions.get(name) is True, f"{scenario}: assertion failed: {name}")
        actual_evidence = record.get("evidence")
        expected_evidence = set(expected.get("required_evidence", []))
        guard.require(isinstance(actual_evidence, dict) and set(actual_evidence) == expected_evidence, f"{scenario}: evidence keys drifted")
        if isinstance(actual_evidence, dict):
            for name in expected_evidence:
                guard.require(isinstance(actual_evidence.get(name), str) and bool(actual_evidence[name].strip()), f"{scenario}: empty evidence: {name}")

    artifacts = run.get("artifacts")
    guard.require(isinstance(artifacts, list) and bool(artifacts), "artifact index must be non-empty")
    seen_paths: set[str] = set()
    for item in artifacts or []:
        if not isinstance(item, dict) or set(item) != {"path", "sha256"}:
            guard.errors.append("each artifact index item must contain only path and sha256")
            continue
        relative = item.get("path")
        expected_hash = item.get("sha256")
        guard.require(isinstance(relative, str) and bool(relative), "artifact path must be non-empty")
        if not isinstance(relative, str) or not relative:
            continue
        relative_path = Path(relative)
        guard.require(not relative_path.is_absolute() and ".." not in relative_path.parts, f"unsafe artifact path: {relative}")
        guard.require(relative not in seen_paths, f"duplicate artifact path: {relative}")
        seen_paths.add(relative)
        artifact_path = evidence_dir / relative_path
        guard.require(artifact_path.is_file(), f"missing artifact: {relative}")
        guard.require(isinstance(expected_hash, str) and SHA_RE.fullmatch(expected_hash) is not None, f"invalid artifact hash: {relative}")
        if artifact_path.is_file() and isinstance(expected_hash, str):
            guard.require(sha256_file(artifact_path) == expected_hash, f"artifact hash mismatch: {relative}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--policy-only", action="store_true")
    parser.add_argument("--print-policy-sha256", action="store_true")
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--allow-fixture", action="store_true")
    args = parser.parse_args()
    if args.print_policy_sha256 and (args.evidence or args.allow_fixture):
        parser.error("--print-policy-sha256 cannot be combined with evidence options")
    root = args.root.resolve()
    guard = Guard(root)
    policy_path = root / "distribution/kubernetes/fault-matrix-policy.json"
    policy = guard.load_json(policy_path, "fault-matrix-policy.json")
    validate_policy(guard, policy)
    if not args.print_policy_sha256:
        validate_sources(guard)
    if args.evidence:
        evidence_dir = args.evidence if args.evidence.is_absolute() else root / args.evidence
        validate_evidence(guard, policy, evidence_dir.resolve(), args.allow_fixture)
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
    print(f"M11-11 fault matrix {mode} guard passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
