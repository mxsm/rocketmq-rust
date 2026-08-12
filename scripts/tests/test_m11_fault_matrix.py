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

"""Positive-fixture and deliberate-violation tests for M11-11 evidence."""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
GUARD = REPO_ROOT / "scripts" / "fault_matrix_guard.py"
FIXTURE = Path("scripts/tests/fixtures/m11-fault-matrix/pass")


class FaultMatrixGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="m11-fault-matrix-")
        self.root = Path(self.temporary.name)
        shutil.copytree(REPO_ROOT / "distribution", self.root / "distribution")
        (self.root / "docker").mkdir(parents=True)
        shutil.copy2(REPO_ROOT / "docker" / "Dockerfile.base", self.root / "docker" / "Dockerfile.base")
        (self.root / "scripts" / "tests" / "fixtures" / "m11-fault-matrix").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / "scripts" / "kind-architecture-refactor-e2e.ps1",
            self.root / "scripts" / "kind-architecture-refactor-e2e.ps1",
        )
        (self.root / "scripts" / "kubernetes").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / "scripts" / "kubernetes" / "live_faults.ps1",
            self.root / "scripts" / "kubernetes" / "live_faults.ps1",
        )
        shutil.copy2(
            REPO_ROOT / "scripts" / "new-m11-evidence-secrets.ps1",
            self.root / "scripts" / "new-m11-evidence-secrets.ps1",
        )
        shutil.copy2(
            REPO_ROOT / "scripts" / "verify_service_image_publication.py",
            self.root / "scripts" / "verify_service_image_publication.py",
        )
        shutil.copy2(
            REPO_ROOT / "scripts" / "tests" / "test_m11_fault_matrix.py",
            self.root / "scripts" / "tests" / "test_m11_fault_matrix.py",
        )
        shutil.copytree(
            REPO_ROOT / FIXTURE,
            self.root / FIXTURE,
        )
        (self.root / ".github" / "workflows").mkdir(parents=True)
        shutil.copy2(
            REPO_ROOT / ".github" / "workflows" / "kubernetes-fault-matrix.yml",
            self.root / ".github" / "workflows" / "kubernetes-fault-matrix.yml",
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    @property
    def run_path(self) -> Path:
        return self.root / FIXTURE / "run.json"

    def run_guard(self, *arguments: str, expect_success: bool) -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            [sys.executable, str(GUARD), "--root", str(self.root), *arguments],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        if expect_success:
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("guard passed", result.stdout)
        else:
            self.assertNotEqual(result.returncode, 0, result.stdout)
            self.assertIn("ERROR:", result.stderr)
        return result

    def mutate_run(self, mutation) -> None:  # type: ignore[no-untyped-def]
        run = json.loads(self.run_path.read_text(encoding="utf-8"))
        mutation(run)
        self.run_path.write_text(json.dumps(run, indent=2) + "\n", encoding="utf-8")

    def mutate_policy(self, mutation) -> None:  # type: ignore[no-untyped-def]
        path = self.root / "distribution" / "kubernetes" / "fault-matrix-policy.json"
        policy = json.loads(path.read_text(encoding="utf-8"))
        mutation(policy)
        path.write_text(json.dumps(policy, indent=2) + "\n", encoding="utf-8")

    def test_policy_and_positive_fixture_pass(self) -> None:
        self.run_guard("--policy-only", expect_success=True)
        self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=True)

    def test_policy_hash_ignores_line_endings(self) -> None:
        policy = self.root / "distribution" / "kubernetes" / "fault-matrix-policy.json"
        policy.write_bytes(policy.read_bytes().replace(b"\n", b"\r\n"))
        self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=True)

    def test_fixture_requires_explicit_opt_in(self) -> None:
        result = self.run_guard("--evidence", str(FIXTURE), expect_success=False)
        self.assertIn("--allow-fixture", result.stderr)

    def test_non_dynamic_production_evidence_is_rejected(self) -> None:
        self.mutate_run(lambda run: run.update(fixture=False))
        result = self.run_guard("--evidence", str(FIXTURE), expect_success=False)
        self.assertIn("dynamic execution", result.stderr)

    def test_candidate_commit_must_be_a_full_sha(self) -> None:
        self.mutate_run(lambda run: run.update(candidate_commit="main"))
        result = self.run_guard(
            "--evidence", str(FIXTURE), "--allow-fixture", expect_success=False
        )
        self.assertIn("candidate_commit must be a full Git SHA", result.stderr)

    def test_failed_global_assertion_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["global_assertions"].update(rollback_verified=False))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("rollback_verified", result.stderr)

    def test_missing_scenario_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["scenarios"].pop())
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("exact ordered 16 scenarios", result.stderr)

    def test_scenario_assertion_key_drift_is_rejected(self) -> None:
        self.mutate_run(lambda run: run["scenarios"][0]["assertions"].pop("queue_offset_preserved"))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("rolling_upgrade: assertion keys drifted", result.stderr)

    def test_scenario_without_abort_or_cleanup_contract_is_rejected(self) -> None:
        self.mutate_policy(
            lambda policy: (
                policy["scenarios"][0].pop("abort_condition"),
                policy["scenarios"][0].pop("cleanup_verification"),
            )
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("abort condition", result.stderr)
        self.assertIn("cleanup verification", result.stderr)

    def test_placeholder_image_digest_is_rejected(self) -> None:
        self.mutate_run(
            lambda run: run["candidate_images"].update(
                broker="example.invalid/broker@sha256:" + "0" * 64
            )
        )
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("placeholder digest", result.stderr)

    def test_unresolved_fault_is_rejected(self) -> None:
        self.mutate_run(lambda run: run.update(unresolved_faults=["worker-remains-cordoned"]))
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("unresolved_faults", result.stderr)

    def test_artifact_hash_mismatch_is_rejected(self) -> None:
        artifact = self.root / FIXTURE / "artifacts" / "rolling_upgrade.txt"
        artifact.write_text("tampered\n", encoding="utf-8")
        result = self.run_guard("--evidence", str(FIXTURE), "--allow-fixture", expect_success=False)
        self.assertIn("artifact hash mismatch", result.stderr)

    def test_runner_that_drops_durability_marker_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        self.assertIn("commitlog_offset_preserved", source)
        runner.write_text(source.replace("commitlog_offset_preserved", "offset_not_checked"), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("commitlog_offset_preserved", result.stderr)

    def test_runner_that_drops_fixed_synthetic_topic_initialization_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "fault matrix may create only its fixed synthetic topic"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "arbitrary topic creation allowed", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("fixed synthetic topic", result.stderr)

    def test_runner_that_drops_atomic_release_identity_rollout_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "candidate image revision must equal CandidateCommit"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "candidate revision unchecked", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("CandidateCommit", result.stderr)

    def test_runner_that_drops_network_fault_injection_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "Set-NodeNetworkImpairment $networkNode @('delay', '200ms', '50ms', 'loss', '10%')"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "network impairment skipped"), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("Set-NodeNetworkImpairment", result.stderr)

    def test_runner_that_restores_multiline_broker_shell_scripts_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        for marker in (
            "i=0; : > /tmp/rocketmq/phase06-fsync.pids; while",
            "attempt=0; active=1; while",
            'case "$state" in ""|Z*)',
        ):
            with self.subTest(marker=marker):
                self.assertIn(marker, source)
                runner.write_text(source.replace(marker, "multiline shell regression", 1), encoding="utf-8")
                result = self.run_guard("--policy-only", expect_success=False)
                self.assertIn(marker, result.stderr)
                runner.write_text(source, encoding="utf-8")

    def test_runner_that_mislabels_the_otel_collector_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "selector: { matchLabels: { app.kubernetes.io/name: otel-collector } }"
        self.assertIn(marker, source)
        runner.write_text(
            source.replace(marker, "selector: { matchLabels: { app: otel-collector } }", 1),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("app.kubernetes.io/name: otel-collector", result.stderr)

    def test_runner_that_drops_fault_job_failure_detection_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "$jobStatus.failed ?? 0"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "$jobStatus.succeeded ?? 0", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("$jobStatus.failed ?? 0", result.stderr)

    def test_runner_that_drops_broker_registration_wait_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "broker cluster registration was not observed before the synthetic topic deadline"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "cluster readiness unchecked", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("broker cluster registration", result.stderr)

    def test_runner_that_drops_ready_proxy_wait_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "Wait-ReadyWorkerPod -Selector 'rocketmq.apache.org/service=proxy'"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, "Get-UncheckedProxyPod"), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("Wait-ReadyWorkerPod", result.stderr)

    def test_runner_that_allows_fault_jobs_on_impaired_nodes_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        for node in ("minorityNode", "networkNode"):
            marker = f"Invoke-Native kubectl @('cordon', ${node})"
            self.assertIn(marker, source)
            runner.write_text(source.replace(marker, "fault node left schedulable", 1), encoding="utf-8")
            result = self.run_guard("--policy-only", expect_success=False)
            self.assertIn(marker, result.stderr)
            runner.write_text(source, encoding="utf-8")

    def test_runner_that_drops_stable_disk_pressure_evidence_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        markers = (
            "rocketmq.apache.org/simulated-disk-pressure",
            "$pressureStatusDuring -match 'rocketmq.apache.org/simulated-disk-pressure'",
            "$_.key -in $diskPressureTaintKeys",
            "$taintCleanup.ExitCode -eq 0 -or $taintCleanup.Output -match 'not found'",
            "$simulationTaintCleanup.ExitCode -eq 0 -or $simulationTaintCleanup.Output -match 'not found'",
        )
        for marker in markers:
            with self.subTest(marker=marker):
                self.assertIn(marker, source)
                runner.write_text(source.replace(marker, "disk pressure evidence removed"), encoding="utf-8")
                result = self.run_guard("--policy-only", expect_success=False)
                self.assertIn(marker, result.stderr)
                runner.write_text(source, encoding="utf-8")

    def test_runner_that_uses_nonexistent_component_labels_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        markers = (
            "rocketmq.apache.org/service=controller",
            "app.kubernetes.io/part-of=rocketmq-rust",
            "$null -eq $_.metadata.deletionTimestamp",
            "$readyFinalPods = @(",
            "$readyFinalPods.Count -eq 12",
        )
        for marker in markers:
            with self.subTest(marker=marker):
                self.assertIn(marker, source)
                runner.write_text(source.replace(marker, "component label contract removed"), encoding="utf-8")
                result = self.run_guard("--policy-only", expect_success=False)
                self.assertIn(marker, result.stderr)
                runner.write_text(source, encoding="utf-8")

    def test_runner_that_drops_leader_aware_controller_rollout_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        markers = (
            "type = 'OnDelete'",
            "ControllerLeaderId\\s+([1-3])",
            "Invoke-Native kubectl @('cordon', $leaderNode)",
            "Sort-Object -Descending",
            "Wait-ControllerReplicationCaughtUp",
            "CommittedLogIndex",
            "AppliedLogIndex",
            "$actualScenarioOrder = (($ScenarioRecords | ForEach-Object { $_.id }) -join ',')",
            "Assert-True ($actualScenarioOrder -eq $expectedScenarioOrder)",
            "Wait-PodRecreatedAndReady -Pod 'rocketmq-broker-0' -PreviousUid $brokerStateBeforeRestart.metadata.uid",
            "$null = Wait-ControllerLeadershipStable -Ordinals $survivingOrdinals",
            "Invoke-Native kubectl @('cordon', $failureLeaderNode)",
            "Wait-ControllerLeadershipStable -Ordinals $failureSurvivingOrdinals",
            "$restoredLeadership = Wait-ControllerLeadershipStable",
            "$snapshotLeadershipBefore = Wait-ControllerLeadershipStable",
            "function Set-PodNetworkIsolation",
            "$minorityFault = Set-PodNetworkIsolation",
            "$minorityDirectProbe = Invoke-RouteProbe -Namesrv $minorityAddress -AllowFailure",
            "$minorityRestore = Clear-PodNetworkIsolation",
            "function Test-MessageQuerySucceeded",
            "$querySucceeded = Test-MessageQuerySucceeded $result",
            "function Wait-CredentialCutover",
            "credential rotation must converge through hot reload without restarting Broker pods",
            "semantic_query=$($rotationResult.DeniedSucceeded)",
        )
        for marker in markers:
            self.assertIn(marker, source)
            runner.write_text(source.replace(marker, "controller rollout stability skipped"), encoding="utf-8")
            result = self.run_guard("--policy-only", expect_success=False)
            self.assertIn(marker, result.stderr)
            runner.write_text(source, encoding="utf-8")

    def test_nameserver_minority_fault_cannot_isolate_the_co_located_node(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "$minorityFault = Set-PodNetworkIsolation -Node $minorityNode -PodIp $minorityPodIp -RuleTag $minorityRuleTag"
        replacement = "$minorityFault = Set-NodeNetworkImpairment $minorityNode @('loss', '100%')"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, replacement, 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("target pod", result.stderr)

    def test_runner_that_restarts_brokers_for_secret_rotation_is_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        marker = "$preRotation = Query-AcknowledgedMessage $ack.Id"
        restart = "Invoke-Native kubectl @('-n', $Namespace, 'rollout', 'restart', 'statefulset/rocketmq-broker') | Out-Null"
        self.assertIn(marker, source)
        runner.write_text(source.replace(marker, f"{marker}\n    {restart}", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("hot reload", result.stderr)

    def test_dynamic_input_and_registry_regressions_are_rejected(self) -> None:
        workflow = self.root / ".github" / "workflows" / "kubernetes-fault-matrix.yml"
        source = workflow.read_text(encoding="utf-8")
        workflow.write_text(source.replace("  packages: read\n", "", 1), encoding="utf-8")
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("contents/package reads", result.stderr)

        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        runner_source = runner.read_text(encoding="utf-8")
        runner.write_text(
            runner_source.replace("Invoke-Native docker @('pull', $image)", "docker pull skipped", 1),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("docker @('pull', $image)", result.stderr)

        runner.write_text(
            runner_source.replace(
                "Invoke-Native kind @('load', 'image-archive', $archivePath, '--name', $ClusterName)",
                "Kind archive import skipped",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("image-archive", result.stderr)

    def test_workflow_artifact_uses_resolved_candidate_commit(self) -> None:
        workflow = self.root / ".github" / "workflows" / "kubernetes-fault-matrix.yml"
        source = workflow.read_text(encoding="utf-8")
        workflow.write_text(
            source.replace(
                "m11-11-${{ env.EVIDENCE_BACKEND }}-${{ steps.bind-candidate.outputs.candidate_commit }}",
                "m11-11-${{ env.EVIDENCE_BACKEND }}-${{ github.sha }}",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("checked-out candidate commit", result.stderr)

    def test_signed_publication_workflow_regressions_are_rejected(self) -> None:
        workflow = self.root / ".github" / "workflows" / "kubernetes-fault-matrix.yml"
        source = workflow.read_text(encoding="utf-8")
        mutations = (
            ("candidate_publication_json:", "candidate_images_json:", "signed candidate publication"),
            ('--candidate "$CANDIDATE_COMMIT"', '--candidate "f"', "before deriving its image map"),
            ("cosign-release: v3.1.2", "cosign-release: latest", "pinned Cosign v3.1.2"),
        )
        for old, new, finding in mutations:
            with self.subTest(old=old):
                self.assertIn(old, source)
                workflow.write_text(source.replace(old, new, 1), encoding="utf-8")
                result = self.run_guard("--policy-only", expect_success=False)
                self.assertIn(finding, result.stderr)

    def test_attestation_verifier_regressions_are_rejected(self) -> None:
        verifier = self.root / "scripts" / "verify_service_image_publication.py"
        source = verifier.read_text(encoding="utf-8")
        mutations = (
            (
                'subjects[0].get("name") == repository',
                'subjects[0].get("name") is not None',
            ),
            (
                "base64.b64decode(padded_payload, validate=True)",
                "base64.b64decode(padded_payload)",
            ),
        )
        for old, new in mutations:
            with self.subTest(old=old):
                self.assertIn(old, source)
                verifier.write_text(source.replace(old, new, 1), encoding="utf-8")
                result = self.run_guard("--policy-only", expect_success=False)
                self.assertIn("publication verifier contract missing", result.stderr)

    def test_deliberate_fault_causality_regressions_are_rejected(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")

        runner.write_text(
            source.replace(
                "eviction_api_used = $null -ne $evictionReplacementProxyPod",
                "eviction_api_used = $true",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("evictionReplacementProxyPod", result.stderr)

        runner.write_text(
            source.replace(
                "stateless_pod_rescheduled = $null -ne $replacementProxyPod",
                "stateless_pod_rescheduled = $true",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("replacementProxyPod", result.stderr)

        runner.write_text(
            source.replace(
                "leader_changed = $leaderAfterOrdinal -ne $failureLeaderOrdinal",
                "leader_changed = $leaderAfter.Output -ne $failureLeaderBefore.Output",
                1,
            ),
            encoding="utf-8",
        )
        result = self.run_guard("--policy-only", expect_success=False)
        self.assertIn("leaderAfterOrdinal", result.stderr)

    def test_ha_rto_uses_only_live_cluster_events(self) -> None:
        runner = self.root / "scripts" / "kind-architecture-refactor-e2e.ps1"
        source = runner.read_text(encoding="utf-8")
        ha_start = source.index("$haBefore = Wait-LiveSingleMaster")
        ha_end = source.index("Complete-Scenario 'ha_replication_lag'", ha_start)
        ha_block = source[ha_start:ha_end]

        self.assertIn("Set-LivePodPortImpairment", ha_block)
        self.assertIn("-Port 10912", ha_block)
        self.assertIn("delete', 'pod', $haOldMaster.Pod", ha_block)
        self.assertIn("Wait-LiveSingleMaster", ha_block)
        self.assertNotIn("cargo", ha_block.lower())
        self.assertNotIn("Invoke-ModelTest", source)


if __name__ == "__main__":
    unittest.main()
