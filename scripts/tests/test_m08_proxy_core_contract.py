# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import ROOT
from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import run_dependency_guard
from scripts.tests.architecture_contract_helpers import workspace_packages


class ProxyCoreContractTests(unittest.TestCase):
    def test_workspace_has_current_proxy_packages(self) -> None:
        packages = workspace_packages()
        self.assertTrue(
            {"rocketmq-proxy", "rocketmq-proxy-core", "rocketmq-proxy-cluster", "rocketmq-proxy-local"}
            <= set(packages)
        )

    def test_core_cluster_and_local_dependency_directions_are_separated(self) -> None:
        core = normal_dependencies("rocketmq-proxy-core/Cargo.toml")
        cluster = normal_dependencies("rocketmq-proxy-cluster/Cargo.toml")
        local = normal_dependencies("rocketmq-proxy-local/Cargo.toml")

        self.assertTrue(
            {"rocketmq-client-rust", "rocketmq-broker", "rocketmq-store"}.isdisjoint(core)
        )
        self.assertIn("rocketmq-client-rust", cluster)
        self.assertNotIn("rocketmq-broker", cluster)
        self.assertIn("rocketmq-broker", local)
        self.assertNotIn("rocketmq-client-rust", local)

    def test_proxy_ingress_and_dependency_target_are_executable(self) -> None:
        self.assertTrue((ROOT / "rocketmq-proxy/tests/grpc_ingress.rs").is_file())
        self.assertTrue((ROOT / "rocketmq-proxy/tests/remoting_ingress.rs").is_file())
        result = run_dependency_guard("target")
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_cluster_execution_uses_exact_managed_keys_and_reserved_control_capacity(self) -> None:
        admission = (
            ROOT / "rocketmq-proxy-cluster/src/cluster_admission.rs"
        ).read_text(encoding="utf-8")
        execution = (
            ROOT / "rocketmq-proxy-cluster/src/cluster_execution.rs"
        ).read_text(encoding="utf-8")
        config = (ROOT / "rocketmq-proxy-cluster/src/config.rs").read_text(
            encoding="utf-8"
        )

        for token in (
            "HashMap<ClusterOrderingKey, RegisteredLane>",
            "with_control_reserve",
            "data_inflight",
            "wait_for_lane_tasks",
            "generation",
        ):
            self.assertIn(token, admission)
        for token in (
            'spawn_service("proxy.cluster.keyed-lane"',
            "CommandCancellationGuard",
            "LaneTaskGuard",
            "cluster_shutdown_error",
        ):
            self.assertIn(token, execution)
        for field in (
            "command_queue_capacity",
            "command_queue_max_bytes",
            "command_queue_max_age_ms",
            "io_max_inflight",
            "control_reserve",
            "execution_lane_idle_timeout_ms",
        ):
            self.assertIn(field, config)

        combined = admission + execution
        self.assertNotIn("CLUSTER_EXECUTION_LANE_COUNT", combined)
        self.assertNotIn("fn cluster_lane(", combined)
        self.assertTrue(
            (ROOT / "rocketmq-doc/en/proxy-cluster-keyed-execution-adr.md").is_file()
        )


if __name__ == "__main__":
    unittest.main()
