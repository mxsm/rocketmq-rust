# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import unittest

from scripts.tests.architecture_contract_helpers import normal_dependencies
from scripts.tests.architecture_contract_helpers import source


class DashboardClientEdgeContractTest(unittest.TestCase):
    def test_dashboards_depend_on_admin_core_not_client(self) -> None:
        for manifest in (
            "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/Cargo.toml",
            "rocketmq-dashboard/rocketmq-dashboard-web/backend/Cargo.toml",
        ):
            dependencies = normal_dependencies(manifest)
            self.assertIn("rocketmq-admin-core", dependencies, manifest)
            self.assertNotIn("rocketmq-client-rust", dependencies, manifest)

    def test_dashboard_runtime_roots_use_fallible_client_construction(self) -> None:
        for relative in (
            "rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/src/lib.rs",
            "rocketmq-dashboard/rocketmq-dashboard-web/backend/src/main.rs",
        ):
            entrypoint = source(relative)
            self.assertIn("RuntimeOwner", entrypoint, relative)
            self.assertIn("ClientRuntime::try_new", entrypoint, relative)
            self.assertNotIn("ClientRuntime::new(", entrypoint, relative)


if __name__ == "__main__":
    unittest.main()
