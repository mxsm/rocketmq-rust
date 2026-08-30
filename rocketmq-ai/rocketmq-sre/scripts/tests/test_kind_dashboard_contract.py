# Copyright 2023 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from pathlib import Path
import unittest


REPOSITORY_ROOT = Path(__file__).resolve().parents[4]


class KindDashboardContractTest(unittest.TestCase):
    def test_kind_runner_builds_loads_and_waits_for_dashboard(self) -> None:
        runner = (REPOSITORY_ROOT / "rocketmq-ai/rocketmq-sre/scripts/kind.ps1").read_text(encoding="utf-8")

        for required in (
            "rocketmq-rust/dashboard-backend:phase00-local",
            "rocketmq-rust/dashboard-frontend:phase00-local",
            "deploy/backend.Dockerfile",
            "deploy/frontend.Dockerfile",
            "deployment/rocketmq-dashboard-backend",
            "deployment/rocketmq-dashboard-frontend",
            "Restart-Workloads $dashboardNamespace",
            "Start-PortForwarders",
            "Service = 'rocketmq-namesrv'; LocalPort = 9876; RemotePort = 9876",
            "Service = 'rocketmq-broker'; LocalPort = 10911; RemotePort = 10911",
            "Service = 'rocketmq-proxy'; LocalPort = 8080; RemotePort = 8080",
            "Service = 'rocketmq-proxy'; LocalPort = 8081; RemotePort = 8081",
            "http://127.0.0.1:3003",
            "127.0.0.1:9876",
            "127.0.0.1:8081",
        ):
            self.assertIn(required, runner)

        supervisor = (REPOSITORY_ROOT / "rocketmq-ai/rocketmq-sre/scripts/kind-port-forward.ps1").read_text(
            encoding="utf-8"
        )
        self.assertIn("while (Test-Path", supervisor)
        self.assertIn("port-forward", supervisor)

    def test_kind_manifest_exposes_authenticated_dashboard_proxy(self) -> None:
        kustomization = (REPOSITORY_ROOT / "rocketmq-ai/rocketmq-sre/deploy/kind/kustomization.yaml").read_text(
            encoding="utf-8"
        )
        manifest = (REPOSITORY_ROOT / "rocketmq-ai/rocketmq-sre/deploy/kind/dashboard-stack.yaml").read_text(
            encoding="utf-8"
        )
        nginx = (REPOSITORY_ROOT / "rocketmq-dashboard/rocketmq-dashboard-web/deploy/nginx.conf").read_text(
            encoding="utf-8"
        )

        self.assertIn("dashboard-stack.yaml", kustomization)
        self.assertIn("DASHBOARD_WEB_LOGIN_REQUIRED", manifest)
        self.assertIn("DASHBOARD_WEB_ROCKETMQ_ACCESS_KEY", manifest)
        self.assertIn("rocketmq-namesrv.rocketmq-system.svc.cluster.local:9876", manifest)
        self.assertIn("proxy_pass http://rocketmq-dashboard-backend:8082", nginx)

    def test_kind_smoke_checks_dashboard_cluster_data(self) -> None:
        smoke = (REPOSITORY_ROOT / "rocketmq-ai/rocketmq-sre/deploy/kind/smoke-job.yaml").read_text(encoding="utf-8")

        self.assertIn("rocketmq-dashboard.rocketmq-dashboard.svc.cluster.local:3003", smoke)
        self.assertIn("/api/dashboard/overview", smoke)
        self.assertIn("systemStatus", smoke)


if __name__ == "__main__":
    unittest.main()
