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

from __future__ import annotations

import base64
import re
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def parse_secret_data(path: Path) -> dict[str, str]:
    data: dict[str, str] = {}
    in_data = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "data:":
            in_data = True
            continue
        if in_data:
            match = re.fullmatch(r"  ([A-Za-z0-9_.-]+): ([A-Za-z0-9+/=]+)", line)
            if not match:
                raise AssertionError(f"invalid generated Secret data line: {line}")
            data[match.group(1)] = base64.b64decode(match.group(2), validate=True).decode("utf-8")
    return data


class DynamicEvidenceInputTests(unittest.TestCase):
    def test_generator_creates_distinct_run_scoped_acl_material_without_logging_it(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            powershell = shutil.which("pwsh") or shutil.which("powershell")
            self.assertIsNotNone(powershell, "PowerShell is required for the evidence generator test")
            completed = subprocess.run(
                [
                    str(powershell),
                    "-NoProfile",
                    "-File",
                    str(ROOT / "scripts" / "new-m11-evidence-secrets.ps1"),
                    "-Mode",
                    "Generate",
                    "-OutputDirectory",
                    directory,
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertEqual("M11_EPHEMERAL_SECRET_MANIFESTS_OK files=4", completed.stdout.strip())

            output = Path(directory)
            runtime = parse_secret_data(output / "runtime-secret.yaml")
            rotated_runtime = parse_secret_data(output / "rotated-runtime-secret.yaml")
            baseline_driver = parse_secret_data(output / "baseline-driver-secret.yaml")
            rotated_driver = parse_secret_data(output / "rotated-driver-secret.yaml")

            self.assertEqual(
                {
                    "admin.identity",
                    "broker-acl.yml",
                    "ca.crt",
                    "controller-acl.yml",
                    "proxy-acl.yml",
                    "request-policy.json",
                    "tls.crt",
                    "tls.key",
                },
                set(runtime),
            )
            self.assertEqual(set(runtime), set(rotated_runtime))
            self.assertNotEqual(runtime["broker-acl.yml"], rotated_runtime["broker-acl.yml"])
            self.assertNotEqual(runtime["controller-acl.yml"], rotated_runtime["controller-acl.yml"])
            self.assertTrue(runtime["tls.crt"].startswith("-----BEGIN CERTIFICATE-----"))
            self.assertTrue(runtime["tls.key"].startswith("-----BEGIN RSA PRIVATE KEY-----"))
            self.assertEqual(runtime["tls.crt"], rotated_runtime["tls.crt"])
            self.assertEqual(runtime["tls.key"], rotated_runtime["tls.key"])
            self.assertIn(baseline_driver["ROCKETMQ_ACL_ACCESS_KEY"], runtime["broker-acl.yml"])
            self.assertIn(baseline_driver["ROCKETMQ_ACL_SECRET_KEY"], runtime["broker-acl.yml"])
            self.assertIn(baseline_driver["ROCKETMQ_ACL_ACCESS_KEY"], runtime["controller-acl.yml"])
            self.assertIn(baseline_driver["ROCKETMQ_ACL_SECRET_KEY"], runtime["controller-acl.yml"])
            self.assertIn(rotated_driver["ROCKETMQ_ACL_ACCESS_KEY"], rotated_runtime["broker-acl.yml"])
            self.assertIn(rotated_driver["ROCKETMQ_ACL_SECRET_KEY"], rotated_runtime["broker-acl.yml"])
            self.assertIn(rotated_driver["ROCKETMQ_ACL_ACCESS_KEY"], rotated_runtime["controller-acl.yml"])
            self.assertIn(rotated_driver["ROCKETMQ_ACL_SECRET_KEY"], rotated_runtime["controller-acl.yml"])
            for value in (*baseline_driver.values(), *rotated_driver.values()):
                self.assertNotIn(value, completed.stdout)

    def test_workflows_keep_untrusted_events_off_the_long_runner(self) -> None:
        slo = (ROOT / ".github" / "workflows" / "architecture-slo-evidence.yml").read_text(encoding="utf-8")
        fault = (ROOT / ".github" / "workflows" / "kubernetes-fault-matrix.yml").read_text(encoding="utf-8")

        self.assertIn("runs-on: [self-hosted, linux, x64, rocketmq-architecture-evidence]", slo)
        self.assertIn("github.ref == 'refs/heads/main'", slo)
        self.assertIn("packages: read", slo)
        self.assertIn("packages: read", fault)
        self.assertNotIn("M11_RUNTIME_SECRET_MANIFEST_B64", slo + fault)
        self.assertNotIn("M11_ROTATED_RUNTIME_SECRET_MANIFEST_B64", slo + fault)
        self.assertIn("new-m11-evidence-secrets.ps1", slo)
        self.assertIn("new-m11-evidence-secrets.ps1", fault)
        self.assertIn("run-architecture-slo-cluster.ps1", slo)

    def test_cluster_wrapper_has_digest_prometheus_and_sustained_probe(self) -> None:
        wrapper = (ROOT / "scripts" / "run-architecture-slo-cluster.ps1").read_text(encoding="utf-8")
        self.assertIn("PrometheusImage must be pinned by digest", wrapper)
        self.assertIn("rocketmq-slo-message-probe", wrapper)
        self.assertIn("while true; do", wrapper)
        self.assertIn("message sendMessage", wrapper)
        self.assertIn("message consumeMessage", wrapper)
        self.assertIn("Stop-Process", wrapper)


if __name__ == "__main__":
    unittest.main()
