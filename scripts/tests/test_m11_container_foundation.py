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

import copy
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_guard():
    path = ROOT / "scripts" / "container_image_guard.py"
    spec = importlib.util.spec_from_file_location("container_image_guard", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load container_image_guard.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


GUARD = load_guard()


class ContainerFoundationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.policy = GUARD.load_policy()
        cls.dockerfile = (ROOT / cls.policy["foundation_dockerfile"]).read_text(encoding="utf-8")
        cls.workflow = (ROOT / cls.policy["workflow"]["path"]).read_text(encoding="utf-8")
        cls.supply_script = (ROOT / "scripts" / "container-supply-chain.ps1").read_text(encoding="utf-8")
        cls.service_script = (ROOT / cls.policy["service_contract_script"]).read_text(encoding="utf-8")
        cls.signal_sources = {
            name: (ROOT / path).read_text(encoding="utf-8")
            for name, path in cls.policy["signal_sources"].items()
        }
        cls.dockerfiles = GUARD.repository_dockerfiles()

    def audit(
        self,
        *,
        policy=None,
        dockerfile=None,
        workflow=None,
        supply_script=None,
        service_script=None,
        signal_sources=None,
        dockerfiles=None,
    ):
        return GUARD.audit_foundation(
            policy or self.policy,
            dockerfile if dockerfile is not None else self.dockerfile,
            workflow if workflow is not None else self.workflow,
            supply_script if supply_script is not None else self.supply_script,
            dockerfiles if dockerfiles is not None else self.dockerfiles,
            service_script if service_script is not None else self.service_script,
            signal_sources if signal_sources is not None else self.signal_sources,
        )

    def test_repository_foundation_contract_passes(self) -> None:
        self.assertEqual([], self.audit())
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "container_image_guard.py")],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("CONTAINER_IMAGE_GUARD_OK", result.stdout)

    def test_mutable_base_or_root_identity_is_rejected(self) -> None:
        mutable = self.dockerfile.replace(
            self.policy["base_images"]["runtime"]["reference"],
            "docker.io/library/debian:latest",
        )
        findings = self.audit(dockerfile=mutable)
        self.assertTrue(any("ARG RUNTIME_IMAGE=" in finding for finding in findings))
        self.assertTrue(any("mutable latest" in finding for finding in findings))

        root = self.dockerfile.replace("USER 10001:10001", "USER 0:0")
        self.assertTrue(any("USER 10001:10001" in finding for finding in self.audit(dockerfile=root)))

    def test_unpinned_action_or_weakened_scanner_is_rejected(self) -> None:
        checkout_sha = self.policy["workflow"]["actions"]["actions/checkout"]
        workflow = self.workflow.replace(
            f"actions/checkout@{checkout_sha}",
            "actions/checkout@v7",
        )
        self.assertTrue(any("actions/checkout" in finding for finding in self.audit(workflow=workflow)))

        weakened = self.supply_script.replace("--severity", "--ignore-unfixed --severity", 1)
        self.assertTrue(any("must not ignore unfixed" in finding for finding in self.audit(supply_script=weakened)))

    def test_unregistered_dockerfile_or_restored_legacy_exception_is_rejected(self) -> None:
        dockerfiles = set(self.dockerfiles)
        dockerfiles.add("docker/Dockerfile.unreviewed")
        self.assertTrue(any("unregistered Dockerfile" in finding for finding in self.audit(dockerfiles=dockerfiles)))

        policy = copy.deepcopy(self.policy)
        policy["compatibility_exceptions"].append(
            {
                "path": "docker/Dockerfile",
                "expires_at": "M11-12",
                "reason": "restore combined image",
            }
        )
        findings = self.audit(policy=policy, dockerfiles=dockerfiles | {"docker/Dockerfile"})
        self.assertTrue(any("retire all legacy" in finding for finding in findings))

    def test_missing_read_only_or_signature_verification_is_rejected(self) -> None:
        no_read_only = self.supply_script.replace("--read-only", "--read-write", 1)
        self.assertTrue(any("--read-only" in finding for finding in self.audit(supply_script=no_read_only)))

        no_verify = self.supply_script.replace("verify-blob", "verify-disabled", 1)
        self.assertTrue(any("verify-blob" in finding for finding in self.audit(supply_script=no_verify)))

    def test_weakened_policy_or_package_snapshot_is_rejected(self) -> None:
        policy = copy.deepcopy(self.policy)
        policy["supply_chain"]["critical_vulnerability_policy"]["maximum_findings"] = 1
        self.assertTrue(any("vulnerability policy" in finding for finding in self.audit(policy=policy)))

        policy = copy.deepcopy(self.policy)
        policy["supply_chain"]["signature"]["published_digest_pattern"] = ".*"
        self.assertTrue(any("GHCR service digest" in finding for finding in self.audit(policy=policy)))

        missing_package = self.dockerfile.replace("    libssl3", "    removed-libssl3", 1)
        self.assertTrue(any("libssl3" in finding for finding in self.audit(dockerfile=missing_package)))

        no_https_handoff = self.dockerfile.replace(
            "sed -i 's#URIs: http://#URIs: https://#'",
            "sed -i 's#URIs: http://#URIs: http://#'",
            1,
        )
        self.assertTrue(any("switch snapshot transport" in finding for finding in self.audit(dockerfile=no_https_handoff)))

    def test_missing_service_target_or_wrong_binary_owner_is_rejected(self) -> None:
        missing_target = self.dockerfile.replace("FROM service-runtime AS proxy", "FROM service-runtime AS removed-proxy")
        self.assertTrue(any("independent target: proxy" in finding for finding in self.audit(dockerfile=missing_target)))

        wrong_owner = self.dockerfile.replace(
            "/opt/rocketmq-binaries/rocketmq-controller-rust /usr/local/bin/rocketmq-controller-rust",
            "/opt/rocketmq-binaries/rocketmq-broker-rust /usr/local/bin/rocketmq-controller-rust",
            1,
        )
        self.assertTrue(any("controller image target missing" in finding for finding in self.audit(dockerfile=wrong_owner)))

    def test_shell_dispatch_or_secret_command_is_rejected(self) -> None:
        shell_entrypoint = self.dockerfile.replace(
            'ENTRYPOINT ["/usr/local/bin/rocketmq-broker-rust"]',
            'ENTRYPOINT ["/bin/sh", "-c", "rocketmq-start"]',
            1,
        )
        findings = self.audit(dockerfile=shell_entrypoint)
        self.assertTrue(any("direct binary entrypoint" in finding for finding in findings))

        secret_command = self.dockerfile.replace(
            'CMD ["--config", "/etc/rocketmq/mcp.toml", "--transport", "stdio"]',
            'CMD ["--config", "/etc/rocketmq/mcp.toml", "--token", "inline-secret"]',
            1,
        )
        findings = self.audit(dockerfile=secret_command)
        self.assertTrue(any("secret arguments" in finding for finding in findings))

    def test_ctrl_c_only_signal_or_weakened_service_smoke_is_rejected(self) -> None:
        signal_sources = dict(self.signal_sources)
        signal_sources["proxy"] = signal_sources["proxy"].replace(
            "wait_for_shutdown_signal().await",
            "tokio::signal::ctrl_c().await",
            1,
        )
        findings = self.audit(signal_sources=signal_sources)
        self.assertTrue(any("Ctrl-C-only" in finding for finding in findings))

        weakened = self.service_script.replace("docker stop --signal SIGTERM --timeout 30", "docker kill", 1)
        self.assertTrue(any("docker stop --signal SIGTERM" in finding for finding in self.audit(service_script=weakened)))

    def test_native_dash_c_arguments_are_preserved_by_real_script_helpers(self) -> None:
        powershell = shutil.which("pwsh") or shutil.which("powershell")
        self.assertIsNotNone(powershell, "PowerShell is required to validate container scripts")
        payload = "set -eu; test native-argument-preserved = native-argument-preserved"
        harness = r"""
param(
    [string]$SourcePath,
    [string]$FunctionName,
    [string]$PythonPath,
    [string]$OutputPath,
    [string]$Payload
)
$ErrorActionPreference = "Stop"
$tokens = $null
$parseErrors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile(
    $SourcePath,
    [ref]$tokens,
    [ref]$parseErrors
)
if ($parseErrors.Count -ne 0) {
    throw "failed to parse source helper: $($parseErrors[0].Message)"
}
$definition = $ast.Find({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -eq $FunctionName
}, $true)
if ($null -eq $definition) {
    throw "missing helper: $FunctionName"
}
Invoke-Expression $definition.Extent.Text
if ($FunctionName -eq "Invoke-Checked") {
    Invoke-Checked $PythonPath -c `
        'import json,pathlib,sys; pathlib.Path(sys.argv[2]).write_text(json.dumps(sys.argv[1:2]))' `
        $Payload $OutputPath
}
elseif ($FunctionName -eq "Invoke-Captured") {
    $captured = Invoke-Captured $PythonPath -c 'import sys; sys.stdout.write(sys.argv[1])' $Payload
    [System.IO.File]::WriteAllText($OutputPath, $captured)
}
else {
    throw "unsupported helper: $FunctionName"
}
"""
        cases = (
            ("container-supply-chain.ps1", "Invoke-Checked", [payload]),
            ("service-image-contract.ps1", "Invoke-Checked", [payload]),
            ("service-image-contract.ps1", "Invoke-Captured", payload),
        )
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory)
            harness_path = temporary / "native-argument-harness.ps1"
            harness_path.write_text(harness, encoding="utf-8")
            for index, (source_name, function_name, expected) in enumerate(cases):
                with self.subTest(source=source_name, helper=function_name):
                    output_path = temporary / f"result-{index}.json"
                    result = subprocess.run(
                        [
                            powershell,
                            "-NoProfile",
                            "-File",
                            str(harness_path),
                            str(ROOT / "scripts" / source_name),
                            function_name,
                            sys.executable,
                            str(output_path),
                            payload,
                        ],
                        cwd=ROOT,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                    self.assertEqual(0, result.returncode, result.stderr)
                    actual = (
                        json.loads(output_path.read_text(encoding="utf-8"))
                        if isinstance(expected, list)
                        else output_path.read_text(encoding="utf-8")
                    )
                    self.assertEqual(expected, actual)

    def test_native_runner_parameter_collision_is_rejected(self) -> None:
        supply_script = self.supply_script.replace("[string]$Executable", "[string]$Command", 1)
        findings = self.audit(supply_script=supply_script)
        self.assertTrue(any("reserve -c" in finding for finding in findings))

        service_script = self.service_script.replace("[string]$Executable", "[string]$Command", 1)
        findings = self.audit(service_script=service_script)
        self.assertTrue(any("reserve -c" in finding for finding in findings))


if __name__ == "__main__":
    unittest.main()
