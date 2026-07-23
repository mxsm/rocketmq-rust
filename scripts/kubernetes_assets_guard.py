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

"""Reject drift between the M11-10 Helm, Kustomize, lifecycle, and container contracts."""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PLACEHOLDER_DIGEST = "sha256:" + "0" * 64
FIXTURE_DIGESTS = {
    "broker": "sha256:" + "1" * 64,
    "namesrv": "sha256:" + "2" * 64,
    "controller": "sha256:" + "3" * 64,
    "proxy": "sha256:" + "4" * 64,
    "mcp": "sha256:" + "5" * 64,
}
CONTROLLER_SERVICE_IPS = ("10.96.0.201", "10.96.0.202", "10.96.0.203")
EXPECTED_SERVICES: dict[str, dict[str, Any]] = {
    "broker": {
        "kind": "StatefulSet",
        "replicas": 1,
        "ports": [8088, 10911, 10912],
        "config": "/etc/rocketmq/broker.toml",
        "data": "/var/lib/rocketmq/broker",
        "pdb": 1,
        "storage": "100Gi",
        "resources": ("1000m", "2Gi", "4000m", "8Gi"),
    },
    "namesrv": {
        "kind": "StatefulSet",
        "replicas": 3,
        "ports": [8088, 9876],
        "config": "/etc/rocketmq/namesrv.toml",
        "data": "/var/lib/rocketmq/namesrv",
        "pdb": 2,
        "storage": "2Gi",
        "resources": ("250m", "256Mi", "1000m", "1Gi"),
    },
    "controller": {
        "kind": "StatefulSet",
        "replicas": 3,
        "ports": [8088, 60109, 60110],
        "config": "/etc/rocketmq/controller.toml",
        "data": "/var/lib/rocketmq/controller",
        "pdb": 2,
        "storage": "10Gi",
        "resources": ("500m", "512Mi", "2000m", "2Gi"),
    },
    "proxy": {
        "kind": "Deployment",
        "replicas": 2,
        "ports": [8080, 8081, 8088],
        "config": "/etc/rocketmq/proxy.toml",
        "data": "/var/lib/rocketmq/proxy",
        "pdb": 1,
        "storage": None,
        "resources": ("500m", "512Mi", "2000m", "2Gi"),
    },
    "mcp": {
        "kind": "Deployment",
        "replicas": 1,
        "ports": [8088, 8089],
        "config": "/etc/rocketmq/mcp.toml",
        "data": "/var/lib/rocketmq/mcp",
        "pdb": 1,
        "storage": "10Gi",
        "resources": ("250m", "256Mi", "1000m", "1Gi"),
    },
}
EXPECTED_TOOLS = {
    "helm": (
        "v4.2.3",
        "e9b88b4ee95b18c706839c28d3a0220e5bc470e9cd9262410c90793c45ff8b7c",
        "5ca7de684c92d48b93d5c34a029fdda57b38e1eac04bc8541bdf1eb249388679",
    ),
    "kustomize": (
        "v5.8.1",
        "029a7f0f4e1932c52a0476cf02a0fd855c0bb85694b82c338fc648dcb53a819d",
        "8ec7f5e815e526d4622c06df0a7793d8cfb6eb1c74f816b46166097fef8b26c6",
    ),
    "kubeconform": (
        "v0.8.0",
        "9bc2bffbf71f261128533edaf912153948b7ff238f9a531ae6d34466ec287883",
        "e3f56102bcf4f50b034a567e2482a1c5330799983ddd655952310211aef73d93",
    ),
}
FORBIDDEN_PROBE_FIELDS = ("tcpSocket:", "startupProbe:")


@dataclass(frozen=True)
class Document:
    kind: str
    name: str
    text: str


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

    def load_json(self, relative: str) -> dict[str, Any]:
        text = self.read(relative)
        if not text:
            return {}
        try:
            value = json.loads(text)
        except json.JSONDecodeError as error:
            self.errors.append(f"invalid JSON in {relative}: {error}")
            return {}
        if not isinstance(value, dict):
            self.errors.append(f"{relative} must contain a JSON object")
            return {}
        return value


def split_documents(text: str) -> list[Document]:
    documents: list[Document] = []
    for raw in re.split(r"(?m)^---\s*$", text):
        kind_match = re.search(r"(?m)^kind:\s*([^\s#]+)", raw)
        if not kind_match:
            continue
        name_match = re.search(r"(?m)^  name:\s*([^\s#]+)", raw)
        if not name_match:
            continue
        documents.append(Document(kind_match.group(1), name_match.group(1).strip('"\''), raw))
    return documents


def find_document(documents: list[Document], kind: str, name: str) -> Document | None:
    matches = [document for document in documents if document.kind == kind and document.name == name]
    return matches[0] if len(matches) == 1 else None


def extract_literal_block(document: Document, key: str) -> str | None:
    lines = document.text.splitlines()
    marker = f"  {key}: |-"
    try:
        start = lines.index(marker) + 1
    except ValueError:
        return None
    body: list[str] = []
    for line in lines[start:]:
        if line and not line.startswith("    "):
            break
        body.append(line[4:] if line.startswith("    ") else "")
    return "\n".join(body).rstrip() + "\n"


def require_valid_toml(guard: Guard, label: str, document: Document, key: str) -> None:
    source = extract_literal_block(document, key)
    guard.require(source is not None, f"{label}: missing config literal {document.name}/{key}")
    if source is None:
        return
    try:
        tomllib.loads(source)
    except tomllib.TOMLDecodeError as error:
        guard.errors.append(f"{label}: invalid TOML in {document.name}/{key}: {error}")


def validate_exact_policy(guard: Guard, policy: dict[str, Any], container_policy: dict[str, Any]) -> None:
    guard.require(policy.get("schema_version") == 1, "deployment policy schema_version must remain 1")
    guard.require(policy.get("milestone") == "M11-10", "deployment policy milestone must remain M11-10")
    guard.require(policy.get("kubernetes_version") == "1.32.0", "Kubernetes schema baseline must remain 1.32.0")
    image = policy.get("image", {})
    guard.require(
        image.get("repository_prefix") == "ghcr.io/mxsm/rocketmq-rust",
        "deployment image repository prefix drifted",
    )
    guard.require(image.get("unpublished_placeholder") == PLACEHOLDER_DIGEST, "unpublished digest sentinel drifted")
    guard.require(
        image.get("secure_values_are_non_publishable_fixtures") is True,
        "secure rendering digest fixtures must remain explicitly non-publishable",
    )
    security = policy.get("security", {})
    expected_security = {
        "profile": "restricted",
        "uid": 10001,
        "gid": 10001,
        "read_only_rootfs": True,
        "allow_privilege_escalation": False,
        "seccomp_profile": "RuntimeDefault",
        "service_account_token": False,
        "secret_mount_path": "/var/run/secrets/rocketmq",
    }
    for key, expected in expected_security.items():
        guard.require(security.get(key) == expected, f"security policy {key} must remain {expected!r}")
    guard.require(security.get("drop_capabilities") == ["ALL"], "all Linux capabilities must be dropped")

    lifecycle = policy.get("lifecycle", {})
    expected_lifecycle = {
        "milestone": "M11-10",
        "health_port": 8088,
        "readiness_path": "/readyz",
        "liveness_path": "/livez",
        "pre_stop_path": "/drainz",
        "shutdown_timeout_seconds": 45,
        "termination_grace_period_seconds": 60,
        "liveness_stale_seconds": 30,
        "phase_order": [
            "reject_admission",
            "drain_sessions",
            "flush_replicate",
            "stop_background",
            "telemetry",
            "report",
        ],
        "tcp_probe_forbidden": True,
        "duplicate_shutdown_extends_deadline": False,
    }
    guard.require(lifecycle == expected_lifecycle, "service lifecycle policy drifted")
    guard.require(
        lifecycle.get("termination_grace_period_seconds", 0) > lifecycle.get("shutdown_timeout_seconds", 0),
        "Pod termination grace must exceed the internal shutdown deadline",
    )

    services = policy.get("services", {})
    container_services = container_policy.get("services", {})
    guard.require(set(services) == set(EXPECTED_SERVICES), "deployment policy must define exactly five services")
    guard.require(set(container_services) == set(EXPECTED_SERVICES), "container policy must define exactly five services")
    for service, expected in EXPECTED_SERVICES.items():
        actual = services.get(service, {})
        container = container_services.get(service, {})
        guard.require(actual.get("workload_kind") == expected["kind"], f"{service} workload kind drifted")
        guard.require(actual.get("replicas") == expected["replicas"], f"{service.capitalize()} replica contract drifted")
        guard.require(actual.get("ports") == expected["ports"], f"{service} deployment ports drifted")
        guard.require(actual.get("config_path") == expected["config"], f"{service} config path drifted")
        guard.require(actual.get("data_path") == expected["data"], f"{service} data path drifted")
        guard.require(actual.get("pdb", {}).get("min_available") == expected["pdb"], f"{service} PDB drifted")
        guard.require(
            actual.get("repository") == f"ghcr.io/mxsm/rocketmq-rust/{service}",
            f"{service} image repository drifted",
        )
        request_cpu, request_memory, limit_cpu, limit_memory = expected["resources"]
        resources = actual.get("resources", {})
        guard.require(
            resources.get("requests") == {"cpu": request_cpu, "memory": request_memory},
            f"{service} resource requests drifted",
        )
        guard.require(
            resources.get("limits") == {"cpu": limit_cpu, "memory": limit_memory},
            f"{service} resource limits drifted",
        )
        if expected["storage"] is None:
            guard.require(actual.get("persistence") == {"required": False}, f"{service} must remain stateless")
        else:
            persistence = actual.get("persistence", {})
            guard.require(persistence.get("required") is True, f"{service} persistence must remain required")
            guard.require(persistence.get("size") == expected["storage"], f"{service} storage budget drifted")
            guard.require(persistence.get("storage_class_required") is True, f"{service} storage class must be explicit")
            guard.require(persistence.get("retention") == "Retain", f"{service} PVC retention must remain Retain")
        guard.require(container.get("ports") == expected["ports"], f"{service} deployment/container port drift")
        guard.require(container.get("config_path") == expected["config"], f"{service} deployment/container config drift")
        guard.require(container.get("data_path") == expected["data"], f"{service} deployment/container data drift")
    guard.require(services.get("controller", {}).get("quorum") == 2, "Controller quorum must remain two of three")
    guard.require(
        services.get("controller", {}).get("raft_bind_address") == "0.0.0.0:60110",
        "Controller Raft bind address must remain separate from remoting",
    )
    guard.require(
        services.get("controller", {}).get("auto_initialize_cluster") is True,
        "Controller three-member bootstrap must remain explicit",
    )
    guard.require(
        services.get("controller", {}).get("peer_service_ips_required") == 3,
        "Controller must retain three stable ordinal Service IPs",
    )
    guard.require(
        services.get("controller", {}).get("peer_service_ip_fixture") == list(CONTROLLER_SERVICE_IPS),
        "Controller stable Service IP fixture drifted",
    )
    guard.require(
        services.get("mcp", {}).get("horizontal_scaling") is False,
        "MCP file-audit deployment must not claim horizontal scaling",
    )

    tools = policy.get("tools", {})
    for name, (version, linux_sha, windows_sha) in EXPECTED_TOOLS.items():
        actual = tools.get(name, {})
        guard.require(actual.get("version") == version, f"{name} version drifted")
        guard.require(actual.get("linux_amd64_sha256") == linux_sha, f"{name} Linux checksum drifted")
        guard.require(actual.get("windows_amd64_sha256") == windows_sha, f"{name} Windows checksum drifted")
    guard.require(
        policy.get("workflow")
        == {
            "path": ".github/workflows/kubernetes-assets-ci.yml",
            "actions": {
                "actions/checkout": "9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
                "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
            },
        },
        "Kubernetes asset workflow/action pin contract drifted",
    )


def validate_source_assets(guard: Guard) -> None:
    policy = guard.load_json("distribution/kubernetes/deployment-policy.json")
    container_policy = guard.load_json("docker/container-policy.json")
    validate_exact_policy(guard, policy, container_policy)

    required = [
        "distribution/helm/rocketmq-rust/Chart.yaml",
        "distribution/helm/rocketmq-rust/values.yaml",
        "distribution/helm/rocketmq-rust/values.schema.json",
        "distribution/helm/rocketmq-rust/ci/secure-values.yaml",
        "distribution/helm/rocketmq-rust/templates/_helpers.tpl",
        "distribution/helm/rocketmq-rust/templates/configmaps.yaml",
        "distribution/helm/rocketmq-rust/templates/workloads.yaml",
        "distribution/helm/rocketmq-rust/templates/networkpolicies.yaml",
        "distribution/kubernetes/base/kustomization.yaml",
        "distribution/kubernetes/base/manifest.yaml",
        "distribution/kubernetes/overlays/secure/kustomization.yaml",
        "distribution/kubernetes/README.md",
        "scripts/kubernetes-assets-contract.ps1",
        ".github/workflows/kubernetes-assets-ci.yml",
    ]
    for relative in required:
        guard.read(relative)

    contract_script = guard.read("scripts/kubernetes-assets-contract.ps1")
    guard.require("$hostIsWindows" in contract_script, "asset contract must use a script-owned host platform flag")
    guard.require(
        re.search(r"(?i)\$iswindows\b", contract_script) is None,
        "asset contract must not assign or reference reserved $IsWindows",
    )

    chart_sources = "\n".join(
        guard.read(f"distribution/helm/rocketmq-rust/templates/{name}")
        for name in (
            "_helpers.tpl",
            "namespace.yaml",
            "serviceaccount.yaml",
            "configmaps.yaml",
            "services.yaml",
            "workloads.yaml",
            "pdbs.yaml",
            "networkpolicies.yaml",
        )
    )
    all_yaml_sources = chart_sources + "\n" + guard.read("distribution/kubernetes/base/manifest.yaml")
    for field in FORBIDDEN_PROBE_FIELDS:
        guard.require(field not in all_yaml_sources, f"forbidden lifecycle probe field found: {field}")
    for forbidden in ("kind: Secret", "stringData:", "hostPath:", "privileged: true", "allowPrivilegeEscalation: true"):
        guard.require(forbidden not in all_yaml_sources, f"forbidden deployment source found: {forbidden}")
    for mutable in (":latest", ":main", ":master"):
        guard.require(mutable not in all_yaml_sources, f"mutable image tag found: {mutable}")
    guard.require(chart_sources.count("readOnlyRootFilesystem: true") >= 1, "chart must require a read-only root filesystem")
    guard.require("drop:\n    - ALL" in chart_sources, "chart must drop all capabilities")
    guard.require("automountServiceAccountToken: false" in chart_sources, "chart must disable service-account token mounts")
    guard.require("pod-security.kubernetes.io/enforce: restricted" in chart_sources, "restricted Pod Security labels missing")
    guard.require("rocketmq-default-deny" in chart_sources, "default-deny NetworkPolicy missing")
    guard.require("secretProviderClass" in chart_sources and "existingSecret" in chart_sources, "external secret references missing")
    guard.require("helm.sh/resource-policy: keep" in chart_sources, "MCP audit PVC must be retained on Helm rollback")
    guard.require("Controller peer Service IP is an unpublished documentation sentinel" in chart_sources, "Controller Service IP sentinel must fail closed")

    values = guard.read("distribution/helm/rocketmq-rust/values.yaml")
    secure_values = guard.read("distribution/helm/rocketmq-rust/ci/secure-values.yaml")
    guard.require(values.count(PLACEHOLDER_DIGEST) == 5, "default Helm values must contain five fail-closed digest sentinels")
    guard.require(secure_values.count("non-publishable") == 1, "secure Helm fixtures must be labeled non-publishable")
    for service, digest in FIXTURE_DIGESTS.items():
        guard.require(digest in secure_values, f"secure Helm fixture digest missing for {service}")
    for address in CONTROLLER_SERVICE_IPS:
        guard.require(address in secure_values, f"secure Helm Controller Service IP fixture missing: {address}")

    schema = guard.load_json("distribution/helm/rocketmq-rust/values.schema.json")
    guard.require(schema.get("additionalProperties") is False, "Helm values schema must reject unknown top-level fields")
    services_schema = schema.get("properties", {}).get("services", {})
    guard.require(services_schema.get("additionalProperties") is False, "Helm service schema must reject unknown services")
    guard.require(set(services_schema.get("required", [])) == set(EXPECTED_SERVICES), "Helm schema must require five services")

    base_kustomization = guard.read("distribution/kubernetes/base/kustomization.yaml")
    secure_overlay = guard.read("distribution/kubernetes/overlays/secure/kustomization.yaml")
    guard.require(base_kustomization.count(PLACEHOLDER_DIGEST) == 5, "Kustomize base must fail closed with five sentinels")
    guard.require(secure_overlay.count("non-publishable") == 1, "secure Kustomize fixture must be labeled non-publishable")
    for service, digest in FIXTURE_DIGESTS.items():
        guard.require(digest in secure_overlay, f"secure Kustomize fixture digest missing for {service}")

    generated = guard.read("distribution/kubernetes/base/manifest.yaml")
    validate_render(guard, "generated Kustomize base", generated, FIXTURE_DIGESTS)

    workflow = guard.read(".github/workflows/kubernetes-assets-ci.yml")
    for pin in (
        "actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0",
        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    ):
        guard.require(pin in workflow, f"workflow action pin missing: {pin}")
    guard.require("permissions:\n  contents: read" in workflow, "workflow must retain contents:read least privilege")
    guard.require("kubernetes-assets-contract.ps1" in workflow, "workflow must execute the full asset contract")
    guard.require("test_m11_kubernetes_assets" in workflow, "workflow must execute deliberate-violation tests")
    guard.require("test_m11_service_lifecycle" in workflow, "workflow must execute lifecycle violation tests")


def validate_workload(guard: Guard, label: str, document: Document, service: str, expected: dict[str, Any], digest: str) -> None:
    text = document.text
    guard.require(re.search(rf"(?m)^  replicas:\s*{expected['replicas']}\s*$", text) is not None, f"{label}: {service} replicas drifted")
    expected_image = f"ghcr.io/mxsm/rocketmq-rust/{service}@{digest}"
    guard.require(expected_image in text, f"{label}: {service} immutable image missing")
    guard.require(expected["config"] in text, f"{label}: {service} config mount drifted")
    guard.require(expected["data"] in text, f"{label}: {service} data mount drifted")
    for port in expected["ports"]:
        guard.require(
            re.search(rf"containerPort:\s*{port}(?:\D|$)", text) is not None,
            f"{label}: {service} container port {port} missing",
        )
    request_cpu, request_memory, limit_cpu, limit_memory = expected["resources"]
    for quantity in (request_cpu, request_memory, limit_cpu, limit_memory):
        guard.require(quantity in text, f"{label}: {service} resource quantity {quantity} missing")
    for snippet in (
        "automountServiceAccountToken: false",
        "runAsNonRoot: true",
        "readOnlyRootFilesystem: true",
        "allowPrivilegeEscalation: false",
        "type: RuntimeDefault",
        "- ALL",
        "mountPath: /var/run/secrets/rocketmq",
        "OTEL_EXPORTER_OTLP_ENDPOINT",
        "topologyKey: kubernetes.io/hostname",
    ):
        guard.require(snippet in text, f"{label}: {service} missing security/topology contract {snippet}")
    for field in FORBIDDEN_PROBE_FIELDS:
        guard.require(field not in text, f"{label}: {service} contains forbidden probe field {field}")
    for snippet in (
        "terminationGracePeriodSeconds: 60",
        "name: ROCKETMQ_HEALTH_BIND_ADDR",
        "name: ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS",
        "name: ROCKETMQ_LIVENESS_STALE_SECONDS",
        "name: health",
        "containerPort: 8088",
        "lifecycle:",
        "preStop:",
        "path: /drainz",
        "readinessProbe:",
        "path: /readyz",
        "livenessProbe:",
        "path: /livez",
    ):
        guard.require(snippet in text, f"{label}: {service} missing lifecycle contract {snippet}")
    for value in ("0.0.0.0:8088", "45", "30"):
        guard.require(
            re.search(rf"value:\s*[\"']?{re.escape(value)}[\"']?(?:[,}}\s]|$)", text) is not None,
            f"{label}: {service} missing lifecycle environment value {value}",
        )
    guard.require(text.count("port: health") == 3, f"{label}: {service} probes must use the named health port")
    guard.require(text.count("scheme: HTTP") == 3, f"{label}: {service} probes must use HTTP")
    guard.require(text.count("path: /drainz") == 1, f"{label}: {service} must expose one idempotent preStop hook")
    guard.require(text.count("path: /readyz") == 1, f"{label}: {service} must expose one readiness probe")
    guard.require(text.count("path: /livez") == 1, f"{label}: {service} must expose one liveness probe")
    if expected["kind"] == "StatefulSet":
        guard.require("volumeClaimTemplates:" in text, f"{label}: {service} StatefulSet lacks PVC template")
        guard.require("storageClassName:" in text, f"{label}: {service} lacks explicit storage class")
        guard.require("whenDeleted: Retain" in text and "whenScaled: Retain" in text, f"{label}: {service} PVC retention drifted")
        guard.require(f"storage: {expected['storage']}" in text, f"{label}: {service} storage budget drifted")
    elif service == "proxy":
        guard.require("emptyDir:" in text, f"{label}: Proxy must use ephemeral bounded data")
        guard.require("volumeClaimTemplates:" not in text, f"{label}: Proxy must remain stateless")
    elif service == "mcp":
        guard.require("replicas: 1" in text and "type: Recreate" in text, f"{label}: MCP audit deployment must be singleton/Recreate")
    if service == "controller":
        guard.require("topologyKey: topology.kubernetes.io/zone" in text, f"{label}: Controller zone spread missing")
        guard.require("subPathExpr: $(POD_NAME).toml" in text, f"{label}: Controller ordinal config selection missing")
        guard.require(
            "ROCKETMQ_CONTROLLER_RAFT_BIND_ADDR" in text and "0.0.0.0:60110" in text,
            f"{label}: Controller separate Raft bind address missing",
        )
        guard.require(
            "ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER" in text and 'value: "true"' in text,
            f"{label}: Controller explicit cluster bootstrap missing",
        )


def validate_render(guard: Guard, label: str, text: str, expected_digests: dict[str, str] | None = None) -> dict[str, Any]:
    documents = split_documents(text)
    guard.require(len(documents) == 37, f"{label}: expected 37 Kubernetes resources, found {len(documents)}")
    for forbidden in ("kind: Secret", "stringData:", "hostPath:", "privileged: true", "allowPrivilegeEscalation: true"):
        guard.require(forbidden not in text, f"{label}: forbidden field {forbidden}")
    for field in FORBIDDEN_PROBE_FIELDS:
        guard.require(field not in text, f"{label}: forbidden lifecycle probe field {field}")

    image_matches = re.findall(
        r"(?m)^\s*image:\s*[\"']?(ghcr\.io/mxsm/rocketmq-rust/(broker|namesrv|controller|proxy|mcp)@"
        r"(sha256:[0-9a-f]{64}))[\"']?\s*$",
        text,
    )
    guard.require(len(image_matches) == 5, f"{label}: expected exactly five digest-only workload images")
    observed_digests = {service: digest for _, service, digest in image_matches}
    guard.require(set(observed_digests) == set(EXPECTED_SERVICES), f"{label}: workload image owners drifted")
    guard.require(PLACEHOLDER_DIGEST not in observed_digests.values(), f"{label}: unpublished placeholder digest rendered")
    if expected_digests is not None:
        guard.require(observed_digests == expected_digests, f"{label}: secure fixture digests drifted")

    summary: dict[str, Any] = {}
    for service, expected in EXPECTED_SERVICES.items():
        document = find_document(documents, expected["kind"], f"rocketmq-{service}")
        guard.require(document is not None, f"{label}: missing unique {expected['kind']} rocketmq-{service}")
        if document is None:
            continue
        digest = observed_digests.get(service, "")
        validate_workload(guard, label, document, service, expected, digest)
        pdb = find_document(documents, "PodDisruptionBudget", f"rocketmq-{service}")
        guard.require(pdb is not None, f"{label}: {service} PDB missing")
        if pdb is not None:
            guard.require(
                re.search(rf"(?m)^  minAvailable:\s*{expected['pdb']}\s*$", pdb.text) is not None,
                f"{label}: {service} PDB quorum drifted",
            )
        summary[service] = (expected["kind"], expected["replicas"], digest, expected["pdb"])

    namespace = find_document(documents, "Namespace", "rocketmq")
    guard.require(namespace is not None, f"{label}: managed restricted Namespace missing")
    if namespace is not None:
        guard.require("pod-security.kubernetes.io/enforce: restricted" in namespace.text, f"{label}: restricted PSA missing")
    service_account = find_document(documents, "ServiceAccount", "rocketmq-runtime")
    guard.require(service_account is not None, f"{label}: runtime ServiceAccount missing")
    if service_account is not None:
        guard.require("automountServiceAccountToken: false" in service_account.text, f"{label}: ServiceAccount token enabled")

    for service_document in (document for document in documents if document.kind == "Service"):
        guard.require("name: health" not in service_document.text, f"{label}: health port must remain kubelet-only")
        guard.require(
            re.search(r"(?m)^\s+(?:port|targetPort):\s*8088\s*$", service_document.text) is None,
            f"{label}: health port 8088 must not be exposed through a Service",
        )

    controller_config = find_document(documents, "ConfigMap", "rocketmq-controller-config")
    guard.require(controller_config is not None, f"{label}: Controller config map missing")
    if controller_config is not None:
        for node_id in (1, 2, 3):
            guard.require(f"nodeId = {node_id}" in controller_config.text, f"{label}: Controller nodeId {node_id} missing")
        guard.require(controller_config.text.count("[[raftPeers]]") == 9, f"{label}: Controller must render three Raft peers per ordinal")
        guard.require(
            controller_config.text.count("[[controllerPeers]]") == 9,
            f"{label}: Controller must render three remoting peers per ordinal",
        )
        for ordinal, address in enumerate(CONTROLLER_SERVICE_IPS):
            guard.require(
                controller_config.text.count(f'{address}:60110') == 3,
                f"{label}: Controller Raft peer {ordinal} membership drifted",
            )
            guard.require(
                controller_config.text.count(f'{address}:60109') == 3,
                f"{label}: Controller remoting peer {ordinal} membership drifted",
            )
            peer_service = find_document(documents, "Service", f"rocketmq-controller-{ordinal}")
            guard.require(peer_service is not None, f"{label}: Controller ordinal Service {ordinal} missing")
            if peer_service is not None:
                guard.require(f"clusterIP: {address}" in peer_service.text, f"{label}: Controller Service IP {ordinal} drifted")
                guard.require("statefulset.kubernetes.io/pod-name:" in peer_service.text, f"{label}: Controller Service selector drifted")
            require_valid_toml(guard, label, controller_config, f"controller-{ordinal}.toml")

    for service in ("broker", "namesrv", "proxy"):
        config = find_document(documents, "ConfigMap", f"rocketmq-{service}-config")
        guard.require(config is not None, f"{label}: {service} config map missing")
        if config is not None:
            require_valid_toml(guard, label, config, f"{service}.toml")

    mcp_config = find_document(documents, "ConfigMap", "rocketmq-mcp-config")
    guard.require(mcp_config is not None, f"{label}: MCP secure config missing")
    if mcp_config is not None:
        require_valid_toml(guard, label, mcp_config, "mcp.toml")
        require_valid_toml(guard, label, mcp_config, "permissions.toml")
        for snippet in (
            'transport = "streamable-http"',
            'mode = "oauth-jwt"',
            'jwt_algorithm = "rs256"',
            'sink = "file"',
            'path = "/var/lib/rocketmq/mcp/audit/rocketmq-mcp-audit.log"',
            'cert_path = "/var/run/secrets/rocketmq/tls.crt"',
            'key_path = "/var/run/secrets/rocketmq/tls.key"',
            "[cache]",
            "[diagnosis]",
            "[roles.read_only]",
            "[roles.diagnose]",
        ):
            guard.require(snippet in mcp_config.text, f"{label}: MCP secure contract missing {snippet}")

    mcp_pvc = find_document(documents, "PersistentVolumeClaim", "rocketmq-mcp-audit")
    guard.require(mcp_pvc is not None, f"{label}: MCP audit PVC missing")
    if mcp_pvc is not None:
        guard.require(
            re.search(r"storageClassName:\s*[\"']?rocketmq-retain[\"']?", mcp_pvc.text) is not None,
            f"{label}: MCP storage class drifted",
        )
        guard.require("storage: 10Gi" in mcp_pvc.text, f"{label}: MCP audit storage budget drifted")
        guard.require("persisted-format: mcp-audit-v1" in mcp_pvc.text, f"{label}: MCP audit format marker missing")

    network_policy_names = {document.name for document in documents if document.kind == "NetworkPolicy"}
    expected_network_policies = {
        "rocketmq-default-deny",
        "rocketmq-intra-cluster",
        "rocketmq-dns-egress",
        "rocketmq-otel-egress",
        "rocketmq-broker-client-ingress",
        "rocketmq-proxy-client-ingress",
        "rocketmq-mcp-client-ingress",
        "rocketmq-mcp-jwks-egress",
    }
    guard.require(network_policy_names == expected_network_policies, f"{label}: NetworkPolicy set drifted")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--helm-rendered", type=Path)
    parser.add_argument("--kustomize-rendered", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = args.root.resolve()
    guard = Guard(root)
    validate_source_assets(guard)
    if (args.helm_rendered is None) != (args.kustomize_rendered is None):
        guard.errors.append("--helm-rendered and --kustomize-rendered must be provided together")
    elif args.helm_rendered is not None and args.kustomize_rendered is not None:
        helm_summary = validate_render(
            guard,
            "Helm render",
            args.helm_rendered.read_text(encoding="utf-8"),
            FIXTURE_DIGESTS,
        )
        kustomize_summary = validate_render(
            guard,
            "Kustomize render",
            args.kustomize_rendered.read_text(encoding="utf-8"),
            FIXTURE_DIGESTS,
        )
        guard.require(helm_summary == kustomize_summary, "Helm and Kustomize service contracts diverged")

    if guard.errors:
        for error in guard.errors:
            print(f"KUBERNETES_ASSETS_GUARD_ERROR: {error}", file=sys.stderr)
        return 1
    print("KUBERNETES_ASSETS_GUARD_OK services=5 helm=1 kustomize=1 milestone=M11-10")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
