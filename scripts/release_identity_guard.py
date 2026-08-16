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

import argparse
from datetime import date
import json
from pathlib import Path
import re
import tomllib
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_IDENTITY = ROOT / "distribution" / "release-identity.json"
DEFAULT_SCHEMA = ROOT / "distribution" / "release-identity.schema.json"

IDENTITY_KINDS = {"unofficial-community", "apache-governance"}
REQUIRED_CONSUMERS = {
    "crate-package-planner",
    "binary-archive-builder",
    "oci-layout-builder",
    "helm-candidate-builder",
    "legal-sbom-provenance",
    "public-staged-metadata",
}
OBJECT_FIELDS = {
    "root": {
        "schema_version",
        "revision",
        "identity_kind",
        "distribution_name",
        "official_apache_release",
        "project",
        "crate_registry",
        "oci",
        "helm",
        "legal",
        "approval",
        "required_consumers",
    },
    "project": {"name", "repository", "homepage"},
    "crate_registry": {"registry", "owner", "namespace", "package_prefix"},
    "oci": {"registry", "namespace"},
    "helm": {"chart_name", "annotations"},
    "helm.annotations": {
        "rocketmqrust.com/distribution-owner",
        "rocketmqrust.com/repository",
        "rocketmqrust.com/official-apache-release",
    },
    "legal": {"license", "notice_owner", "upstream_owner", "disclaimer"},
    "approval": {
        "approver",
        "approved_revision",
        "approved_on",
        "effective_scope",
        "decision_source",
    },
}


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return value


def _path_text(path: tuple[str, ...]) -> str:
    return ".".join(path) if path else "root"


def _is_forbidden_identity_key(key: str) -> bool:
    if key == "$schema":
        return False
    normalized = key.lower().replace("-", "_")
    tokens = normalized.split("_")
    return any(
        token in {"digest", "hash", "checksum", "fingerprint"} or token.startswith("sha")
        for token in tokens
    )


def _find_forbidden_keys(
    value: Any,
    path: tuple[str, ...],
    findings: list[str],
) -> None:
    if isinstance(value, dict):
        for key, nested in value.items():
            key_text = str(key)
            nested_path = (*path, key_text)
            if _is_forbidden_identity_key(key_text):
                findings.append(f"{_path_text(nested_path)}: digest/hash fields are forbidden")
            _find_forbidden_keys(nested, nested_path, findings)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _find_forbidden_keys(nested, (*path, str(index)), findings)


def _validate_schema_policy(schema: dict[str, Any]) -> list[str]:
    findings: list[str] = []
    _find_forbidden_keys(schema, ("schema",), findings)

    def visit(value: Any, path: tuple[str, ...]) -> None:
        if isinstance(value, dict):
            if value.get("type") == "object" and value.get("additionalProperties") is not False:
                findings.append(
                    f"{_path_text(path)}.additionalProperties: object schemas must be closed"
                )
            for key, nested in value.items():
                visit(nested, (*path, str(key)))
        elif isinstance(value, list):
            for index, nested in enumerate(value):
                visit(nested, (*path, str(index)))

    visit(schema, ("schema",))
    properties = schema.get("properties")
    if not isinstance(properties, dict):
        findings.append("schema.properties: expected an object")
        return findings
    if set(properties) != OBJECT_FIELDS["root"]:
        findings.append("schema.properties: release identity fields are not closed")
    if set(schema.get("required", [])) != OBJECT_FIELDS["root"]:
        findings.append("schema.required: release identity fields are not closed")
    identity_rule = properties.get("identity_kind", {})
    if set(identity_rule.get("enum", [])) != IDENTITY_KINDS:
        findings.append("schema.properties.identity_kind.enum: approved choices changed")
    consumer_rule = properties.get("required_consumers", {}).get("items", {})
    if set(consumer_rule.get("enum", [])) != REQUIRED_CONSUMERS:
        findings.append("schema.properties.required_consumers.items.enum: consumers changed")

    nested_rules = {
        "project": properties.get("project"),
        "crate_registry": properties.get("crate_registry"),
        "oci": properties.get("oci"),
        "helm": properties.get("helm"),
        "legal": properties.get("legal"),
        "approval": properties.get("approval"),
    }
    helm_rule = nested_rules.get("helm")
    if isinstance(helm_rule, dict):
        nested_rules["helm.annotations"] = helm_rule.get("properties", {}).get("annotations")
    for name, rule in nested_rules.items():
        expected = OBJECT_FIELDS[name]
        if not isinstance(rule, dict):
            findings.append(f"schema.properties.{name}: expected an object schema")
            continue
        if set(rule.get("properties", {})) != expected or set(rule.get("required", [])) != expected:
            findings.append(f"schema.properties.{name}: fields are not closed")
    return findings


def _validate_json_value(
    value: Any,
    rule: dict[str, Any],
    path: tuple[str, ...],
    findings: list[str],
) -> None:
    location = _path_text(path)
    expected_type = rule.get("type")
    matches_type = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "boolean": isinstance(value, bool),
    }.get(expected_type, True)
    if not matches_type:
        findings.append(f"{location}: expected {expected_type}")
        return
    if "const" in rule and value != rule["const"]:
        findings.append(f"{location}: expected {rule['const']!r}")
    if "enum" in rule and value not in rule["enum"]:
        findings.append(f"{location}: unsupported value {value!r}")
    if isinstance(value, str):
        if len(value.strip()) < rule.get("minLength", 0):
            findings.append(f"{location}: must not be blank")
        pattern = rule.get("pattern")
        if isinstance(pattern, str) and re.search(pattern, value) is None:
            findings.append(f"{location}: does not match the required format")
    if isinstance(value, int) and not isinstance(value, bool) and "minimum" in rule:
        if value < rule["minimum"]:
            findings.append(f"{location}: must be at least {rule['minimum']}")
    if isinstance(value, dict):
        properties = rule.get("properties", {})
        required = set(rule.get("required", []))
        for missing in sorted(required - set(value)):
            findings.append(f"{location}.{missing}: required field is missing")
        if rule.get("additionalProperties") is False:
            for extra in sorted(set(value) - set(properties)):
                findings.append(f"{location}.{extra}: field is not allowed")
        for key, nested in value.items():
            nested_rule = properties.get(key)
            if isinstance(nested_rule, dict):
                _validate_json_value(nested, nested_rule, (*path, str(key)), findings)
    if isinstance(value, list):
        if rule.get("uniqueItems") and len({json.dumps(item, sort_keys=True) for item in value}) != len(value):
            findings.append(f"{location}: values must be unique")
        item_rule = rule.get("items")
        if isinstance(item_rule, dict):
            for index, nested in enumerate(value):
                _validate_json_value(nested, item_rule, (*path, str(index)), findings)


def _workspace_metadata(root: Path) -> dict[str, Any]:
    cargo = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
    workspace_package = cargo.get("workspace", {}).get("package")
    if not isinstance(workspace_package, dict):
        raise ValueError("Cargo.toml: missing [workspace.package]")
    return workspace_package


def validate_identity(
    identity: dict[str, Any],
    schema: dict[str, Any],
    *,
    root: Path = ROOT,
) -> list[str]:
    findings = _validate_schema_policy(schema)
    _find_forbidden_keys(identity, (), findings)
    _validate_json_value(identity, schema, (), findings)
    if findings:
        return findings

    approval = identity["approval"]
    project = identity["project"]
    crate_registry = identity["crate_registry"]
    oci = identity["oci"]
    helm = identity["helm"]
    annotations = helm["annotations"]
    legal = identity["legal"]

    for location, value in (
        ("approval.approver", approval["approver"]),
        ("approval.effective_scope", approval["effective_scope"]),
        ("crate_registry.owner", crate_registry["owner"]),
        ("crate_registry.namespace", crate_registry["namespace"]),
        ("oci.namespace", oci["namespace"]),
    ):
        if not isinstance(value, str) or not value.strip():
            findings.append(f"{location}: must not be blank")

    if approval["approved_revision"] != identity["revision"]:
        findings.append("approval.approved_revision: must match identity revision")
    try:
        approved_on = date.fromisoformat(approval["approved_on"])
        if approved_on > date.today():
            findings.append("approval.approved_on: must not be in the future")
    except ValueError:
        findings.append("approval.approved_on: expected an ISO calendar date")
    if approval["effective_scope"] != "core-release-1.0":
        findings.append("approval.effective_scope: expected core-release-1.0")
    if set(identity["required_consumers"]) != REQUIRED_CONSUMERS:
        findings.append("required_consumers: must list every release-preparation consumer")

    workspace = _workspace_metadata(root)
    for field in ("repository", "homepage"):
        if project[field] != workspace.get(field):
            findings.append(f"project.{field}: must match Cargo workspace metadata")
    if legal["license"] != workspace.get("license"):
        findings.append("legal.license: must match Cargo workspace metadata")
    notice = (root / "NOTICE").read_text(encoding="utf-8")
    if legal["notice_owner"] not in notice:
        findings.append("legal.notice_owner: must identify the owner recorded in NOTICE")

    if annotations["rocketmqrust.com/repository"] != project["repository"]:
        findings.append("helm.annotations.rocketmqrust.com/repository: must match project.repository")
    if annotations["rocketmqrust.com/distribution-owner"] != legal["notice_owner"]:
        findings.append(
            "helm.annotations.rocketmqrust.com/distribution-owner: must match legal.notice_owner"
        )

    if identity["identity_kind"] == "unofficial-community":
        if identity["official_apache_release"] is not False:
            findings.append("official_apache_release: community releases must set false")
        if annotations["rocketmqrust.com/official-apache-release"] != "false":
            findings.append(
                "helm.annotations.rocketmqrust.com/official-apache-release: community releases must set false"
            )
        if "community" not in identity["distribution_name"].lower():
            findings.append("distribution_name: community identity must be explicit")
        disclaimer = legal["disclaimer"].lower()
        if "not an official apache software foundation release" not in disclaimer:
            findings.append("legal.disclaimer: unofficial status must be explicit")
    else:
        if identity["official_apache_release"] is not True:
            findings.append("official_apache_release: Apache governance releases must set true")
        if annotations["rocketmqrust.com/official-apache-release"] != "true":
            findings.append(
                "helm.annotations.rocketmqrust.com/official-apache-release: Apache governance releases must set true"
            )
    return findings


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the frozen release identity")
    parser.add_argument("--identity", type=Path, default=DEFAULT_IDENTITY)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--stage", choices=("preflight",), default="preflight")
    args = parser.parse_args()
    try:
        identity = read_json(args.identity)
        schema = read_json(args.schema)
        findings = validate_identity(identity, schema)
    except (OSError, json.JSONDecodeError, ValueError, tomllib.TOMLDecodeError) as error:
        print(f"RELEASE_IDENTITY_ERROR {error}")
        return 2
    if findings:
        print("RELEASE_IDENTITY_FAILED")
        for finding in findings:
            print(f"- {finding}")
        return 1
    approval = identity["approval"]
    print(
        "RELEASE_IDENTITY_OK "
        f"kind={identity['identity_kind']} "
        f"approver={approval['approver']} "
        f"scope={approval['effective_scope']} "
        f"stage={args.stage}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
