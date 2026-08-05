#!/usr/bin/env python3
# Copyright 2023 The RocketMQ Rust Authors
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

"""Normalize reviewed override metadata and source references."""

from __future__ import annotations

import json
from pathlib import Path


OWNER = "rocketmq-rust protocol maintainers"

EXTRA_PRESENCE_REVIEWS = [
    ("ConsumerSendMsgBackRequestHeader", "unitMode", "optional", "approved-rust-default"),
    ("AlterSyncStateSetRequestHeader", "masterBrokerId", "optional", "approved-rust-default"),
    ("AlterSyncStateSetRequestHeader", "masterEpoch", "optional", "approved-rust-default"),
    ("ApplyBrokerIdRequestHeader", "appliedBrokerId", "optional", "approved-rust-default"),
    ("ElectMasterRequestHeader", "invokeTime", "optional", "approved-rust-default"),
    ("GetConsumerRunningInfoRequestHeader", "jstackEnable", "optional", "approved-rust-default"),
    ("GetMaxOffsetRequestHeader", "committed", "optional", "approved-rust-default"),
    ("GetMetaDataResponseHeader", "isLeader", "primitive", "defer-strict-presence-to-v3"),
    ("NotificationRequestHeader", "order", "optional", "approved-rust-default"),
]


def main() -> None:
    root = Path(__file__).resolve().parent
    override_path = root / "schema-overrides.json"
    mapping = json.loads((root / "header-class-map.json").read_text(encoding="utf-8"))
    document = json.loads(override_path.read_text(encoding="utf-8"))
    by_type = {entry["rustType"]: entry for entry in mapping["entries"]}

    for entry in document["requiredDrift"]:
        if entry["rustType"] == "ResetMasterFlushOffsetRequestHeader":
            entry["rustType"] = "ResetMasterFlushOffsetHeader"

    if not any(
        entry["rustType"] == "GetBrokerConfigResponseHeader" and entry["field"] == "version"
        for entry in document["requiredDrift"]
    ):
        document["requiredDrift"].append(
            {
                "rustType": "GetBrokerConfigResponseHeader",
                "field": "version",
                "javaPresence": "required",
                "decision": "defer-strict-presence-to-v3",
                "wireCompatibility": "accept-missing-from-legacy-rust",
                "malformedPolicy": "reject-invalid",
                "requestCodes": [],
            }
        )

    existing = {(entry["rustType"], entry["field"]) for entry in document["requiredDrift"]}
    for rust_type, field, java_presence, decision in EXTRA_PRESENCE_REVIEWS:
        if (rust_type, field) in existing:
            continue
        document["requiredDrift"].append(
            {
                "rustType": rust_type,
                "field": field,
                "javaPresence": java_presence,
                "decision": decision,
                "wireCompatibility": "accept-missing-with-reviewed-rust-default",
                "malformedPolicy": "reject-invalid",
                "requestCodes": by_type[rust_type]["requestCodes"],
            }
        )

    groups = ("defaults", "nameMappings", "aliasConflictPolicies", "requiredDrift")
    for group in groups:
        for entry in document[group]:
            rust_type = entry["rustType"]
            mapping_entry = by_type.get(rust_type)
            if mapping_entry is None and rust_type == "RpcRequestHeader":
                mapping_entry = by_type["RpcRequestHeader"]
            if mapping_entry is None:
                raise ValueError(f"no source mapping for override type {rust_type}")
            entry["owner"] = entry.get("owner", OWNER)
            if "reason" not in entry:
                if entry.get("decision") == "align-java-optional":
                    entry["reason"] = "Accept legal Java frames that omit the field while rejecting malformed values"
                elif entry.get("decision") == "align-java-required":
                    entry["reason"] = "Match Java CFNotNull presence without changing the field wire type"
                else:
                    entry["reason"] = "Preserve the public optional Rust API while V3 enforces Java strict presence"
            entry["referenceSource"] = {
                "rust": mapping_entry["rustSource"],
                "java": mapping_entry["javaSource"],
            }

    document["requiredDrift"].sort(key=lambda entry: (entry["rustType"], entry["field"]))
    override_path.write_text(
        json.dumps(document, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


if __name__ == "__main__":
    main()
