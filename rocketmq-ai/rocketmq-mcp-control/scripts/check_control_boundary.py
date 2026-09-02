#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import json
import os
import pathlib
import subprocess
import sys


PROJECT = pathlib.Path(__file__).resolve().parents[1]


def fail(message: str) -> None:
    print(f"control boundary violation: {message}", file=sys.stderr)
    raise SystemExit(1)


def metadata(features: list[str]) -> dict:
    command = ["cargo", "metadata", "--locked", "--format-version", "1"]
    if features:
        command.extend(["--features", ",".join(features)])
    completed = subprocess.run(command, cwd=PROJECT, check=False, capture_output=True, text=True)
    if completed.returncode != 0:
        fail(f"cargo metadata failed: {completed.stderr.strip()}")
    return json.loads(completed.stdout)


def package_names(document: dict) -> set[str]:
    package_by_id = {package["id"]: package["name"] for package in document["packages"]}
    return {package_by_id[node["id"]] for node in document["resolve"]["nodes"]}


def node_features(document: dict, package_name: str) -> set[str]:
    package_by_id = {package["id"]: package["name"] for package in document["packages"]}
    for node in document["resolve"]["nodes"]:
        if package_by_id[node["id"]] == package_name:
            return set(node["features"])
    fail(f"missing metadata node for {package_name}")
    return set()


def run_query_contract(command: list[str]) -> None:
    query_project = PROJECT.parent / "rocketmq-mcp"
    environment = os.environ.copy()
    environment["INSTA_UPDATE"] = "no"
    environment.pop("RUST_MIN_STACK", None)
    completed = subprocess.run(
        command,
        cwd=query_project,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        fail(f"query MCP contract command failed: {' '.join(command)}\n{completed.stderr.strip()}")


manifest = (PROJECT / "Cargo.toml").read_text(encoding="utf-8")
if 'default = []' not in manifest:
    fail("default feature set must be empty")
if 'write-tools = ["dep:rocketmq-admin-core"]' not in manifest:
    fail("write-tools must contain only the optional Admin Core dependency")
if 'features = ["mutation-client-adapter"]' not in manifest:
    fail("Admin Core dependency must select mutation-client-adapter")

default_metadata = metadata([])
default_names = package_names(default_metadata)
for forbidden in ("rocketmq-admin-core", "rocketmq-client-rust"):
    if forbidden in default_names:
        fail(f"default dependency closure contains {forbidden}")

write_metadata = metadata(["write-tools"])
write_names = package_names(write_metadata)
for required in ("rocketmq-admin-core", "rocketmq-client-rust"):
    if required not in write_names:
        fail(f"write-tools dependency closure is missing {required}")

admin_features = node_features(write_metadata, "rocketmq-admin-core")
client_features = node_features(write_metadata, "rocketmq-client-rust")
if "mutation-client-adapter" not in admin_features or "admin-mutation" not in client_features:
    fail("write-tools did not select the mutation-only feature chain")
for forbidden in ("read-client-adapter", "client-adapter"):
    if forbidden in admin_features:
        fail(f"write-tools enabled forbidden Admin Core feature {forbidden}")
for forbidden in ("admin-read", "admin-full"):
    if forbidden in client_features:
        fail(f"write-tools enabled forbidden client feature {forbidden}")

source_paths = sorted((PROJECT / "src").glob("**/*.rs"))
source = "\n".join(path.read_text(encoding="utf-8") for path in source_paths)
for forbidden in (
    "transport-io",
    "std::process",
    "tokio::process",
    "Command::new",
    "println!",
    "print!",
    "read_client_adapter",
    "ReadOnlyQuery",
):
    if forbidden in source:
        fail(f"production source contains prohibited surface {forbidden}")

# Dedicated test modules and inline `mod tests` bodies are excluded from the production lifecycle scan.
production_units = []
for path in source_paths:
    if path.name == "tests.rs":
        continue
    unit = path.read_text(encoding="utf-8")
    unit = unit.split("#[cfg(test)]\nmod tests", maxsplit=1)[0]
    production_units.append(unit)
production_source = "\n".join(production_units)
for forbidden in (
    "tokio::spawn",
    "spawn_blocking",
    "std::thread",
    "JoinSet",
    "tokio::runtime::Runtime::new",
    "tokio::runtime::Runtime::block_on",
):
    if forbidden in production_source:
        fail(f"production source contains unmanaged lifecycle surface {forbidden}")
if "spawn_service(\"mcp-control-mutation-supervisor\"" not in production_source:
    fail("reviewed tool execution is not visibly owned by the injected task group")

for required in (
    "rocketmq_upsert_topic",
    "rocketmq_upsert_consumer_group",
    "rocketmq_reset_consumer_offset",
    "rocketmq_patch_broker_config",
    "rocketmq_set_consumer_request_mode",
):
    if required not in source:
        fail(f"reviewed write catalog is missing {required}")

for forbidden in (
    "rocketmq_delete_",
    "rocketmq_skip_",
    "rocketmq_resend_",
):
    if forbidden in source:
        fail(f"production source contains unreviewed tool {forbidden}")

run_query_contract([sys.executable, "scripts/check_read_only_boundary.py"])
for arguments in (
    ["cargo", "test", "--locked", "complete_tool_contract_snapshot"],
    ["cargo", "test", "--locked", "--all-features", "complete_tool_contract_snapshot"],
    ["cargo", "test", "--locked", "mcp_protocol_surface_snapshot"],
    ["cargo", "test", "--locked", "--all-features", "mcp_protocol_surface_snapshot"],
):
    run_query_contract(arguments)

print("rocketmq-mcp-control boundary checks passed")
