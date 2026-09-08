#!/usr/bin/env python3
# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

"""Restart one core Controller voter after a fresh quorum check (Python 3.11+).

Requires kubectl and rocketmq-admin-cli. The Controller address must reach the
current leader; a port-forward is supported. Admin credentials are inherited
through ROCKETMQ_ACL_ACCESS_KEY, ROCKETMQ_ACL_SECRET_KEY and ROCKETMQ_ACL_SECURITY_TOKEN.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import tomllib
from urllib.parse import quote


class RolloutError(RuntimeError):
    pass


def execute(command: list[str], *, body: str | None = None, timeout: float = 30) -> str:
    result = subprocess.run(command, input=body, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode:
        # Command output can include authentication diagnostics. The operation's
        # exit code is sufficient here; operators can run read-only queries directly.
        raise RolloutError(f"{command[0]} failed with exit code {result.returncode}; rollout stopped")
    return result.stdout


def quorum_observation(output: str, target: int, peers: dict[str, str]) -> dict:
    observations = []
    for line in output.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and "schemaVersion" in value:
            observations.append(value)
    if len(observations) != 1:
        raise RolloutError("Admin did not return one supported quorum observation")
    value = observations[0]
    voters = value.get("voters", [])
    ready = value.get("remainingReadyVoters", [])
    if not isinstance(voters, list) or not isinstance(ready, list):
        raise RolloutError("Controller quorum observation is malformed")
    if not all(type(node) is int and node > 0 for node in voters + ready):
        raise RolloutError("Controller voter identities are invalid")
    if (value.get("schemaVersion") != 1 or value.get("allowed") is not True
            or value.get("targetNodeId") != target or len(voters) < 3
            or len(set(voters)) != len(voters) or len(set(ready)) != len(ready)
            or target not in voters or target in ready or value.get("leaderId") not in voters
            or not set(ready).issubset(voters) or len(ready) <= len(voters) // 2
            or value.get("peerEndpoints") != peers or set(peers) != {str(node) for node in voters}):
        raise RolloutError("Remaining quorum for this Controller deployment is not verified")
    return value


def owned_by(pod: dict, statefulset_uid: str) -> bool:
    return any(owner.get("kind") == "StatefulSet" and owner.get("uid") == statefulset_uid
               and owner.get("controller") is True
               for owner in pod.get("metadata", {}).get("ownerReferences", []))


def restart_controller(args, run=execute, *, clock=time.monotonic, pause=time.sleep) -> str:
    if args.ordinal < 0 or not all(re.fullmatch(r"[a-z0-9][a-z0-9.-]*", item)
                                   for item in (args.namespace, args.statefulset)):
        raise RolloutError("A valid namespace, StatefulSet and nonnegative ordinal are required")
    kubectl = [args.kubectl, "--namespace", args.namespace]

    def get(kind: str, name: str, *, missing_ok: bool = False, timeout: float = 30) -> dict | None:
        command = [*kubectl, "get", kind, name, "-o", "json"]
        if missing_ok:
            command.append("--ignore-not-found")
        source = run(command, timeout=timeout)
        return json.loads(source) if source.strip() else None

    workload = get("statefulset", args.statefulset)
    if not workload or workload["metadata"].get("labels", {}).get("app.kubernetes.io/component") != "controller":
        raise RolloutError("The selected StatefulSet is not a core Controller")
    spec = workload["spec"]
    if spec.get("updateStrategy", {}).get("type") != "OnDelete" or args.ordinal >= spec["replicas"]:
        raise RolloutError("The selected Controller requires OnDelete and a configured ordinal")
    pod_name = f"{args.statefulset}-{args.ordinal}"
    pod = get("pod", pod_name)
    owner_uid = workload["metadata"]["uid"]
    if not pod or not owned_by(pod, owner_uid) or pod["metadata"].get("deletionTimestamp"):
        raise RolloutError("The target Pod is missing, terminating or belongs to another StatefulSet")
    config_volume = next((v for v in spec["template"]["spec"]["volumes"] if v["name"] == "config"), None)
    if not config_volume or "configMap" not in config_volume:
        raise RolloutError("The Controller has no rendered configuration volume")
    configmap = get("configmap", config_volume["configMap"]["name"])
    config = tomllib.loads(configmap["data"][f"{pod_name}.toml"])
    target = config["nodeId"]
    peers = {str(peer["id"]): peer["addr"] for peer in config["raftPeerEndpoints"]}
    if target != args.ordinal + 1:
        raise RolloutError("The rendered Controller identity does not match its Pod ordinal")
    # Require the chart PDB to exist before using the API that enforces it.
    pdb = get("pdb", args.statefulset)
    labels = pod["metadata"].get("labels", {})
    selector = pdb["spec"].get("selector", {}).get("matchLabels", {}) if pdb else {}
    if (not selector or not all(labels.get(key) == value for key, value in selector.items())
            or selector != spec["selector"].get("matchLabels")
            or pdb["spec"].get("selector", {}).get("matchExpressions")
            or pdb["spec"].get("minAvailable") != len(peers) // 2 + 1):
        raise RolloutError("The Controller majority disruption budget is missing or mismatched")

    output = run([args.admin, "getControllerMetaData", "-a", args.controller_address,
                  "--check-quorum-for-node", str(target)], timeout=10)
    quorum_observation(output, target, peers)
    eviction = {
        "apiVersion": "policy/v1", "kind": "Eviction",
        "metadata": {"name": pod_name, "namespace": args.namespace},
        "deleteOptions": {"preconditions": {"uid": pod["metadata"]["uid"]}},
    }
    path = f"/api/v1/namespaces/{quote(args.namespace, safe='')}/pods/{quote(pod_name, safe='')}/eviction"
    run([*kubectl, "create", "--raw", path, "-f", "-"], body=json.dumps(eviction))

    deadline = clock() + args.timeout
    while clock() < deadline:
        replacement = get("pod", pod_name, missing_ok=True, timeout=min(30, max(0.01, deadline - clock())))
        if replacement and replacement["metadata"]["uid"] != pod["metadata"]["uid"]:
            if not owned_by(replacement, owner_uid):
                raise RolloutError("Replacement Pod belongs to a different StatefulSet")
            if not replacement["metadata"].get("deletionTimestamp") and any(c.get("type") == "Ready" and c.get("status") == "True"
                   for c in replacement.get("status", {}).get("conditions", [])):
                return pod_name
        pause(min(1, max(0, deadline - clock())))
    raise RolloutError("Pod was evicted, but its replacement did not become Ready before the timeout")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--statefulset", required=True)
    parser.add_argument("--ordinal", required=True, type=int)
    parser.add_argument("--controller-address", required=True)
    parser.add_argument("--kubectl", default="kubectl")
    parser.add_argument("--admin", default="rocketmq-admin-cli")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    try:
        pod = restart_controller(args)
    except (RolloutError, subprocess.TimeoutExpired, OSError, KeyError, ValueError, TypeError) as error:
        message = str(error) if isinstance(error, RolloutError) else "invalid configuration or unavailable command; rollout stopped"
        print(message, file=sys.stderr)
        return 1
    print(f"{pod}: replacement is Ready; recheck quorum before restarting another voter")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
