# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

import copy
import json
from types import SimpleNamespace
import unittest

from scripts.tests.release_test_support import load_module

rollout = load_module("core_helm_rollout", "distribution/helm/rocketmq-rust-core/files/rollout.py")


class Deployment:
    def __init__(self):
        self.calls = []
        self.evicted = False
        self.eviction_denied = False
        self.time = 0
        self.args = SimpleNamespace(kubectl="kubectl", admin="admin", namespace="test",
                                    statefulset="core-controller", ordinal=1,
                                    controller_address="localhost:19878", timeout=3)
        labels = {"app.kubernetes.io/instance": "core", "app.kubernetes.io/component": "controller"}
        self.workload = {"metadata": {"uid": "sts-1", "labels": labels}, "spec": {
            "replicas": 3, "updateStrategy": {"type": "OnDelete"},
            "selector": {"matchLabels": labels},
            "template": {"spec": {"volumes": [{"name": "config", "configMap": {"name": "controller-config"}}]}}}}
        self.pod = {"metadata": {"uid": "old-pod", "labels": labels,
                    "ownerReferences": [{"kind": "StatefulSet", "uid": "sts-1", "controller": True}]},
                    "status": {"conditions": [{"type": "Ready", "status": "True"}]}}
        self.replacement = copy.deepcopy(self.pod)
        self.replacement["metadata"]["uid"] = "replacement"
        peers = {str(i): f"core-controller-{i - 1}.core-controller-peer.test.svc.cluster.local:9879" for i in range(1, 4)}
        self.observation = {"schemaVersion": 1, "targetNodeId": 2, "leaderId": 1, "voters": [1, 2, 3],
                            "peerEndpoints": peers, "remainingReadyVoters": [1, 3], "allowed": True}
        source = "nodeId = 2\n" + "\n".join(f'[[raftPeerEndpoints]]\nid = {i}\naddr = "{addr}"' for i, addr in peers.items())
        self.configmap = {"data": {"core-controller-1.toml": source}}
        self.pdb = {"spec": {"minAvailable": 2, "selector": {"matchLabels": labels}}}

    def run(self, command, *, body=None, timeout=30):
        self.calls.append((command, body))
        if command[0] == "admin":
            return json.dumps(self.observation)
        if "create" in command:
            if self.eviction_denied:
                raise rollout.RolloutError("PDB refused eviction")
            self.evicted = True
            return "{}"
        kind = command[command.index("get") + 1]
        objects = {"statefulset": self.workload, "configmap": self.configmap, "pdb": self.pdb,
                   "pod": self.replacement if self.evicted else self.pod}
        value = objects[kind]
        return json.dumps(value) if value is not None else ""

    def pause(self, duration):
        self.time += duration

    def restart(self):
        return rollout.restart_controller(self.args, self.run, clock=lambda: self.time, pause=self.pause)


class CoreHelmRolloutTests(unittest.TestCase):
    def test_fresh_check_precedes_uid_guarded_eviction_and_new_pod_readiness(self):
        deployment = Deployment()
        self.assertEqual("core-controller-1", deployment.restart())
        commands = [command for command, _ in deployment.calls]
        check = next(i for i, command in enumerate(commands) if command[0] == "admin")
        eviction = next(i for i, command in enumerate(commands) if "create" in command)
        self.assertEqual(check + 1, eviction)
        body = json.loads(deployment.calls[eviction][1])
        self.assertEqual("policy/v1", body["apiVersion"])
        self.assertEqual({"uid": "old-pod"}, body["deleteOptions"]["preconditions"])
        self.assertTrue(any("/eviction" in value for value in commands[eviction]))
        self.assertFalse(any("delete" in command for command in commands))

    def test_unverified_or_wrong_cluster_quorum_never_evicts(self):
        for replacement in [{}, {"allowed": False}, {"remainingReadyVoters": [1]},
                            {"remainingReadyVoters": [1, 1]}, {"targetNodeId": 3},
                            {"peerEndpoints": {"1": "other-cluster:9879"}}]:
            with self.subTest(replacement=replacement):
                deployment = Deployment()
                if replacement:
                    deployment.observation.update(replacement)
                else:
                    deployment.observation = {}
                with self.assertRaises(rollout.RolloutError):
                    deployment.restart()
                self.assertFalse(deployment.evicted)

    def test_pdb_refusal_is_not_retried_or_bypassed(self):
        deployment = Deployment()
        deployment.eviction_denied = True
        with self.assertRaisesRegex(rollout.RolloutError, "PDB refused"):
            deployment.restart()
        self.assertEqual(1, sum("create" in command for command, _ in deployment.calls))
        self.assertFalse(deployment.evicted)

    def test_old_ready_pod_does_not_count_as_replacement(self):
        deployment = Deployment()
        deployment.replacement = deployment.pod
        with self.assertRaisesRegex(rollout.RolloutError, "replacement did not become Ready"):
            deployment.restart()
        self.assertTrue(deployment.evicted)

    def test_replacement_from_another_owner_is_rejected(self):
        deployment = Deployment()
        deployment.replacement["metadata"]["ownerReferences"][0]["uid"] = "different-sts"
        with self.assertRaisesRegex(rollout.RolloutError, "different StatefulSet"):
            deployment.restart()

    def test_terminating_ready_replacement_does_not_finish_rollout(self):
        deployment = Deployment()
        deployment.replacement["metadata"]["deletionTimestamp"] = "2026-09-08T00:00:00Z"
        with self.assertRaisesRegex(rollout.RolloutError, "replacement did not become Ready"):
            deployment.restart()

    def test_mismatched_pdb_or_automatic_rollout_is_rejected_before_check(self):
        for change in ("selector", "strategy"):
            with self.subTest(change=change):
                deployment = Deployment()
                if change == "selector":
                    deployment.pdb["spec"]["selector"] = {"matchLabels": {"unrelated": "true"}}
                else:
                    deployment.workload["spec"]["updateStrategy"]["type"] = "RollingUpdate"
                with self.assertRaises(rollout.RolloutError):
                    deployment.restart()
                self.assertFalse(any(command[0] == "admin" for command, _ in deployment.calls))


if __name__ == "__main__":
    unittest.main()
