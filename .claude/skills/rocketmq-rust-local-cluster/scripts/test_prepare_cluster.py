#!/usr/bin/env python3
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

"""Check generated topology relationships and non-destructive preparation."""

import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import tomllib
import unittest

from prepare_cluster import PROFILES, build_plan, check_ports, write_plan


SCRIPT = Path(__file__).with_name("prepare_cluster.py")
REPO = Path(__file__).resolve().parents[4]


class PrepareClusterTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="rocketmq-skill-test-")
        self.addCleanup(self.temporary.cleanup)
        self.output = Path(self.temporary.name) / "environment with spaces"

    def plan(self, profile, **kwargs):
        return build_plan(REPO, self.output, "test", profile, **kwargs)

    def test_all_profiles_isolate_paths_and_resolve_backend_endpoints(self):
        for profile in PROFILES:
            with self.subTest(profile=profile):
                manifest, files = self.plan(profile)
                addresses = [entry["address"] for entry in manifest["ports"]]
                self.assertEqual(len(addresses), len(set(addresses)))
                self.assertEqual(len(manifest["nodes"]), len({node["cwd"] for node in manifest["nodes"]}))
                nameservers = {node["ports"]["remoting"] for node in manifest["nodes"] if node["service"] == "namesrv"}
                for node in manifest["nodes"]:
                    config = tomllib.loads(files[f"conf/{node['id']}.toml"])
                    self.assertTrue(Path(node["cwd"]).is_relative_to(self.output))
                    self.assertTrue(Path(node["argv"][1]).is_relative_to(self.output))
                    if node["service"] == "broker":
                        self.assertEqual(set(config["broker"]["namesrvAddr"].split(";")), nameservers)
                        self.assertEqual(config["broker"]["storePathRootDir"], config["store"]["storePathRootDir"])
                        self.assertTrue(Path(config["store"]["storePathBrokerIdentity"]).is_relative_to(node["cwd"]))
                    if node["service"] == "proxy" and profile != "proxy-local":
                        self.assertEqual(set(config["cluster"]["namesrvAddr"].split(";")), nameservers)
                        self.assertEqual(config["cluster"]["brokerClusterName"], manifest["cluster_name"])

    def test_two_replication_groups_resolve_their_own_master(self):
        for replication in ("sync", "async"):
            manifest, files = self.plan("dev-ha-2m2s", replication=replication)
            brokers = [node for node in manifest["nodes"] if node["service"] == "broker"]
            for master, slave in (brokers[:2], brokers[2:]):
                primary = tomllib.loads(files[f"conf/{master['id']}.toml"])
                secondary = tomllib.loads(files[f"conf/{slave['id']}.toml"])
                self.assertEqual(secondary["store"]["haMasterAddress"], master["ports"]["ha"])
                self.assertEqual(primary["broker"]["brokerIdentity"]["brokerName"], secondary["broker"]["brokerIdentity"]["brokerName"])
                self.assertNotEqual(primary["broker"]["brokerIdentity"]["brokerId"], secondary["broker"]["brokerIdentity"]["brokerId"])
                self.assertEqual(primary["store"]["minInSyncReplicas"], 2 if replication == "sync" else 1)

    def test_controller_membership_bootstrap_and_broker_discovery_match(self):
        manifest, files = self.plan("controller-ha", with_proxy=True)
        controllers = [node for node in manifest["nodes"] if node["service"] == "controller"]
        remoting = {node["ports"]["remoting"] for node in controllers}
        raft = {node["ports"]["raft"] for node in controllers}
        self.assertFalse(remoting & raft)
        initialized = [node for node in controllers if node["env"]["ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER"] == "true"]
        self.assertEqual([node["id"] for node in initialized], [controllers[0]["id"]])
        for node in controllers:
            config = tomllib.loads(files[f"conf/{node['id']}.toml"])
            self.assertEqual({peer["addr"] for peer in config["raftPeers"]}, raft)
            self.assertEqual({peer["addr"] for peer in config["controllerPeers"]}, remoting)
            self.assertEqual(next(peer["addr"] for peer in config["raftPeers"] if peer["id"] == config["nodeId"]), config["raftListenAddr"])
        for node in manifest["nodes"]:
            if node["service"] != "broker":
                continue
            config = tomllib.loads(files[f"conf/{node['id']}.toml"])
            self.assertEqual(set(config["broker"]["controllerAddr"].split(";")), remoting)
            self.assertGreater(config["broker"]["brokerIdentity"]["brokerId"], 0)
            self.assertEqual(config["store"]["brokerRole"], "SLAVE")
            self.assertNotIn("haMasterAddress", config["store"])
            self.assertLessEqual(config["store"]["minInSyncReplicas"], config["store"]["inSyncReplicas"])
            self.assertLessEqual(config["store"]["inSyncReplicas"], config["store"]["totalReplicas"])

    def test_offset_updates_every_address_without_changing_topology(self):
        original, _ = self.plan("controller-ha", with_proxy=True, proxy_remoting=True)
        shifted, _ = self.plan("controller-ha", offset=2000, with_proxy=True, proxy_remoting=True)
        for left, right in zip(original["ports"], shifted["ports"], strict=True):
            self.assertEqual(left["node"], right["node"])
            self.assertEqual(int(right["address"].split(":")[1]) - int(left["address"].split(":")[1]), 2000)

    def test_local_proxy_has_no_external_backend_and_matching_features(self):
        manifest, files = self.plan("proxy-local")
        self.assertEqual([node["service"] for node in manifest["nodes"]], ["proxy"])
        config = tomllib.loads(files["conf/proxy-1.toml"])
        self.assertNotIn("cluster", config)
        self.assertEqual(manifest["namesrv_addr"], "")
        self.assertEqual(manifest["build_commands"][0][-1], "local-mode")

    def test_existing_environment_is_preserved_and_new_files_parse(self):
        manifest, files = self.plan("controller-ha")
        write_plan(manifest, files)
        sentinel = Path(manifest["nodes"][0]["cwd"]) / "data" / "messages"
        sentinel.write_text("preserve me", encoding="utf-8")
        with self.assertRaises(FileExistsError):
            write_plan(*self.plan("dev-single"))
        self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve me")
        self.assertEqual(json.loads((self.output / "manifest.json").read_text(encoding="utf-8")), manifest)
        for config in (self.output / "conf").glob("*.toml"):
            with config.open("rb") as stream:
                tomllib.load(stream)

    def test_dry_run_has_no_filesystem_side_effects(self):
        result = subprocess.run([sys.executable, str(SCRIPT), "--repo-root", str(REPO), "--name", "test",
                                 "--profile", "proxy-cluster", "--output", str(self.output), "--dry-run"],
                                capture_output=True, text=True, timeout=10, check=True)
        self.assertEqual(json.loads(result.stdout)["profile"], "proxy-cluster")
        self.assertFalse(self.output.exists())

    def test_invalid_combinations_and_names_fail_before_writing(self):
        for kwargs in ({"replication": "async"}, {"offset": 65535}, {"offset": -1}, {"proxy_remoting": True}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.plan("controller-ha", **kwargs)
        for name in ("../escape", "a/b", "CON", "nul", "com1"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                build_plan(REPO, self.output, name, "dev-single")
        self.assertFalse(self.output.exists())

    def test_port_conflict_is_reported_without_disturbing_owner(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as owner:
            owner.bind(("127.0.0.1", 0))
            owner.listen()
            address = f"127.0.0.1:{owner.getsockname()[1]}"
            with self.assertRaisesRegex(ValueError, "unavailable"):
                check_ports({"ports": [{"node": "owned", "purpose": "health", "address": address}]})
            self.assertGreater(owner.getsockname()[1], 0)


if __name__ == "__main__":
    unittest.main()
