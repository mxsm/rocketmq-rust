# Copyright 2026 The RocketMQ Rust Authors
# Licensed under the Apache License, Version 2.0.

from __future__ import annotations

import os
import shutil
import subprocess
import unittest

from scripts.core_helm_configs import CHART, POLICY, configurations, render


class CoreHelmRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.helm = os.environ.get("HELM") or shutil.which("helm")
        if not cls.helm:
            raise unittest.SkipTest("Helm is required for rendered runtime contracts")
        cls.profiles = {profile: render(profile, cls.helm) for profile in POLICY["profiles"]}

    def test_all_profiles_lint_and_connect_pod_identity_to_real_config(self):
        for profile, documents in self.profiles.items():
            with self.subTest(profile=profile):
                subprocess.run([self.helm, "lint", str(CHART), "-f", str(CHART / profile)],
                               check=True, capture_output=True, text=True, timeout=60)
                configs = {d["metadata"]["name"]: d["data"] for d in documents if d["kind"] == "ConfigMap"}
                for workload in [d for d in documents if d["kind"] in ("StatefulSet", "Deployment")]:
                    name = workload["metadata"]["name"]
                    pod = workload["spec"]["template"]["spec"]
                    container = pod["containers"][0]
                    env = {v["name"]: v for v in container["env"]}
                    self.assertNotIn("command", container)
                    if workload["kind"] == "StatefulSet":
                        self.assertEqual("OnDelete", workload["spec"]["updateStrategy"]["type"])
                        self.assertEqual("Parallel", workload["spec"]["podManagementPolicy"])
                        self.assertEqual(["-c", "/etc/rocketmq/$(POD_NAME).toml"], container["args"])
                        self.assertEqual("metadata.name", env["POD_NAME"]["valueFrom"]["fieldRef"]["fieldPath"])
                        self.assertEqual({f"{name}-{i}.toml" for i in range(workload["spec"]["replicas"])}, set(configs[name]))
                        self.assertEqual({"whenDeleted": "Retain", "whenScaled": "Retain"},
                                         workload["spec"]["persistentVolumeClaimRetentionPolicy"])
                    for probe, path in [("readinessProbe", "/readyz"), ("livenessProbe", "/livez")]:
                        self.assertEqual({"path": path, "port": "health"}, container[probe]["httpGet"])
                    self.assertEqual("/drainz", container["lifecycle"]["preStop"]["httpGet"]["path"])
                    self.assertGreater(pod["terminationGracePeriodSeconds"], int(env["ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS"]["value"]))

    def test_production_enables_auth_and_mounts_inner_client_secrets(self):
        for profile, documents in self.profiles.items():
            if "production" not in profile:
                continue
            for service, _, _, config in configurations(documents):
                auth = config["broker"] if service == "broker" else config.get("auth", config)
                self.assertTrue(auth["authenticationEnabled"])
                self.assertTrue(auth["authorizationEnabled"])
                self.assertEqual("/etc/rocketmq/acl/plain_acl.yml", auth["aclFile"])
                if service == "proxy":
                    self.assertTrue(config["enableAclRpcHookForClusterMode"])
            for workload in [d for d in documents if d["kind"] in ("StatefulSet", "Deployment")]:
                service = workload["metadata"]["labels"]["app.kubernetes.io/component"]
                if service not in ("broker", "proxy"):
                    continue
                pod = workload["spec"]["template"]["spec"]
                env = {item["name"]: item.get("value") for item in pod["containers"][0]["env"]}
                self.assertEqual("/etc/rocketmq/acl/inner-client.json", env["ROCKETMQ_INNER_CLIENT_CREDENTIALS_FILE"])
                acl = next(v["secret"] for v in pod["volumes"] if v["name"] == "acl")
                self.assertIn({"key": "inner-client.json", "path": "inner-client.json"}, acl["items"])

    def test_production_rejects_authentication_downgrade(self):
        for override in ("securityProfile=null", "services.broker.auth.credentialsKey=null"):
            with self.subTest(override=override), self.assertRaises(subprocess.CalledProcessError):
                render("values-production-default-ha.yaml", self.helm, override)
        for service in ("namesrv", "broker", "proxy"):
            with self.subTest(service=service), self.assertRaises(subprocess.CalledProcessError):
                render("values-production-proxy-tls.yaml", self.helm, f"services.{service}.auth.enabled=false")
        with self.assertRaises(subprocess.CalledProcessError):
            render("values-production-controller-ha.yaml", self.helm, "services.controller.auth.enabled=false")

    def test_controller_membership_and_broker_replication_are_real(self):
        documents = self.profiles["values-production-controller-ha.yaml"]
        configs = list(configurations(documents))
        controllers = [config for service, _, _, config in configs if service == "controller"]
        self.assertEqual({1, 2, 3}, {config["nodeId"] for config in controllers})
        for config in controllers:
            self.assertEqual({1, 2, 3}, {p["id"] for p in config["raftPeerEndpoints"]})
            self.assertEqual(controllers[0]["raftPeerEndpoints"], config["raftPeerEndpoints"])
            self.assertEqual(controllers[0]["controllerPeerEndpoints"], config["controllerPeerEndpoints"])
            for peer in config["raftPeerEndpoints"]:
                self.assertIn(f"controller-{peer['id'] - 1}.core-controller-peer.", peer["addr"])
        brokers = [config for service, _, _, config in configs if service == "broker"]
        self.assertEqual(3, len({c["broker"]["brokerIdentity"]["brokerId"] for c in brokers}))
        for config in brokers:
            self.assertTrue(config["broker"]["enableControllerMode"])
            self.assertEqual(3, len(config["broker"]["controllerAddr"].split(";")))
            self.assertEqual("SLAVE", config["store"]["brokerRole"])
            self.assertEqual(3, config["store"]["totalReplicas"])
            self.assertEqual(2, config["store"]["minInSyncReplicas"])
        controller = next(d for d in documents if d["kind"] == "StatefulSet" and d["metadata"]["name"] == "core-controller")
        self.assertTrue(controller["spec"]["template"]["spec"]["affinity"]["podAntiAffinity"]["requiredDuringSchedulingIgnoredDuringExecution"])
        pdb = next(d for d in documents if d["kind"] == "PodDisruptionBudget" and d["metadata"]["name"] == "core-controller")
        self.assertEqual(2, pdb["spec"]["minAvailable"])

    def test_default_ha_roles_and_separate_peer_services(self):
        documents = self.profiles["values-production-default-ha.yaml"]
        brokers = [c for s, _, _, c in configurations(documents) if s == "broker"]
        self.assertEqual(["SYNC_MASTER", "SLAVE"], [c["store"]["brokerRole"] for c in brokers])
        self.assertEqual([0, 1], [c["broker"]["brokerIdentity"]["brokerId"] for c in brokers])
        self.assertEqual(brokers[0]["broker"]["brokerIp2"] + ":10912", brokers[1]["store"]["haMasterAddress"])
        for service in [d for d in documents if d["kind"] == "Service"]:
            if service["metadata"]["name"].endswith("-peer"):
                self.assertEqual("None", service["spec"]["clusterIP"])
                self.assertTrue(service["spec"]["publishNotReadyAddresses"])
            else:
                self.assertEqual(["service"], [p["name"] for p in service["spec"]["ports"]])

    def test_tls_and_acl_reference_secrets_and_preserve_ca_projection(self):
        documents = render("values-production-proxy-tls.yaml", self.helm,
                           "services.proxy.tls.clientAuth=require", "services.proxy.auth.enabled=true",
                           "services.proxy.auth.secretName=proxy-acl")
        config = next(c for s, _, _, c in configurations(documents) if s == "proxy")
        self.assertEqual("0.0.0.0:8081", config["grpc"]["listenAddr"])
        self.assertEqual("/etc/rocketmq/tls/tls.key", config["grpc"]["tls"]["privateKeyPath"])
        self.assertEqual("/etc/rocketmq/tls/ca.crt", config["grpc"]["tls"]["clientCaPath"])
        self.assertTrue(config["auth"]["authenticationEnabled"])
        self.assertEqual("/etc/rocketmq/acl/plain_acl.yml", config["auth"]["aclFile"])
        pod = next(d for d in documents if d["kind"] == "Deployment")["spec"]["template"]["spec"]
        secrets = {v["name"]: v["secret"] for v in pod["volumes"] if "secret" in v}
        self.assertEqual("proxy-acl", secrets["acl"]["secretName"])
        self.assertIn({"key": "ca.crt", "path": "ca.crt"}, secrets["proxy-tls"]["items"])

    def test_invalid_cross_field_values_fail_before_deployment(self):
        for override in ["services.broker.controllerMode=true", "services.controller.enabled=true,services.controller.replicas=2",
                         "services.broker.port=8088", "services.broker.port=65536", "services.broker.minInSyncReplicas=2",
                         "services.proxy.cluster.controlReserve=256", "services.proxy.cluster.commandQueueCapacity=10",
                         "services.proxy.tls.enabled=true", "services.broker.auth.enabled=true",
                         "runtime.terminationGracePeriodSeconds=45", "services.broker.resources.requests.cpu=3",
                         "services.broker.resources.requests.memory=3Gi"]:
            with self.subTest(override=override), self.assertRaises(subprocess.CalledProcessError):
                render("values-dev-single.yaml", self.helm, override)

    def test_client_access_and_dns_have_valid_network_selectors(self):
        documents = render("values-dev-single.yaml", self.helm, "networkPolicy.clientNamespaceSelector.team=messaging")
        policies = {d["metadata"]["name"]: d for d in documents if d["kind"] == "NetworkPolicy"}
        selector = policies["core-clients"]["spec"]["ingress"][0]["from"][0]["namespaceSelector"]
        self.assertEqual({"matchLabels": {"team": "messaging"}}, selector)
        dns = policies["core-core"]["spec"]["egress"][0]
        self.assertEqual({"TCP", "UDP"}, {p["protocol"] for p in dns["ports"]})
        self.assertEqual({53}, {p["port"] for p in dns["ports"]})


if __name__ == "__main__":
    unittest.main()
