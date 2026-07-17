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

import hashlib
import json
import re
import subprocess
import tomllib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
CORE = ROOT / "rocketmq-proxy-core"
CLUSTER = ROOT / "rocketmq-proxy-cluster"
LOCAL = ROOT / "rocketmq-proxy-local"
FACADE = ROOT / "rocketmq-proxy"
POLICY = ROOT / "scripts" / "architecture-dependency-policy.json"
PLAN_ROOT = ROOT / "docs" / "plans" / "architecture-refactor-migration"

EXPECTED_SCHEMA_HASHES = {
    "definition.proto": "28706c9d2dee01dadf54daaf7a070a1ab4b30172f284ffb2f8189569f13ac2c1",
    "service.proto": "b7e1026b16c2284921a4d11a0b348074df82a15a8399c43f00e6005142bb5128",
}
EXPECTED_INTERNAL_DEPENDENCIES = {
    "rocketmq-error",
    "rocketmq-model",
    "rocketmq-protocol",
    "rocketmq-runtime",
    "rocketmq-transport",
}
FORBIDDEN_CORE_TOKENS = (
    "rocketmq_auth",
    "rocketmq_broker",
    "rocketmq_client_rust",
    "rocketmq_common",
    "rocketmq_remoting",
    "rocketmq_rust",
    "rocketmq_store",
)
FORBIDDEN_CORE_PACKAGES = {
    "rocketmq-auth",
    "rocketmq-broker",
    "rocketmq-client-rust",
    "rocketmq-common",
    "rocketmq-proxy",
    "rocketmq-remoting",
    "rocketmq-store",
}
EXPECTED_CLUSTER_INTERNAL_DEPENDENCIES = {
    "rocketmq-client-rust",
    "rocketmq-error",
    "rocketmq-model",
    "rocketmq-protocol",
    "rocketmq-proxy-core",
    "rocketmq-runtime",
    "rocketmq-security-api",
}
FORBIDDEN_CLUSTER_TOKENS = (
    "rocketmq_auth",
    "rocketmq_broker",
    "rocketmq_common",
    "rocketmq_proxy_local",
    "rocketmq_remoting",
    "rocketmq_rust",
    "rocketmq_store",
)
FORBIDDEN_CLUSTER_PACKAGES = {
    "rocketmq-broker",
    "rocketmq-proxy-local",
    "rocketmq-store",
    "rocketmq-store-local",
    "rocketmq-store-rocksdb",
}
EXPECTED_LOCAL_INTERNAL_DEPENDENCIES = {
    "rocketmq-broker",
    "rocketmq-error",
    "rocketmq-model",
    "rocketmq-proxy-core",
    "rocketmq-runtime",
}
EXPECTED_FACADE_INTERNAL_DEPENDENCIES = {
    "rocketmq-auth",
    "rocketmq-common",
    "rocketmq-error",
    "rocketmq-model",
    "rocketmq-observability",
    "rocketmq-proxy-cluster",
    "rocketmq-proxy-core",
    "rocketmq-proxy-local",
    "rocketmq-remoting",
    "rocketmq-runtime",
}
FORBIDDEN_LOCAL_TOKENS = (
    "rocketmq_auth",
    "rocketmq_client_rust",
    "rocketmq_common",
    "rocketmq_proxy_cluster",
    "rocketmq_remoting",
    "rocketmq_rust",
    "rocketmq_store",
)


def load_manifest(path: Path) -> dict:
    return tomllib.loads(path.read_text(encoding="utf-8"))


class ProxyCoreContractTests(unittest.TestCase):
    def test_workspace_has_all_thirty_two_target_packages(self) -> None:
        root = load_manifest(ROOT / "Cargo.toml")
        members = root["workspace"]["members"]
        policy = json.loads(POLICY.read_text(encoding="utf-8"))

        self.assertEqual(32, len(members))
        self.assertIn("rocketmq-proxy-core", members)
        self.assertIn("rocketmq-proxy-cluster", members)
        self.assertIn("rocketmq-proxy-local", members)
        self.assertEqual(set(), set(policy["planned_packages"]) - set(members))
        self.assertEqual("./rocketmq-proxy-core", root["workspace"]["dependencies"]["rocketmq-proxy-core"]["path"])
        self.assertEqual(
            "./rocketmq-proxy-cluster",
            root["workspace"]["dependencies"]["rocketmq-proxy-cluster"]["path"],
        )
        self.assertEqual("./rocketmq-proxy-local", root["workspace"]["dependencies"]["rocketmq-proxy-local"]["path"])

    def test_proto_schema_hashes_and_generation_owner_are_unique(self) -> None:
        for name, expected in EXPECTED_SCHEMA_HASHES.items():
            schema = CORE / "proto" / name
            self.assertTrue(schema.is_file(), schema)
            self.assertEqual(expected, hashlib.sha256(schema.read_bytes()).hexdigest())

        self.assertTrue((CORE / "build.rs").is_file())
        self.assertFalse((FACADE / "build.rs").exists())
        self.assertFalse((FACADE / "proto").exists())

        build_owners = []
        include_owners = []
        for crate in ROOT.glob("rocketmq-proxy*"):
            for path in crate.rglob("*.rs"):
                source = path.read_text(encoding="utf-8")
                if "compile_protos" in source:
                    build_owners.append(path.relative_to(ROOT).as_posix())
                if 'include_proto!("apache.rocketmq.v2")' in source:
                    include_owners.append(path.relative_to(ROOT).as_posix())
        self.assertEqual(["rocketmq-proxy-core/build.rs"], build_owners)
        self.assertEqual(["rocketmq-proxy-core/src/proto.rs"], include_owners)

    def test_core_dependency_and_source_closure_is_neutral(self) -> None:
        manifest = load_manifest(CORE / "Cargo.toml")
        internal = {name for name in manifest["dependencies"] if name.startswith("rocketmq-")}
        self.assertEqual(EXPECTED_INTERNAL_DEPENDENCIES, internal)
        self.assertEqual([], manifest["features"]["default"])

        source = "\n".join(path.read_text(encoding="utf-8") for path in (CORE / "src").rglob("*.rs"))
        for token in FORBIDDEN_CORE_TOKENS:
            self.assertNotIn(token, source, token)

        cargo_tree = subprocess.run(
            [
                "cargo",
                "tree",
                "--locked",
                "-p",
                "rocketmq-proxy-core",
                "-e",
                "normal",
                "--prefix",
                "none",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        packages = {line.split()[0] for line in cargo_tree.splitlines() if line.strip()}
        self.assertEqual(set(), FORBIDDEN_CORE_PACKAGES & packages)

    def test_target_gap_ledger_stays_exact_and_proxy_adapters_have_no_findings(self) -> None:
        guard = subprocess.run(
            [
                "python",
                "scripts/architecture_dependency_guard.py",
                "--mode",
                "target",
                "--allow-missing-planned-crates",
            ],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(1, guard.returncode)
        output = guard.stdout + guard.stderr
        findings = [line for line in output.splitlines() if line.startswith("VIOLATION ")]
        self.assertEqual(48, len(findings))
        self.assertEqual(0, sum("rule=client-manifest-allowlist" in line for line in findings))
        self.assertEqual(0, sum("rule=client-source-allowlist" in line for line in findings))
        self.assertEqual(46, sum("rule=target-dag-direct-dependency" in line for line in findings))
        self.assertEqual(2, sum("rule=transitive-forbidden-reachability" in line for line in findings))
        self.assertFalse(any("caller=rocketmq-proxy-core " in line for line in findings))
        self.assertFalse(any("caller=rocketmq-proxy-cluster " in line for line in findings))
        self.assertFalse(any("caller=rocketmq-proxy-local " in line for line in findings))
        self.assertNotIn("TARGET_INCOMPLETE", output)

    def test_cluster_dependency_source_and_runtime_closure_is_owned(self) -> None:
        manifest = load_manifest(CLUSTER / "Cargo.toml")
        internal = {name for name in manifest["dependencies"] if name.startswith("rocketmq-")}
        self.assertEqual(EXPECTED_CLUSTER_INTERNAL_DEPENDENCIES, internal)
        self.assertEqual([], manifest["features"]["default"])

        source = "\n".join(path.read_text(encoding="utf-8") for path in (CLUSTER / "src").rglob("*.rs"))
        for token in FORBIDDEN_CLUSTER_TOKENS:
            self.assertNotIn(token, source, token)
        for token in (
            "ActorRuntime",
            "ArcMut",
            "spawn_current_thread",
            "mpsc::unbounded_channel",
            "TaskGroup::root",
        ):
            self.assertNotIn(token, source, token)

        client_source = (ROOT / "rocketmq-client" / "src" / "lib.rs").read_text(encoding="utf-8")
        client_runtime_source = "\n".join(
            path.read_text(encoding="utf-8") for path in (ROOT / "rocketmq-client" / "src").rglob("*.rs")
        )
        self.assertIn("pub struct ClientInstanceHandle", client_source)
        self.assertNotIn("pub type ClientInstanceHandle", client_source)
        handle = re.search(
            r"pub struct ClientInstanceHandle\s*\{(?P<body>.*?)\n\s*\}",
            client_source,
            re.DOTALL,
        )
        self.assertIsNotNone(handle)
        self.assertNotIn("pub inner", handle.group("body"))
        self.assertIsNone(
            re.search(
                r"impl\s+(?:std::ops::)?Deref(?:Mut)?\s+for\s+ClientInstanceHandle",
                client_source,
            )
        )
        self.assertNotIn("get_mq_client_api_impl", source)
        for lifecycle_contract in (
            "client_config_for_managed_domain",
            "ShutdownDeadline::after",
            "shutdown_deadline.remaining()",
            'service_context.child("proxy.cluster.adapter")',
            "producer.shutdown()",
            "client.shutdown()",
        ):
            self.assertIn(lifecycle_contract, source, lifecycle_contract)
        worker_cleanup = re.search(
            r"let shutdown_deadline = ShutdownDeadline::after.*?(?=\n\}\n\nfn cluster_client_config)",
            source,
            re.DOTALL,
        )
        self.assertIsNotNone(worker_cleanup)
        self.assertLess(
            worker_cleanup.group(0).index("producer.shutdown()"),
            worker_cleanup.group(0).index("client.shutdown()"),
        )
        for client_ownership_contract in (
            "ManagedClientEntry",
            "remove_client_factory_if_same",
            "managed_client_isolated_across_runtime_domains",
            "owned_partial_start_shutdown_cleans_starting_and_failed_states",
        ):
            self.assertIn(client_ownership_contract, client_runtime_source, client_ownership_contract)

        cargo_tree = subprocess.run(
            ["cargo", "tree", "--locked", "-p", "rocketmq-proxy-cluster", "-e", "normal", "--prefix", "none"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        packages = {line.split()[0] for line in cargo_tree.splitlines() if line.strip()}
        self.assertEqual(set(), FORBIDDEN_CLUSTER_PACKAGES & packages)

        facade_manifest = load_manifest(FACADE / "Cargo.toml")
        self.assertIn("rocketmq-proxy-cluster", facade_manifest["dependencies"])
        self.assertNotIn("rocketmq-client-rust", facade_manifest["dependencies"])
        facade_source = "\n".join(path.read_text(encoding="utf-8") for path in (FACADE / "src").rglob("*.rs"))
        self.assertNotIn("rocketmq_client_rust", facade_source)

        runtime_audit = (ROOT / "scripts" / "runtime-audit.ps1").read_text(encoding="utf-8-sig")
        self.assertIn('^rocketmq-proxy(?:-core|-cluster|-local)?/src/', runtime_audit)

    def test_local_dependency_source_and_runtime_closure_is_owned(self) -> None:
        manifest = load_manifest(LOCAL / "Cargo.toml")
        internal = {name for name in manifest["dependencies"] if name.startswith("rocketmq-")}
        self.assertEqual(EXPECTED_LOCAL_INTERNAL_DEPENDENCIES, internal)
        self.assertEqual([], manifest["features"]["default"])
        self.assertEqual(["rocketmq-broker/otel-metrics"], manifest["features"]["observability"])
        self.assertEqual(["rocketmq-broker/tieredstore"], manifest["features"]["tieredstore"])

        source = "\n".join(path.read_text(encoding="utf-8") for path in (LOCAL / "src").rglob("*.rs"))
        for token in FORBIDDEN_LOCAL_TOKENS:
            self.assertNotIn(token, source, token)
        for token in (
            "ActorRuntime",
            "ArcMut",
            "spawn_current_thread",
            "mpsc::unbounded_channel",
            "tokio::spawn",
            "TaskGroup::root",
        ):
            self.assertNotIn(token, source, token)
        for lifecycle_contract in (
            "ServiceContext",
            "ShutdownDeadline::after",
            'service_context.child("proxy.local.adapter")',
            "cancellation.cancelled()",
            "facade.shutdown()",
            "LOCAL_COMMAND_CAPACITY",
        ):
            self.assertIn(lifecycle_contract, source, lifecycle_contract)

        cargo_tree = subprocess.run(
            ["cargo", "tree", "--locked", "-p", "rocketmq-proxy-local", "-e", "normal", "--prefix", "none"],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        packages = {line.split()[0] for line in cargo_tree.splitlines() if line.strip()}
        self.assertNotIn("rocketmq-client-rust", packages)
        self.assertNotIn("rocketmq-proxy-cluster", packages)

        facade_manifest = load_manifest(FACADE / "Cargo.toml")
        self.assertIn("rocketmq-proxy-local", facade_manifest["dependencies"])
        self.assertNotIn("rocketmq-broker", facade_manifest["dependencies"])
        self.assertNotIn("rocketmq-store", facade_manifest["dependencies"])

        local_wrapper = (FACADE / "src" / "local.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_local::local::*;", local_wrapper)
        self.assertLess(len(local_wrapper.splitlines()), 30)
        config = (FACADE / "src" / "config.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_local::LocalConfig;", config)
        self.assertNotIn("pub struct LocalConfig", config)
        service = (FACADE / "src" / "service.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_local::service::*;", service)
        self.assertNotIn("pub struct LocalServiceManager", service)

    def test_facade_modules_are_reexports_of_the_canonical_owners(self) -> None:
        wrappers = {
            "error.rs": "rocketmq_proxy_core::error::*",
            "proto.rs": "rocketmq_proxy_core::proto::*",
            "status.rs": "rocketmq_proxy_core::status::*",
        }
        for name, expected in wrappers.items():
            source = (FACADE / "src" / name).read_text(encoding="utf-8")
            self.assertIn(f"pub use {expected};", source)
            self.assertLess(len(source.splitlines()), 30)

        context = (FACADE / "src" / "context.rs").read_text(encoding="utf-8")
        self.assertIn(
            "rocketmq_proxy_core::context::ProxyContextWithPrincipal<crate::auth::AuthenticatedPrincipal>",
            context,
        )
        self.assertNotIn("pub use rocketmq_proxy_core::context::*", context)

        auth = (FACADE / "src" / "auth.rs").read_text(encoding="utf-8")
        self.assertIn("pub struct AuthenticatedPrincipal", auth)
        self.assertIn("fn white_listed(", auth)
        self.assertNotIn("pub fn white_listed(", auth)
        self.assertNotIn("pub fn new(username: String, source_ip: String", auth)

        session = (FACADE / "src" / "session.rs").read_text(encoding="utf-8")
        self.assertIn("rocketmq_proxy_core::session::ClientSessionRegistry<Channel>", session)
        self.assertNotIn("struct ClientSessionRegistry", session)

        facade_manifest = load_manifest(FACADE / "Cargo.toml")
        self.assertIn("rocketmq-proxy-core", facade_manifest["dependencies"])
        self.assertNotIn("build-dependencies", facade_manifest)

        cluster = (FACADE / "src" / "cluster.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_cluster::cluster::*;", cluster)
        self.assertLess(len(cluster.splitlines()), 30)

        config = (FACADE / "src" / "config.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_cluster::ClusterConfig;", config)
        self.assertNotIn("pub struct ClusterConfig", config)

        service = (FACADE / "src" / "service.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_cluster::service::*;", service)
        self.assertNotIn("pub struct ClusterServiceManager", service)

    def test_r0_facade_manifest_and_source_are_composition_only(self) -> None:
        manifest = load_manifest(FACADE / "Cargo.toml")
        internal = {name for name in manifest["dependencies"] if name.startswith("rocketmq-")}
        self.assertEqual(EXPECTED_FACADE_INTERNAL_DEPENDENCIES, internal)
        self.assertEqual([], manifest["features"]["default"])

        for adapter in ("rocketmq-proxy-core", "rocketmq-proxy-cluster", "rocketmq-proxy-local"):
            dependency = manifest["dependencies"][adapter]
            self.assertNotEqual(True, dependency.get("optional", False), adapter)

        for next_major_feature in ("cluster-mode", "local-mode", "compat-all-modes"):
            self.assertNotIn(next_major_feature, manifest["features"])

        for forbidden_dependency in (
            "rocketmq-broker",
            "rocketmq-client-rust",
            "rocketmq-rust",
            "rocketmq-store",
        ):
            self.assertNotIn(forbidden_dependency, manifest["dependencies"])

        source = "\n".join(path.read_text(encoding="utf-8") for path in (FACADE / "src").rglob("*.rs"))
        for forbidden_token in (
            "rocketmq_broker",
            "rocketmq_client_rust",
            "rocketmq_rust",
            "rocketmq_store::",
        ):
            self.assertNotIn(forbidden_token, source, forbidden_token)

        config = (FACADE / "src" / "config.rs").read_text(encoding="utf-8")
        for normalized_owner in (
            "rocketmq_proxy_core::config::GrpcConfig",
            "rocketmq_proxy_core::config::RemotingConfig",
            "rocketmq_proxy_core::config::RuntimeConfig",
            "rocketmq_proxy_core::config::SessionConfig",
            "rocketmq_proxy_cluster::ClusterConfig",
            "rocketmq_proxy_local::LocalConfig",
        ):
            self.assertIn(normalized_owner, config, normalized_owner)

        bootstrap = (FACADE / "src" / "bootstrap.rs").read_text(encoding="utf-8")
        for composition_contract in (
            "default_service_manager_and_backend",
            "config.cluster.clone()",
            "config.local.clone()",
            "ClusterServiceManager::from_cluster_client",
            "local_components_from_config_with_service_context",
        ):
            self.assertIn(composition_contract, bootstrap, composition_contract)

        owner_declarations = {
            "processor.rs": ("pub trait MessagingProcessor", "pub struct DefaultMessagingProcessor"),
            "service.rs": ("pub trait ServiceManager", "pub struct ClusterServiceManager", "pub struct LocalServiceManager"),
            "cluster.rs": ("pub trait ClusterClient", "pub struct RocketmqClusterClient"),
            "local.rs": ("pub struct LocalServiceManager",),
        }
        for name, declarations in owner_declarations.items():
            wrapper = (FACADE / "src" / name).read_text(encoding="utf-8")
            for declaration in declarations:
                self.assertNotIn(declaration, wrapper, f"{name}: {declaration}")

        core_source = "\n".join(path.read_text(encoding="utf-8") for path in (CORE / "src").rglob("*.rs"))
        local_source = "\n".join(path.read_text(encoding="utf-8") for path in (LOCAL / "src").rglob("*.rs"))
        for reverse_token in ("rocketmq_proxy::", "rocketmq_proxy_cluster", "rocketmq_client_rust"):
            self.assertNotIn(reverse_token, core_source, reverse_token)
        for reverse_token in ("rocketmq_proxy::", "rocketmq_proxy_cluster", "rocketmq_client_rust"):
            self.assertNotIn(reverse_token, local_source, reverse_token)

    def test_core_owns_error_status_context_session_identity_and_ingress_config(self) -> None:
        for name in ("config.rs", "context.rs", "error.rs", "identity.rs", "proto.rs", "session.rs", "status.rs"):
            self.assertTrue((CORE / "src" / name).is_file(), name)

        config = (FACADE / "src" / "config.rs").read_text(encoding="utf-8")
        for type_name in ("GrpcConfig", "ProxyMode", "RemotingConfig", "RuntimeConfig", "SessionConfig"):
            self.assertIn(f"pub use rocketmq_proxy_core::config::{type_name};", config)
            self.assertNotIn(f"pub struct {type_name}", config)
            self.assertNotIn(f"pub enum {type_name}", config)

        service = (FACADE / "src" / "service.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_core::ResourceIdentity;", service)
        self.assertNotIn("pub struct ResourceIdentity", service)

        error_guard = (ROOT / "scripts" / "error_architecture_guard.py").read_text(encoding="utf-8")
        self.assertIn(
            'PROXY_STATUS_MAPPER = ROOT / "rocketmq-proxy-core" / "src" / "status.rs"',
            error_guard,
        )
        self.assertNotIn('ROOT / "rocketmq-proxy" / "src" / "status.rs"', error_guard)

    def test_core_owns_neutral_plans_ports_services_and_ingress(self) -> None:
        for name in ("message.rs", "processor.rs", "service.rs", "remoting.rs", "grpc.rs"):
            self.assertTrue((CORE / "src" / name).is_file(), name)
        for name in (
            "adapter.rs",
            "middleware.rs",
            "server.rs",
            "service/admission.rs",
            "service/consumer.rs",
            "service/housekeeping.rs",
            "service/producer.rs",
            "service/telemetry.rs",
            "service/topic.rs",
            "service/transaction.rs",
        ):
            self.assertTrue((CORE / "src" / "grpc" / name).is_file(), name)

        processor = (CORE / "src" / "processor.rs").read_text(encoding="utf-8")
        for type_name in (
            "SendMessagePlan",
            "PullMessagePlan",
            "AckMessagePlan",
            "QueryRoutePlan",
            "EndTransactionPlan",
            "MessagingProcessor",
            "DefaultMessagingProcessor",
        ):
            self.assertIn(type_name, processor)

        service = (CORE / "src" / "service.rs").read_text(encoding="utf-8")
        for trait_name in (
            "RouteService",
            "MetadataService",
            "AssignmentService",
            "MessageService",
            "ConsumerService",
            "TransactionService",
            "ServiceManager",
        ):
            self.assertIn(f"pub trait {trait_name}", service)

        facade_processor = (FACADE / "src" / "processor.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_core::processor::*;", facade_processor)
        self.assertLess(len(facade_processor.splitlines()), 30)
        facade_service = (FACADE / "src" / "service.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_core::service::*;", facade_service)
        for trait_name in ("RouteService", "MessageService", "ConsumerService", "TransactionService"):
            self.assertNotIn(f"pub trait {trait_name}", facade_service)

        facade_adapter = (FACADE / "src" / "grpc" / "adapter.rs").read_text(encoding="utf-8")
        self.assertIn("pub use rocketmq_proxy_core::grpc::adapter::*;", facade_adapter)
        self.assertLess(len(facade_adapter.splitlines()), 60)
        facade_middleware = (FACADE / "src" / "grpc" / "middleware.rs").read_text(encoding="utf-8")
        self.assertIn("rocketmq_proxy_core::grpc::middleware::ingress_context_interceptor", facade_middleware)
        self.assertLess(len(facade_middleware.splitlines()), 30)

        core_server = (CORE / "src" / "grpc" / "server.rs").read_text(encoding="utf-8")
        facade_server = (FACADE / "src" / "grpc" / "server.rs").read_text(encoding="utf-8")
        self.assertIn("pub async fn serve_with_lifecycle", core_server)
        self.assertIn("rocketmq_proxy_core::grpc::server::serve_with_lifecycle", facade_server)

        core_remoting = (CORE / "src" / "remoting.rs").read_text(encoding="utf-8")
        self.assertIn("pub trait ProxyRemotingBackend", core_remoting)
        self.assertIn("pub enum RemotingIngressRoute", core_remoting)
        self.assertIn("pub struct RemotingStatusMapper", core_remoting)
        self.assertNotIn("initialize_client_instance", core_remoting)

        facade_grpc_service = (FACADE / "src" / "grpc" / "service.rs").read_text(encoding="utf-8")
        self.assertNotIn("crate::cluster", facade_grpc_service)
        self.assertIn("transaction_producer_group", facade_grpc_service)

    def test_migration_checklists_record_completion_and_next_slice(self) -> None:
        checklist = (PLAN_ROOT / "CHECKLIST.md").read_text(encoding="utf-8")
        task = (
            PLAN_ROOT
            / "phase-2-core-boundaries"
            / "08-proxy-three-way-split.md"
        ).read_text(encoding="utf-8")
        handoff = (
            PLAN_ROOT
            / "phase-2-core-boundaries"
            / "07-client-edge-closeout-handoff.md"
        ).read_text(encoding="utf-8")
        readme = (PLAN_ROOT / "README.md").read_text(encoding="utf-8")

        self.assertIn("| PR 级工作包 | 53 | 0 | 29 未开始；合计 29 尚未完成 | 82 |", checklist)
        self.assertIn("- [x] PR-M08-01：创建 `rocketmq-proxy-core` 与 proto owner", checklist)
        self.assertIn("- [x] PR-M08-02：迁移中立 plan、port、service 与 ingress", checklist)
        self.assertIn("- [x] PR-M08-03：创建 Cluster adapter", checklist)
        self.assertIn("- [x] PR-M08-04：创建 Local adapter", checklist)
        self.assertIn("- [x] PR-M08-05：将现有 Proxy 降为 composition/facade", checklist)
        self.assertIn("- [x] PR-M08-06：验证 feature closure 与下一 major fixture", checklist)
        self.assertIn("当前下一工作包为 PR-M09-01", checklist)
        work_packages = re.findall(r"^- \[([ x])\] (PR-M\d{2}-\d{2}[a-z]?)：", checklist, re.MULTILINE)
        self.assertEqual(82, len(work_packages))
        self.assertEqual(53, sum(state == "x" for state, _ in work_packages))
        self.assertEqual(29, sum(state == " " for state, _ in work_packages))
        open_by_milestone: dict[str, int] = {}
        for state, package in work_packages:
            if state == " ":
                milestone = package.split("-")[1]
                open_by_milestone[milestone] = open_by_milestone.get(milestone, 0) + 1
        self.assertEqual({"M09": 6, "M10": 5, "M11": 12, "M12": 6}, open_by_milestone)
        self.assertIn("Local 8 项", checklist)
        self.assertIn("PR-M08-06 与 M08 Gate 已签署，下一工作包为 PR-M09-01", task)
        self.assertIn("- [x] `[DEV]` 按 send/pull/pop/ack/route/transaction 迁 plan 和 port", task)
        self.assertIn("## 12. PR-M08-04 消费记录（2026-07-17）", handoff)
        self.assertIn("根 workspace 已达到目标 32 个 package", readme)
        self.assertIn("下一工作包为 PR-M09-01", readme)


if __name__ == "__main__":
    unittest.main()
