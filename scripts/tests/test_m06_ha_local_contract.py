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

from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def source(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


class M06HALocalContractTests(unittest.TestCase):
    def test_transfer_and_ha_algorithms_have_one_local_owner(self) -> None:
        local_root = source("rocketmq-store-local/src/lib.rs")
        local_ha = source("rocketmq-store-local/src/ha.rs")
        local_transfer = source("rocketmq-store-local/src/transfer.rs")
        local_planner = source("rocketmq-store-local/src/transfer/planner.rs")
        local_segment = source("rocketmq-store-local/src/transfer/segment.rs")
        local_engine = source("rocketmq-store-local/src/ha/transfer_engine.rs")
        local_wire = source("rocketmq-store-local/src/ha/wire.rs")
        local_replication = source("rocketmq-store-local/src/ha/replication.rs")
        local_tests = source("rocketmq-store-local/tests/ha_transfer_boundary.rs")

        store_planner = source("rocketmq-store/src/transfer/planner.rs")
        store_segment = source("rocketmq-store/src/transfer/segment.rs")
        store_engine = source("rocketmq-store/src/ha/transfer_engine.rs")
        store_state = source("rocketmq-store/src/ha/ha_connection_state.rs")
        store_metrics = source("rocketmq-store/src/ha/transfer_metrics.rs")
        store_connection = source("rocketmq-store/src/ha/default_ha_connection.rs")
        store_connection_production = store_connection.split(
            "#[cfg(test)]", maxsplit=1
        )[0]
        store_client = source("rocketmq-store/src/ha/default_ha_client.rs")
        store_client_production = store_client.split("#[cfg(test)]", maxsplit=1)[0]
        store_local = source(
            "rocketmq-store/src/message_store/local_file_message_store.rs"
        )
        store_commit_log = source("rocketmq-store/src/log_file/commit_log.rs")
        store_mapped_file_queue = source(
            "rocketmq-store/src/consume_queue/mapped_file_queue.rs"
        )
        store_auto_switch = source(
            "rocketmq-store/src/ha/auto_switch/auto_switch_ha_service.rs"
        )
        store_auto_switch_production = store_auto_switch.split(
            "#[cfg(test)]", maxsplit=1
        )[0]
        store_ha_service = source("rocketmq-store/src/ha/ha_service.rs")
        store_general = source("rocketmq-store/src/ha/general_ha_service.rs")
        store_group = source("rocketmq-store/src/ha/group_transfer_service.rs")
        store_default_service = source("rocketmq-store/src/ha/default_ha_service.rs")
        store_default_service_production = store_default_service.split(
            "#[cfg(test)]", maxsplit=1
        )[0]

        self.assertIn("pub mod transfer;", local_root)
        self.assertIn("pub mod ha;", local_root)
        for module in ("batch", "error", "planner", "segment"):
            self.assertIn(f"pub mod {module};", local_transfer)
        for module in (
            "connection_state",
            "connection_state_notification_request",
            "error",
            "flow",
            "replication",
            "transfer_engine",
            "transfer_metrics",
            "wire",
        ):
            self.assertIn(f"pub mod {module};", local_ha)

        for owner in (
            "pub struct TransferPlanInput",
            "pub struct TransferPlanner",
            "pub const DEFAULT_TRANSFER_BATCH_SIZE",
        ):
            self.assertIn(owner, local_planner)
        for owner in (
            "pub enum SegmentSource",
            "pub struct CommitLogSegment",
            "pub struct SegmentLease",
        ):
            self.assertIn(owner, local_segment)
        self.assertIn("pub use rocketmq_store_local::transfer::planner::*;", store_planner)
        self.assertIn("pub use rocketmq_store_local::transfer::segment::*;", store_segment)
        self.assertNotIn("pub struct TransferPlanner", store_planner)
        self.assertNotIn("pub struct SegmentLease", store_segment)

        for owner in (
            "pub enum TransferEngineKind",
            "pub enum TransferEnginePreference",
            "pub struct TransferStats",
            "pub enum HaTransferEngine<W>",
        ):
            self.assertIn(owner, local_engine)
        self.assertIn("pub use rocketmq_store_local::ha::transfer_engine::*;", store_engine)
        self.assertIn(
            "pub use rocketmq_store_local::ha::connection_state::*;",
            store_state,
        )
        self.assertIn(
            "pub use rocketmq_store_local::ha::transfer_metrics::*;",
            store_metrics,
        )

        for owner in (
            "pub struct TransferHeader",
            "pub struct ReplicaFramePlan",
            "pub struct OffsetDecoder",
            "pub fn encode_transfer_header(",
            "pub fn decode_transfer_header(",
            "pub fn plan_replica_frame(",
            "pub const fn clamp_confirm_offset(",
        ):
            self.assertIn(owner, local_wire)
        self.assertIn("rocketmq_store_local::ha::wire::encode_transfer_header(", store_connection)
        self.assertIn("rocketmq_store_local::ha::wire::plan_replica_frame(", store_client)
        self.assertIn("rocketmq_store_local::ha::wire::encode_offset_report(", store_client)
        self.assertNotIn("pub(crate) struct TransferHeader", store_connection)
        self.assertNotIn("pub(in crate::ha) struct OffsetDecoder", store_connection)

        self.assertIn("inner: Arc<Inner>", store_client_production)
        self.assertNotIn("inner: ArcMut<Inner>", store_client_production)
        self.assertIn("let client = Arc::clone(&self.inner);", store_client_production)
        self.assertNotIn("ArcMut::clone(&self.inner)", store_client_production)
        for unused_runtime_field in (
            "write_stream:",
            "read_stream:",
            "dispatch_position:",
            "byte_buffer_read:",
            "byte_buffer_backup:",
        ):
            self.assertNotIn(unused_runtime_field, store_client_production)
        self.assertIn("buf: BytesMut", store_client_production)
        self.assertIn("report_offset: BytesMut", store_client_production)
        self.assertNotIn(
            "ArcMut<LocalFileMessageStore>", store_client_production
        )
        self.assertEqual(
            store_client_production.count(
                "\n    replica_store: HAReplicaStoreHandle,"
            ),
            2,
        )
        self.assertIn(
            "replica_store: HAReplicaStoreHandle",
            store_default_service_production,
        )
        self.assertNotIn(
            "ArcMut<LocalFileMessageStore>", store_default_service_production
        )
        self.assertIn(
            "connection_context: DefaultHAConnectionContext",
            store_default_service_production,
        )
        self.assertIn(
            "connections: Weak<Mutex<HashMap<HAConnectionId, GeneralHAConnection>>>",
            store_default_service_production,
        )
        self.assertIn(
            "group_transfer_service: OnceLock<Weak<GroupTransferService>>",
            store_default_service_production,
        )
        self.assertIn(
            "state_notification_service: OnceLock<Weak<HAConnectionStateNotificationService>>",
            store_default_service_production,
        )
        self.assertNotIn("ArcMut", store_connection_production)
        self.assertNotIn("DefaultHAService", store_connection_production)
        self.assertIn("fn init(this: &mut Self", store_default_service_production)
        self.assertNotIn(
            "fn init(this: &mut ArcMut<Self>", store_default_service_production
        )
        self.assertIn("delegate: Box<DefaultHAService>", store_auto_switch_production)
        self.assertNotIn(
            "delegate: ArcMut<DefaultHAService>", store_auto_switch_production
        )
        self.assertNotIn("ArcMut::new(delegate)", store_auto_switch_production)
        self.assertIn("pub(crate) struct HAReplicaStoreHandle", store_local)
        self.assertIn("commit_log: CommitLogReplicaHandle", store_local)
        for capability in (
            "master_flushed_offset: Arc<AtomicI64>",
            "alive_replica_num_in_group: Arc<AtomicI32>",
            "state_machine_version: Arc<AtomicI64>",
            "controller_epoch_start_offset: Arc<AtomicI64>",
            "pub(crate) fn select_segments(",
            "pub(crate) fn get_confirm_offset_directly(&self)",
        ):
            self.assertIn(capability, store_local)
        self.assertIn("pub(crate) struct CommitLogReplicaHandle", store_commit_log)
        for capability in (
            "append: MappedFileQueueAppendHandle",
            "put_message_lock: Arc<tokio::sync::Mutex<()>>",
            "runtime_state: Arc<CommitLogRuntimeState>",
            "store_checkpoint: Arc<StoreCheckpoint>",
        ):
            self.assertIn(capability, store_commit_log)
        self.assertIn(
            "pub(crate) struct MappedFileQueueAppendHandle",
            store_mapped_file_queue,
        )

        for owner in (
            "pub struct ReplicationStateRoot",
            "pub struct ReplicationProgress",
            "pub fn has_required_sync_state_set_acks(",
            "pub fn has_required_acks(",
            "pub fn compute_confirm_offset(",
        ):
            self.assertIn(owner, local_replication)
        self.assertIn("replication: Arc<ReplicationStateRoot>", store_auto_switch)
        self.assertIn("replication: Arc::new(ReplicationStateRoot::new(is_master))", store_auto_switch)
        self.assertNotIn("struct SyncStateTracker", store_auto_switch)
        self.assertNotIn("fn has_required_acks(", store_group)
        self.assertIn(
            "pub(crate) use rocketmq_store_local::ha::replication::HAAckedReplicaSnapshot;",
            store_ha_service,
        )
        self.assertIn("use crate::ha::ha_service::HAAckedReplicaSnapshot;", store_general)
        self.assertIn("ReplicationProgress", store_default_service)
        self.assertNotIn("push2_slave_max_offset", store_default_service)

        for test_name in (
            "local_planner_owns_offset_resolution_flow_budget_and_file_boundary",
            "local_vectored_engine_preserves_frame_order_across_partial_writes",
        ):
            self.assertIn(f"fn {test_name}()", local_tests)

    def test_store_keeps_runtime_and_protocol_adapters_outside_local(self) -> None:
        local_manifest = source("rocketmq-store-local/Cargo.toml")
        local_sources = "\n".join(
            source(str(path.relative_to(ROOT)).replace("\\", "/"))
            for path in sorted((ROOT / "rocketmq-store-local/src/ha").rglob("*.rs"))
        )
        store_group = source("rocketmq-store/src/ha/group_transfer_service.rs")
        store_connection = source("rocketmq-store/src/ha/default_ha_connection.rs")
        store_api = source("rocketmq-store-api/src/lib.rs")

        self.assertNotIn("rocketmq-remoting", local_manifest)
        self.assertNotIn("rocketmq-common", local_manifest)
        self.assertNotIn("rocketmq-rust", local_manifest)
        self.assertNotIn("rocketmq_remoting", local_sources)
        self.assertNotIn("rocketmq_common", local_sources)
        self.assertNotIn("LocalFileMessageStore", local_sources)
        self.assertIn("ServiceManager", store_group)
        self.assertIn("ServiceContext", store_group)
        self.assertIn("rocketmq_runtime::TaskGroup", store_connection)
        self.assertNotIn("tokio::spawn", local_sources)
        self.assertNotIn("HAConnectionState", store_api)
        self.assertNotIn("TransferEngineKind", store_api)


if __name__ == "__main__":
    unittest.main()
