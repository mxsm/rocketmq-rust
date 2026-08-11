// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use rocketmq_model::common::broker::broker_role::BrokerRole;
use rocketmq_model::common::message::message_ext_broker_inner::MessageExtBrokerInner;
use rocketmq_model::common::message::MessageConst;
use rocketmq_model::common::message::MessageTrait;
use rocketmq_model::common::message::MessageVersion;
use rocketmq_model::utils::crc32_utils::crc32_bytes;
use rocketmq_store_local::commit_log::runtime_state::CommitLogRuntimeState;
use tracing::Instrument;

use crate::base::message_encoder_pool;
use crate::base::message_result::PutMessageResult;
use crate::base::select_result::SelectMappedBufferResult;
use crate::base::store_checkpoint::StoreCheckpoint;
use crate::config::flush_disk_type::FlushDiskType;
use crate::config::message_store_config::MessageStoreConfig;
use crate::config::store_runtime_config::StoreRuntimeConfig;
use crate::consume_queue::mapped_file_queue::MappedFileQueueAppendHandle;
use crate::consume_queue::mapped_file_queue::MappedFileQueueCleanupHandle;
use crate::consume_queue::mapped_file_queue::MappedFileQueueReadHandle;
use crate::log_file::mapped_file::MappedFile;
use crate::message_store::runtime_state::StoreRuntimeState;
use crate::store_error::StoreError;
use crate::store_error::StoreOperation;
use crate::transfer::error::TransferError;
use crate::transfer::error::TransferResult;
use crate::transfer::segment::SegmentLease;

use super::append_sequencer::CommitLogAppendPort;
use super::CommitLogStoreContext;

/// Safe, narrow capability for scheduled commit-log retention work.
#[derive(Clone)]
pub(crate) struct CommitLogCleanupHandle {
    pub(super) mapped_file_queue: MappedFileQueueCleanupHandle,
}

impl CommitLogCleanupHandle {
    #[inline]
    pub(crate) fn get_min_offset(&self) -> i64 {
        self.mapped_file_queue.get_min_offset()
    }

    pub(crate) fn delete_expired_files_by_time_before(
        &self,
        expired_time: i64,
        delete_files_interval: i32,
        interval_forcibly: i64,
        clean_immediately: bool,
        delete_file_batch_max: i32,
        pinned_file_offset: Option<u64>,
    ) -> i32 {
        self.mapped_file_queue.delete_expired_files_by_time_before(
            expired_time,
            delete_files_interval,
            interval_forcibly,
            clean_immediately,
            delete_file_batch_max,
            pinned_file_offset,
        )
    }

    #[inline]
    pub(crate) fn retry_delete_first_file(&self, interval_forcibly: i64) -> bool {
        self.mapped_file_queue.retry_delete_first_file(interval_forcibly)
    }
}

/// Safe, cloneable capability for long-lived commit-log readers.
#[derive(Clone)]
pub(crate) struct CommitLogReadHandle {
    pub(super) mapped_file_queue: MappedFileQueueReadHandle,
    pub(super) message_store_config: Arc<MessageStoreConfig>,
    pub(super) store_runtime_state: Arc<StoreRuntimeState>,
    pub(super) broker_config: Arc<StoreRuntimeConfig>,
    pub(super) store_context: CommitLogStoreContext,
    pub(super) runtime_state: Arc<CommitLogRuntimeState>,
}

impl CommitLogReadHandle {
    pub(crate) fn get_message(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        self.mapped_file_queue.get_message(offset, size)
    }

    pub(crate) fn get_message_for_transfer(&self, offset: i64, size: i32) -> Option<SelectMappedBufferResult> {
        self.mapped_file_queue.get_message_for_transfer(offset, size)
    }

    pub(crate) fn get_bulk_data(&self, offset: i64, size: i32) -> Option<Vec<SelectMappedBufferResult>> {
        self.mapped_file_queue.get_bulk_data(offset, size)
    }

    pub(crate) fn get_data(&self, offset: i64) -> Option<SelectMappedBufferResult> {
        self.mapped_file_queue.get_data(offset)
    }

    pub(crate) fn get_data_bounded(&self, offset: i64, max_bytes: usize) -> Option<SelectMappedBufferResult> {
        self.mapped_file_queue.get_data_bounded(offset, max_bytes)
    }

    pub(crate) fn get_max_offset(&self) -> i64 {
        self.mapped_file_queue.get_max_offset()
    }

    pub(crate) fn get_min_offset(&self) -> i64 {
        self.mapped_file_queue.get_min_offset()
    }

    pub(crate) fn get_flushed_where(&self) -> i64 {
        self.mapped_file_queue.get_flushed_where()
    }

    pub(crate) fn pickup_store_timestamp(&self, offset: i64, size: i32) -> i64 {
        if offset < self.get_min_offset() || offset + size as i64 > self.get_max_offset() {
            return -1;
        }
        self.get_message(offset, size)
            .map(|result| rocketmq_store_local::commit_log::header::store_timestamp_from_frame(result.get_buffer()))
            .unwrap_or(-1)
    }

    pub(crate) fn check_self(&self) {
        self.mapped_file_queue.check_self();
    }

    pub(crate) fn roll_next_file(&self, offset: i64) -> i64 {
        self.mapped_file_queue.roll_next_file(offset)
    }

    pub(crate) fn get_confirm_offset(&self) -> i64 {
        resolve_commit_log_confirm_offset(
            self.message_store_config.as_ref(),
            self.store_runtime_state.broker_role(),
            self.broker_config.as_ref(),
            &self.store_context,
            self.runtime_state.confirm_offset(),
            self.mapped_file_queue.get_max_offset(),
            self.mapped_file_queue.get_flushed_where(),
        )
    }

    pub(crate) fn get_confirm_offset_directly(&self) -> i64 {
        if self.broker_config.enable_controller_mode {
            if self.store_runtime_state.broker_role() != BrokerRole::Slave
                && !self.store_context.running_flags.is_fenced()
            {
                let max_phy_offset = self.get_max_offset();
                if let Some(ha_service) = self.store_context.ha_service() {
                    if ha_service.local_sync_state_set_size(max_phy_offset) <= 1 {
                        return max_phy_offset;
                    }
                }
            }

            self.runtime_state.confirm_offset()
        } else if self.broker_config.duplication_enable {
            self.runtime_state.confirm_offset()
        } else {
            self.get_max_offset()
        }
    }

    pub(crate) fn select_segments(
        &self,
        offset: i64,
        max_bytes: usize,
        allow_cross_file: bool,
    ) -> TransferResult<Vec<SegmentLease>> {
        if offset < 0 {
            return Err(TransferError::InvalidInput(format!(
                "offset must be non-negative: {offset}"
            )));
        }
        if max_bytes == 0 {
            return Ok(Vec::new());
        }
        let mapped_file_size = self.message_store_config.mapped_file_size_commit_log;
        if mapped_file_size == 0 {
            return Err(TransferError::InvalidInput(
                "mapped_file_size_commit_log must be greater than zero".to_string(),
            ));
        }

        let mut max_bytes = max_bytes.min(i32::MAX as usize);
        if !allow_cross_file {
            let position_in_file = offset.rem_euclid(mapped_file_size as i64) as usize;
            max_bytes = max_bytes.min(mapped_file_size.saturating_sub(position_in_file));
        }

        let Some(results) = self.mapped_file_queue.get_bulk_transfer_data(offset, max_bytes as i32) else {
            return Ok(Vec::new());
        };
        Ok(results.into_iter().filter_map(SegmentLease::from_selection).collect())
    }
}

/// Safe, cloneable append capability for Timer's internal redelivery messages.
#[derive(Clone)]
pub(crate) struct CommitLogInternalMessageWriteHandle {
    pub(super) message_store_config: Arc<MessageStoreConfig>,
    pub(super) store_runtime_state: Arc<StoreRuntimeState>,
    pub(super) enabled_append_prop_crc: bool,
    pub(super) append_port: CommitLogAppendPort,
    pub(super) telemetry_handle: rocketmq_observability::TelemetryHandle,
}

impl CommitLogInternalMessageWriteHandle {
    pub(crate) async fn put_message(&self, mut msg: MessageExtBrokerInner) -> PutMessageResult {
        let append_span = rocketmq_observability::trace::store::append_span(&self.telemetry_handle);
        msg.set_wait_store_msg_ok(false);
        #[cfg(any(feature = "observability", feature = "observability-traces"))]
        rocketmq_observability::trace::record_message_properties_with_handle(
            &self.telemetry_handle,
            &append_span,
            msg.get_properties(),
            msg.get_body().map(|body| body.len()),
        );
        msg.message_ext_inner.body_crc = crc32_bytes(msg.message_ext_inner.message.get_body());
        if self.enabled_append_prop_crc {
            msg.delete_property(MessageConst::PROPERTY_CRC32);
        }

        msg.with_version(MessageVersion::V1);
        if self.message_store_config.auto_message_version_on_topic_len && msg.topic().len() > i8::MAX as usize {
            msg.with_version(MessageVersion::V2);
        }
        if msg.born_host().is_ipv6() {
            msg.with_born_host_v6_flag();
        }
        if msg.store_host().is_ipv6() {
            msg.with_store_host_v6_flag();
        }

        let need_assign_offset = !(self.message_store_config.duplication_enable
            && self.store_runtime_state.broker_role() != BrokerRole::Slave);
        let prepared = match message_encoder_pool::prepare_message_with_pool(&msg, &self.message_store_config) {
            Ok(prepared) => prepared,
            Err(result) => return result,
        };
        self.append_port
            .append_message(msg, prepared, need_assign_offset)
            .instrument(append_span)
            .await
            .result
    }
}

/// Safe, cloneable capability used by HA replica readers.
#[derive(Clone)]
pub(crate) struct CommitLogReplicaHandle {
    pub(super) read: CommitLogReadHandle,
    pub(super) append: MappedFileQueueAppendHandle,
    pub(super) put_message_lock: Arc<tokio::sync::Mutex<()>>,
    pub(super) runtime_state: Arc<CommitLogRuntimeState>,
    pub(super) store_checkpoint: Arc<StoreCheckpoint>,
}

impl CommitLogReplicaHandle {
    #[inline]
    pub(crate) fn get_max_offset(&self) -> i64 {
        self.read.get_max_offset()
    }

    #[inline]
    pub(crate) fn get_min_offset(&self) -> i64 {
        self.read.get_min_offset()
    }

    #[inline]
    pub(crate) fn get_flushed_where(&self) -> i64 {
        self.read.get_flushed_where()
    }

    #[inline]
    pub(crate) fn get_confirm_offset(&self) -> i64 {
        self.read.get_confirm_offset()
    }

    #[inline]
    pub(crate) fn get_confirm_offset_directly(&self) -> i64 {
        self.read.get_confirm_offset_directly()
    }

    pub(crate) fn select_segments(
        &self,
        offset: i64,
        max_bytes: usize,
        allow_cross_file: bool,
    ) -> TransferResult<Vec<SegmentLease>> {
        self.read.select_segments(offset, max_bytes, allow_cross_file)
    }

    pub(crate) async fn append_data(
        &self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        let lock = self.put_message_lock.lock().await;
        let mapped_file = self.append.get_last_mapped_file(start_offset as u64, true);
        if mapped_file.is_none() {
            drop(lock);
            return Err(StoreError::mapped_file_not_found(StoreOperation::Append));
        }
        let Some(mapped_file) = self.append.get_last_mapped_file(start_offset as u64, true) else {
            drop(lock);
            return Err(StoreError::mapped_file_not_found(StoreOperation::Append));
        };
        let appended = mapped_file.append_message_offset_length(data, data_start as usize, data_length as usize);
        drop(lock);
        Ok(appended)
    }

    #[inline]
    pub(crate) fn publish_confirm_offset(&self, phy_offset: i64) {
        publish_confirm_offset(&self.runtime_state, &self.store_checkpoint, phy_offset);
    }
}

#[inline]
pub(super) fn publish_confirm_offset(
    runtime_state: &CommitLogRuntimeState,
    store_checkpoint: &StoreCheckpoint,
    phy_offset: i64,
) {
    runtime_state.publish_confirm_offset(phy_offset);
    store_checkpoint.set_confirm_phy_offset(phy_offset as u64);
}

pub(super) fn resolve_commit_log_confirm_offset(
    message_store_config: &MessageStoreConfig,
    broker_role: BrokerRole,
    broker_config: &StoreRuntimeConfig,
    store_context: &CommitLogStoreContext,
    stored_confirm_offset: i64,
    max_phy_offset: i64,
    flushed_where: i64,
) -> i64 {
    if broker_config.enable_controller_mode {
        if broker_role == BrokerRole::Slave || store_context.running_flags.is_fenced() {
            return stored_confirm_offset;
        }

        let Some(ha_service) = store_context.ha_service() else {
            return stored_confirm_offset;
        };
        if ha_service.local_sync_state_set_size(max_phy_offset) <= 1 || !message_store_config.all_ack_in_sync_state_set
        {
            return max_phy_offset;
        }
        if stored_confirm_offset >= 0 {
            return stored_confirm_offset;
        }
        return ha_service.compute_confirm_offset(stored_confirm_offset, max_phy_offset);
    }

    if broker_config.duplication_enable {
        stored_confirm_offset
    } else if message_store_config.flush_disk_type == FlushDiskType::SyncFlush {
        flushed_where
    } else {
        max_phy_offset
    }
}
