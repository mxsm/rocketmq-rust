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

use super::*;

pub(super) fn estimate_in_mem_by_commit_offset(
    offset_py: i64,
    max_offset_py: i64,
    message_store_config: &Arc<MessageStoreConfig>,
) -> bool {
    let memory =
        (*TOTAL_PHYSICAL_MEMORY_SIZE as f64) * (message_store_config.access_message_in_memory_max_ratio as f64 / 100.0);
    (max_offset_py - offset_py) <= memory as i64
}

pub(super) fn is_the_batch_full(
    size_py: i32,
    unit_batch_num: i32,
    max_msg_nums: i32,
    max_msg_size: i64,
    buffer_total: i32,
    message_total: i32,
    is_in_mem: bool,
    message_store_config: &Arc<MessageStoreConfig>,
) -> bool {
    if buffer_total == 0 || message_total == 0 {
        return false;
    }

    if message_total + unit_batch_num > max_msg_nums {
        return true;
    }

    if buffer_total as i64 + size_py as i64 > max_msg_size {
        return true;
    }

    if is_in_mem {
        if (buffer_total + size_py) as u64 > message_store_config.max_transfer_bytes_on_message_in_memory {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_memory - 1
    } else {
        if (buffer_total + size_py) as u64 > message_store_config.max_transfer_bytes_on_message_in_disk {
            return true;
        }

        message_total as u64 > message_store_config.max_transfer_count_on_message_in_disk - 1
    }
}

impl LocalFileMessageStore {
    pub(super) async fn read_messages(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        self.read_messages_with_size_limit(
            group,
            topic,
            queue_id,
            offset,
            max_msg_nums,
            MAX_PULL_MSG_SIZE,
            message_filter,
        )
        .await
    }

    #[allow(
        unused_assignments,
        reason = "preserve the legacy status transition matrix while moving the read path"
    )]
    pub(super) async fn read_messages_with_size_limit(
        &self,
        group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: Option<ArcMessageFilter>,
    ) -> Option<GetMessageResult> {
        let lifecycle_state = self.lifecycle_state();
        if lifecycle_state == LocalStoreState::Shutdown || lifecycle_state.is_recovering() {
            warn!("message store is not available, so getMessage is forbidden");
            return None;
        }

        if self.shutdown.load(Ordering::Relaxed) {
            warn!("message store has shutdown, so getMessage is forbidden");
            return None;
        }

        if !self.running_flags.is_readable() {
            warn!(
                "message store is not readable, so getMessage is forbidden {}",
                self.running_flags.get_flag_bits()
            );
            return None;
        }
        let topic_config = self.get_topic_config(topic);
        let policy = get_delete_policy_arc_mut(topic_config.as_ref());
        if policy == CleanupPolicy::COMPACTION && self.message_store_config.enable_compaction {
            if let Some(result) = self
                .compaction_store
                .get_message(group, topic, queue_id, offset, max_msg_nums, max_total_msg_size)
                .await
            {
                return Some(result);
            }
        }
        let begin_time = Instant::now();

        let mut status = GetMessageStatus::NoMessageInQueue;

        let mut next_begin_offset = offset;
        let mut min_offset = 0;
        let mut max_offset = 0;
        let result_capacity = (max_msg_nums.max(0) as usize).min(self.message_store_config.max_msgs_num_batch.max(1));
        let mut get_result = GetMessageResult::new_result_size(result_capacity);
        let max_offset_py = self.commit_log.get_max_offset();
        let consume_queue = self.find_consume_queue(topic, queue_id);
        if let Some(consume_queue) = consume_queue {
            let consume_queue = consume_queue.read();
            min_offset = consume_queue.get_min_offset_in_queue();
            max_offset = consume_queue.get_max_offset_in_queue();
            if max_offset == 0 {
                status = GetMessageStatus::NoMessageInQueue;
                next_begin_offset = self.next_offset_correction(offset, 0);
            } else if offset < min_offset {
                status = GetMessageStatus::OffsetTooSmall;
                next_begin_offset = self.next_offset_correction(offset, min_offset);
            } else if offset == max_offset {
                status = GetMessageStatus::OffsetOverflowOne;
                next_begin_offset = self.next_offset_correction(offset, offset);
            } else if offset > max_offset {
                status = GetMessageStatus::OffsetOverflowBadly;
                next_begin_offset = self.next_offset_correction(offset, max_offset);
            } else {
                let max_filter_message_size = self
                    .message_store_config
                    .max_filter_message_size
                    .max(max_msg_nums * consume_queue.get_unit_size());
                let disk_fall_recorded = self.message_store_config.disk_fall_recorded;
                let mut max_pull_size = max_total_msg_size.max(100);
                if max_pull_size > MAX_PULL_MSG_SIZE {
                    warn!(
                        "The max pull size is too large maxPullSize={} topic={} queueId={}",
                        max_pull_size, topic, queue_id
                    );
                    max_pull_size = MAX_PULL_MSG_SIZE;
                }
                status = GetMessageStatus::NoMatchedMessage;
                let mut max_phy_offset_pulling = 0;
                let mut cq_file_num = 0;
                while get_result.buffer_total_size() <= 0
                    && next_begin_offset < max_offset
                    && cq_file_num < self.message_store_config.travel_cq_file_num_when_get_message
                {
                    cq_file_num += 1;
                    let buffer_consume_queue = consume_queue.iterate_from_with_count(next_begin_offset, max_msg_nums);
                    if buffer_consume_queue.is_none() {
                        status = GetMessageStatus::OffsetFoundNull;
                        next_begin_offset = self.next_offset_correction(
                            next_begin_offset,
                            self.consume_queue_store
                                .roll_next_file(consume_queue.as_ref(), next_begin_offset),
                        );
                        warn!(
                            "consumer request topic: {}, offset: {}, minOffset: {}, maxOffset: {}, but access logic \
                             queue failed. Correct nextBeginOffset to {}",
                            topic, offset, min_offset, max_offset, next_begin_offset
                        );
                        break;
                    }
                    let mut next_phy_file_start_offset = i64::MIN;
                    let Some(mut buffer_consume_queue) = buffer_consume_queue else {
                        break;
                    };
                    loop {
                        if next_begin_offset >= max_offset {
                            break;
                        }
                        if let Some(cq_unit) = buffer_consume_queue.next() {
                            let offset_py = cq_unit.pos;
                            let size_py = cq_unit.size;
                            let is_in_mem =
                                estimate_in_mem_by_commit_offset(offset_py, max_offset_py, &self.message_store_config);
                            if (cq_unit.queue_offset - offset) * consume_queue.get_unit_size() as i64
                                > max_filter_message_size as i64
                            {
                                break;
                            }
                            let get_result_ref = &mut get_result;
                            if is_the_batch_full(
                                size_py,
                                cq_unit.batch_num as i32,
                                max_msg_nums,
                                max_pull_size as i64,
                                get_result_ref.buffer_total_size(),
                                get_result_ref.message_count(),
                                is_in_mem,
                                &self.message_store_config,
                            ) {
                                break;
                            }
                            if get_result_ref.buffer_total_size() >= max_pull_size {
                                break;
                            }
                            max_phy_offset_pulling = offset_py;
                            next_begin_offset = cq_unit.queue_offset + cq_unit.batch_num as i64;
                            if next_phy_file_start_offset != i64::MIN && offset_py < next_phy_file_start_offset {
                                continue;
                            }

                            if let Some(filter) = message_filter.as_ref() {
                                if !filter.is_matched_by_consume_queue(
                                    cq_unit.get_valid_tags_code_as_long(),
                                    cq_unit.cq_ext_unit.as_ref(),
                                ) {
                                    if get_result_ref.buffer_total_size() == 0 {
                                        status = GetMessageStatus::NoMatchedMessage;
                                    }
                                    continue;
                                }
                            }

                            let Some(select_result) = self.commit_log.get_message(offset_py, size_py) else {
                                if get_result_ref.buffer_total_size() == 0 {
                                    status = GetMessageStatus::MessageWasRemoving;
                                }
                                next_phy_file_start_offset = self.commit_log.roll_next_file(offset_py);
                                continue;
                            };
                            if self.message_store_config.cold_data_flow_control_enable
                                && !is_sys_consumer_group_for_no_cold_read_limit(group)
                                && !select_result.is_in_cache()
                            {
                                get_result_ref.set_cold_data_sum(get_result_ref.cold_data_sum() + size_py as i64);
                            }

                            if let Some(filter) = message_filter.as_ref() {
                                if !filter.is_matched_by_commit_log(Some(select_result.get_buffer()), None) {
                                    if get_result_ref.buffer_total_size() == 0 {
                                        status = GetMessageStatus::NoMatchedMessage;
                                    }
                                    continue;
                                }
                            }
                            self.store_stats_service
                                .get_message_transferred_msg_count()
                                .fetch_add(cq_unit.batch_num as usize, Ordering::Relaxed);
                            get_result.add_message(
                                select_result,
                                cq_unit.queue_offset as u64,
                                cq_unit.batch_num as i32,
                            );
                            status = GetMessageStatus::Found;
                            next_phy_file_start_offset = i64::MIN;
                        } else {
                            break;
                        }
                    }
                }
                if disk_fall_recorded {
                    let fall_behind = max_offset_py - max_phy_offset_pulling;
                    if let Some(broker_stats_manager) = self.broker_stats_manager.as_ref() {
                        broker_stats_manager.record_disk_fall_behind_size(group, topic, queue_id, fall_behind);
                    } else {
                        warn!("disk fall behind recording is enabled but BrokerStatsManager is not initialized");
                    }
                }
                let diff = max_offset_py - max_phy_offset_pulling;
                let memory = ((*TOTAL_PHYSICAL_MEMORY_SIZE as f64)
                    * (self.message_store_config.access_message_in_memory_max_ratio as f64 / 100.0))
                    as i64;
                get_result.set_suggest_pulling_from_slave(diff > memory);
            }
        } else {
            status = GetMessageStatus::NoMatchedLogicQueue;
            next_begin_offset = self.next_offset_correction(offset, 0);
        }

        let mut result = get_result;
        result.set_status(Some(status));
        result.set_next_begin_offset(next_begin_offset);
        result.set_max_offset(max_offset);
        result.set_min_offset(min_offset);

        #[cfg(feature = "tieredstore")]
        if TieredStoreDecorator::should_try_get_message(status) {
            if let Some(tiered_result) = match self.tiered_store.as_ref() {
                Some(tiered_store) => {
                    tiered_store
                        .get_message(
                            group,
                            topic,
                            queue_id,
                            offset,
                            max_msg_nums,
                            max_total_msg_size,
                            message_filter.clone(),
                        )
                        .await
                }
                None => None,
            } {
                if tiered_result.status() != Some(GetMessageStatus::NoMatchedLogicQueue) {
                    result = tiered_result;
                    status = result.status().unwrap_or(status);
                }
            }
        }

        if GetMessageStatus::Found == status {
            self.store_stats_service
                .get_message_times_total_found()
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.store_stats_service
                .get_message_times_total_miss()
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_time = begin_time.elapsed().as_millis() as u64;
        self.store_stats_service.set_get_message_entire_time_max(elapsed_time);

        Some(result)
    }

    /*    async fn get_message_with_size_limit_async(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
        message_filter: &dyn MessageFilter,
    ) -> Result<GetMessageResult, StoreError> {

    }*/

    pub(super) async fn query_messages(
        &self,
        topic: &CheetahString,
        key: &CheetahString,
        max_num: i32,
        begin_timestamp: i64,
        end_timestamp: i64,
    ) -> Option<QueryMessageResult> {
        let mut query_message_result = QueryMessageResult::default();
        let index_safe_phyoffset = self.current_index_safe_offset();
        let index_safety = self
            .composition
            .query()
            .index_safety(index_safe_phyoffset, self.get_confirm_offset());
        query_message_result.set_index_query_safety(
            index_safety.safe,
            index_safety.safe_offset,
            index_safety.confirm_offset,
        );
        let mut last_query_msg_time = end_timestamp;
        for i in 1..3 {
            let mut query_offset_result =
                self.index_service
                    .query_offset(topic, key, max_num, begin_timestamp, end_timestamp);
            if query_offset_result.get_phy_offsets().is_empty() {
                break;
            }

            query_offset_result.get_phy_offsets_mut().sort();

            query_message_result.index_last_update_timestamp = query_offset_result.get_index_last_update_timestamp();
            query_message_result.index_last_update_phyoffset = query_offset_result.get_index_last_update_phyoffset();
            let phy_offsets = query_offset_result.get_phy_offsets();
            for (m, offset) in phy_offsets.iter().copied().enumerate() {
                if m == 0 {
                    if let Some(msg) = self.look_message_by_offset(offset) {
                        last_query_msg_time = msg.store_timestamp;
                    } else {
                        warn!("index query returned unreadable message offset {offset}");
                    }
                }
                let result = self.commit_log.get_data_with_option(offset, false);
                if let Some(sbr) = result {
                    query_message_result.add_message(sbr);
                }
            }
            if query_message_result.buffer_total_size > 0 {
                break;
            }
            if last_query_msg_time < begin_timestamp {
                break;
            }
        }

        #[cfg(feature = "tieredstore")]
        if query_message_result.buffer_total_size == 0 {
            if let Some(tiered_store) = self.tiered_store.as_ref() {
                if let Some(tiered_result) = tiered_store
                    .query_message(topic, key, max_num, begin_timestamp, end_timestamp)
                    .await
                {
                    return Some(tiered_result);
                }
            }
        }

        if self
            .composition
            .query()
            .should_record_degradation(query_message_result.buffer_total_size == 0, index_safety)
        {
            self.background_index_query_degradation_total
                .fetch_add(1, Ordering::Relaxed);
        }

        Some(query_message_result)
    }

    /*async fn query_message_async(
        &self,
        topic: &str,
        key: &str,
        max_num: i32,
        begin: i64,
        end: i64,
    ) -> Result<QueryMessageResult, StoreError> {

    }*/
}
