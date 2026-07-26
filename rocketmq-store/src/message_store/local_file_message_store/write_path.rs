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

pub(super) fn murmur3_x64_128(bytes: &[u8], seed: u32) -> (u64, u64) {
    const C1: u64 = 0x87c3_7b91_1142_53d5;
    const C2: u64 = 0x4cf5_ad43_2745_937f;

    let mut h1 = seed as u64;
    let mut h2 = seed as u64;

    let block_count = bytes.len() / 16;
    for index in 0..block_count {
        let offset = index * 16;
        let mut k1 = u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("slice is exactly 8 bytes"));
        let mut k2 = u64::from_le_bytes(
            bytes[offset + 8..offset + 16]
                .try_into()
                .expect("slice is exactly 8 bytes"),
        );

        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;

        h1 = h1.rotate_left(27);
        h1 = h1.wrapping_add(h2);
        h1 = h1.wrapping_mul(5).wrapping_add(0x52dc_e729);

        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;

        h2 = h2.rotate_left(31);
        h2 = h2.wrapping_add(h1);
        h2 = h2.wrapping_mul(5).wrapping_add(0x3849_5ab5);
    }

    let tail = &bytes[block_count * 16..];
    let mut k1 = 0u64;
    let mut k2 = 0u64;

    for (index, byte) in tail.iter().enumerate() {
        if index < 8 {
            k1 |= (*byte as u64) << (index * 8);
        } else {
            k2 |= (*byte as u64) << ((index - 8) * 8);
        }
    }

    if tail.len() > 8 {
        k2 = k2.wrapping_mul(C2);
        k2 = k2.rotate_left(33);
        k2 = k2.wrapping_mul(C1);
        h2 ^= k2;
    }

    if !tail.is_empty() {
        k1 = k1.wrapping_mul(C1);
        k1 = k1.rotate_left(31);
        k1 = k1.wrapping_mul(C2);
        h1 ^= k1;
    }

    h1 ^= bytes.len() as u64;
    h2 ^= bytes.len() as u64;

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 = h1.wrapping_add(h2);
    h2 = h2.wrapping_add(h1);

    (h1, h2)
}

pub(super) fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^= value >> 33;
    value
}

pub(super) fn murmur3_x64_128_bytes(bytes: &[u8], seed: u32) -> [u8; 16] {
    let (h1, h2) = murmur3_x64_128(bytes, seed);
    let mut result = [0u8; 16];
    result[..8].copy_from_slice(&h1.to_le_bytes());
    result[8..].copy_from_slice(&h2.to_le_bytes());
    result
}

impl LocalFileMessageStore {
    pub(super) async fn append_replica_bytes(
        &mut self,
        start_offset: i64,
        data: &[u8],
        data_start: i32,
        data_length: i32,
    ) -> Result<bool, StoreError> {
        if self.shutdown.load(Ordering::Acquire) {
            warn!("message store has shutdown, so appendToCommitLog is forbidden");
            return Ok(false);
        }

        let result = self
            .commit_log
            .append_data(start_offset, data, data_start, data_length)
            .await?;
        if !result {
            error!(
                "DefaultMessageStore#appendToCommitLog: failed to append data to commitLog, physical offset={}, data \
                 length={}",
                start_offset, data_length
            );
        }
        Ok(result)
    }

    pub(crate) async fn put_message_shared(&self, mut msg: MessageExtBrokerInner) -> PutMessageResult {
        if !self.is_store_available_for_io() {
            warn!("message store has shutdown, so putMessage is forbidden");
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        }

        for hook in self.put_message_hook_list.snapshot() {
            if let Some(result) = hook.execute_before_put_message(&mut msg) {
                return result;
            }
        }
        let lmq_dispatch_queue_keys = self.prepare_lmq_dispatch(&mut msg);
        let lmq_dispatch_message_num = self.get_lmq_dispatch_message_num(&msg);

        if msg
            .message_ext_inner
            .properties()
            .contains_key(MessageConst::PROPERTY_INNER_NUM)
            && !MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG)
        {
            warn!(
                "[BUG]The message had property {} but is not an inner batch",
                MessageConst::PROPERTY_INNER_NUM
            );
            return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
        }

        if MessageSysFlag::check(msg.sys_flag(), MessageSysFlag::INNER_BATCH_FLAG) {
            let topic_config = self.get_topic_config(msg.topic());
            if !QueueTypeUtils::is_batch_cq_arc_mut(topic_config.as_ref()) {
                error!("[BUG]The message is an inner batch but cq type is not batch cq");
                return PutMessageResult::new_default(PutMessageStatus::MessageIllegal);
            }
        }
        let begin_time = Instant::now();
        let result = self.commit_log.put_message(msg).await;
        let elapsed_time = begin_time.elapsed().as_millis();
        if elapsed_time > 500 {
            warn!(
                "DefaultMessageStore#putMessage: CommitLog#putMessage cost {}ms",
                elapsed_time,
            );
        }
        self.store_stats_service
            .set_put_message_entire_time_max(elapsed_time as u64);
        if !result.is_ok() {
            self.store_stats_service
                .get_put_message_failed_times()
                .fetch_add(1, Ordering::AcqRel);
        }

        if result.is_ok() {
            if !lmq_dispatch_queue_keys.is_empty() {
                self.update_lmq_offsets(&lmq_dispatch_queue_keys, lmq_dispatch_message_num);
            }
            self.reput_message_service.notify_new_message();
        }

        result
    }

    pub(crate) async fn put_messages_shared(&self, mut message_ext_batch: MessageExtBatch) -> PutMessageResult {
        if !self.is_store_available_for_io() {
            warn!("message store has shutdown, so putMessages is forbidden");
            return PutMessageResult::new_default(PutMessageStatus::ServiceNotAvailable);
        }

        for hook in self.put_message_hook_list.snapshot() {
            if let Some(result) = hook.execute_before_put_message(&mut message_ext_batch.message_ext_broker_inner) {
                return result;
            }
        }
        let lmq_dispatch_queue_keys = self.prepare_lmq_dispatch(&mut message_ext_batch.message_ext_broker_inner);
        let lmq_dispatch_message_num = self.get_lmq_dispatch_message_num(&message_ext_batch.message_ext_broker_inner);

        let begin_time = Instant::now();
        let result = self.commit_log.put_messages(message_ext_batch).await;
        let elapsed_time = begin_time.elapsed().as_millis();
        if elapsed_time > 500 {
            warn!("not in lock eclipse time(ms) {}ms", elapsed_time,);
        }
        self.store_stats_service
            .set_put_message_entire_time_max(elapsed_time as u64);
        if !result.is_ok() {
            self.store_stats_service
                .get_put_message_failed_times()
                .fetch_add(1, Ordering::Relaxed);
        }

        if result.is_ok() {
            if !lmq_dispatch_queue_keys.is_empty() {
                self.update_lmq_offsets(&lmq_dispatch_queue_keys, lmq_dispatch_message_num);
            }
            self.reput_message_service.notify_new_message();
        }

        result
    }
}
