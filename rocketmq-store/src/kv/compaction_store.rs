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

use std::collections::BTreeMap;
use std::collections::HashMap;

use bytes::Bytes;
use cheetah_string::CheetahString;
use dashmap::DashMap;

use crate::base::dispatch_request::DispatchRequest;
use crate::base::get_message_result::GetMessageResult;
use crate::base::message_status_enum::GetMessageStatus;
use crate::base::select_result::SelectMappedBufferResult;
use crate::log_file::mapped_file::MappedFile;

#[derive(Default)]
pub struct CompactionStore {
    queues: DashMap<CompactionQueueKey, BTreeMap<i64, CompactionMessage>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CompactionQueueKey {
    topic: CheetahString,
    queue_id: i32,
}

impl CompactionQueueKey {
    fn new(topic: &CheetahString, queue_id: i32) -> Self {
        Self {
            topic: topic.clone(),
            queue_id,
        }
    }
}

#[derive(Clone, Debug)]
struct CompactionMessage {
    queue_offset: i64,
    batch_num: i32,
    key: Option<CheetahString>,
    payload: Bytes,
}

impl CompactionStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put_message(&self, topic: &CheetahString, queue_id: i32, queue_offset: i64, batch_num: i32, payload: Bytes) {
        self.put_message_with_key(topic, queue_id, queue_offset, batch_num, None, payload);
    }

    pub(crate) fn put_dispatch_message(&self, dispatch_request: &DispatchRequest, payload: Bytes) {
        let key = (!dispatch_request.keys.is_empty()).then(|| dispatch_request.keys.clone());
        self.put_message_with_key(
            &dispatch_request.topic,
            dispatch_request.queue_id,
            dispatch_request.consume_queue_offset,
            dispatch_request.batch_size as i32,
            key,
            payload,
        );
    }

    pub(crate) fn put_message_with_key(
        &self,
        topic: &CheetahString,
        queue_id: i32,
        queue_offset: i64,
        batch_num: i32,
        message_key: Option<CheetahString>,
        payload: Bytes,
    ) {
        if queue_offset < 0 || payload.is_empty() {
            return;
        }

        let key = CompactionQueueKey::new(topic, queue_id);
        let mut queue = self.queues.entry(key).or_default();
        queue.insert(
            queue_offset,
            CompactionMessage {
                queue_offset,
                batch_num: batch_num.max(1),
                key: message_key,
                payload,
            },
        );
    }

    pub fn dispatch_from_commit_log<MF: MappedFile>(
        &self,
        dispatch_request: &DispatchRequest,
        commit_log_file: &MF,
    ) -> bool {
        if !dispatch_request.success
            || dispatch_request.msg_size <= 0
            || dispatch_request.commit_log_offset < 0
            || dispatch_request.consume_queue_offset < 0
        {
            return false;
        }

        let file_from_offset = commit_log_file.get_file_from_offset() as i64;
        if dispatch_request.commit_log_offset < file_from_offset {
            return false;
        }

        let pos = (dispatch_request.commit_log_offset - file_from_offset) as usize;
        let size = dispatch_request.msg_size as usize;
        let Some(payload) = commit_log_file.get_data(pos, size) else {
            return false;
        };

        self.put_dispatch_message(dispatch_request, payload);
        true
    }

    pub fn compact_once(&self) -> usize {
        let mut removed = 0;

        for mut queue in self.queues.iter_mut() {
            let mut latest_offset_by_key = HashMap::<CheetahString, i64>::new();
            for (&queue_offset, message) in queue.iter() {
                let Some(key) = message.key.as_ref() else {
                    continue;
                };
                latest_offset_by_key
                    .entry(key.clone())
                    .and_modify(|latest_offset| *latest_offset = (*latest_offset).max(queue_offset))
                    .or_insert(queue_offset);
            }

            if latest_offset_by_key.is_empty() {
                continue;
            }

            let before = queue.len();
            queue.retain(|queue_offset, message| {
                let Some(key) = message.key.as_ref() else {
                    return true;
                };
                match latest_offset_by_key.get(key) {
                    Some(latest_offset) => *latest_offset == *queue_offset,
                    None => true,
                }
            });
            removed += before.saturating_sub(queue.len());
        }

        removed
    }

    pub fn get_message(
        &self,
        _group: &CheetahString,
        topic: &CheetahString,
        queue_id: i32,
        offset: i64,
        max_msg_nums: i32,
        max_total_msg_size: i32,
    ) -> Option<GetMessageResult> {
        let key = CompactionQueueKey::new(topic, queue_id);
        let Some(queue) = self.queues.get(&key) else {
            return Some(status_result(GetMessageStatus::NoMatchedLogicQueue, 0, 0, 0));
        };
        if queue.is_empty() {
            return Some(status_result(GetMessageStatus::NoMessageInQueue, 0, 0, 0));
        }

        let min_offset = *queue.keys().next().unwrap_or(&0);
        let max_offset = queue
            .values()
            .map(|message| message.queue_offset + message.batch_num as i64)
            .max()
            .unwrap_or(min_offset);

        if offset < min_offset {
            return Some(status_result(
                GetMessageStatus::OffsetTooSmall,
                min_offset,
                min_offset,
                max_offset,
            ));
        }
        if offset == max_offset {
            return Some(status_result(
                GetMessageStatus::OffsetOverflowOne,
                offset,
                min_offset,
                max_offset,
            ));
        }
        if offset > max_offset {
            return Some(status_result(
                GetMessageStatus::OffsetOverflowBadly,
                max_offset,
                min_offset,
                max_offset,
            ));
        }

        let mut result = GetMessageResult::new_result_size(max_msg_nums.max(1) as usize);
        let max_msg_nums = max_msg_nums.max(1);
        let max_total_msg_size = max_total_msg_size.max(1);
        let mut next_begin_offset = offset;

        for (&queue_offset, message) in queue.range(offset..) {
            if result.message_count() >= max_msg_nums {
                break;
            }

            let payload_size = message.payload.len() as i32;
            if result.buffer_total_size() > 0 && result.buffer_total_size() + payload_size > max_total_msg_size {
                break;
            }

            result.add_message(
                SelectMappedBufferResult {
                    start_offset: 0,
                    bytes: Some(message.payload.clone()),
                    size: payload_size,
                    mapped_file: None,
                    is_in_cache: true,
                },
                queue_offset as u64,
                message.batch_num,
            );
            next_begin_offset = message.queue_offset + message.batch_num as i64;
        }

        if result.message_count() == 0 {
            return Some(status_result(
                GetMessageStatus::NoMatchedMessage,
                offset,
                min_offset,
                max_offset,
            ));
        }

        result.set_status(Some(GetMessageStatus::Found));
        result.set_next_begin_offset(next_begin_offset);
        result.set_min_offset(min_offset);
        result.set_max_offset(max_offset);
        Some(result)
    }

    pub fn message_count(&self, topic: &CheetahString, queue_id: i32) -> usize {
        self.queues
            .get(&CompactionQueueKey::new(topic, queue_id))
            .map(|queue| queue.len())
            .unwrap_or_default()
    }
}

fn status_result(
    status: GetMessageStatus,
    next_begin_offset: i64,
    min_offset: i64,
    max_offset: i64,
) -> GetMessageResult {
    let mut result = GetMessageResult::new_result_size(0);
    result.set_status(Some(status));
    result.set_next_begin_offset(next_begin_offset);
    result.set_min_offset(min_offset);
    result.set_max_offset(max_offset);
    result
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cheetah_string::CheetahString;

    use super::CompactionStore;
    use crate::base::message_status_enum::GetMessageStatus;

    #[test]
    fn get_message_returns_java_style_status_for_missing_queue() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-missing-topic");
        let group = CheetahString::from_static_str("compaction-missing-group");

        let result = store
            .get_message(&group, &topic, 0, 0, 32, 1024)
            .expect("compaction store should return a status result");

        assert_eq!(result.status(), Some(GetMessageStatus::NoMatchedLogicQueue));
        assert_eq!(result.message_count(), 0);
    }

    #[test]
    fn put_and_get_message_round_trips_in_compaction_store() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-topic");
        let group = CheetahString::from_static_str("compaction-group");

        store.put_message(&topic, 0, 7, 1, Bytes::from_static(b"compacted-message"));

        let result = store
            .get_message(&group, &topic, 0, 7, 32, 1024)
            .expect("compaction result");

        assert_eq!(result.status(), Some(GetMessageStatus::Found));
        assert_eq!(result.message_count(), 1);
        assert_eq!(result.next_begin_offset(), 8);
        assert_eq!(
            result.message_mapped_list()[0].get_bytes_ref().unwrap().as_ref(),
            b"compacted-message"
        );
    }

    #[test]
    fn compact_once_keeps_latest_message_for_same_key() {
        let store = CompactionStore::new();
        let topic = CheetahString::from_static_str("compaction-topic");
        let group = CheetahString::from_static_str("compaction-group");
        let key = CheetahString::from_static_str("same-key");

        store.put_message_with_key(&topic, 0, 0, 1, Some(key.clone()), Bytes::from_static(b"old-message"));
        store.put_message_with_key(&topic, 0, 1, 1, Some(key), Bytes::from_static(b"latest-message"));

        assert_eq!(store.message_count(&topic, 0), 2);
        assert_eq!(store.compact_once(), 1);
        assert_eq!(store.message_count(&topic, 0), 1);

        let old_result = store
            .get_message(&group, &topic, 0, 0, 32, 1024)
            .expect("old offset should return a boundary status");
        assert_eq!(old_result.status(), Some(GetMessageStatus::OffsetTooSmall));

        let latest_result = store
            .get_message(&group, &topic, 0, 1, 32, 1024)
            .expect("latest result");
        assert_eq!(latest_result.status(), Some(GetMessageStatus::Found));
        assert_eq!(
            latest_result.message_mapped_list()[0].get_bytes_ref().unwrap(),
            &Bytes::from_static(b"latest-message")
        );
    }
}
