// Copyright 2026 The RocketMQ Rust Authors
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

use crate::message::types::MessageError;
use crate::message::types::MessageResult;
use rocketmq_common::common::message::message_queue::MessageQueue;
use rocketmq_dashboard_common::MessagePageQueryRequest;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

const MESSAGE_PAGE_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const DEFAULT_PAGE_SIZE: u32 = 10;
const MAX_PAGE_SIZE: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MessagePageCacheKey {
    pub(crate) topic: String,
    pub(crate) begin: i64,
    pub(crate) end: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct MessagePageCacheEntry {
    pub(crate) task_id: String,
    pub(crate) query_key: MessagePageCacheKey,
    pub(crate) namesrv_generation: u64,
    pub(crate) created_at: Instant,
    pub(crate) total: usize,
    pub(crate) queue_states: Vec<QueueScanState>,
}

impl MessagePageCacheEntry {
    pub(crate) fn is_expired(&self) -> bool {
        self.created_at.elapsed() >= MESSAGE_PAGE_CACHE_TTL
    }

    pub(crate) fn matches_request(&self, request: &NormalizedMessagePageQuery, generation: u64) -> bool {
        !self.is_expired()
            && self.namesrv_generation == generation
            && self.query_key
                == MessagePageCacheKey {
                    topic: request.topic.clone(),
                    begin: request.begin,
                    end: request.end,
                }
    }
}

#[derive(Clone, Default)]
pub(crate) struct MessagePageCache {
    entries: Arc<Mutex<HashMap<String, MessagePageCacheEntry>>>,
}

impl MessagePageCache {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn get(&self, task_id: &str) -> Option<MessagePageCacheEntry> {
        let mut entries = self.entries.lock().await;
        entries.retain(|_, entry| !entry.is_expired());
        entries.get(task_id).cloned()
    }

    pub(crate) async fn put(
        &self,
        query_key: MessagePageCacheKey,
        namesrv_generation: u64,
        total: usize,
        queue_states: Vec<QueueScanState>,
    ) -> String {
        let task_id = Uuid::new_v4().to_string();
        let entry = MessagePageCacheEntry {
            task_id: task_id.clone(),
            query_key,
            namesrv_generation,
            created_at: Instant::now(),
            total,
            queue_states,
        };

        let mut entries = self.entries.lock().await;
        entries.retain(|_, existing| !existing.is_expired());
        entries.insert(task_id.clone(), entry);
        task_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueScanState {
    pub(crate) idx: usize,
    pub(crate) broker_addr: String,
    pub(crate) message_queue: MessageQueue,
    pub(crate) start: i64,
    pub(crate) end: i64,
    pub(crate) start_offset: i64,
    pub(crate) end_offset: i64,
}

impl QueueScanState {
    pub(crate) fn new(idx: usize, broker_addr: String, message_queue: MessageQueue, start: i64, end: i64) -> Self {
        Self {
            idx,
            broker_addr,
            message_queue,
            start,
            end,
            start_offset: start,
            end_offset: start,
        }
    }

    pub(crate) fn span(&self) -> usize {
        (self.end - self.start).max(0) as usize
    }

    pub(crate) fn selection_len(&self) -> usize {
        (self.end_offset - self.start_offset).max(0) as usize
    }

    pub(crate) fn reset_selection(&mut self) {
        self.start_offset = self.start;
        self.end_offset = self.start;
    }

    pub(crate) fn inc_start_offset(&mut self) {
        self.start_offset += 1;
        self.end_offset += 1;
    }

    pub(crate) fn inc_start_offset_by(&mut self, amount: i64) {
        self.start_offset += amount;
        self.end_offset += amount;
    }

    pub(crate) fn inc_end_offset(&mut self) {
        self.end_offset += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NormalizedMessagePageQuery {
    pub(crate) topic: String,
    pub(crate) begin: i64,
    pub(crate) end: i64,
    pub(crate) page_num: u32,
    pub(crate) page_index: usize,
    pub(crate) page_size: usize,
    pub(crate) task_id: Option<String>,
}

impl NormalizedMessagePageQuery {
    pub(crate) fn first_page(&self) -> Self {
        let mut first_page = self.clone();
        first_page.page_num = 1;
        first_page.page_index = 0;
        first_page
    }
}

pub(crate) fn normalize_message_page_query(
    request: MessagePageQueryRequest,
) -> MessageResult<NormalizedMessagePageQuery> {
    let topic = request.topic.trim().to_string();
    if topic.is_empty() {
        return Err(MessageError::Validation(
            "`topic` is required for message page queries.".to_string(),
        ));
    }

    if request.begin < 0 || request.end < 0 {
        return Err(MessageError::Validation(
            "`begin` and `end` must be non-negative timestamps.".to_string(),
        ));
    }

    if request.end < request.begin {
        return Err(MessageError::Validation(
            "`end` must be greater than or equal to `begin`.".to_string(),
        ));
    }

    let page_num = request.page_num.max(1);
    let page_size = if request.page_size <= 1 {
        DEFAULT_PAGE_SIZE
    } else {
        request.page_size.min(MAX_PAGE_SIZE)
    } as usize;
    let task_id = request
        .task_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    Ok(NormalizedMessagePageQuery {
        topic,
        begin: request.begin,
        end: request.end,
        page_num,
        page_index: page_num.saturating_sub(1) as usize,
        page_size,
        task_id,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageSelection {
    pub(crate) queue_states: Vec<QueueScanState>,
    pub(crate) total: usize,
    pub(crate) selected: usize,
}

pub(crate) fn build_page_selection(queue_states: &[QueueScanState], query: &NormalizedMessagePageQuery) -> PageSelection {
    let mut queue_states = queue_states.to_vec();
    for queue_state in &mut queue_states {
        queue_state.reset_selection();
    }

    let total = queue_states.iter().map(QueueScanState::span).sum::<usize>();
    let offset = query.page_index * query.page_size;
    if queue_states.is_empty() || total <= offset {
        return PageSelection {
            queue_states,
            total,
            selected: 0,
        };
    }

    let selected = (total - offset).min(query.page_size);
    let next = move_start_offset(&mut queue_states, offset);
    move_end_offset(&mut queue_states, query.page_size, next);

    PageSelection {
        queue_states,
        total,
        selected,
    }
}

fn move_start_offset(queue_states: &mut [QueueScanState], mut offset: usize) -> usize {
    let size = queue_states.len();
    if size == 0 || offset == 0 {
        return 0;
    }

    let mut ordered = queue_states.to_vec();
    ordered.sort_by_key(|state| state.end - state.start_offset);

    for index in 0..size {
        if offset < (size - index) {
            break;
        }

        let min_span = (ordered[index].end - ordered[index].start_offset).max(0) as usize;
        if min_span == 0 {
            continue;
        }

        let reduce = min_span * (size - index);
        if reduce <= offset {
            offset -= reduce;
            for state in &mut ordered[index..] {
                state.inc_start_offset_by(min_span as i64);
            }
        } else {
            let add_offset = offset / (size - index);
            offset -= add_offset * (size - index);
            if add_offset != 0 {
                for state in &mut ordered[index..] {
                    state.inc_start_offset_by(add_offset as i64);
                }
            }
        }
    }

    for state in ordered {
        let idx = state.idx;
        queue_states[idx] = state;
    }

    let mut next = 0;
    for state in queue_states.iter_mut() {
        if offset == 0 {
            break;
        }
        next = (next + 1) % size;
        if state.start_offset < state.end {
            state.inc_start_offset();
            offset -= 1;
        }
    }

    next
}

fn move_end_offset(queue_states: &mut [QueueScanState], page_size: usize, mut next: usize) {
    let size = queue_states.len();
    if size == 0 {
        return;
    }

    for _ in 0..page_size {
        let mut current = next;
        next = (next + 1) % size;
        let start = next;

        while queue_states[current].end_offset >= queue_states[current].end {
            current = next;
            next = (next + 1) % size;
            if start == next {
                return;
            }
        }

        queue_states[current].inc_end_offset();
    }
}

#[cfg(test)]
mod tests {
    use super::build_page_selection;
    use super::normalize_message_page_query;
    use super::MessagePageCacheEntry;
    use super::MessagePageCacheKey;
    use super::NormalizedMessagePageQuery;
    use super::QueueScanState;
    use rocketmq_common::common::message::message_queue::MessageQueue;
    use rocketmq_dashboard_common::MessagePageQueryRequest;
    use std::time::Duration;
    use std::time::Instant;

    #[test]
    fn normalize_message_page_query_matches_java_defaults_and_bounds() {
        let query = normalize_message_page_query(MessagePageQueryRequest {
            topic: " TopicTest ".to_string(),
            begin: 1,
            end: 2,
            page_num: 0,
            page_size: 999,
            task_id: Some(" task-1 ".to_string()),
        })
        .expect("query should normalize");

        assert_eq!(query.topic, "TopicTest");
        assert_eq!(query.page_num, 1);
        assert_eq!(query.page_index, 0);
        assert_eq!(query.page_size, 100);
        assert_eq!(query.task_id.as_deref(), Some("task-1"));
    }

    #[test]
    fn build_page_selection_distributes_first_page_round_robin() {
        let query = NormalizedMessagePageQuery {
            topic: "TopicTest".to_string(),
            begin: 0,
            end: 0,
            page_num: 1,
            page_index: 0,
            page_size: 4,
            task_id: None,
        };
        let selection = build_page_selection(
            &[
                queue_state(0, 0, 5),
                queue_state(1, 0, 5),
                queue_state(2, 0, 5),
            ],
            &query,
        );

        assert_eq!(selection.total, 15);
        assert_eq!(selection.selected, 4);
        assert_eq!(selection.queue_states[0].selection_len(), 2);
        assert_eq!(selection.queue_states[1].selection_len(), 1);
        assert_eq!(selection.queue_states[2].selection_len(), 1);
    }

    #[test]
    fn message_page_cache_entry_rejects_expired_or_mismatched_requests() {
        let query = NormalizedMessagePageQuery {
            topic: "TopicTest".to_string(),
            begin: 100,
            end: 200,
            page_num: 1,
            page_index: 0,
            page_size: 10,
            task_id: Some("task-1".to_string()),
        };
        let entry = MessagePageCacheEntry {
            task_id: "task-1".to_string(),
            query_key: MessagePageCacheKey {
                topic: "TopicTest".to_string(),
                begin: 100,
                end: 200,
            },
            namesrv_generation: 7,
            created_at: Instant::now() - Duration::from_secs(10),
            total: 20,
            queue_states: vec![queue_state(0, 0, 2)],
        };

        assert!(entry.matches_request(&query, 7));
        assert!(!entry.matches_request(&query, 8));
    }

    fn queue_state(idx: usize, start: i64, end: i64) -> QueueScanState {
        QueueScanState::new(
            idx,
            "broker-a:10911".to_string(),
            MessageQueue::from_parts("TopicTest", "broker-a", idx as i32),
            start,
            end,
        )
    }
}
