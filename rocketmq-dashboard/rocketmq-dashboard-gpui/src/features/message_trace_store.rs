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

//! Candidate reads and selected Trace reads have independent identities and epochs.

use super::messages_store::{MessageQueryRequest, MessagesStore};
use crate::{
    services::messages::MessageRequestScope,
    state::{Loadable, RequestEpoch, UiError},
};
use rocketmq_dashboard_common::{
    MessageIdentity, MessageSearchPage, MessageSearchQuery, MessageTrace, MessageTraceQuery,
};

#[derive(Clone)]
pub struct TraceRequest {
    pub scope: MessageRequestScope,
    pub query: MessageTraceQuery,
}

/// Shared by the standalone workspace and the lazy Message Inspector tab.
pub struct SelectedTrace {
    pub query: Option<MessageTraceQuery>,
    pub state: Loadable<MessageTrace>,
    epoch: RequestEpoch,
}
impl Default for SelectedTrace {
    fn default() -> Self {
        Self {
            query: None,
            state: Loadable::Idle,
            epoch: RequestEpoch::initial(),
        }
    }
}
impl SelectedTrace {
    pub fn begin(&mut self, identity: MessageIdentity, trace_topic: String, revision: u64) -> Option<TraceRequest> {
        self.clear();
        let epoch = self.epoch.advance().ok()?;
        let query = MessageTraceQuery { identity, trace_topic };
        self.query = Some(query.clone());
        self.state = Loadable::InitialLoading;
        Some(TraceRequest {
            scope: MessageRequestScope {
                revision,
                epoch: epoch.value(),
            },
            query,
        })
    }
    pub fn finish(&mut self, request: &TraceRequest, revision: u64, result: Result<MessageTrace, UiError>) -> bool {
        if request.scope.revision != revision
            || request.scope.epoch != self.epoch.value()
            || self.query.as_ref() != Some(&request.query)
        {
            return false;
        }
        self.state = match result {
            Ok(trace) => Loadable::Ready(trace),
            Err(error) => Loadable::Failed { error, previous: None },
        };
        true
    }
    pub fn clear(&mut self) {
        let _ = self.epoch.advance();
        self.query = None;
        self.state = Loadable::Idle;
    }
}

#[derive(Default)]
pub struct MessageTraceStore {
    pub candidates: MessagesStore,
    pub trace: SelectedTrace,
}
impl MessageTraceStore {
    pub fn apply(&mut self, query: MessageSearchQuery, revision: u64) -> Option<MessageQueryRequest> {
        self.trace.clear();
        self.candidates.apply(query, revision)
    }
    pub fn finish_candidates(
        &mut self,
        request: &MessageQueryRequest,
        revision: u64,
        result: Result<MessageSearchPage, UiError>,
    ) -> bool {
        self.candidates.finish(request, revision, result)
    }
    pub fn select(&mut self, identity: &MessageIdentity, trace_topic: String, revision: u64) -> Option<TraceRequest> {
        self.trace.clear();
        let row = self.candidates.select(identity)?;
        self.trace.begin(row.identity, trace_topic, revision)
    }
    pub fn edit_candidates(&mut self) {
        self.candidates.edit_draft();
        self.trace.clear();
    }
    pub fn leave(&mut self) {
        self.candidates.cancel_reads();
        self.trace.clear();
    }
    pub fn clear_context(&mut self) {
        self.candidates.clear_context();
        self.trace.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        services::messages::test_support::{page, row},
        state::UiErrorCode,
    };
    use rocketmq_dashboard_common::DEFAULT_TRACE_TOPIC;
    fn query() -> MessageSearchQuery {
        MessageSearchQuery::Key {
            topic: "orders".into(),
            key: "key".into(),
        }
    }
    fn trace(identity: MessageIdentity) -> MessageTrace {
        MessageTrace {
            identity,
            producer_message_id: None,
            trace_topic: DEFAULT_TRACE_TOPIC.into(),
            trace_topic_found: true,
            nodes: Vec::new(),
            limit_reached: false,
            unanswered_brokers: Vec::new(),
        }
    }
    #[test]
    fn trace_candidates_and_selection_reject_late_query_and_late_candidate_nodes() {
        let mut store = MessageTraceStore::default();
        let old = store.apply(query(), 7).unwrap();
        let current = store.apply(query(), 7).unwrap();
        assert!(!store.finish_candidates(&old, 7, Ok(page())));
        assert!(store.finish_candidates(&current, 7, Ok(page())));
        assert!(matches!(store.trace.state, Loadable::Idle));
        let first = store.select(&row().identity, DEFAULT_TRACE_TOPIC.into(), 7).unwrap();
        let second = store.select(&row().identity, "custom-trace".into(), 7).unwrap();
        assert!(!store.trace.finish(&first, 7, Ok(trace(row().identity))));
        assert!(matches!(store.trace.state, Loadable::InitialLoading));
        let failure = UiError::new("RPC failed.", UiErrorCode::Connection, true);
        assert!(store.trace.finish(&second, 7, Err(failure)));
        assert!(matches!(store.trace.state, Loadable::Failed { previous: None, .. }));
        let current = store.select(&row().identity, DEFAULT_TRACE_TOPIC.into(), 7).unwrap();
        assert!(store.trace.finish(&current, 7, Ok(trace(row().identity))));
        assert!(store.trace.state.value().unwrap().nodes.is_empty());
        store.edit_candidates();
        assert!(store.candidates.selected.is_none());
        assert!(matches!(store.trace.state, Loadable::Idle));
        assert!(!store.trace.finish(&current, 7, Ok(trace(row().identity))));
    }
    #[test]
    fn trace_context_and_page_leave_clear_selection_without_accepting_old_completion() {
        let mut store = MessageTraceStore::default();
        let candidates = store.apply(query(), 1).unwrap();
        store.finish_candidates(&candidates, 1, Ok(page()));
        let selected = store.select(&row().identity, DEFAULT_TRACE_TOPIC.into(), 1).unwrap();
        store.leave();
        assert!(!store.trace.finish(&selected, 1, Ok(trace(row().identity))));
        let request = store.apply(query(), 1).unwrap();
        store.clear_context();
        assert!(!store.finish_candidates(&request, 2, Ok(page())));
        assert!(store.candidates.applied.is_none());
        assert!(store.trace.query.is_none());
    }
}
