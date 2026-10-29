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

//! Applied Message queries, independent read epochs, and body-free list selection.

use crate::{
    services::messages::MessageRequestScope,
    state::{Loadable, RequestEpoch, UiError},
};
use rocketmq_dashboard_common::{MessageIdentity, MessageMetadataRow, MessageSearchPage, MessageSearchQuery};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MessageSearchMode {
    #[default]
    TopicRange,
    Key,
    MessageId,
}

#[derive(Clone)]
pub struct MessageQueryRequest {
    pub scope: MessageRequestScope,
    pub query: MessageSearchQuery,
}

pub struct MessagesStore {
    pub applied: Option<MessageSearchQuery>,
    pub rows: Loadable<MessageSearchPage>,
    pub selected: Option<MessageMetadataRow>,
    pub draft_changed: bool,
    epoch: RequestEpoch,
}
impl Default for MessagesStore {
    fn default() -> Self {
        Self {
            applied: None,
            rows: Loadable::Idle,
            selected: None,
            draft_changed: false,
            epoch: RequestEpoch::initial(),
        }
    }
}
impl MessagesStore {
    pub fn apply(&mut self, query: MessageSearchQuery, revision: u64) -> Option<MessageQueryRequest> {
        self.cancel_reads();
        self.applied = Some(query);
        self.draft_changed = false;
        self.begin(revision)
    }
    pub fn begin(&mut self, revision: u64) -> Option<MessageQueryRequest> {
        let query = self.applied.clone()?;
        let epoch = self.epoch.advance().ok()?;
        self.selected = None;
        self.rows = std::mem::replace(&mut self.rows, Loadable::Idle).begin();
        Some(MessageQueryRequest {
            scope: MessageRequestScope {
                revision,
                epoch: epoch.value(),
            },
            query,
        })
    }
    pub fn finish(
        &mut self,
        request: &MessageQueryRequest,
        revision: u64,
        result: Result<MessageSearchPage, UiError>,
    ) -> bool {
        if request.scope.revision != revision
            || request.scope.epoch != self.epoch.value()
            || self.applied.as_ref() != Some(&request.query)
        {
            return false;
        }
        self.rows = match result {
            Ok(page) => Loadable::Ready(page),
            Err(error) => std::mem::replace(&mut self.rows, Loadable::Idle).fail(error),
        };
        true
    }
    pub fn edit_draft(&mut self) {
        self.cancel_reads();
        self.draft_changed = true;
    }
    pub fn cancel_reads(&mut self) {
        let _ = self.epoch.advance();
        self.rows = Loadable::Idle;
        self.selected = None;
    }
    pub fn clear_context(&mut self) {
        self.cancel_reads();
        self.applied = None;
        self.draft_changed = false;
    }
    pub fn select(&mut self, identity: &MessageIdentity) -> Option<MessageMetadataRow> {
        let row = self
            .rows
            .value()?
            .rows
            .iter()
            .find(|row| &row.identity == identity)?
            .clone();
        self.selected = Some(row.clone());
        Some(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::messages::test_support::{page, row};
    use rocketmq_dashboard_common::MessagePageExtent;
    #[test]
    fn message_store_applied_refresh_page_and_epoch_isolate_old_selection_and_results() {
        let mut store = MessagesStore::default();
        let query = MessageSearchQuery::TopicRange {
            topic: "orders".into(),
            begin: 100,
            end: 200,
            page: 1,
            page_size: 1,
        };
        let first = store.apply(query.clone(), 7).unwrap();
        assert!(store.finish(&first, 7, Ok(page())));
        assert!(store.select(&row().identity).is_some());
        let refreshing = store.begin(7).unwrap();
        assert!(store.rows.value().is_some());
        assert!(store.selected.is_none());
        store.edit_draft();
        assert!(matches!(store.rows, Loadable::Idle));
        assert!(!store.finish(&refreshing, 7, Ok(page())));
        let replay = store.begin(7).unwrap();
        assert_eq!(replay.query, query);
        let next = store.apply(query.with_page(2).unwrap(), 7).unwrap();
        assert!(!store.finish(&replay, 7, Ok(page())));
        assert!(store.finish(
            &next,
            7,
            Ok(MessageSearchPage {
                rows: Vec::new(),
                extent: MessagePageExtent::End
            })
        ));
        assert!(store.rows.value().unwrap().rows.is_empty());
        assert!(store.selected.is_none());
        let old_context = store.begin(7).unwrap();
        store.clear_context();
        assert!(!store.finish(&old_context, 8, Ok(page())));
        assert!(store.applied.is_none());
        assert!(store.selected.is_none());
    }
    #[test]
    fn message_refresh_failure_retains_only_prior_metadata_and_new_query_clears_it() {
        let mut store = MessagesStore::default();
        let query = MessageSearchQuery::Key {
            topic: "orders".into(),
            key: "key-a".into(),
        };
        let request = store.apply(query, 1).unwrap();
        store.finish(&request, 1, Ok(page()));
        let request = store.begin(1).unwrap();
        store.finish(
            &request,
            1,
            Err(crate::state::UiError::new(
                "Read failed.",
                crate::state::UiErrorCode::Connection,
                true,
            )),
        );
        assert_eq!(store.rows.value().unwrap().rows[0], row());
        store.apply(
            MessageSearchQuery::MessageId {
                topic: "orders".into(),
                message_id: "id-b".into(),
            },
            1,
        );
        assert!(store.rows.value().is_none());
    }
}
